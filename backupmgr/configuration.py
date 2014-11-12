#!/usr/bin/env python2.7

import os
import sys
import os.path
import json
import errno
import time
import datetime
import collections
import itertools
import socket
import argparse

import dateutil.parser, dateutil.tz, dateutil.relativedelta

from . import package_logger
from . import error
from . import backend_types

LOCAL_TZ = dateutil.tz.tzlocal()
UTC_TZ = dateutil.tz.tzutc()

LOCAL_EPOCH = datetime.datetime.fromtimestamp(0).replace(tzinfo=LOCAL_TZ)
UTC_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=UTC_TZ)

if sys.platform.startswith("darwin"):
    DEFAULT_STATEFILE = "/var/db/backupmgr.state"
else:
    DEFAULT_STATEFILE = "/var/lib/backupmgr/state"

CONFIG_LOCATION = "/etc/backupmgr.conf"

WEEKDAYS = [object() for _ in xrange(7)]
MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = WEEKDAYS
MONTHLY = object()
WEEKLY = object()

WEEKDAY_NUMBERS = dict(zip(WEEKDAYS, itertools.count()))

WEEKDAY_RELATIVE_DAY_MAP = [
    dateutil.relativedelta.MO,
    dateutil.relativedelta.TU,
    dateutil.relativedelta.WE,
    dateutil.relativedelta.TH,
    dateutil.relativedelta.FR,
    dateutil.relativedelta.SA,
    dateutil.relativedelta.SU
]

def module_logger():
    return package_logger().getChild("configuration")

class NoConfigError(error.Error):
    def __init__(self):
        super(NoConfigError, self).__init__("No config exists.")


class InvalidConfigError(error.Error):
    def __init__(self, msg):
        super(InvalidConfigError, self).__init__("Invalid config: {}".format(msg))


def prefix_match(s1, s2, required_length):
    return s1[:required_length] == s2[:required_length]

def validate_timespec(spec):
    if isinstance(spec, basestring):
        if spec.lower() == "weekly":
            return [WEEKLY]
        if spec.lower() == "monthly":
            return  [MONTHLY]
        if spec.lower() == "daily":
            return [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]
        else:
            spec = [spec]

    if not isinstance(spec, list) or any((not isinstance(x, basestring) for x in spec)):
        raise InvalidConfigError("Invalid timespec {}".format(spec))

    new_spec = set()
    for item in (item.lower() for item in spec):
        newitem = None
        if prefix_match(item, "monday", 2):
            newitem = MONDAY
        elif prefix_match(item, "tuesday", 2):
            newitem = TUESDAY
        elif prefix_match(item, "wednesday", 2):
            newitem = WEDNESDAY
        elif prefix_match(item, "thursday", 2):
            newitem = THURSDAY
        elif prefix_match(item, "friday", 2):
            newitem = FRIDAY
        elif prefix_match(item, "saturday", 2):
            newitem = SATURDAY
        elif prefix_match(item, "sunday", 2):
            newitem = SUNDAY
        new_spec.add(newitem)

    return new_spec

def validate_paths(paths):
    if (not isinstance(paths, dict)
        or any((not isinstance(x, basestring) for x in paths.keys()))
        or any((not isinstance(x, basestring) for x in paths.values()))):
        raise InvalidConfigError("paths should be a string -> string dictionary")

    names = set()
    for path, name in paths.items():
        if not os.path.isabs(path):
            raise InvalidConfigError("{} was not an absolute path".format(path))
        if not name or '/' in name or '\\' in name or name == '..':
            raise InvalidConfigError("Invalid name: \"{}\"".format(name))
        if name in names:
            raise InvalidConfigError("Name collision: \"{}\"".format(name))
        names.add(name)

    return paths

def next_due_run(timespec, since):
    def next_due_run_part(part):
        tgt = None
        if part is MONTHLY:
            rel = (dateutil.relativedelta
                   .relativedelta(months=1, day=1, hour=0, minute=0, second=0,
                                  microsecond=0))
            # Go to midnight on the first of the next month
            tgt = since + rel
        if part is WEEKLY:
            part = MONDAY
        if part in WEEKDAYS:
            # if since is the same weekday as we have selected, "1st <day>"
            # would just be that day, so we need to also advance one day before
            # asking for the "1st <day>".
            relative_cls = WEEKDAY_RELATIVE_DAY_MAP[WEEKDAY_NUMBERS[part]]
            day = relative_cls(1)
            rel = (dateutil.relativedelta
                   .relativedelta(weekday=day, days=+1, hour=0, minute=0,
                                  second=0, microsecond=0))
            tgt = since + rel

        assert tgt is not None
        return tgt
    return min((next_due_run_part(part) for part in timespec))


class ConfiguredBackup(object):
    @property
    def logger(self):
        return module_logger().getChild("ConfiguredBackup")

    def __init__(self, name, paths, backup_name, timespec, backends):
        self.name = name
        self.paths = paths
        self.backup_name = backup_name
        self.timespec = timespec
        self.backends = backends

    def should_run(self, last_run, now):
        due = next_due_run(self.timespec, last_run)
        return due < now

    def perform(self):
        success = True
        for backend in self.backends:
            success = success and backend.perform(self.paths, self.name)
        return success

    def get_all_archives(self, backends=None):
        if backends is None:
            backends = self.backends

        for backend in backends:
            if backend not in self.backends:
                raise Exception("Passed a backend we don't own!?")

        pairs = []

        for backend in backends:
            pairs.append(
                [backend, backend.existing_archives_for_name(self.name)])

        return pairs

    def get_backends(self):
        return list(self.backends)


class ConfiguredBackupSet(object):
    @property
    def logger(self):
        return package_logger().getChild("ConfiguredBackupSet")

    def __init__(self, state, configured_backups, config_mtime, state_mtime,
                 now):
        self.configured_backups = configured_backups
        self.config_mtime = config_mtime
        self.state_mtime = state_mtime
        self.state = state
        self.now = now

    def state_after_backups(self, backups):
        new_state = self.state.copy()
        for backup in backups:
            new_state[backup.name] = time.mktime(self.now.timetuple())
        return new_state

    def last_run_of_backup(self, backup):
        stamp = self.state.get(backup.name, 0)
        return datetime.datetime.fromtimestamp(stamp).replace(tzinfo=LOCAL_TZ)

    def backups_due(self):
        backups_to_run = []

        if self.config_mtime > self.state_mtime:
            self.logger.info("Configuration changed. Should run all backups.")
            return self.configured_backups

        for backup in self.configured_backups:
            if backup.should_run(self.last_run_of_backup(backup), self.now):
                backups_to_run.append(backup)
        return backups_to_run

    def all_backups(self):
        return self.configured_backups


def parse_simple_date(datestr):
    try:
        timestamp = float(datestr)
    except ValueError:
        date = dateutil.parser.parse(datestr)
        if date.tzinfo is None:
            # If there is no tz info in this date, assume the user meant to use
            # local time
            date = date.replace(tzinfo=dateutil.tz.tzlocal())
    else:
        date = datetime.datetime.fromtimestamp(timestamp)
        date = date.replace(tzinfo=LOCAL_TZ)

    return date

class Config(object):
    @property
    def logger(self):
        return module_logger().getChild("Config")

    def parse_args(self):
        parser = argparse.ArgumentParser(prog=self.prog)
        parser.add_argument("-q", "--quiet", action="store_true",
                            help="Be quiet on logging to stdout/stderr")
        subparsers = parser.add_subparsers()

        parser_backup = subparsers.add_parser("backup")
        parser_backup.set_defaults(verb="backup")

        parser_list = subparsers.add_parser("list")
        parser_list.set_defaults(verb="list")
        parser_list.add_argument("--before", dest="before", default=None,
                                 type=parse_simple_date)
        parser_list.add_argument("--after", dest="after", default=None,
                                 type=parse_simple_date)

        parser_restore = subparsers.add_parser("restore")
        parser_restore.set_defaults(verb="restore")
        parser_restore.add_argument("backup", metavar="BACKUPNAME", type=str)
        parser_restore.add_argument("backend", metavar="BACKENDNAME", type=str)
        parser_restore.add_argument("archive_spec", metavar="SPEC", type=str)
        parser_restore.add_argument("destination", metavar="DEST", type=str)

        parser_list_backups = subparsers.add_parser("list-configured-backups")
        parser_list_backups.set_defaults(verb="list-configured-backups")

        parser_list_backends = subparsers.add_parser("list-backends")
        parser_list_backends.set_defaults(verb="list-backends")

        return parser.parse_args(self.argv)

    def default_state(self):
        return {}

    def load_state(self):
        try:
            with open(self.statefile_path) as f:
                state = json.load(f)
        except Exception as e:
            self.logger.warn(e)
            self.logger.warn("Could not read state. Assuming default state.")
            state = self.default_state()

        return state

    def save_state(self, state):
        with open(self.statefile_path, 'w') as f:
            json.dump(state, f)

    def save_state_given_new_backups(self, backups):
        new_state = self.configured_backups.state_after_backups(backups)
        self.save_state(new_state)

    def all_configured_backups(self):
        return self.configured_backups.all_backups()

    def configured_backup_set(self):
        return self.configured_backups

    def all_configured_backends(self):
        return self.configured_backends.values()

    def __init__(self, argv, prog):
        self.argv = argv
        self.prog = prog
        ns = self.parse_args()
        self.config_options = ns

        self.configfile = CONFIG_LOCATION
        try:
            with open(self.configfile) as f:
                try:
                    config_dict = json.load(f)
                except Exception as e:
                    raise InvalidConfigError(e.message)
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoConfigError()
            else:
                raise
        self.notification_address = config_dict.get("notification_address", "root")
        self.statefile_path = config_dict.get("statefile", DEFAULT_STATEFILE)

        def parse_backend_type(backend_dict):
            if not isinstance(backend_dict, dict):
                raise InvalidConfigError("Expected a dictionary describing the backend")

            typestr = backend_dict.pop("type", None)
            BackendType = backend_types.backend_type(typestr)

            if BackendType is None:
                raise InvalidConfigError("missing or invalid type for backend: {}".format(typestr))

            return BackendType(backend_dict)

        if not isinstance(config_dict.get("backends", None), list):
            raise InvalidConfigError("Expected a list of backends")
        self.configured_backends = {
            backend.name: backend for backend in (parse_backend_type(backend_dict) for backend_dict in config_dict["backends"])
        }

        def parse_backup(backup_dict):
            if not isinstance(backup_dict, dict):
                raise InvalidConfigError("Expected a dictionary describing the backup")

            name  = backup_dict.get("name", None)
            paths = validate_paths(backup_dict.get("paths", None))
            backup_name = backup_dict.get("backup_name", None)
            timespec = validate_timespec(backup_dict.get("timespec", None))
            backends = backup_dict.get("backends", None)

            if name is None:
                raise InvalidConfigError("Backups must have names")

            if not isinstance(backends, list) or any([not isinstance(x, basestring) for x in backends]):
                raise InvalidConfigError("Expected a list of strings for backends")
            def find_backend(name):
                backend = self.configured_backends.get(name, None)
                if backend is None:
                    raise InvalidConfigError("Could not find backend {}".format(name))
                return backend
            backends = [find_backend(backend_name) for backend_name in backends]

            return ConfiguredBackup(name, paths, backup_name, timespec, backends)

        if not isinstance(config_dict.get("backups", None), list):
            raise InvalidConfigError("Expected a list of backups")
        configured_backups = [
            parse_backup(backup_dict) for backup_dict in config_dict["backups"]
        ]

        for name, count in collections.Counter([backup.name for backup in configured_backups]).items():
            if count > 1:
                raise InvalidConfigError("Duplicate backup \"{}\"".format(name))

        try:
            state_mtime = os.stat(self.statefile_path).st_mtime
        except OSError as e:
            if e.errno == errno.ENOENT:
                state_mtime = 0
            else:
                raise

        state = self.load_state()

        self.configured_backups = ConfiguredBackupSet(
            state, configured_backups, os.stat(self.configfile).st_mtime,
            state_mtime, datetime.datetime.now().replace(tzinfo=LOCAL_TZ))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import dateutil.parser, dateutil.tz

from . import package_logger
from . import error
from . import backend_types
from . import backup

from .backup import (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY,
                     SUNDAY, WEEKLY, MONTHLY)

LOCAL_TZ = dateutil.tz.tzlocal()

if sys.platform.startswith("darwin"):
    DEFAULT_STATEFILE = "/var/db/backupmgr.state"
else:
    DEFAULT_STATEFILE = "/var/lib/backupmgr/state"

CONFIG_LOCATION = "/etc/backupmgr.conf"

def module_logger():
    return package_logger().getChild("configuration")

def prefix_match(s1, s2, required_length):
    return s1[:required_length] == s2[:required_length]

def validate_timespec(spec):
    if isinstance(spec, str):
        if spec.lower() == "weekly":
            return [WEEKLY]
        if spec.lower() == "monthly":
            return  [MONTHLY]
        if spec.lower() == "daily":
            return [MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY]
        else:
            spec = [spec]

    if not isinstance(spec, list) or any((not isinstance(x, str) for x in spec)):
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
        or any((not isinstance(x, str) for x in paths.keys()))
        or any((not isinstance(x, str) for x in paths.values()))):
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


BackupPruningConfiguration = collections.namedtuple(
    "BackupPruningConfiguration", ["backup_name", "daily_count", "weekly_count", "monthly_count"])

class PruningConfiguration(object):
    def __init__(self, backup_pruning_configs):
        self.__map = {}
        for config in backup_pruning_configs:
            self.__map[config.backup_name] = config

    def get_backup_pruning_config(self, name):
        inf = float("inf")
        return self.__map.get(name, BackupPruningConfiguration(name, inf, inf, inf))


class NoConfigError(error.Error):
    def __init__(self):
        super(NoConfigError, self).__init__("No config exists.")


class InvalidConfigError(error.Error):
    def __init__(self, msg):
        super(InvalidConfigError, self).__init__("Invalid config: {}".format(msg))


class Config(object):
    @property
    def logger(self):
        return module_logger().getChild("Config")

    def parse_args(self):
        parser = argparse.ArgumentParser(prog=self.prog)
        parser.add_argument("-q", "--quiet", action="store_true",
                            help="Be quiet on logging to stdout/stderr")
        parser.add_argument("--version", action="store_const", dest="verb",
                        const="version")
        parser.set_defaults(verb=None)
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

        parser_prune = subparsers.add_parser("prune")
        parser_prune.set_defaults(verb="prune")

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

    def _parse_pruning_behavior(self, pruning_info):
        if not isinstance(pruning_info, dict):
            raise InvalidConfigError("Pruning info must be a dictionary")
        parsed_configs = []
        for name, config in pruning_info.items():
            if name not in [x.name for x in self.all_configured_backups()]:
                msg = "Attempt to define backup configuration for unknown backup {}".format(name)
                raise InvalidConfigError(msg)
            inf = float("inf")
            daily = config.get("daily", inf)
            weekly = config.get("weekly", inf)
            monthly = config.get("montly", inf)
            backup_config = BackupPruningConfiguration(name, daily, weekly, monthly)
            parsed_configs.append(backup_config)
        self.pruning_configuration = PruningConfiguration(parsed_configs)

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
                    raise InvalidConfigError(str(e))
        except IOError as e:
            if e.errno == errno.ENOENT:
                nce = NoConfigError()
                nce.__cause__ = e
                raise nce
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

            if not isinstance(backends, list) or any([not isinstance(x, str) for x in backends]):
                raise InvalidConfigError("Expected a list of strings for backends")
            def find_backend(name):
                backend = self.configured_backends.get(name, None)
                if backend is None:
                    raise InvalidConfigError("Could not find backend {}".format(name))
                return backend
            backends = [find_backend(backend_name) for backend_name in backends]

            return backup.Backup(name, paths, backup_name, timespec, backends)

        if not isinstance(config_dict.get("backups", None), list):
            raise InvalidConfigError("Expected a list of backups")
        configured_backups = [
            parse_backup(backup_dict) for backup_dict in config_dict["backups"]
        ]

        for name, count in collections.Counter([configured_backup.name for configured_backup in configured_backups]).items():
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

        self.configured_backups = backup.BackupSet(
            state, configured_backups, os.stat(self.configfile).st_mtime,
            state_mtime, datetime.datetime.now().replace(tzinfo=LOCAL_TZ))

        self._parse_pruning_behavior(config_dict.get("pruning", {}))

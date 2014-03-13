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

from . import package_logger
from . import error
from . import backend_types

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

class ConfiguredBackup(object):
    @property
    def logger(self):
        return package_logger().getChild("backup")

    def __init__(self, name, paths, backup_name, timespec, backends):
        self.name = name
        self.paths = paths
        self.backup_name = backup_name
        self.timespec = timespec
        self.backends = backends

    def should_run(self, last_run):
        if last_run == datetime.datetime.fromtimestamp(0):
            return True
        if MONTHLY in self.timespec and datetime.datetime.now().day == 0:
            return True
        if WEEKLY in self.timespec and datetime.datetime.now().weekday() == WEEKDAY_NUMBERS[MONDAY]:
            return True

        days = {WEEKDAY_NUMBERS[day] for day in self.timespec if day in WEEKDAYS}
        return datetime.datetime.now().weekday() in days

    def perform(self):
        info = {
            "hostname": socket.gethostname(),
            "timestamp": time.time()
        }
        backup_name = self.backup_name.format(**info)
        for backend in self.backends:
            backend.perform(self.paths, backup_name)


class Config(object):
    @property
    def logger(self):
        return package_logger().getChild("configuration")

    def default_state(self):
        return {}

    def __init__(self):
        try:
            with open(CONFIG_LOCATION) as f:
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
        self.statefile = config_dict.get("statefile", DEFAULT_STATEFILE)
        try:
            with open(self.statefile) as f:
                self.state = json.load(f)
        except Exception as e:
            self.logger.warn(e)
            self.logger.warn("Could not read state. Assuming default state.")
            self.state = self.default_state()

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
        self.configured_backups = [
            parse_backup(backup_dict) for backup_dict in config_dict["backups"]
        ]

        for name, count in collections.Counter([backup.name for backup in self.configured_backups]).items():
            if count > 1:
                raise InvalidConfigError("Duplicate backup \"{}\"".format(name))

    def log_run(self, backups):
        for backup in backups:
            self.state[backup.name] = time.time()
        with open(self.statefile, 'w') as f:
            json.dump(self.state, f)

    def last_run_of_backup(self, backup):
        stamp = self.state.get(backup.name, 0)
        return datetime.datetime.fromtimestamp(stamp)

    def backups_due(self):
        return [backup for backup in self.configured_backups if backup.should_run(self.last_run_of_backup(backup))]

def read_config():
    return Config()

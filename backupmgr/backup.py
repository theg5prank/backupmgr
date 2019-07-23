#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import itertools
import time

import dateutil.tz, dateutil.relativedelta

from . import package_logger

WEEKDAYS = [object() for _ in range(7)]
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

LOCAL_TZ = dateutil.tz.tzlocal()

def module_logger():
    return package_logger().getChild("backup")

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


class Backup(object):
    @property
    def logger(self):
        return module_logger().getChild("Backup")

    def __init__(self, name, paths, backup_name, timespec, backends):
        self.name = name
        self.paths = paths
        self.backup_name = backup_name
        self.timespec = timespec
        self.backends = backends

    def should_run(self, last_run, now):
        due = next_due_run(self.timespec, last_run)
        return due < now

    def perform(self, now):
        success = True
        for backend in self.backends:
            success = success and backend.perform(self.paths, self.name, now)
        return success

    def get_all_archives(self, backends=None, backend_to_primed_list_token_map=None):
        if backends is None:
            backends = self.backends

        for backend in backends:
            if backend not in self.backends:
                raise Exception("Passed a backend we don't own!?")

        pairs = []

        for backend in backends:
            token = None
            if backend_to_primed_list_token_map is not None:
                assert backend in backend_to_primed_list_token_map, "Backend {} not in primed token map?".format(backend)
                token = backend_to_primed_list_token_map[backend]
            pairs.append(
                [backend, backend.existing_archives_for_name(self.name, primed_list_token=token)])

        return pairs

    def get_backends(self):
        return list(self.backends)


class BackupSet(object):
    @property
    def logger(self):
        return package_logger().getChild("BackupSet")

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


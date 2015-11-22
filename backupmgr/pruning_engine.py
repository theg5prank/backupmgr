#!/usr/bin/env python2.7

import itertools

from . import time_utilities
from . import package_logger

class PruningEngine(object):
    @property
    def logger(self):
        return package_logger().getChild("PruningEngine")

    def __init__(self, pruning_config):
        self.pruning_config = pruning_config

    def prunable_archives(self, archives):
        fresh = []
        daily_saved = {}
        weekly_saved = {}
        monthly_saved = {}

        sorted_archives = sorted(archives, cmp=lambda x, y: cmp(y.datetime, x.datetime))

        for archive in sorted_archives:
            archive_day = time_utilities.day(archive.datetime)
            archive_week = time_utilities.week(archive.datetime)
            archive_month = time_utilities.month(archive.datetime)

            since = time_utilities.local_timestamp() - archive.datetime

            if since.days < 1:
                self.logger.info("Retaining {} because it was performed in the last 24 hours".format(archive))
                fresh.append(archive)
                continue
            elif len(daily_saved) < self.pruning_config.daily_count and archive_day not in daily_saved:
                self.logger.info("Retaining {} as a daily backup".format(archive))
                daily_saved[archive_day] = archive
            elif len(weekly_saved) < self.pruning_config.weekly_count and archive_week not in weekly_saved:
                self.logger.info("Retaining {} as a weekly backup".format(archive))
                weekly_saved[archive_week] = archive
            elif len (monthly_saved) < self.pruning_config.monthly_count and archive_month not in monthly_saved:
                self.logger.info("Retaining {} as a monthly backup".format(archive))
                monthly_saved[archive_month] = archive

        prunable = []
        saved_archives = set()
        for saved_archive in itertools.chain(fresh, daily_saved.values(),
                                             weekly_saved.values(),
                                             monthly_saved.values()):
            saved_archives.add(saved_archive)

        return [archive for archive in sorted_archives if archive not in saved_archives]

    def prune_archives(self, archives):
        for archive in archives:
            archive.destroy()

#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import unittest
import datetime

import dateutil
import dateutil.tz

from .. import backup

class NextDueRunTests(unittest.TestCase):
    specmonthly = [backup.MONTHLY]
    specm = [backup.MONDAY]
    specwkly = [backup.WEEKLY]
    specmw = [backup.MONDAY, backup.WEDNESDAY]
    speccomplex = [backup.MONTHLY, backup.WEEKLY, backup.FRIDAY]
    tests = [
        (specm, datetime.datetime(month=11, day=17, year=2014,
                                  tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=24, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "basics_ran_today"),
        (specm, datetime.datetime(month=11, day=16, year=2014,
                                  tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=17, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "basics_ran_yesterday"),
        (specwkly, datetime.datetime(month=11, day=17, year=2014,
                                  tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=24, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "weekly_ran_today"),
        (specwkly, datetime.datetime(month=11, day=16, year=2014,
                                  tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=17, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "weekly_ran_yesterday"),
        (specmw, datetime.datetime(month=11, day=17, year=2014,
                                   tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=19, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "multiple_days_ran_today"),
        (specmw, datetime.datetime(month=11, day=18, year=2014,
                                   tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=19, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "multiple_days_ran_between"),
        (specmw, datetime.datetime(month=11, day=16, year=2014,
                                   tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=17, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "multiple_days_ran_today"),
        (specmw, datetime.datetime(month=11, day=19, year=2014,
                                   tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=24, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "multiple_days_ran_later"),
        (specmw, datetime.datetime(month=11, day=19, year=2014,
                                   tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=24, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "multiple_days_ran_later"),
        (specmonthly, datetime.datetime(month=11, day=19, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=12, day=1, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "monthly_ran_mid"),
        (specmonthly, datetime.datetime(month=11, day=1, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=12, day=1, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "monthly_ran_first"),
        (specmonthly, datetime.datetime(month=11, day=30, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=12, day=1, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "monthly_ran_last"),
        (speccomplex, datetime.datetime(month=11, day=30, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=12, day=1, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "complex_ran_last"),
        (speccomplex, datetime.datetime(month=11, day=1, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=3, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "complex_ran_first"),
        (speccomplex, datetime.datetime(month=11, day=3, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=7, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "complex_ran_mon"),
        (speccomplex, datetime.datetime(month=11, day=7, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=10, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "complex_ran_fri"),
        (speccomplex, datetime.datetime(month=11, day=11, year=2014,
                                        tzinfo=dateutil.tz.tzlocal()),
         datetime.datetime(month=11, day=14, year=2014, hour=0, minute=0,
                           tzinfo=dateutil.tz.tzlocal()),
         "complex_ran_rand"),
        ]

    def mk_closure(spec, time, tgt, name):
        def tst(self):
            next = backup.next_due_run(spec, time)
            self.assertEqual(next, tgt)

        tst.__name__ = "test_{}".format(name)
        return tst

    for spec, time, tgt, name in tests:
        tst = mk_closure(spec, time, tgt, name)
        locals()[tst.__name__] = tst

    del mk_closure, tst, spec, time, tgt, name

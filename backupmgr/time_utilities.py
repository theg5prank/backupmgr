#!/usr/bin/env python3

import datetime
import dateutil
import dateutil.tz
import dateutil.relativedelta

LOCAL_TZ = dateutil.tz.tzlocal()

def local_timestamp():
    return datetime.datetime.now().replace(tzinfo=LOCAL_TZ)

def day(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def week(dt):
    return (
        dt - dateutil.relativedelta.relativedelta(weekday=dateutil.relativedelta.MO(-1))
    ).replace(hour=0, minute=0, second=0, microsecond=0)

def month(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

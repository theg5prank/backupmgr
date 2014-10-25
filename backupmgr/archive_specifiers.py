#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import dateutil.parser
import dateutil.tz
import datetime
import itertools

CONCRETE_SPECIFIERS = set()

class ArchiveSpecifierMeta(type):
    def __init__(cls, *args, **kwargs):
        super(ArchiveSpecifierMeta, cls).__init__(*args, **kwargs)
        if cls.concrete:
            CONCRETE_SPECIFIERS.add(cls)

    def __call__(cls, specifier_str, *args, **kwargs):
        if cls in CONCRETE_SPECIFIERS:
            return super(ArchiveSpecifierMeta, cls).__call__(specifier_str, *args, **kwargs)

        for specifier_type in CONCRETE_SPECIFIERS:
            if specifier_type.acceptable_specifier(specifier_str):
                return specifier_type(specifier_str)

        raise ValueError("No specifier matched {}".format(specifier_str))

    @property
    def concrete(self):
        return not self.__dict__.get("ABSTRACT", False)


class ArchiveSpecifier(object):
    __metaclass__ = ArchiveSpecifierMeta
    ABSTRACT = True


class OrdinalArchiveSpecifier(ArchiveSpecifier):
    @classmethod
    def acceptable_specifier(cls, specifier_str):
        try:
            value = int(specifier_str)
        except ValueError:
            return False
        return value < 1000000000

    def __init__(self, specifier_str):
        self.ordinal = int(specifier_str)

    def evaluate(self, archive, ordinal):
        return ordinal == self.ordinal


class TimestampArchiveSpecifier(ArchiveSpecifier):
    @classmethod
    def acceptable_specifier(cls, specifier_str):
        try:
            value = float(specifier_str)
        except ValueError:
            return False
        has_dot = "." in specifier_str
        return value > 1000000000 or has_dot

    def __init__(self, specifier_str):
        self.timestamp = float(specifier_str)

    def evaluate(self, archive, ordinal):
        return archive.timestamp == self.timestamp


class FuzzyDatetimeArchiveSpecifier(ArchiveSpecifier):
    @classmethod
    def acceptable_specifier(cls, specifier_str):
        try:
            dateutil.parser.parse(specifier_str)
        except:
            return False
        return True

    def __init__(self, specifier_str):
        default = datetime.datetime(year=1, month=1, day=1)
        dt = dateutil.parser.parse(specifier_str, default=default)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=dateutil.tz.tzlocal())
        self.datetime = dt

    def evaluate(self, archive, ordinal):
        timezone_corrected_archive_time = archive.datetime.astimezone(self.datetime.tzinfo)

        check = ["year", "month", "day", "hour", "minute", "second"]
        check = reversed(list(itertools.dropwhile(lambda k: getattr(self.datetime, k) == 0 and k != "day", reversed(check))))
        return all((getattr(self.datetime, k) == getattr(timezone_corrected_archive_time, k) for k in check))

#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

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

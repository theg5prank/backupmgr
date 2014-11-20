#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import unittest
import datetime

import dateutil

from .. import backend_types
from ..backup_backends import tarsnap

class TestBackendsRegistered(unittest.TestCase):
    def test_types_registered(self):
        self.assertIs(backend_types.backend_type("tarsnap"), tarsnap.TarsnapBackend)

class TestUnregistration(unittest.TestCase):
    def test_unregistration_works(self):
        class MyNewBackend(backend_types.BackupBackend):
            NAMES = ("mytype",)

        self.assertIs(backend_types.backend_type("mytype"), MyNewBackend)
        backend_types.unregister_backend_type("mytype")
        self.assertIsNone(backend_types.backend_type("mytype"))

class TestBackupBackendName(unittest.TestCase):
    def test_str_correctness(self):
        class MyBackend(backend_types.BackupBackend):
            NAMES = ("mytype",)
        self.assertEquals(str(MyBackend({"name":"foo"})), "MyBackend: foo")
        backend_types.unregister_backend_type("mytype")

class TestArchiveBasics(unittest.TestCase):
    def test_archive_datetime_property(self):
        arch = backend_types.Archive()
        ts = 1416279400
        arch.timestamp = ts
        dt = datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=dateutil.tz.tzutc())
        self.assertEqual(arch.datetime, dt)

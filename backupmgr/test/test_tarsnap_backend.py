#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import datetime
import subprocess
import io
import os
import sys

import mock
import dateutil

from ..backup_backends import tarsnap
from .. import backend_types

class TarasnapBackendClassTests(unittest.TestCase):
    def test_is_registered(self):
        self.assertIs(backend_types.backend_type("tarsnap"),
                      tarsnap.TarsnapBackend)

class TarsnapBackendTests(unittest.TestCase):
    def setUp(self):
        self.backend = tarsnap.TarsnapBackend({"keyfile": "/root/theKey.key",
                                               "name": "test backend",
                                               "host": "thishost"})
        self.ts = datetime.datetime.fromtimestamp(1416279400,
                                                  dateutil.tz.tzlocal())

    def test_dunder_str(self):
        expected = "TarsnapBackend: test backend (thishost with /root/theKey.key)"
        self.assertEqual(expected, str(self.backend))

    def test_backup_instance_name(self):
        name = self.backend.create_backup_instance_name("great", self.ts)
        self.assertEqual('17b690276b0184062d03b56fbf0d66b775c2a19c-1416279400.0-great',
                          name)

    def test_perform(self):
        instance_mock = mock.NonCallableMock()
        instance_mock.stdout = io.BytesIO(b"do not parse\nthis content")
        with mock.patch("subprocess.Popen",
                        return_value=instance_mock) as mock_popen:
            def do_wait():
                # take this opportunity to check the directory and make sure
                # the links are what we expect
                path = mock_popen.call_args[0][0][2]
                self.assertEqual(set(os.listdir(path)),
                                  set(["one", "two", "three"]))
                self.assertEqual(os.readlink(os.path.join(path, "one")), "/uno")
                self.assertEqual(os.readlink(os.path.join(path, "two")), "/dos")
                self.assertEqual(os.readlink(os.path.join(path, "three")), "/tres")
                do_wait.tmpdir = path
                return 0
            do_wait.tmpdir = None
            instance_mock.wait = do_wait
            self.backend.perform({"/uno":"one", "/dos":"two", "/tres":"three"},
                                 "mrgl", self.ts)

        self.assertEqual(mock_popen.call_count, 1)
        self.assertTrue(do_wait.tmpdir) # make sure we ran this bit
        self.assertEqual(len(mock_popen.call_args[0]), 1)
        self.assertEqual(mock_popen.call_args[0][0][:2], ["/usr/local/bin/tarsnap",
                                                           "-C"])
        self.assertEqual(mock_popen.call_args[0][0][3:8],
                          ["-H", "-cf",
                           "712fded485ebd593f5954e38acb78ea437c15997-1416279400.0-mrgl",
                           "--keyfile", "/root/theKey.key"])
        self.assertEqual(sorted(mock_popen.call_args[0][0][8:]),
                         sorted(["one", "two", "three"]))
        self.assertEqual(mock_popen.call_args[1]["stderr"], subprocess.STDOUT)
        self.assertEqual(mock_popen.call_args[1]["stdout"], subprocess.PIPE)
        self.assertFalse(os.path.exists(do_wait.tmpdir)) # clean up your turds

    def test_perform_exit_status(self):
        instance_mock = mock.NonCallableMock()
        instance_mock.stdout = io.BytesIO(b"boring stuff")
        with mock.patch("subprocess.Popen",
                        return_value=instance_mock) as mock_popen:
            instance_mock.wait = lambda: 0
            self.assertTrue(self.backend.perform({"/foo" : "bar"}, "mrgl", self.ts))
            instance_mock.wait = lambda: 1
            print("abnormal tarsnap exit is expected here", file=sys.stderr)
            self.assertFalse(self.backend.perform({"/foo" : "bar"}, "mrgl", self.ts))

    def test_archive_listing_calls_correctly(self):
        instance_mock = mock.NonCallableMagicMock()
        fake_output = io.BytesIO(b'')
        instance_mock.stdout = fake_output
        instance_mock.wait = mock.MagicMock(return_value=0)
        with mock.patch("subprocess.Popen",
                        return_value=instance_mock) as mock_popen:
            self.backend.existing_archives_for_name("nomatter")
            mock_popen.assert_called_once_with(
                ["/usr/local/bin/tarsnap", "--list-archives", "--keyfile",
                 "/root/theKey.key"],
                stdout=subprocess.PIPE)

    def test_archive_listing_parses_correctly_basics(self):
        instance_mock = mock.NonCallableMagicMock()
        lines = [
            b"712fded485ebd593f5954e38acb78ea437c15997-1416279400.0-mrgl",
            b"712fded485ebd593f5954e38acb78ea437c1599f-1416280000.0-brgl",
            b"712fded485ebd593f5954e38acb78ea437c15997-1416369139.0-mrgl"
        ]
        instance_mock.stdout = io.BytesIO(b"\n".join(lines))
        instance_mock.wait = lambda: 0
        with mock.patch("subprocess.Popen",
                        return_value=instance_mock) as mock_popen:
            results = self.backend.existing_archives_for_name("mrgl")

        self.assertEqual(len(results), 2)
        for result in results:
            self.assertIs(result.backend, self.backend)

        for archive, fullname in zip(results, [line for line in lines if b"mrgl" in line]):
            self.assertEqual(archive.fullname, fullname.decode('utf-8'))

        for archive, time in zip(results, [1416279400, 1416369139]):
            self.assertEqual(archive.timestamp, time)

class TestTarsnapArchive(unittest.TestCase):
    def setUp(self):
        self.backend = tarsnap.TarsnapBackend({"keyfile": "/root/theKey.key",
                                               "name": "test backend",
                                               "host": "thishost"})

        self.archive = tarsnap.TarsnapArchive(
            self.backend,
            1416279400.0,
            "712fded485ebd593f5954e38acb78ea437c15997-1416279400.0-mrgl",
            "mrgl")

    def test_restore_invokes_tarsnap_correctly(self):
        instance_mock = mock.NonCallableMagicMock()
        instance_mock.wait = lambda: 0
        with mock.patch("subprocess.Popen",
                        return_value=instance_mock) as mock_popen:
            self.archive.restore("/tmp/nothing")
            mock_popen.assert_called_once_with(
                ["/usr/local/bin/tarsnap", "-C", "/tmp/nothing", "-x", "-f",
                 "712fded485ebd593f5954e38acb78ea437c15997-1416279400.0-mrgl"],
                stderr=subprocess.STDOUT, stdout=subprocess.PIPE)

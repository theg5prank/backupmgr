#!/usr/bin/env python2.7

import os
import tempfile
import shutil
import errno
import subprocess
import hashlib
import datetime
import dateutil.tz
import time
import re

from .. import backend_types
from .. import package_logger

TARSNAP_PATH = "/usr/local/bin/tarsnap"

def backup_instance_regex(identifier, name):
    return re.compile(
        r"^(?P<identifier>{})-(?P<timestamp>\d+(.\d+)?)-(?P<name>{})$"
            .format(re.escape(identifier), re.escape(name)))

class TarsnapArchive(backend_types.Archive):
    @property
    def logger(self):
        return package_logger().getChild("tarsnap_archive")

    def __init__(self, backend, timestamp, fullname):
        self.fullname = fullname
        self.backend = backend
        self.timestamp = timestamp

    def restore(self, destination):
        argv = [TARSNAP_PATH, "-C", destination, "-x", "-f", self.fullname]
        proc = subprocess.Popen(argv, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        proc_logger = self.logger.getChild("tarsnap_output")
        for line in proc.stdout:
            proc_logger.info(line.strip())
        code = proc.wait()
        if code != 0:
            self.logger.error("Tarsnap invocation failed with exit code {}".format(code))
            return False
        return True


class TarsnapBackend(backend_types.BackupBackend):
    NAMES = {"tarsnap"}

    @property
    def logger(self):
        return package_logger().getChild("tarsnap_backend")

    def __init__(self, config):
        super(TarsnapBackend, self).__init__(config)
        self.keyfile = config.pop("keyfile", None)
        self.host = config.pop("host", None)

    def __str__(self):
        addendum = " ({} with {})".format(self.host, self.keyfile)
        return super(TarsnapBackend, self).__str__() + addendum

    def create_backup_identifier(self, backup_name):
        ctx = hashlib.sha1()
        ctx.update(self.name.decode("utf-8"))
        ctx.update(backup_name.decode("utf-8"))
        return ctx.hexdigest()

    def create_backup_instance_name(self, backup_name, timestamp):
        unixtime = time.mktime(timestamp.timetuple())
        return "{}-{}-{}".format(self.create_backup_identifier(backup_name),
                                 unixtime, backup_name)

    def perform(self, paths, backup_name, now_timestamp):
        backup_instance_name = self.create_backup_instance_name(backup_name,
                                                                now_timestamp)
        self.logger.info("Creating backup \"{}\": {}"
                            .format(backup_instance_name, ", ".join(paths)))
        tmpdir = tempfile.mkdtemp()
        try:
            for path, name in paths.items():
                os.symlink(path, os.path.join(tmpdir, name))
            argv = [TARSNAP_PATH, "-C", tmpdir, "-H", "-cf", backup_instance_name]
            if self.keyfile is not None:
                argv += ["--keyfile", self.keyfile]
            argv += paths.values()
            self.logger.info("Invoking tarsnap: {}".format(argv))
            proc = subprocess.Popen(argv, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
            proc_logger = self.logger.getChild("tarsnap_output")
            for line in proc.stdout:
                proc_logger.info(line.strip())
            code = proc.wait()
            if code != 0:
                self.logger.error("Tarsnap invocation failed with exit code {}".format(code))
                return False
            else:
                return True
        finally:
            for path, name in paths.items():
                path = os.path.join(tmpdir, name)
                try:
                    os.unlink(path)
                except OSError, e:
                    if e.errno == errno.ENOENT:
                        pass
            os.rmdir(tmpdir)

    def existing_archives_for_name(self, backup_name):
        argv = [TARSNAP_PATH, "--list-archives"]
        if self.keyfile is not None:
            argv += ["--keyfile", self.keyfile]

        proc = subprocess.Popen(argv, stdout=subprocess.PIPE)

        identifier = self.create_backup_identifier(backup_name)
        regex = backup_instance_regex(identifier, backup_name)

        results = []
        for line in proc.stdout:
            m = regex.match(line)
            if m:
                ts = float(m.groupdict()["timestamp"])
                results.append(TarsnapArchive(self, ts, m.group()))

        if proc.wait() != 0:
            self.logger.error("Tarsnap invocation failed with exit code {}".format(proc.returncode))

        return results

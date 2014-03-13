#!/usr/bin/env python2.7

import os
import tempfile
import shutil
import errno
import subprocess

from .. import backend_types
from .. import package_logger

TARSNAP_PATH = "/usr/local/bin/tarsnap"

class TarsnapBackend(backend_types.BackupBackend):
    NAMES = {"tarsnap"}

    @property
    def logger(self):
        return package_logger().getChild("tarsnap_backend")

    def __init__(self, config):
        super(TarsnapBackend, self).__init__(config)
        self.keyfile = config.pop("keyfile", None)

    def perform(self, paths, backup_name):
        self.logger.info("Creating backup \"{}\": {}".format(backup_name, " ,".join(paths)))
        tmpdir = tempfile.mkdtemp()
        try:
            for path, name in paths.items():
                os.symlink(path, os.path.join(tmpdir, name))
            argv = [TARSNAP_PATH, "-C", tmpdir, "-H", "-cf", backup_name]
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

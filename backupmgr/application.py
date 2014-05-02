#!/usr/bin/env python2.7

import logging
import os
import sys
import traceback
import datetime

from . import configuration
from . import package_logger
from . import error
from . import backend_types
from . import logging_handlers

class Application(object):
    @property
    def logger(self):
        return package_logger().getChild("application")

    def __init__(self, argv):
        self.argv = argv

    def configure_logging(self):
        logging.basicConfig()
        l = package_logger()
        l.propagate=False
        l.setLevel(logging.DEBUG)
        self.email_handler = logging_handlers.EmailHandler("root", "root")
        self.stderr_handler = logging_handlers.SwitchableStreamHandler()
        self.stderr_handler.formatter = logging.Formatter('%(levelname)s: %(name)s: %(message)s')
        l.addHandler(self.email_handler)
        l.addHandler(self.stderr_handler)

    def bootstrap(self):
        self.configure_logging()
        backend_types.load_backend_types()

    def load_config(self):
        self.config = configuration.Config(self.argv, "backupmgr")
        self.email_handler.toaddr = self.config.notification_address
        if self.config.config_options.quiet:
            self.stderr_handler.disable()

    def prepare_backups(self):
        backups = self.config.configured_backups.backups_due()
        self.logger.info("Backups due: {}".format(", ".join([b.name for b in backups])))
        return backups

    def note_successful_backups(self, backups):
        self.config.save_state_given_new_backups(backups)

    def should_send_email(self):
        os.isatty(0)

    def finalize(self):
        if self.should_send_email():
            self.email_handler.finalize()

    def perform_backups(self):
        backups = self.prepare_backups()
        backup_successes = []
        for backup in backups:
            if backup.perform():
                backup_successes.append(backup)
        self.note_successful_backups(backup_successes)
        self.logger.info("Successfully completed {}/{} backups.".format(len(backup_successes), len(backups)))

    def unknown_verb(self):
        raise Exception("Unknown verb")

    def run(self):
        verbs = {
            "backup": self.perform_backups
        }
        try:
            self.bootstrap()
            self.load_config()
            verbs.get(self.config.config_options.verb, self.unknown_verb)()
        except error.Error as e:
            self.logger.fatal(e.message)
            sys.exit(1)
        except Exception as e:
            self.logger.fatal("Unexpected error: {}".format(e))
            self.logger.fatal(traceback.format_exc())
            sys.exit(1)
        else:
            sys.exit(0)
        finally:
            self.finalize()

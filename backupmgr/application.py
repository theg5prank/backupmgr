#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import sys
import traceback
import datetime
import dateutil
import dateutil.tz

from . import configuration
from . import package_logger
from . import error
from . import backend_types
from . import logging_handlers
from . import archive_specifiers
from . import pruning_engine

def pretty_archive(archive):
    local_time = archive.datetime.astimezone(dateutil.tz.tzlocal())
    human_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
    return "{} ({})".format(human_time, archive.timestamp)

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

    def get_due_backups(self):
        backups = self.config.configured_backup_set().backups_due()
        self.logger.info("Backups due: {}".format(", ".join([b.name for b in backups])))
        return backups

    def get_all_backups(self):
        return self.config.all_configured_backups()

    def get_backup_by_name(self, name):
        for backup in self.get_all_backups():
            if backup.name == name:
                return backup
        else:
            raise error.Error("Couldn't find backup with name {}".format(name))

    def get_all_backends(self):
        return self.config.all_configured_backends()

    def get_backend_by_name(self, name):
        for backend in self.get_all_backends():
            if backend.name == name:
                return backend
        else:
            raise error.Error("Couldn't find backend with name {}".format(name))

    def note_successful_backups(self, backups):
        self.config.save_state_given_new_backups(backups)

    def should_send_email(self):
        return not os.isatty(0)

    def within_timespec(self, archive):
        before = self.config.config_options.before
        after = self.config.config_options.after
        archive_date = archive.datetime
        if before is not None and archive_date >= before:
            return False
        if after is not None and archive_date <= after:
            return False
        return True

    def finalize(self):
        if self.should_send_email():
            self.email_handler.finalize()

    def perform_backups(self):
        backups = self.get_due_backups()
        backup_successes = []
        for backup in backups:
            if backup.perform(datetime.datetime.now(dateutil.tz.tzlocal())):
                backup_successes.append(backup)
        self.note_successful_backups(backup_successes)
        self.logger.info("Successfully completed {}/{} backups.".format(len(backup_successes), len(backups)))

    def get_backend_to_primed_list_token_map(self):
        backend_to_primed_list_token_map = {}
        for backup in self.get_all_backups():
            for backend in backup.backends:
                if backend not in backend_to_primed_list_token_map:
                    token = backend.get_primed_list_token()
                    backend_to_primed_list_token_map[backend] = token
        return backend_to_primed_list_token_map


    def list_archives(self):
        backend_to_primed_list_token_map = self.get_backend_to_primed_list_token_map()

        for backup in self.get_all_backups():
            sys.stdout.write("{}:\n".format(backup.name))
            for backend, archives in backup.get_all_archives(backend_to_primed_list_token_map=backend_to_primed_list_token_map):
                sorted_archives = sorted(archives, key=lambda x: x.datetime)
                enumerated_archives = ((i, archive) for i, archive in enumerate(sorted_archives) if self.within_timespec(archive))
                sys.stdout.write("\t{}:\n".format(backend.name))
                for i, archive in enumerated_archives:
                    sys.stdout.write("\t\t{}: {}\n".format(i, pretty_archive(archive)))

    def list_configured_backups(self):
        for backup in self.get_all_backups():
            sys.stdout.write("{}\n".format(backup.name))
            for backend in backup.get_backends():
                sys.stdout.write("\tto {}\n".format(backend.name))

    def list_backends(self):
        for backend in self.get_all_backends():
            sys.stdout.write("{}\n".format(backend))

    def restore_backup(self):
        backup_name = self.config.config_options.backup
        backup = self.get_backup_by_name(backup_name)
        backend_name = self.config.config_options.backend
        backend = self.get_backend_by_name(backend_name)
        if backend not in backup.backends:
            raise error.Error(
                "backend {} not configured for backup {}".format(backend_name,
                                                                 backup_name))
        spec_str = self.config.config_options.archive_spec
        spec = archive_specifiers.ArchiveSpecifier(spec_str)
        matches = []
        for _, archives in backup.get_all_archives(backends=[backend]):
            for i, archive in enumerate(sorted(archives, key=lambda x: x.datetime)):
                if spec.evaluate(archive, i):
                    matches.append(archive)

        if len(matches) > 1:
            msg = "Spec {} matched more than one archive!".format(spec_str)
            for match in matches:
                msg += "\n\t{}".format(pretty_archive(match))
            raise error.Error(msg)
        if len(matches) == 0:
            raise error.Error("Spec {} matched no archives!".format(spec_str))

        archive = matches[0]
        return archive.restore(self.config.config_options.destination)

    def prune_archives(self):
        backend_to_primed_list_token_map = self.get_backend_to_primed_list_token_map()
        for backup in self.get_all_backups():
            pruning_config = self.config.pruning_configuration.get_backup_pruning_config(backup.name)
            for backend, archives in backup.get_all_archives(backend_to_primed_list_token_map=backend_to_primed_list_token_map):
                engine = pruning_engine.PruningEngine(pruning_config)
                archives_to_prune = engine.prunable_archives(archives)
                engine.prune_archives(archives_to_prune)

    def print_version(self):
        from . import _metadata
        print(f"backupmgr {_metadata.__version__}")

    def unknown_verb(self):
        raise Exception("Unknown verb")

    def run(self):
        verbs = {
            "backup": self.perform_backups,
            "list": self.list_archives,
            "list-configured-backups": self.list_configured_backups,
            "list-backends": self.list_backends,
            "restore": self.restore_backup,
            "prune": self.prune_archives,
            "version": self.print_version,
        }
        try:
            self.bootstrap()
            self.load_config()
            if self.config.config_options.verb is None:
                self.logger.fatal("No verb provided.")
                ok = False
            else:
                ok = verbs.get(self.config.config_options.verb, self.unknown_verb)()
            sys.exit(0 if ok or ok is None else 1)
        except error.Error as e:
            self.logger.fatal(str(e))
            sys.exit(1)
        except Exception as e:
            self.logger.fatal("Unexpected error: {}".format(e))
            self.logger.fatal(traceback.format_exc())
            sys.exit(1)
        finally:
            self.finalize()

#!/usr/bin/env python2.7

import pkgutil
import inspect

from . import error

class BackendConfigurationError(error.Error):
    pass

_BACKEND_TYPES = {}

def register_backend_type(type, name):
    if name in _BACKEND_TYPES:
        raise Exception("Backend name collision: {}".format(name))
    _BACKEND_TYPES[name] = type

def backend_type(name):
    return _BACKEND_TYPES.get(name, None)


class BackendType(type):
    def __init__(self, *args, **kwargs):
        super(BackendType, self).__init__(*args, **kwargs)

        for name in self.NAMES:
            register_backend_type(self, name)


class BackupBackend(object):
    __metaclass__ = BackendType
    NAMES = ()

    def __init__(self, config):
        self.name = config.pop("name")
        if self.name is None:
            raise BackendConfigurationError("Missing name for backend")

    def __str__(self):
        return "{}: {}".format(self.__class__.__name__, self.name)

def load_backend_types():
    from . import backup_backends

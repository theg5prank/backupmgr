#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import os
import sys
import inspect
import unittest

def get_suite():
    this_file = os.path.abspath(inspect.getsourcefile(lambda: None))
    tests_dir = os.path.dirname(this_file)
    package_dir = os.path.join(tests_dir, "..")
    container_dir = os.path.join(package_dir, "..")
    loader = unittest.defaultTestLoader
    suite = loader.discover(package_dir, top_level_dir=container_dir)
    return suite

def main():
    try:
        import mock
    except ImportError:
        sys.stderr.write("Need the mock library to run backupmgr tests.\n")
        return 1
    if unittest.TextTestRunner().run(get_suite()).wasSuccessful():
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())

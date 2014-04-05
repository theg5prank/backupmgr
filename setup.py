#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import email.utils
import imp
import inspect
import pkg_resources
import pydoc
import setuptools

SRCROOT = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: None)))
READMES = [
    'README.txt',
    'CHANGES.txt'
]


def metadata(fullname):
    mdpath = fullname.split('.')
    mdpath.append('_metadata.py')
    module = imp.new_module(fullname)
    module.__file__ = os.path.join(SRCROOT, *mdpath)
    with open(module.__file__, 'r') as fh:
        exec fh in vars(module)
    return module

def setup(args=None):
    # make sure our directory is at the front of sys.path
    module = metadata('backupmgr')

    # get the version and description from the source
    version = module.__version__
    description = pydoc.splitdoc(pydoc.getdoc(module))[0]
    author, author_email = email.utils.parseaddr(module.__authors__[0])

    # get the long description from README-type files
    long_description = []
    for path in READMES:
        with open(os.path.join(SRCROOT, path), 'r') as fh:
            long_description.append(fh.read())
    long_description = '\n'.join([ x for x in long_description if x ])
    # use setuptools to do the rest
    setuptools.setup(
        name=pkg_resources.safe_name(module.__name__),
        packages=setuptools.find_packages(),
        version=version,
        description=description,
        author=author,
        author_email=author_email,
        zip_safe=True,
        #url=None,
        install_requires=None,
        long_description=long_description,
        license='BSD',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
        ])

if __name__ == '__main__':
    sys.exit(setup())

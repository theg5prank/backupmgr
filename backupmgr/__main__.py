#!/usr/bin/env python2.7

import sys
import json

from . import application

def main():
    return application.Application(sys.argv[1:]).run()

if __name__ == "__main__":
    sys.exit(main())

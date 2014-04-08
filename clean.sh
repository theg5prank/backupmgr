#!/bin/sh

find . -name '*.pyc' | xargs rm
rm -rf build
rm -rf *.egg-info
rm -rf dist
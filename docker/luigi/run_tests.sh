#!/bin/bash

# Fail on the first error
set -e

echo Installing test dependencies
pip install -r requirements/test.pip

flake8

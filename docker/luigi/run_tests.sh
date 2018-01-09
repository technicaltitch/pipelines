#!/bin/bash

# Fail on the first error
set -e

echo Installing test dependencies
pip install -r requirements/test.pip

echo Running flake8
flake8

echo flake8 successful
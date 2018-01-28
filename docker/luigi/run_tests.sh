#!/bin/bash

# Fail on the first error
set -e

echo Running flake8
flake8
echo flake8 successful

echo Running mypy
mypy $(pwd) --ignore-missing-imports
echo mypy successful

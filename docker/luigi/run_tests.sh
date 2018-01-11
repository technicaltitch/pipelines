#!/bin/bash

# Fail on the first error
set -e

echo Running flake8
flake8
echo flake8 successful

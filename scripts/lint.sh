#!/usr/bin/env bash
set -e

black pydataapi tests --check
isort --recursive --check-only pydataapi tests
mypy pydataapi
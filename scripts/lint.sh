#!/usr/bin/env bash
set -e

black pydataapi tests --check
isort --check-only pydataapi tests
mypy pydataapi
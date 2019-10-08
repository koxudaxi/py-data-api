#!/usr/bin/env bash
set -e

black pydataapi tests --check --skip-string-normalization
isort --recursive --check-only -w 88  --combine-as --thirdparty pydataapi pydataapi tests -m 3 -tc
mypy pydataapi --disallow-untyped-defs --ignore-missing-imports
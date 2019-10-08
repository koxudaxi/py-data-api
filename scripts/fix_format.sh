#!/usr/bin/env bash
set -e

black pydataapi tests --skip-string-normalization
isort --recursive -w 88  --combine-as --thirdparty pydataapi pydataapi tests -m 3 -tc

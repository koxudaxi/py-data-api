#!/usr/bin/env bash
set -e

black pydataapi tests
isort pydataapi tests

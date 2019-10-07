#!/usr/bin/env bash
set -e

pytest --cov=pydataapi --ignore-glob=tests/integration/** tests
pytest  --docker-compose-no-build --use-running-containers --docker-compose=tests/integration/docker-compose.yml tests/integration/
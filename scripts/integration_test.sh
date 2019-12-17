#!/usr/bin/env bash
set -e

export AWS_DEFAULT_REGION=us-west-2
pytest --cov=pydataapi  --cov-report=xml --cov-report=term-missing --cov-append --docker-compose-no-build --use-running-containers --docker-compose=tests/integration/docker-compose.yml tests/integration/
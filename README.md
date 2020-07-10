# py-data-api - Data API Client for Python

[![Test Status](https://github.com/koxudaxi/py-data-api/workflows/Test/badge.svg)](https://github.com/koxudaxi/py-data-api/actions)
[![PyPI version](https://badge.fury.io/py/pydataapi.svg)](https://badge.fury.io/py/pydataapi)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydataapi)](https://pypi.python.org/pypi/pydataapi)
[![codecov](https://codecov.io/gh/koxudaxi/py-data-api/branch/master/graph/badge.svg)](https://codecov.io/gh/koxudaxi/py-data-api)
![license](https://img.shields.io/github/license/koxudaxi/py-data-api.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

py-data-api is a client for Data API of [Aurora Serverless](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html).
Also, the package includes SQLAlchemy Dialects and DB API 2.0 Client.

## Features
- SQLAlchemy Dialects
- DB API 2.0 compatible client [PEP 249](https://www.python.org/dev/peps/pep-0249/)

## Support Database Engines
- MySQL
- PostgreSQL

## What's AWS Aurora Serverless's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html

## This project is an experimental phase.
Warning: Some interface will be changed.

## How to install
pydataapi requires Python 3.6.1 or later 
```bash
$ pip install pydataapi
```

## Example

```python
from typing import List

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from pydataapi import DataAPI, Result


class Pets(declarative_base()):
    __tablename__ = 'pets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255, collation='utf8_unicode_ci'), default=None)


database: str = 'test'
resource_arn: str = 'arn:aws:rds:us-east-1:123456789012:cluster:serverless-test-1'
secret_arn: str = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:serverless-test1'

def example_driver_for_sqlalchemy():
    from sqlalchemy.engine import create_engine
    engine = create_engine(
        'mysql+pydataapi://',
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test'}
    )

    result = engine.execute("select * from pets")
    print(result.fetchall())

def example_simple_execute():
    data_api = DataAPI(resource_arn=resource_arn, secret_arn=secret_arn, database=database)
    result: Result = data_api.execute('show tables')
    print(result.scalar())
    # Pets
```

## Contributing to pydataapi
We are waiting for your contributions to `pydataapi`.

### How to contribute
[https://koxudaxi.github.io/py-data-api/contributing](https://koxudaxi.github.io/py-data-api/contributing)


## Related projects
### local-data-api

DataAPI Server for local 

https://github.com/koxudaxi/local-data-api

## PyPi 

[https://pypi.org/project/pydataapi](https://pypi.org/project/pydataapi)

## Source Code

[https://github.com/koxudaxi/py-data-api](https://github.com/koxudaxi/py-data-api)

## Documentation

[https://koxudaxi.github.io/py-data-api](https://koxudaxi.github.io/py-data-api)

## License

py-data-api is released under the MIT License. http://www.opensource.org/licenses/mit-license

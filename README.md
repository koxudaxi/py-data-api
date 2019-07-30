# py-data-api - Data API Client for Python

[![Build Status](https://travis-ci.org/koxudaxi/py-data-api.svg?branch=master)](https://travis-ci.org/koxudaxi/py-data-api)
[![PyPI version](https://badge.fury.io/py/pydataapi.svg)](https://badge.fury.io/py/pydataapi)
[![codecov](https://codecov.io/gh/koxudaxi/py-data-api/branch/master/graph/badge.svg)](https://codecov.io/gh/koxudaxi/py-data-api)

py-data-api is a user-friendly client which supports SQLAlchemy models.

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
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert

from pydataapi import DataAPI, transaction


class Users(declarative_base()):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255, collation='utf8_unicode_ci'), default=None)


database: str = 'test'
resource_arn: str = 'arn:aws:rds:us-east-1:123456789012:cluster:serverless-test-1'
secret_arn: str = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:serverless-test1'


@transaction(database=database, resource_arn=resource_arn, secret_arn=secret_arn)
def add_users(data_api: DataAPI, user_names: List[str]) -> None:
    # start transaction
    for user_name in user_names:
        data_api.execute(Insert(Users, {'name': user_name}))
        # some logic ...

    # commit


def example_simple_execute():
    data_api = DataAPI(resource_arn, secret_arn, database=database)
    result = data_api.execute('show tables')
    print(result)
    # [[Persons, Users, Pets]]


def example_decorator():
    user_names: List[str] = ['ken', 'rei', 'lisa', 'taro']
    add_users(user_names)


def example_with_statement():
    with DataAPI(database=database, resource_arn=resource_arn, secret_arn=secret_arn) as data_api:

        # start transaction

        insert: Insert = Insert(Users, {'name': 'ken'})
        # INSERT INTO users (name) VALUES ('ken')

        result = data_api.execute(insert)
        print(result)
        # [Result(generated_fields=None, number_of_records_updated=1)]

        query = Query(Users).filter(Users.id == 1)
        result = data_api.execute(query)
        # SELECT users.id, users.name FROM users WHERE users.id = 1

        print(result)
        # [[1, 'ken']]

        result = data_api.execute('select * from users', with_columns=True)
        print(result)
        # [{'id': 1, 'name': 'ken'}]

        # batch insert
        insert: Insert = Insert(Users)
        data_api.execute(insert, [
            {'id': 2, 'name': 'rei'},
            {'id': 3, 'name': 'lisa'},
            {'id': 4, 'name': 'taro'},
        ])

        result = data_api.execute('select * from users')
        print(result)
        # [[1, 'ken'], [2, 'rei'], [3, 'lisa'], [4, 'taro']]

        # commit


def example_rollback():
    with DataAPI(resource_arn, secret_arn) as data_api:
        data_api.execute(Insert(Users, {'name': 'ken'}))
        # you can rollback by Exception
        raise Exception


def example_rollback_with_custom_exception():
    class OriginalError(Exception):
        pass

    with DataAPI(resource_arn, secret_arn, rollback_exception=OriginalError) as data_api:

        data_api.execute(Insert(Users, {'name': 'ken'}))
        # some logic ...

        # rollback when happen `rollback_exception`
        raise OriginalError  # rollback

        # raise Exception <- DataAPI don't rollback

```

## 
## Features
### Implemented
- `BeginTransaction`  - core  
- `CommitTransaction` - core 
- `ExecuteStatement` - core 
- `RollbackTransaction` - core
- `BatchExecuteStatement` - core

### Not Implemented

- `ExecuteSql(Deprecated API)`


## TODO
- add documents include docstrings
- add simply function client

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

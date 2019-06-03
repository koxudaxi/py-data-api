# py-data-api - Data API Client for Python

[![Build Status](https://travis-ci.org/koxudaxi/py-data-api.svg?branch=master)](https://travis-ci.org/koxudaxi/py-data-api)

py-data-api is a user-friendly client which supports SQLAlchemy models.


## What's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html



```python
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert

from pydataapi.pydataapi import DataApi


class Users(declarative_base()):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255, collation='utf8_unicode_ci'), default=None)



with DataApi(database='test', resource_arn='arn:aws:rds:us-east-1:123456789012:cluster:serverless-test-1',
             secret_arn='arn:aws:secretsmanager:us-east-1:123456789012:secret:serverless-test1') as data_api:

    # start transaction

    insert: Insert = Insert(Users, {'name': 'ken'})
    # INSERT INTO my_table (name) VALUES ('ken')

    result = data_api.execute(insert)
    print(result)
    # [{'id': 1, 'name': 'ken'}]

    query = Query(Users).filter(Users.id == 1)
    result = data_api.execute(query)
    # SELECT users.id, users.name FROM users WHERE my_table.id = 1

    print(result)
    # [[1, 'ken']]

    result = data_api.execute('select * from users', with_columns=True)
    print(result)
    # [{'id': 1, 'name': 'ken'}]

    # commit transaction

```

## Features
### Implemented
- `BeginTransaction`  - core  
- `CommitTransaction` - core 
- `ExecuteStatement` - core 
- `RollbackTransaction` - core


### Not Implemented
- `BatchExecuteStatement`
- `ExecuteSql`


## This project is an experimental phase.
Warning: Some interface will be change.

## TODO
- upload to pypi for pip
- add unittests
- add documents include docstrings
- add simply function client

## Related projects
### local-data-api

DataAPI Server for local 

https://github.com/koxudaxi/local-data-api
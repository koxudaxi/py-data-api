from typing import List

from sqlalchemy import Column, Integer, String
from sqlalchemy.engine import ResultProxy
from sqlalchemy.ext.declarative import declarative_base

from pydataapi import DataAPI, Record, Result, transaction


class Pets(declarative_base()):
    __tablename__ = 'pets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255, collation='utf8_unicode_ci'), default=None)


database: str = 'test'
resource_arn: str = 'arn:aws:rds:us-east-1:123456789012:cluster:serverless-test-1'
secret_arn: str = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:serverless-test1'


def example_with_statement():
    # DataAPI supports with statement for handling transaction
    with DataAPI(database=database, resource_arn=resource_arn, secret_arn=secret_arn) as data_api:

        # start transaction

        insert: Insert = Insert(Pets, {'name': 'dog'})
        # INSERT INTO pets (name) VALUES ('dog')

        # `execute` accepts SQL statement as str or SQL Alchemy SQL objects
        result: Result = data_api.execute(insert)
        print(result.number_of_records_updated)
        # 1

        query = Query(Pets).filter(Pets.id == 1)
        result: Result = data_api.execute(query)  # or data_api.execute('select id, name from pets')
        # SELECT pets.id, pets.name FROM pets WHERE pets.id = 1

        # `Result` like a Result object in SQL Alchemy
        print(result.scalar())
        # 1

        print(result.one())
        # [Record<id=1, name='dog'>]

        # `Result` is Sequence[Record]
        records: List[Record] = list(result)
        print(records)
        # [Record<id=1, name='dog'>]

        # Record is Sequence and Iterator
        record = records[0]
        print(record[0])
        # 1
        print(record[1])
        # dog

        for column in record:
            print(column)
            # 1 ...

        # show record as dict()
        print(record.dict())
        # {'id': 1, 'name': 'dog'}

        # batch insert
        insert: Insert = Insert(Pets)
        data_api.batch_execute(insert, [
            {'id': 2, 'name': 'cat'},
            {'id': 3, 'name': 'snake'},
            {'id': 4, 'name': 'rabbit'},
        ])

        result = data_api.execute('select * from pets')
        print(list(result))
        # [Record<id=1, name='dog'>, Record<id=2, name='cat'>, Record<id=3, name='snake'>, Record<id=4, name='rabbit'>]

        # result is a sequence object
        for record in result:
            print(record)
            # Record<id=1, name='dog'> ...

        # commit


def example_decorator():
    pet_names: List[str] = ['dog', 'cat', 'snake']
    add_pets(pet_names)


@transaction(database=database, resource_arn=resource_arn, secret_arn=secret_arn)
def add_pets(data_api: DataAPI, pet_names: List[str]) -> None:
    # start transaction
    for pet_name in pet_names:
        data_api.execute(Insert(Pets, {'name': pet_name}))
        # some logic ...

    # commit


def example_simple_execute():
    data_api = DataAPI(resource_arn=resource_arn, secret_arn=secret_arn, database=database)
    result: Result = data_api.execute('show tables')
    print(result.scalar())
    # Pets


def example_rollback():
    with DataAPI(resource_arn=resource_arn, secret_arn=secret_arn) as data_api:
        data_api.execute(Insert(Pets, {'name': 'dog'}))
        # you can rollback by Exception
        raise Exception


def example_rollback_with_custom_exception():
    class OriginalError(Exception):
        pass

    with DataAPI(resource_arn=resource_arn, secret_arn=secret_arn, rollback_exception=OriginalError) as data_api:
        data_api.execute(Insert(Pets, {'name': 'dog'}))
        # some logic ...

        # rollback when happen `rollback_exception`
        raise OriginalError  # rollback

        # raise Exception <- DataAPI don't rollback


def example_driver_for_sqlalchemy():
    from sqlalchemy.engine import create_engine
    import boto3
    client = boto3.client('rds-data')
    engine = create_engine(
        'mysql+pydataapi://',  # or 'postgresql+pydataapi://',
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': client},
    )

    result: ResultProxy = engine.execute("select * from pets")
    print(result.fetchall())

import time
from typing import List

import boto3
import pytest
from pydataapi import DataAPI, Result, transaction
from pydataapi.pydataapi import Record
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query, sessionmaker
from sqlalchemy.sql import Insert

pytest_plugins = ["docker_compose"]


class Pets(declarative_base()):
    __tablename__ = 'pets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255, collation='utf8_unicode_ci'), default=None)


database: str = 'test'
resource_arn: str = 'arn:aws:rds:us-east-1:123456789012:cluster:dummy'
secret_arn: str = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'


def get_connection() -> Connection:
    return create_engine(
        'mysql+pymysql://root:example@127.0.0.1:13306/test?charset=utf8mb4'
    ).connect()


@pytest.fixture(scope='module')
def db_connection(module_scoped_container_getter) -> Connection:
    retries = 60
    while True:
        try:
            connection = get_connection()
            try:
                yield connection
            finally:
                if not connection.closed:
                    connection.close()
                break
        except Exception as e:
            print(str(e))
            if retries > 0:
                retries -= 1
                time.sleep(1)
                continue
            raise


@pytest.fixture()
def create_table(db_connection) -> None:
    db_connection.execute('drop table if exists pets;')
    db_connection.execute(
        'create table pets (id int auto_increment not null primary key, name varchar(10));'
    )


@pytest.fixture()
def rds_data_client(db_connection, create_table):
    return boto3.client(
        'rds-data',
        endpoint_url='http://127.0.0.1:8080',
        aws_access_key_id='aaa',
        aws_secret_access_key='bbb',
    )


def test_simple_execute(rds_data_client):
    data_api = DataAPI(
        resource_arn=resource_arn,
        secret_arn=secret_arn,
        database=database,
        client=rds_data_client,
    )
    result: Result = data_api.execute('show tables')
    assert len(result.one()) == 1
    assert result.one()[0] == 'pets'


def test_decorator(rds_data_client, db_connection):
    @transaction(
        database=database,
        resource_arn=resource_arn,
        secret_arn=secret_arn,
        client=rds_data_client,
    )
    def add_pet(data_api: DataAPI, pet_names: List[str]) -> None:
        response = data_api.execute(Insert(Pets, {'name': pet_names[0]}))
        assert response.generated_fields_first == 1
        response = data_api.execute(Insert(Pets, {'name': pet_names[1]}))
        assert response.generated_fields_first == 2

    pet_names: List[str] = ['dog', 'cat']
    add_pet(pet_names)
    result = list(db_connection.execute('select * from pets'))
    assert result[0][1] == 'dog'
    assert result[1][1] == 'cat'


def test_with_statement(rds_data_client, db_connection):
    with DataAPI(
        database=database,
        resource_arn=resource_arn,
        secret_arn=secret_arn,
        client=rds_data_client,
    ) as data_api:
        insert: Insert = Insert(Pets, {'name': 'dog'})

        result = data_api.execute(insert)
        assert result.number_of_records_updated == 1

        query = Query(Pets).filter(Pets.id == 1)
        result = data_api.execute(query)

        assert list(result) == [Record([1, 'dog'], [])]

        result = data_api.execute('select * from pets')
        assert result.one().dict() == {'id': 1, 'name': 'dog'}

        insert: Insert = Insert(Pets)
        data_api.batch_execute(
            insert,
            [
                {'id': 2, 'name': 'cat'},
                {'id': 3, 'name': 'snake'},
                {'id': 4, 'name': 'rabbit'},
            ],
        )

        result = data_api.execute('select * from pets')
        expected = [
            Record([1, 'dog'], ['id', 'name']),
            Record([2, 'cat'], ['id', 'name']),
            Record([3, 'snake'], ['id', 'name']),
            Record([4, 'rabbit'], ['id', 'name']),
        ]
        assert list(result) == expected

        for row, expected_row in zip(result, expected):
            assert row == expected_row


def test_rollback(rds_data_client, db_connection):
    try:
        with DataAPI(resource_arn=resource_arn, secret_arn=secret_arn) as data_api:
            data_api.execute(Insert(Pets, {'name': 'dog'}))
            # you can rollback by Exception
            raise Exception
    except:
        pass
    result = list(db_connection.execute('select * from pets'))
    assert result == []


def test_rollback_with_custom_exception(db_connection):
    rds_data_client = boto3.client(
        'rds-data',
        endpoint_url='http://127.0.0.1:8080',
        aws_access_key_id='aaa',
        aws_secret_access_key='bbb',
    )

    class OriginalError(Exception):
        pass

    class OtherError(Exception):
        pass

    try:
        with DataAPI(
            resource_arn=resource_arn,
            secret_arn=secret_arn,
            rollback_exception=OriginalError,
            database=database,
            client=rds_data_client,
        ) as data_api:
            data_api.execute(Insert(Pets, {'name': 'dog'}))
            raise OriginalError  # rollback
    except:
        pass
    result = list(db_connection.execute('select * from pets'))
    assert result == []

    try:
        with DataAPI(
            resource_arn=resource_arn,
            secret_arn=secret_arn,
            rollback_exception=OriginalError,
            database=database,
            client=rds_data_client,
        ) as data_api:
            data_api.execute(Insert(Pets, {'name': 'dog'}))
            raise OtherError
    except:
        pass
    result = list(get_connection().execute('select * from pets'))
    assert result == [(2, 'dog')]


def test_dialect() -> None:
    rds_data_client = boto3.client(
        'rds-data',
        endpoint_url='http://127.0.0.1:8080',
        aws_access_key_id='aaa',
        aws_secret_access_key='bbb',
    )
    engine = create_engine(
        'mysql+pydataapi://',
        echo=True,
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': rds_data_client,
        },
    )

    assert engine.has_table('foo') is False
    assert engine.has_table('pets') is True

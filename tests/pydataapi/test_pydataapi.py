import datetime
from decimal import Decimal
from typing import Any, Dict

import pytest
from pydantic import BaseModel, ValidationError
from sqlalchemy import Column, Integer, String
from sqlalchemy import types as types
from sqlalchemy.ext.declarative import declarative_base

from pydataapi.exceptions import DataAPIError, MultipleResultsFound, NoResultFound
from pydataapi.pydataapi import (
    DataAPI,
    GeneratedFields,
    Record,
    Result,
    UpdateResults,
    _get_value_from_row,
    convert_array_value,
    create_sql_parameter,
    create_sql_parameters,
    transaction,
)


class MyType(types.TypeDecorator):
    impl = types.Unicode

    def process_result_value(self, value, dialect):
        return f'my_type_{value}'


class Pets(declarative_base()):
    __tablename__ = 'pets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(MyType(255, collation='utf8_unicode_ci'), default=None)


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


@pytest.mark.parametrize(
    'input_value, expected',
    [
        ('str', {'stringValue': 'str'}),
        (123, {'longValue': 123}),
        (1.23, {'doubleValue': 1.23}),
        (True, {'booleanValue': True}),
        (False, {'booleanValue': False}),
        (b'bytes', {'blobValue': b'bytes'}),
        (None, {'isNull': True}),
        ([], {'isNull': True}),
        ('str', {'stringValue': 'str'}),
        ([123, 456], {'arrayValue': {'longValues': [123, 456]}}),
        ([1.23, 4.56], {'arrayValue': {'doubleValues': [1.23, 4.56]}}),
        ([True, False], {'arrayValue': {'booleanValues': [True, False]}}),
        ([b'bytes', b'blob'], {'arrayValue': {'blobValues': [b'bytes', b'blob']}}),
    ],
)
def test_create_sql_parameter(input_value: Any, expected: Dict[str, Any]) -> None:
    assert create_sql_parameter('', input_value)['value'] == expected


def test_convert_value_other_types() -> None:
    class Dummy:
        def __str__(self):
            return 'Dummy'

    assert create_sql_parameter('', Dummy())['value'] == {'stringValue': 'Dummy'}

    assert create_sql_parameter('decimal', Decimal(123456789)) == {
        'name': 'decimal',
        'typeHint': 'DECIMAL',
        'value': {'stringValue': '123456789'},
    }

    assert create_sql_parameter(
        'datetime', datetime.datetime(2020, 1, 2, 3, 4, 5, 678912)
    ) == {
        'name': 'datetime',
        'typeHint': 'TIMESTAMP',
        'value': {'stringValue': '2020-01-02 03:04:05.678912'},
    }

    assert create_sql_parameter('date', datetime.date(2020, 1, 2)) == {
        'name': 'date',
        'typeHint': 'DATE',
        'value': {'stringValue': '2020-01-02'},
    }

    assert create_sql_parameter('time', datetime.time(3, 4, 5, 678912)) == {
        'name': 'time',
        'typeHint': 'TIME',
        'value': {'stringValue': '03:04:05.678912'},
    }


@pytest.mark.parametrize(
    'input_value, expected',
    [
        (['str', 'string'], {'arrayValue': {'stringValues': ['str', 'string']}}),
        ([123, 456], {'arrayValue': {'longValues': [123, 456]}}),
        ([1.23, 4.56], {'arrayValue': {'doubleValues': [1.23, 4.56]}}),
        ([True, False], {'arrayValue': {'booleanValues': [True, False]}}),
        ([b'bytes', b'blob'], {'arrayValue': {'blobValues': [b'bytes', b'blob']}}),
        (
            [[123, 456], [789]],
            {
                'arrayValue': {
                    'arrayValues': [
                        {'arrayValue': {'longValues': [123, 456]}},
                        {'arrayValue': {'longValues': [789]}},
                    ]
                }
            },
        ),
    ],
)
def test_convert_array_value(input_value: Any, expected: Dict[str, Any]) -> None:
    assert convert_array_value(input_value) == expected


def test_convert_arrary_value_fail() -> None:
    class Dummy:
        pass

    with pytest.raises(Exception):
        convert_array_value([Dummy()])


@pytest.mark.parametrize(
    'input_value, expected',
    [
        ({'arrayValue': {'stringValues': ['str', 'string']}}, ['str', 'string']),
        ({'longValue': 123}, 123),
        (
            {
                'arrayValue': {
                    'arrayValues': [
                        {'arrayValue': {'longValues': [123, 456]}},
                        {'arrayValue': {'longValues': [789]}},
                    ]
                }
            },
            [[123, 456], [789]],
        ),
    ],
)
def test_get_value_from_row(input_value: Dict[str, Any], expected: Any) -> None:
    assert _get_value_from_row(input_value) == expected


def test_create_parameters() -> None:
    expected = [
        {'name': 'int', 'value': {'longValue': 1}},
        {'name': 'float', 'value': {'doubleValue': 1.2}},
        {'name': 'str', 'value': {'stringValue': 'str'}},
        {'name': 'bytes', 'value': {'blobValue': b'bytes'}},
        {'name': 'bool', 'value': {'booleanValue': True}},
        {'name': 'None', 'value': {'isNull': True}},
    ]

    assert (
        create_sql_parameters(
            {
                'int': 1,
                'float': 1.2,
                'str': 'str',
                'bytes': b'bytes',
                'bool': True,
                'None': None,
            }
        )
        == expected
    )


def test_record() -> None:
    record = Record([1, 'dog'], ['id', 'name'])
    assert str(record) == '<Record(id=1, name=dog)>'
    assert record.headers == ['id', 'name']
    assert next(record) == 1
    assert next(record) == 'dog'
    with pytest.raises(StopIteration):
        next(record)

    assert record.dict() == {'id': 1, 'name': 'dog'}

    class Pet(BaseModel):
        id: int
        name: str

    assert record.model(Pet) == Pet(id=1, name='dog')

    assert record == Record([1, 'dog'], ['id', 'name'])
    assert record == [1, 'dog']
    assert record == (1, 'dog')
    assert record != Record([2, 'cat'], ['id', 'name'])
    assert record != []
    assert record != tuple()
    assert record != ''


def test_result() -> None:
    column_metadata = [
        {
            "arrayBaseColumnType": 0,
            "isAutoIncrement": False,
            "isCaseSensitive": False,
            "isCurrency": False,
            "isSigned": True,
            "label": "id",
            "name": "id",
            "nullable": 1,
            "precision": 11,
            "scale": 0,
            "schemaName": "",
            "tableName": "pets",
            "type": 4,
            "typeName": "INT",
        },
        {
            "arrayBaseColumnType": 0,
            "isAutoIncrement": False,
            "isCaseSensitive": False,
            "isCurrency": False,
            "isSigned": False,
            "label": "name",
            "name": "name",
            "nullable": 1,
            "precision": 255,
            "scale": 0,
            "schemaName": "",
            "tableName": "pets",
            "type": 12,
            "typeName": "VARCHAR",
        },
    ]
    result = Result(
        {
            'numberOfRecordsUpdated': 0,
            'records': [
                [{'longValue': 1}, {'stringValue': 'dog'}],
                [{'longValue': 2}, {'stringValue': 'cat'}],
                [{'longValue': 3}, {'isNull': True}],
            ],
            "columnMetadata": column_metadata,
        }
    )

    assert result[0] == [1, 'dog']
    assert result[1] == [2, 'cat']
    assert result[2] == [3, None]
    dog, cat, none = result[0:3]
    assert dog == [1, 'dog']
    assert cat == [2, 'cat']
    assert none == [3, None]
    assert next(result) == Record([1, 'dog'], ['id', 'name'])
    assert next(result) == Record([2, 'cat'], ['id', 'name'])
    assert next(result) == Record([3, None], ['id', 'name'])
    with pytest.raises(StopIteration):
        next(result)

    assert result.all() == [
        Record([1, 'dog'], ['id', 'name']),
        Record([2, 'cat'], ['id', 'name']),
        Record([3, None], ['id', 'name']),
    ]
    assert result.first() == Record([1, 'dog'], ['id', 'name'])
    with pytest.raises(MultipleResultsFound):
        result.one()

    with pytest.raises(MultipleResultsFound):
        result.one_or_none()

    result_one = Result(
        {
            'numberOfRecordsUpdated': 0,
            'records': [[{'longValue': 1}, {'stringValue': 'dog'}]],
            "columnMetadata": column_metadata,
        }
    )
    assert result_one.one() == Record([1, 'dog'], ['id', 'name'])
    assert result_one.one_or_none() == Record([1, 'dog'], ['id', 'name'])
    assert result_one.scalar() == 1

    result_empty = Result(
        {'numberOfRecordsUpdated': 0, 'records': [], "columnMetadata": column_metadata}
    )
    with pytest.raises(NoResultFound):
        result_empty.one()
    assert result_empty.one_or_none() is None
    assert result_empty.first() is None


def test_generated_fields_first() -> None:
    assert GeneratedFields([{'1': 1}, {'2': 2}, {'3': 3}]).generated_fields_first == 1
    assert (
        GeneratedFields([{'1': 1.1}, {'2': 2}, {'3': 3}]).generated_fields_first == 1.1
    )
    assert (
        GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]).generated_fields_first
        == 'abc'
    )


def test_generated_fields() -> None:
    assert GeneratedFields([{'1': 1}, {'2': 2}, {'3': 3}]).generated_fields == [1, 2, 3]
    assert GeneratedFields([{'1': 1.1}, {'2': 2}, {'3': 3}]).generated_fields == [
        1.1,
        2,
        3,
    ]
    assert GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]).generated_fields == [
        'abc',
        2,
        3,
    ]

    assert GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]) == GeneratedFields(
        [{'1': 'abc'}, {'2': 2}, {'3': 3}]
    )
    assert GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]) == ['abc', 2, 3]
    assert GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]) == ('abc', 2, 3)
    assert GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]) != 'bar'
    assert (
        str(GeneratedFields([{'1': 'abc'}, {'2': 2}, {'3': 3}]))
        == '<GeneratedFields(abc, 2, 3)>'
    )


def test_generated_fields_empty() -> None:
    assert GeneratedFields([]).generated_fields == []


def test_generated_fields_first_empty() -> None:
    assert GeneratedFields([]).generated_fields_first is None


def test_update_results() -> None:
    update_results = UpdateResults(
        [
            {'generatedFields': [{'1': 1}, {'2': 2}, {'3': 3}]},
            {'generatedFields': [{'4': 4}, {'5': 5}, {'6': 6}]},
            {'generatedFields': [{'7': 7}, {'8': 8}, {'9': 9}]},
        ]
    )
    assert update_results[0].generated_fields == [1, 2, 3]
    assert update_results[0].generated_fields_first == 1
    assert update_results[1].generated_fields == [4, 5, 6]
    assert update_results[1].generated_fields_first == 4
    assert update_results[2].generated_fields == [7, 8, 9]
    assert update_results[2].generated_fields_first == 7
    assert update_results[0:1] == [GeneratedFields([{'1': 1}, {'2': 2}, {'3': 3}])]

    empty = UpdateResults([{'generatedFields': []}])
    assert len(empty) == 1
    assert empty[0].generated_fields == []
    assert empty[0].generated_fields_first is None


def test_client(mocker) -> None:
    mock_client = mocker.Mock()
    data_api: DataAPI = DataAPI(
        resource_arn='arn:aws:rds:dummy', secret_arn='dummy', client=mock_client
    )
    assert data_api.client == mock_client


def test_resource_arn(mocker, mocked_client) -> None:
    mock_client = mocker.Mock()
    mock_client.describe_db_clusters.return_value = {
        'DBClusters': [{'DBClusterArn': 'arn:aws:rds:dummy'}]
    }
    data_api: DataAPI = DataAPI(
        resource_name='dummy',
        secret_arn='dummy',
        client=mock_client,
        rds_client=mock_client,
    )
    assert data_api.resource_arn == 'arn:aws:rds:dummy'

    mocked_client.return_value = mock_client

    data_api: DataAPI = DataAPI(
        resource_name='dummy', secret_arn='dummy', client=mock_client
    )
    assert data_api.resource_arn == 'arn:aws:rds:dummy'


def test_not_found_resource_arn_and_resource_arn(mocker, mocked_client) -> None:
    mock_client = mocker.Mock()
    mock_client.describe_db_clusters.return_value = {
        'DBClusters': [{'DBClusterArn': 'arn:aws:rds:dummy'}]
    }
    with pytest.raises(DataAPIError, match='Not Found resource_arn.'):
        DataAPI(secret_arn='dummy', client=mock_client, rds_client=mock_client)


def test_found_resource_arn_and_resource_arn(mocker, mocked_client) -> None:
    mock_client = mocker.Mock()
    mock_client.describe_db_clusters.return_value = {
        'DBClusters': [{'DBClusterArn': 'arn:aws:rds:dummy'}]
    }
    with pytest.raises(
        DataAPIError,
        match='resource_name should be set without resource_arn. resource_arn: arn:aws:rds:dummy, resource_name: dummy',
    ):
        DataAPI(
            resource_arn='arn:aws:rds:dummy',
            resource_name='dummy',
            secret_arn='dummy',
            client=mock_client,
            rds_client=mock_client,
        )


def test_with_statement(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    ):
        mocked_client.begin_transaction.assert_called_once_with(
            database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
        )
    mocked_client.commit_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_exception(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with DataAPI(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise Exception('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_custom_exception(mocked_client, mocker) -> None:
    class CustomError(Exception):
        pass

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(CustomError):
        with DataAPI(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
            rollback_exception=CustomError,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise CustomError('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )

    second_mocked_client = mocker.patch('boto3.client')
    second_mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with DataAPI(
            resource_arn='arn:aws:rds:dummy',
            secret_arn='dummy',
            database='test',
            client=second_mocked_client,
            rollback_exception=CustomError,
        ):
            second_mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
            )
            raise Exception('error')
    second_mocked_client.rollback_transaction.assert_not_called()


def test_begin(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    assert data_api.begin(schema='schema') == 'abc'
    assert mocked_client.begin_transaction.call_args == mocker.call(
        database='test',
        resourceArn='arn:aws:rds:dummy',
        schema='schema',
        secretArn='dummy',
    )


def test_transaction(mocked_client) -> None:
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        transaction_id='abc',
        client=mocked_client,
    )
    assert data_api.transaction_id == 'abc'


def test_transaction_status(mocked_client) -> None:
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        transaction_id='abc',
        client=mocked_client,
    )
    data_api._transaction_status = 'dummy status'
    assert data_api.transaction_status == 'dummy status'


def test_commit(mocked_client, mocker) -> None:
    mocked_client.commit_transaction.return_value = {'transactionStatus': 'abc'}
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    assert data_api.commit(transaction_id='abc') == 'abc'
    assert mocked_client.commit_transaction.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy', transactionId='abc', secretArn='dummy'
    )


def test_rollback(mocked_client, mocker) -> None:
    mocked_client.rollback_transaction.return_value = {'transactionStatus': 'abc'}
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    assert data_api.rollback(transaction_id='abc') == 'abc'
    assert mocked_client.rollback_transaction.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy', transactionId='abc', secretArn='dummy'
    )


def test_execute_insert(mocked_client, mocker) -> None:
    mocked_client.execute_statement.return_value = {
        'generatedFields': [],
        'numberOfRecordsUpdated': 1,
    }
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.execute(
        "insert into pets values(1, 'cat')", transaction_id='abc'
    )
    assert results.generated_fields == []
    assert results.number_of_records_updated == 1
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        transactionId='abc',
        sql="insert into pets values(1, 'cat')",
        database='test',
    )


def test_execute_insert_parameters(mocked_client, mocker) -> None:
    mocked_client.execute_statement.return_value = {
        'generatedFields': [],
        'numberOfRecordsUpdated': 1,
    }
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.execute(
        "insert into pets values(:id, :name)",
        {'id': 1, 'name': 'cat'},
        transaction_id='abc',
    )
    assert results.generated_fields == []
    assert results.number_of_records_updated == 1
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        includeResultMetadata=True,
        parameters=[
            {'name': 'id', 'value': {'longValue': 1}},
            {'name': 'name', 'value': {'stringValue': 'cat'}},
        ],
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        transactionId='abc',
        sql="insert into pets values(:id, :name)",
        database='test',
    )


def test_execute_select(mocked_client, mocker) -> None:
    mocked_client.execute_statement.return_value = {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
    }
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    assert list(data_api.execute("select * from pets")[0]) == [1, 'cat']
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql='select * from pets',
    )


def test_execute_select_as_model(mocked_client, mocker) -> None:
    mocked_client.execute_statement.return_value = {
        "columnMetadata": [
            {
                "arrayBaseColumnType": 0,
                "isAutoIncrement": False,
                "isCaseSensitive": False,
                "isCurrency": False,
                "isSigned": True,
                "label": "id",
                "name": "id",
                "nullable": 1,
                "precision": 11,
                "scale": 0,
                "schemaName": "",
                "tableName": "users",
                "type": 4,
                "typeName": "INT",
            },
            {
                "arrayBaseColumnType": 0,
                "isAutoIncrement": False,
                "isCaseSensitive": False,
                "isCurrency": False,
                "isSigned": False,
                "label": "name",
                "name": "name",
                "nullable": 1,
                "precision": 255,
                "scale": 0,
                "schemaName": "",
                "tableName": "users",
                "type": 12,
                "typeName": "VARCHAR",
            },
        ],
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
    }
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )

    class Pet(BaseModel):
        id: int
        name: str

    result = data_api.execute("select * from pets")
    assert len(result) == 1
    assert result[0].model(Pet) == Pet(name='cat', id=1)

    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql='select * from pets',
    )


def test_execute_insert_parameter_set(mocked_client, mocker) -> None:
    mocked_client.batch_execute_statement.return_value = {
        'updateResults': [
            {'generatedFields': [{'longValue': 3}]},
            {'generatedFields': [{'longValue': 4}]},
        ]
    }

    mocked_client.begin_transaction.return_value = {'transactionId': '12345'}

    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    results = data_api.batch_execute(
        "insert into test.pets  values (:id , :name)",
        [{'id': 3, 'name': 'bird'}, {'id': 4, 'name': 'lion'}],
    )
    assert len(results) == 2
    assert results[0].generated_fields == [3]
    assert results[0].generated_fields_first == 3
    assert results[1].generated_fields == [4]
    assert results[1].generated_fields_first == 4

    assert mocked_client.batch_execute_statement.call_args == mocker.call(
        resourceArn='arn:aws:rds:dummy',
        secretArn='dummy',
        sql="insert into test.pets  values (:id , :name)",
        parameterSets=[
            [
                {'name': 'id', 'value': {'longValue': 3}},
                {'name': 'name', 'value': {'stringValue': 'bird'}},
            ],
            [
                {'name': 'id', 'value': {'longValue': 4}},
                {'name': 'name', 'value': {'stringValue': 'lion'}},
            ],
        ],
        database='test',
        transactionId='12345',
    )


def test_execute_insert_parameter_set_invalid_1(mocked_client, mocker) -> None:
    mocked_client.batch_execute_statement.side_effect = Exception('Invalid Request')

    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
        transaction_id='12345',
    )

    with pytest.raises(Exception):
        data_api.batch_execute(
            "insert into test.pets  values (:id , :name)",
            [{'id': 3, 'invalid': 'bird'}],
        )


def test_execute_insert_parameter_set_invalid_2(mocked_client, mocker) -> None:
    data_api = DataAPI(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )

    with pytest.raises(ValidationError):
        data_api.batch_execute(
            "insert into test.pets  values (:id , :name)", {'id': 3, 'name': 'bird'}
        )


def test_transaction_add_user(mocked_client):
    @transaction(
        resource_arn='arn:aws:rds:dummy',
        secret_arn='dummy',
        database='test',
        client=mocked_client,
    )
    def add_user(data_api: DataAPI, id_, name):
        data_api.execute(f"insert into pets values({id_}, {name})")

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    add_user(1, 'cat')
    mocked_client.begin_transaction.assert_called_once_with(
        database='test', resourceArn='arn:aws:rds:dummy', secretArn='dummy'
    )
    mocked_client.commit_transaction.assert_called_once_with(
        resourceArn='arn:aws:rds:dummy', secretArn='dummy', transactionId='abc'
    )

import pytest
from pydantic import BaseModel, ValidationError
from pydataapi.exceptions import MultipleResultsFound, NoResultFound
from pydataapi.pydataapi import (
    DataAPI,
    GeneratedFields,
    Record,
    Result,
    UpdateResults,
    convert_value,
    create_sql_parameters,
    generate_sql,
    transaction,
)
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


def test_convert_value() -> None:
    assert convert_value('str') == {'stringValue': 'str'}
    assert convert_value(123), {'longValue': 123}
    assert convert_value(1.23), {'doubleValue': 1.23}
    assert convert_value(True), {'booleanValue': True}
    assert convert_value(False), {'booleanValue': False}
    assert convert_value(b'bytes'), {'blobValue': b'bytes'}
    assert convert_value(None), {'isNull': True}

    class Dummy:
        pass

    with pytest.raises(Exception):
        convert_value(Dummy())


def test_generate_sql() -> None:
    class Users(declarative_base()):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(255, collation='utf8_unicode_ci'), default=None)

    insert: Insert = Insert(Users, {'name': 'ken'})
    assert generate_sql(insert) == "INSERT INTO users (name) VALUES ('ken')"

    assert (
        generate_sql(Query(Users).filter(Users.id == 1))
        == "SELECT users.id, users.name \n"
        "FROM users \n"
        "WHERE users.id = 1"
    )


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
            ],
            "columnMetadata": column_metadata,
        }
    )

    assert result[0] == [1, 'dog']
    assert result[1] == [2, 'cat']
    dog, cat = result[0:2]
    assert dog == [1, 'dog']
    assert cat == [2, 'cat']
    assert next(result) == Record([1, 'dog'], ['id', 'name'])
    assert next(result) == Record([2, 'cat'], ['id', 'name'])
    with pytest.raises(StopIteration):
        next(result)

    assert result.all() == [
        Record([1, 'dog'], ['id', 'name']),
        Record([2, 'cat'], ['id', 'name']),
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
        resource_arn='dummy', secret_arn='dummy', client=mock_client
    )
    assert data_api.client == mock_client


def test_with_statement(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    ):
        mocked_client.begin_transaction.assert_called_once_with(
            database='test', resourceArn='dummy', secretArn='dummy'
        )
    mocked_client.commit_transaction.assert_called_once_with(
        resourceArn='dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_exception(mocked_client) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with DataAPI(
            resource_arn='dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='dummy', secretArn='dummy'
            )
            raise Exception('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='dummy', secretArn='dummy', transactionId='abc'
    )


def test_with_statement_custom_exception(mocked_client, mocker) -> None:
    class CustomError(Exception):
        pass

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(CustomError):
        with DataAPI(
            resource_arn='dummy',
            secret_arn='dummy',
            database='test',
            client=mocked_client,
            rollback_exception=CustomError,
        ):
            mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='dummy', secretArn='dummy'
            )
            raise CustomError('error')
    mocked_client.rollback_transaction.assert_called_once_with(
        resourceArn='dummy', secretArn='dummy', transactionId='abc'
    )

    second_mocked_client = mocker.patch('boto3.client')
    second_mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    with pytest.raises(Exception):
        with DataAPI(
            resource_arn='dummy',
            secret_arn='dummy',
            database='test',
            client=second_mocked_client,
            rollback_exception=CustomError,
        ):
            second_mocked_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='dummy', secretArn='dummy'
            )
            raise Exception('error')
    second_mocked_client.rollback_transaction.assert_not_called()


def test_begin(mocked_client, mocker) -> None:
    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    assert data_api.begin(schema='schema') == 'abc'
    assert mocked_client.begin_transaction.call_args == mocker.call(
        database='test', resourceArn='dummy', schema='schema', secretArn='dummy'
    )


def test_transaction(mocked_client) -> None:
    data_api = DataAPI(
        resource_arn='dummy',
        secret_arn='dummy',
        transaction_id='abc',
        client=mocked_client,
    )
    assert data_api.transaction_id == 'abc'


def test_transaction_status(mocked_client) -> None:
    data_api = DataAPI(
        resource_arn='dummy',
        secret_arn='dummy',
        transaction_id='abc',
        client=mocked_client,
    )
    data_api._transaction_status = 'dummy status'
    assert data_api.transaction_status == 'dummy status'


def test_commit(mocked_client, mocker) -> None:
    mocked_client.commit_transaction.return_value = {'transactionStatus': 'abc'}
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    assert data_api.commit(transaction_id='abc') == 'abc'
    assert mocked_client.commit_transaction.call_args == mocker.call(
        resourceArn='dummy', transactionId='abc', secretArn='dummy'
    )


def test_rollback(mocked_client, mocker) -> None:
    mocked_client.rollback_transaction.return_value = {'transactionStatus': 'abc'}
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    assert data_api.rollback(transaction_id='abc') == 'abc'
    assert mocked_client.rollback_transaction.call_args == mocker.call(
        resourceArn='dummy', transactionId='abc', secretArn='dummy'
    )


def test_execute_insert(mocked_client, mocker) -> None:
    mocked_client.execute_statement.return_value = {
        'generatedFields': [],
        'numberOfRecordsUpdated': 1,
    }
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    results = data_api.execute(
        "insert into pets values(1, 'cat')", transaction_id='abc'
    )
    assert results.generated_fields == []
    assert results.number_of_records_updated == 1
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        includeResultMetadata=True,
        resourceArn='dummy',
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
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
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
        resourceArn='dummy',
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
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    assert list(data_api.execute("select * from pets")[0]) == [1, 'cat']
    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='dummy',
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
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
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
        resourceArn='dummy',
        secretArn='dummy',
        sql='select * from pets',
    )


def test_execute_select_query(mocked_client, mocker) -> None:
    class Users(declarative_base()):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(255, collation='utf8_unicode_ci'), default=None)

    mocked_client.execute_statement.return_value = {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
    }
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    result = data_api.execute(Query(Users).filter(Users.id == 1))
    assert len(result) == 1
    assert list(result[0]) == [1, 'cat']

    assert mocked_client.execute_statement.call_args == mocker.call(
        continueAfterTimeout=True,
        database='test',
        includeResultMetadata=True,
        resourceArn='dummy',
        secretArn='dummy',
        sql='SELECT users.id, users.name \nFROM users \nWHERE users.id = 1',
    )


def test_execute_insert_parameter_set(mocked_client, mocker) -> None:
    mocked_client.batch_execute_statement.return_value = {
        'updateResults': [
            {'generatedFields': [{'longValue': 3}]},
            {'generatedFields': [{'longValue': 4}]},
        ]
    }

    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
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
        resourceArn='dummy',
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
    )


def test_execute_insert_parameter_set_invalid(mocked_client, mocker) -> None:
    data_api = DataAPI(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )

    with pytest.raises(ValidationError):
        data_api.batch_execute(
            "insert into test.pets  values (:id , :name)", {'id': 3, 'name': 'bird'}
        )


def test_transaction_add_user(mocked_client):
    @transaction(
        resource_arn='dummy', secret_arn='dummy', database='test', client=mocked_client
    )
    def add_user(data_api: DataAPI, id_, name):
        data_api.execute(f"insert into pets values({id_}, {name})")

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    add_user(1, 'cat')
    mocked_client.begin_transaction.assert_called_once_with(
        database='test', resourceArn='dummy', secretArn='dummy'
    )
    mocked_client.commit_transaction.assert_called_once_with(
        resourceArn='dummy', secretArn='dummy', transactionId='abc'
    )

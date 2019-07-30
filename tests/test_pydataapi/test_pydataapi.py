from unittest import TestCase
from unittest.mock import Mock, call, patch

from pydataapi.pydataapi import (
    DataAPI,
    Result,
    convert_value,
    create_sql_parameters,
    generate_sql,
    transaction,
)
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert


class TestDataAPIFunction(TestCase):
    def setUp(self) -> None:
        pass

    def test_convert_value(self) -> None:
        self.assertDictEqual(convert_value('str'), {'stringValue': 'str'})
        self.assertDictEqual(convert_value(123), {'longValue': 123})
        self.assertDictEqual(convert_value(1.23), {'doubleValue': 1.23})
        self.assertDictEqual(convert_value(True), {'booleanValue': True})
        self.assertDictEqual(convert_value(False), {'booleanValue': False})
        self.assertDictEqual(convert_value(b'bytes'), {'blobValue': b'bytes'})
        self.assertDictEqual(convert_value(None), {'isNull': True})

        class Dummy:
            pass

        with self.assertRaises(Exception):
            convert_value(Dummy())

    def test_generate_sql(self) -> None:
        class Users(declarative_base()):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String(255, collation='utf8_unicode_ci'), default=None)

        insert: Insert = Insert(Users, {'name': 'ken'})
        self.assertEqual(
            generate_sql(insert), "INSERT INTO users (name) VALUES ('ken')"
        )

        self.assertEqual(
            generate_sql(Query(Users).filter(Users.id == 1)),
            "SELECT users.id, users.name \n" "FROM users \n" "WHERE users.id = 1",
        )

    def test_create_parameters(self) -> None:
        expected = [
            {'name': 'int', 'value': {'longValue': 1}},
            {'name': 'float', 'value': {'doubleValue': 1.2}},
            {'name': 'str', 'value': {'stringValue': 'str'}},
            {'name': 'bytes', 'value': {'blobValue': b'bytes'}},
            {'name': 'bool', 'value': {'booleanValue': True}},
            {'name': 'None', 'value': {'isNull': True}},
        ]

        self.assertListEqual(
            create_sql_parameters(
                {
                    'int': 1,
                    'float': 1.2,
                    'str': 'str',
                    'bytes': b'bytes',
                    'bool': True,
                    'None': None,
                }
            ),
            expected,
        )


class TestResult(TestCase):
    def test_generated_fields_first(self) -> None:
        self.assertEqual(Result(generated_fields=[1, 2, 3]).generated_fields_first, 1)
        self.assertEqual(
            Result(generated_fields=[1.1, 2, 3]).generated_fields_first, 1.1
        )
        self.assertEqual(
            Result(generated_fields=['abc', 2, 3]).generated_fields_first, 'abc'
        )

    def test_generated_fields_first_empty(self) -> None:
        self.assertEqual(Result(generated_fields=[]).generated_fields_first, None)


class TestDataAPI(TestCase):
    def setUp(self) -> None:
        pass

    def test_client(self) -> None:
        mock_client: Mock = Mock()
        data_api: DataAPI = DataAPI(
            resource_arn='dummy', secret_arn='dummy', client=mock_client
        )
        self.assertEqual(data_api.client, mock_client)

    def test_with_statement(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            with DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            ) as data_api:
                mock_client.begin_transaction.assert_called_once_with(
                    database='test', resourceArn='dummy', secretArn='dummy'
                )
            mock_client.commit_transaction.assert_called_once_with(
                resourceArn='dummy', secretArn='dummy', transactionId='abc'
            )

    def test_with_statement_exception(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            with self.assertRaises(Exception):
                with DataAPI(
                    resource_arn='dummy',
                    secret_arn='dummy',
                    database='test',
                    client=mock_client,
                ) as data_api:
                    mock_client.begin_transaction.assert_called_once_with(
                        database='test', resourceArn='dummy', secretArn='dummy'
                    )
                    raise Exception('error')
            mock_client.rollback_transaction.assert_called_once_with(
                resourceArn='dummy', secretArn='dummy', transactionId='abc'
            )

    def test_with_statement_custom_exception(self) -> None:
        class CustomError(Exception):
            pass

        with patch('boto3.client') as mock_client:
            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            with self.assertRaises(CustomError):
                with DataAPI(
                    resource_arn='dummy',
                    secret_arn='dummy',
                    database='test',
                    client=mock_client,
                    rollback_exception=CustomError,
                ):
                    mock_client.begin_transaction.assert_called_once_with(
                        database='test', resourceArn='dummy', secretArn='dummy'
                    )
                    raise CustomError('error')
            mock_client.rollback_transaction.assert_called_once_with(
                resourceArn='dummy', secretArn='dummy', transactionId='abc'
            )

        with patch('boto3.client') as mock_client:
            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            with self.assertRaises(Exception):
                with DataAPI(
                    resource_arn='dummy',
                    secret_arn='dummy',
                    database='test',
                    client=mock_client,
                    rollback_exception=CustomError,
                ):
                    mock_client.begin_transaction.assert_called_once_with(
                        database='test', resourceArn='dummy', secretArn='dummy'
                    )
                    raise Exception('error')
            mock_client.rollback_transaction.assert_not_called()

    def test_begin(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(data_api.begin(schema='schema'), 'abc')
            self.assertEqual(
                mock_client.begin_transaction.call_args,
                call(
                    database='test',
                    resourceArn='dummy',
                    schema='schema',
                    secretArn='dummy',
                ),
            )

    def test_transaction(self) -> None:
        with patch('boto3.client') as mock_client:
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                transaction_id='abc',
                client=mock_client,
            )
            self.assertEqual(data_api.transaction_id, 'abc')

    def test_transaction_status(self) -> None:
        with patch('boto3.client') as mock_client:
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                transaction_id='abc',
                client=mock_client,
            )
            data_api._transaction_status = 'dummy status'
            self.assertEqual(data_api.transaction_status, 'dummy status')

    def test_commit(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.commit_transaction.return_value = {'transactionStatus': 'abc'}
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(data_api.commit(transaction_id='abc'), 'abc')
            self.assertEqual(
                mock_client.commit_transaction.call_args,
                call(resourceArn='dummy', transactionId='abc', secretArn='dummy'),
            )

    def test_rollback(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.rollback_transaction.return_value = {'transactionStatus': 'abc'}
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(data_api.rollback(transaction_id='abc'), 'abc')
            self.assertEqual(
                mock_client.rollback_transaction.call_args,
                call(resourceArn='dummy', transactionId='abc', secretArn='dummy'),
            )

    def test_execute_insert(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.execute_statement.return_value = {
                'generatedFields': [],
                'numberOfRecordsUpdated': 1,
            }
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(
                data_api.execute(
                    "insert into pets values(1, 'cat')", transaction_id='abc'
                ),
                [Result(generated_fields=None, number_of_records_updated=1)],
            )
            self.assertEqual(
                mock_client.execute_statement.call_args,
                call(
                    continueAfterTimeout=True,
                    includeResultMetadata=False,
                    resourceArn='dummy',
                    secretArn='dummy',
                    transactionId='abc',
                    sql="insert into pets values(1, 'cat')",
                    database='test',
                ),
            )

    def test_execute_insert_parameters(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.execute_statement.return_value = {
                'generatedFields': [],
                'numberOfRecordsUpdated': 1,
            }
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(
                data_api.execute(
                    "insert into pets values(:id, :name)",
                    {'id': 1, 'name': 'cat'},
                    transaction_id='abc',
                ),
                [Result(generated_fields=None, number_of_records_updated=1)],
            )
            self.assertEqual(
                mock_client.execute_statement.call_args,
                call(
                    continueAfterTimeout=True,
                    includeResultMetadata=False,
                    parameters=[
                        {'name': 'id', 'value': {'longValue': 1}},
                        {'name': 'name', 'value': {'stringValue': 'cat'}},
                    ],
                    resourceArn='dummy',
                    secretArn='dummy',
                    transactionId='abc',
                    sql="insert into pets values(:id, :name)",
                    database='test',
                ),
            )

    def test_execute_select(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.execute_statement.return_value = {
                'numberOfRecordsUpdated': 0,
                'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
            }
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(data_api.execute("select * from pets"), [[1, 'cat']])
            self.assertEqual(
                mock_client.execute_statement.call_args,
                call(
                    continueAfterTimeout=True,
                    database='test',
                    includeResultMetadata=False,
                    resourceArn='dummy',
                    secretArn='dummy',
                    sql='select * from pets',
                ),
            )

    def test_execute_select_include_metadata(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.execute_statement.return_value = {
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
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(
                data_api.execute("select * from pets", with_columns=True),
                [{'id': 1, 'name': 'cat'}],
            )
            self.assertEqual(
                mock_client.execute_statement.call_args,
                call(
                    continueAfterTimeout=True,
                    database='test',
                    includeResultMetadata=True,
                    resourceArn='dummy',
                    secretArn='dummy',
                    sql='select * from pets',
                ),
            )

    def test_execute_select_query(self) -> None:
        class Users(declarative_base()):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String(255, collation='utf8_unicode_ci'), default=None)

        with patch('boto3.client') as mock_client:
            mock_client.execute_statement.return_value = {
                'numberOfRecordsUpdated': 0,
                'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
            }
            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(
                data_api.execute(Query(Users).filter(Users.id == 1)), [[1, 'cat']]
            )

            self.assertEqual(
                mock_client.execute_statement.call_args,
                call(
                    continueAfterTimeout=True,
                    database='test',
                    includeResultMetadata=False,
                    resourceArn='dummy',
                    secretArn='dummy',
                    sql='SELECT users.id, users.name \nFROM users \nWHERE users.id = 1',
                ),
            )

    def test_execute_insert_parameter_set(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.batch_execute_statement.return_value = {
                'updateResults': [
                    {'generatedFields': [{'longValue': 3}]},
                    {'generatedFields': [{'longValue': 4}]},
                ]
            }

            data_api = DataAPI(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            self.assertEqual(
                data_api.execute(
                    "insert into test.pets  values (:id , :name)",
                    [{'id': 3, 'name': 'bird'}, {'id': 4, 'name': 'lion'}],
                ),
                [
                    Result(generated_fields=[3], number_of_records_updated=None),
                    Result(generated_fields=[4], number_of_records_updated=None),
                ],
            )
            self.assertEqual(
                mock_client.batch_execute_statement.call_args,
                call(
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
                ),
            )


class TestTransaction(TestCase):
    def test_transaction(self):
        with patch('boto3.client') as mock_client:

            @transaction(
                resource_arn='dummy',
                secret_arn='dummy',
                database='test',
                client=mock_client,
            )
            def add_user(data_api: DataAPI, id_, name):
                data_api.execute(f"insert into pets values({id_}, {name})")

            mock_client.begin_transaction.return_value = {'transactionId': 'abc'}
            add_user(1, 'cat')
            mock_client.begin_transaction.assert_called_once_with(
                database='test', resourceArn='dummy', secretArn='dummy'
            )
            mock_client.commit_transaction.assert_called_once_with(
                resourceArn='dummy', secretArn='dummy', transactionId='abc'
            )

from unittest import TestCase
from unittest.mock import Mock, patch, call

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert

from pydataapi.pydataapi import convert_value, generate_sql, create_sql_parameters, DataAPI, Result


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
        self.assertEqual(generate_sql(insert), "INSERT INTO users (name) VALUES ('ken')")

        self.assertEqual(generate_sql(Query(Users).filter(Users.id == 1)),
                         "SELECT users.id, users.name \n"
                         "FROM users \n"
                         "WHERE users.id = 1")

    def test_create_parameters(self) -> None:
        expected = [{'name': 'int', 'value': {'longValue': 1}}, {'name': 'float', 'value': {'doubleValue': 1.2}},
                    {'name': 'str', 'value': {'stringValue': 'str'}}, {'name': 'bytes',
                                                                       'value': {'blobValue': b'bytes'}},
                    {'name': 'bool', 'value': {'booleanValue': True}}, {'name': 'None', 'value': {'isNull': True}}]

        self.assertListEqual(create_sql_parameters({'int': 1, 'float': 1.2, 'str': 'str', 'bytes': b'bytes',
                                                    'bool': True, 'None': None}), expected)


class TestResult(TestCase):
    def test_generated_fields_first(self) -> None:
        self.assertEqual(Result(generated_fields=[1, 2, 3]).generated_fields_first, 1)


class TestDataAPI(TestCase):
    def setUp(self) -> None:
        pass

    def test_client(self) -> None:
        mock_client: Mock = Mock()
        data_api: DataAPI = DataAPI(resource_arn='dummy', secret_arn='dummy', client=mock_client)
        self.assertEqual(data_api.client, mock_client)

    def test_begin(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.begin_transaction.return_value = {'transactionId': 'abc'}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test', )
            self.assertEqual(data_api.begin(schema='schema'), 'abc')
            self.assertEqual(mock_client.return_value.begin_transaction.call_args,
                             call(database='test', resourceArn='dummy', schema='schema', secretArn='dummy'))

    def test_transaction(self) -> None:
        data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', transaction_id='abc')
        self.assertEqual(data_api.transaction_id, 'abc')

    def test_commit(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.commit_transaction.return_value = {'transactionStatus': 'abc'}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test', )
            self.assertEqual(data_api.commit(transaction_id='abc'), 'abc')
            self.assertEqual(mock_client.return_value.commit_transaction.call_args,
                             call(resourceArn='dummy', transactionId='abc', secretArn='dummy'))

    def test_rollback(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.rollback_transaction.return_value = {'transactionStatus': 'abc'}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test', )
            self.assertEqual(data_api.rollback(transaction_id='abc'), 'abc')
            self.assertEqual(mock_client.return_value.rollback_transaction.call_args,
                             call(resourceArn='dummy', transactionId='abc', secretArn='dummy'))

    def test_execute_insert(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.execute_statement.return_value = {'generatedFields': [],
                                                                       'numberOfRecordsUpdated': 1}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test')
            self.assertEqual(data_api.execute("insert into pets values(1, 'cat')", transaction_id='abc'),
                             [Result(generated_fields=None, number_of_records_updated=1)])
            self.assertEqual(mock_client.return_value.execute_statement.call_args,
                             call(continueAfterTimeout=True, includeResultMetadata=False, resourceArn='dummy',
                                  secretArn='dummy', transactionId='abc',
                                  sql="insert into pets values(1, 'cat')", database='test'))

    def test_execute_insert_parameters(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.execute_statement.return_value = {'generatedFields': [],
                                                                       'numberOfRecordsUpdated': 1}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test')
            self.assertEqual(data_api.execute("insert into pets values(:id, :name)", {'id': 1, 'name': 'cat'},
                                              transaction_id='abc'),
                             [Result(generated_fields=None, number_of_records_updated=1)])
            self.assertEqual(mock_client.return_value.execute_statement.call_args,
                             call(continueAfterTimeout=True, includeResultMetadata=False,
                                  parameters=[{'name': 'id', 'value': {'longValue': 1}},
                                              {'name': 'name', 'value': {'stringValue': 'cat'}}],
                                  resourceArn='dummy', secretArn='dummy', transactionId='abc',
                                  sql="insert into pets values(:id, :name)", database='test'))

    def test_execute_select(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.execute_statement.return_value = {'numberOfRecordsUpdated': 0,
                                                                       'records': [
                                                                           [{'longValue': 1}, {'stringValue': 'cat'}]]}
            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test')
            self.assertEqual(data_api.execute("select * from pets"),
                             [[1, 'cat']])
            self.assertEqual(mock_client.return_value.execute_statement.call_args,
                             call(continueAfterTimeout=True, database='test', includeResultMetadata=False,
                                  resourceArn='dummy', secretArn='dummy',
                                  sql='select * from pets'))

    def test_execute_insert_parameter_set(self) -> None:
        with patch('boto3.client') as mock_client:
            mock_client.return_value.batch_execute_statement.return_value = {
                'updateResults': [{'generatedFields': [{'longValue': 3}]}, {'generatedFields': [{'longValue': 4}]}]}

            data_api = DataAPI(resource_arn='dummy', secret_arn='dummy', database='test')
            self.assertEqual(data_api.execute("insert into test.pets  values (:id , :name)",
                                              [{'id': 3, 'name': 'bird'}, {'id': 4, 'name': 'lion'}]
                                              ),
                             [Result(generated_fields=[3], number_of_records_updated=None),
                              Result(generated_fields=[4], number_of_records_updated=None)])
            self.assertEqual(mock_client.return_value.batch_execute_statement.call_args,
                             call(resourceArn='dummy', secretArn='dummy',
                                  sql="insert into test.pets  values (:id , :name)",
                                  parameterSets=[[{'name': 'id', 'value': {'longValue': 3}},
                                                  {'name': 'name', 'value': {'stringValue': 'bird'}}],
                                                 [{'name': 'id', 'value': {'longValue': 4}},
                                                  {'name': 'name', 'value': {'stringValue': 'lion'}}]],
                                  database='test'))

from contextlib import AbstractContextManager

from typing import Optional, Dict, Any, Type, Union, List

import boto3

from sqlalchemy.dialects import mysql
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Insert, Update, Delete, Select


def generate_sql(query: Union[Query, Insert, Update, Delete, Select]) -> str:
    kwargs = {'dialect': mysql.dialect(), 'compile_kwargs': {"literal_binds": True}}
    if hasattr(query, 'statement'):
        sql: str = query.statement.compile(**kwargs)
    else:
        sql = query.compile(**kwargs)
    print(str(sql))
    return str(sql)


class DataApi(AbstractContextManager):
    def __init__(self, resource_arn: str, secret_arn: str, database: str,
                 transaction_id: Optional[str] = None, client: Optional[boto3.session.Session.client] = None,
                 rollback_exception: Optional[Type[Exception]] = None) -> None:
        self.resource_arn: str = resource_arn
        self.secret_arn = secret_arn
        self.database: str = database

        self._transaction_id = transaction_id
        self._client = client or boto3.client('rds-data')
        self._transaction_status: Optional[str] = None
        self._rollback_exception: Optional[Type[Exception]] = rollback_exception

    def __enter__(self):
        self.begin_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit_transaction()
        else:
            if self._rollback_exception:
                if isinstance(exc_type, self._rollback_exception):
                    self.rollback_transaction()
            else:
                self.rollback_transaction()

    @property
    def client(self) -> boto3.session.Session.client:
        return self._client

    @property
    def transaction_id(self) -> Optional[str]:
        return self._transaction_id

    @property
    def transaction_status(self) -> Optional[str]:
        return self._transaction_status

    def begin_transaction(self, database: Optional[str] = None, resource_arn: Optional[str] = None,
                          schema: Optional[str] = None, secret_arn: Optional[str] = None) -> str:
        kwargs: Dict[str, Any] = {
          'resourceArn': resource_arn or self.resource_arn,
          'secretArn': secret_arn or self.secret_arn,
        }
        if database or self.database:
            kwargs['database'] = database or self.database
        if schema:
            kwargs['schema'] = schema

        response: Dict[str, str] = self.client.begin_transaction(**kwargs)
        self._transaction_id = response['transactionId']

        return self.transaction_id

    def commit_transaction(self, transaction_id: Optional[str] = None, resource_arn: Optional[str] = None,
                           secret_arn: Optional[str] = None):
        kwargs: Dict[str, Any] = {
          'resourceArn': resource_arn or self.resource_arn,
          'secretArn': secret_arn or self.secret_arn,
          'transactionId': transaction_id or self.transaction_id

        }

        response: Dict[str, str] = self.client.commit_transaction(**kwargs)
        self._transaction_status = response['transactionStatus']

    def rollback_transaction(self, transaction_id: Optional[str] = None, resource_arn: Optional[str] = None,
                             secret_arn: Optional[str] = None):
        kwargs: Dict[str, Any] = {
          'resourceArn': resource_arn or self.resource_arn,
          'secretArn': secret_arn or self.secret_arn,
          'transactionId': transaction_id or self.transaction_id

        }

        response: Dict[str, str] = self.client.rollback_transaction(**kwargs)
        self._transaction_status = response['transactionStatus']

    def execute(self, query: Union[Query, Insert, Update, str], with_columns: bool = False,
                transaction_id: Optional[str] = None, continue_after_timeout: bool = True,
                resource_arn: Optional[str] = None, secret_arn: Optional[str] = None,
                database: Optional[str] = None) -> Union[int, List[Union[List, Dict[str, Any]]]]:

        kwargs: Dict[str, Any] = {
          'resourceArn': resource_arn or self.resource_arn,
          'secretArn': secret_arn or self.secret_arn,
          'continueAfterTimeout': continue_after_timeout

        }

        if transaction_id or self.transaction_id:
            kwargs['transactionId'] = transaction_id or self.transaction_id

        if database or self.database:
            kwargs['database'] = database or self.database
        if not isinstance(query, str):
            sql = generate_sql(query)
        else:
            sql = query

        response: Dict[str, Any] = self._client.execute_statement(
                includeResultMetadata=with_columns,
                sql=sql,
                **kwargs
            )
        if 'records' not in response:
            return response['numberOfRecordsUpdated']

        if with_columns:
            headers = [meta['label'] for meta in response['columnMetadata']]

            def create_key_value_result(record) -> Dict:
                return {header: list(column.values())[0] for header, column in zip(headers, record)}

            result = [create_key_value_result(record) for record in response['records']]
        else:
            def create_value_result(record) -> List:
                return [list(column.values())[0] for column in record]

            result = [create_value_result(record) for record in response['records']]

        return result

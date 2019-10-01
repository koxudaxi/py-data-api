from abc import abstractmethod
from contextlib import AbstractContextManager
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import boto3
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Delete, Insert, Select, Update

Field = Dict[str, Any]
Row = Tuple[Any]
RowDict = Dict[str, Any]

DIALECT: Dialect = mysql.dialect(paramstyle='named')

QUERY_STATEMENT_COMPILE_PARAMS = {
    'dialect': mysql.dialect(paramstyle='named'),
    'compile_kwargs': {"literal_binds": True},
}


def generate_sql(query: Union[Query, Insert, Update, Delete, Select]) -> str:
    if hasattr(query, 'statement'):
        sql: str = query.statement.compile(**QUERY_STATEMENT_COMPILE_PARAMS)
    else:
        sql = query.compile(**QUERY_STATEMENT_COMPILE_PARAMS)
    return str(sql)


def convert_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {'booleanValue': value}
    elif isinstance(value, str):
        return {'stringValue': value}
    elif isinstance(value, int):
        return {'longValue': value}
    elif isinstance(value, float):
        return {'doubleValue': value}
    elif isinstance(value, bytes):
        return {'blobValue': value}
    elif value is None:
        return {'isNull': True}
    else:
        raise Exception(f'unsupported type {type(value)}: {value} ')


def create_sql_parameters(
    parameter: Dict[str, Any]
) -> List[Dict[str, Union[str, Dict]]]:
    return [
        {'name': key, 'value': convert_value(value)} for key, value in parameter.items()
    ]


T = TypeVar('T')


class Results(Sequence[Tuple]):
    def __getitem__(  # type: ignore
        self, i: Union[int, slice]
    ) -> Union[Tuple, Sequence[Any]]:
        return tuple(
            tuple(column.values())[0] for column in self._rows[i]  # type: ignore
        )

    def __len__(self) -> int:
        return len(self._rows)

    def __init__(self, response: Dict):
        self._response = response
        self._rows: Sequence[Tuple[Dict]] = response.get('records', [])
        self._column_metadata: List[Dict[str, Any]] = response.get('columnMetadata', [])
        self._headers: Optional[List[str]] = None
        self._generated_fields: Optional[List] = None
        self._generated_fields_list: Optional[List[List]] = None

    @property
    def number_of_records_updated(self) -> Optional[int]:
        return self._response.get('numberOfRecordsUpdated')

    @property
    def headers(self) -> List[str]:
        if self._headers is None:
            self._headers = [meta['label'] for meta in self._column_metadata]
        return self._headers

    def as_dict(self) -> List[Dict[str, Any]]:
        def create_key_value_result(record: Tuple[Dict]) -> Dict:
            return {
                header: list(column.values())[0]
                for header, column in zip(self.headers, record)
            }

        return [create_key_value_result(row) for row in self._rows]

    def as_model(self, model_type: Type[T]) -> List[T]:
        return [model_type(**row) for row in self.as_dict()]  # type: ignore

    @staticmethod
    def _get_generated_fields(result: Dict[str, List[Dict]]) -> List:
        return [list(f.values())[0] for f in result['generatedFields']]

    @property
    def generated_fields(self) -> Optional[List]:
        if 'generatedFields' in self._response:
            if self._generated_fields is None:
                self._generated_fields = self._get_generated_fields(self._response)
            return self._generated_fields
        return None

    @property
    def generated_fields_list(self) -> Optional[List[List]]:
        if 'updateResults' in self._response:
            if self._generated_fields is None:
                self._generated_fields = [
                    self._get_generated_fields(r)
                    for r in self._response['updateResults']
                    if 'generatedFields' in r
                ]
            return self._generated_fields
        return None

    @property
    def generated_fields_first(self) -> Union[str, int, float, None]:
        if self.generated_fields:
            return self.generated_fields[0]
        return None

    @property
    def generated_fields_first_list(self) -> Optional[List[Union[str, int, float]]]:
        if self.generated_fields_list:
            return [f[0] for f in self.generated_fields_list]
        return None


class DataAPI(AbstractContextManager):
    def __init__(
        self,
        resource_arn: str,
        secret_arn: str,
        database: Optional[str] = None,
        transaction_id: Optional[str] = None,
        client: Optional[boto3.session.Session.client] = None,
        rollback_exception: Optional[Type[Exception]] = None,
    ) -> None:
        self.resource_arn: str = resource_arn
        self.secret_arn: str = secret_arn
        self.database: Optional[str] = database

        self._transaction_id: Optional[str] = transaction_id
        self._client: boto3.session.Session.client = client or boto3.client('rds-data')
        self._transaction_status: Optional[str] = None
        self._rollback_exception: Optional[Type[Exception]] = rollback_exception

    def __enter__(self) -> 'DataAPI':
        self.begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            if self._rollback_exception:
                if issubclass(exc_type, self._rollback_exception):
                    self.rollback()
            else:
                self.rollback()

    @property
    def client(self) -> boto3.session.Session.client:
        return self._client

    @property
    def transaction_id(self) -> Optional[str]:
        return self._transaction_id

    @property
    def transaction_status(self) -> Optional[str]:
        return self._transaction_status

    def begin(
        self,
        database: Optional[str] = None,
        resource_arn: Optional[str] = None,
        schema: Optional[str] = None,
        secret_arn: Optional[str] = None,
    ) -> str:
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

        return response['transactionId']

    def commit(
        self,
        transaction_id: Optional[str] = None,
        resource_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
    ) -> str:
        kwargs: Dict[str, Any] = {
            'resourceArn': resource_arn or self.resource_arn,
            'secretArn': secret_arn or self.secret_arn,
            'transactionId': transaction_id or self.transaction_id,
        }

        response: Dict[str, str] = self.client.commit_transaction(**kwargs)
        self._transaction_status = response['transactionStatus']

        return self._transaction_status

    def rollback(
        self,
        transaction_id: Optional[str] = None,
        resource_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
    ) -> str:
        kwargs: Dict[str, Any] = {
            'resourceArn': resource_arn or self.resource_arn,
            'secretArn': secret_arn or self.secret_arn,
            'transactionId': transaction_id or self.transaction_id,
        }

        response: Dict[str, str] = self.client.rollback_transaction(**kwargs)
        self._transaction_status = response['transactionStatus']

        return self._transaction_status

    def execute(
        self,
        query: Union[Query, Insert, Update, Delete, Select, str],
        parameters: Union[None, Dict[str, Any], List[Dict[str, Any]]] = None,
        with_columns: bool = False,
        transaction_id: Optional[str] = None,
        continue_after_timeout: bool = True,
        resource_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        database: Optional[str] = None,
    ) -> Union[Results, List[RowDict]]:

        kwargs: Dict[str, Any] = {
            'resourceArn': resource_arn or self.resource_arn,
            'secretArn': secret_arn or self.secret_arn,
        }

        if transaction_id or self.transaction_id:
            kwargs['transactionId'] = transaction_id or self.transaction_id

        if database or self.database:
            kwargs['database'] = database or self.database
        if not isinstance(query, str):
            sql: str = generate_sql(query)
        else:
            sql = query

        # batch
        if isinstance(parameters, List):
            sql_parameter_set: List[List[Field]] = [
                create_sql_parameters(parameter) for parameter in parameters
            ]
            response: Dict[str, Any] = self.client.batch_execute_statement(
                sql=sql, parameterSets=sql_parameter_set, **kwargs
            )
            return Results(response)

        if continue_after_timeout:
            kwargs['continueAfterTimeout'] = continue_after_timeout

        if isinstance(parameters, Dict):
            sql_parameters: List[Field] = create_sql_parameters(parameters)
            kwargs['parameters'] = sql_parameters

        response = self.client.execute_statement(
            includeResultMetadata=True, sql=sql, **kwargs
        )
        result = Results(response)

        if with_columns:
            return result.as_dict()
        return result


def transaction(
    resource_arn: str,
    secret_arn: str,
    database: Optional[str] = None,
    transaction_id: Optional[str] = None,
    client: Optional[boto3.session.Session.client] = None,
    rollback_exception: Optional[Type[Exception]] = None,
) -> Callable:
    def get_func(func: Callable) -> Callable:
        @wraps(func)
        def wrap(*args: Any, **kwargs: Any) -> Any:
            with DataAPI(
                resource_arn,
                secret_arn,
                database,
                transaction_id,
                client,
                rollback_exception,
            ) as data_api:
                result: Any = func(data_api, *args, **kwargs)
            return result

        return wrap

    return get_func

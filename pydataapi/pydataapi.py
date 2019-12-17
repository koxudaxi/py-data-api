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
)

import boto3
from more_itertools import chunked, flatten
from pydantic import BaseModel, Field, root_validator, validator
from pydataapi.exceptions import DataAPIError, MultipleResultsFound, NoResultFound
from sqlalchemy import Column
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect, default
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Delete, Insert, Select, Update

MAX_RECORDS: int = 1000

DIALECT: Dialect = mysql.dialect(paramstyle='named')

QUERY_STATEMENT_COMPILE_PARAMS = {
    'dialect': mysql.dialect(paramstyle='named'),
    'compile_kwargs': {"literal_binds": True},
}

BOOLEAN_VALUE: str = 'booleanValue'
STRING_VALUE: str = 'stringValue'
LONG_VALUE: str = 'longValue'
DOUBLE_VALUE: str = 'doubleValue'
BLOB_VALUE: str = 'blobValue'
IS_NULL: str = 'isNull'
ARRAY_VALUE: str = 'arrayValue'
ARRAY_VALUES: str = 'arrayValues'
BOOLEAN_VALUES: str = 'booleanValues'
STRING_VALUES: str = 'stringValues'
LONG_VALUES: str = 'longValues'
DOUBLE_VALUES: str = 'doubleValues'
BLOB_VALUES: str = 'blobValues'


def generate_sql(query: Union[Query, Insert, Update, Delete, Select]) -> str:
    if hasattr(query, 'statement'):
        sql: str = query.statement.compile(**QUERY_STATEMENT_COMPILE_PARAMS)
    else:
        sql = query.compile(**QUERY_STATEMENT_COMPILE_PARAMS)
    return str(sql)


def wrap_process_result_value_function(
    process_result_value: Callable, dialect: default.DefaultDialect
) -> Callable:
    @wraps(process_result_value)
    def wrapped(value: Any) -> Callable:
        return process_result_value(value, dialect)

    return wrapped


def get_process_result_value_function(
    table_name: str,
    column_name: str,
    query: Union[Select, Query],
    dialect: default.DefaultDialect,
) -> Callable:
    process_result_value: Optional[Callable] = None
    if isinstance(query, Select):  # pragma: no cover
        for column in query.columns:
            if column.name == column_name:
                process_result_value = getattr(
                    column.type, 'process_result_value', None
                )
                break
    elif isinstance(query, Query):  # pragma: no cover
        for column_description in query.column_descriptions:
            type_ = column_description['type']
            if type_.__tablename__ == table_name:
                column = getattr(type_, column_name, None)
                if column:
                    expression = getattr(column, 'expression', None)
                    if (
                        isinstance(expression, Column)
                        and expression.name == column_name
                    ):
                        process_result_value = getattr(
                            expression.type, 'process_result_value', None
                        )
                break
    if process_result_value:
        return wrap_process_result_value_function(process_result_value, dialect)
    return lambda v: v


def create_process_result_value_function_list(
    column_metadata: List[Dict[str, Any]],
    query: Union[Select, Query],
    dialect: default.DefaultDialect,
) -> List[Callable]:
    return [
        get_process_result_value_function(cm['tableName'], cm['name'], query, dialect)
        for cm in column_metadata
    ]


def convert_array_value(value: Union[List, Tuple]) -> Dict[str, Any]:
    first_value: Any = value[0]
    if isinstance(first_value, (list, tuple)):
        return {
            ARRAY_VALUE: {
                ARRAY_VALUES: [
                    convert_array_value(nested_value) for nested_value in value
                ]
            }
        }

    values_key: Optional[str] = None
    if isinstance(first_value, bool):
        values_key = BOOLEAN_VALUES
    elif isinstance(first_value, str):
        values_key = STRING_VALUES
    elif isinstance(first_value, int):
        values_key = LONG_VALUES
    elif isinstance(first_value, float):
        values_key = DOUBLE_VALUES
    elif isinstance(first_value, bytes):
        values_key = BLOB_VALUES
    if values_key:
        return {ARRAY_VALUE: {values_key: list(value)}}
    raise Exception(f'unsupported array type {type(value[0])}]: {value} ')


def convert_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {BOOLEAN_VALUE: value}
    elif isinstance(value, str):
        return {STRING_VALUE: value}
    elif isinstance(value, int):
        return {LONG_VALUE: value}
    elif isinstance(value, float):
        return {DOUBLE_VALUE: value}
    elif isinstance(value, bytes):
        return {BLOB_VALUE: value}
    elif value is None:
        return {IS_NULL: True}
    elif isinstance(value, (list, tuple)):
        if not value:
            return {IS_NULL: True}
        return convert_array_value(value)
    # TODO: support structValue
    return {STRING_VALUE: str(value)}


def create_sql_parameters(
    parameter: Dict[str, Any]
) -> List[Dict[str, Union[str, Dict]]]:
    return [
        {'name': key, 'value': convert_value(value)} for key, value in parameter.items()
    ]


def _get_value_from_row(row: Dict[str, Any]) -> Any:
    key = tuple(row.keys())[0]
    if key == IS_NULL:
        return None
    value = row[key]
    if key == ARRAY_VALUE:
        array_key: str = tuple(value.keys())[0]
        array_value: Union[List[Dict[str, Dict]], Dict[str, List]] = value[array_key]
        if array_key == ARRAY_VALUES:
            return [
                tuple(nested_value[ARRAY_VALUE].values())[0]  # type: ignore
                for nested_value in array_value
            ]
        return array_value
    return value


T = TypeVar('T')


class GeneratedFields:
    def __repr__(self) -> str:
        values: str = ', '.join(str(f) for f in self.generated_fields)
        return f'<{self.__class__.__name__}({values})>'

    def __init__(self, generated_fields: List[Dict[str, Any]]):
        self._generated_fields_raw: List[Dict[str, Any]] = generated_fields
        self._generated_fields: Optional[List] = None

    @property
    def generated_fields(self) -> List:
        if self._generated_fields is None:
            self._generated_fields = [
                _get_value_from_row(f) for f in self._generated_fields_raw
            ]
        return self._generated_fields

    @property
    def generated_fields_first(self) -> Union[str, int, float, None]:
        if self.generated_fields:
            return self.generated_fields[0]
        return None

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, GeneratedFields):
            return self.generated_fields == other.generated_fields
        elif isinstance(other, list):
            return self.generated_fields == other
        elif isinstance(other, tuple):
            return self.generated_fields == list(other)
        return False


class Record(Sequence, Iterator):
    def __repr__(self) -> str:
        values: str = ', '.join(f'{k}={str(v)}' for k, v in self.dict().items())
        return f'<{self.__class__.__name__}({values})>'

    def __next__(self) -> Any:
        self._index += 1
        try:
            return self[self._index]
        except IndexError:
            raise StopIteration

    def __getitem__(  # type: ignore
        self, i: Union[int, slice]
    ) -> Any:
        return self._record[i]  # type: ignore

    def __len__(self) -> int:
        return len(self._record)

    def __init__(self, row: List, headers: List[str]) -> None:
        self._record: List = row
        self._headers: List[str] = headers
        self._index: int = -1

    @property
    def headers(self) -> List:
        return self._headers

    def dict(self) -> Dict:
        return {header: column for header, column in zip(self.headers, self._record)}

    def model(self, model_type: Type[T]) -> T:
        return model_type(**self.dict())  # type: ignore

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Record):
            return self._record == other._record
        elif isinstance(other, list):
            return self._record == other
        elif isinstance(other, tuple):
            return self._record == list(other)
        return False


class Result(Sequence[Record], Iterator[Record], GeneratedFields):
    def __next__(self) -> Any:
        self._index += 1
        try:
            return self[self._index]
        except IndexError:
            raise StopIteration

    def __getitem__(  # type: ignore
        self, i: Union[int, slice]
    ) -> Union['Record', List['Record']]:
        if isinstance(i, slice):
            return [Record(r, self.headers) for r in self._rows[i]]
        return Record(self._rows[i], self.headers)  # type: ignore

    def __len__(self) -> int:
        return len(self._rows)

    def __init__(
        self,
        response: Dict,
        process_result_value_function_list: Optional[List[Callable]] = None,
    ) -> None:
        self._response = response
        if process_result_value_function_list:
            self._rows: Sequence[List] = [
                [
                    process_result_value(_get_value_from_row(column))
                    for column, process_result_value in zip(
                        row, process_result_value_function_list
                    )
                ]
                for row in response.get('records', [])  # type: ignore
            ]
        else:
            self._rows = [
                [_get_value_from_row(column) for column in row]
                for row in response.get('records', [])  # type: ignore
            ]
        self._column_metadata: List[Dict[str, Any]] = response.get('columnMetadata', [])
        self._headers: Optional[List[str]] = None
        self._index: int = -1
        super().__init__(response.get('generatedFields', []))

    @property
    def number_of_records_updated(self) -> int:
        return self._response.get('numberOfRecordsUpdated', 0)

    @property
    def headers(self) -> List[str]:
        if self._headers is None:
            self._headers = [meta['label'] for meta in self._column_metadata]
        return self._headers

    def first(self) -> Optional[Record]:
        if len(self) > 0:
            return self[0]  # type: ignore
        return None

    def one(self) -> Record:
        if len(self) == 1:
            return self[0]  # type: ignore
        elif len(self) > 1:
            raise MultipleResultsFound
        raise NoResultFound

    def one_or_none(self) -> Optional[Record]:
        if len(self) == 1:
            return self[0]  # type: ignore
        elif len(self) > 1:
            raise MultipleResultsFound
        return None

    def scalar(self) -> Any:
        return self.one()[0]

    def all(self) -> List[Record]:
        return list(self)


class UpdateResults(Sequence[GeneratedFields]):
    def __getitem__(  # type: ignore
        self, i: Union[int, slice]
    ) -> Union['GeneratedFields', List['GeneratedFields']]:
        if isinstance(i, slice):
            return [
                GeneratedFields(r['generatedFields']) for r in self._update_results[i]
            ]
        return GeneratedFields(
            self._update_results[i]['generatedFields']
        )  # type: ignore

    def __len__(self) -> int:
        return len(self._update_results)

    def __init__(self, update_results: List[Dict[str, List[Dict[str, Any]]]]) -> None:
        self._update_results = update_results


class Options(BaseModel):
    resourceArn: str
    secretArn: str
    sql: Optional[str]
    database: Optional[str]
    schema_: Optional[str] = Field(None, alias='schema')
    transactionId: Optional[str]
    continueAfterTimeout: Optional[bool]
    parameters: Optional[List[Dict[str, Any]]]
    parameterSets: Optional[List[List[Dict[str, Any]]]]

    @root_validator
    def validate_all(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in values.items() if v is not None}

    @validator('parameters', pre=True)
    def convert_parameters(cls, v: Any) -> Any:
        if isinstance(v, Dict):
            return create_sql_parameters(v)
        return v

    @validator('parameterSets', pre=True)
    def convert_parameter_sets(cls, v: Any) -> Any:
        if isinstance(v, (list, tuple)):  # pragma: no cover
            return [create_sql_parameters(parameter) for parameter in v]
        return v  # pragma: no cover

    @validator('sql', pre=True)
    def validate_sql(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v
        return generate_sql(v)

    def build(self) -> Dict[str, Any]:
        return self.dict(exclude_unset=True, by_alias=True)


def find_arn_by_resource_name(
    resource_name: str, boto3_client: Optional[boto3.session.Session.client]
) -> str:
    if not boto3_client:
        boto3_client = boto3.client('rds')
    return boto3_client.describe_db_clusters(DBClusterIdentifier=resource_name)[
        'DBClusters'
    ][0]['DBClusterArn']


class DataAPI(AbstractContextManager):
    def __init__(
        self,
        *,
        secret_arn: str,
        resource_arn: Optional[str] = None,
        resource_name: Optional[str] = None,
        database: Optional[str] = None,
        transaction_id: Optional[str] = None,
        client: Optional[boto3.session.Session.client] = None,
        rollback_exception: Optional[Type[Exception]] = None,
        rds_client: Optional[boto3.session.Session.client] = None,
    ) -> None:
        if resource_name:
            if resource_arn:
                raise DataAPIError(
                    f'resource_name should be set without resource_arn. resource_arn: {resource_arn},'
                    f' resource_name: {resource_name}'
                )
            resource_arn = find_arn_by_resource_name(resource_name, rds_client)
        if not resource_arn:
            raise DataAPIError('Not Found resource_arn.')
        self.resource_arn: str = resource_arn
        self.secret_arn: str = secret_arn
        self.database: Optional[str] = database

        self._transaction_id: Optional[str] = transaction_id
        self._client: boto3.session.Session.client = client or boto3.client('rds-data')
        self._transaction_status: Optional[str] = None
        self.rollback_exception: Optional[Type[Exception]] = rollback_exception

    def __enter__(self) -> 'DataAPI':
        self.begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            if self.rollback_exception:
                if issubclass(exc_type, self.rollback_exception):
                    self.rollback()
                else:
                    self.commit()
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
        self, database: Optional[str] = None, schema: Optional[str] = None
    ) -> str:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            database=database or self.database,
            schema=schema,
        )

        response: Dict[str, str] = self.client.begin_transaction(**options.build())
        self._transaction_id = response['transactionId']

        return response['transactionId']

    def commit(self, transaction_id: Optional[str] = None) -> str:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id or self.transaction_id,
        )

        response: Dict[str, str] = self.client.commit_transaction(**options.build())
        self._transaction_status = response['transactionStatus']

        return self._transaction_status

    def rollback(self, transaction_id: Optional[str] = None) -> str:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id or self.transaction_id,
        )

        response: Dict[str, str] = self.client.rollback_transaction(**options.build())
        self._transaction_status = response['transactionStatus']

        return self._transaction_status

    def execute(
        self,
        query: Union[Query, Insert, Update, Delete, Select, str],
        parameters: Optional[Dict[str, Any]] = None,
        transaction_id: Optional[str] = None,
        continue_after_timeout: bool = True,
        database: Optional[str] = None,
    ) -> Result:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            database=database or self.database,
            transactionId=transaction_id or self.transaction_id,
            continueAfterTimeout=continue_after_timeout,
            parameters=parameters,
            sql=query,
        )

        response = self.client.execute_statement(
            includeResultMetadata=True, **options.build()
        )

        if isinstance(query, (Query, Select)):
            process_result_value_function_list = create_process_result_value_function_list(
                response.get('columnMetadata', []),
                query,
                QUERY_STATEMENT_COMPILE_PARAMS['dialect'],
            )
            return Result(response, process_result_value_function_list)
        return Result(response)

    def batch_execute(
        self,
        query: Union[Query, Insert, Update, Delete, Select, str],
        parameter_sets: Optional[List[Dict[str, Any]]],
        transaction_id: Optional[str] = None,
        database: Optional[str] = None,
    ) -> UpdateResults:

        if self.transaction_id:
            start_transaction: bool = False
        else:
            self.begin(database=database)
            start_transaction = True
        try:
            results_sets = list(
                flatten(
                    self.client.batch_execute_statement(
                        **Options(
                            resourceArn=self.resource_arn,
                            secretArn=self.secret_arn,
                            database=database or self.database,
                            transactionId=transaction_id or self.transaction_id,
                            parameterSets=chunked_parameter_sets,
                            sql=query,
                        ).build()
                    )["updateResults"]
                    for chunked_parameter_sets in chunked(
                        parameter_sets or [], MAX_RECORDS
                    )
                )
            )
        except:
            if start_transaction:
                self.rollback()
            raise
        if start_transaction:
            self.commit()
        return UpdateResults(results_sets)


def transaction(
    secret_arn: str,
    resource_arn: Optional[str] = None,
    resource_name: Optional[str] = None,
    database: Optional[str] = None,
    transaction_id: Optional[str] = None,
    client: Optional[boto3.session.Session.client] = None,
    rollback_exception: Optional[Type[Exception]] = None,
) -> Callable:
    def get_func(func: Callable) -> Callable:
        @wraps(func)
        def wrap(*args: Any, **kwargs: Any) -> Any:
            with DataAPI(
                secret_arn=secret_arn,
                resource_arn=resource_arn,
                resource_name=resource_name,
                database=database,
                transaction_id=transaction_id,
                client=client,
                rollback_exception=rollback_exception,
            ) as data_api:
                result: Any = func(data_api, *args, **kwargs)
            return result

        return wrap

    return get_func

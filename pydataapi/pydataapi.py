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
from pydantic import BaseModel, Field, root_validator, validator
from pydataapi.exceptions import MultipleResultsFound, NoResultFound
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Delete, Insert, Select, Update

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
                list(f.values())[0] for f in self._generated_fields_raw
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

    def __init__(self, response: Dict):
        self._response = response
        self._rows: Sequence[List[Dict]] = [
            [tuple(column.values())[0] for column in row]
            for row in response.get('records', [])  # type: ignore
        ]
        self._column_metadata: List[Dict[str, Any]] = response.get('columnMetadata', [])
        self._headers: Optional[List[str]] = None
        self._index: int = -1
        super().__init__(response.get('generatedFields', []))

    @property
    def number_of_records_updated(self) -> Optional[int]:
        return self._response.get('numberOfRecordsUpdated')

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
        if isinstance(v, list):
            return [create_sql_parameters(parameter) for parameter in v]
        return v

    @validator('sql', pre=True)
    def validate_sql(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v
        return generate_sql(v)

    def build(self) -> Dict[str, Any]:
        return self.dict(skip_defaults=True, by_alias=True)


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
        return Result(response)

    def batch_execute(
        self,
        query: Union[Query, Insert, Update, Delete, Select, str],
        parameter_sets: Optional[List[Dict[str, Any]]],
        transaction_id: Optional[str] = None,
        database: Optional[str] = None,
    ) -> UpdateResults:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            database=database or self.database,
            transactionId=transaction_id or self.transaction_id,
            parameterSets=parameter_sets,
            sql=query,
        )

        response: Dict[str, Any] = self.client.batch_execute_statement(
            **options.build()
        )
        return UpdateResults(response["updateResults"])


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

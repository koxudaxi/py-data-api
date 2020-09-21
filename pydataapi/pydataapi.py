from datetime import date, datetime, time
from decimal import Decimal
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
from sqlalchemy import Column
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect, default

from pydataapi.exceptions import DataAPIError, MultipleResultsFound, NoResultFound

MAX_RECORDS: int = 1000

DIALECT: Dialect = mysql.dialect(paramstyle="named")

QUERY_STATEMENT_COMPILE_PARAMS = {
    "dialect": mysql.dialect(paramstyle="named"),
    "compile_kwargs": {"literal_binds": True},
}

BOOLEAN_VALUE: str = "booleanValue"
STRING_VALUE: str = "stringValue"
LONG_VALUE: str = "longValue"
DOUBLE_VALUE: str = "doubleValue"
BLOB_VALUE: str = "blobValue"
IS_NULL: str = "isNull"
ARRAY_VALUE: str = "arrayValue"
ARRAY_VALUES: str = "arrayValues"
BOOLEAN_VALUES: str = "booleanValues"
STRING_VALUES: str = "stringValues"
LONG_VALUES: str = "longValues"
DOUBLE_VALUES: str = "doubleValues"
BLOB_VALUES: str = "blobValues"

DECIMAL_TYPE_HINT: str = "DECIMAL"
TIMESTAMP_TYPE_HINT: str = "TIMESTAMP"
TIME_TYPE_HINT: str = "TIME"
DATE_TYPE_HINT: str = "DATE"


def convert_array_value(value: Union[List[Any], Tuple[Any, ...]]) -> Dict[str, Any]:
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
    raise Exception(f"unsupported array type {type(value[0])}]: {value} ")


def create_sql_parameter(key: str, value: Any) -> Dict[str, Any]:
    converted_value: Dict[str, Any]
    type_hint: Optional[str] = None

    if isinstance(value, bool):
        converted_value = {BOOLEAN_VALUE: value}
    elif isinstance(value, str):
        converted_value = {STRING_VALUE: value}
    elif isinstance(value, int):
        converted_value = {LONG_VALUE: value}
    elif isinstance(value, float):
        converted_value = {DOUBLE_VALUE: value}
    elif isinstance(value, bytes):
        converted_value = {BLOB_VALUE: value}
    elif value is None:
        converted_value = {IS_NULL: True}
    elif isinstance(value, (list, tuple)):
        if value:
            converted_value = convert_array_value(value)
        else:
            converted_value = {IS_NULL: True}
    elif isinstance(value, Decimal):
        converted_value = {STRING_VALUE: str(value)}
        type_hint = DECIMAL_TYPE_HINT
    elif isinstance(value, datetime):
        converted_value = {STRING_VALUE: value.strftime("%Y-%m-%d %H:%M:%S.%f")}
        type_hint = TIMESTAMP_TYPE_HINT
    elif isinstance(value, time):
        converted_value = {STRING_VALUE: value.strftime("%H:%M:%S.%f")}
        type_hint = TIME_TYPE_HINT
    elif isinstance(value, date):
        converted_value = {STRING_VALUE: value.strftime("%Y-%m-%d")}
        type_hint = DATE_TYPE_HINT
    else:
        # TODO: support structValue
        converted_value = {STRING_VALUE: str(value)}
    if type_hint:
        return {"name": key, "value": converted_value, "typeHint": type_hint}
    return {"name": key, "value": converted_value}


def create_sql_parameters(
    parameter: Dict[str, Any]
) -> List[Dict[str, Union[str, Dict[str, Any]]]]:
    return [create_sql_parameter(key, value) for key, value in parameter.items()]


def _get_value_from_row(row: Dict[str, Any]) -> Any:
    key = tuple(row.keys())[0]
    if key == IS_NULL:
        return None
    value = row[key]
    if key == ARRAY_VALUE:
        array_key: str = tuple(value.keys())[0]
        array_value: Union[
            List[Dict[str, Dict[str, Any]]], Dict[str, List[Any]]
        ] = value[array_key]
        if array_key == ARRAY_VALUES:
            return [
                tuple(nested_value[ARRAY_VALUE].values())[0]  # type: ignore
                for nested_value in array_value
            ]
        return array_value
    return value


T = TypeVar("T")


class GeneratedFields:
    def __repr__(self) -> str:
        values: str = ", ".join(str(f) for f in self.generated_fields)
        return f"<{self.__class__.__name__}({values})>"

    def __init__(self, generated_fields: List[Dict[str, Any]]):
        self._generated_fields_raw: List[Dict[str, Any]] = generated_fields
        self._generated_fields: Optional[List[str]] = None

    @property
    def generated_fields(self) -> List[Any]:
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


class Record(Sequence[Any], Iterator[Any]):
    def __repr__(self) -> str:
        values: str = ", ".join(f"{k}={str(v)}" for k, v in self.dict().items())
        return f"<{self.__class__.__name__}({values})>"

    def __next__(self) -> Any:
        self._index += 1
        try:
            return self[self._index]
        except IndexError:
            raise StopIteration

    def __getitem__(self, i: Union[int, slice]) -> Any:
        return self._record[i]

    def __len__(self) -> int:
        return len(self._record)

    def __init__(self, row: List[Any], headers: List[str]) -> None:
        self._record: List[Any] = row
        self._headers: List[str] = headers
        self._index: int = -1

    @property
    def headers(self) -> List[str]:
        return self._headers

    def dict(self) -> Dict[str, Any]:
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


class Result(
    Sequence[Union["Record", List["Record"]]],
    Iterator[Union["Record", List["Record"]]],
    GeneratedFields,
):
    def __next__(self) -> "Record":
        self._index += 1
        try:
            return self[self._index]  # type: ignore
        except IndexError:
            raise StopIteration

    def __getitem__(self, i: Union[int, slice]) -> Union["Record", List["Record"]]:
        if isinstance(i, slice):
            return [Record(r, self.headers) for r in self._rows[i]]
        return Record(self._rows[i], self.headers)

    def __len__(self) -> int:
        return len(self._rows)

    def __init__(
        self,
        response: Dict[Any, Any],
    ) -> None:
        self._response = response
        self._rows = [
            [_get_value_from_row(column) for column in row]
            for row in response.get("records", [])
        ]
        self._column_metadata: List[Dict[str, Any]] = response.get("columnMetadata", [])
        self._headers: Optional[List[str]] = None
        self._index: int = -1
        super().__init__(response.get("generatedFields", []))

    @property
    def number_of_records_updated(self) -> int:
        return self._response.get("numberOfRecordsUpdated", 0)

    @property
    def headers(self) -> List[str]:
        if self._headers is None:
            self._headers = [meta["label"] for meta in self._column_metadata]
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
        return list(self)  # type: ignore


class UpdateResults(Sequence[GeneratedFields]):
    def __getitem__(  # type: ignore
        self, i: Union[int, slice]
    ) -> Union["GeneratedFields", List["GeneratedFields"]]:
        if isinstance(i, slice):
            return [
                GeneratedFields(r["generatedFields"]) for r in self._update_results[i]
            ]
        return GeneratedFields(self._update_results[i]["generatedFields"])

    def __len__(self) -> int:
        return len(self._update_results)

    def __init__(self, update_results: List[Dict[str, List[Dict[str, Any]]]]) -> None:
        self._update_results = update_results


class Options(BaseModel):
    resourceArn: str
    secretArn: str
    sql: Optional[str]
    database: Optional[str]
    schema_: Optional[str] = Field(None, alias="schema")
    transactionId: Optional[str]
    continueAfterTimeout: Optional[bool]
    parameters: Optional[List[Dict[str, Any]]]
    parameterSets: Optional[List[List[Dict[str, Any]]]]

    @root_validator
    def validate_all(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in values.items() if v is not None}

    @validator("parameters", pre=True)
    def convert_parameters(cls, v: Any) -> Any:
        if isinstance(v, Dict):
            return create_sql_parameters(v)
        return v

    @validator("parameterSets", pre=True)
    def convert_parameter_sets(cls, v: Any) -> Any:
        if isinstance(v, (list, tuple)):  # pragma: no cover
            return [create_sql_parameters(parameter) for parameter in v]
        return v  # pragma: no cover

    def build(self) -> Dict[str, Any]:
        return self.dict(exclude_unset=True, by_alias=True)


def find_arn_by_resource_name(
    resource_name: str, boto3_client: Optional[boto3.session.Session.client]
) -> str:
    if not boto3_client:
        boto3_client = boto3.client("rds")
    return boto3_client.describe_db_clusters(DBClusterIdentifier=resource_name)[
        "DBClusters"
    ][0]["DBClusterArn"]


class DataAPI:
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
        auto_transaction: Optional[bool] = None,
    ) -> None:
        if resource_name:
            if resource_arn:
                raise DataAPIError(
                    f"resource_name should be set without resource_arn. resource_arn: {resource_arn},"
                    f" resource_name: {resource_name}"
                )
            resource_arn = find_arn_by_resource_name(resource_name, rds_client)
        if not resource_arn:
            raise DataAPIError("Not Found resource_arn.")
        self.resource_arn: str = resource_arn
        self.secret_arn: str = secret_arn
        self.database: Optional[str] = database

        client_kwargs = {}
        region_name = resource_arn.split(":")[3]
        client_kwargs["region_name"] = region_name

        self._transaction_id: Optional[str] = transaction_id
        self._client: boto3.session.Session.client = client or boto3.client(
            "rds-data", **client_kwargs
        )
        self._transaction_status: Optional[str] = None
        self.rollback_exception: Optional[Type[Exception]] = rollback_exception
        self._auto_transaction: Optional[bool] = auto_transaction

    def __enter__(self) -> "DataAPI":
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

    @property
    def auto_transaction(self) -> Optional[bool]:
        return self._auto_transaction

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
        self._transaction_id = response["transactionId"]

        return response["transactionId"]

    def commit(self, transaction_id: Optional[str] = None) -> str:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id or self.transaction_id,
        )

        response: Dict[str, str] = self.client.commit_transaction(**options.build())
        self._transaction_status = response["transactionStatus"]

        return self._transaction_status

    def rollback(self, transaction_id: Optional[str] = None) -> str:

        options = Options(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id or self.transaction_id,
        )

        response: Dict[str, str] = self.client.rollback_transaction(**options.build())
        self._transaction_status = response["transactionStatus"]

        return self._transaction_status

    def execute(
        self,
        query: str,
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
            parameters=parameters,  # type: ignore
            sql=query,
        )

        response = self.client.execute_statement(
            includeResultMetadata=True, **options.build()
        )

        return Result(response)

    def batch_execute(
        self,
        query: str,
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
                            parameterSets=chunked_parameter_sets,  # type: ignore
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
) -> Callable[..., Any]:
    def get_func(func: Callable[..., Any]) -> Callable[..., Any]:
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

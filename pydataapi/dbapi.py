from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

import boto3
from pydantic import BaseModel

from .pydataapi import DataAPI

apilevel: str = '2.0'
threadsafety: int = 2
paramstyle: str = 'named'


def get_description(column_metadata: List[Dict[str, Any]]) -> Tuple:
    return tuple(
        (
            meta['label'],  # name
            0,  # type_code,
            0,  # display_size,
            0,  # internal_size,
            meta['precision'],  # precision,
            meta['scale'],  # scale,
            meta['nullable'],
        )
        for meta in column_metadata
    )


class Error(Exception):
    pass


class ConnectArgs(BaseModel):
    secret_arn: str
    resource_arn: Optional[str]
    resource_name: Optional[str]
    database: Optional[str] = None
    transaction_id: Optional[str] = None
    client: Optional[Any] = None
    rollback_exception: Optional[Type[Exception]] = None
    rds_client: Optional[Any] = None


class Connection:
    paramstyle = paramstyle
    Error = Error

    def __init__(self, **kwargs: Any) -> None:
        connect_args = ConnectArgs.parse_obj(kwargs)
        self._data_api = DataAPI(
            secret_arn=connect_args.secret_arn,
            resource_arn=connect_args.resource_arn,
            resource_name=connect_args.resource_name,
            database=connect_args.database,
            transaction_id=connect_args.transaction_id,
            client=connect_args.client,
            rollback_exception=connect_args.rollback_exception,
            rds_client=connect_args.rds_client,
        )

        self.closed = False
        self.cursors: List[Cursor] = []

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        if self._data_api.transaction_id:
            self._data_api.commit()
            self._data_api._transaction_id = None

    def rollback(self) -> None:
        if self._data_api.transaction_id:
            self._data_api.rollback()
            self._data_api._transaction_id = None

    def cursor(self) -> 'Cursor':
        if not self._data_api.transaction_id:
            self._data_api.begin()
        cursor = Cursor(self._data_api)
        self.cursors.append(cursor)

        return cursor

    @classmethod
    def connect(cls, **kwargs: Any) -> 'Connection':
        return cls(**kwargs)

    def execute(self, operation: Any, parameters: Any = None) -> 'Cursor':
        return self.cursor().execute(operation, parameters)

    def __enter__(self) -> 'Connection':
        self._data_api.begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            if self._data_api.rollback_exception:
                if issubclass(exc_type, self._data_api.rollback_exception):
                    self.rollback()
                else:
                    self.commit()
            else:
                self.rollback()


class Cursor:
    def __init__(self, data_api: DataAPI) -> None:
        self._data_api: DataAPI = data_api
        self.arraysize = 1

        self.closed = False

        self.description: Optional[List] = None

        self._rows: List[List] = []
        self._rowcount: int = -1
        self._lastrowid: Optional[int] = None

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Optional[int]:
        return self._lastrowid

    def close(self) -> None:
        self.closed = True

    def execute(
        self, operation: Any, parameters: Optional[Dict[str, Any]] = None
    ) -> 'Cursor':
        self.description = None
        result = self._data_api.execute(operation, parameters)
        self.description = get_description(  # type: ignore
            getattr(result, '_column_metadata')
        )
        rows: List[List] = getattr(result, '_rows')
        self._rows = rows
        self._rowcount = len(rows) or result.number_of_records_updated
        self._lastrowid = result.generated_fields_first  # type: ignore
        return self

    def executemany(
        self, operation: Any, seq_of_parameters: Optional[List[Dict[str, Any]]] = None
    ) -> 'Cursor':
        self.description = None
        results = self._data_api.batch_execute(operation, seq_of_parameters)
        self._rows = [result.generated_fields for result in results]
        self._rowcount = len(self._rows)
        self.description = []
        self._lastrowid = (  # type: ignore
            results[-1].generated_fields_first if results else None  # type: ignore
        )
        return self

    def fetchone(self) -> Optional[List]:
        try:
            return self._rows.pop(0)
        except IndexError:
            return None

    def fetchmany(self, size: Optional[int] = None) -> List[List]:
        size = size or self.arraysize
        result, self._rows = self._rows[:size], self._rows[size:]
        return result

    def fetchall(self) -> List[List]:
        rows = self._rows
        self._rows = []
        return rows

    def setinputsizes(self, sizes: Any) -> None:  # pragma: no cover
        pass

    def setoutputsizes(self, sizes: Any) -> None:  # pragma: no cover
        pass

    def __iter__(self) -> Iterator[List]:
        return iter(self._rows)


def connect(
    secret_arn: str,
    resource_arn: Optional[str] = None,
    resource_name: Optional[str] = None,
    database: Optional[str] = None,
    transaction_id: Optional[str] = None,
    client: Optional[boto3.session.Session.client] = None,
    rollback_exception: Optional[Type[Exception]] = None,
    rds_client: Optional[boto3.session.Session.client] = None,
    **kwargs: Any
) -> Connection:
    return Connection(
        secret_arn=secret_arn,
        resource_arn=resource_arn,
        resource_name=resource_name,
        database=database,
        transaction_id=transaction_id,
        client=client,
        rollback_exception=rollback_exception,
        rds_client=rds_client,
        **kwargs
    )

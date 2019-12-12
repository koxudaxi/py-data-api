import datetime
import re
from abc import ABC
from typing import Any, Callable, List, Optional, Pattern, Type, TypeVar, Union

from botocore.exceptions import ClientError
from pydataapi.dbapi import Connection
from sqlalchemy import cast
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine


class DataAPIDialect(DefaultDialect, ABC):
    driver: str = 'dataapi'
    supports_alter = True

    supports_native_boolean = True

    max_identifier_length = 255
    max_index_name_length = 64

    supports_native_enum = False

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_multivalues_insert = True

    supports_comments = True
    inline_comments = True

    cte_follows_insert = True

    _backslash_escapes = True
    _server_ansiquotes = False

    @classmethod
    def dbapi(cls) -> Type[Connection]:
        return Connection


DatetimeProtocol = Union[datetime.date, datetime.datetime, datetime.time]

DATE_PATTERN: Pattern = re.compile(r'^\d{4}-[0-1]\d-[0-3]\d$')

DATETIME_PATTERN: Pattern = re.compile(
    r'^\d{4}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d$'
)
DATETIME_MICROSECOND_PATTERN: Pattern = re.compile(
    r'^\d{4}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d\.\d{1,6}$'
)

DATE_FORMAT: str = '%Y-%m-%d'
DATETIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'
DATETIME_MICROSECOND_FORMAT: str = '%Y-%m-%d %H:%M:%S.%f'


def _parse_datetime(value: Union[str, float, int]) -> Optional[datetime.datetime]:
    if isinstance(value, str):  # TODO Support timezone
        if re.search(DATETIME_PATTERN, value):
            return datetime.datetime.strptime(value, DATETIME_FORMAT)
        elif re.search(DATETIME_MICROSECOND_PATTERN, value):
            return datetime.datetime.strptime(value, DATETIME_MICROSECOND_FORMAT)
        elif re.search(DATE_PATTERN, value):
            return datetime.datetime.strptime(value, DATE_FORMAT)
    elif isinstance(value, (int, float)):
        return datetime.datetime.fromtimestamp(value)
    return None  # pragma: no cover


class DataAPIDatetimeBase:
    python_type: Type[DatetimeProtocol]
    db_type: Type[TypeEngine]

    def bind_expression(self, value: Any) -> Any:
        return cast(value, self.db_type)

    def bind_processor(self, dialect: DataAPIDialect) -> Callable:
        def process_bind_value(value: Any) -> Any:
            if isinstance(value, self.python_type):
                return value.strftime(DATETIME_MICROSECOND_FORMAT)
            return value

        return process_bind_value

    def result_processor(self, dialect: DataAPIDialect, coltype: List) -> Any:
        def process_result_value(value: Any) -> Any:
            parsed_datetime = _parse_datetime(value)
            if parsed_datetime:
                if self.python_type == datetime.time:  # pragma: no cover
                    return parsed_datetime.time()
                elif self.python_type == datetime.date:
                    return parsed_datetime.date()
                return parsed_datetime
            return value  # pragma: no cover

        return process_result_value


class DataAPIDatetime(DataAPIDatetimeBase, sqltypes.DATE):
    python_type: Type[DatetimeProtocol] = datetime.datetime
    db_type: Type[TypeEngine] = sqltypes.DATE


class DataAPIDialectMixin:
    def has_table(self, connection, table_name, schema=None) -> bool:  # type: ignore
        try:
            return super().has_table(connection, table_name, schema)  # type: ignore
        except ClientError as e:
            if re.match(
                r"Table '.+' doesn't exist", e.response['Error']['Message']
            ):  # pragma: no cover
                return False
            raise  # pragma: no cover

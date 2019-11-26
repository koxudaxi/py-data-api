import datetime
import re
from abc import ABC
from typing import Any, Callable, List, Pattern, Type, TypeVar, Union

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


DATETIME_PATTERN: Pattern = re.compile(
    r'^\d{4}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d$'
)
DATETIME_MICROSECOND_PATTERN: Pattern = re.compile(
    r'^\d{4}-[0-1]\d-[0-3]\d [0-2]\d:[0-6]\d:[0-6]\d\.\d{1,6}$'
)
DATETIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'
DATETIME_MICROSECOND_FORMAT: str = '%Y-%m-%d %H:%M:%S.%f'


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
            if isinstance(value, str):  # TODO Support timezone
                if re.search(DATETIME_PATTERN, value):
                    return self.python_type.fromisoformat(value)
                elif re.search(DATETIME_MICROSECOND_PATTERN, value):
                    return self.python_type.fromisoformat(f'{value:<026}')
            elif isinstance(value, (int, float)) and issubclass(
                self.python_type, datetime.datetime
            ):
                return self.python_type.fromtimestamp(value)
            return value  # pragma: no cover

        return process_result_value


class DataAPIDatetime(DataAPIDatetimeBase, sqltypes.DATE):
    python_type: Type[DatetimeProtocol] = datetime.datetime
    db_type: Type[TypeEngine] = sqltypes.DATE

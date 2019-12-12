from typing import Any, Type

from sqlalchemy import util
from sqlalchemy.dialects.mysql.base import DATE, DATETIME, TIME, TIMESTAMP, MySQLDialect
from sqlalchemy.sql.type_api import TypeEngine

from .base import DataAPIDatetimeBase, DataAPIDialect, DataAPIDialectMixin


class DataAPITimestamp(DataAPIDatetimeBase, TIMESTAMP):
    db_type: Type[TypeEngine] = TIMESTAMP


class DataAPITime(DataAPIDatetimeBase, TIME):
    db_type: Type[TypeEngine] = TIME


class DataAPIDate(DataAPIDatetimeBase, DATE):
    db_type: Type[TypeEngine] = DATE


class DataAPIDateTime(DataAPIDatetimeBase, DATETIME):
    db_type: Type[TypeEngine] = DATETIME


class MySQLDataAPIDialect(DataAPIDialectMixin, MySQLDialect, DataAPIDialect):
    def get_primary_keys(self, connection, table_name, schema=None, **kw):  # type: ignore
        pass

    def get_temp_table_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    def has_sequence(self, connection, sequence_name, schema=None):  # type: ignore
        pass

    # https://github.com/sqlalchemy/sqlalchemy/blob/master/lib/sqlalchemy/dialects/mysql/mysqldb.py
    def _extract_error_code(self, exception: Exception) -> Any:  # pragma: no cover
        return exception.args[0]

    def _detect_charset(self, connection: Any) -> Any:  # pragma: no cover
        return connection.execute(
            "show variables like 'character_set_client'"
        ).fetchone()[1]

    name = "mysql"
    default_paramstyle = "named"

    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
            TIMESTAMP: DataAPITimestamp,
            DATE: DataAPIDate,
            TIME: DataAPITime,
            DATETIME: DataAPIDateTime,
        },
    )

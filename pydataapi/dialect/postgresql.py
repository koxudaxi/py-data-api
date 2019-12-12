from typing import Type

from sqlalchemy import util
from sqlalchemy.dialects.postgresql.base import DATE, TIME, TIMESTAMP, PGDialect
from sqlalchemy.sql.type_api import TypeEngine

from ..dbapi import Connection
from .base import DataAPIDatetimeBase, DataAPIDialect, DataAPIDialectMixin


class DataAPITimestamp(DataAPIDatetimeBase, TIMESTAMP):
    db_type: Type[TypeEngine] = TIMESTAMP


class DataAPITime(DataAPIDatetimeBase, TIME):
    db_type: Type[TypeEngine] = TIME


class DataAPIDate(DataAPIDatetimeBase, DATE):
    db_type: Type[TypeEngine] = DATE


class PostgreSQLDataAPIDialect(DataAPIDialectMixin, PGDialect, DataAPIDialect):
    def get_primary_keys(self, connection, table_name, schema=None, **kw):  # type: ignore
        pass

    def get_temp_table_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    @classmethod
    def dbapi(cls) -> Type[Connection]:
        return Connection

    name = "postgresql"
    default_paramstyle = "named"
    supports_alter = True
    max_identifier_length = 63
    supports_sane_rowcount = True
    isolation_level = None

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {TIMESTAMP: DataAPITimestamp, DATE: DataAPIDate, TIME: DataAPITime},
    )

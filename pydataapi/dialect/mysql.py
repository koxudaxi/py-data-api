from typing import Any, Type

import boto3
from sqlalchemy import util
from sqlalchemy.dialects.mysql.base import DATE, DATETIME, TIME, TIMESTAMP, MySQLDialect
from sqlalchemy.sql.type_api import TypeEngine

from .base import DataAPIDatetimeBase, DataAPIDialect


class DataAPITimestamp(DataAPIDatetimeBase, TIMESTAMP):
    db_type: Type[TypeEngine] = TIMESTAMP


class DataAPITime(DataAPIDatetimeBase, TIME):
    db_type: Type[TypeEngine] = TIME


class DataAPIDate(DataAPIDatetimeBase, DATE):
    db_type: Type[TypeEngine] = DATE


class DataAPIDateTime(DataAPIDatetimeBase, DATETIME):
    db_type: Type[TypeEngine] = DATETIME


class MySQLDataAPIDialect(MySQLDialect, DataAPIDialect):
    def get_primary_keys(self, connection, table_name, schema=None, **kw):  # type: ignore
        pass

    def get_temp_table_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):  # type: ignore
        pass

    def has_sequence(self, connection, sequence_name, schema=None):  # type: ignore
        pass

    def has_table(self, connection, table_name, schema=None): # type: ignore
        # SHOW TABLE STATUS LIKE and SHOW TABLES LIKE do not function properly
        # on macosx (and maybe win?) with multibyte table names.
        #
        # TODO: if this is not a problem on win, make the strategy swappable
        # based on platform.  DESCRIBE is slower.

        # [ticket:726]
        # full_name = self.identifier_preparer.format_table(table,
        #                                                   use_schema=True)

        full_name = ".".join(
            self.identifier_preparer._quote_free_identifiers(
                schema, table_name
            )
        )
        st = "DESCRIBE %s" % full_name
        rs = None
        try:
            try:
                rs = connection.execution_options(
                    skip_user_error_events=True
                ).execute(st)
                have = rs.fetchone() is not None
                rs.close()
                return have
            except boto3.client("rds-data").exceptions.BadRequestException as e:
                if "doesn't exist" in str(e):
                    return False
                raise
        finally:
            if rs:
                rs.close()

        name = "mysql"
        default_paramstyle = "named"

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

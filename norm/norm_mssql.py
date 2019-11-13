
from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy


class MSSQL_INSERT(INSERT):
    bind_prefix = ':'
    bind_postfix = ''


class MSSQL_SELECT(SELECT):
    bind_prefix = ':'
    bind_postfix = ''


class MSSQL_UPDATE(UPDATE):
    bind_prefix = ':'
    bind_postfix = ''


class MSSQL_DELETE(DELETE):
    bind_prefix = ':'
    bind_postfix = ''


class MSSQL_CursorProxy(CursorProxy):
    pass


class MSSQL_ConnectionProxy(ConnectionProxy):
    cursor_proxy = MSSQL_CursorProxy


class MSSQL_ConnectionFactory(ConnectionFactory):
    connection_proxy = MSSQL_ConnectionProxy


__all__ = [MSSQL_INSERT,
           MSSQL_SELECT,
           MSSQL_UPDATE,
           MSSQL_DELETE,
           MSSQL_CursorProxy,
           MSSQL_ConnectionProxy,
           MSSQL_ConnectionFactory]

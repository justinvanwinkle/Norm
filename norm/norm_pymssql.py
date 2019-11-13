
from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy


class PYMSSQL_INSERT(INSERT):
    pass


class PYMSSQL_SELECT(SELECT):
    pass


class PYMSSQL_UPDATE(UPDATE):
    pass


class PYMSSQL_DELETE(DELETE):
    pass


class PYMSSQL_CursorProxy(CursorProxy):
    pass


class PYMSSQL_ConnectionProxy(ConnectionProxy):
    cursor_proxy = PYMSSQL_CursorProxy


class PYMSSQL_ConnectionFactory(ConnectionFactory):
    connection_proxy = PYMSSQL_ConnectionProxy


__all__ = [PYMSSQL_INSERT,
           PYMSSQL_SELECT,
           PYMSSQL_UPDATE,
           PYMSSQL_DELETE,
           PYMSSQL_CursorProxy,
           PYMSSQL_ConnectionProxy,
           PYMSSQL_ConnectionFactory]

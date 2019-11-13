
from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy


class SQLI_INSERT(INSERT):
    bind_prefix = ':'
    bind_postfix = ''


class SQLI_SELECT(SELECT):
    bind_prefix = ':'
    bind_postfix = ''


class SQLI_UPDATE(UPDATE):
    bind_prefix = ':'
    bind_postfix = ''


class SQLI_DELETE(DELETE):
    bind_prefix = ':'
    bind_postfix = ''


class SQLI_CursorProxy(CursorProxy):
    pass


class SQLI_ConnectionProxy(ConnectionProxy):
    cursor_proxy = SQLI_CursorProxy


class SQLI_ConnectionFactory(ConnectionFactory):
    connection_proxy = SQLI_ConnectionProxy


__all__ = [SQLI_INSERT,
           SQLI_SELECT,
           SQLI_UPDATE,
           SQLI_DELETE,
           SQLI_CursorProxy,
           SQLI_ConnectionProxy,
           SQLI_ConnectionFactory]

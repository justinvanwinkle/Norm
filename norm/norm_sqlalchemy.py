
from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy


class SQLA_INSERT(INSERT):
    bind_prefix = ':'
    bind_postfix = ''


class SQLA_SELECT(SELECT):
    bind_prefix = ':'
    bind_postfix = ''


class SQLA_UPDATE(UPDATE):
    bind_prefix = ':'
    bind_postfix = ''


class SQLA_DELETE(DELETE):
    bind_prefix = ':'
    bind_postfix = ''


class SQLA_CursorProxy(CursorProxy):
    pass


class SQLA_ConnectionProxy(ConnectionProxy):
    cursor_proxy = SQLA_CursorProxy


class SQLA_ConnectionFactory(ConnectionFactory):
    connection_proxy = SQLA_ConnectionProxy


__all__ = [SQLA_INSERT,
           SQLA_SELECT,
           SQLA_UPDATE,
           SQLA_DELETE,
           SQLA_CursorProxy,
           SQLA_ConnectionProxy,
           SQLA_ConnectionFactory]

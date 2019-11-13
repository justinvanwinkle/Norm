from .norm import SELECT
from .norm import INSERT
from .norm import UPDATE
from .norm import DELETE
from .connection import ConnectionFactory
from .connection import ConnectionProxy
from .connection import CursorProxy


class MY_CON_INSERT(INSERT):
    pass


class MY_CON_SELECT(SELECT):
    pass


class MY_CON_UPDATE(UPDATE):
    pass


class MY_CON_DELETE(DELETE):
    pass


class MY_CON_CursorProxy(CursorProxy):
    pass


class MY_CON_ConnectionProxy(ConnectionProxy):
    cursor_proxy = MY_CON_CursorProxy


class MY_CON_ConnectionFactory(ConnectionFactory):
    connection_proxy = MY_CON_ConnectionProxy


__all__ = [MY_CON_INSERT,
           MY_CON_SELECT,
           MY_CON_UPDATE,
           MY_CON_DELETE,
           MY_CON_CursorProxy,
           MY_CON_ConnectionProxy,
           MY_CON_ConnectionFactory]

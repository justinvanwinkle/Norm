from __future__ import unicode_literals

from norm import SELECT
from norm import INSERT
from norm import UPDATE
from norm import DELETE
from connection import ConnectionFactory
from connection import ConnectionProxy
from connection import CursorProxy


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

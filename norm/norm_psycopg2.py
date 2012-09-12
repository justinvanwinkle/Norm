from __future__ import unicode_literals

from psycopg2.extensions import AsIs

from norm import SELECT
from norm import INSERT
from norm import UPDATE
from norm import DELETE
from connection import ConnectionFactory
from connection import ConnectionProxy
from connection import CursorProxy


class PG_INSERT(INSERT):
    defaultdefault = AsIs('DEFAULT')


class PG_SELECT(SELECT):
    pass


class PG_UPDATE(UPDATE):
    pass


class PG_DELETE(DELETE):
    pass


class PG_CursorProxy(CursorProxy):
    pass


class PG_ConnectionProxy(ConnectionProxy):
    cursor_proxy = PG_CursorProxy


class PG_ConnectionFactory(ConnectionFactory):
    connection_proxy = PG_ConnectionProxy

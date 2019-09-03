
from psycopg2.extensions import AsIs

from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy

DEFAULT = AsIs('DEFAULT')


class PG_INSERT(INSERT):
    defaultdefault = DEFAULT


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

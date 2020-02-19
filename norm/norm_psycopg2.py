
from psycopg2.extensions import AsIs

from .norm import SELECT
from .norm import INSERT
from .norm import UPDATE
from .norm import DELETE
from .connection import ConnectionFactory
from .connection import ConnectionProxy
from .connection import CursorProxy

DEFAULT = AsIs('DEFAULT')

PG_NormAsIs = AsIs


class PG_INSERT(INSERT):
    defaultdefault = DEFAULT

    def _is_asis(self, val):
        return False


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


__all__ = [DEFAULT,
           PG_INSERT,
           PG_SELECT,
           PG_UPDATE,
           PG_DELETE,
           PG_CursorProxy,
           PG_ConnectionProxy,
           PG_ConnectionFactory]

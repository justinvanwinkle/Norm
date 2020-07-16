import os

from .norm import SELECT
from .norm import UPDATE
from .norm import INSERT
from .norm import DELETE
from .norm import WITH
from norm.rows import RowsProxy
from norm.connection import ConnectionProxy
from norm.connection import ConnectionFactory
from norm.connection import CursorProxy

enable_logging = False
max_query_log_length = 5000

if int(os.getenv('NORM_LOG_QUERIES', 0)):
    enable_logging = True


__all__ = ['SELECT',
           'UPDATE',
           'DELETE',
           'INSERT',
           'WITH',
           'RowsProxy',
           'ConnectionProxy',
           'ConnectionFactory',
           'CursorProxy']

from time import monotonic
import sys

import norm
from norm.rows import RowsProxy


def _to_query_binds(q, params):
    if params is None:
        params = {}
    if not isinstance(q, str):
        return q.query, q.binds
    else:
        return q, params


class CursorProxy(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    @property
    def column_names(self):
        if self.description is None:
            return
        return [d[0] for d in self.description]

    def execute(self, query, params=None):
        sql_query, sql_binds = _to_query_binds(query, params)
        start = monotonic()
        if sql_binds:
            res = self.cursor.execute(sql_query, sql_binds)
        else:
            res = self.cursor.execute(sql_query)
        end = monotonic()

        if norm.enable_logging:
            try:
                loggable_query = self._query_to_log(
                    query, sql_query, sql_binds)
                loggable_query = loggable_query[:norm.max_query_log_length]
                print(
                    f'\nQuery took {end-start:.2f} seconds:\n'
                    f'{loggable_query}\n\n',
                    file=sys.stderr)
            except Exception:
                pass
        return res

    def _query_to_log(self, query, sql_query, params):
        if hasattr(query, '_loggable_query'):
            loggable_query = query._loggable_query
        else:
            loggable_query = sql_query
        return loggable_query

    def run_query(self, query, params=None):
        self.execute(query, params)
        return self.fetchall()

    def run_queryone(self, query, params=None):
        self.execute(query, params)
        result = self.fetchone()
        return result

    def fetchall(self):
        return RowsProxy(self.cursor.fetchall(), self.column_names)

    def fetchmany(self, count):
        return RowsProxy(self.cursor.fetchmany(count), self.column_names)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return row
        return dict(zip(self.column_names, row))

    def __iter__(self):
        for row in self.cursor:
            yield dict(zip(self.column_names, row))


class ConnectionProxy(object):
    cursor_proxy = CursorProxy

    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def cursor(self, *args, **kw):
        return self.cursor_proxy(self.conn.cursor(*args, **kw))

    def execute(self, query, params=None):
        cur = self.cursor()
        try:
            cur.execute(query, params)
        finally:
            cur.close()

    def run_query(self, query, params=None):
        cur = self.cursor()
        try:
            return cur.run_query(query, params)
        finally:
            cur.close()

    def run_queryone(self, query, params=None):
        cur = self.cursor()
        try:
            return cur.run_queryone(query, params)
        finally:
            cur.close()


class ConnectionFactory(object):
    connection_proxy = ConnectionProxy

    def __init__(self, connection_maker):
        self.connection_maker = connection_maker

    def __call__(self):
        start = monotonic()
        conn = self.connection_proxy(self.connection_maker())
        end = monotonic()

        try:
            if norm.enable_logging:
                print(f'\nConnecting to db with {self.connection_maker}'
                      f' took {end-start:.2f} seconds',
                      file=sys.stderr)
        except Exception:
            pass

        return conn


__all__ = [CursorProxy,
           ConnectionProxy,
           ConnectionFactory]

from __future__ import unicode_literals

from rows import RowsProxy


class ConnectionFactory(object):
    def __init__(self, connection_maker):
        # Closure to prevent connection_maker being called with 'self'
        def _conn_maker(self):
            return connection_maker()
        self.connection_maker = _conn_maker

    def __call__(self):
        return ConnectionProxy(self.connection_maker())


class ConnectionProxy(object):
    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def cursor(self, *args, **kw):
        return CursorProxy(self.conn.cursor(*args, **kw))

    def run_query(self, q):
        cur = self.cursor()
        cur.execute(q.query, q.binds)
        return q.fetchall()

    def run_queryone(self, q):
        cur = self.cursor()
        cur.execute(q.query, q.binds)
        return q.fetchone()


class CursorProxy(object):
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def column_names(self):
        return [d.name for d in self.description]

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def fetchall(self):
        return RowsProxy(self.cursor.fetchall(), self.column_names)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return row
        return dict(zip(self.column_names, row))

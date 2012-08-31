from __future__ import unicode_literals

from rows import RowsProxy


class ConnectionFactory(object):
    def __init__(self, connection_maker):
        self.connection_maker = connection_maker

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
        try:
            cur.execute(q.query, q.binds)
            return cur.fetchall()
        except:
            raise
        finally:
            cur.close()

    def run_queryone(self, q, scalar=False):
        cur = self.cursor()
        try:
            cur.execute(q.query, q.binds)
            result = cur.fetchone()
            if result and scalar:
                return result[0]
            return result
        finally:
            cur.close()


class CursorProxy(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    @property
    def column_names(self):
        return [d[0] for d in self.description]

    def fetchall(self):
        return RowsProxy(self.cursor.fetchall(), self.column_names)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return row
        return dict(zip(self.column_names, row))

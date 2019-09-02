from norm.rows import RowsProxy


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
        if not isinstance(query, str):
            return self.cursor.execute(query.query, query.binds)
        return self.cursor.execute(query, params)

    def fetchall(self):
        return RowsProxy(self.cursor.fetchall(), self.column_names)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return row
        return dict(zip(self.column_names, row))


class ConnectionProxy(object):
    cursor_proxy = CursorProxy

    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def cursor(self, *args, **kw):
        return self.cursor_proxy(self.conn.cursor(*args, **kw))

    def run_query(self, q):
        cur = self.cursor()
        try:
            cur.execute(q.query, q.binds)
            return cur.fetchall()
        except Exception:
            raise
        finally:
            cur.close()

    def run_queryone(self, q):
        cur = self.cursor()
        try:
            cur.execute(q.query, q.binds)
            result = cur.fetchone()
            return result
        finally:
            cur.close()


class ConnectionFactory(object):
    connection_proxy = ConnectionProxy

    def __init__(self, connection_maker):
        self.connection_maker = connection_maker

    def __call__(self):
        return self.connection_proxy(self.connection_maker())

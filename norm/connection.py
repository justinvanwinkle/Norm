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
        return self.cursor.execute(*_to_query_binds(query, params))

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

    def execute(self, q, params=None):
        cur = self.cursor()
        try:
            cur.execute(*_to_query_binds(q, params))
        except Exception:
            raise
        finally:
            cur.close()

    def run_query(self, q, params=None):
        cur = self.cursor()
        try:
            cur.execute(*_to_query_binds(q, params))
            return cur.fetchall()
        except Exception:
            raise
        finally:
            cur.close()

    def run_queryone(self, q, params=None):
        cur = self.cursor()
        try:
            cur.execute(*_to_query_binds(q, params))
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

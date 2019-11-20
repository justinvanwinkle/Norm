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
        if sql_binds:
            res = self.cursor.execute(sql_query, sql_binds)
        else:
            res = self.cursor.execute(sql_query)
        return res

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
        return self.connection_proxy(self.connection_maker())


__all__ = [CursorProxy,
           ConnectionProxy,
           ConnectionFactory]

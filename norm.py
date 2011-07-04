from __future__ import unicode_literals

QUERY_TYPE = b'qt'
COLUMN = b'c'
FROM = b'f'
JOIN = b'j'
WHERE = b'w'
GROUP_BY = b'gb'
LIMIT = b'l'
OFFSET = b'os'
EXTRA = b'ex'

SELECT_QT = b's'
UPDATE_QT = b'u'
INSERT_QT = b'i'

SEP = '\n       '


class BogusQuery(Exception):
    pass


class SQLCompiler(object):
    def __init__(self, chain):
        self.chain = chain

        self.query_type = None
        self.columns = []
        self.from_ = []
        self.where = []
        self.group_by = None
        self.limit = None
        self.offset = None
        self.tail = None

    def set_query_type(self, query_type):
        self.query_type = query_type

    def add_column(self, column_expr):
        self.columns.append(column_expr)

    def compile_columns(self):
        if not self.columns:
            raise BogusQuery('SELECT without any column expression.')
        return 'SELECT ' + (',' + SEP).join(self.columns)

    def add_from(self,
                 table_expr,
                 join,
                 op,
                 criteria):
        if not join:
            if self.from_:
                self.from_[-1] += ','
            self.from_.append(table_expr)
        else:
            self.from_[-1] += '\n  JOIN ' + table_expr
            if op is not None:
                self.from_[-1] += '\n       ' + op + ' ' + criteria

    def compile_from(self):
        return '  FROM ' + SEP.join(self.from_)

    def add_where(self, where_expr):
        self.where.append(where_expr)

    def compile_where(self):
        return ' WHERE ' + (' AND' + SEP).join(self.where)

    def compile_having(self):
        pass

    def compile_order_by(self):
        pass

    def compile_limit(self):
        pass

    def compile_offset(self):
        pass

    def compile_extra(self):
        pass

    def compile(self):
        for op, options in self.chain:
            if op == COLUMN:
                self.add_column(*options)
            elif op == WHERE:
                self.add_where(*options)
            elif op == FROM:
                self.add_from(*options)
            elif op == QUERY_TYPE:
                self.set_query_type(*options)
            elif op == GROUP_BY:
                self.add_group_by(*options)
            elif op == LIMIT:
                self.set_limit(*options)
            elif op == EXTRA:
                self.add_extra(*options)
            else:
                raise BogusQuery('There was a fatal error compiling query.')

        if self.query_type == SELECT_QT:
            parts = [
                self.compile_columns(),
                self.compile_from(),
                self.compile_where(),
                self.compile_having(),
                self.compile_order_by(),
                self.compile_limit(),
                self.compile_offset(),
                self.compile_extra()]
        elif self.query_type == UPDATE_QT:
            parts = [
                self.compile_update(),
                self.compile_set(),
                self.compile_from(),
                self.compile_where(),
                self.compile_returning()]

        return '\n'.join(part for part in parts if part is not None) + ';'


class SELECT(object):
    def __init__(self, *args):
        self.parent = None
        self.chain = [(QUERY_TYPE, (SELECT_QT,))]

        for stmt in args:
            self.chain.append((COLUMN, (stmt,)))

        self._query = None

    def child(self):
        s = SELECT()
        s.parent = self
        return s

    def SELECT(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((COLUMN, (stmt,)))
        return s

    def FROM(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((FROM, (stmt, False, None, None)))
        return s

    def JOIN(self, stmt, ON=None, USING=None):
        if ON is not None and USING is not None:
            raise BogusQuery("You can't specify both ON and USING.")
        elif ON is not None:
            op = 'ON'
            criteria = ON
        elif USING is not None:
            op = 'USING'
            criteria = USING
        else:
            raise BogusQuery('No join criteria specified.')

        s = self.child()
        s.chain.append((FROM, (stmt, True, op, criteria)))
        return s

    def WHERE(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((WHERE, (stmt,)))
        return s

    def HAVING(self, *args):
        pass

    def ORDER_BY(self, *args):
        pass

    def LIMIT(self, limit):
        pass

    def OFFSET(self, offset):
        pass

    def EXTRA(self, *args):
        pass

    def build_chain(self):
        parent_chain = self.parent.build_chain() if self.parent else []
        return parent_chain + self.chain

    @property
    def query(self):
        if self._query is None:
            comp = SQLCompiler(self.build_chain())
            self._query = comp.compile()
        return self._query


class UPDATE(object):
    def __init__(self, table):
        self.table = table
        self.parent = None

    def SET(self, *args):
        return self

    @property
    def query(self):
        return ''



"""
[ WITH [ RECURSIVE ] with_query [, ...] ]
SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
    * | expression [ [ AS ] output_name ] [, ...]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY expression [, ...] ]
    [ HAVING condition [, ...] ]
    [ WINDOW window_name AS ( window_definition ) [, ...] ]
    [ { UNION | INTERSECT | EXCEPT } [ ALL ] select ]
    [ ORDER BY expression [ ASC | DESC | USING operator ] \
      [ NULLS { FIRST | LAST } ] [, ...] ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start [ ROW | ROWS ] ]
    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]
    [ FOR { UPDATE | SHARE } [ OF table_name [, ...] ] [ NOWAIT ] [...] ]

where from_item can be one of:

    [ ONLY ] table_name [ * ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    ( select ) [ AS ] alias [ ( column_alias [, ...] ) ]
    with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    function_name ( [ argument [, ...] ] ) [ AS ] alias \
      [ ( column_alias [, ...] | column_definition [, ...] ) ]
    function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )
    from_item [ NATURAL ] join_type from_item [ ON join_condition | USING \
      ( join_column [, ...] ) ]

and with_query is:

    with_query_name [ ( column_name [, ...] ) ] AS ( select )

TABLE { [ ONLY ] table_name [ * ] | with_query_name }
"""




"""
UPDATE [ ONLY ] table [ [ AS ] alias ]
    SET { column = { expression | DEFAULT } |
          ( column [, ...] ) = ( { expression | DEFAULT } [, ...] ) } [, ...]
    [ FROM fromlist ]
    [ WHERE condition | WHERE CURRENT OF cursor_name ]
    [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
"""


"""
INSERT INTO table [ ( column [, ...] ) ]
    { DEFAULT VALUES | VALUES ( { expression | DEFAULT } \
      [, ...] ) [, ...] | query }
    [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
"""


class INSERT(object):
    def __init__(self, table):
        self.table = table
        self.binds = {}

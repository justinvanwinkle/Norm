from __future__ import unicode_literals

QUERY_TYPE = b'qt'
COLUMN = b'c'
FROM = b'f'
WHERE = b'w'
HAVING = b'h'
GROUP_BY = b'gb'
ORDER_BY = b'ob'
LIMIT = b'l'
OFFSET = b'os'
EXTRA = b'ex'
TABLE= b't'

SELECT_QT = b's'
UPDATE_QT = b'u'
INSERT_QT = b'i'

SEP = '\n       '


class BogusQuery(Exception):
    pass


class SQLCompiler(object):
    def __init__(self, chain, query_type):
        self.chain = chain
        self.query_type = query_type
        self.table = None
        self.columns = []
        self.from_ = []
        self.where = []
        self.having = None
        self.group_by = None
        self.order_by = None
        self.limit = None
        self.offset = None
        self.extra = []

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

    def set_having(self, having_expr):
        self.having = having_expr

    def compile_having(self):
        if self.having is None:
            return None
        return 'HAVING ' + self.having

    def set_group_by(self, group_by_expr):
        self.group_by = group_by_expr

    def compile_group_by(self):
        if self.group_by is None:
            return None
        return 'GROUP BY ' + self.group_by

    def set_order_by(self, order_by_expr):
        self.order_by = order_by_expr

    def compile_order_by(self):
        if self.order_by is None:
            return None
        return 'ORDER BY ' + self.order_by

    def set_limit(self, limit_expr):
        self.limit = limit_expr

    def compile_limit(self):
        if self.limit is None:
            return None
        return ' LIMIT ' + self.limit

    def set_offset(self, offset_expr):
        self.offset = offset_expr

    def compile_offset(self):
        if self.offset is None:
            return None
        return 'OFFSET ' + self.offset

    def add_extra(self, extra_expr):
        self.extra.append(extra_expr)

    def compile_extra(self):
        if not self.extra:
            return None
        return '\n'.join(self.extra) if self.extra is not None else None

    def set_table(self, table_expr):
        self.table = table_expr

    def compile_table(self):
        return 'UPDATE ' + self.table

    def compile(self):
        for op, options in self.chain:
            if op == COLUMN:
                self.add_column(*options)
            elif op == WHERE:
                self.add_where(*options)
            elif op == FROM:
                self.add_from(*options)
            elif op == TABLE:
                self.set_table(*options)
            elif op == GROUP_BY:
                self.set_group_by(*options)
            elif op == ORDER_BY:
                self.set_order_by(*options)
            elif op == LIMIT:
                self.set_limit(*options)
            elif op == OFFSET:
                self.set_offset(*options)
            elif op == HAVING:
                self.set_having(*options)
            elif op == EXTRA:
                self.add_extra(*options)
            else:
                raise BogusQuery('There was a fatal error compiling query.')

        if self.query_type == SELECT_QT:
            parts = [
                self.compile_columns(),
                self.compile_from(),
                self.compile_where(),
                self.compile_group_by(),
                self.compile_having(),
                self.compile_order_by(),
                self.compile_limit(),
                self.compile_offset(),
                self.compile_extra()]
        elif self.query_type == UPDATE_QT:
            parts = [
                self.compile_table(),
                self.compile_set(),
                self.compile_from(),
                self.compile_where(),
                self.compile_returning()]

        return '\n'.join(part for part in parts if part is not None) + ';'


class Query(object):
    query_type = None

    def __init__(self):
        self.parent = None
        self.chain = []
        self._binds = {}
        self._query = None

    @property
    def binds(self):
        binds = {}
        if self.parent is not None:
            binds.update(self.parent.binds)
        binds.update(self._binds)

        return binds

    def bind(self, **binds):
        s = self.child()
        s._binds.update(binds)
        return s

    def child(self):
        s = self.__class__()
        s.parent = self
        return s

    def build_chain(self):
        parent_chain = self.parent.build_chain() if self.parent else []
        return parent_chain + self.chain

    @property
    def query(self):
        if self._query is None:
            comp = SQLCompiler(self.build_chain(), self.query_type)
            self._query = comp.compile()
        return self._query


class _SELECT_UPDATE(Query):
    def WHERE(self, *args, **kw):
        # TODO: this is an injection waiting to happen.
        s = self.child()
        for stmt in args:
            s.chain.append((WHERE, (stmt,)))
        for column_name, value in kw.iteritems():
            bind_val_name = 'norm_gen_bind_%s' % len(self.binds)
            self._binds[bind_val_name] = value
            expr = unicode(column_name) + ' = %(' + bind_val_name + ')s'
            s.chain.append((WHERE, (expr,)))
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


class SELECT(_SELECT_UPDATE):
    query_type = SELECT_QT

    def __init__(self, *args):
        super(SELECT, self).__init__()

        for stmt in args:
            self.chain.append((COLUMN, (stmt,)))

    def SELECT(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((COLUMN, (stmt,)))
        return s

    def HAVING(self, stmt):
        s = self.child()
        s.chain.append((HAVING, (stmt,)))
        return s

    def ORDER_BY(self, stmt):
        s = self.child()
        s.chain.append((ORDER_BY, (stmt,)))
        return s

    def GROUP_BY(self, stmt):
        s = self.child()
        s.chain.append((GROUP_BY, (stmt,)))
        return s

    def LIMIT(self, stmt):
        if isinstance(stmt, (int, long)):
            stmt = unicode(stmt)
        s = self.child()
        s.chain.append((LIMIT, (stmt,)))
        return s

    def OFFSET(self, stmt):
        if isinstance(stmt, (int, long)):
            stmt = unicode(stmt)
        s = self.child()
        s.chain.append((OFFSET, (stmt,)))
        return s


class UPDATE(_SELECT_UPDATE):
    query_type = UPDATE_QT

    def __init__(self, table):
        super(UPDATE, self).__init__()

        self.chain.append((TABLE, (table,)))

    def SET(self, *args):
        return self

    def EXTRA(self, *args):
        pass

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

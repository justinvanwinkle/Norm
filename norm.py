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
TABLE = b't'
SET = b's'

SELECT_QT = b's'
UPDATE_QT = b'u'
INSERT_QT = b'i'

SEP = '\n       '
COLUMN_SEP = ',' + SEP


class BogusQuery(Exception):
    pass


def compile(chain, query_type):
    table = None
    columns = []
    from_ = []
    where = []
    having = None
    group_by = None
    order_by = None
    limit = None
    offset = None
    set_ = []
    extra = []

    for op, option in chain:
        if op == COLUMN:
            columns.append(option)
        elif op == WHERE:
            where.append(option)
        elif op == FROM:
            expr, join, op, criteria = option
            if not join:
                if from_:
                    from_[-1] += ','
                from_.append(expr)
            else:
                from_[-1] += '\n  JOIN ' + expr
                if op is not None:
                    from_[-1] += '\n       ' + op + ' ' + criteria
        elif op == TABLE:
            table = option
        elif op == SET:
            set_.append(option)
        elif op == GROUP_BY:
            group_by = option
        elif op == ORDER_BY:
            order_by = option
        elif op == LIMIT:
            limit = option
        elif op == OFFSET:
            offset = option
        elif op == HAVING:
            having = option
        elif op == EXTRA:
            extra.append(option)
        else:
            raise BogusQuery('There was a fatal error compiling query.')

    query = ''
    if query_type == SELECT_QT:
        query += 'SELECT ' + COLUMN_SEP.join(columns)
        if from_:
            query += '\n  FROM ' + SEP.join(from_)
        if where:
            query += '\n WHERE ' + (' AND' + SEP).join(where)
        if group_by is not None:
            query += '\nGROUP BY ' + group_by
        if having is not None:
            query += '\nHAVING ' + having
        if order_by is not None:
            query += '\nORDER BY ' + order_by
        if limit is not None:
            query += '\n LIMIT ' + limit
        if offset is not None:
            query += '\nOFFSET ' + offset
        if extra:
            query += '\n'.join(extra)
    elif query_type == UPDATE_QT:
        query += 'UPDATE ' + table
        if set_:
            query += '\n   SET ' + (',' + SEP).join(set_)
        if from_:
            query += '\n  FROM ' + SEP.join(from_)
        if where:
            query += '\n WHERE ' + (' AND' + SEP).join(where)
        if extra:
            query += '\n'.join(extra)

    query += ';'
    return query


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
            self._query = compile(self.build_chain(), self.query_type)
        return self._query


class _SELECT_UPDATE(Query):
    def WHERE(self, *args, **kw):
        # TODO: handle OR
        s = self.child()
        for stmt in args:
            s.chain.append((WHERE, stmt))
        for column_name, value in kw.iteritems():
            column_name = unicode(column_name)
            bind_val_name = '%s_bind_%s' % (column_name, len(self.binds))
            self._binds[bind_val_name] = value
            expr = column_name + ' = %(' + bind_val_name + ')s'
            s.chain.append((WHERE, expr))
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
        _SELECT_UPDATE.__init__(self)

        for stmt in args:
            self.chain.append((COLUMN, stmt))

    def SELECT(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((COLUMN, stmt))
        return s

    def HAVING(self, stmt):
        s = self.child()
        s.chain.append((HAVING, stmt))
        return s

    def ORDER_BY(self, stmt):
        s = self.child()
        s.chain.append((ORDER_BY, stmt))
        return s

    def GROUP_BY(self, stmt):
        s = self.child()
        s.chain.append((GROUP_BY, stmt))
        return s

    def LIMIT(self, stmt):
        if isinstance(stmt, (int, long)):
            stmt = unicode(stmt)
        s = self.child()
        s.chain.append((LIMIT, stmt))
        return s

    def OFFSET(self, stmt):
        if isinstance(stmt, (int, long)):
            stmt = unicode(stmt)
        s = self.child()
        s.chain.append((OFFSET, stmt))
        return s


class UPDATE(_SELECT_UPDATE):
    query_type = UPDATE_QT

    def __init__(self, table=None):
        super(UPDATE, self).__init__()

        if table is not None:
            self.chain.append((TABLE, table))

    def SET(self, *args, **kw):
        s = self.child()
        for stmt in args:
            self.chain.append((SET, stmt))

        for column_name, value in kw.iteritems():
            bind_name = column_name + '_bind'
            self._binds[bind_name] = value
            expr = unicode(column_name) + ' = %(' + bind_name + ')s'
            s.chain.append((SET, expr))
        return s

    def RETURNING(self, *args):
        pass

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

from .cached_property import cached_property

QUERY_TYPE = b'qt'
COLUMN = b'c'
FROM = b'f'
WHERE = b'w'
HAVING = b'h'
GROUP_BY = b'gb'
ORDER_BY = b'ob'
TOP = b'top'
LIMIT = b'l'
OFFSET = b'os'
EXTRA = b'ex'
TABLE = b't'
SET = b's'
RETURNING = b'r'
DISTINCT_ON = b'do'
JOIN_LATERAL = b'jl'

SELECT_QT = b's'
UPDATE_QT = b'u'
DELETE_QT = b'd'
INSERT_QT = b'i'

SEP = '\n       '
WHERE_SEP = ' AND\n       '
GROUP_BY_SEP = ',\n         '
ORDER_BY_SEP = ',\n         '
HAVING_SEP = ' AND\n       '
COLUMN_SEP = ',' + SEP
INSERT_COLUMNS_SEP = ',\n   '
INSERT_VALUES_SEP = ',\n          '


class NormAsIs:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f'AsIs({repr(self.value)})'

    def __eq__(self, o):
        try:
            if o.value == self.value:
                return True
            return False
        except AttributeError:
            return False

    @classmethod
    def isasis(cls, o):
        return isinstance(o, cls)


class BogusQuery(Exception):
    pass


def indent_string(s, indent=0, skip_first=False):
    if not indent:
        return s
    lines = s.splitlines()
    if skip_first:
        rest = lines[1:]
        indented = lines[0]
        if rest:
            indented += '\n' + '\n'.join(' ' * indent + line
                                         for line in lines[1:])
        return indented
    return '\n'.join(' ' * indent + line for line in lines)


def compile(chain, query_type):
    table = None
    distinct_on = []
    columns = []
    from_ = []
    where = []
    having = []
    group_by = []
    order_by = []
    top = None
    limit = None
    offset = None
    set_ = []
    extra = []
    returning = []

    for op, option in chain:
        if op == DISTINCT_ON:
            distinct_on.append(option)
        elif op == COLUMN:
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
                from_[-1] += '\n  ' + join + ' ' + expr
                if op is not None:
                    from_[-1] += SEP + op + ' ' + criteria
        elif op == TABLE:
            table = option
        elif op == SET:
            set_.append(option)
        elif op == GROUP_BY:
            group_by.append(option)
        elif op == ORDER_BY:
            order_by.append(option)
        elif op == TOP:
            top = option
        elif op == LIMIT:
            limit = option
        elif op == OFFSET:
            offset = option
        elif op == HAVING:
            having.append(option)
        elif op == EXTRA:
            extra.append(option)
        elif op == RETURNING:
            returning.append(option)
        else:
            raise BogusQuery('There was a fatal error compiling query.')

    query = ''
    if query_type == SELECT_QT:
        query += 'SELECT '
        if top is not None:
            query += 'TOP ' + top + SEP
        if distinct_on:
            query += 'DISTINCT ON (' + ', '.join(distinct_on) + ')'
            query += SEP

        query += COLUMN_SEP.join(columns)

        if from_:
            query += '\n  FROM ' + SEP.join(from_)
        if where:
            query += '\n WHERE ' + indent_string(' AND\n'.join(where), 7, True)
        if group_by:
            query += '\nGROUP BY ' + GROUP_BY_SEP.join(group_by)
        if having:
            query += '\nHAVING ' + HAVING_SEP.join(having)
        if order_by:
            query += '\nORDER BY ' + ORDER_BY_SEP.join(order_by)
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
            query += '\n WHERE ' + WHERE_SEP.join(where)
        if extra:
            query += '\n'.join(extra)
        if returning:
            query += '\nRETURNING ' + ', '.join(returning)
    elif query_type == DELETE_QT:
        query += 'DELETE FROM ' + table
        if from_:
            query += '\n  FROM ' + SEP.join(from_)
        if where:
            query += '\n WHERE ' + WHERE_SEP.join(where)
        if returning:
            query += '\nRETURNING ' + ', '.join(returning)

    query += ';'
    return query


class Query:
    query_type = None
    bind_prefix = '%('
    bind_postfix = ')s'

    def __init__(self):
        self.parent = None
        self.chain = []
        self._binds = []
        self._query = None

    @classmethod
    def clean_bind_name(cls, s):
        s = s.replace('.', '___')
        return s

    @classmethod
    def bnd(cls, s):
        s = cls.clean_bind_name(s)
        return "%s%s%s" % (cls.bind_prefix, s, cls.bind_postfix)

    @property
    def bind_len(self):
        total = 0
        if self.parent is not None:
            total += self.parent.bind_len
        total += len(self._binds)
        return total

    @property
    def bind_items(self):
        if self.parent is not None:
            yield from self.parent.bind_items
        if self._binds:
            yield from self._binds

    @property
    def binds(self):
        operative_binds = {}
        for key, value in self.bind_items:
            if NormAsIs.isasis(value):
                operative_binds.pop(key, None)
                continue
            operative_binds[key] = value
        return operative_binds

    def bind(self, **binds):
        s = self.child()
        final_binds = []
        for name, value in binds.items():
            name = self.clean_bind_name(name)
            final_binds.append((name, value))
        s._binds = final_binds
        return s

    def child(self):
        s = self.__class__()
        s.parent = self
        return s

    def build_chain(self):
        if self.parent is not None:
            chain = self.parent.build_chain()
        else:
            chain = []
        return chain + self.chain

    @property
    def query(self):
        if self._query is None:
            self._query = compile(self.build_chain(), self.query_type)
        query = self._query

        final_binds = {}
        for key, value in self.bind_items:
            final_binds[key] = value
        for key in sorted(final_binds.keys(), key=len, reverse=True):
            value = final_binds[key]
            if NormAsIs.isasis(value):
                query = query.replace(self.bnd(key), value.value)
        return query

    @property
    def _loggable_query(self):
        query = self.query
        for key, value in self.binds.items():
            query = query.replace(self.bnd(key), repr(value))
        return query

    def _merge_subquery(self, subquery, indent=0):
        if isinstance(subquery, str):
            return subquery

        try:
            query = subquery.query.rstrip(';')
            for pair in subquery.bind_items:
                self._binds.append(pair)
            return indent_string(query, indent)

        except AttributeError:
            raise BogusQuery("don't know how to handle this")


class _SELECT_UPDATE(Query):
    def WHERE(self, *args, **kw):
        # TODO: handle OR
        s = self.child()
        for stmt in args:
            clause = s._merge_subquery(stmt)
            s.chain.append((WHERE, clause))
        for column_name, value in kw.items():
            bind_val_name = '%s_bind_%s' % (
                self.clean_bind_name(column_name), s.bind_len)
            s._binds.append((bind_val_name, value))
            expr = column_name + ' = ' + self.bnd(bind_val_name)
            s.chain.append((WHERE, expr))
        return s

    def FROM(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((FROM, (stmt, False, None, None)))
        return s

    def JOIN(self, stmt, ON=None, USING=None, outer=False):
        if outer:
            keyword = 'LEFT JOIN'
        else:
            keyword = 'JOIN'
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
        s.chain.append((FROM, (stmt, keyword, op, criteria)))
        return s

    def LEFTJOIN(self, *args, **kw):
        return self.JOIN(*args, outer=True, **kw)

    def RETURNING(self, *args):
        s = self.child()
        for arg in args:
            self.chain.append((RETURNING, arg))
        return s


class SELECT(_SELECT_UPDATE):
    query_type = SELECT_QT

    def __init__(self, *args):
        _SELECT_UPDATE.__init__(self)

        for stmt in args:
            try:
                stmt = str(int(stmt))
            except ValueError:
                pass

            self.chain.append((COLUMN, stmt))

    def SELECT(self, *args):
        s = self.child()
        for stmt in args:
            s.chain.append((COLUMN, stmt))
        return s

    def HAVING(self, *args):
        s = self.child()
        for arg in args:
            s.chain.append((HAVING, arg))
        return s

    def ORDER_BY(self, *args):
        s = self.child()
        for arg in args:
            s.chain.append((ORDER_BY, arg))
        return s

    def GROUP_BY(self, *args):
        s = self.child()
        for arg in args:
            s.chain.append((GROUP_BY, arg))
        return s

    def DISTINCT_ON(self, *args):
        s = self.child()
        for arg in args:
            s.chain.append((DISTINCT_ON, arg))
        return s

    def TOP(self, stmt):
        if isinstance(stmt, int):
            stmt = str(stmt)
        s = self.child()
        s.chain.append((TOP, stmt))
        return s

    def LIMIT(self, stmt):
        if isinstance(stmt, int):
            stmt = str(stmt)
        s = self.child()
        s.chain.append((LIMIT, stmt))
        return s

    def OFFSET(self, stmt):
        if isinstance(stmt, int):
            stmt = str(stmt)
        s = self.child()
        s.chain.append((OFFSET, stmt))
        return s


class EXISTS(SELECT):
    @property
    def query(self):
        query = 'EXISTS (\n'
        query += indent_string(super().query.rstrip(';'), 2) + ')'
        return query


class NOT_EXISTS(EXISTS):
    @property
    def query(self):
        return 'NOT ' + super().query


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

        for column_name, value in kw.items():
            clean_column_name = self.clean_bind_name(column_name)
            bind_name = clean_column_name + '_bind'
            s._binds.append((bind_name, value))
            expr = str(column_name) + ' = ' + self.bnd(bind_name)
            s.chain.append((SET, expr))
        return s

    def EXTRA(self, *args):
        pass


class DELETE(_SELECT_UPDATE):
    query_type = DELETE_QT

    def __init__(self, table=None):
        super(DELETE, self).__init__()

        if table is not None:
            self.chain.append((TABLE, table))


class _default:
    pass


class INSERT:
    bind_prefix = '%('
    bind_postfix = ')s'
    defaultdefault = None

    def __init__(self,
                 table,
                 data=None,
                 columns=None,
                 statement=None,
                 default=_default,
                 on_conflict=None,
                 returning=None):
        self.table = table
        self.data = data
        self._columns = columns
        self.statement = statement
        if default is _default:
            self.default = self.defaultdefault
        else:
            self.default = default
        self.on_conflict = on_conflict
        self.returning = returning

    @classmethod
    def bnd(cls, s):
        return "%s%s%s" % (cls.bind_prefix, s, cls.bind_postfix)

    @property
    def binds(self):
        binds = {}
        if self.statement:
            binds.update(self.statement.binds)
        if self.data is None:
            return binds

        if self.multi_data:
            data = self.data
        else:
            data = [self.data]

        for index, d in enumerate(data):
            for col_name in self.columns:
                key = f'{col_name}_{index}'

                if col_name in d:
                    if not NormAsIs.isasis(d[col_name]):
                        binds[key] = d[col_name]
                else:
                    if not NormAsIs.isasis(self.default):
                        binds[key] = self.default

        return binds

    @cached_property
    def multi_data(self):
        if hasattr(self.data, 'keys'):
            return False
        return True

    @cached_property
    def columns(self):
        if self._columns is None:
            if self.data is None:
                return None
            if not self.multi_data:
                return list(sorted([key for key in self.data]))
            else:
                columns = set()
                for d in self.data:
                    columns |= set(d)
                self._columns = list(sorted(columns))
        return self._columns

    @property
    def query(self):
        if self.multi_data:
            return self._query(self.data)
        else:
            return self._query([self.data])

    def _bind_param_name(self, col_name, index):
        return f'{self.bind_prefix}{col_name}_{index}{self.bind_postfix}'

    def _query(self, data):
        q = 'INSERT INTO %s ' % self.table

        if self.columns:
            q += '('
            q += ', '.join(col_name for col_name in self.columns)
            q += ')'

        if self.statement:
            q += '\n' + indent_string(self.statement.query[:-1], 2)
        elif self.data is None:
            q += 'DEFAULT VALUES'
        else:
            q += '\n  VALUES\n'
            for index, d in enumerate(data):
                if index > 0:
                    q += ',\n'

                q += '('
                last_col_ix = len(self.columns) - 1
                for ix, col_name in enumerate(self.columns):
                    col_val = d.get(col_name, self.default)
                    if NormAsIs.isasis(col_val):
                        q += col_val.value
                    else:
                        q += self._bind_param_name(col_name, index)
                    if last_col_ix != ix:
                        q += ', '

                q += ')'
        if self.on_conflict:
            q += f'\nON CONFLICT {self.on_conflict}'

        if self.returning:
            if isinstance(self.returning, str):
                returning = [self.returning]
            else:
                returning = self.returning
            q += '\nRETURNING ' + ', '.join(returning)

        q += ';'

        return q


class WITH(Query):
    def __init__(self, **kw):
        Query.__init__(self)
        self.tables = kw
        self.primary = None

    def __call__(self, primary):
        self.primary = primary
        return self

    @property
    def query(self):
        parts = []
        for name, query in self.tables.items():
            query_part = query.query[:-1]  # TODO: HACK!
            query_section = indent_string(f'({query_part})\n', 8)[1:]
            parts.append(f'{name} AS\n{query_section}')
        return ('WITH ' +
                ',\n     '.join(parts) +
                '\n\n' +
                self.primary.query)

    @property
    def binds(self):
        d = dict()
        for query in self.tables.values():
            d.update(query.binds)
        if self.primary:
            d.update(self.primary.binds)
        return d


__all__ = [Query,
           SELECT,
           UPDATE,
           DELETE,
           INSERT,
           WITH,
           BogusQuery]

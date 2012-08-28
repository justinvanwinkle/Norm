from __future__ import unicode_literals

from itertools import groupby


class RowsProxy(object):
    def __init__(self, rows):
        self.rows = rows

    def __call__(self, *args):
        def key_func(row):
            if len(args) == 1:
                return row.get(args[0])
            l = []
            for key in args:
                l.append(row.get(key))
            return tuple(l)

        for key, sub_rows_iter in groupby(self.rows, key=key_func):
            yield key, RowsProxy(sub_rows_iter)

    def __iter__(self):
        for row in self.rows:
            yield row

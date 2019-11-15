from itertools import groupby


class RowsProxy(object):
    def __init__(self, rows, column_names=None):
        self.rows = rows
        self.column_names = column_names

    def __len__(self):
        return len(self.rows)

    def __call__(self, *args):
        def key_func(row):
            if len(args) == 1:
                return row.get(args[0])
            cols = []
            for key in args:
                cols.append(row.get(key))
            return tuple(cols)

        for key, sub_rows_iter in groupby(self, key=key_func):
            yield key, RowsProxy(sub_rows_iter)

    def __iter__(self):
        column_names = self.column_names
        for row in self.rows:
            if column_names is None or hasattr(row, 'get'):
                yield row
            else:
                yield dict(zip(column_names, row))


__all__ = [RowsProxy]


from norm.norm import SELECT
from norm.norm import INSERT
from norm.norm import UPDATE
from norm.norm import DELETE
from norm.norm import _default
from norm.norm import NormAsIs
from norm.connection import ConnectionFactory
from norm.connection import ConnectionProxy
from norm.connection import CursorProxy


PYMSSQL_AsIs = NormAsIs
PYMSSQL_DEFAULT = NormAsIs('DEFAULT')

_encrypt_statement = (
    "EncryptByKey(Key_GUID('{key_name}'), CAST({bind} AS VARCHAR(4000)))")


class PymssqlLoggingMixin:
    @property
    def _loggable_query(self):
        from _mssql import quote_data
        query = self.query
        for key, value in self.binds.items():
            quoted_data = quote_data(value)
            if isinstance(quoted_data, bytes):
                quoted_data = quoted_data.decode('utf-8')
            query = query.replace(self.bnd(key), quoted_data)
        return query


class PYMSSQL_INSERT(INSERT):
    defaultdefault = PYMSSQL_DEFAULT

    def __init__(self,
                 table,
                 data=None,
                 columns=None,
                 statement=None,
                 default=_default,
                 on_conflict=None,
                 returning=None,
                 encrypted_columns=None,
                 encryption_key=None):
        super().__init__(table,
                         data=data,
                         columns=columns,
                         statement=statement,
                         default=default,
                         on_conflict=on_conflict,
                         returning=returning)

        if encrypted_columns is None:
            self.encrypted_columns = set()
        else:
            self.encrypted_columns = set(encrypted_columns)
        self.encryption_key = encryption_key

        if self.encrypted_columns and not self.encryption_key:
            raise RuntimeError('You must supply an encryption key name when'
                               ' using encrypted columns')

    def _bind_param_name(self, col_name, index):
        bind = f'{self.bind_prefix}{col_name}_{index}{self.bind_postfix}'
        if col_name in self.encrypted_columns:
            return _encrypt_statement.format(key_name=self.encryption_key,
                                             bind=bind)
        return bind


class PYMSSQL_SELECT(PymssqlLoggingMixin, SELECT):
    pass


class PYMSSQL_UPDATE(PymssqlLoggingMixin, UPDATE):
    pass


class PYMSSQL_DELETE(PymssqlLoggingMixin, DELETE):
    pass


class PYMSSQL_CursorProxy(CursorProxy):
    pass


class PYMSSQL_ConnectionProxy(ConnectionProxy):
    cursor_proxy = PYMSSQL_CursorProxy


class PYMSSQL_ConnectionFactory(ConnectionFactory):
    connection_proxy = PYMSSQL_ConnectionProxy


__all__ = [PYMSSQL_INSERT,
           PYMSSQL_SELECT,
           PYMSSQL_UPDATE,
           PYMSSQL_DELETE,
           PYMSSQL_CursorProxy,
           PYMSSQL_ConnectionProxy,
           PYMSSQL_ConnectionFactory]

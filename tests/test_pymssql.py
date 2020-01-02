from pytest import raises

from norm.norm_pymssql import PYMSSQL_INSERT as INSERT

rows = [{'test': 'good', 'bub': 5}]


def test_insert():
    i = INSERT('my_table', rows)

    assert i.query == (
        'INSERT INTO my_table (bub, test) VALUES (%(bub_0)s, %(test_0)s);')


def test_encrypted_insert():
    i = INSERT('my_table',
               rows,
               encrypted_columns=['bub'],
               encryption_key='fookey')
    assert i.query == (
        'INSERT INTO my_table (bub, test)'
        " VALUES (EncryptByKey(Key_GUID('fookey'),"
        ' CAST(%(bub_0)s AS VARCHAR(4000)), %(test_0)s);')


def test_error_on_no_key():
    with raises(RuntimeError):
        INSERT('my_table',
               rows,
               encrypted_columns=['bub'])

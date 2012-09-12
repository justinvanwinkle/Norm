from __future__ import unicode_literals

import sqlite3

from norm.norm_sqlite3 import SQLI_ConnectionFactory as ConnectionFactory
from norm.norm_sqlite3 import SQLI_SELECT as SELECT
from norm.norm_sqlite3 import SQLI_INSERT as INSERT


def conn_maker():
    conn = sqlite3.connect(':memory:')
    conn.execute(
        '''CREATE TABLE users (
               user_id INTEGER PRIMARY KEY AUTOINCREMENT,
               first_name VARCHAR(64)
        )''')
    conn.commit()
    return conn


def test_connection_factory_and_connection_proxy_cursor():
    cf = ConnectionFactory(conn_maker)
    conn = cf()
    # make sure we get a 'connection'
    conn.cursor()


def test_run_query():
    cf = ConnectionFactory(conn_maker)
    conn = cf()

    s = SELECT('user_id').FROM('users')
    user_ids = conn.run_query(s)

    assert list(user_ids) == []

    i = INSERT('users', data={'first_name': 'Justin'})
    conn.run_query(i)
    conn.commit()

    user_ids = conn.run_query(s)
    assert list(user_ids) == [{'user_id': 1}]


def test_runqueryone():
    cf = ConnectionFactory(conn_maker)
    conn = cf()

    s = SELECT('user_id').FROM('users')
    row = conn.run_queryone(s)
    assert row is None

    i = INSERT('users', data={'first_name': 'Justin'})
    conn.run_query(i)
    conn.commit()

    s = SELECT('user_id').FROM('users')
    row = conn.run_queryone(s)
    assert row == {'user_id': 1}

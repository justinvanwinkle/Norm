from __future__ import unicode_literals

import sqlite3

from norm import ConnectionFactory
from norm import SELECT
from norm import INSERT


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

    user_ids = conn.run_query(s)
    assert list(user_ids) == [{'user_id': 1}]

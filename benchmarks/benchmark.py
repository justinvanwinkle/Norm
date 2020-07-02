from time import monotonic
from tempfile import mktemp
import sqlite3

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy import create_engine

from norm.norm_sqlite3 import SQLI_ConnectionFactory
from norm.norm_sqlite3 import SQLI_SELECT as SELECT
from norm.norm_sqlite3 import SQLI_INSERT as INSERT


metadata = MetaData()
users = Table(
    'users', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String))

addresses = Table(
    'addresses', metadata,
    Column('address_id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String, nullable=False))


fake_users = [{'user_id': id, 'name': 'Bob', 'fullname': 'Bob Loblaw'}
              for id in range(10000)]

fake_addresses = [{'user_id': user_id, 'email_address': 'bob@loblaw.com'}
                  for user_id in range(10000)] * 8

setup_users = '''\
CREATE TABLE users (
  user_id INTEGER PRIMARY KEY,
  name VARCHAR(255),
  fullname VARCHAR(255));'''

setup_addresses = '''\
CREATE TABLE addresses (
  address_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users (user_id),
  email_address VARCHAR(255) NOT NULL);'''


def setup_db():
    fn = mktemp()
    conn = sqlite3.connect(fn)
    conn.execute(setup_users)
    conn.execute(setup_addresses)
    conn.commit()
    return fn


def make_db_conn():
    fn = setup_db()
    return sqlite3.connect(fn)


def make_sqla_conn():
    fn = setup_db()
    print(fn)
    engine = create_engine('sqlite:///' + fn)
    return engine.connect()


def norm_conn():
    return SQLI_ConnectionFactory(make_db_conn)()


def sqlalchemy_bench():
    s = (select([users.c.name, users.c.fullname, addresses.c.email_address],
                users.c.id == addresses.c.user_id)
         .where(users.c.id > 1)
         .where(users.c.name.startswith('Justin')))
    return str(s)


def sqlalchemy_insert_bench():
    conn = make_sqla_conn()
    with conn.begin():
        conn.execute(users.insert(), fake_users)
        conn.execute(addresses.insert(), fake_addresses)


def norm_insert_bench():
    conn = norm_conn()
    conn.execute(INSERT('users', fake_users))
    conn.execute(INSERT('addresses', fake_addresses))
    conn.commit()


def norm_bench():
    s = (SELECT('users.name',
                'users.fullname',
                'addresses.email_address')
         .FROM('users')
         .JOIN('addresses', ON='users.id = addresses.user_id')
         .WHERE('users.id > %(id)s').bind(id=1)
         .WHERE("users.name LIKE %(name)s")
         .bind(name='Justin%'))

    return s.query


def raw_bench():
    s = """SELECT users.name,
       users.fullname,
       addresses.email_address
  FROM users
  JOIN addresses
       ON users.id = addresses.user_id
 WHERE users.id > %(id)s AND
       users.name LIKE %(name)s;"""
    return s


def time_it(f, last=None):
    start = monotonic()
    for x in range(10000):
        query = f()

    elapsed = monotonic() - start
    faster = 1
    if last is not None:
        faster = last / elapsed
    print(f'*** Begin {f.__name__}')
    print(f'Elapsed Time: {elapsed:.4f}')
    print(f'Faster than SQLA factor: {faster:.4f}')
    print(query)
    return elapsed


def time_insert(f, last=None):
    start = monotonic()
    f()
    elapsed = monotonic() - start
    faster = 1
    if last is not None:
        faster = last / elapsed
    print(f'*** Begin {f.__name__}')
    print(f'Elapsed Time: {elapsed:.4f}')
    print(f'Faster than SQLA factor: {faster:.4f}')
    return elapsed


def run_benchmark():
    # print('*' * 70)
    # print('*' * 70)
    # sqla_time = time_it(sqlalchemy_bench)
    # norm_time = time_it(norm_bench, sqla_time)
    # time_it(raw_bench, norm_time)
    print('*' * 70)
    print('*' * 70)
    sqla_time = time_insert(sqlalchemy_insert_bench)
    time_insert(norm_insert_bench, sqla_time)


if __name__ == '__main__':
    run_benchmark()

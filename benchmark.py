import time

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import select

from norm import SELECT

metadata = MetaData()
users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String))

addresses = Table(
    'addresses', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String, nullable=False))


def sqlalchemy_bench():
    s = select([users, addresses], users.c.id == addresses.c.user_id)
    return str(s)


def norm_bench():
    s = (SELECT('users.id',
                'users.name',
                'users.fullname',
                'addresses.id',
                'addresses.user_id',
                'addresses.email_address')
         .FROM('users')
         .JOIN('addresses', ON='users.id = addresses.user_id'))

    return s.query


def time_it(f):
    start = time.time()
    for x in xrange(50000):
        f()

    return time.time() - start


def run_benchmark():
    print 'SQLAlchemy', time_it(sqlalchemy_bench)
    print 'Norm', time_it(norm_bench)


if __name__ == '__main__':
    run_benchmark()

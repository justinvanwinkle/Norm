import time

try:
    from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
    from sqlalchemy.sql import select
except ImportError:
    # only need these if you want to run the benchmark
    pass

from norm import SELECT


def sqlalchemy_bench():
    s = (select([users.c.name, users.c.fullname, addresses.c.email_address],
                users.c.id == addresses.c.user_id)
         .where(users.c.id > 1)
         .where(users.c.name.startswith('Justin')))
    return str(s)


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
    start = time.time()
    for x in range(10000):
        query = f()

    elapsed = time.time() - start
    faster = ''
    if last is not None:
        faster = '%.1f' % (last / elapsed)
    print('Begin %s' % f.__name__, '*' * 50)
    print('%s %.4f, %s' % (f.__name__, elapsed, faster))
    print(query)
    return elapsed


def run_benchmark():
    sqla_time = time_it(sqlalchemy_bench)
    norm_time = time_it(norm_bench, sqla_time)
    time_it(raw_bench, norm_time)


if __name__ == '__main__':
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

    run_benchmark()

'''
   This is an example of efficiently combining data between two databases
     using Norm's fast INSERT queries.
'''
from time import monotonic
from tempfile import mktemp
import tracemalloc

import sqlite3
import itertools

from norm.norm_sqlite3 import SQLI_INSERT
from norm.norm_sqlite3 import SQLI_SELECT
from norm.norm_sqlite3 import SQLI_ConnectionFactory

# To try this example you need two databases, which you can set up by
#   modifying the connection functions and the imports above.

_sqlite_source_db_fn = mktemp()
_sqlite_dest_db_fn = mktemp()
_chunk_size = 50


def make_source_db_conn():
    # assumes db exists and can be accessed from a local socket
    return sqlite3.connect(_sqlite_source_db_fn)


def make_dest_db_conn():
    return sqlite3.connect(_sqlite_dest_db_fn)


# setup connection factories

source_db_cf = SQLI_ConnectionFactory(make_source_db_conn)
dest_db_cf = SQLI_ConnectionFactory(make_dest_db_conn)


def chunk_up(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def shuttle_rows(source_cur, dest_cur, table_name, chunk_size=_chunk_size):
    for chunk in chunk_up(source_cur, chunk_size):
        dest_cur.execute(SQLI_INSERT(table_name, chunk))


clean_up_source_table = '''\
DROP TABLE IF EXISTS norm_shuttle_example_source;'''

create_source_table = '''\
CREATE TABLE norm_shuttle_example_source (
  example_id INTEGER PRIMARY KEY AUTOINCREMENT,
  some_data_column TEXT);'''

create_sqlite_table = '''\
CREATE TABLE norm_shuttle_example_dest (
   example_id integer,
   some_data_column TEXT,
   method TEXT);'''


def setup_test_tables():
    # lets set up some test data
    source_conn = source_db_cf()
    source_conn.execute(clean_up_source_table)
    source_conn.execute(create_source_table)
    source_conn.commit()

    dest_conn = dest_db_cf()
    dest_conn.execute(create_sqlite_table)
    dest_conn.commit()

    test_data_insert = SQLI_INSERT(
        'norm_shuttle_example_source',
        [{'some_data_column': str(n)} for n in range(1000)])
    # lets insert it 1000 times so we have 1000 * 1000 == 1M rows
    for batch in range(1, 1001):
        if batch % 100 == 0:
            print(f'Inserting batch {batch}')
        source_conn.execute(test_data_insert)
    source_conn.commit()


def run_example_streaming():
    # lets set up some test data
    source_conn = source_db_cf()
    dest_conn = dest_db_cf()

    # move the data
    source_cur = source_conn.cursor()
    dest_cur = dest_conn.cursor()

    start = monotonic()
    # execute the select so the source_cursor can be read from
    source_cur.execute(SQLI_SELECT('example_id',
                                   'some_data_column',
                                   "'example_streaming' AS method")
                       .FROM('norm_shuttle_example_source'))

    shuttle_rows(source_cur, dest_cur, 'norm_shuttle_example_dest')
    dest_conn.commit()
    end = monotonic()

    row_count = dest_conn.run_queryone(SQLI_SELECT('COUNT(*) AS rows_moved')
                                       .FROM('norm_shuttle_example_dest')
                                       .WHERE(method='example_streaming'))
    print(f'Moved {row_count["rows_moved"]} rows in {end-start:.2f} seconds')


def run_example_all():
    # lets set up some test data
    source_conn = source_db_cf()
    dest_conn = dest_db_cf()

    # move the data
    source_cur = source_conn.cursor()
    dest_cur = dest_conn.cursor()

    start = monotonic()
    # execute the select so the source_cursor can be read from
    source_cur.execute(SQLI_SELECT('example_id',
                                   'some_data_column',
                                   "'example_all' AS method")
                       .FROM('norm_shuttle_example_source'))

    all_rows = source_cur.fetchall()

    # we still have to insert in chunks due to sqlite limits
    for rows in chunk_up(all_rows, _chunk_size):
        i = SQLI_INSERT('norm_shuttle_example_dest', rows)
        dest_cur.execute(i)
    dest_conn.commit()
    end = monotonic()

    row_count = dest_conn.run_queryone(SQLI_SELECT('COUNT(*) AS rows_moved')
                                       .FROM('norm_shuttle_example_dest')
                                       .WHERE(method='example_all'))
    print(f'Moved {row_count["rows_moved"]} rows in {end-start:.2f} seconds')


if __name__ == '__main__':
    setup_test_tables()

    tracemalloc.start()
    run_example_streaming()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Streaming memory usage peak was {peak / 10**6:.2f}MB")

    tracemalloc.start()
    run_example_all()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Fetchall memory usage peak was {peak / 10**6:.2f}MB")

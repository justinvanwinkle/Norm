# norm is like sql


## Query Generation
The primary purpose of Norm is to make it easier to generate SQL.

### The SELECT class
#### Basic queries

```python
In [1]: from norm.norm_sqlite3 import SQLI_SELECT as SELECT

In [2]: s = (SELECT('val')
   ...:      .FROM('foos'))

In [3]: print(s.query)
SELECT val
  FROM foos;

In [4]: s2 = s.WHERE(val = 5)

In [5]: s2 = s2.SELECT('foos_id')

In [6]: print(s2.query)
SELECT val,
       foos_id
  FROM foos
 WHERE val = :val_bind_0;

In [7]: print(s.query)
SELECT val
  FROM foos;

```

Bind parameters can be automatically handled, for example in `.WHERE(val=5)`

```python
In [8]: print(s2.binds)
{'val_bind_0': 5}

```

Using .query and .binds seperately lets you use norm wherever you can execute SQL.  For example, with a SQLAlchemy Session object:

```python
res = Session.execute(s.query, s.binds)
```

#### More powerful query generation
In addition to the simple, static queries above, it is possible to add query clauses.

```python
In [9]: print(s.query)
SELECT val
  FROM foos;

In [10]: s = s.WHERE('val * 2 = 4')

In [11]: print(s.query)
SELECT val
  FROM foos
 WHERE val * 2 = 4;

In [12]: s = s.JOIN('bars', ON='foos.val = bars.bar_id')

In [13]: print(s.query)
SELECT val
  FROM foos
  JOIN bars
       ON foos.val = bars.bar_id
 WHERE val * 2 = 4;
```

Of course you can put it all together:
```python
In [14]: s = (SELECT('val')
    ...:      .FROM('foos')
    ...:      .JOIN('bars', ON='foos.val = bars.bar_id')
    ...:      .WHERE(val=5)
    ...:      .WHERE('val * 2 = 4'))

In [15]: print(s.query)
SELECT val
  FROM foos
  JOIN bars
       ON foos.val = bars.bar_id
 WHERE val = :val_bind_0 AND
       val * 2 = 4;
```

Or you can evolve queries dynamically:

```python
def get_users(cursor, user_ids, only_girls=False, minimum_age=0):
    s = (SELECT('first_name', 'age')
         .FROM('people')
         .WHERE('user_ids IN :user_ids')
         .bind(user_ids=user_ids))
    if only_girls:
        s = s.WHERE(gender='f')
    if minimum_age:
        s = (s.WHERE('age >= :minimum_age')
             .bind(minimum_age=minimum_age))
    return cursor.run_query(s)
```

You can also add `SELECT` statements dynamically.

```python
def get_users(cursor, user_ids, has_dog=False):
    s = (SELECT('p.first_name', 'p.age')
         .FROM('p.people')
         .WHERE('user_ids IN :user_ids')
         .bind(user_ids=user_ids))

    if has_dog:
        s = (s.SELECT('d.dog_name', 'd.breed')
             .JOIN('dogs as d', ON='p.person_id = d.owner_id'))

    return cursor.run_query(s)
```

Calling methods on a query object does not change the object, it returns a new query object.

In other words, query objects are immutable.  This means it is always safe to create a base query and add clauses without modifying it.

```python

_user_query = (SELECT('first_name', 'last_name')
               .FROM('people'))


def get_old_people(conn, min_age=65):
    old_people_query = s.WHERE('age > :min_age').bind(min_age=min_age)
    return conn.run_query(old_people_query)


def get_karls(conn):
    karl_query = s.WHERE(first_name='Karl')
    return conn.run_query(karl_query)
```


### UPDATE, DELETE

UPDATE and DELETE work basically the same as SELECT

```python
In [1]: from norm import UPDATE

In [2]: from norm import SELECT

In [3]: fix_karls = (UPDATE('people')
   ...:              .SET(first_name='Karl')
   ...:              .WHERE(first_name='karl'))

In [4]: print(fix_karls.query)
UPDATE people
   SET first_name = %(first_name_bind)s
 WHERE first_name = %(first_name_bind_1)s;

In [5]: print(fix_karls.binds)
{'first_name_bind': 'Karl', 'first_name_bind_1': 'karl'}

```

```python
In [8]: from norm import DELETE

In [9]: remove_karls = (DELETE('people')
   ...:                 .WHERE(first_name='Karl'))

In [10]: print(remove_karls.query)
DELETE FROM people
 WHERE first_name = %(first_name_bind_0)s;

In [11]: print(remove_karls.binds)
{'first_name_bind_0': 'Karl'}
```

### INSERT
Inserts just take dictionaries and treat them like rows.

All the rows are inserted as one large INSERT statement with many bind parameters.  This means that if your database or library doesn't support large numbers of bind parameters, you may have to break the rows you wish to insert into several batches.

```python
rows = [dict(first_name='bob', last_name='dobs', age=132),
        dict(first_name='bill, last_name='gates, age=67),
        dict(first_name='steve', last_name='jobs', age=60),
        dict(first_name='bob', last_name='jones'),
        dict(first_name='mike', last_name='jones', age=15)]
i = INSERT('people', rows)
conn.execute(i)
```

The behavior for missing keys depends on the database/library norm backend you are using.  For psycopg2/postgres it will fill in mising keys with DEFAULT.  For most databases which do not provide an AsIs DBAPI wrapper, the default default is None (NULL).  This can be overridden:

```python
i = INSERT('people', default=AsIs('DEFAULT'))
```

This should not be used with a value like `5` or something, it is meant to be a way to specify the DEAULT keyword for the library/database you are using.  For psycopg2/postgresql, it will automatically fill in DEFAULT, using http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.AsIs  For inferior databases there may not be a defined way to do this safely.  To allow literal SQL to be included as part of an insert, there is `norm.NormAsIs`.


### WITH (Commont Table Expressions)

For `WITH`, a `WITH` object can be used to wrap other queries into CTE tables.  The final query in the CTE is provided by calling the WITH instance.

```python
In [1]: from norm import WITH

In [2]: from norm import SELECT

In [3]: all_active_players = (SELECT('player_id')
   ...:                       .FROM('players')
   ...:                       .WHERE(status='ACTIVE'))

In [4]: career_runs_scored = (SELECT('player_id',
   ...:                              'SUM(runs_scored) AS total_runs')
   ...:                       .FROM('games')
   ...:                       .GROUP_BY('player_id'))

In [5]: w = WITH(all_active_players=all_active_players,
   ...:          career_runs_scored=career_runs_scored)

In [6]: active_players_total_runs = (SELECT('crs.player_id AS player_id',
   ...:                                     'crs.total_runs AS total_runs')
   ...:                              .FROM('all_active_players aap')
   ...:                              .JOIN('career_runs_scored crs',
   ...:                                    ON='crs.player_id = aap.player_id'))

In [7]: w = w(active_players_total_runs)

In [8]: print(w.query)
WITH all_active_players AS
       (SELECT player_id
          FROM players
         WHERE status = %(status_bind_0)s),
     career_runs_scored AS
       (SELECT player_id,
               SUM(runs_scored) AS total_runs
          FROM games
        GROUP BY player_id)

SELECT crs.player_id AS player_id,
       crs.total_runs AS total_runs
  FROM all_active_players aap
  JOIN career_runs_scored crs
       ON crs.player_id = aap.player_id;

In [9]: outer_w = WITH(active_players_total_runs=w)

In [10]: outer_w = outer_w(SELECT('aptr.player_id')
   ...:                    .FROM('active_players_total_runs aptr')
   ...:                    .WHERE('aptr.total_runs > 500'))

In [11]: print(outer_w.query)
WITH active_players_total_runs AS
       (WITH all_active_players AS
               (SELECT player_id
                  FROM players
                 WHERE status = %(status_bind_0)s),
             career_runs_scored AS
               (SELECT player_id,
                       SUM(runs_scored) AS total_runs
                  FROM games
                GROUP BY player_id)

        SELECT crs.player_id AS player_id,
               crs.total_runs AS total_runs
          FROM all_active_players aap
          JOIN career_runs_scored crs
               ON crs.player_id = aap.player_id)

SELECT aptr.player_id
  FROM active_players_total_runs aptr
 WHERE aptr.total_runs > 500;

# This example is a little contrived, there are obviously
#   better ways to do this query

```

### LIMIT / OFFSET
```python
In [1]: from norm import SELECT

In [2]: s = (SELECT('FirstName as first_name',
   ...:             'LastName as last_name')
   ...:      .FROM('people')
   ...:      .LIMIT(1))

In [3]: print(s.query)
SELECT FirstName as first_name,
       LastName as last_name
  FROM people
 LIMIT 1;

In [5]: s = s.bind(my_limit=5)

In [6]: print(s.query)
SELECT FirstName as first_name,
       LastName as last_name
  FROM people
 LIMIT %(my_limit)s;

In [7]: print(s.binds)
{'my_limit': 5}

```
LIMIT and OFFSET can only appear in a given SQL statement one time.  Rather than being built up like the SELECT columns, WHERE clauses, etc, the Norm query will take the final value or expression provided.

```python
In [2]: s = (SELECT('FirstName as first_name',
   ...:             'LastName as last_name')
   ...:      .FROM('people')
   ...:      .LIMIT('%(my_limit)s'))

In [3]: s = s.LIMIT(250)

In [4]: print(s.query)
SELECT FirstName as first_name,
       LastName as last_name
  FROM people
 LIMIT 250;

In [5]: s = s.OFFSET(10)

In [6]: print(s.query)
SELECT FirstName as first_name,
       LastName as last_name
  FROM people
 LIMIT 250
OFFSET 10;

In [7]: s = s.OFFSET(99)

In [8]: print(s.query)
SELECT FirstName as first_name,
       LastName as last_name
  FROM people
 LIMIT 250
OFFSET 99;
```

#### LIMIT vs. TOP

While many SQL flavors prefer `LIMIT`, MS SQL Server favors `TOP`.

```python
In [1]: from norm.norm_pymssql import PYMSSQL_SELECT as SELECT

In [2]: s = (SELECT('FirstName as first_name',
   ...:             'LastName as last_name')
   ...:      .FROM('people')
   ...:      .TOP(1))

In [3]: print(s.query)
SELECT TOP 1
       FirstName as first_name,
       LastName as last_name
  FROM people;
```

### GROUP BY, ORDER BY, HAVING, RETURNING
These methods work much like WHERE in that they can be stacked.

```python
In [4]: s = SELECT('u.user_id', 'u.first_name').FROM('users u')

In [5]: print(s.query)
SELECT u.user_id,
       u.first_name
  FROM users u;

In [6]: s = SELECT('u.first_name').FROM('users u')

In [7]: print(s.query)
SELECT u.first_name
  FROM users u;

In [8]: s = s.GROUP_BY('u.first_name')

In [9]: s = s.SELECT('COUNT(*) AS cnt')

In [10]: print(s.query)
SELECT u.first_name,
       COUNT(*) AS cnt
  FROM users u
GROUP BY u.first_name;

In [11]: s = s.HAVING('COUNT(*) > 3')

In [12]: print(s.query)
SELECT u.first_name,
       COUNT(*) AS cnt
  FROM users u
GROUP BY u.first_name
HAVING COUNT(*) > 3;

In [13]: s = s.ORDER_BY('COUNT(*)')

In [14]: print(s.query)
SELECT u.first_name,
       COUNT(*) AS cnt
  FROM users u
GROUP BY u.first_name
HAVING COUNT(*) > 3
ORDER BY COUNT(*);

In [15]: s = s.ORDER_BY('u.first_name')

In [16]: print(s.query)
SELECT u.first_name,
       COUNT(*) AS cnt
  FROM users u
GROUP BY u.first_name
HAVING COUNT(*) > 3
ORDER BY COUNT(*),
         u.first_name;


In [17]: u = UPDATE('users').SET(first_name='Bob').WHERE(first_name='Robert')

In [18]: print(u.query)
UPDATE users
   SET first_name = %(first_name_bind)s
 WHERE first_name = %(first_name_bind_1)s;

In [19]: print(u.binds)
{'first_name_bind': 'Bob', 'first_name_bind_1': 'Robert'}

In [20]: u = u.RETURNING('u.user_id')

In [21]: print(u.query)
UPDATE users
   SET first_name = %(first_name_bind)s
 WHERE first_name = %(first_name_bind_1)s
RETURNING u.user_id;

```


### Connection Factory
The norm connection factory is a helper to produce norm.connection.ConnectionProxy wrapped DB-API connection objects.

#### Connection Factory Example:
```python
import sqlite3

# notice that there is a specific class for each type of database
from norm.norm_sqlite3 import SQLI_ConnectionFactory as ConnectionFactory

def _make_connection():
    conn = sqlite3.connect(':memory:')
    return conn

my_connection_factory = ConnectionFactory(_make_connection)


# now we can get connections and use them
conn = my_connection_factory()

row = conn.run_queryone('SELECT 1 AS test_column')
# row == {'test_column': 1}

```

#### Connection Proxy
A norm connection factory will return connection objects that look a lot like whatever dbapi connection object you are used to from the library you use to create connections (psycopg2, pymssql, sqlite3, etc) but with some important exceptions.  While it passes on any method call to the actual connection object, it intercepts .cursor.  Additionally, it adds .run_query, .run_queryone  and .execute.


##### .cursor
the .cursor(...) method passes all arguments to the .cursor method of the actual connection object.  However, it wraps the cursor which is returned inside a CursorProxy object.

##### .execute
Calling .execute on a ConnectionProxy creates a cursor, executes the sql provided, and then closes the cursor.  It is meant as a convenience to avoid creating a cursor for queries where you do not care about any data returned.

##### .run_query
Calling this returns a generator which produces rows in the form of dictionaries.

```python
import sqlite3
from norm.norm_sqlite3 import SQLI_ConnectionFactory as ConnectionFactory

def conn_maker():
    conn = sqlite3.connect(':memory:')
    conn.execute(
        '''CREATE TABLE users (
               user_id INTEGER PRIMARY KEY AUTOINCREMENT,
               first_name VARCHAR(64)
        )''')
    conn.commit()
    return conn


cf = ConnectionFactory(conn_maker)

conn = cf()

conn.execute(
    '''CREATE TABLE foos (
           val VARCHAR(64)
    )''')

for val in range(10):
    conn.execute('INSERT INTO foos VALUES (:value)',
                 dict(value=val))

rows = conn.run_query('SELECT val FROM foos')
print(rows)
# prints: <norm.rows.RowsProxy object at 0x7f0ae07191d0>

print(list(rows))
# prints: [{'val': '0'}, {'val': '1'}, {'val': '2'}, ...]

```


#### Example: Efficiently move and join data between databases.
see `benchmarks/shuttle_data_example.py`

A run on my desktop produced:
```
Moved 1000000 rows in 20.09 seconds
Streaming memory usage peak was 0.05MB
Moved 1000000 rows in 16.58 seconds
Fetchall memory usage peak was 212.13MB
```

#### Example: Faster INSERTs by reducing the number of queries executed
see `benchmarks/benchmark.py`

When you are inserting many rows, most Python DB-API libraries will produce 1 INSERT statement for each row.  There is a cursor.executemany method that many libraries provide, but this usually still produces a seperate INSERT statement per row inserted, and simply does these in a for loop.

Lets say you are inserting 1000 rows into a table.  With Norm you can batch these into a single INSERT object and execute it, and a single large SQL statement is produced.  Back and forth trips to the database == 1.

Lets say you use SQLAlchemy or even directly use one of the sql libraries.  Even if you use executemany, this will produce 1000 back and forth trips to the database.

In a datacenter environment, ping times to the db will be short, lets estimate the time to send a query to a database, plus the time to parse a short INSERT statement to be 1ms.  For 1000 rows, just the round trips to the database will add 1 second to the execution time.

A run on my desktop, writing to a local postgresql database (latency is effectively 0), showed the INSERT was still completed in 1/4th the time.  Adding network latency will make the improvement more dramatic.
```
*** Begin sqlalchemy_insert_bench
Elapsed Time: 3.4474
Faster than SQLA factor: 1.0000
*** Begin norm_insert_bench
Elapsed Time: 0.7697
Faster than SQLA factor: 4.4790
```

# norm is like sql

## setup

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
# prints: [{'val': '0'}, {'val': '1'}, {'val': '2'}, {'val': '3'}, {'val': '4'}, {'val': '5'}, {'val': '6'}, {'val': '7'}, {'val': '8'}, {'val': '9'}]

```


## Query Generation
The primary purpose of Norm is to make it easier to generate SQL.

### The SELECT class
#### Basic queries

```python
from norm.norm_sqlite3 import SQLI_SELECT as SELECT

# continuing from the example above

s = (SELECT('val')
     .FROM('foos'))

print(list(conn.run_query(s)))
# prints: [{'val': '0'}, {'val': '1'}, {'val': '2'}, {'val': '3'}, {'val': '4'}, {'val': '5'}, {'val': '6'}, {'val': '7'}, {'val': '8'}, {'val': '9'}]


s = (SELECT('val')
     .FROM('foos')
     .WHERE(val=5))
print(list(conn.run_query(s)))
# prints: [{'val': '5'}]

print(conn.run_queryone(s))
# prints: {'val': '5'}

```

Bind parameters are automatically handled, for example in `.WHERE(val=5)`

```python
print(s.query)
# prints:
# SELECT val
#   FROM foos
#  WHERE val = :val_bind_0;

print(s.binds)
# prints: {'val_bind_0': 5}
```

Using .query and .binds seperately lets you use norm wherever you can execute SQL.  For example, with a SQLAlchemy Session object:

```python
res = Session.execute(s.query, s.binds)
```

#### More powerful query generation
In addition to the simple, static queries above, it is possible to add query clauses.

```python
print(s.query)
# SELECT val
#   FROM foos
#  WHERE val = :val_bind_0;

s = s.WHERE('val * 2 = 4')
print(s.query)
# SELECT val
#   FROM foos
#  WHERE val = :val_bind_0 AND
#        val * 2 = 4;

s = s.JOIN('bars', ON='foos.val = bars.bar_id')
print(s.query)
#  SELECT val
#    FROM foos
#    JOIN bars
#         ON foos.val = bars.bar_id
#   WHERE val = :val_bind_0 AND
#         val * 2 = 4;

```

Of course you can put it all together:
```python
s = (SELECT('val')
     .FROM('foos')
     .JOIN('bars', ON='foos.val = bars.bar_id')
     .WHERE(val=5)
     .WHERE('val * 2 = 4'))
```

Or you can evolve queries dynamically:

```python
def get_users(cursor, user_ids, only_girls=False):
    s = (SELECT('first_name', 'age')
         .FROM('people'))
    if only_girls:
        s = s.WHERE(gender='f')
    return conn.run_query(s)
```

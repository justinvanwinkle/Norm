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

Calling methods on a query object does not change the object.  In other words, query objects are immutable.  This means it is always safe to create a base query and add clauses without modifying it.

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
fix_karls = (UPDATE('people')
             .SET(first_name='Karl')
             .WHERE(first_name='karl'))
conn.execute(fix_karls)
```

```python
remove_karls = (DELETE('people')
                .WHERE(first_name='Karl'))
conn.execute(remove_karls)
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

This should not be used with a value like `5` or something, it is meant to be a way to specify the DEAULT keyword for the library/database you are using.  For psycopg2/postgresql, it will automatically fill in DEFAULT, using http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.AsIs  For inferior databases there may not be a defined way to do this safely.

#### INSERT CURRENT DB DATETIME

There is currently no way to signify that the datetime of the DB should be inserted for a field. Instead of using messy conversions, there is still a way to get there with norm! Query the DB date and/or time and save it as a variable. You can then use that variable when inserting into the DB.

Below is an example of using pymssql's `GETDATE()`:

```python
db_date = conn.run_queryone('SELECT GETDATE() as curDate')['curDate']
rows = [dict(first_name='Ada', last_name='Lovelace', current_date=db_date),
        dict(first_name='Grace', last_name='Hopper', current_date=db_date),
        dict(first_name='Anita', last_name='Borg', current_date=db_date),
        dict(first_name='Janie', last_name='Tsao', current_date=db_date),
        dict(first_name='Katherine', last_name='Johnson', current_date=db_date)]
i = INSERT('people', rows)
conn.execute(i)
```


### WITH (Commont Table Expressions)

Implemented, documentation TBD.

### LIMIT

Implemented, documentation TBD.

#### LIMIT vs. TOP

While many SQL flavors prefer `LIMIT`, MS SQL Server favors `TOP`.

For instance, if you only want to return one result, you would write the following:

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


If you only want to return five results, you would write the following:

```python
query = (
    SELECT('TOP 5 FirstName as first_name',
           'LastName as last_name')
    .FROM('people'))
rows = conn.run_queryone(query)
print(list(rows))
# prints : [{'first_name': 'Ada', 'last_name': 'Lovelace'}, {'first_name': 'Grace', 'last_name': 'Hopper'}, {'first_name': 'Anita', 'last_name': 'Borg'}, {'first_name': 'Janie', 'last_name': 'Tsao'}, {'first_name': 'Katherine', 'last_name': 'Johnson'}]
```

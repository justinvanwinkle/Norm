from datetime import datetime

from norm import SELECT
from norm import UPDATE
from norm import DELETE
from norm import INSERT
from norm import WITH
from norm import EXISTS
from norm import NOT_EXISTS
from norm.norm import NormAsIs


simple_select_query = """\
SELECT tbl1.column1 AS col1
  FROM table1 AS tbl1
 WHERE tbl1.col2 = 'testval';"""


def test_simple_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_select_query
    assert s.binds == {}


simple_asis_query = """\
SELECT tbl1.column1 AS col1
  FROM table1 AS tbl1
 WHERE tbl1.col2 = FOO AND
       x = CURRENT_TIMESTAMP;"""


def test_simple_asis():
    literal2 = NormAsIs('CURRENT_TIMESTAMP')
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = %(literal1)s",
                x=literal2)
         .bind(literal1=NormAsIs('FOO')))

    assert s.query == simple_asis_query
    assert s.binds == {}


kw_alias_query = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = %(tbl1___col2_bind_0)s;"""


def test_kw_aliases():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .JOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE(**{"tbl1.col2": 'testval'}))

    assert s.query == kw_alias_query
    assert s.binds == {'tbl1___col2_bind_0': 'testval'}


simple_inner_join_select_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval';"""


def test_simple_inner_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .JOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_inner_join_select_expected
    assert s.binds == {}


simple_inner_using_join_select_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  JOIN table2 AS tbl2
       USING (tubs)
 WHERE tbl1.col2 = 'testval';"""


def test_simple_inner_using_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .JOIN("table2 AS tbl2", USING='tubs')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_inner_using_join_select_expected
    assert s.binds == {}


simple_inner_using_multi_join_select_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  JOIN table2 AS tbl2
       USING (tubs, bubs)
 WHERE tbl1.col2 = 'testval';"""


def test_simple_inner_using_multi_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .JOIN("table2 AS tbl2", USING=('tubs', 'bubs'))
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_inner_using_multi_join_select_expected
    assert s.binds == {}


simple_left_join_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  LEFT JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval';"""


def test_simple_left_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .LEFTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_left_join_expected
    assert s.binds == {}


simple_right_join_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  RIGHT JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval';"""


def test_simple_right_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .RIGHTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_right_join_expected
    assert s.binds == {}


simple_full_join_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  FULL JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval';"""


def test_simple_full_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .FULLJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))

    assert s.query == simple_full_join_expected
    assert s.binds == {}


distinct_on_expected = """\
SELECT DISTINCT ON (tbl1.column1)
       tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  LEFT JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval';"""


def test_distinct_on():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .LEFTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'")
         .DISTINCT_ON('tbl1.column1'))

    assert s.query == distinct_on_expected
    assert s.binds == {}


exists_subquery_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  LEFT JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval' AND
       EXISTS (
         SELECT 1
           FROM foo
          WHERE foobar = tbl1.column1 AND
                bugs = %(bugs)s);"""


def test_exists_subquery():
    sub = (EXISTS(1)
           .FROM('foo')
           .WHERE('foobar = tbl1.column1',
                  'bugs = %(bugs)s')
           .bind(bugs='spiders'))

    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .LEFTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'",
                sub))

    assert s.query == exists_subquery_expected
    assert s.binds == {'bugs': 'spiders'}


not_exists_subquery_expected = """\
SELECT tbl1.column1 AS col1,
       tbl2.column2 AS col2,
       tbl2.column3 AS col3
  FROM table1 AS tbl1
  LEFT JOIN table2 AS tbl2
       ON tbl1.tid = tbl2.tid
 WHERE tbl1.col2 = 'testval' AND
       NOT EXISTS (
         SELECT 1
           FROM foo
          WHERE foobar = tbl1.column1 AND
                bugs = %(bugs)s);"""


def test_not_exists_subquery():
    sub = (NOT_EXISTS(1)
           .FROM('foo')
           .WHERE('foobar = tbl1.column1',
                  'bugs = %(bugs)s'))

    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .LEFTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'",
                sub)
         .bind(bugs='spiders'))

    assert s.query == not_exists_subquery_expected
    assert s.binds == {'bugs': 'spiders'}


multiple_where_expected = """\
SELECT tbl1.column1 AS col1
  FROM table1 AS tbl1
 WHERE tbl1.col2 = 'testval' AND
       tbl1.col3 = 'otherval';"""


def test_multiple_where():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .WHERE("tbl1.col3 = 'otherval'"))

    assert s.query == multiple_where_expected
    assert s.binds == {}


all_select_methods_expected = """\
SELECT tbl1.column1 AS col1,
       table2.blah
  FROM table1 AS tbl1
  JOIN table2
       ON table2.blah = tbl1.col2
 WHERE tbl1.col2 = 'testval'
GROUP BY table2.blah,
         col1
HAVING count(*) > 5 AND
       count(*) > 6
ORDER BY count(*)
 LIMIT 5
OFFSET 3;"""


def test_all_select_methods():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .JOIN("table2", ON="table2.blah = tbl1.col2")
         .SELECT("table2.blah")
         .HAVING("count(*) > 5",
                 "count(*) > 6")
         .GROUP_BY("table2.blah", "col1")
         .ORDER_BY("count(*)")
         .LIMIT(5)
         .OFFSET(3))

    assert s.query == all_select_methods_expected
    assert s.binds == {}


top_select_expected = """\
SELECT TOP 5
       tbl1.column1 AS col1,
       table2.blah
  FROM table1 AS tbl1
  JOIN table2
       ON table2.blah = tbl1.col2
 WHERE tbl1.col2 = 'testval'
GROUP BY table2.blah,
         col1
HAVING count(*) > 5 AND
       count(*) > 6
ORDER BY count(*);"""


def test_top_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .JOIN("table2", ON="table2.blah = tbl1.col2")
         .SELECT("table2.blah")
         .HAVING("count(*) > 5",
                 "count(*) > 6")
         .GROUP_BY("table2.blah", "col1")
         .ORDER_BY("count(*)")
         .TOP(5))

    assert s.query == top_select_expected
    assert s.binds == {}


overwriting_select_methods_expected = """\
SELECT TOP 2
       tbl1.column1 AS col1,
       table2.blah
  FROM table1 AS tbl1
  JOIN table2
       ON table2.blah = tbl1.col2
 WHERE tbl1.col2 = 'testval'
GROUP BY table1.column1,
         table2.blah
HAVING count(*) < 10 AND
       count(*) > 5
ORDER BY table1.column1,
         count(*)
 LIMIT 5
OFFSET 3;"""


def test_overwriting_select_methods():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .JOIN("table2", ON="table2.blah = tbl1.col2")
         .SELECT("table2.blah")
         .HAVING("count(*) < 10")
         .HAVING("count(*) > 5")
         .GROUP_BY("table1.column1")
         .GROUP_BY("table2.blah")
         .ORDER_BY("table1.column1")
         .ORDER_BY("count(*)")
         .TOP('no way')
         .TOP(2)
         .LIMIT('no way')
         .LIMIT(5)
         .OFFSET('should not see this')
         .OFFSET(3))

    assert s.query == overwriting_select_methods_expected
    assert s.binds == {}


binds_expected = """\
SELECT tbl1.column1 AS col1
  FROM table1 AS tbl1
 WHERE tbl1.col2 = 'testval' AND
       tbl1.col3 = %(bind1)s;"""


def test_binds():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE("tbl1.col2 = 'testval'")
          .WHERE("tbl1.col3 = %(bind1)s")
          .bind(bind1='bind1value'))

    assert s1.query == binds_expected
    assert s1.binds == {'bind1': 'bind1value'}


generate_binds_expected = """\
SELECT tbl1.column1 AS col1
  FROM table1 AS tbl1
 WHERE id = %(id_bind_0)s AND
       name = %(name_bind_1)s AND
       occupation = %(occupation_bind_2)s AND
       salary = %(salary_bind_3)s AND
       tbl1.col3 = %(bind1)s;"""


def test_generate_binds():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE(id=1)
          .WHERE(name='bossanova')
          .WHERE(occupation='rascal', salary=None)
          .WHERE("tbl1.col3 = %(bind1)s")
          .bind(bind1='bind1value'))

    assert s1.query == generate_binds_expected
    assert s1.binds == {'bind1': 'bind1value',
                        'id_bind_0': 1,
                        'name_bind_1': 'bossanova',
                        'occupation_bind_2': 'rascal',
                        'salary_bind_3': None}


def test_generative_query():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE("tbl1.col2 = 'testval'")
          .WHERE("tbl1.col3 = 'otherval'"))

    s2 = s1.WHERE("tbl1.col4 = 'otherother'")

    s3 = s2.JOIN("table2 AS tbl2", USING="somecol").bind(val='whatevs')
    s4 = s3.JOIN("table3 AS tbl3", ON="tbl3.colx = tbl2.coly")
    s5 = s4.SELECT("tbl3.whatever AS whatever").bind(test='test2', val='nope')

    expected1 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval';"])

    expected2 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval' AND",
        "       tbl1.col4 = 'otherother';"])

    expected3 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        "  JOIN table2 AS tbl2",
        "       USING (somecol)",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval' AND",
        "       tbl1.col4 = 'otherother';"])

    expected4 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        "  JOIN table2 AS tbl2",
        "       USING (somecol)",
        "  JOIN table3 AS tbl3",
        "       ON tbl3.colx = tbl2.coly",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval' AND",
        "       tbl1.col4 = 'otherother';"])

    expected5 = '\n'.join([
        "SELECT tbl1.column1 AS col1,",
        "       tbl3.whatever AS whatever",
        "  FROM table1 AS tbl1",
        "  JOIN table2 AS tbl2",
        "       USING (somecol)",
        "  JOIN table3 AS tbl3",
        "       ON tbl3.colx = tbl2.coly",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval' AND",
        "       tbl1.col4 = 'otherother';"])

    assert s5.query == expected5
    assert s5.binds == {'test': 'test2', 'val': 'nope'}
    assert s4.query == expected4
    assert s4.binds == {'val': 'whatevs'}
    assert s3.query == expected3
    assert s3.binds == {'val': 'whatevs'}
    assert s2.query == expected2
    assert s2.binds == {}
    assert s1.query == expected1
    assert s1.binds == {}


simple_update_expected = """\
UPDATE table1
   SET col1 = 'test',
       col2 = 'test2';"""


def test_simple_update():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2'"))

    assert u.query == simple_update_expected
    assert u.binds == {}


simple_update_star_kw_query = """\
UPDATE table1
   SET col1 = %(col1_bind)s,
       col2 = %(col2_bind)s;"""


def test_simple_update_star_kw():
    u = (UPDATE("table1")
         .SET(**{'col1': 'test',
                 'col2': 'test2'}))

    assert u.query == simple_update_star_kw_query
    assert u.binds == dict(col1_bind='test',
                           col2_bind='test2')


update_one_row_query = """\
UPDATE table1
   SET col1 = 'test',
       col2 = 'test2'
 WHERE id = %(id_bind_0)s;"""


def test_update_one_row():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2'")
         .WHERE(id=5))

    assert u.query == update_one_row_query
    assert u.binds == {'id_bind_0': 5}


named_arg_update_query = """\
UPDATE table1
   SET col1 = %(col1_bind)s,
       col2 = 'test2'
 WHERE id = %(id_bind_1)s;"""


def test_named_arg_update():
    u = (UPDATE("table1")
         .SET(col1='test')
         .SET("col2 = 'test2'")
         .WHERE(id=5))

    assert u.query == named_arg_update_query
    assert u.binds == {'col1_bind': 'test',
                       'id_bind_1': 5}


update_returning_query = """\
UPDATE table1
   SET col1 = %(col1_bind)s
RETURNING test, test1;"""


def test_update_returning():
    u = (UPDATE("table1")
         .SET(col1='test')
         .RETURNING('test', 'test1'))

    assert u.query == update_returning_query
    assert u.binds == {'col1_bind': 'test'}


def test_simple_delete():
    d = DELETE('table1')

    assert d.query == 'DELETE FROM table1;'
    assert d.binds == {}


delete_where_query = """\
DELETE FROM table2
 WHERE x > 5;"""


def test_delete_where():
    d = (DELETE('table2')
         .WHERE('x > 5'))

    assert d.query == delete_where_query
    assert d.binds == {}


def test_delete_where_autobind():
    d = (DELETE('table3')
         .WHERE(x=25))

    assert d.query == '\n'.join([
        "DELETE FROM table3",
        " WHERE x = %(x_bind_0)s;"])
    assert d.binds == {'x_bind_0': 25}


def test_delete_returning():
    d = (DELETE('table3')
         .WHERE(x=25)
         .RETURNING('this', 'that'))

    assert d.query == '\n'.join([
        "DELETE FROM table3",
        " WHERE x = %(x_bind_0)s",
        "RETURNING this, that;"])
    assert d.binds == {'x_bind_0': 25}


row1 = {'name': 'justin', 'zipcode': 23344}
row2 = {'name': 'nintendo', 'phone': '1112223333'}


def test_basic_insert():
    i = INSERT('table1', data=row1)

    assert i.binds == {'name_0': 'justin', 'zipcode_0': 23344}
    assert i.query == ('INSERT INTO table1 (name, zipcode)\n'
                       '  VALUES\n'
                       '(%(name_0)s, %(zipcode_0)s);')


def test_multi_insert():
    i = INSERT('table1', data=[row1, row2])
    assert i.binds == {'name_0': 'justin',
                       'phone_0': None,
                       'zipcode_0': 23344,
                       'name_1': 'nintendo',
                       'phone_1': '1112223333',
                       'zipcode_1': None}

    assert i.query == ('INSERT INTO table1 (name, phone, zipcode)\n'
                       '  VALUES\n'
                       '(%(name_0)s, %(phone_0)s, %(zipcode_0)s),\n'
                       '(%(name_1)s, %(phone_1)s, %(zipcode_1)s);')


def test_setting_default():
    i = INSERT('table1', data=[row1, row2], default=2)
    assert i.binds == {'name_0': 'justin',
                       'phone_0': 2,
                       'zipcode_0': 23344,
                       'name_1': 'nintendo',
                       'phone_1': '1112223333',
                       'zipcode_1': 2}


def test_setting_columns():
    i = INSERT('table1', data=row1, columns=['name', 'address'])
    assert i.binds == {'name_0': 'justin', 'address_0': None}
    assert i.query == ('INSERT INTO table1 (name, address)\n'
                       '  VALUES\n'
                       '(%(name_0)s, %(address_0)s);')


def test_setting_columns_default():
    i = INSERT('table1', data=[row1, row2], columns=['phone'], default='blah')
    assert i.binds == {'phone_0': 'blah',
                       'phone_1': '1112223333'}

    assert i.query == ('INSERT INTO table1 (phone)\n'
                       '  VALUES\n'
                       '(%(phone_0)s),\n'
                       '(%(phone_1)s);')


def test_insert_no_columns():
    i = INSERT('table1', data=[row1, row2], columns=['phone'], default='blah')
    assert i.binds == {'phone_0': 'blah',
                       'phone_1': '1112223333'}

    assert i.query == ('INSERT INTO table1 (phone)\n'
                       '  VALUES\n'
                       '(%(phone_0)s),\n'
                       '(%(phone_1)s);')


def test_insert_default_values():
    i = INSERT('table1')

    assert i.binds == {}
    assert i.query == 'INSERT INTO table1 DEFAULT VALUES;'


def test_insert_on_conflict():
    i = INSERT('table1',
               data={'col1': 'val1', 'col2': 'val2'},
               on_conflict='(col1) DO NOTHING')

    assert i.binds == dict(col1_0='val1', col2_0='val2')
    assert i.query == ('INSERT INTO table1 (col1, col2)\n'
                       '  VALUES\n'
                       '(%(col1_0)s, %(col2_0)s)'
                       '\nON CONFLICT (col1) DO NOTHING;')


def test_asis_insert():
    row = {'name': NormAsIs('now()'), 'zipcode': 23344}

    i = INSERT('table1', data=[row])
    assert i.query == ('INSERT INTO table1 (name, zipcode)\n'
                       '  VALUES\n'
                       '(now(), %(zipcode_0)s);')
    assert i.binds == {'zipcode_0': 23344}


test_with_query = """\
WITH my_fake_table AS
       (UPDATE sometable
           SET foo = %(foo_bind)s
        RETURNING foo, bar, whatever)

INSERT INTO my_table (foo, bar, whatever)
  SELECT foo,
         bub,
         derp
    FROM my_fake_table;"""


def test_with():
    u = (UPDATE('sometable')
         .SET(foo='123')
         .RETURNING('foo', 'bar', 'whatever'))
    w = WITH(my_fake_table=u)
    w = w(INSERT('my_table',
                 columns=('foo', 'bar', 'whatever'),
                 statement=(SELECT('foo', 'bub', 'derp')
                            .FROM('my_fake_table'))))

    assert w.query == test_with_query
    assert w.binds == {'foo_bind': '123'}


with_multiple_query = """\
WITH cte_table_1 AS
       (SELECT mt.row1,
               mt.row2 AS row2
          FROM mytable AS mt
          JOIN othertable AS ot
               ON ot.bub = mt.foo
         WHERE 1 = 1),
     cte_table_2 AS
       (SELECT mt.row1,
               mt.row2 AS row2
          FROM mytable AS mt)

SELECT ct1.row1
  FROM cte_table_1 ct1;"""


def test_with_multiple():
    s1 = (SELECT('mt.row1', 'mt.row2 AS row2')
          .FROM('mytable AS mt')
          .JOIN('othertable AS ot',
                ON='ot.bub = mt.foo')
          .WHERE('1 = 1'))
    s2 = (SELECT('mt.row1', 'mt.row2 AS row2')
          .FROM('mytable AS mt'))

    w = WITH(cte_table_1=s1, cte_table_2=s2)
    w = w(SELECT('ct1.row1').FROM('cte_table_1 ct1'))

    assert w.query == with_multiple_query
    assert w.binds == {}


loggable_query_test = """\
UPDATE table1
   SET col1 = 'test',
       col2 = datetime.datetime(1997, 11, 15, 1, 1)
RETURNING test, test1;"""


def test_loggable_query():
    u = (UPDATE("table1")
         .SET(col1='test',
              col2=datetime(1997, 11, 15, 1, 1))
         .RETURNING('test', 'test1'))

    assert u._loggable_query == loggable_query_test

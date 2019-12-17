from __future__ import unicode_literals

from norm import SELECT
from norm import UPDATE
from norm import DELETE
from norm import INSERT


def test_simple_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'"))
    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE tbl1.col2 = 'testval';"])
    assert s.query == expected


def test_simple_inner_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .JOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))
    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1,",
        "       tbl2.column2 AS col2,",
        "       tbl2.column3 AS col3",
        "  FROM table1 AS tbl1",
        "  JOIN table2 AS tbl2",
        "       ON tbl1.tid = tbl2.tid",
        " WHERE tbl1.col2 = 'testval';"])
    assert s.query == expected


def test_simple_outer_join_select():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .LEFTJOIN("table2 AS tbl2", ON='tbl1.tid = tbl2.tid')
         .SELECT("tbl2.column2 AS col2",
                 "tbl2.column3 AS col3")
         .WHERE("tbl1.col2 = 'testval'"))
    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1,",
        "       tbl2.column2 AS col2,",
        "       tbl2.column3 AS col3",
        "  FROM table1 AS tbl1",
        "  LEFT JOIN table2 AS tbl2",
        "       ON tbl1.tid = tbl2.tid",
        " WHERE tbl1.col2 = 'testval';"])
    assert s.query == expected


def test_multiple_where():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .WHERE("tbl1.col3 = 'otherval'"))
    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval';"])

    assert s.query == expected


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

    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1,",
        "       table2.blah",
        "  FROM table1 AS tbl1",
        "  JOIN table2",
        "       ON table2.blah = tbl1.col2",
        " WHERE tbl1.col2 = 'testval'",
        "GROUP BY table2.blah,",
        "         col1",
        "HAVING count(*) > 5 AND",
        "       count(*) > 6",
        "ORDER BY count(*)",
        " LIMIT 5",
        "OFFSET 3;"])

    assert s.query == expected


def test_overwriting_select_methods_overwrite():
    s = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .JOIN("table2", ON="table2.blah = tbl1.col2")
         .SELECT("table2.blah")
         .HAVING("count(*) > BAD")
         .HAVING("count(*) > 5")
         .GROUP_BY("THIS IS BAD")
         .GROUP_BY("table2.blah")
         .ORDER_BY("STILL BAD")
         .ORDER_BY("count(*)")
         .LIMIT('no way')
         .LIMIT(5)
         .OFFSET('should not see this')
         .OFFSET(3))

    expected = '\n'.join([
        "SELECT tbl1.column1 AS col1,",
        "       table2.blah",
        "  FROM table1 AS tbl1",
        "  JOIN table2",
        "       ON table2.blah = tbl1.col2",
        " WHERE tbl1.col2 = 'testval'",
        "GROUP BY THIS IS BAD,",
        "         table2.blah",
        "HAVING count(*) > BAD AND",
        "       count(*) > 5",
        "ORDER BY STILL BAD,",
        "         count(*)",
        " LIMIT 5",
        "OFFSET 3;"])

    assert s.query == expected


def test_binds():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE("tbl1.col2 = 'testval'")
          .WHERE("tbl1.col3 = %(bind1)s")
          .bind(bind1='bind1value'))

    expected1 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = %(bind1)s;"])

    assert s1.query == expected1
    assert s1.binds == {'bind1': 'bind1value'}


def test_generate_binds():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE(id=1)
          .WHERE(name='bossanova')
          .WHERE(occupation='rascal', salary=None)
          .WHERE("tbl1.col3 = %(bind1)s")
          .bind(bind1='bind1value'))

    expected1_v1 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        " WHERE id = %(id_bind_0)s AND",
        "       name = %(name_bind_1)s AND",
        "       occupation = %(occupation_bind_2)s AND",
        "       salary = %(salary_bind_3)s AND",
        "       tbl1.col3 = %(bind1)s;"])

    assert s1.query == expected1_v1
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
        "       USING somecol",
        " WHERE tbl1.col2 = 'testval' AND",
        "       tbl1.col3 = 'otherval' AND",
        "       tbl1.col4 = 'otherother';"])

    expected4 = '\n'.join([
        "SELECT tbl1.column1 AS col1",
        "  FROM table1 AS tbl1",
        "  JOIN table2 AS tbl2",
        "       USING somecol",
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
        "       USING somecol",
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


def test_simple_update():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2'"))
    expected = '\n'.join([
        "UPDATE table1",
        "   SET col1 = 'test',",
        "       col2 = 'test2';"])

    assert u.query == expected


def test_update_one_row():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2'")
         .WHERE(id=5))
    expected = '\n'.join([
        "UPDATE table1",
        "   SET col1 = 'test',",
        "       col2 = 'test2'",
        " WHERE id = %(id_bind_0)s;"])

    assert u.query == expected
    assert u.binds == {'id_bind_0': 5}


def test_named_arg_update():
    u = (UPDATE("table1")
         .SET(col1='test')
         .SET("col2 = 'test2'")
         .WHERE(id=5))
    expected = '\n'.join([
        "UPDATE table1",
        "   SET col1 = %(col1_bind)s,",
        "       col2 = 'test2'",
        " WHERE id = %(id_bind_1)s;"])

    assert u.query == expected
    assert u.binds == {'col1_bind': 'test',
                       'id_bind_1': 5}


def test_update_returning():
    u = (UPDATE("table1")
         .SET(col1='test')
         .RETURNING('test', 'test1'))

    assert u.query == '\n'.join([
        "UPDATE table1",
        "   SET col1 = %(col1_bind)s",
        "RETURNING test, test1;"])


def test_simple_delete():
    d = DELETE('table1')

    assert d.query == 'DELETE FROM table1;'
    assert d.binds == {}


def test_delete_where():
    d = (DELETE('table2')
         .WHERE('x > 5'))

    assert d.query == '\n'.join([
        "DELETE FROM table2",
        " WHERE x > 5;"])
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
    assert i.query == ('INSERT INTO table1 '
                       '(name, zipcode) VALUES (%(name_0)s, %(zipcode_0)s);')


def test_multi_insert():
    i = INSERT('table1', data=[row1, row2])
    assert i.binds == {'name_0': 'justin',
                       'phone_0': None,
                       'zipcode_0': 23344,
                       'name_1': 'nintendo',
                       'phone_1': '1112223333',
                       'zipcode_1': None}

    assert i.query == ('INSERT INTO table1 '
                       '(name, phone, zipcode) '
                       'VALUES (%(name_0)s, %(phone_0)s, %(zipcode_0)s),\n'
                       '       (%(name_1)s, %(phone_1)s, %(zipcode_1)s);')


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
    assert i.query == ('INSERT INTO table1 '
                       '(name, address) VALUES (%(name_0)s, %(address_0)s);')


def test_setting_columns_default():
    i = INSERT('table1', data=[row1, row2], columns=['phone'], default='blah')
    assert i.binds == {'phone_0': 'blah',
                       'phone_1': '1112223333'}

    assert i.query == ('INSERT INTO table1 '
                       '(phone) '
                       'VALUES (%(phone_0)s),\n'
                       '       (%(phone_1)s);')


def test_insert_no_columns():
    i = INSERT('table1', data=[row1, row2], columns=['phone'], default='blah')
    assert i.binds == {'phone_0': 'blah',
                       'phone_1': '1112223333'}

    assert i.query == ('INSERT INTO table1 '
                       '(phone) '
                       'VALUES (%(phone_0)s),\n'
                       '       (%(phone_1)s);')


def test_insert_default_values():
    i = INSERT('table1')

    assert i.binds == {}
    assert i.query == ('INSERT INTO table1 '
                       'DEFAULT VALUES;')


def test_insert_on_conflict():
    i = INSERT('table1',
               data={'col1': 'val1', 'col2': 'val2'},
               on_conflict='(col1) DO NOTHING')

    assert i.binds == dict(col1_0='val1', col2_0='val2')
    assert i.query == ('INSERT INTO table1 '
                       '(col1, col2) '
                       'VALUES (%(col1_0)s, %(col2_0)s)'
                       '\nON CONFLICT (col1) DO NOTHING;')

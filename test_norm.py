from __future__ import unicode_literals

from norm import SELECT
from norm import UPDATE


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
         .HAVING("count(*) > 5")
         .GROUP_BY("table2.blah, col1")
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
            "GROUP BY table2.blah, col1",
            "HAVING count(*) > 5",
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
         .GROUP_BY("table2.blah, col1")
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
            "GROUP BY table2.blah, col1",
            "HAVING count(*) > 5",
            "ORDER BY count(*)",
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


def test_gen_binds():
    s1 = (SELECT("tbl1.column1 AS col1")
          .FROM("table1 AS tbl1")
          .WHERE(id=1)
          .WHERE(name='bossanova')
          .WHERE(occupation='rascal', salary=None)
          # This last line makes testing a bit complex, there
          #   is no way to predict reliably what order those two
          #   constraints will come out in.
          .WHERE("tbl1.col3 = %(bind1)s")
          .bind(bind1='bind1value'))

    expected1_v1 = '\n'.join([
            "SELECT tbl1.column1 AS col1",
            "  FROM table1 AS tbl1",
            " WHERE id = %(norm_gen_bind_0)s AND",
            "       name = %(norm_gen_bind_1)s AND",
            "       occupation = %(norm_gen_bind_2)s AND",
            "       salary = %(norm_gen_bind_3)s AND",
            "       tbl1.col3 = %(bind1)s;"])

    expected1_v2 = '\n'.join([
            "SELECT tbl1.column1 AS col1",
            "  FROM table1 AS tbl1",
            " WHERE id = %(norm_gen_bind_0)s AND",
            "       name = %(norm_gen_bind_1)s AND",
            "       salary = %(norm_gen_bind_2)s AND",
            "       occupation = %(norm_gen_bind_3)s AND",
            "       tbl1.col3 = %(bind1)s;"])
    if s1.query.find('salary') > s1.query.find('occupation'):
        assert s1.query == expected1_v1
        assert s1.binds == {'bind1': 'bind1value',
                            'norm_gen_bind_0': 1,
                            'norm_gen_bind_1': 'bossanova',
                            'norm_gen_bind_2': 'rascal',
                            'norm_gen_bind_3': None}

    else:
        assert s1.query == expected1_v2
        assert s1.binds == {'bind1': 'bind1value',
                            'norm_gen_bind_0': 1,
                            'norm_gen_bind_1': 'bossanova',
                            'norm_gen_bind_3': 'rascal',
                            'norm_gen_bind_2': None}


def test_generative_query():
    s1 = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .WHERE("tbl1.col3 = 'otherval'"))

    s2 = s1.WHERE("tbl1.col4 = 'otherother'")

    s3 = s2.JOIN("table2 AS tbl2", USING="somecol").bind(val='whatevs')
    s4 = s3.JOIN("table3 AS tbl3", ON="tbl3.colx = tbl2.coly")
    s5 = s4.SELECT("tbl3.whatever AS whatever").bind(test='test2')

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
    assert s5.binds == {'test': 'test2', 'val': 'whatevs'}
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
            " WHERE id = %(norm_gen_bind_0)s;"])

    assert u.query == expected
    assert u.binds == {'norm_gen_bind_0': 5}


def test_named_arg_update():
    u = (UPDATE("table1")
         .SET(col1='test')
         .SET("col2 = 'test2'")
         .WHERE(id=5))
    expected = '\n'.join([
            "UPDATE table1",
            "   SET col1 = %(col1_bind)s,",
            "       col2 = 'test2'",
            " WHERE id = %(norm_gen_bind_1)s;"])

    assert u.query == expected
    assert u.binds == {'col1_bind': 'test',
                       'norm_gen_bind_1': 5}

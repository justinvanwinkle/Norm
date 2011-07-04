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


def test_generative_query():
    s1 = (SELECT("tbl1.column1 AS col1")
         .FROM("table1 AS tbl1")
         .WHERE("tbl1.col2 = 'testval'")
         .WHERE("tbl1.col3 = 'otherval'"))

    s2 = s1.WHERE("tbl1.col4 = 'otherother'")

    s3 = s2.JOIN("table2 AS tbl2", USING="somecol")
    s4 = s3.JOIN("table3 AS tbl3", ON="tbl3.colx = tbl2.coly")
    s5 = s4.SELECT("tbl3.whatever AS whatever")

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
    assert s4.query == expected4
    assert s3.query == expected3
    assert s2.query == expected2
    assert s1.query == expected1


def test_simple_update():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2"))
    expected = '\n'.join([
            "UPDATE table1",
            "   SET col1 = 'test',",
            "       col2 = 'test2';"])

    assert u.query == expected

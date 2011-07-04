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
    print s.query
    print expected
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

    print s.query
    print expected
    assert s.query == expected


def test_simple_update():
    u = (UPDATE("table1")
         .SET("col1 = 'test'")
         .SET("col2 = 'test2"))
    expected = '\n'.join([
            "UPDATE table1",
            "   SET col1 = 'test',",
            "       col2 = 'test2';"])

    assert u.query == expected

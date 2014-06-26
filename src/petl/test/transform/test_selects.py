__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


from nose.tools import eq_


from petl.testutils import ieq
from petl.transform.selects import select, selectin, selectcontains, \
    rowselect, rowlenselect, selectre, selectusingcontext, facet, rangefacet


def test_select():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec[0] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice
 
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice
 
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = select(table, lambda rec: rec.foo == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice
 
    # check select complement
    actual = select(table, lambda rec: rec['foo'] == 'a', complement=True)
    expect = (('foo', 'bar', 'baz'),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice

    actual = select(table, lambda rec: rec['foo'] == 'a' and rec['bar'] > 3)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    ieq(expect, actual)

    actual = select(table, "{foo} == 'a'")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)

    actual = select(table, "{foo} == 'a' and {bar} > 3")
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3))
    ieq(expect, actual)

    # check error handling on short rows
    actual = select(table, lambda rec: rec['baz'] > 88.1)
    expect = (('foo', 'bar', 'baz'),
              ('a', 2, 88.2),
              ('d', 7, 100.9))
    ieq(expect, actual)
    
    # check single field tests
    actual = select(table, 'foo', lambda v: v == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice
    
    # check select complement
    actual = select(table, 'foo', lambda v: v == 'a', complement=True)
    expect = (('foo', 'bar', 'baz'),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9),
              ('c', 2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_select_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = select(table, lambda r: r['foo'] == r['bar'])
    ieq(expect, actual)


def test_selectin():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = selectin(table, 'foo', ['a', 'x', 'y'])
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_selectcontains():
    
    table = (('foo', 'bar', 'baz'),
             ('aaa', 4, 9.3),
             ('aa', 2, 88.2),
             ('bab', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = selectcontains(table, 'foo', 'a')
    expect = (('foo', 'bar', 'baz'),
              ('aaa', 4, 9.3),
              ('aa', 2, 88.2),
              ('bab', 1, 23.3))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_rowselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowselect(table, lambda row: row[0] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_rowlenselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowlenselect(table, 3)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2),
              ('b', 1, 23.3),
              ('c', 8, 42.0),
              ('d', 7, 100.9))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_recordselect():
    
    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))

    actual = rowselect(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


def test_selectre():
    
    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('b', 1, 23.3),
             ('ccc', 8, 42.0),
             ('bb', 7, 100.9),
             ('c', 2))
    actual = selectre(table, 'foo', '[ab]{2}')
    expect = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('bb', 7, 100.9))
    ieq(expect, actual)
    ieq(expect, actual)


def test_selectusingcontext():

    table1 = (('foo', 'bar'),
              ('A', 1),
              ('B', 4),
              ('C', 5),
              ('D', 9))

    expect = (('foo', 'bar'),
              ('B', 4),
              ('C', 5))

    def query(prv, cur, nxt):
        return ((prv is not None and (cur.bar - prv.bar) < 2)
                or (nxt is not None and (nxt.bar - cur.bar) < 2))

    actual = selectusingcontext(table1, query)
    ieq(expect, actual)
    ieq(expect, actual)


def test_facet():

    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, 23.3),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))
    fct = facet(table, 'foo')
    assert set(fct.keys()) == set(['a', 'b', 'c', 'd'])
    expect_fcta = (('foo', 'bar', 'baz'),
                   ('a', 4, 9.3),
                   ('a', 2, 88.2))
    ieq(fct['a'], expect_fcta)
    ieq(fct['a'], expect_fcta)  # check can iterate twice
    expect_fctc = (('foo', 'bar', 'baz'),
                   ('c', 8, 42.0),
                   ('c', 2))
    ieq(fct['c'], expect_fctc)
    ieq(fct['c'], expect_fctc)  # check can iterate twice


def test_facet_2():

    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aa', 2, 88.2),
             ('bb', 1, 23.3),
             ('cc', 8, 42.0),
             ('dd', 7, 100.9),
             ('cc', 2))
    fct = facet(table, 'foo')
    assert set(fct.keys()) == set(['aa', 'bb', 'cc', 'dd'])
    expect_fcta = (('foo', 'bar', 'baz'),
                   ('aa', 4, 9.3),
                   ('aa', 2, 88.2))
    ieq(fct['aa'], expect_fcta)
    ieq(fct['aa'], expect_fcta)  # check can iterate twice
    expect_fctc = (('foo', 'bar', 'baz'),
                   ('cc', 8, 42.0),
                   ('cc', 2))
    ieq(fct['cc'], expect_fctc)
    ieq(fct['cc'], expect_fctc)  # check can iterate twice


def test_facet_empty():
    table = (('foo', 'bar'),)
    actual = facet(table, 'foo')
    eq_(list(), actual.keys())


def test_rangefacet():

    table1 = (('foo', 'bar'),
              ('a', 3),
              ('a', 7),
              ('b', 2),
              ('b', 1),
              ('b', 9),
              ('c', 4),
              ('d', 3))
    rf = rangefacet(table1, 'bar', 2)
    eq_([(1, 3), (3, 5), (5, 7), (7, 9)], rf.keys())
    expect_13 = (('foo', 'bar'),
                 ('b', 2),
                 ('b', 1))  # N.B., it get's sorted
    ieq(expect_13, rf[(1, 3)])
    ieq(expect_13, rf[(1, 3)])
    expect_79 = (('foo', 'bar'),
                 ('a', 7),
                 ('b', 9))
    ieq(expect_79, rf[(7, 9)])

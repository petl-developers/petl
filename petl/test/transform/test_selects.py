from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import FieldSelectionError
from petl.test.helpers import ieq, eq_
from petl.comparison import Comparable
from petl.transform.selects import select, selectin, selectcontains, \
    rowlenselect, selectusingcontext, facet, selectgt, selectlt


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
    actual = select(table, lambda rec: Comparable(rec['baz']) > 88.1)
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


def test_rowselect_headerless():
    table = []
    expect = []
    actual = select(table, 'True')
    ieq(expect, actual)


def test_fieldselect_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        for i in select(table, 'foo', lambda v: v == 'a'):
            pass


def test_select_falsey():
    table = (('foo',), 
             ([],),
             ('',))
    expect = (('foo',),)
    actual = select(table, '{foo}')
    ieq(expect, actual)


def test_selectgt():

    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, None),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))
    actual = selectgt(table, 'baz', 50)
    expect = (('foo', 'bar', 'baz'),
              ('a', 2, 88.2),
              ('d', 7, 100.9))
    ieq(expect, actual)
    ieq(expect, actual)


def test_selectlt():

    table = (('foo', 'bar', 'baz'),
             ('a', 4, 9.3),
             ('a', 2, 88.2),
             ('b', 1, None),
             ('c', 8, 42.0),
             ('d', 7, 100.9),
             ('c', 2))
    actual = selectlt(table, 'baz', 50)
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('b', 1, None),
              ('c', 8, 42.0),
              ('c', 2))
    ieq(expect, actual)
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

    actual = select(table, lambda row: row[0] == 'a')
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

    actual = select(table, lambda rec: rec['foo'] == 'a')
    expect = (('foo', 'bar', 'baz'),
              ('a', 4, 9.3),
              ('a', 2, 88.2))
    ieq(expect, actual)
    ieq(expect, actual)  # check can iterate twice


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
    assert set(fct.keys()) == {'a', 'b', 'c', 'd'}
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
    assert set(fct.keys()) == {'aa', 'bb', 'cc', 'dd'}
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
    eq_(list(), list(actual.keys()))

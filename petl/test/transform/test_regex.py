# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import pytest

from petl.compat import next
from petl.errors import ArgumentError
from petl.test.helpers import ieq, eq_
from petl.transform.regex import capture, split, search, searchcomplement, splitdown
from petl.transform.basics import TransformError


def test_capture():

    table = (('id', 'variable', 'value'),
             ('1', 'A1', '12'),
             ('2', 'A2', '15'),
             ('3', 'B1', '18'),
             ('4', 'C12', '19'))

    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))

    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'))
    ieq(expectation, result)
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                     include_original=False)
    ieq(expectation, result)

    # what about including the original field?
    expectation = (('id', 'variable', 'value', 'treat', 'time'),
                   ('1', 'A1', '12', 'A', '1'),
                   ('2', 'A2', '15', 'A', '2'),
                   ('3', 'B1', '18', 'B', '1'),
                   ('4', 'C12', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)', ('treat', 'time'),
                     include_original=True)
    ieq(expectation, result)

    # what about if number of captured groups is different from new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'),
                   ('4', '19', 'C', '12'))
    result = capture(table, 'variable', '(\\w)(\\d+)')
    ieq(expectation, result)


def test_capture_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = capture(table, 'bar', r'(\w)(\d)', ('baz', 'qux'))
    ieq(expect, actual)


def test_capture_headerless():
    table = []
    with pytest.raises(ArgumentError):
        for i in capture(table, 'bar', r'(\w)(\d)', ('baz', 'qux')):
            pass


def test_capture_nonmatching():

    table = (('id', 'variable', 'value'),
             ('1', 'A1', '12'),
             ('2', 'A2', '15'),
             ('3', 'B1', '18'),
             ('4', 'C12', '19'))

    expectation = (('id', 'value', 'treat', 'time'),
                   ('1', '12', 'A', '1'),
                   ('2', '15', 'A', '2'),
                   ('3', '18', 'B', '1'))

    # default behaviour, raise exception
    result = capture(table, 'variable', r'([A-B])(\d+)', ('treat', 'time'))
    it = iter(result)
    eq_(expectation[0], next(it))  # header
    eq_(expectation[1], next(it))
    eq_(expectation[2], next(it))
    eq_(expectation[3], next(it))
    try:
        next(it)  # doesn't match
    except TransformError:
        pass  # expected
    else:
        assert False, 'expected exception'

    # explicit fill
    result = capture(table, 'variable', r'([A-B])(\d+)',
                     newfields=('treat', 'time'), fill=['', 0])
    it = iter(result)
    eq_(expectation[0], next(it))  # header
    eq_(expectation[1], next(it))
    eq_(expectation[2], next(it))
    eq_(expectation[3], next(it))
    eq_(('4', '19', '', 0), next(it))


def test_split():

    table = (('id', 'variable', 'value'),
             ('1', 'parad1', '12'),
             ('2', 'parad2', '15'),
             ('3', 'tempd1', '18'),
             ('4', 'tempd2', '19'))

    expectation = (('id', 'value', 'variable', 'day'),
                   ('1', '12', 'para', '1'),
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))

    result = split(table, 'variable', 'd', ('variable', 'day'))
    ieq(expectation, result)
    ieq(expectation, result)

    # proper regex
    result = split(table, 'variable', '[Dd]', ('variable', 'day'))
    ieq(expectation, result)

    # integer field reference
    result = split(table, 1, 'd', ('variable', 'day'))
    ieq(expectation, result)

    expectation = (('id', 'variable', 'value', 'variable', 'day'),
                   ('1', 'parad1', '12', 'para', '1'),
                   ('2', 'parad2', '15', 'para', '2'),
                   ('3', 'tempd1', '18', 'temp', '1'),
                   ('4', 'tempd2', '19', 'temp', '2'))

    result = split(table, 'variable', 'd', ('variable', 'day'),
                   include_original=True)
    ieq(expectation, result)

    # what about if no new fields?
    expectation = (('id', 'value'),
                   ('1', '12', 'para', '1'),
                   ('2', '15', 'para', '2'),
                   ('3', '18', 'temp', '1'),
                   ('4', '19', 'temp', '2'))
    result = split(table, 'variable', 'd')
    ieq(expectation, result)


def test_split_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz', 'qux'),)
    actual = split(table, 'bar', 'd', ('baz', 'qux'))
    ieq(expect, actual)


def test_split_headerless():
    table = []
    with pytest.raises(ArgumentError):
        for i in split(table, 'bar', 'd', ('baz', 'qux')):
            pass


def test_search():

    table1 = (('foo', 'bar', 'baz'),
              ('orange', 12, 'oranges are nice fruit'),
              ('mango', 42, 'I like them'),
              ('banana', 74, 'lovely too'),
              ('cucumber', 41, 'better than mango'))

    # search any field
    table2 = search(table1, '.g.')
    expect2 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field
    table3 = search(table1, 'foo', '.g.')
    expect3 = (('foo', 'bar', 'baz'),
               ('orange', 12, 'oranges are nice fruit'),
               ('mango', 42, 'I like them'))
    ieq(expect3, table3)
    ieq(expect3, table3)


def test_search_2():

    # test ported from selectre
    table = (('foo', 'bar', 'baz'),
             ('aa', 4, 9.3),
             ('aaa', 2, 88.2),
             ('b', 1, 23.3),
             ('ccc', 8, 42.0),
             ('bb', 7, 100.9),
             ('c', 2))
    actual = search(table, 'foo', '[ab]{2}')
    expect = (('foo', 'bar', 'baz'),
              ('aa', 4, 9.3),
              ('aaa', 2, 88.2),
              ('bb', 7, 100.9))
    ieq(expect, actual)
    ieq(expect, actual)


def test_search_headerless():
    table = []
    actual = search(table, 'foo', '[ab]{2}')
    expect = []
    ieq(expect, actual)
    ieq(expect, actual)


def test_searchcomplement():

    table1 = (('foo', 'bar', 'baz'),
              ('orange', 12, 'oranges are nice fruit'),
              ('mango', 42, 'I like them'),
              ('banana', 74, 'lovely too'),
              ('cucumber', 41, 'better than mango'))

    # search any field
    table2 = searchcomplement(table1, '.g.')
    expect2 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field
    table3 = searchcomplement(table1, 'foo', '.g.')
    expect3 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect3, table3)
    ieq(expect3, table3)

    # search any field, using complement
    table2 = search(table1, '.g.', complement=True)
    expect2 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # search a specific field, using complement
    table3 = search(table1, 'foo', '.g.', complement=True)
    expect3 = (('foo', 'bar', 'baz'),
               ('banana', 74, 'lovely too'),
               ('cucumber', 41, 'better than mango'))
    ieq(expect3, table3)
    ieq(expect3, table3)


def test_search_unicode():
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4))
    actual = search(tbl, u'.Խա.')
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', 1))
    ieq(expect, actual)
    ieq(expect, actual)


def test_splitdown():

    tbl = ((u'name', u'roles'),
           (u'Jane Doe', u'president,engineer,tailor,lawyer'),
           (u'John Doe', u'rocket scientist,optometrist,chef,knight,sailor'))

    actual = splitdown(tbl, 'roles', ',')
    expect = ((u'name', u'roles'),
              (u'Jane Doe', u'president'),
              (u'Jane Doe', u'engineer'),
              (u'Jane Doe', u'tailor'),
              (u'Jane Doe', u'lawyer'),
              (u'John Doe', u'rocket scientist'),
              (u'John Doe', u'optometrist'),
              (u'John Doe', u'chef'),
              (u'John Doe', u'knight'),
              (u'John Doe', u'sailor'))

    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)
    ieq(expect, actual)


# TODO test sub()


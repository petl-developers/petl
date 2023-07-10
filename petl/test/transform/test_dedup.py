from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import FieldSelectionError
from petl.test.helpers import ieq
from petl.transform.dedup import duplicates, unique, conflicts, distinct, \
    isunique


def test_duplicates():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3))

    result = duplicates(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', u'3', u'7.8', True),
                   ('B', '2', 42),
                   ('D', 'xyz', 9.0),
                   ('D', 4, 12.3))
    ieq(expectation, result)

    # test with compound key
    result = duplicates(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', 42))
    ieq(expectation, result)


def test_duplicates_headerless_no_keys():
    """Removing the duplicates from an empty table without specifying which
    columns shouldn't be a problem.
    """
    table = []
    actual = duplicates(table)
    expect = []
    ieq(expect, actual)


def test_duplicates_headerless_explicit():
    table = []
    with pytest.raises(FieldSelectionError):
        for i in duplicates(table, 'foo'):
            pass


def test_duplicates_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = duplicates(table, key='foo')
    ieq(expect, actual)


def test_duplicates_wholerow():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = duplicates(table)
    expectation = (('foo', 'bar', 'baz'),
                   ('B', '2', '3.4'),
                   ('B', '2', '3.4'))
    ieq(expectation, result)


def test_unique():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('D', 'xyz', 9.0),
             ('B', u'3', u'7.8', True),
             ('B', '2', 42),
             ('E', None),
             ('D', 4, 12.3),
             ('F', 7, 2.3))

    result = unique(table, 'foo')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)

    # test with compound key
    result = unique(table, key=('foo', 'bar'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('B', u'3', u'7.8', True),
                   ('D', 4, 12.3),
                   ('D', 'xyz', 9.0),
                   ('E', None),
                   ('F', 7, 2.3))
    ieq(expectation, result)
    ieq(expectation, result)


def test_unique_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = unique(table, key='foo')
    ieq(expect, actual)


def test_unique_wholerow():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = unique(table)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('D', 4, 12.3))
    ieq(expectation, result)


def test_conflicts():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', None),
             ('D', 'xyz', 9.4),
             ('B', None, u'7.8', True),
             ('E', None),
             ('D', 'xyz', 12.3),
             ('A', 2, None))

    result = conflicts(table, 'foo', missing=None)
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, exclude='baz')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, exclude=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),)
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, include='bar')
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None))
    print(expectation)
    print(list(result))
    ieq(expectation, result)
    ieq(expectation, result)

    result = conflicts(table, 'foo', missing=None, include=('bar', 'baz'))
    expectation = (('foo', 'bar', 'baz'),
                   ('A', 1, 2),
                   ('A', 2, None),
                   ('D', 'xyz', 9.4),
                   ('D', 'xyz', 12.3))
    ieq(expectation, result)
    ieq(expectation, result)


def test_conflicts_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'bar'),)
    actual = conflicts(table, key='foo')
    ieq(expect, actual)


def test_distinct():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3),
             (None, None, None))

    result = distinct(table)
    expect = (('foo', 'bar', 'baz'),
              (None, None, None),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              ('D', 4, 12.3))
    ieq(expect, result)


def test_distinct_count():

    table = (('foo', 'bar', 'baz'),
             (None, None, None),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '3.4'),
             ('D', 4, 12.3))

    result = distinct(table, count='count')
    expect = (('foo', 'bar', 'baz', 'count'),
              (None, None, None, 1),
              ('A', 1, 2, 1),
              ('B', '2', '3.4', 2),
              ('D', 4, 12.3, 1))
    ieq(expect, result)


def test_key_distinct():

    table = (('foo', 'bar', 'baz'),
             (None, None, None),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '5'),
             ('D', 4, 12.3))

    result = distinct(table, key='foo')
    expect = (('foo', 'bar', 'baz'),
              (None, None, None),
              ('A', 1, 2),
              ('B', '2', '3.4'),
              ('D', 4, 12.3))
    ieq(expect, result)


def test_key_distinct_2():
    # test for https://github.com/alimanfoo/petl/issues/318

    tbl = (('a', 'b'),
           ('x', '1'),
           ('x', '3'),
           ('y', '1'),
           (None, None))

    result = distinct(tbl, key='b')
    expect = (('a', 'b'),
              (None, None),
              ('x', '1'),
              ('x', '3'))
    ieq(expect, result)


def test_key_distinct_count():

    table = (('foo', 'bar', 'baz'),
             ('A', 1, 2),
             ('B', '2', '3.4'),
             ('B', '2', '5'),
             ('D', 4, 12.3),
             (None, None, None))

    result = distinct(table, key='foo', count='count')
    expect = (('foo', 'bar', 'baz', 'count'),
              (None, None, None, 1),
              ('A', 1, 2, 1),
              ('B', '2', '3.4', 2),
              ('D', 4, 12.3, 1))
    ieq(expect, result)


def test_isunique():

    table = (('foo', 'bar'), ('a', 1), ('b',), ('b', 2), ('c', 3, True))
    assert not isunique(table, 'foo')
    assert isunique(table, 'bar')

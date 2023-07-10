from __future__ import absolute_import, print_function, division

import pytest

from petl.test.helpers import ieq
from petl.errors import FieldSelectionError
from petl.util import fieldnames
from petl.transform.headers import setheader, extendheader, pushheader, skip, \
    rename, prefixheader, suffixheader, sortheader


def test_setheader():

    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    table2 = setheader(table1, ['foofoo', 'barbar'])
    expect2 = (('foofoo', 'barbar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?


def test_setheader_empty():

    table1 = (('foo', 'bar'),)
    table2 = setheader(table1, ['foofoo', 'barbar'])
    expect2 = (('foofoo', 'barbar'),)
    ieq(expect2, table2)


def test_setheader_headerless():
    table = []
    actual = setheader(table, ['foo', 'bar'])
    expect = [('foo', 'bar')]
    ieq(expect, actual)


def test_extendheader():

    table1 = (('foo',),
              ('a', 1, True),
              ('b', 2, False))
    table2 = extendheader(table1, ['bar', 'baz'])
    expect2 = (('foo', 'bar', 'baz'),
               ('a', 1, True),
               ('b', 2, False))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?


def test_extendheader_empty():

    table1 = (('foo',),)
    table2 = extendheader(table1, ['bar', 'baz'])
    expect2 = (('foo', 'bar', 'baz'),)
    ieq(expect2, table2)


def test_extendheader_headerless():
    table = []
    actual = extendheader(table, ['foo', 'bar'])
    expect = [('foo', 'bar')]
    ieq(expect, actual)
    ieq(expect, actual)


def test_pushheader():

    table1 = (('a', 1),
              ('b', 2))
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?


def test_pushheader_empty():

    table1 = (('a', 1),)
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),
               ('a', 1))
    ieq(expect2, table2)

    table1 = tuple()
    table2 = pushheader(table1, ['foo', 'bar'])
    expect2 = (('foo', 'bar'),)
    ieq(expect2, table2)

    table1 = tuple()
    table2 = pushheader(table1, 'foo', 'bar')
    expect2 = (('foo', 'bar'),)
    ieq(expect2, table2)


def test_pushheader_headerless():
    table = []
    actual = pushheader(table, ['foo', 'bar'])
    expect = [('foo', 'bar')]
    ieq(expect, actual)
    ieq(expect, actual)


def test_pushheader_positional():

    table1 = (('a', 1),
              ('b', 2))
    # positional arguments instead of list
    table2 = pushheader(table1, 'foo', 'bar')
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?

    # test with many fields
    table1 = (('a', 1, 11, 111, 1111),
              ('b', 2, 22, 222, 2222))
    # positional arguments instead of list
    table2 = pushheader(table1, 'foo', 'bar', 'foo1', 'foo2', 'foo3')
    expect2 = (('foo', 'bar', 'foo1', 'foo2', 'foo3'),
               ('a', 1, 11, 111, 1111),
               ('b', 2, 22, 222, 2222))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?

    # test with too few fields in header
    table1 = (('a', 1, 11, 111, 1111),
              ('b', 2, 22, 222, 2222))
    # positional arguments instead of list
    table2 = pushheader(table1, 'foo', 'bar', 'foo1', 'foo2')
    expect2 = (('foo', 'bar', 'foo1', 'foo2'),
               ('a', 1, 11, 111, 1111),
               ('b', 2, 22, 222, 2222))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?

    # test with too many fields in header
    table1 = (('a', 1, 11, 111, 1111),
              ('b', 2, 22, 222, 2222))
    # positional arguments instead of list
    table2 = pushheader(table1, 'foo', 'bar', 'foo1', 'foo2', 'foo3', 'foo4')
    expect2 = (('foo', 'bar', 'foo1', 'foo2', 'foo3', 'foo4'),
               ('a', 1, 11, 111, 1111),
               ('b', 2, 22, 222, 2222))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?


def test_skip():

    table1 = (('#aaa', 'bbb', 'ccc'),
              ('#mmm',),
              ('foo', 'bar'),
              ('a', 1),
              ('b', 2))
    table2 = skip(table1, 2)
    expect2 = (('foo', 'bar'),
               ('a', 1),
               ('b', 2))
    ieq(expect2, table2)
    ieq(expect2, table2)  # can iterate twice?


def test_skip_empty():

    table1 = (('#aaa', 'bbb', 'ccc'),
              ('#mmm',),
              ('foo', 'bar'))
    table2 = skip(table1, 2)
    expect2 = (('foo', 'bar'),)
    ieq(expect2, table2)


def test_skip_headerless():
    table = []
    actual = skip(table, 2)
    expect = []
    ieq(expect, actual)


def test_rename():

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))

    result = rename(table, 'foo', 'foofoo')
    assert fieldnames(result) == ('foofoo', 'bar')

    result = rename(table, 0, 'foofoo')
    assert fieldnames(result) == ('foofoo', 'bar')

    result = rename(table, {'foo': 'foofoo', 'bar': 'barbar'})
    assert fieldnames(result) == ('foofoo', 'barbar')

    result = rename(table, {0: 'baz', 1: 'quux'})
    assert fieldnames(result) == ('baz', 'quux')

    result = rename(table)
    result['foo'] = 'spong'
    assert fieldnames(result) == ('spong', 'bar')


def test_rename_strict():

    table = (('foo', 'bar'),
             ('M', 12),
             ('F', 34),
             ('-', 56))

    result = rename(table, 'baz', 'quux')
    try:
        fieldnames(result)
    except FieldSelectionError:
        pass
    else:
        assert False, 'exception expected'

    result = rename(table, 2, 'quux')
    try:
        fieldnames(result)
    except FieldSelectionError:
        pass
    else:
        assert False, 'exception expected'

    result = rename(table, 'baz', 'quux', strict=False)
    assert fieldnames(result) == ('foo', 'bar')
    result = rename(table, 2, 'quux', strict=False)
    assert fieldnames(result) == ('foo', 'bar')


def test_rename_empty():
    table = (('foo', 'bar'),)
    expect = (('foofoo', 'bar'),)
    actual = rename(table, 'foo', 'foofoo')
    ieq(expect, actual)


def test_rename_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        for i in rename(table, 'foo', 'foofoo'):
            pass


def test_prefixheader():

    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))

    expect = (('pre_foo', 'pre_bar'),
              (1, 'A'),
              (2, 'B'))

    actual = prefixheader(table1, 'pre_')
    ieq(expect, actual)
    ieq(expect, actual)


def test_prefixheader_headerless():
    table = []
    actual = prefixheader(table, 'pre_')
    expect = []
    ieq(expect, actual)


def test_suffixheader():

    table1 = (('foo', 'bar'),
              (1, 'A'),
              (2, 'B'))

    expect = (('foo_suf', 'bar_suf'),
              (1, 'A'),
              (2, 'B'))

    actual = suffixheader(table1, '_suf')
    ieq(expect, actual)
    ieq(expect, actual)


def test_suffixheader_headerless():
    table = []
    actual = suffixheader(table, '_suf')
    expect = []
    ieq(expect, actual)


def test_sortheaders():
    table1 = (
        ('id', 'foo', 'bar', 'baz'),
        ('a', 1, 2, 3),
        ('b', 4, 5, 6))

    expect = (
        ('bar', 'baz', 'foo', 'id'),
        (2, 3, 1, 'a'),
        (5, 6, 4, 'b'),
    )

    actual = sortheader(table1)
    ieq(expect, actual)


def test_sortheaders_duplicate_headers():
    """ Failing test case provided in sortheader()
    with duplicate column names overlays values #392 """
    table1 = (
        ('id', 'foo', 'foo', 'foo'),
        ('a', 1, 2, 3),
        ('b', 4, 5, 6))

    expect = (
        ('foo', 'foo', 'foo', 'id'),
        (1, 2, 3, 'a'),
        (4, 5, 6, 'b'),
    )

    actual = sortheader(table1)
    ieq(expect, actual)


def test_sortheader_headerless():
    table = []
    actual = sortheader(table)
    expect = []
    ieq(expect, actual)

from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import ArgumentError
from petl.test.helpers import ieq
from petl.transform.unpacks import unpack, unpackdict


def test_unpack():

    table1 = (('foo', 'bar'),
              (1, ['a', 'b']),
              (2, ['c', 'd']),
              (3, ['e', 'f']))
    table2 = unpack(table1, 'bar', ['baz', 'quux'])
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check twice

    # check no new fields
    table3 = unpack(table1, 'bar')
    expect3 = (('foo',),
               (1,),
               (2,),
               (3,))
    ieq(expect3, table3)

    # check more values than new fields
    table4 = unpack(table1, 'bar', ['baz'])
    expect4 = (('foo', 'baz'),
               (1, 'a'),
               (2, 'c'),
               (3, 'e'))
    ieq(expect4, table4)

    # check include original
    table5 = unpack(table1, 'bar', ['baz'], include_original=True)
    expect5 = (('foo', 'bar', 'baz'),
               (1, ['a', 'b'], 'a'),
               (2, ['c', 'd'], 'c'),
               (3, ['e', 'f'], 'e'))
    ieq(expect5, table5)

    # check specify number to unpack
    table6 = unpack(table1, 'bar', 3)
    expect6 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', None),
               (2, 'c', 'd', None),
               (3, 'e', 'f', None))
    ieq(expect6, table6)

    # check specify number to unpack, non-default missing value
    table7 = unpack(table1, 'bar', 3, missing='NA')
    expect7 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', 'NA'),
               (2, 'c', 'd', 'NA'),
               (3, 'e', 'f', 'NA'))
    ieq(expect7, table7)

    # check can use field index
    table8 = unpack(table1, 1, 3)
    expect8 = (('foo', 'bar1', 'bar2', 'bar3'),
               (1, 'a', 'b', None),
               (2, 'c', 'd', None),
               (3, 'e', 'f', None))
    ieq(expect8, table8)


def test_unpack_empty():

    table1 = (('foo', 'bar'),)
    table2 = unpack(table1, 'bar', ['baz', 'quux'])
    expect2 = (('foo', 'baz', 'quux'),)
    ieq(expect2, table2)


def test_unpack_headerless():
    table = []
    with pytest.raises(ArgumentError):
        for i in unpack(table, 'bar', ['baz', 'quux']):
            pass


def test_unpackdict():

    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar')
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, 'c', 'd'),
               (3, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check twice

    # test include original
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar', includeoriginal=True)
    expect2 = (('foo', 'bar', 'baz', 'quux'),
               (1, {'baz': 'a', 'quux': 'b'}, 'a', 'b'),
               (2, {'baz': 'c', 'quux': 'd'}, 'c', 'd'),
               (3, {'baz': 'e', 'quux': 'f'}, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check twice

    # test specify keys
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, {'baz': 'c', 'quux': 'd'}),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar', keys=['quux'])
    expect2 = (('foo', 'quux'),
               (1, 'b'),
               (2, 'd'),
               (3, 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check twice

    # test dodgy data
    table1 = (('foo', 'bar'),
              (1, {'baz': 'a', 'quux': 'b'}),
              (2, 'foobar'),
              (3, {'baz': 'e', 'quux': 'f'}))
    table2 = unpackdict(table1, 'bar')
    expect2 = (('foo', 'baz', 'quux'),
               (1, 'a', 'b'),
               (2, None, None),
               (3, 'e', 'f'))
    ieq(expect2, table2)
    ieq(expect2, table2)  # check twice

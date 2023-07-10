from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import FieldSelectionError
from petl.test.helpers import eq_
from petl.util.materialise import columns, facetcolumns


def test_columns():

    table = [['foo', 'bar'], ['a', 1], ['b', 2], ['b', 3]]
    cols = columns(table)
    eq_(['a', 'b', 'b'], cols['foo'])
    eq_([1, 2, 3], cols['bar'])


def test_columns_empty():
    table = [('foo', 'bar')]
    cols = columns(table)
    eq_([], cols['foo'])
    eq_([], cols['bar'])


def test_columns_headerless():
    table = []
    cols = columns(table)
    eq_({}, cols)


def test_facetcolumns():

    table = [['foo', 'bar', 'baz'],
             ['a', 1, True],
             ['b', 2, True],
             ['b', 3]]

    fc = facetcolumns(table, 'foo')
    eq_(['a'], fc['a']['foo'])
    eq_([1], fc['a']['bar'])
    eq_([True], fc['a']['baz'])
    eq_(['b', 'b'], fc['b']['foo'])
    eq_([2, 3], fc['b']['bar'])
    eq_([True, None], fc['b']['baz'])


def test_facetcolumns_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        facetcolumns(table, 'foo')

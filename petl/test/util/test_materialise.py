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


@pytest.mark.parametrize('missing', [None, 'NA', [], {}])
def test_columns_ragged_rows(missing):
    table = [['NA', 'group', 'value'],
             ['kept', 'A', 10, 'extra', 'also extra'],
             ['next', 'A']]

    cols = columns(table, missing=missing)

    eq_({'NA': ['kept', 'next'],
         'group': ['A', 'A'],
         'value': [10, missing]}, cols)
    eq_(['NA', 'group', 'value'], list(cols))


@pytest.mark.parametrize('missing', [None, 'NA', [], {}])
def test_facetcolumns_ragged_rows(missing):
    table = [['NA', 'group', 'value'],
             ['kept', 'A', 10, 'extra', 'also extra'],
             ['next', 'A'],
             ['third', 'B', 20, 'extra'],
             ['last', 'B']]

    actual = facetcolumns(table, 'group', missing=missing)

    eq_({'A': {'NA': ['kept', 'next'],
               'group': ['A', 'A'],
               'value': [10, missing]},
         'B': {'NA': ['third', 'last'],
               'group': ['B', 'B'],
               'value': [20, missing]}}, actual)

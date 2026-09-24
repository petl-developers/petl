# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import pytest

import petl as etl
from petl.compat import integer_types
from petl.test.helpers import ieq
from petl.io.pandas import todataframe, fromdataframe


try:
    import pandas as pd # noqa: F401
except ImportError as e:
    pytest.skip('SKIP pandas tests: %s' % e, allow_module_level=True)
else:

    def test_todataframe():
        tbl = [('foo', 'bar', 'baz'),
               ('apples', 1, 2.5),
               ('oranges', 3, 4.4),
               ('pears', 7, .1)]

        expect = pd.DataFrame.from_records(tbl[1:], columns=tbl[0])
        actual = todataframe(tbl)
        assert expect.equals(actual)

    def test_headerless():
        tbl = []
        expect = pd.DataFrame()
        actual = todataframe(tbl)
        assert expect.equals(actual)

    def test_fromdataframe():
        tbl = [('foo', 'bar', 'baz'),
               ('apples', 1, 2.5),
               ('oranges', 3, 4.4),
               ('pears', 7, .1)]
        df = pd.DataFrame.from_records(tbl[1:], columns=tbl[0])
        ieq(tbl, fromdataframe(df))
        ieq(tbl, fromdataframe(df))

    def test_integration():
        tbl = [('foo', 'bar', 'baz'),
               ('apples', 1, 2.5),
               ('oranges', 3, 4.4),
               ('pears', 7, .1)]
        df = etl.wrap(tbl).todataframe()
        tbl2 = etl.fromdataframe(df)
        ieq(tbl, tbl2)
        ieq(tbl, tbl2)

    @pytest.mark.parametrize('include_index', [False, True])
    @pytest.mark.parametrize('value', [1, 2 ** 53 + 1, -(2 ** 53 + 1)])
    def test_fromdataframe_preserves_integers(value, include_index):
        df = pd.DataFrame({'id': [value], 'score': [0.5]},
                          columns=['id', 'score'])
        table = fromdataframe(df, include_index=include_index)
        expected = [('id', 'score'), (value, 0.5)]
        if include_index:
            expected = [('index', 'id', 'score'), (0, value, 0.5)]
        for _ in range(2):
            actual = list(table)
            assert actual == expected
            assert isinstance(actual[1][int(include_index)], integer_types)

    @pytest.mark.parametrize('include_index', [False, True])
    def test_fromdataframe_multiindex_and_duplicate_columns(include_index):
        index = pd.MultiIndex.from_tuples([('a', 1), ('b', 2)])
        df = pd.DataFrame([[1, 2], [3, 4]], index=index,
                          columns=['not an identifier', 'not an identifier'])
        expected = [('not an identifier', 'not an identifier'), (1, 2), (3, 4)]
        if include_index:
            expected = [('index', 'not an identifier', 'not an identifier'),
                        (('a', 1), 1, 2), (('b', 2), 3, 4)]
        actual = list(fromdataframe(df, include_index=include_index))
        assert actual == expected
        assert all(type(row) is tuple for row in actual)

    @pytest.mark.parametrize('include_index', [False, True])
    def test_fromdataframe_empty(include_index):
        df = pd.DataFrame(columns=['id', 'score'])
        header = ('index', 'id', 'score') if include_index else ('id', 'score')
        assert list(fromdataframe(df, include_index=include_index)) == [header]

    @pytest.mark.parametrize('include_index', [False, True])
    def test_fromdataframe_no_columns(include_index):
        df = pd.DataFrame(index=['a', 'b'])
        expected = [('index',), ('a',), ('b',)] if include_index else [(), (), ()]
        assert list(fromdataframe(df, include_index=include_index)) == expected

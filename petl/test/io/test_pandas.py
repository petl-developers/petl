# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import pytest

import petl as etl
from petl.test.helpers import ieq
from petl.io.pandas import todataframe, fromdataframe


try:
    # noinspection PyUnresolvedReferences
    import pandas as pd
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

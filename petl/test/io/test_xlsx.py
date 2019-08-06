# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import sys
import pkg_resources
from datetime import datetime
from tempfile import NamedTemporaryFile


import petl as etl
from petl.io.xlsx import fromxlsx, toxlsx, appendxlsx
from petl.test.helpers import ieq


try:
    # noinspection PyUnresolvedReferences
    import openpyxl
except ImportError as e:
    print('SKIP xlsx tests: %s' % e, file=sys.stderr)
else:

    def test_fromxlsx():
        filename = pkg_resources.resource_filename(
            'petl', 'test/resources/test.xlsx'
        )
        tbl = fromxlsx(filename, 'Sheet1')
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', datetime(2012, 1, 1)))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_fromxlsx_nosheet():
        filename = pkg_resources.resource_filename(
            'petl', 'test/resources/test.xlsx'
        )
        tbl = fromxlsx(filename)
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', datetime(2012, 1, 1)))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_fromxlsx_range():
        filename = pkg_resources.resource_filename(
            'petl', 'test/resources/test.xlsx'
        )
        tbl = fromxlsx(filename, 'Sheet2', range_string='B2:C6')
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', datetime(2012, 1, 1)))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_fromxlsx_offset():
        filename = pkg_resources.resource_filename(
            'petl', 'test/resources/test.xlsx'
        )
        tbl = fromxlsx(filename, 'Sheet1', min_row=2, min_col=2)
        expect = ((1,),
                  (2,),
                  (2,),
                  (datetime(2012, 1, 1, 0, 0),))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_toxlsx_appendxlsx():

        # setup
        tbl = (('foo', 'bar'),
               ('A', 1),
               ('B', 2),
               ('C', 2),
               (u'é', datetime(2012, 1, 1)))
        f = NamedTemporaryFile(delete=False, suffix='.xlsx')
        f.close()

        # test toxlsx
        toxlsx(tbl, f.name, 'Sheet1')
        actual = fromxlsx(f.name, 'Sheet1')
        ieq(tbl, actual)

        # test appendxlsx
        appendxlsx(tbl, f.name, 'Sheet1')
        expect = etl.cat(tbl, tbl)
        ieq(expect, actual)

    def test_toxlsx_nosheet():
        tbl = (('foo', 'bar'),
               ('A', 1),
               ('B', 2),
               ('C', 2),
               (u'é', datetime(2012, 1, 1)))
        f = NamedTemporaryFile(delete=False, suffix='.xlsx')
        f.close()
        toxlsx(tbl, f.name)
        actual = fromxlsx(f.name)
        ieq(tbl, actual)

    def test_integration():
        tbl = (('foo', 'bar'),
               ('A', 1),
               ('B', 2),
               ('C', 2),
               (u'é', datetime(2012, 1, 1)))
        f = NamedTemporaryFile(delete=False, suffix='.xlsx')
        f.close()
        tbl = etl.wrap(tbl)
        tbl.toxlsx(f.name, 'Sheet1')
        actual = etl.fromxlsx(f.name, 'Sheet1')
        ieq(tbl, actual)
        tbl.appendxlsx(f.name, 'Sheet1')
        expect = tbl.cat(tbl)
        ieq(expect, actual)


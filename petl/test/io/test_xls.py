# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import sys
from datetime import datetime
from tempfile import NamedTemporaryFile

import petl as etl
from petl.io.xls import fromxls, toxls
from petl.test.helpers import ieq


def _get_test_xls():
    try:
        import pkg_resources
        return pkg_resources.resource_filename('petl', 'test/resources/test.xls')
    except:
        return None


try:
    # noinspection PyUnresolvedReferences
    import xlrd
    # noinspection PyUnresolvedReferences
    import xlwt
except ImportError as e:
    print('SKIP xls tests: %s' % e, file=sys.stderr)
else:

    def test_fromxls():
        filename = _get_test_xls()
        if filename is None:
            return
        tbl = fromxls(filename, 'Sheet1')
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', datetime(2012, 1, 1)))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_fromxls_nosheet():
        filename = _get_test_xls()
        if filename is None:
            return
        tbl = fromxls(filename)
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', datetime(2012, 1, 1)))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_fromxls_use_view():
        filename = _get_test_xls()
        if filename is None:
            return
        tbl = fromxls(filename, 'Sheet1', use_view=False)
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2),
                  (u'é', 40909.0))
        ieq(expect, tbl)
        ieq(expect, tbl)

    def test_toxls():
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2))
        f = NamedTemporaryFile(delete=False)
        f.close()
        toxls(expect, f.name, 'Sheet1')
        actual = fromxls(f.name, 'Sheet1')
        ieq(expect, actual)
        ieq(expect, actual)

    def test_toxls_date():
        expect = (('foo', 'bar'),
                  (u'é', datetime(2012, 1, 1)),
                  (u'éé', datetime(2013, 2, 22)))
        f = NamedTemporaryFile(delete=False)
        f.close()
        toxls(expect, f.name, 'Sheet1',
              styles={'bar': xlwt.easyxf(num_format_str='DD/MM/YYYY')})
        actual = fromxls(f.name, 'Sheet1')
        ieq(expect, actual)

    def test_integration():
        expect = (('foo', 'bar'),
                  ('A', 1),
                  ('B', 2),
                  ('C', 2))
        f = NamedTemporaryFile(delete=False)
        f.close()
        etl.wrap(expect).toxls(f.name, 'Sheet1')
        actual = etl.fromxls(f.name, 'Sheet1')
        ieq(expect, actual)
        ieq(expect, actual)

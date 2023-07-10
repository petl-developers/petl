# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


from datetime import datetime
from tempfile import NamedTemporaryFile

import pytest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

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
    pytest.skip('SKIP xls tests: %s' % e, allow_module_level=True)
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

    def test_toxls_headerless():
        expect = []
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

    def test_passing_kwargs_to_xlutils_view():
        filename = _get_test_xls()
        if filename is None:
            return

        from petl.io.xlutils_view import View
        org_init = View.__init__

        def wrapper(self, *args, **kwargs):
            assert "ignore_workbook_corruption" in kwargs
            return org_init(self, *args, **kwargs)

        with patch("petl.io.xlutils_view.View.__init__", wrapper):
            tbl = fromxls(filename, 'Sheet1', use_view=True, ignore_workbook_corruption=True)
            expect = (('foo', 'bar'),
                      ('A', 1),
                      ('B', 2),
                      ('C', 2),
                      (u'é', datetime(2012, 1, 1)))
            ieq(expect, tbl)
            ieq(expect, tbl)

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from datetime import datetime
from tempfile import NamedTemporaryFile

import pytest

import petl as etl
from petl.io.xlsx import fromxlsx, toxlsx, appendxlsx
from petl.test.helpers import ieq, eq_

openpyxl = pytest.importorskip("openpyxl")


@pytest.fixture()
def xlsx_test_filename():
    pkg_resources = pytest.importorskip("pkg_resources") # conda is missing pkg_resources
    return pkg_resources.resource_filename('petl', 'test/resources/test.xlsx')


@pytest.fixture(scope="module")
def xlsx_test_table():
    return (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 2),
              (u'é', datetime(2012, 1, 1)))


@pytest.fixture(scope="module")
def xlsx_table_with_non_str_header():
    class Header:
        def __init__(self, name):
            self.__name = name

        def __str__(self):
            return self.__name

        def __eq__(self, other):
            return str(other) == str(self)

    return ((Header('foo'), Header('bar')),
            ('A', 1),
            ('B', 2),
            ('C', 2))


def test_fromxlsx(xlsx_test_table, xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, 'Sheet1')
    expect = xlsx_test_table
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_read_only(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, sheet='Sheet1', read_only=True)
    expect = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 2),
              (u'é', datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_nosheet(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename)
    expect = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 2),
              (u'é', datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_range(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, 'Sheet2', range_string='B2:C6')
    expect = (('foo', 'bar'),
              ('A', 1),
              ('B', 2),
              ('C', 2),
              (u'é', datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_offset(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, 'Sheet1', min_row=2, min_col=2)
    expect = ((1,),
              (2,),
              (2,),
              (datetime(2012, 1, 1, 0, 0),))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_toxlsx_appendxlsx(xlsx_test_table):

    # setup
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    # test toxlsx
    toxlsx(xlsx_test_table, f.name, 'Sheet1')
    actual = fromxlsx(f.name, 'Sheet1')
    ieq(xlsx_test_table, actual)

    # test appendxlsx
    appendxlsx(xlsx_test_table, f.name, 'Sheet1')
    expect = etl.cat(xlsx_test_table, xlsx_test_table)
    ieq(expect, actual)


def test_toxlsx_nosheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()
    toxlsx(xlsx_test_table, f.name)
    actual = fromxlsx(f.name)
    ieq(xlsx_test_table, actual)


def test_integration(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()
    tbl = etl.wrap(xlsx_test_table)
    tbl.toxlsx(f.name, 'Sheet1')
    actual = etl.fromxlsx(f.name, 'Sheet1')
    ieq(tbl, actual)
    tbl.appendxlsx(f.name, 'Sheet1')
    expect = tbl.cat(tbl)
    ieq(expect, actual)


def test_toxlsx_overwrite(xlsx_test_table):
    f = NamedTemporaryFile(delete=False, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="overwrite")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_file(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="overwrite")
    toxlsx(xlsx_test_table, f.name, sheet=None, mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_sheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="overwrite")
    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_sheet_nofile(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_add_nosheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="overwrite")
    toxlsx(xlsx_test_table, f.name, None, mode="add")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(2, len(wb.sheetnames))


def test_toxlsx_add_sheet_nomatch(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_test_table, f.name, 'Sheet1', mode="overwrite")
    toxlsx(xlsx_test_table, f.name, 'Sheet2', mode="add")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(2, len(wb.sheetnames))


def test_toxlsx_add_sheet_match(xlsx_test_table):
    tbl = xlsx_test_table
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(tbl, f.name, 'Sheet1', mode="overwrite")
    with pytest.raises(ValueError) as excinfo:
        toxlsx(tbl, f.name, 'Sheet1', mode="add")
    assert 'Sheet Sheet1 already exists in file' in str(excinfo.value)


def test_toxlsx_with_non_str_header(xlsx_table_with_non_str_header):
    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    toxlsx(xlsx_table_with_non_str_header, f.name, 'Sheet1')
    actual = etl.fromxlsx(f.name, 'Sheet1')
    ieq(xlsx_table_with_non_str_header, actual)


def test_appendxlsx_with_non_str_header(xlsx_table_with_non_str_header, xlsx_test_table):

    f = NamedTemporaryFile(delete=True, suffix='.xlsx')
    f.close()

    # write first table
    toxlsx(xlsx_test_table, f.name, 'Sheet1')
    actual = fromxlsx(f.name, 'Sheet1')
    ieq(xlsx_test_table, actual)

    # test appendxlsx
    appendxlsx(xlsx_table_with_non_str_header, f.name, 'Sheet1')
    expect = etl.cat(xlsx_test_table, xlsx_table_with_non_str_header)
    ieq(expect, actual)


def test_toxlsx_headerless():
    expect = []
    f = NamedTemporaryFile(delete=False)
    f.close()
    toxlsx(expect, f.name)
    actual = fromxlsx(f.name)
    ieq(expect, actual)
    ieq(expect, actual)

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from datetime import datetime
from io import BytesIO
from tempfile import NamedTemporaryFile
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import pytest

import petl as etl
from petl.io.xlsx import fromxlsx, toxlsx, appendxlsx
from petl.test.helpers import ieq, eq_

openpyxl = pytest.importorskip("openpyxl")

try:
    from importlib.resources import files
except ImportError:  # Python < 3.9
    from importlib_resources import files


@pytest.fixture()
def xlsx_test_filename():
    return str(files("petl") / "test/resources/test.xlsx")


@pytest.fixture()
def xlsm_filename(tmp_path):
    # Build a workbook with an opaque marker in place of executable VBA.
    book = openpyxl.Workbook()
    book.active.title = 'Data'
    book.active.append(['old'])
    book.create_sheet('Other').append(['untouched'])
    original = BytesIO()
    book.save(original)
    filename = str(tmp_path / 'example.xlsm')
    with ZipFile(original) as src, ZipFile(filename, 'w') as dest:
        for name in src.namelist():
            blob = src.read(name)
            if name == '[Content_Types].xml':
                root = ET.fromstring(blob)
                for part in root:
                    if part.get('PartName') == '/xl/workbook.xml':
                        part.set('ContentType', 'application/vnd.ms-excel.sheet.macroEnabled.main+xml')
                ET.SubElement(root, '{http://schemas.openxmlformats.org/package/2006/content-types}Default', Extension='bin',
                              ContentType='application/vnd.ms-office.vbaProject')
                blob = ET.tostring(root)
            elif name == 'xl/_rels/workbook.xml.rels':
                root = ET.fromstring(blob)
                ET.SubElement(root, root[0].tag, Id='rIdVba', Target='vbaProject.bin',
                              Type='http://schemas.microsoft.com/office/2006/relationships/vbaProject')
                blob = ET.tostring(root)
            dest.writestr(name, blob)
        dest.writestr('xl/vbaProject.bin', b'PETL test project')
    return filename


@pytest.mark.parametrize('mode,sheet,options,preserved', [
    ('replace', 'Data', {'keep_vba': True}, True),
    ('add', 'Added', {'keep_vba': True}, True),
    ('add', None, {'keep_vba': True, 'write_header': False}, True),
    ('replace', 'Data', {}, False),
    ('replace', 'Data', {'keep_vba': False}, False),
    ('overwrite', 'Data', {'keep_vba': True}, False),
    ('replace', None, {'keep_vba': True}, False),
])
def test_toxlsx_keep_vba(xlsm_filename, mode, sheet, options, preserved):
    rows = [('name',), ('new',)]
    toxlsx(rows, xlsm_filename, sheet=sheet, mode=mode, **options)
    result_sheet = -1 if mode == 'add' else sheet
    expected = rows if options.get('write_header', True) else rows[1:]
    assert list(fromxlsx(xlsm_filename, result_sheet)) == expected
    if mode == 'add':
        assert list(fromxlsx(xlsm_filename, 'Data')) == [('old',)]
    if mode == 'add' or (mode == 'replace' and sheet is not None):
        assert list(fromxlsx(xlsm_filename, 'Other')) == [('untouched',)]
    with ZipFile(xlsm_filename) as archive:
        assert ('xl/vbaProject.bin' in archive.namelist()) == preserved
        if preserved:
            assert archive.read('xl/vbaProject.bin') == b'PETL test project'
        types = ET.fromstring(archive.read('[Content_Types].xml'))
        workbook_type = next(p.get('ContentType') for p in types
                             if p.get('PartName') == '/xl/workbook.xml')
        assert ('macroEnabled' in workbook_type) == preserved
        rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
        assert any(r.get('Type').endswith('/vbaProject') and
                   r.get('Target') == 'vbaProject.bin' for r in rels) == preserved


@pytest.mark.parametrize('mode', ['replace', 'add'])
def test_keep_vba_closes_archive(xlsm_filename, monkeypatch, mode):
    archives = []
    load = openpyxl.load_workbook

    def track_archive(*args, **kwargs):
        book = load(*args, **kwargs)
        archives.append(book.vba_archive)
        return book

    monkeypatch.setattr(openpyxl, 'load_workbook', track_archive)
    if mode == 'add':
        with pytest.raises(ValueError, match='already exists'):
            toxlsx([('value',)], xlsm_filename, sheet='Data', mode=mode, keep_vba=True)
    else:
        toxlsx([('value',)], xlsm_filename, sheet='Data', mode=mode, keep_vba=True)
    assert len(archives) == 1
    assert archives[0].fp is None


@pytest.mark.parametrize('macro_enabled', [False, True])
def test_keep_vba_without_project(tmp_path, macro_enabled):
    original = BytesIO()
    openpyxl.Workbook().save(original)
    filename = tmp_path / ('empty.xlsm' if macro_enabled else 'plain.xlsx')
    with ZipFile(original) as src, ZipFile(str(filename), 'w') as dest:
        for name in src.namelist():
            blob = src.read(name)
            if macro_enabled and name == '[Content_Types].xml':
                blob = blob.replace(b'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml',
                                    b'application/vnd.ms-excel.sheet.macroEnabled.main+xml')
            dest.writestr(name, blob)
    before = filename.read_bytes()
    with pytest.raises(ValueError, match='no VBA project'):
        toxlsx([('value',)], str(filename), sheet='Sheet', keep_vba=True)
    assert filename.read_bytes() == before


@pytest.fixture(scope="module")
def xlsx_test_table():
    return (("foo", "bar"), ("A", 1), ("B", 2), ("C", 2), (u"é", datetime(2012, 1, 1)))


@pytest.fixture(scope="module")
def xlsx_table_with_non_str_header():
    class Header:
        def __init__(self, name):
            self.__name = name

        def __str__(self):
            return self.__name

        def __eq__(self, other):
            return str(other) == str(self)

    return ((Header("foo"), Header("bar")), ("A", 1), ("B", 2), ("C", 2))


def test_fromxlsx(xlsx_test_table, xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, "Sheet1")
    expect = xlsx_test_table
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_read_only(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, sheet="Sheet1", read_only=True)
    expect = (("foo", "bar"), ("A", 1), ("B", 2), ("C", 2), (u"é", datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


@pytest.mark.parametrize("action", ["exhaust", "close", "throw", "bad_sheet"])
def test_fromxlsx_closes_read_only_workbook(xlsx_test_filename, monkeypatch,
                                           action):
    load_workbook = openpyxl.load_workbook
    workbooks = []

    def track_workbook(*args, **kwargs):
        workbook = load_workbook(*args, **kwargs)
        workbooks.append(workbook)
        return workbook

    monkeypatch.setattr(openpyxl, 'load_workbook', track_workbook)
    sheet = 'missing' if action == 'bad_sheet' else 'Sheet1'
    rows = iter(fromxlsx(xlsx_test_filename, sheet=sheet, read_only=True))
    try:
        if action == 'exhaust':
            list(rows)
        elif action == 'bad_sheet':
            with pytest.raises(KeyError):
                next(rows)
        else:
            next(rows)
            if action == 'close':
                rows.close()
            else:
                with pytest.raises(RuntimeError, match='consumer failure'):
                    rows.throw(RuntimeError('consumer failure'))
        assert len(workbooks) == 1
        assert workbooks[0]._archive.fp is None
    finally:
        rows.close()
        for workbook in workbooks:
            workbook.close()


def test_fromxlsx_nosheet(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename)
    expect = (("foo", "bar"), ("A", 1), ("B", 2), ("C", 2), (u"é", datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_fromxlsx_range(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, "Sheet2", range_string="B2:C6")
    expect = (("foo", "bar"), ("A", 1), ("B", 2), ("C", 2), (u"é", datetime(2012, 1, 1)))
    ieq(expect, tbl)
    ieq(expect, tbl)


@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("range_string, expect", [
    ("B2", ((1,),)),
    ("B2:B2", ((1,),)),
    ("$B$2", ((1,),)),
    ("1", (("foo", "bar"),)),
    ("1:1", (("foo", "bar"),)),
    ("2:3", (("A", 1), ("B", 2))),
    ("A1:B1", (("foo", "bar"),)),
])
def test_fromxlsx_range_shapes(xlsx_test_filename, read_only,
                               range_string, expect):
    tbl = fromxlsx(xlsx_test_filename, range_string=range_string,
                   read_only=read_only)
    ieq(expect, tbl)
    ieq(expect, tbl)


@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("range_string, column_count", [
    ("A", 1), ("A:A", 1), ("A:B", 2),
])
def test_fromxlsx_column_ranges(xlsx_test_filename, xlsx_test_table,
                                read_only, range_string, column_count):
    tbl = fromxlsx(xlsx_test_filename, range_string=range_string,
                   read_only=read_only)
    expect = [row[:column_count] for row in xlsx_test_table]
    ieq(expect, tbl)
    ieq(expect, tbl)


@pytest.mark.parametrize("read_only", [False, True])
def test_fromxlsx_range_overrides_bounds(xlsx_test_filename, read_only):
    tbl = fromxlsx(xlsx_test_filename, range_string="A1:B1",
                   min_row=2, max_row=3, min_col=2, max_col=2,
                   read_only=read_only)
    ieq((("foo", "bar"),), tbl)


@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("range_string, error", [
    ("", IndexError),
    ("A1:B", ValueError),
    ("B0", ValueError),
    ("A0:B0", ValueError),
])
def test_fromxlsx_invalid_range(xlsx_test_filename, read_only,
                                range_string, error):
    with pytest.raises(error):
        list(fromxlsx(xlsx_test_filename, range_string=range_string,
                      read_only=read_only))


def test_fromxlsx_offset(xlsx_test_filename):
    tbl = fromxlsx(xlsx_test_filename, "Sheet1", min_row=2, min_col=2)
    expect = ((1,), (2,), (2,), (datetime(2012, 1, 1, 0, 0),))
    ieq(expect, tbl)
    ieq(expect, tbl)


def test_toxlsx_appendxlsx(xlsx_test_table):

    # setup
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    # test toxlsx
    toxlsx(xlsx_test_table, f.name, "Sheet1")
    actual = fromxlsx(f.name, "Sheet1")
    ieq(xlsx_test_table, actual)

    # test appendxlsx
    appendxlsx(xlsx_test_table, f.name, "Sheet1")
    expect = etl.cat(xlsx_test_table, xlsx_test_table)
    ieq(expect, actual)


def test_toxlsx_nosheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()
    toxlsx(xlsx_test_table, f.name)
    actual = fromxlsx(f.name)
    ieq(xlsx_test_table, actual)


def test_integration(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()
    tbl = etl.wrap(xlsx_test_table)
    tbl.toxlsx(f.name, "Sheet1")
    actual = etl.fromxlsx(f.name, "Sheet1")
    ieq(tbl, actual)
    tbl.appendxlsx(f.name, "Sheet1")
    expect = tbl.cat(tbl)
    ieq(expect, actual)


def test_toxlsx_overwrite(xlsx_test_table):
    f = NamedTemporaryFile(delete=False, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="overwrite")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_file(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="overwrite")
    toxlsx(xlsx_test_table, f.name, sheet=None, mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_sheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="overwrite")
    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_replace_merged_sheet(tmpdir, xlsx_test_table):
    filename = str(tmpdir.join("merged.xlsx"))
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "data"
    sheet.merge_cells("A1:B1")
    sheet.merge_cells("A2:A3")
    other = workbook.create_sheet("other")
    other["A1"] = "keep"
    other.merge_cells("A1:B1")
    workbook.save(filename)
    workbook.close()

    toxlsx(xlsx_test_table, filename, "data", mode="replace")

    workbook = openpyxl.load_workbook(filename)
    try:
        assert workbook.sheetnames == ["data", "other"]
        assert not workbook["data"].merged_cells.ranges
        assert list(workbook["data"].values) == list(xlsx_test_table)
        assert workbook["other"]["A1"].value == "keep"
        assert str(workbook["other"].merged_cells) == "A1:B1"
    finally:
        workbook.close()


def test_toxlsx_replace_overlapping_merges(tmpdir):
    filename = str(tmpdir.join("overlapping.xlsx"))
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "data"
    sheet.merge_cells("A1:C1")
    sheet.merge_cells("B1:D1")
    workbook.save(filename)
    workbook.close()

    with pytest.raises(KeyError):
        toxlsx([("a", "b", "c", "d")], filename, "data", mode="replace")


def test_toxlsx_replace_sheet_nofile(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="replace")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(1, len(wb.sheetnames))


def test_toxlsx_add_nosheet(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="overwrite")
    toxlsx(xlsx_test_table, f.name, None, mode="add")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(2, len(wb.sheetnames))


def test_toxlsx_add_sheet_nomatch(xlsx_test_table):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_test_table, f.name, "Sheet1", mode="overwrite")
    toxlsx(xlsx_test_table, f.name, "Sheet2", mode="add")
    wb = openpyxl.load_workbook(f.name, read_only=True)
    eq_(2, len(wb.sheetnames))


def test_toxlsx_add_sheet_match(xlsx_test_table):
    tbl = xlsx_test_table
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(tbl, f.name, "Sheet1", mode="overwrite")
    with pytest.raises(ValueError) as excinfo:
        toxlsx(tbl, f.name, "Sheet1", mode="add")
    assert "Sheet Sheet1 already exists in file" in str(excinfo.value)


def test_toxlsx_with_non_str_header(xlsx_table_with_non_str_header):
    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    toxlsx(xlsx_table_with_non_str_header, f.name, "Sheet1")
    actual = etl.fromxlsx(f.name, "Sheet1")
    ieq(xlsx_table_with_non_str_header, actual)


def test_appendxlsx_with_non_str_header(
    xlsx_table_with_non_str_header, xlsx_test_table
):

    f = NamedTemporaryFile(delete=True, suffix=".xlsx")
    f.close()

    # write first table
    toxlsx(xlsx_test_table, f.name, "Sheet1")
    actual = fromxlsx(f.name, "Sheet1")
    ieq(xlsx_test_table, actual)

    # test appendxlsx
    appendxlsx(xlsx_table_with_non_str_header, f.name, "Sheet1")
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

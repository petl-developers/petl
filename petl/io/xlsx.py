# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import locale


from petl.util.base import Table


def fromxlsx(filename, sheet=None, range_string=None, row_offset=0,
             column_offset=0, **kwargs):
    """
    Extract a table from a sheet in an Excel .xlsx file.
    
    N.B., the sheet name is case sensitive.

    The `sheet` argument can be omitted, in which case the first sheet in
    the workbook is used by default.

    The `range_string` argument can be used to provide a range string
    specifying a range of cells to extract.

    The `row_offset` and `column_offset` arguments can be used to
    specify offsets.

    Any other keyword arguments are passed through to
    :func:`openpyxl.load_workbook()`.

    """

    return XLSXView(filename, sheet=sheet, range_string=range_string,
                    row_offset=row_offset, column_offset=column_offset,
                    **kwargs)


class XLSXView(Table):
    
    def __init__(self, filename, sheet=None, range_string=None,
                 row_offset=0, column_offset=0, **kwargs):
        self.filename = filename
        self.sheet = sheet
        self.range_string = range_string
        self.row_offset = row_offset
        self.column_offset = column_offset
        self.kwargs = kwargs

    def __iter__(self):
        import openpyxl
        wb = openpyxl.load_workbook(filename=self.filename,
                                    use_iterators=True, **self.kwargs)
        if self.sheet is None:
            ws = wb.get_sheet_by_name(wb.get_sheet_names()[0])
        elif isinstance(self.sheet, int):
            ws = wb.get_sheet_by_name(wb.get_sheet_names()[self.sheet])
        else:
            ws = wb.get_sheet_by_name(str(self.sheet))

        for row in ws.iter_rows(range_string=self.range_string,
                                row_offset=self.row_offset,
                                column_offset=self.column_offset):
            yield tuple(cell.value for cell in row)


def toxlsx(tbl, filename, sheet=None, encoding=None):
    """
    Write a table to a new Excel .xlsx file.

    """

    import openpyxl
    if encoding is None:
        encoding = locale.getpreferredencoding()
    wb = openpyxl.Workbook(optimized_write=True, encoding=encoding)
    ws = wb.create_sheet(title=sheet)
    for row in tbl:
        ws.append(row)
    wb.save(filename)


Table.toxlsx = toxlsx

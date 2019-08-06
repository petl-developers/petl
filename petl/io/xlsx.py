# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import locale


from petl.util.base import Table


def fromxlsx(filename, sheet=None, range_string=None, min_row=None,
             min_col=None, max_row=None, max_col=None, **kwargs):
    """
    Extract a table from a sheet in an Excel .xlsx file.

    N.B., the sheet name is case sensitive.

    The `sheet` argument can be omitted, in which case the first sheet in
    the workbook is used by default.

    The `range_string` argument can be used to provide a range string
    specifying a range of cells to extract.

    The `min_row`, `min_col`, `max_row` and `max_col` arguments can be
    used to limit the range of cells to extract. They will be ignored
    if `range_string` is provided.

    Any other keyword arguments are passed through to
    :func:`openpyxl.load_workbook()`.

    """

    return XLSXView(filename, sheet=sheet, range_string=range_string,
                    min_row=min_row, min_col=min_col, max_row=max_row,
                    max_col=max_col, **kwargs)


class XLSXView(Table):

    def __init__(self, filename, sheet=None, range_string=None,
                 min_row=None, min_col=None, max_row=None, max_col=None, **kwargs):
        self.filename = filename
        self.sheet = sheet
        self.range_string = range_string
        self.min_row = min_row
        self.min_col = min_col
        self.max_row = max_row
        self.max_col = max_col
        self.kwargs = kwargs

    def __iter__(self):
        import openpyxl
        wb = openpyxl.load_workbook(filename=self.filename,
                                    read_only=True, **self.kwargs)
        if self.sheet is None:
            ws = wb[wb.sheetnames[0]]
        elif isinstance(self.sheet, int):
            ws = wb[wb.sheetnames[self.sheet]]
        else:
            ws = wb[str(self.sheet)]

        if self.range_string is not None:
            rows = ws[self.range_string]
        else:
            rows = ws.iter_rows(min_row=self.min_row,
                                min_col=self.min_col,
                                max_row=self.max_row,
                                max_col=self.max_col)

        for row in rows:
            yield tuple(cell.value for cell in row)

        try:
            wb._archive.close()
        except AttributeError:
            # just here in case openpyxl stops exposing an _archive property.
            pass


def toxlsx(tbl, filename, sheet=None):
    """
    Write a table to a new Excel .xlsx file.

    """

    import openpyxl
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(title=sheet)
    for row in tbl:
        ws.append(row)
    wb.save(filename)


Table.toxlsx = toxlsx

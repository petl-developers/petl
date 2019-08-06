# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import locale


from petl.compat import izip_longest, next, xrange
from petl.util.base import Table


def fromxls(filename, sheet=None, use_view=True, **kwargs):
    """
    Extract a table from a sheet in an Excel .xls file.
    
    Sheet is identified by its name or index number.
    
    N.B., the sheet name is case sensitive.

    """
    
    return XLSView(filename, sheet=sheet, use_view=use_view, **kwargs)


class XLSView(Table):

    def __init__(self, filename, sheet=None, use_view=True, **kwargs):
        self.filename = filename
        self.sheet = sheet
        self.use_view = use_view
        self.kwargs = kwargs

    def __iter__(self):

        # prefer implementation using xlutils.view as dates are automatically
        # converted
        if self.use_view:
            from petl.io import xlutils_view
            wb = xlutils_view.View(self.filename)
            if self.sheet is None:
                ws = wb[0]
            else:
                ws = wb[self.sheet]
            for row in ws:
                yield tuple(row)

        else:
            import xlrd
            with xlrd.open_workbook(filename=self.filename,
                                    on_demand=True, **self.kwargs) as wb:
                if self.sheet is None:
                    ws = wb.sheet_by_index(0)
                elif isinstance(self.sheet, int):
                    ws = wb.sheet_by_index(self.sheet)
                else:
                    ws = wb.sheet_by_name(str(self.sheet))
                for rownum in xrange(ws.nrows):
                    yield tuple(ws.row_values(rownum))


def toxls(tbl, filename, sheet, encoding=None, style_compression=0,
          styles=None):
    """
    Write a table to a new Excel .xls file.

    """

    import xlwt
    if encoding is None:
        encoding = locale.getpreferredencoding()
    wb = xlwt.Workbook(encoding=encoding, style_compression=style_compression)
    ws = wb.add_sheet(sheet)

    if styles is None:
        # simple version, don't worry about styles
        for r, row in enumerate(tbl):
            for c, v in enumerate(row):
                ws.write(r, c, label=v)
    else:
        # handle styles
        it = iter(tbl)
        hdr = next(it)
        flds = list(map(str, hdr))
        for c, f in enumerate(flds):
            ws.write(0, c, label=f)
            if f not in styles or styles[f] is None:
                styles[f] = xlwt.Style.default_style
        # convert to list for easy zipping
        styles = [styles[f] for f in flds]
        for r, row in enumerate(it):
            for c, (v, style) in enumerate(izip_longest(row, styles,
                                                        fillvalue=None)):
                ws.write(r+1, c, label=v, style=style)

    wb.save(filename)


Table.toxls = toxls

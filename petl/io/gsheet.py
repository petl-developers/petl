# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from petl.util.base import Table


def fromgsheet(filename, credentials, filekey=False, sheet=None,
               range_string=None):
    """
    Extract a table from a google spreadsheet.

    The `filename` can either be the key of the spreadsheet or its name. If the
    filename is a key, set `filekey` to True.

    Credentials are used to authenticate with the google apis.
    For more info visit: http://gspread.readthedocs.io/en/latest/oauth2.html

    Set `filekey` to `True` if accessing the sheet from a key rather than name.

    N.B., the sheet name is case sensitive.

    The `sheet` argument can be omitted, in which case the first sheet in
    the workbook is used by default.

    The `range_string` argument can be used to provide a range string
    specifying a range of cells to extract. (i.e. 'A1:C7').

    Example usage follows::
        >>> import petl as etl
        >>> from oauth2client.service_account import ServiceAccountCredentials
        >>> scope = ['https://spreadsheets.google.com/feeds']
        >>> credentials = ServiceAccountCredentials.from_json_keyfile_name('path/to/credentials.json', scope)
        >>> tbl = etl.fromgsheet('example', credentials)

    This module relies heavily on the work by @burnash and his great gspread
    module: http://gspread.readthedocs.io/en/latest/index.html


    """

    return GoogleSheetView(filename, credentials, filekey=filekey, sheet=sheet,
                           range_string=range_string)


class GoogleSheetView(Table):
    """This module resembles XLSXView, as both use abstracted modules."""

    def __init__(self, filename, credentials, filekey, sheet, range_string):
        self.filename = filename
        self.credentials = credentials
        self.filekey = filekey
        self.sheet = sheet
        self.range_string = range_string

    def __iter__(self):
        import gspread
        gspread_client = gspread.authorize(self.credentials)
        # @TODO Find a cleaner way to differentiate between the two
        if self.filekey:
            wb = gspread_client.open_by_key(self.filename)
        else:
            wb = gspread_client.open(self.filename)

        # Allow for user to specify no sheet, sheet index or sheet name
        if self.sheet is None:
            ws = wb.sheet1
        elif isinstance(self.sheet, int):
            ws = wb.get_worksheet(self.sheet)
        else:
            ws = wb.worksheet(str(self.sheet))

        # grab the range or grab the whole sheet
        if self.range_string:
            start, end = self.range_string.split(':')
            start_row, start_col = gspread.utils.a1_to_rowcol(start)
            end_row, end_col = gspread.utils.a1_to_rowcol(end)
            print(start_col, end_col, start_row, end_row)
            for i, row in enumerate(ws.get_all_values(), start=1):
                if i in range(start_row, end_row + 1):
                    machine_start_col = start_col - 1
                    yield tuple(row[machine_start_col:end_col])
        else:
            # This function returns the value of each cell
            for row in ws.get_all_values():
                yield tuple(row)


def togsheet(tbl, filename, credentials, sheet=None, user_email=None):
    """
    Write a table to a new google sheet.

    filename will be the title of the sheet when uploaded to google sheets.

    credentials are the credentials used to authenticate with the google apis.
    For more info visit: http://gspread.readthedocs.io/en/latest/oauth2.html

    If user_email is entered, that will be the account that the sheet will be
    shared to automatically upon creation with write privileges.

    If sheet is specified, the first sheet in the spreadsheet will be renamed
    to sheet.

    Note: necessary scope for using togsheet is:
         'https://spreadsheets.google.com/feeds'
         'https://www.googleapis.com/auth/drive'

    Example usage::
        >>> import petl as etl
        >>> from oauth2client.service_account import ServiceAccountCredentials
        >>> scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
        >>> credentials = ServiceAccountCredentials.from_json_keyfile_name('path/to/credentials.json', scope)
        >>> cols = [[0, 1, 2], ['a', 'b', 'c']]
        >>> tbl = etl.fromcolumns(cols)
        >>> etl.togsheet(tbl, 'example', credentials)
    """

    import gspread
    gspread_client = gspread.authorize(credentials)
    spread = gspread_client.create(filename)
    rows = len(tbl)
    # even in a blank table, the header row will be an empty tuple
    cols = len(tbl[0])
    # output to first sheet
    worksheet = spread.sheet1
    # match row and column length
    worksheet.resize(rows=rows, cols=cols)
    # rename sheet if set
    if sheet:
        worksheet.update_title(title=sheet)
    # enumerate from 1 instead of from 0 (compat. with p2.6+)
    for x, row in enumerate(tbl, start=1):
        for y, val in enumerate(row, start=1):
            worksheet.update_cell(x, y, val)
    # specify the user account to share to
    if user_email:
        spread.share(user_email, perm_type='user', role='writer')


Table.togsheet = togsheet

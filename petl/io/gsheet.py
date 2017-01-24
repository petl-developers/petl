# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from petl.util.base import Table
from petl.compat import text_type


def fromgsheet(filename, credentials, forcename=False, worksheet_title=None,
               range_string=None):
    """
    Extract a table from a google spreadsheet.

    The `filename` can either be the key of the spreadsheet or its name.
    If you want to force the module to see it as a name, set `forcename=True`.
    NOTE: Only the top level of google drive will be searched for the filename
    due to API limitations.

    `credentials` are used to authenticate with the google apis.
    For more info visit: http://gspread.readthedocs.io/en/latest/oauth2.html

    Set `forcename` to `True` in order to treat `filename` as a name

    N.B., the worksheet name is case sensitive.

    The `worksheet_title` argument can be omitted, in which case the first
    sheet in the workbook is used by default.

    The `range_string` argument can be used to provide a range string
    specifying the top left and bottom right corners of a set of cells to
    extract. (i.e. 'A1:C7').

    Example usage follows::
        >>> import petl as etl
        >>> from oauth2client.service_account import ServiceAccountCredentials
        >>> scope = ['https://spreadsheets.google.com/feeds']
        >>> credentials = ServiceAccountCredentials.from_json_keyfile_name('path/to/credentials.json', scope)
        >>> tbl = etl.fromgsheet('example', credentials)
        or
        >>> tbl = etl.fromgsheet('9zDNETemfau0uY8ZJF0YzXEPB_5GQ75JV', credentials)

    This module relies heavily on the work by @burnash and his great gspread
    module: http://gspread.readthedocs.io/en/latest/index.html

    """

    return GoogleSheetView(filename,
                           credentials,
                           forcename=forcename,
                           worksheet_title=worksheet_title,
                           range_string=range_string)


class GoogleSheetView(Table):
    """This module resembles XLSXView."""

    def __init__(self, filename, credentials, forcename, worksheet_title,
                 range_string):
        self.filename = filename
        self.credentials = credentials
        self.forcename = forcename
        self.worksheet_title = worksheet_title
        self.range_string = range_string

    def __iter__(self):
        import gspread
        gspread_client = gspread.authorize(self.credentials)
        if self.forcename:
            wb = gspread_client.open(self.filename)
        else:
            try:
                wb = gspread_client.open_by_key(self.filename)
            except gspread.exceptions.SpreadsheetNotFound:
                wb = gspread_client.open(self.filename)

        # Allow for user to specify no sheet, sheet index or sheet name
        if self.worksheet_title is None:
            ws = wb.sheet1
        elif isinstance(self.worksheet_title, int):
            ws = wb.get_worksheet(self.worksheet_title)
        else:
            # use text_type for cross version compatibility
            ws = wb.worksheet(text_type(self.worksheet_title))

        # grab the range or grab the whole sheet
        if self.range_string:
            # start_cell -> top left, end_cell -> bottom right
            start_cell, end_cell = self.range_string.split(':')
            start_row, start_col = gspread.utils.a1_to_rowcol(start_cell)
            end_row, end_col = gspread.utils.a1_to_rowcol(end_cell)
            # gspread starts its indices at 1
            for i, row in enumerate(ws.get_all_values(), start=1):
                if i in range(start_row, end_row + 1):
                    start_col_index = start_col - 1
                    yield tuple(row[start_col_index:end_col])
        else:
            # no range specified, so return all the rows
            for row in ws.get_all_values():
                yield tuple(row)


def togsheet(tbl, filename, credentials, worksheet_title=None,
             share_emails=[], role='writer'):
    """
    Write a table to a new google sheet.

    `filename` will be the title of the workbook when uploaded to google sheets.

    `credentials` are used to authenticate with the google apis.
    For more info, visit: http://gspread.readthedocs.io/en/latest/oauth2.html

    If `worksheet_title` is specified, the first worksheet in the spreadsheet
    will be renamed to the value of `worksheet_title`.

    The spreadsheet will be shared with all emails in `share_emails` with
    `role` permissions granted.
    For more info, visit: https://developers.google.com/drive/v3/web/manage-sharing

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
    spreadsheet = gspread_client.create(filename)
    worksheet = spreadsheet.sheet1
    # make smallest table possible
    worksheet.resize(rows=1, cols=1)
    # rename sheet if set
    if worksheet_title:
        worksheet.update_title(title=worksheet_title)
    # gspread indices start at 1, therefore row index insert starts at 1
    for index, row in enumerate(tbl, start=1):
        worksheet.insert_row(row, index)
    # specify the user account to share to
    for user_email in share_emails:
        spreadsheet.share(user_email, perm_type='user', role=role)


def appendgsheet(tbl, filename, credentials, worksheet_title="Sheet1"):
    """
    Append a table to an existing google shoot at either a new worksheet
    or the end of an existing worksheet

    `filename` is the name of the workbook to append to.

    `credentials` are used to authenticate with the google apis.
    For more info, visit: http://gspread.readthedocs.io/en/latest/oauth2.html

    `worksheet_title` is the title of the worksheet to append to or create if
    the worksheet does not exist. NOTE: sheet index cannot be used, and None is
    not an option.
    """
    import gspread
    gspread_client = gspread.authorize(credentials)
    # be able to give filename or key for file
    try:
        wb = gspread_client.open_by_key(filename)
    except gspread.exceptions.SpreadsheetNotFound:
        wb = gspread_client.open(filename)
    # check to see if worksheet_title exists, if so append, otherwise create
    if worksheet_title in [worksheet.title for worksheet in wb.worksheets()]:
        worksheet = wb.worksheet(text_type(worksheet_title))
    else:
        worksheet = wb.add_worksheet(text_type(worksheet_title), 1, 1)
    # efficiency loss, but get_all_values() will only return meaningful rows,
    # therefore len(rows) + 1 gives the earliest open insert index
    start_point = len(worksheet.get_all_values()) + 1
    for index, row in enumerate(tbl, start=start_point):
        worksheet.insert_row(row, index)


Table.togsheet = togsheet

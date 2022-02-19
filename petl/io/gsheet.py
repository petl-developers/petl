# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from petl.util.base import Table
from petl.compat import text_type
from petl.errors import ArgumentError as PetlArgError


def _get_gspread_client(auth_info):
    import gspread

    if isinstance(auth_info, gspread.Client):
        return auth_info
    if isinstance(auth_info, dict):
        gd = gspread.service_account_from_dict(auth_info)
        return gd
    import google

    if isinstance(auth_info, google.oauth2.service_account.Credentials):
        gc = gspread.authorize(auth_info)
        return gc
    if auth_info is None:
        ga = gspread.service_account()
        return ga
    raise PetlArgError("gspread: Invalid account credentials")


def _open_spreadsheet(gspread_client, spreadsheet, open_by_key=False):
    if open_by_key:
        from gspread.exceptions import SpreadsheetNotFound
        try:
            wb = gspread_client.open_by_key(spreadsheet)
        except SpreadsheetNotFound:
            wb = gspread_client.open(spreadsheet)
    elif spreadsheet is not None:
        wb = gspread_client.open(spreadsheet)
    else:
        raise PetlArgError("gspread requires argument spreadsheet")
    return wb


def _select_worksheet(wb, worksheet):
    # Allow for user to specify no sheet, sheet index or sheet name
    if worksheet is None:
        ws = wb.sheet1
    elif isinstance(worksheet, int):
        ws = wb.get_worksheet(worksheet)
    else:
        # use text_type for cross version compatibility
        ws = wb.worksheet(text_type(worksheet))
    return ws


def fromgsheet(
    credentials_or_client, spreadsheet, open_by_key=False,
    worksheet=None, cell_range=None
):
    """
    Extract a table from a google spreadsheet.

    The `credentials_or_client` are used to authenticate with the google apis.
    For more info, check `authentication`_. 

    The `spreadsheet` can either be the key of the spreadsheet or its name.

    Set `open_by_key` to `True` in order to treat `spreadsheet` as spreadsheet key.

    The `worksheet` argument can be omitted, in which case the first
    sheet in the workbook is used by default.

    The `cell_range` argument can be used to provide a range string
    specifying the top left and bottom right corners of a set of cells to
    extract. (i.e. 'A1:C7').

    .. note::
        - Only the top level of google drive will be searched for the 
          spreadsheet filename due to API limitations.
        - The worksheet name is case sensitive.

    Example usage follows::

        >>> from petl import fromgsheet
        >>> import gspread # doctest: +SKIP
        >>> client = gspread.service_account() # doctest: +SKIP
        >>> tbl1 = fromgsheet(client, 'example_spreadsheet', 'Sheet1') # doctest: +SKIP
        >>> tbl2 = fromgsheet(client, '9zDNETemfau0uY8ZJF0YzXEPB_5GQ75JV', credentials) # doctest: +SKIP

    This functionality relies heavily on the work by @burnash and his great 
    `gspread module`_.

    .. _gspread module: http://gspread.readthedocs.io/
    .. _authentication: http://gspread.readthedocs.io/en/latest/oauth2.html
    """

    return GoogleSheetView(
        credentials_or_client,
        spreadsheet,
        open_by_key,
        worksheet=worksheet,
        cell_range=cell_range
    )


class GoogleSheetView(Table):
    """This module resembles XLSXView."""

    def __init__(
        self, credentials_or_client, spreadsheet, open_by_key, worksheet, cell_range
    ):
        self.auth_info = credentials_or_client
        self.spreadsheet = spreadsheet
        self.open_by_key = open_by_key
        self.worksheet = worksheet
        self.cell_range = cell_range

    def __iter__(self):
        gspread_client = _get_gspread_client(self.auth_info)
        wb = _open_spreadsheet(gspread_client, self.spreadsheet, self.open_by_key)
        ws = _select_worksheet(wb, self.worksheet)
        # grab the range or grab the whole sheet
        if self.cell_range is not None:
            return self._yield_by_range(ws)
        return self._yield_all_rows(ws)

    def _yield_all_rows(self, ws):
        # no range specified, so return all the rows
        for row in ws.get_all_values():
            yield tuple(row)

    def _yield_by_range(self, ws):
        # start_cell -> top left, end_cell -> bottom right
        start_cell, end_cell = self.cell_range.split(":")
        from gspread.utils import a1_to_rowcol

        start_row, start_col = a1_to_rowcol(start_cell)
        end_row, end_col = a1_to_rowcol(end_cell)
        # gspread starts its indices at 1
        for i, row in enumerate(ws.get_all_values(), start=1):
            if i in range(start_row, end_row + 1):
                start_col_index = start_col - 1
                yield tuple(row[start_col_index:end_col])


def togsheet(
    tbl, credentials_or_client, spreadsheet,
    worksheet=None, share_emails=None, role="reader"
):
    """
    Write a table to a new google sheet.

    The `credentials_or_client` are used to authenticate with the google apis.
    For more info, check `authentication`_. 

    The `spreadsheet` will be the title of the workbook created google sheets.

    If `worksheet` is specified, the first worksheet in the spreadsheet
    will be renamed to its value.

    The spreadsheet will be shared with all emails in `share_emails` with
    `role` permissions granted. For more info, check `sharing`_. 

    .. _sharing: https://developers.google.com/drive/v3/web/manage-sharing

    .. note::
        The `gspread`_ package doesn't support serialization of `date` and 
        `datetime` types yet.

    Example usage::

        >>> from petl import fromcolumns, togsheet
        >>> import gspread # doctest: +SKIP
        >>> client = gspread.service_account() # doctest: +SKIP
        >>> cols = [[0, 1, 2], ['a', 'b', 'c']]
        >>> tbl = fromcolumns(cols)
        >>> togsheet(tbl, client, 'example_spreadsheet') # doctest: +SKIP
    """

    gspread_client = _get_gspread_client(credentials_or_client)
    wb = gspread_client.create(spreadsheet)
    ws = wb.sheet1
    # make smallest table possible
    ws.resize(rows=1, cols=1)
    # rename sheet if set
    if worksheet:
        ws.update_title(title=worksheet)
    # gspread indices start at 1, therefore row index insert starts at 1
    for index, row in enumerate(tbl, start=1):
        ws.insert_row(row, index)
    # specify the user account to share to
    if share_emails is not None:
        for user_email in share_emails:
            wb.share(user_email, perm_type="user", role=role)
    return wb.id


def appendgsheet(
    tbl, credentials_or_client, spreadsheet, open_by_key=False, worksheet="Sheet1"
):
    """
    Append a table to an existing google shoot at either a new worksheet
    or the end of an existing worksheet.

    The `credentials_or_client` are used to authenticate with the google apis.
    For more info, check `authentication`_. 

    The `spreadsheet` is the name of the workbook to append to.

    The `worksheet` is the title of the worksheet to append to or create when it
    does not exist yet.

    .. note:: 
        The sheet index cannot be used, and None is not an option.
    """
    gspread_client = _get_gspread_client(credentials_or_client)
    # be able to give filename or key for file
    wb = _open_spreadsheet(gspread_client, spreadsheet, open_by_key)
    # check to see if worksheet exists, if so append, otherwise create
    if worksheet in [wbs.title for wbs in wb.worksheets()]:
        ws = wb.worksheet(text_type(worksheet))
    else:
        ws = wb.add_worksheet(text_type(worksheet), 1, 1)
    # efficiency loss, but get_all_values() will only return meaningful rows,
    # therefore len(rows) + 1 gives the earliest open insert index
    start_point = len(ws.get_all_values()) + 1
    for index, row in enumerate(tbl, start=start_point):
        ws.insert_row(row, index)


Table.togsheet = togsheet

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import datetime
import os
import json
import time

import pytest

from petl.compat import text_type
from petl.io.gsheet import fromgsheet, togsheet, appendgsheet
from petl.test.helpers import ieq, get_env_vars_named

gspread = pytest.importorskip("gspread")
uuid = pytest.importorskip("uuid")

# region helpers


def _get_gspread_credentials():
    json_path = os.getenv("PETL_GCP_JSON_PATH", None)
    if json_path is not None and os.path.exists(json_path):
        return json_path
    json_props = get_env_vars_named("PETL_GCP_CREDS_")
    if json_props is not None:
        return json_props
    user_path = os.path.expanduser("~/.config/gspread/service_account.json")
    if os.path.isfile(user_path) and os.path.exists(user_path):
        return user_path
    return None


found_gcp_credentials = pytest.mark.skipif(
    _get_gspread_credentials() is None, 
    reason="""SKIPPED. to/from gspread needs json credentials for testing.
In order to run google spreadsheet tests, follow the steps bellow: 
1. Create a json authorization file, following the steps described at
   http://gspread.readthedocs.io/en/latest/oauth2.html, and save to a local path
2. Point the envvar `PETL_GCP_JSON_PATH` to the json authorization file path
2. Or fill the properties inside the json authorization file in envrionment 
   variables named with prefix PETL_GCP_CREDS_: PETL_GCP_CREDS_project_id=petl
3. Or else save the file in one of the following paths:
      unix: ~/.config/gspread/service_account.json
   windows: %APPDATA%\\gspread\\service_account.json"""
    )


def _get_env_credentials():
    creds = _get_gspread_credentials()
    if isinstance(creds, dict):
        return creds
    if isinstance(creds, text_type):
        with open(creds, encoding="utf-8") as json_file:
            creds = json.load(json_file)
            return creds
    return None


def _get_gspread_client():
    credentials = _get_env_credentials()
    try:
        if credentials is None:
            gspread_client = gspread.service_account()
        else:
            gspread_client = gspread.service_account_from_dict(credentials)
    except gspread.exceptions.APIError as ex:
        pytest.skip("SKIPPED. to/from gspread authentication error: %s" % ex)
        return None
    return gspread_client


def _get_env_sharing_emails():
    emails = get_env_vars_named("PETL_GSHEET_EMAIL", remove_prefix=False)
    if emails is not None:
        return list(emails.values())
    return []


def _get_gspread_test_params():
    filename = "test-{}".format(str(uuid.uuid4()))
    gspread_client = _get_gspread_client()
    emails = _get_env_sharing_emails()
    return filename, gspread_client, emails


def _test_to_fromg_sheet(table, sheetname, cell_range, expected):
    filename, gspread_client, emails = _get_gspread_test_params()
    # test to from gsheet
    spread_id = togsheet(
        table, gspread_client, filename, worksheet=sheetname, share_emails=emails
    )
    try:
        result = fromgsheet(
            gspread_client, filename, worksheet=sheetname, cell_range=cell_range
        )
        # make sure the expected_result matches the result
        ieq(expected, result)
    finally:
        # clean up created table
        gspread_client.del_spreadsheet(spread_id)


def _test_append_from_gsheet(table_list, expected, sheetname=None):
    filename, gspread_client, emails = _get_gspread_test_params()
    # append from the second table from the list
    table1 = table_list[0]
    other_tables = table_list[1:]
    # create the spreadshteet and the 1st sheet
    spread_id = togsheet(
        table1, gspread_client, filename, worksheet=sheetname, share_emails=emails
    )
    try:
        for tableN in other_tables:
            appendgsheet(
                tableN, gspread_client, spread_id, worksheet=sheetname, 
                open_by_key=True
            )
        # read the result appended to the sheet
        result = fromgsheet(
            gspread_client, spread_id, worksheet=sheetname, open_by_key=True
        )
        # make sure the expected_result matches the result
        ieq(expected, result)
    finally:
        # clean up created table
        gspread_client.del_spreadsheet(spread_id)


def teardown_function():
    # try to avoid: User rate limit exceeded.
    time.sleep(3)


# endregion

# region test cases data

TEST_TABLE = [
    ["foo", "bar"],
    ["A", "1"],
    ["B", "2"], 
    ["C", "3"], 
    ["D", "random_stuff-in+_名字"], 
    ["é", "3/4/2012"],
    ["F", "6"], 
]

# endregion

# region test cases execution


@found_gcp_credentials
def test_tofromgsheet_01_basic():
    _test_to_fromg_sheet( TEST_TABLE[:], None, None, TEST_TABLE[:] )


@found_gcp_credentials
def test_tofromgsheet_02_uneven_row():
    test_table_t1 = [x + ["3"] if i in [2] else x for i, x in enumerate(TEST_TABLE[:])]
    test_table_f1 = [x + [""] if len(x) < 3 else x for x in test_table_t1[:]]
    _test_to_fromg_sheet( test_table_t1, None, None, test_table_f1 )


@found_gcp_credentials
def test_tofromgsheet_03_empty_table():
    _test_to_fromg_sheet( (), None, None, () )


@found_gcp_credentials
def test_tofromgsheet_04_cell_range():
    test_table_f2 = [[x[1]] for x in TEST_TABLE[0:4]]
    _test_to_fromg_sheet(  TEST_TABLE[:], None, "B1:B4", test_table_f2 )


@found_gcp_credentials
def test_tofromgsheet_05_sheet_title():
    _test_to_fromg_sheet(  TEST_TABLE[:], "random_stuff-in+_名字", None, TEST_TABLE[:] )


@found_gcp_credentials
@pytest.mark.xfail(
    raises=TypeError, 
    reason="When this stop failing, uncomment datetime.date in TEST1 and TEST2"
    )
def test_tofromgsheet_06_datetime_date():
    test_table_dt = [[x[0],  datetime.date(2012, 5, 6)] if i in [5] else x for i, x in enumerate(TEST_TABLE[:])]
    _test_to_fromg_sheet( test_table_dt[:], None, "B1:B4", test_table_dt[:] )


@found_gcp_credentials
def test_tofromgsheet_07_open_by_key():
    filename, gspread_client, emails = _get_gspread_test_params()
    # test to from gsheet
    table = TEST_TABLE[:]
    # test to from gsheet
    spread_id = togsheet(table, gspread_client, filename, share_emails=emails)
    try:
        result = fromgsheet(gspread_client, spread_id, open_by_key=True)
        # make sure the expected_result matches the result
        ieq(table, result)
    finally:
        # clean up created table
        gspread_client.del_spreadsheet(spread_id)


@found_gcp_credentials
def test_tofromgsheet_08_recreate():
    filename, gspread_client, emails = _get_gspread_test_params()
    # test to from gsheet
    table1 = TEST_TABLE[:]
    table2 = [[ x[0] , text_type(i)] if i > 0 else x for i, x in enumerate(table1)]
    # test to from gsheet
    spread_id = togsheet(table1, gspread_client, filename, share_emails=emails)
    try:
        result1 = fromgsheet(gspread_client, spread_id, open_by_key=True)
        ieq(table1, result1)
        spread_id2 = togsheet(table2, gspread_client, filename, share_emails=emails)
        try:
            result2 = fromgsheet(gspread_client, spread_id2, open_by_key=True)
            ieq(table2, result2)
        finally:
            gspread_client.del_spreadsheet(spread_id2)
        # make sure the expected_result matches the result
    finally:
        # clean up created table
        gspread_client.del_spreadsheet(spread_id)


def _get_testcase_for_append():
    table_list = [TEST_TABLE[:], TEST_TABLE[:]]
    expected = TEST_TABLE[:] + TEST_TABLE[1:]
    return table_list, expected


@found_gcp_credentials
def test_appendgsheet_10_double():
    table_list, expected = _get_testcase_for_append()
    _test_append_from_gsheet(table_list, expected)


@found_gcp_credentials
def test_appendgsheet_11_named_sheet():
    table_list, expected = _get_testcase_for_append()
    _test_append_from_gsheet(table_list, expected, sheetname="petl_append")


@found_gcp_credentials
def test_appendgsheet_12_other_sheet():
    filename, gspread_client, emails = _get_gspread_test_params()
    # test to append gsheet
    table = TEST_TABLE[:]
    table2 = TEST_TABLE[1:]
    spread_id = togsheet(table, gspread_client, filename, share_emails=emails)
    try:
        appendgsheet(table, gspread_client, filename, worksheet="petl")
        # get the results from the 2 sheets
        result1 = fromgsheet(gspread_client, filename, worksheet=None)
        ieq(result1, table)
        result2 = fromgsheet(gspread_client, filename, worksheet="petl")
        ieq(result2, table2)
    finally:
        gspread_client.del_spreadsheet(spread_id)


# endregion

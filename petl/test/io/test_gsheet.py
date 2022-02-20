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
    json_props = get_env_vars_named("PETL_GSPREAD_")
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
2. Point the envvar `PETL_GSPREAD_JSON_PATH` to the json authorization file path
2. Or fill the properties inside the json authorization file in envrionment 
   variables named with prefix PETL_GSPREAD_: PETL_GSPREAD_project_id=petl
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


def _get_gspread_test_params():
    return "test-{}".format(str(uuid.uuid4()))


def teardown_function():
    # try to avoid: User rate limit exceeded.
    time.sleep(3)


# endregion

# region test cases data

TEST1 = [
    # straight copy test
    (
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2"), ("é", "1/1/2012")),
        None,
        None,
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2"), ("é", "1/1/2012")),
    ),
    # Uneven row test
    (
        (("foo", "bar"), ("A", "1"), ("B", "2", "3"), ("C", "2"), ("é", "1/1/2012")),
        None,
        None,
        (
            ("foo", "bar", ""),
            ("A", "1", ""),
            ("B", "2", "3"),
            ("C", "2", ""),
            ("é", "1/1/2012", ""),
        ),
    ),
    # datetime to string representation test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            # ("é", datetime.date(2012, 1, 1)),
            ("é", "2012-01-01"),
        ),
        "Sheet1",
        None,
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2"), ("é", "2012-01-01")),
    ),
    # empty table test
    ((), None, None, ()),
    # cell_range specified test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            # ("é", datetime.date(2012, 1, 1)),
            ("é", "2012-01-01"),
        ),
        None,
        "B1:B4",
        (("bar",), ("1",), ("2",), ("2",)),
    ),
    # cell_range+sheet specified test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            # ("é", datetime.date(2012, 1, 1)),
            ("é", "2012-01-01"),
        ),
        "random_stuff-in+_名字",
        "B1:B4",
        (("bar",), ("1",), ("2",), ("2",)),
    ),
]

TEST2 = [
    # Simplest test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
        ),
        "Sheet1",
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("foo", "bar"),
            ("A", "1"),
        ),
    ),
    # appending to the first sheet
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            # ("é", datetime.date(2012, 1, 1)),
            ("é", "2012-01-01"),
        ),
        "Sheet1",
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            ("é", "2012-01-01"),
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
        ),
    ),
    # appending to a new sheet
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            # ("é", datetime.date(2012, 1, 1)),
            ("é", "2012-01-01"),
        ),
        "testing_time",
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2")),
    ),
]

# endregion

# region test cases execution


@found_gcp_credentials
@pytest.mark.parametrize("table,sheetname,cell_range,expected_result", TEST1)
def test_tofromgsheet(table, sheetname, cell_range, expected_result):
    _test_tofromgsheet(table, sheetname, cell_range, expected_result)


@found_gcp_credentials
@pytest.mark.xfail(
    raises=TypeError, 
    reason="When this stop failing, uncomment datetime.date in TEST1 and TEST2"
    )
def test_tofromgshee_fail_datetime():
    table_with_datetime = (
        ("foo", "bar"),
        ("A", "1"),
        ("B", "2"),
        ("C", "2"),
        ("é", datetime.date(2012, 1, 1))
    )
    _test_tofromgsheet(table_with_datetime, "Sheet1", None, table_with_datetime)


def _test_tofromgsheet(table, sheetname, cell_range, expected_result):
    filename = _get_gspread_test_params()
    gspread_client = _get_gspread_client()
    # test to from gsheet
    spread_id = togsheet(table, gspread_client, filename, worksheet=sheetname)
    result = fromgsheet(
        gspread_client, filename, worksheet=sheetname, cell_range=cell_range
    )
    # make sure the expected_result matches the result
    ieq(result, expected_result)

    key_result = fromgsheet(
        gspread_client, spread_id, open_by_key=True, worksheet=sheetname, 
        cell_range=cell_range
    )
    ieq(key_result, expected_result)
    # clean up created table
    gspread_client.del_spreadsheet(spread_id)


@found_gcp_credentials
@pytest.mark.parametrize("table,sheetname,expected_result", TEST2)
def test_toappendfrom(table, sheetname, expected_result):
    filename = _get_gspread_test_params()
    gspread_client = _get_gspread_client()
    # test to append gsheet
    spread_id = togsheet(table, gspread_client, filename)
    table2 = table[:-1]
    appendgsheet(table2, gspread_client, filename, worksheet=sheetname)
    result = fromgsheet(gspread_client, filename, worksheet=sheetname)
    ieq(result, expected_result)
    # then delete the file
    gspread_client.del_spreadsheet(spread_id)


# endregion

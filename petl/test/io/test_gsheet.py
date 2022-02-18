# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os
import datetime

import pytest

from petl.io.gsheet import fromgsheet, togsheet, appendgsheet
from petl.test.helpers import ieq, get_env_vars_named

gspread = pytest.importorskip("gspread")
uuid = pytest.importorskip("uuid")


def _has_gshet_credentials():
    if os.getenv("PETL_GCP_JSON_PATH", None) is not None:
        return True
    json_props = get_env_vars_named("PETL_GSPREAD_")
    if json_props is not None:
        return True
    if os.path.isfile(os.path.expanduser("~/.config/gspread/service_account.json")):
        return True
    return False


if not _has_gshet_credentials():
    pytest.skip("""SKIPPED. to/from gspread needs json credentials for testing.
In order to run google spreadsheet tests, follow the steps bellow: 
1. Create a json authorization file, following the steps described at
   http://gspread.readthedocs.io/en/latest/oauth2.html, and save to a local path
2. Point the envvar `PETL_GSPREAD_JSON_PATH` to the json authorization file path
2. Or fill the properties inside the json authorization file in envrionment 
   variables named with prefix PETL_GSPREAD_: PETL_GSPREAD_project_id=petl
3. Or else save the file in one of the following paths:
      unix: ~/.config/gspread/service_account.json
   windows: %APPDATA%\gspread\service_account.json
""")


@pytest.fixture(autouse=True, scope="module")
def credentials():
    json_path = os.getenv("PETL_GCP_JSON_PATH", None)
    if json_path is not None:
        creds_from_file = gspread.service_account(filename=json_path)
        return creds_from_file
    json_props = get_env_vars_named("PETL_GSPREAD_")
    if json_props is not None:
        creds_from_env = gspread.service_account_from_dict(json_props)
        return creds_from_env
    default_path = os.path.expanduser("~/.config/gspread/service_account.json")
    if os.path.isfile(default_path):
        # gc = gspread.service_account()
        gc = gspread.service_account_from_dict(default_path)
        return gc
    return None


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
            ("é", datetime.date(2012, 1, 1)),
        ),
        "Sheet1",
        None,
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2"), ("é", "2012-01-01")),
    ),
    # empty table test
    ((), None, None, ()),
    # range_string specified test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            ("é", datetime.date(2012, 1, 1)),
        ),
        None,
        "B1:B4",
        (("bar",), ("1",), ("2",), ("2",)),
    ),
    # range_string+sheet specified test
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            ("é", datetime.date(2012, 1, 1)),
        ),
        "random_stuff-in+_名字",
        "B1:B4",
        (("bar",), ("1",), ("2",), ("2",)),
    ),
]


def test_tofromgsheet1():
    t1 = TEST1[0]
    test_tofromgsheet(t1[0], t1[2], t1[2], t1[3])


@pytest.mark.parametrize("table,worksheet,range_string,expected_result", TEST1)
def test_tofromgsheet(table, worksheet, range_string, expected_result):
    filename = "test-{}".format(str(uuid.uuid4()))
    # test to from gsheet
    togsheet(table, filename, credentials, worksheet_title=worksheet)
    result = fromgsheet(
        filename, credentials, worksheet_title=worksheet, range_string=range_string
    )
    # make sure the expected_result matches the result
    ieq(result, expected_result)

    # test open by key
    client = gspread.authorize(credentials)
    # get spreadsheet id (key) of previously created sheet
    filekey = client.open(filename).id
    key_result = fromgsheet(
        filekey, credentials, worksheet_title=worksheet, range_string=range_string
    )
    ieq(key_result, expected_result)
    # clean up created table
    client.del_spreadsheet(filekey)


TEST2 = [
    # appending to the first sheet
    (
        (
            ("foo", "bar"),
            ("A", "1"),
            ("B", "2"),
            ("C", "2"),
            ("é", datetime.date(2012, 1, 1)),
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
            ("é", datetime.date(2012, 1, 1)),
        ),
        "testing_time",
        (("foo", "bar"), ("A", "1"), ("B", "2"), ("C", "2")),
    ),
]


@pytest.mark.parametrize("table,append_worksheet,expected_result", TEST2)
def test_toappendfrom(table, append_worksheet, expected_result):
    filename = "test-{}".format(str(uuid.uuid4()))
    togsheet(table, filename, credentials)
    appendgsheet(table[:-1], filename, credentials, worksheet_title=append_worksheet)
    result = fromgsheet(filename, credentials, worksheet_title=append_worksheet)
    ieq(result, expected_result)
    # get client to get information
    client = gspread.authorize(credentials)
    # get spreadsheet id (key) of previously created sheet
    filekey = client.open(filename).id
    # then delete the file
    client.del_spreadsheet(filekey)

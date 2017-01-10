# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os

import petl as etl
from petl.io.gsheet import fromgsheet, togsheet
from petl.test.helpers import ieq

"""
In order to run these tests, follow the steps described at
http://gspread.readthedocs.io/en/latest/oauth2.html to create a json
authorization file. Point `JSON_PATH` to local file or put the path in the
env variable at `GSHEET_JSON_PATH`.
Afterwards, create a spreadsheet modeled on:
https://docs.google.com/spreadsheets/d/12oFimWB81Jk7dzjdnH8WiYnSo4rl6Xe1xdOadbvAsJI/edit#gid=0
and share it with the service_account specified in the JSON file.
"""

SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive.file'
]

JSON_PATH = os.getenv("GSHEET_JSON_PATH", 'default/fallback.json')

try:
    # noinspection PyUnresolvedReferences
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials as sac
except ImportError as e:
    print('SKIP gsheet tests: %s' % e, file=sys.stderr)
else:

    def test_fromgsheet():
        filename = 'test'
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = fromgsheet(filename, credentials, sheet='Sheet1')
        expect = (('foo', 'bar'),
                  ('A', '1'),
                  ('B', '2'),
                  ('C', '2'),
                  (u'é', '1/1/2012'))
        ieq(expect, tbl)

    def test_fromgsheet_int():
        filename = 'test'
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = fromgsheet(filename, credentials, sheet=0)
        expect = (('foo', 'bar'),
                  ('A', '1'),
                  ('B', '2'),
                  ('C', '2'),
                  (u'é', '1/1/2012'))
        ieq(expect, tbl)

    def test_fromgsheet_key():
        filename = '12oFimWB81Jk7dzjdnH8WiYnSo4rl6Xe1xdOadbvAsJI'
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = fromgsheet(filename, credentials, sheet='Sheet1', filekey=True)
        expect = (('foo', 'bar'),
                  ('A', '1'),
                  ('B', '2'),
                  ('C', '2'),
                  (u'é', '1/1/2012'))
        ieq(expect, tbl)

    def test_fromgsheet_nosheet():
        filename = 'test'
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = fromgsheet(filename, credentials)
        expect = (('foo', 'bar'),
                  ('A', '1'),
                  ('B', '2'),
                  ('C', '2'),
                  (u'é', '1/1/2012'))
        ieq(expect, tbl)

    def test_fromgsheet_range():
        filename = 'test'
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = fromgsheet(filename, credentials, sheet='Sheet2',
                         range_string='B2:C6')
        expect = (('foo', 'bar'),
                  ('A', '1'),
                  ('B', '2'),
                  ('C', '2'),
                  (u'é', '1/1/2012'))
        ieq(expect, tbl)

    def test_togsheet():
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = (('foo', 'bar'),
               ('A', '1'),
               ('B', '2'),
               ('C', '2'),
               (u'é', '1/1/2012'))
        filename = 'test_togsheet'
        togsheet(tbl, filename, credentials, sheet='Sheet1')
        actual = fromgsheet(filename, credentials, sheet='Sheet1')
        ieq(tbl, actual)
        # clean up created table
        client = gspread.authorize(credentials)
        client.del_spreadsheet(client.open(filename).id)

    def test_togsheet_nosheet():
        credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)
        tbl = (('foo', 'bar'),
               ('A', '1'),
               ('B', '2'),
               ('C', '2'),
               (u'é', '1/1/2012'))
        filename = 'test_togsheet_nosheet'
        togsheet(tbl, filename, credentials)
        actual = fromgsheet(filename, credentials)
        ieq(tbl, actual)
        # clean up created table
        client = gspread.authorize(credentials)
        client.del_spreadsheet(client.open(filename).id)

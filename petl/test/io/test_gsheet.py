# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os
import datetime

import petl as etl
from petl.io.gsheet import fromgsheet, togsheet
from petl.test.helpers import ieq

"""
In order to run these tests, follow the steps described at
http://gspread.readthedocs.io/en/latest/oauth2.html to create a json
authorization file. Point `JSON_PATH` to local file or put the path in the
env variable at `GSHEET_JSON_PATH`.
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
    import uuid
except ImportError as e:
    print('SKIP gsheet tests: %s' % e, file=sys.stderr)
else:
    args = [ # straight copy test
            ((('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', '1/1/2012')),
             None,
             None,
             (('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', '1/1/2012'))),

          # Uneven row test
            ((('foo', 'bar'),
              ('A', '1'),
              ('B', '2', '3'),
              ('C', '2'),
              (u'é', '1/1/2012')),
              None,
              None,
              (('foo', 'bar', ''),
               ('A', '1', ''),
               ('B', '2', '3'),
               ('C', '2', ''),
               (u'é', '1/1/2012', ''))),

          # datetime to string representation test
            ((('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', datetime.date(2012,1,1))),
             'Sheet1',
             None,
             (('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', '2012-01-01'))),

          # empty table test
            ((),
             None,
             None,
             ()),

          # range_string specified test
            ((('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', datetime.date(2012,1,1))),
             None,
             'B1:B4',
             (('bar',),
              ('1',),
              ('2',),
              ('2',))),

          # range_string+sheet specified test
            ((('foo', 'bar'),
              ('A', '1'),
              ('B', '2'),
              ('C', '2'),
              (u'é', datetime.date(2012,1,1))),
             u'random_stuff-in+_名字',
             'B1:B4',
             (('bar',),
              ('1',),
              ('2',),
              ('2',)))
        ]

    def test_gsheet():
        def test_tofromgsheet(table, worksheet, range_string, expected_result):
            filename = 'test-{}'.format(str(uuid.uuid4()))
            credentials = sac.from_json_keyfile_name(JSON_PATH, SCOPE)

            # test to from gsheet
            togsheet(table, filename, credentials, worksheet_title=worksheet)
            result = fromgsheet(filename,
                                credentials,
                                worksheet_title=worksheet,
                                range_string=range_string)
            # make sure the expected_result matches the result
            ieq(result, expected_result)

            # test open by key
            client = gspread.authorize(credentials)
            # get spreadsheet id (key) of previously created sheet
            filekey = client.open(filename).id
            key_result = fromgsheet(filekey,
                                    credentials,
                                    worksheet_title=worksheet,
                                    range_string=range_string)
            ieq(key_result, expected_result)
            # clean up created table
            client.del_spreadsheet(filekey)


        # yield a test for each tuple of arguments in order to display with nose
        for arg_tuple in args:
            yield (test_tofromgsheet, *arg_tuple)

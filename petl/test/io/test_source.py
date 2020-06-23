# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os

from petl.compat import PY3
from petl.test.helpers import ieq
from petl.io.avro import fromavro, toavro
from petl.io.csv import fromcsv, tocsv
from petl.util.vis import look

# region Codec test cases

try:
    import s3fs
except ImportError as e:
    print('SKIP S3 protocol tests: %s' % e, file=sys.stderr)
else:
    def test_s3():
        _write_read_from_url('PETL_S3_URL', "export PETL_S3_URL='s3://mybucket/path/folder'")

try:
    import smbclient
except ImportError as e:
    print('SKIP SMB protocol tests: %s' % e, file=sys.stderr)
else:
    def test_smb():
        _write_read_from_url('PETL_SMB_URL', "export PETL_SMB_URL='smb://DOMAIN;myuserID:mypassword@host/share'")

# endregion

# region Execution

def _write_read_from_url(env_var_name, example):
    base_url = os.getenv(env_var_name, 'skip')
    if base_url == 'skip':
        m = "# Skipping test because env var '{}' is not defined. Try this:\n$ {}"
        msg = m.format(env_var_name, example)
        print(msg)
        return

    csv_url = os.path.join(base_url, 'filename1.csv')
    gzc_url = os.path.join(base_url, 'filename3.csv.gz')
    gza_url = os.path.join(base_url, 'filename4.avro.gz')
    avr_url = os.path.join(base_url, 'filename2.avro')

    _table = ( (u'name', u'friends', u'age'),
                (u'Bob', '42', '33'),
                (u'Jim', '13', '69'),
                (u'Joe', '86', '17'),
                (u'Ted', '23', '51'))

    _show__rows_from("Expected:", _table)

    has_avro = _test_avro_too()

    tocsv(_table, csv_url, encoding='ascii', lineterminator='\n')
    if PY3:
        tocsv(_table, gzc_url, encoding='ascii', lineterminator='\n')
    if has_avro:
        toavro(_table, avr_url)
        if PY3:
            toavro(_table, gza_url)

    csv_actual = fromcsv(csv_url, encoding='ascii')
    if PY3:
        gzp_actual = fromcsv(gzc_url, encoding='ascii')
    if has_avro:
        avr_actual = fromavro(avr_url)
        if PY3:
            gza_actual = fromavro(gza_url)

    _show__rows_from("Actual:", csv_url)

    ieq(_table, csv_actual)
    ieq(_table, csv_actual)  # verify can iterate twice
    if PY3:
        ieq(_table, gzp_actual)
        ieq(_table, gzp_actual)  # verify can iterate twice
    if has_avro:
        ieq(_table, avr_actual)
        ieq(_table, avr_actual)  # verify can iterate twice
        if PY3:
            ieq(_table, gza_actual)
            ieq(_table, gza_actual)  # verify can iterate twice


def _show__rows_from(label, test_rows, limit=0):
    print(label)
    print(look(test_rows, limit=limit))

def _test_avro_too():
    try:
        import fastavro
        return True
    except:
        return False

# endregion

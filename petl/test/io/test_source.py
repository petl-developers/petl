# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os

from petl.compat import PY3
from petl.test.helpers import ieq, eq_
from petl.io.avro import fromavro, toavro
from petl.io.csv import fromcsv, tocsv
from petl.util.vis import look

from petl.io.source.smb import _parse_smb_url

# region Codec test cases

try:
    import s3fs
except ImportError as e:
    print('SKIP S3 helper tests: %s' % e, file=sys.stderr)
else:
    def test_helper_s3():
        _write_read_from_url('PETL_S3_URL', "export PETL_S3_URL='s3://mybucket/path/folder'")

try:
    import smbclient
except ImportError as e:
    print('SKIP SMB helper tests: %s' % e, file=sys.stderr)
else:
    def test_helper_smb():
        _write_read_from_url('PETL_SMB_URL', "export PETL_SMB_URL='smb://DOMAIN;myuserID:mypassword@host/share'")

    def test_helper_smb_url_parse():
        url = r'smb://workgroup;user:password@server:444/share/folder/file.csv'
        domain, host, port, user, passwd, server_path = _parse_smb_url(url)
        print("Parsed:", domain, host, port, user, passwd, server_path)
        eq_(domain, r'workgroup')
        eq_(host, r'server')
        eq_(port, 444)
        eq_(user, r'user')
        eq_(passwd, r'password')
        eq_(server_path, "\\\\server\\share\\folder\\file.csv")

# endregion

# region Execution

def _write_read_from_url(env_var_name, example):

    base_url = os.getenv(env_var_name, 'skip')

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

    if base_url == 'skip':
        m = "# Skipping test because env var '{}' is not defined. Try this:\n$ {}"
        msg = m.format(env_var_name, example)
        print(msg)
        return

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

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import sys
import os
from tempfile import NamedTemporaryFile

from petl.compat import PY3
from petl.test.helpers import ieq
from petl.io.avro import fromavro, toavro
from petl.io.csv import fromcsv, tocsv
from petl.util.vis import look

# region Codec test cases

try:
    import lzma
except ImportError as e:
    print('SKIP XZ helper tests: %s' % e, file=sys.stderr)
else:
    def test_helper_xz():
        _write_read_with_codec('.xz')

try:
    import lz4.frame
except ImportError as e:
    print('SKIP LZ4 helper tests: %s' % e, file=sys.stderr)
else:
    def test_helper_lz4():
        _write_read_with_codec('.lz4')

try:
    import zstandard as zstd
except ImportError as e:
    print('SKIP ZSTANDARD helper tests: %s' % e, file=sys.stderr)
else:
    def test_helper_zstd():
        _write_read_with_codec('.zstd')

# endregion

# region Execution


def _write_read_with_codec(file_ext):

    _table = ((u'name', u'friends', u'age'),
              (u'Bob', '42', '33'),
              (u'Jim', '13', '69'),
              (u'Joe', '86', '17'),
              (u'Ted', '23', '51'))

    _show__rows_from("Expected:", _table)

    has_avro = _test_avro_too()

    compressed_csv = _get_temp_file_for('.csv' + file_ext)
    compressed_avr = _get_temp_file_for('.avro' + file_ext)

    tocsv(_table, compressed_csv, encoding='ascii', lineterminator='\n')
    if has_avro:
        toavro(_table, compressed_avr)

    csv_actual = fromcsv(compressed_csv, encoding='ascii')
    if has_avro:
        avr_actual = fromavro(compressed_avr)

    _show__rows_from("Actual:", csv_actual)
    
    ieq(_table, csv_actual)
    ieq(_table, csv_actual)  # verify can iterate twice
    if has_avro:
        ieq(_table, avr_actual)
        ieq(_table, avr_actual)  # verify can iterate twice


def _get_temp_file_for(file_ext):
    with NamedTemporaryFile(delete=False, mode='wb') as fo:
        return fo.name + file_ext


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

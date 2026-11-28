# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import csv
import gzip
import os
import logging

import pytest

from petl.test.helpers import ieq, eq_
from petl.io.csv import fromcsv, fromtsv, tocsv, appendcsv, totsv, appendtsv
from petl.io.sources import MemorySource


logger = logging.getLogger(__name__)
debug = logger.debug


def test_fromcsv():

    data = [b'foo,bar',
            b'a,1',
            b'b,2',
            b'c,2']
    f = NamedTemporaryFile(mode='wb', delete=False)
    f.write(b'\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = fromcsv(f.name, encoding='ascii')
    debug(actual)
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_strip_nulls(tmp_path):
    source = tmp_path / 'input.csv'
    raw = (b'na\x00me;note\r\n'
           b'\x00"A;B";"first\r\nse\x00\x00cond"\r\n'
           b'\x00;keep\x0bthis\r\n')
    source.write_bytes(raw)

    table = fromcsv(str(source), encoding='ascii', delimiter=';', strip_nulls=True)
    expected = [('name', 'note'), ('A;B', 'first\r\nsecond'), ('', 'keep\x0bthis')]
    assert list(iter(table)) == expected
    assert list(iter(table)) == expected
    assert source.read_bytes() == raw


@pytest.mark.parametrize('raw', [
    b'\x00', b'h\n\x00\x00', b'\x00\n', b'h\x00\n\nv\n',
    b'h\r\x00\nv\r\n', b'h\r\x00\x00\n\nv\r',
    b'"a\r\x00\nb"\r\x00c\r\r', b'h\r\x00', b'h\r\r\nv', b'h\r\x00v',
])
def test_fromcsv_strip_nulls_record_boundaries(tmp_path, raw):
    source = tmp_path / 'input.csv'
    rewritten = tmp_path / 'rewritten.csv'
    source.write_bytes(raw)
    rewritten.write_bytes(raw.replace(b'\x00', b''))
    with rewritten.open(encoding='ascii', newline='') as stream:
        expected = [tuple(row) for row in csv.reader(stream)]
    assert list(iter(fromcsv(str(source), strip_nulls=True))) == expected


def test_fromtsv_strip_nulls_after_decoding(tmp_path):
    source = tmp_path / 'input.tsv'
    source.write_bytes('caf\x00\xe9\t10\n'.encode('utf-16'))
    table = fromtsv(str(source), encoding='utf-16', strip_nulls=True,
                    header=('external\x00', 'count'))
    assert list(iter(table)) == [('external\x00', 'count'), ('caf\xe9', '10')]


@pytest.mark.parametrize('options', [{}, {'strip_nulls': False}])
@pytest.mark.parametrize('reader, dialect, line', [
    (fromcsv, 'excel', 'a\x00,b\r\n'),
    (fromtsv, 'excel-tab', 'a\x00\tb\r\n'),
])
def test_strip_nulls_disabled(tmp_path, options, reader, dialect, line):
    source = tmp_path / 'input.txt'
    source.write_bytes(line.encode('ascii'))
    table = reader(str(source), **options)
    try:
        row = next(csv.reader([line], dialect=dialect))
    except csv.Error:
        with pytest.raises(csv.Error):
            next(iter(table))
    else:
        assert list(iter(table)) == [tuple(row)]


def test_fromcsv_strip_nulls_streams_rows(tmp_path):
    source = tmp_path / 'input.csv'
    # Keep the decoding error beyond the text stream's initial buffer.
    source.write_bytes(b'na\x00me,id\n' + b'A,1\n' * 32768 + b'\xff')
    rows = iter(fromcsv(str(source), encoding='ascii', strip_nulls=True))
    assert next(rows) == ('name', 'id')
    with pytest.raises(UnicodeDecodeError):
        list(rows)


def test_fromcsv_strip_nulls_streams_bare_cr():
    raw = b'na\x00me,id\r' + b'A' * 50000 + b',1\xff\r'
    rows = iter(fromcsv(MemorySource(raw), encoding='ascii', strip_nulls=True))
    assert next(rows) == ('name', 'id')
    with pytest.raises(UnicodeDecodeError):
        next(rows)


def test_fromcsv_strip_nulls_bare_cr_lookahead():
    value = 'A' * 50000
    raw = b'name,id\r\x00' + value.encode('ascii') + b',1\r'
    source = MemorySource(raw)
    rows = iter(fromcsv(source, encoding='ascii', strip_nulls=True))
    assert next(rows) == ('name', 'id')
    assert source.buffer.tell() < len(raw)
    assert list(rows) == [(value, '1')]


def test_fromcsv_lineterminators():

    data = [b'foo,bar',
            b'a,1',
            b'b,2',
            b'c,2']

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    for lt in b'\r', b'\n', b'\r\n':
        debug(repr(lt))
        f = NamedTemporaryFile(mode='wb', delete=False)
        f.write(lt.join(data))
        f.close()
        with open(f.name, 'rb') as g:
            debug(repr(g.read()))
        actual = fromcsv(f.name, encoding='ascii')
        debug(actual)
        ieq(expect, actual)


def test_fromcsv_quoted():
    data = [b'"foo","bar"',
            b'"a",1',
            b'"b",2',
            b'"c",2']
    f = NamedTemporaryFile(mode='wb', delete=False)
    f.write(b'\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', 1),
              ('b', 2),
              ('c', 2))
    actual = fromcsv(f.name, quoting=csv.QUOTE_NONNUMERIC)
    debug(actual)
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromtsv():

    data = [b'foo\tbar',
            b'a\t1',
            b'b\t2',
            b'c\t2']
    f = NamedTemporaryFile(mode='wb', delete=False)
    f.write(b'\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = fromtsv(f.name, encoding='ascii')
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_gz():

    data = [b'foo,bar',
            b'a,1',
            b'b,2',
            b'c,2']

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    lts = b'\r', b'\n', b'\r\n'
    for lt in lts:
        f = NamedTemporaryFile(delete=False)
        f.close()
        fn = f.name + '.gz'
        os.rename(f.name, fn)
        fz = gzip.open(fn, 'wb')
        fz.write(lt.join(data))
        fz.close()
        actual = fromcsv(fn, encoding='ascii')
        ieq(expect, actual)
        ieq(expect, actual)  # verify can iterate twice


def test_tocsv_appendcsv():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    tocsv(table, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        data = [b'foo,bar',
                b'a,1',
                b'b,2',
                b'c,2']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        data = [b'foo,bar',
                b'a,1',
                b'b,2',
                b'c,2',
                b'd,7',
                b'e,9',
                b'f,1']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)


def test_tocsv_noheader():

    # check explicit no header
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tocsv(table, f.name, encoding='ascii', lineterminator='\n',
          write_header=False)

    # check what it did
    with open(f.name, 'rb') as o:
        data = [b'a,1',
                b'b,2',
                b'c,2']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)


def test_totsv_appendtsv():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    totsv(table, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        data = [b'foo\tbar',
                b'a\t1',
                b'b\t2',
                b'c\t2']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendtsv(table2, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'rb') as o:
        data = [b'foo\tbar',
                b'a\t1',
                b'b\t2',
                b'c\t2',
                b'd\t7',
                b'e\t9',
                b'f\t1']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)


def test_tocsv_appendcsv_gz():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    fn = f.name + '.gz'
    f.close()
    tocsv(table, fn, encoding='ascii', lineterminator='\n')

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        data = [b'foo,bar',
                b'a,1',
                b'b,2',
                b'c,2']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)
    finally:
        o.close()

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, fn, encoding='ascii', lineterminator='\n')

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        data = [b'foo,bar',
                b'a,1',
                b'b,2',
                b'c,2',
                b'd,7',
                b'e,9',
                b'f,1']
        # don't forget final terminator
        expect = b'\n'.join(data) + b'\n'
        actual = o.read()
        eq_(expect, actual)
    finally:
        o.close()
        
def test_fromcsv_header():

    header = ['foo', 'bar']
    data = [b'a,1',
            b'b,2',
            b'c,2']
    f = NamedTemporaryFile(mode='wb', delete=False)
    f.write(b'\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = fromcsv(f.name, encoding='ascii', header=header)
    debug(actual)
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import gzip
import os
import logging
from petl.compat import PY2


from petl.test.helpers import ieq, eq_
from petl.io.csv import fromcsv, fromtsv, tocsv, appendcsv, totsv, appendtsv


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
    import csv
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

    # '\r' not supported in PY2 because universal newline mode is
    # not supported by gzip module
    if PY2:
        lts = b'\n', b'\r\n'
    else:
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
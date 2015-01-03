from __future__ import absolute_import, print_function, division, \
    unicode_literals


from tempfile import NamedTemporaryFile
import gzip
import os
from petl.compat import PY2


from petl.testutils import ieq, eq_
from petl.io.csv import fromcsv, fromtsv, tocsv, appendcsv, totsv, appendtsv


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
    actual = fromcsv(f.name)
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
        f = NamedTemporaryFile(mode='wb', delete=False)
        f.write(lt.join(data))
        f.close()
        actual = fromcsv(f.name)
        ieq(expect, actual)


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
    actual = fromtsv(f.name)
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
        actual = fromcsv(fn)
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
    tocsv(table, f.name, lineterminator='\n')

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
    appendcsv(table2, f.name, lineterminator='\n')

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
    tocsv(table, f.name, lineterminator='\n', write_header=False)

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
    totsv(table, f.name, lineterminator='\n')

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
    appendtsv(table2, f.name, lineterminator='\n')

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
    tocsv(table, fn, lineterminator='\n')

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
    appendcsv(table2, fn, lineterminator='\n')

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

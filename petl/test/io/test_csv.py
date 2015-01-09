from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import gzip
import os
from petl.compat import PY2


from petl.test.helpers import ieq, eq_
from petl.io.csv import fromcsv, fromtsv, tocsv, appendcsv, totsv, appendtsv


def test_fromcsv():

    data = ['foo,bar',
            'a,1',
            'b,2',
            'c,2']
    f = NamedTemporaryFile(mode='w', delete=False)
    f.write('\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = fromcsv(f.name, encoding='ascii')
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_lineterminators():

    data = ['foo,bar',
            'a,1',
            'b,2',
            'c,2']

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    for lt in '\r', '\n', '\r\n':
        f = NamedTemporaryFile(mode='w', delete=False)
        f.write(lt.join(data))
        f.close()
        actual = fromcsv(f.name, encoding='ascii')
        ieq(expect, actual)


def test_fromtsv():

    data = ['foo\tbar',
            'a\t1',
            'b\t2',
            'c\t2']
    f = NamedTemporaryFile(mode='w', delete=False)
    f.write('\n'.join(data))
    f.close()

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    actual = fromtsv(f.name, encoding='ascii')
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_gz():

    data = ['foo,bar',
            'a,1',
            'b,2',
            'c,2']

    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    # '\r' not supported in PY2 because universal newline mode is
    # not supported by gzip module
    if PY2:
        lts = '\n', '\r\n'
    else:
        lts = '\r', '\n', '\r\n'
    for lt in lts:
        f = NamedTemporaryFile(delete=False)
        f.close()
        fn = f.name + '.gz'
        os.rename(f.name, fn)
        fz = gzip.open(fn, 'wt')
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
    with open(f.name, 'r') as o:
        data = ['foo,bar',
                'a,1',
                'b,2',
                'c,2']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
        actual = o.read()
        eq_(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'r') as o:
        data = ['foo,bar',
                'a,1',
                'b,2',
                'c,2',
                'd,7',
                'e,9',
                'f,1']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
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
    with open(f.name, 'r') as o:
        data = ['a,1',
                'b,2',
                'c,2']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
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
    with open(f.name, 'r') as o:
        data = ['foo\tbar',
                'a\t1',
                'b\t2',
                'c\t2']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
        actual = o.read()
        eq_(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendtsv(table2, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with open(f.name, 'r') as o:
        data = ['foo\tbar',
                'a\t1',
                'b\t2',
                'c\t2',
                'd\t7',
                'e\t9',
                'f\t1']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
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
    o = gzip.open(fn, 'rt')
    try:
        data = ['foo,bar',
                'a,1',
                'b,2',
                'c,2']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
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
    o = gzip.open(fn, 'rt')
    try:
        data = ['foo,bar',
                'a,1',
                'b,2',
                'c,2',
                'd,7',
                'e,9',
                'f,1']
        # don't forget final terminator
        expect = '\n'.join(data) + '\n'
        actual = o.read()
        eq_(expect, actual)
    finally:
        o.close()

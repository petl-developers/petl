from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


from tempfile import NamedTemporaryFile
import csv
import gzip
import os


from petl.testutils import ieq
from petl.io.csv import fromcsv, fromtsv, tocsv, appendcsv, totsv, appendtsv


def test_fromcsv():

    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()

    actual = fromcsv(f.name, delimiter='\t')
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_lineterminators():

    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    for lt in '\r', '\n', '\r\n':
        f = NamedTemporaryFile(delete=False)
        writer = csv.writer(f, lineterminator=lt)
        for row in table:
            writer.writerow(row)
        f.close()
        actual = fromcsv(f.name)
        ieq(expect, actual)


def test_fromtsv():

    f = NamedTemporaryFile(delete=False)
    writer = csv.writer(f, delimiter='\t')
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    for row in table:
        writer.writerow(row)
    f.close()

    actual = fromtsv(f.name)
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_tocsv_appendcsv():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tocsv(table, f.name, delimiter='\t')

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, f.name, delimiter='\t')

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)

    # check explicit no header
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tocsv(table, f.name, delimiter='\t', write_header=False)

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)


def test_totsv_appendtsv():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    totsv(table, f.name)

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendtsv(table2, f.name)

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)

    # check explicit no header
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    tocsv(table, f.name, delimiter='\t', write_header=False)

    # check what it did
    with open(f.name, 'rb') as o:
        actual = csv.reader(o, delimiter='\t')
        expect = [['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)


def test_fromcsv_gz():

    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))

    for lt in '\n', '\r\n':
        # N.B., '\r' not supported because universal newline mode is
        # not supported by gzip module
        f = NamedTemporaryFile(delete=False)
        f.close()
        fn = f.name + '.gz'
        os.rename(f.name, fn)
        fz = gzip.open(fn, 'wb')
        writer = csv.writer(fz, delimiter='\t', lineterminator=lt)
        for row in table:
            writer.writerow(row)
        fz.close()
        actual = fromcsv(fn, delimiter='\t')
        ieq(expect, actual)
        ieq(expect, actual)  # verify can iterate twice


def test_tocsv_appendcsv_gz():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    fn = f.name + '.gz'
    f.close()
    tocsv(table, fn, delimiter='\t')

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2']]
        ieq(expect, actual)
    finally:
        o.close()

    # check appending
    table2 = (('foo', 'bar'),
              ('d', 7),
              ('e', 9),
              ('f', 1))
    appendcsv(table2, fn, delimiter='\t')

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        actual = csv.reader(o, delimiter='\t')
        expect = [['foo', 'bar'],
                  ['a', '1'],
                  ['b', '2'],
                  ['c', '2'],
                  ['d', '7'],
                  ['e', '9'],
                  ['f', '1']]
        ieq(expect, actual)
    finally:
        o.close()

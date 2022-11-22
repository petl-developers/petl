# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import gzip
import bz2
import zipfile
from tempfile import NamedTemporaryFile
from petl.compat import PY2


from petl.test.helpers import ieq, eq_
import petl as etl
from petl.io.sources import MemorySource, PopenSource, ZipSource, \
    StdoutSource, GzipSource, BZ2Source


def test_memorysource():
    tbl1 = (('foo', 'bar'),
            ('a', '1'),
            ('b', '2'),
            ('c', '2'))

    # test writing to a string buffer
    ss = MemorySource()
    etl.tocsv(tbl1, ss)
    expect = "foo,bar\r\na,1\r\nb,2\r\nc,2\r\n"
    if not PY2:
        expect = expect.encode('ascii')
    actual = ss.getvalue()
    eq_(expect, actual)

    # test reading from a string buffer
    tbl2 = etl.fromcsv(MemorySource(actual))
    ieq(tbl1, tbl2)
    ieq(tbl1, tbl2)

    # test appending
    etl.appendcsv(tbl1, ss)
    actual = ss.getvalue()
    expect = "foo,bar\r\na,1\r\nb,2\r\nc,2\r\na,1\r\nb,2\r\nc,2\r\n"
    if not PY2:
        expect = expect.encode('ascii')
    eq_(expect, actual)


def test_memorysource_2():

    data = 'foo,bar\r\na,1\r\nb,2\r\nc,2\r\n'
    if not PY2:
        data = data.encode('ascii')
    actual = etl.fromcsv(MemorySource(data))
    expect = (('foo', 'bar'),
              ('a', '1'),
              ('b', '2'),
              ('c', '2'))
    ieq(expect, actual)
    ieq(expect, actual)


def test_popensource():

    expect = (('foo', 'bar'),)
    delimiter = ' '
    actual = etl.fromcsv(PopenSource(r'echo foo bar',
                                     shell=True),
                         delimiter=delimiter)
    ieq(expect, actual)


def test_zipsource():

    # setup
    tbl = [('foo', 'bar'), ('a', '1'), ('b', '2')]
    fn_tsv = NamedTemporaryFile().name
    etl.totsv(tbl, fn_tsv)
    fn_zip = NamedTemporaryFile().name
    z = zipfile.ZipFile(fn_zip, mode='w')
    z.write(fn_tsv, 'data.tsv')
    z.close()

    # test
    actual = etl.fromtsv(ZipSource(fn_zip, 'data.tsv'))
    ieq(tbl, actual)


def test_stdoutsource():

    tbl = [('foo', 'bar'), ('a', 1), ('b', 2)]
    etl.tocsv(tbl, StdoutSource(), encoding='ascii')
    etl.tohtml(tbl, StdoutSource(), encoding='ascii')
    etl.topickle(tbl, StdoutSource())


def test_stdoutsource_none(capfd):

    tbl = [('foo', 'bar'), ('a', 1), ('b', 2)]
    etl.tocsv(tbl, encoding='ascii')
    captured = capfd.readouterr()
    outp = captured.out
    # TODO: capfd works on vscode but not in console/tox
    if outp:
        assert outp in ( 'foo,bar\r\na,1\r\nb,2\r\n' , 'foo,bar\na,1\nb,2\n' )


def test_stdoutsource_unicode():

    tbl = [('foo', 'bar'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2)]
    etl.tocsv(tbl, StdoutSource(), encoding='utf-8')
    etl.tohtml(tbl, StdoutSource(), encoding='utf-8')
    etl.topickle(tbl, StdoutSource())


def test_gzipsource():

    # setup
    tbl = [('foo', 'bar'), ('a', '1'), ('b', '2')]
    fn = NamedTemporaryFile().name + '.gz'
    expect = b"foo,bar\na,1\nb,2\n"

    # write explicit
    etl.tocsv(tbl, GzipSource(fn), lineterminator='\n')
    actual = gzip.open(fn).read()
    eq_(expect, actual)
    # write implicit
    etl.tocsv(tbl, fn, lineterminator='\n')
    actual = gzip.open(fn).read()
    eq_(expect, actual)

    # read explicit
    tbl2 = etl.fromcsv(GzipSource(fn))
    ieq(tbl, tbl2)
    # read implicit
    tbl2 = etl.fromcsv(fn)
    ieq(tbl, tbl2)


def test_bzip2source():

    # setup
    tbl = [('foo', 'bar'), ('a', '1'), ('b', '2')]
    fn = NamedTemporaryFile().name + '.bz2'
    expect = b"foo,bar\na,1\nb,2\n"

    # write explicit
    etl.tocsv(tbl, BZ2Source(fn), lineterminator='\n')
    actual = bz2.BZ2File(fn).read()
    eq_(expect, actual)
    # write implicit
    etl.tocsv(tbl, fn, lineterminator='\n')
    actual = bz2.BZ2File(fn).read()
    eq_(expect, actual)

    # read explicit
    tbl2 = etl.fromcsv(BZ2Source(fn))
    ieq(tbl, tbl2)
    # read implicit
    tbl2 = etl.fromcsv(fn)
    ieq(tbl, tbl2)

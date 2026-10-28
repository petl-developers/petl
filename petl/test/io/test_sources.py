# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import os
import sys
import subprocess
import gzip
import bz2
import zipfile
import io
import pytest
from tempfile import NamedTemporaryFile
from petl.compat import PY2


from petl.test.helpers import ieq, eq_
import petl as etl
from petl.io.sources import MemorySource, PopenSource, ZipSource, \
    StdoutSource, GzipSource, BZ2Source, _get_stdin_binary, _get_stdout_binary


class DetachedTextIOWrapper(io.TextIOWrapper):

    @property
    def buffer(self):
        # Match Python 3.15.1's behavior on older supported interpreters too.
        raise ValueError('underlying buffer has been detached')


def detached_text_stream():
    stream = DetachedTextIOWrapper(io.BytesIO())
    stream.detach()
    return stream


@pytest.mark.parametrize('name, get_binary', [
    ('stdin', _get_stdin_binary), ('stdout', _get_stdout_binary),
])
@pytest.mark.parametrize('case', ['current', 'original', 'both', 'normal', 'missing'])
def test_standard_binary_with_unavailable_buffers(monkeypatch, name, get_binary, case):
    current = io.StringIO()
    original = io.TextIOWrapper(io.BytesIO())
    expected = original.buffer
    if case in ('current', 'both'):
        current = detached_text_stream()
    if case in ('original', 'both'):
        original = detached_text_stream()
        expected = current
    if case == 'normal':
        current = io.TextIOWrapper(io.BytesIO())
        expected = current.buffer
    if case == 'missing':
        original = io.StringIO()
        expected = current

    # Restore standard streams before assertions or pytest's error reporting.
    with monkeypatch.context() as patch:
        patch.setattr(sys, name, current)
        patch.setattr(sys, '__' + name + '__', original)
        actual = get_binary()
    assert actual is expected


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


@pytest.mark.parametrize('compression', [zipfile.ZIP_STORED,
                                         zipfile.ZIP_DEFLATED])
@pytest.mark.skipif(sys.version_info < (3, 6),
                    reason='ZipFile.open() writes require Python 3.6')
def test_zipsource_write_csv(tmpdir, compression):
    filename = str(tmpdir.join('table.zip'))
    table = [('name', 'value'), ('alpha', '1'), ('beta', '2')]
    source = ZipSource(filename, 'data.csv', compression=compression)

    etl.tocsv(table, source, lineterminator='\n')

    with zipfile.ZipFile(filename) as archive:
        eq_(archive.namelist(), ['data.csv'])
        eq_(archive.read('data.csv'), b'name,value\nalpha,1\nbeta,2\n')
    ieq(table, etl.fromcsv(source))


@pytest.mark.parametrize('mode', ['rb', 'wb'])
@pytest.mark.parametrize('error_type', [None, RuntimeError, KeyboardInterrupt])
def test_zipsource_closes_member(tmpdir, monkeypatch, mode, error_type):
    if mode == 'wb' and sys.version_info < (3, 6):
        pytest.skip('ZipFile.open() writes require Python 3.6')
    filename = str(tmpdir.join('table.zip'))
    payload = b'name,value\nalpha,1\n'
    if mode == 'rb':
        with zipfile.ZipFile(filename, 'w') as archive:
            archive.writestr('data.csv', payload)

    # Track real archives so the failing baseline can also release its handles.
    archives = []
    zip_file = zipfile.ZipFile

    def tracked_zip_file(*args, **kwargs):
        archive = zip_file(*args, **kwargs)
        archives.append(archive)
        return archive

    monkeypatch.setattr(zipfile, 'ZipFile', tracked_zip_file)
    source = ZipSource(filename, 'data.csv')
    error = error_type('interrupted ZIP operation') if error_type else None
    member = None
    try:
        try:
            with source.open(mode) as member:
                if mode == 'rb':
                    eq_(member.read(1), payload[:1])
                else:
                    eq_(member.write(payload), len(payload))
                if error is not None:
                    raise error
        except BaseException as actual:
            if error is None:
                raise
            assert actual is error
        else:
            assert error is None
        assert member.closed
        # A retained member must no longer keep the archive file open on Windows.
        os.remove(filename)
    finally:
        if member is not None:
            member.close()
        for archive in archives:
            archive.close()


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


def test_bzip2source_without_bz2_module():

    # python can be built without the bz2 module, petl should still import
    # and only fail when a .bz2 file is actually used
    fn = NamedTemporaryFile().name
    etl.tocsv([('foo', 'bar'), ('a', '1')], fn + '.bz2')
    code = """
import sys
sys.modules['bz2'] = None
import petl as etl
fn = sys.argv[1]
tbl = [('foo', 'bar'), ('a', '1')]
etl.tocsv(tbl, fn + '.csv')
assert list(etl.fromcsv(fn + '.csv')) == tbl
for f in (lambda: etl.tocsv(tbl, fn + '.out.bz2'),
          lambda: list(etl.fromcsv(fn + '.bz2'))):
    try:
        f()
    except ImportError as e:
        assert 'bz2' in str(e), str(e)
    else:
        raise RuntimeError('expected ImportError')
"""
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(etl.__file__)))
    subprocess.check_call([sys.executable, '-c', code, fn], cwd=cwd)

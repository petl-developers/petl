from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


import gzip
import sys
import bz2
import zipfile
import urllib2
from contextlib import contextmanager
import cStringIO
import subprocess


class FileSource(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open_(self, mode='r'):
        return open(self.filename, mode, **self.kwargs)


class GzipSource(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    @contextmanager
    def open_(self, mode='r'):
        source = gzip.open(self.filename, mode, **self.kwargs)
        try:
            yield source
        finally:
            source.close()


class BZ2Source(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open_(self, mode='r'):
        return bz2.BZ2File(self.filename, mode, **self.kwargs)


class ZipSource(object):

    def __init__(self, filename, membername, pwd=None, **kwargs):
        self.filename = filename
        self.membername = membername
        self.pwd = pwd
        self.kwargs = kwargs

    @contextmanager
    def open_(self, mode):
        outer_mode = mode.translate(None, 'bU')
        zf = zipfile.ZipFile(self.filename, outer_mode, **self.kwargs)
        try:
            if self.pwd is not None:
                yield zf.open(self.membername, mode, self.pwd)
            else:
                yield zf.open(self.membername, mode)
        finally:
            zf.close()


class StdinSource(object):

    @contextmanager
    def open_(self, mode='r'):
        if not mode.startswith('r'):
            raise Exception('source is read-only')
        yield sys.stdin


class StdoutSource(object):

    @contextmanager
    def open_(self, mode):
        if mode.startswith('r'):
            raise Exception('source is write-only')
        yield sys.stdout


class URLSource(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open_(self, mode='r'):
        if not mode.startswith('r'):
            raise Exception('source is read-only')
        f = urllib2.urlopen(*self.args, **self.kwargs)
        try:
            yield f
        finally:
            f.close()


class StringSource(object):

    def __init__(self, s=None):
        self.s = s
        self.buffer = None

    @contextmanager
    def open_(self, mode='r'):
        try:
            if mode.startswith('r'):  # read
                if self.s is not None:
                    self.buffer = cStringIO.StringIO(self.s)
                else:
                    raise Exception('no string data supplied')
            elif mode.startswith('w'):  # write
                # drop existing buffer
                if self.buffer is not None:
                    self.buffer.close()
                # new buffer
                self.buffer = cStringIO.StringIO()
            elif mode.startswith('a'):  # append
                # new buffer only if none already
                if self.buffer is None:
                    self.buffer = cStringIO.StringIO()
            yield self.buffer
        except:
            raise
        finally:
            pass # don't close the buffer

    def getvalue(self):
        if self.buffer:
            return self.buffer.getvalue()


class PopenSource(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open_(self, mode='r'):
        if not mode.startswith('r'):
            raise Exception('source is read-only')
        self.kwargs['stdout'] = subprocess.PIPE
        proc = subprocess.Popen(*self.args, **self.kwargs)
        try:
            yield proc.stdout
        finally:
            pass


_invalid_source_msg = 'invalid source argument, expected None or a string or ' \
                      'an object implementing open_(), found %r'


def read_source_from_arg(source):
    if source is None:
        return StdinSource()
    elif isinstance(source, basestring):
        if any(map(source.startswith, ['http://', 'https://', 'ftp://'])):
            return URLSource(source)
        elif source.endswith('.gz') or source.endswith('.bgz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        assert (hasattr(source, 'open_')
                and callable(getattr(source, 'open_'))), \
            _invalid_source_msg % source
        return source


def write_source_from_arg(source):
    if source is None:
        return StdoutSource()
    elif isinstance(source, basestring):
        if source.endswith('.gz') or source.endswith('.bgz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        assert (hasattr(source, 'open_')
                and callable(getattr(source, 'open_'))), \
            _invalid_source_msg % source
        return source

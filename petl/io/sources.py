# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import os
import io
import gzip
import sys
import bz2
import zipfile
from contextlib import contextmanager
import subprocess
import logging


from petl.errors import ArgumentError
from petl.compat import urlopen, StringIO, BytesIO, string_types, PY2


logger = logging.getLogger(__name__)
warning = logger.warning
info = logger.info
debug = logger.debug


class FileSource(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open(self, mode='r'):
        return io.open(self.filename, mode, **self.kwargs)


class GzipSource(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        source = gzip.open(self.filename, mode, **self.kwargs)
        try:
            yield source
        finally:
            source.close()


class BZ2Source(object):

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open(self, mode='r'):
        return bz2.BZ2File(self.filename, mode, **self.kwargs)


class ZipSource(object):

    def __init__(self, filename, membername, pwd=None, **kwargs):
        self.filename = filename
        self.membername = membername
        self.pwd = pwd
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode):
        if PY2:
            mode = mode.translate(None, 'bU')
        else:
            mode = mode.translate({ord('b'): None, ord('U'): None})
        zf = zipfile.ZipFile(self.filename, mode, **self.kwargs)
        try:
            if self.pwd is not None:
                yield zf.open(self.membername, mode, self.pwd)
            else:
                yield zf.open(self.membername, mode)
        finally:
            zf.close()


class StdinSource(object):

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')
        yield Uncloseable(os.fdopen(sys.__stdin__.fileno(), mode, 0))
        # if 'b' in mode:
        #     if PY2:
        #         yield sys.__stdin__
        #     else:
        #         yield sys.__stdin__.buffer
        # else:
        #     if PY2:
        #         yield sys.__stdin__
        #     else:
        #         yield sys.__stdin__


class Uncloseable(object):

    def __init__(self, inner):
        object.__setattr__(self, '_inner', inner)

    def __getattr__(self, item):
        return getattr(self._inner, item)

    def __setattr__(self, key, value):
        setattr(self._inner, key, value)

    def close(self):
        debug('uncloseable: close called')
        pass


if PY2:

    if hasattr(sys.stdout, 'fileno'):
        _stdout = sys.stdout
    else:
        # fall back to original stdout
        _stdout = sys.__stdout__
    try:
        # open once only
        _stdout_binary = os.fdopen(_stdout.fileno(), 'ab', 0)
    except Exception as e:
        warning('stdout is unavailable: %s' % e)


class StdoutSource(object):

    @contextmanager
    def open(self, mode):
        if mode.startswith('r'):
            raise ArgumentError('source is write-only')
        if 'b' in mode:
            yield _stdout_binary
        else:
            yield _stdout


class URLSource(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')
        f = urlopen(*self.args, **self.kwargs)
        try:
            yield f
        finally:
            f.close()


class MemorySource(object):

    def __init__(self, s=None):
        self.s = s
        self.buffer = None

    @contextmanager
    def open(self, mode='rb'):
        try:
            if 'r' in mode:
                if self.s is not None:
                    if 'b' in mode:
                        self.buffer = BytesIO(self.s)
                    else:
                        self.buffer = StringIO(self.s)
                else:
                    raise ArgumentError('no string data supplied')
            elif 'w' in mode:
                if self.buffer is not None:
                    self.buffer.close()
                if 'b' in mode:
                    self.buffer = BytesIO()
                else:
                    self.buffer = StringIO()
            elif 'a' in mode:
                if self.buffer is None:
                    if 'b' in self.buffer:
                        self.buffer = BytesIO()
                    else:
                        self.buffer = StringIO()
            yield self.buffer
        except:
            raise
        finally:
            pass  # don't close the buffer

    def getvalue(self):
        if self.buffer:
            return self.buffer.getvalue()


# backwards compatibility
StringSource = MemorySource


class PopenSource(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')
        self.kwargs['stdout'] = subprocess.PIPE
        proc = subprocess.Popen(*self.args, **self.kwargs)
        try:
            yield proc.stdout
        finally:
            pass


_invalid_source_msg = 'invalid source argument, expected None or a string or ' \
                      'an object implementing open(), found %r'


def read_source_from_arg(source):
    if source is None:
        return StdinSource()
    elif isinstance(source, string_types):
        if any(map(source.startswith, ['http://', 'https://', 'ftp://'])):
            return URLSource(source)
        elif source.endswith('.gz') or source.endswith('.bgz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        assert (hasattr(source, 'open')
                and callable(getattr(source, 'open'))), \
            _invalid_source_msg % source
        return source


def write_source_from_arg(source):
    if source is None:
        return StdoutSource()
    elif isinstance(source, string_types):
        if source.endswith('.gz') or source.endswith('.bgz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        assert (hasattr(source, 'open')
                and callable(getattr(source, 'open'))), \
            _invalid_source_msg % source
        return source

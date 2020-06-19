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

    def __init__(self, filename, remote=False, **kwargs):
        self.filename = filename
        self.remote = remote
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if self.remote:
            if not mode.startswith('r'):
                raise ArgumentError('source is read-only')
            filehandle = urlopen(self.filename)
        else:
            filehandle = self.filename
        source = gzip.open(filehandle, mode, **self.kwargs)
        try:
            yield source
        finally:
            source.close()


class BZ2Source(object):

    def __init__(self, filename, remote=False, **kwargs):
        self.filename = filename
        self.remote = remote
        self.kwargs = kwargs

    @contextmanager
    def open(self, mode='r'):
        if self.remote:
            if not mode.startswith('r'):
                raise ArgumentError('source is read-only')
            filehandle = urlopen(self.filename)
        else:
            filehandle = self.filename
        source = bz2.BZ2File(filehandle, mode, **self.kwargs)
        try:
            yield source
        finally:
            source.close()


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


class Uncloseable(object):

    def __init__(self, inner):
        object.__setattr__(self, '_inner', inner)

    def __getattr__(self, item):
        return getattr(self._inner, item)

    def __setattr__(self, key, value):
        setattr(self._inner, key, value)

    def close(self):
        debug('Uncloseable: close called (%r)' % self._inner)
        pass


def _get_stdout_binary():
    try:
        return sys.stdout.buffer
    except AttributeError:
        pass
    try:
        fd = sys.stdout.fileno()
        return os.fdopen(fd, 'ab', 0)
    except Exception:
        pass
    try:
        return sys.__stdout__.buffer
    except AttributeError:
        pass
    try:
        fd = sys.__stdout__.fileno()
        return os.fdopen(fd, 'ab', 0)
    except Exception:
        pass
    # fallback
    return sys.stdout


stdout_binary = _get_stdout_binary()


def _get_stdin_binary():
    try:
        return sys.stdin.buffer
    except AttributeError:
        pass
    try:
        fd = sys.stdin.fileno()
        return os.fdopen(fd, 'rb', 0)
    except Exception:
        pass
    try:
        return sys.__stdin__.buffer
    except AttributeError:
        pass
    try:
        fd = sys.__stdin__.fileno()
        return os.fdopen(fd, 'rb', 0)
    except Exception:
        pass
    # fallback
    return sys.stdin


stdin_binary = _get_stdin_binary()


class StdoutSource(object):

    @contextmanager
    def open(self, mode):
        if mode.startswith('r'):
            raise ArgumentError('source is write-only')
        if 'b' in mode:
            yield Uncloseable(stdout_binary)
        else:
            yield Uncloseable(sys.stdout)


class StdinSource(object):

    @contextmanager
    def open(self, mode='r'):
        if not mode.startswith('r'):
            raise ArgumentError('source is read-only')
        if 'b' in mode:
            yield Uncloseable(stdin_binary)
        else:
            yield Uncloseable(sys.stdin)


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
    """Memory data source. E.g.::

        >>> import petl as etl
        >>> data = b'foo,bar\\na,1\\nb,2\\nc,2\\n'
        >>> source = etl.MemorySource(data)
        >>> tbl = etl.fromcsv(source)
        >>> tbl
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

        >>> sink = etl.MemorySource()
        >>> tbl.tojson(sink)
        >>> sink.getvalue()
        b'[{"foo": "a", "bar": "1"}, {"foo": "b", "bar": "2"}, {"foo": "c", "bar": "2"}]'

    Also supports appending.

    """

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
                    if 'b' in mode:
                        self.buffer = BytesIO()
                    else:
                        self.buffer = StringIO()
            yield Uncloseable(self.buffer)
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


_READERS = {}
_CODECS = {}
_WRITERS = {}


def register_codec(extension, handler_class):
    '''Allows automatically compressing and decompressing in write and read.'''

    assert isinstance(extension, string_types), _invalid_source_msg % extension
    assert isinstance(handler_class, type), _invalid_source_msg % extension
    _CODECS[extension] = handler_class


def register_reader(protocol, handler_class):
    '''Allows automatically reading data from unsupported filesystems.'''

    assert isinstance(protocol, string_types), _invalid_source_msg % protocol
    assert isinstance(handler_class, type), _invalid_source_msg % protocol
    _READERS[protocol] = handler_class


def register_writer(protocol, handler_class):
    '''Allows automatically writing data to unsupported filesystems.'''

    assert isinstance(protocol, string_types), _invalid_source_msg % protocol
    assert isinstance(handler_class, type), _invalid_source_msg % protocol
    _WRITERS[protocol] = handler_class

# Setup default sources
    
register_codec('.gz', GzipSource)
register_codec('.bgz', GzipSource)
register_codec('.bz2', BZ2Source)
    
register_reader('ftp', URLSource)
register_reader('http', URLSource)
register_reader('https', URLSource)


def _get_codec_for(source):
    for ext, codec_class in _CODECS.items():
        if source.endswith(ext):
            return codec_class
    return None


def _get_handler_from(source, handlers):
    protocol_index = source.find('://')
    if protocol_index <= 0:
        return None
    protocol = source[:protocol_index]
    for prefix, handler_class in handlers.items():
        if prefix == protocol:
            print('# Using protocol: {}'.format(protocol))
            return handler_class
    print('# No handler for: {}'.format(source))
    return None


def _resolve_source_from_arg(source, handlers, sync_mode):
    if source is None:
        return StdinSource()
    elif isinstance(source, string_types):
        handler = _get_handler_from(source, handlers)
        codec = _get_codec_for(source)
        if handler is None:
            if codec is not None:
                return codec(source)
            return FileSource(source)
        io_handler = handler(source)
        if codec is None:
            return io_handler
        # lay a en/decoder over the reader/writer of the protocol
        stream = io_handler.open(mode=sync_mode)
        coder = codec(stream)
        return coder
    else:
        assert (hasattr(source, 'open')
                and callable(getattr(source, 'open'))), \
            _invalid_source_msg % source
        return source


def read_source_from_arg(source):
    return _resolve_source_from_arg(source, _READERS, 'rb')


def write_source_from_arg(source, mode='wb'):
    return _resolve_source_from_arg(source, _WRITERS, mode)

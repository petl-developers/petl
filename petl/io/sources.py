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


class CompressedSource(object):
    '''Handle IO from a file-like object and (de)compress with a codec
    
    The `source` argument (source class) is the source class that will handle
    the actual input/output stream. E.g: :class:`petl.io.sources.URLSource`.
    
    The `codec` argument (source class) is the source class that will handle
    the (de)compression of the stream. E.g: :class:`petl.io.sources.GzipSource`.
    '''

    def __init__(self, source, codec):
        self.source = source
        self.codec = codec

    @contextmanager
    def open(self, mode='rb'):
        with self.source.open(mode=mode) as filehandle:
            transcoder = self.codec(filehandle)
            with transcoder.open(mode=mode) as stream:
                yield stream


_invalid_source_msg = 'invalid source argument, expected None or a string or ' \
                      'an object implementing open(), found %r'


_READERS = {}
_CODECS = {}
_WRITERS = {}


def _assert_source_has_open(source_class):
    source = source_class('test')
    assert (hasattr(source, 'open')
            and callable(getattr(source, 'open'))), \
        _invalid_source_msg % source


def _register_handler(handler_type, handler_class, handler_list):

    assert isinstance(handler_type, string_types), _invalid_source_msg % handler_type
    assert isinstance(handler_class, type), _invalid_source_msg % handler_type
    _assert_source_has_open(handler_class)
    handler_list[handler_type] = handler_class


def _get_handler(handler_type, handler_list):

    if isinstance(handler_type, string_types):
        if handler_type in handler_list:
            return handler_list[handler_type]
    return None


def register_codec(extension, handler_class):
    '''
    Register handler for automatic compression and decompression for file I/O

    Use of the handler is determined matching the file `extension` with the
    source specified in ``from...()`` and ``to...()`` functions.

    .. versionadded:: 1.5.0
    '''
    _register_handler(extension, handler_class, _CODECS)


def register_reader(protocol, handler_class):
    '''
    Register handler for automatic reading using a remote protocol.

    Use of the handler is determined matching the `protocol` with the scheme
    part of the url in ``from...()`` function (e.g: `http://`).

    .. versionadded:: 1.5.0
    '''
    _register_handler(protocol, handler_class, _READERS)


def register_writer(protocol, handler_class):
    '''
    Register handler for automatic writing using a remote protocol.

    Use of the handler is determined matching the `protocol` with the scheme
    part of the url in ``to...()`` function (e.g: `smb://`).

    .. versionadded:: 1.5.0
    '''
    _register_handler(protocol, handler_class, _WRITERS)


def get_reader(protocol):
    '''
    Retrieve the handler responsible for reading from a remote protocol.

    .. versionadded:: 1.6.0
    '''
    return _get_handler(protocol, _READERS)


def get_writer(protocol):
    '''
    Retrieve the handler responsible for writing from a remote protocol.

    .. versionadded:: 1.6.0
    '''
    return _get_handler(protocol, _WRITERS)


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
            return handler_class
    return None


def _resolve_source_from_arg(source, handlers):
    if isinstance(source, string_types):
        handler = _get_handler_from(source, handlers)
        codec = _get_codec_for(source)
        if handler is None:
            if codec is not None:
                return codec(source)
            assert '://' not in source, _invalid_source_msg % source
            return FileSource(source)
        return handler(source)
    else:
        assert (hasattr(source, 'open')
                and callable(getattr(source, 'open'))), \
            _invalid_source_msg % source
        return source


def read_source_from_arg(source):
    '''
    Retrieve a open stream for reading from the source provided.

    The result stream will be open by a handler that would return raw bytes and
    transparently take care of the decompression, remote authentication,
    network transfer, format decoding, and data extraction.

    .. versionadded:: 1.4.0
    '''
    if source is None:
        return StdinSource()
    return _resolve_source_from_arg(source, _READERS)


def write_source_from_arg(source, mode='wb'):
    '''
    Retrieve a open stream for writing to the source provided.

    The result stream will be open by a handler that would write raw bytes and
    transparently take care of the compression, remote authentication,
    network transfer, format encoding, and data writing.

    .. versionadded:: 1.4.0
    '''
    if source is None:
        return StdoutSource()
    return _resolve_source_from_arg(source, _WRITERS)

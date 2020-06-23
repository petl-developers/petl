# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import io
from contextlib import contextmanager

from petl.io.sources import register_codec


class ZstandardCodec(object):
    '''
    Allows compressing and decompressing .zstd files

    `Zstandard`_ is a real-time compression algorithm, providing 
    high compression ratios. It offers a very wide range of compression / speed
    trade-off, while being backed by a very fast decoder.

    .. note::

        For working this codec require `zstd`_ to be installed, e.g.::

            $ pip install zstandard

    .. versionadded:: 1.5.0

    .. _zstd: https://github.com/indygreg/python-zstandard
    .. _Zstandard: http://www.zstd.net
    '''

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open_file(self, mode='rb'):
        import zstandard as zstd
        if mode.startswith('r'):
            cctx = zstd.ZstdDecompressor(**self.kwargs)
            compressed = io.open(self.filename, mode)
            source = cctx.stream_reader(compressed)
        else:
            cctx = zstd.ZstdCompressor(**self.kwargs)
            uncompressed = io.open(self.filename, mode)
            source = cctx.stream_writer(uncompressed)
        return source

    @contextmanager
    def open(self, mode='r'):
        mode2 = mode[:1] + r'b'  # python2
        source = self.open_file(mode=mode2)
        try:
            yield source
        finally:
            source.close()


register_codec('.zst', ZstandardCodec)

# end #

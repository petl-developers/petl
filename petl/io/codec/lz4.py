# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from contextlib import contextmanager

from petl.io.sources import register_codec


class LZ4Codec(object):
    '''
    Allows compressing and decompressing .lz4 files

    `LZ4`_ is lossless compression algorithm, providing compression
    speed greather than 500 MB/s per core (>0.15 Bytes/cycle). It features an
    extremely fast decoder, with speed in multiple GB/s per core (~1Byte/cycle)

    .. note::

        For working this codec require `python-lz4`_ to be installed, e.g.::

            $ pip install lz4

    .. versionadded:: 1.5.0

    .. _python-lz4: https://github.com/python-lz4/python-lz4
    .. _LZ4: http://www.lz4.org
    '''

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open_file(self, mode='rb'):
        import lz4.frame
        source = lz4.frame.open(self.filename, mode=mode, **self.kwargs)
        return source

    @contextmanager
    def open(self, mode='r'):
        mode2 = mode[:1] + r'b' # python2
        source = self.open_file(mode=mode2)
        try:
            yield source
        finally:
            source.close()


register_codec('.lz4', LZ4Codec)

# end #

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

from contextlib import contextmanager

from petl.io.sources import register_codec

class XZCodec(object):
    '''
    Allows compressing and decompressing .xz files compressed with `lzma`_.

    .. versionadded:: 1.5.0

    .. _lzma: https://docs.python.org/3/library/lzma.html
    '''

    def __init__(self, filename, **kwargs):
        self.filename = filename
        self.kwargs = kwargs

    def open_file(self, mode='rb'):
        import lzma
        source = lzma.open(self.filename, mode=mode, **self.kwargs)
        return source

    @contextmanager
    def open(self, mode='r'):
        mode2 = mode[:1] + r'b' # python2
        source = self.open_file(mode=mode2)
        try:
            yield source
        finally:
            source.close()


register_codec('.xz', XZCodec)

# end #

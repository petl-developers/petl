# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import io
from petl.io.sources import register_reader, register_writer
from contextlib import contextmanager


class S3Source(object):
    '''Downloads or uploads to AWS S3 filesystem. E.g.::

        >>> def example_s3():
        ...     import petl as etl
        ...     url = 's3://mybucket/prefix/to/myfilename.csv'
        ...     data = b'foo,bar\\na,1\\nb,2\\nc,2\\n'
        ...     etl.tocsv(data, url)
        ...     tbl = etl.fromcsv(url)
        ...     
        >>> example_s3() # doctest: +SKIP
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' | '1' |
        +-----+-----+
        | 'b' | '2' |
        +-----+-----+
        | 'c' | '2' |
        +-----+-----+

    .. note::

        For working this source require `s3fs`_ to be installed, e.g.::

            $ pip install s3fs

    It is strongly recommended that you open files in binary mode.

    For authentication check `credentials`_.

    .. versionadded:: 1.5.0

    .. _s3fs: https://github.com/dask/s3fs/
    .. _credentials: https://s3fs.readthedocs.io/en/latest/#credentials
    '''

    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs

    def open_file(self, mode='rb'):
        import s3fs
        fs = s3fs.S3FileSystem()
        source = fs.open(self.url, mode=mode, **self.kwargs)
        return source

    @contextmanager
    def open(self, mode='rb'):
        mode2 = mode[:1] + r'b' # python2
        source = self.open_file(mode=mode2)
        try:
            yield source
        finally:
            source.close()


register_reader('s3', S3Source)
register_writer('s3', S3Source)

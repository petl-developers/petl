# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import locale
import codecs
from petl.compat import izip_longest

from petl.util.base import Table


def getcodec(encoding):
    if encoding is None:
        encoding = locale.getpreferredencoding()
    codec = codecs.lookup(encoding)
    return codec


def fromcolumns(cols, header=None, missing=None):
    """View a sequence of columns as a table, e.g.::

        >>> import petl as etl
        >>> cols = [[0, 1, 2], ['a', 'b', 'c']]
        >>> tbl = etl.fromcolumns(cols)
        >>> tbl
        +----+-----+
        | f0 | f1  |
        +====+=====+
        |  0 | 'a' |
        +----+-----+
        |  1 | 'b' |
        +----+-----+
        |  2 | 'c' |
        +----+-----+

    If columns are not the same length, values will be padded to the length
    of the longest column with `missing`, which is None by default, e.g.::

        >>> cols = [[0, 1, 2], ['a', 'b']]
        >>> tbl = etl.fromcolumns(cols, missing='NA')
        >>> tbl
        +----+------+
        | f0 | f1   |
        +====+======+
        |  0 | 'a'  |
        +----+------+
        |  1 | 'b'  |
        +----+------+
        |  2 | 'NA' |
        +----+------+

    See also :func:`petl.io.json.fromdicts`.

    .. versionadded:: 1.1.0

    """

    return ColumnsView(cols, header=header, missing=missing)


class ColumnsView(Table):

    def __init__(self, cols, header=None, missing=None):
        self.cols = cols
        self.header = header
        self.missing = missing

    def __iter__(self):
        return itercolumns(self.cols, self.header, self.missing)


def itercolumns(cols, header, missing):
    if header is None:
        header = ['f%s' % i for i in range(len(cols))]
    yield tuple(header)
    for row in izip_longest(*cols, **dict(fillvalue=missing)):
        yield row

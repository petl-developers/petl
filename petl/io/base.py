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


def fromcolumns(cols, header=None):
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

    See also :func:`petl.io.json.fromdicts`.

    .. versionadded:: 1.1.0

    """

    return ColumnsView(cols, header)


class ColumnsView(Table):

    def __init__(self, cols, header=None):
        self.cols = cols
        self.header = header

    def __iter__(self):
        return itercolumns(self.cols, self.header)


def itercolumns(cols, header):
    if header is None:
        header = ['f%s' % i for i in range(len(cols))]
    yield tuple(header)
    for row in izip_longest(*cols):
        yield row

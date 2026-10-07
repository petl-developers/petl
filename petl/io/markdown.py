# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import io
from petl.compat import text_type, next, PY2, izip_longest


# internal dependencies
from petl.util.base import Table
from petl.io.base import getcodec
from petl.io.sources import write_source_from_arg


def tomarkdown(table, source=None, encoding=None, errors='strict',
               vrepr=text_type, lineterminator='\n'):
    """
    Write the table as a Markdown pipe table, the format used by GitHub,
    GitLab, Jupyter and most other Markdown renderers. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 'x|y']]
        >>> etl.tomarkdown(table1, 'example.md')
        >>> print(open('example.md').read())
        | foo | bar |
        | --- | --- |
        | a | 1 |
        | b | 2 |
        | c | x\\|y |

    Values are converted to text with the `vrepr` function. Pipe characters
    are escaped and line breaks are replaced with ``<br>``, so that each row
    is written on a single line.

    .. versionadded:: 1.7.29

    """

    source = write_source_from_arg(source)
    with source.open('wb') as buf:

        # deal with text encoding
        if PY2:
            codec = getcodec(encoding)
            f = codec.streamwriter(buf, errors=errors)
        else:
            f = io.TextIOWrapper(buf,
                                 encoding=encoding,
                                 errors=errors,
                                 newline='')

        # write the table
        try:
            it = iter(table)
            try:
                hdr = next(it)
            except StopIteration:
                hdr = []
            if hdr:
                _write_row(f, [_escape(text_type(h)) for h in hdr],
                           lineterminator)
                _write_row(f, ['---'] * len(hdr), lineterminator)
                for row in it:
                    cells = [_escape(vrepr(v)) for _, v in
                             izip_longest(hdr, row, fillvalue=None)]
                    _write_row(f, cells, lineterminator)
            f.flush()

        finally:
            if not PY2:
                f.detach()


Table.tomarkdown = tomarkdown


def _escape(s):
    s = s.replace('|', '\\|')
    s = s.replace('\r\n', '<br>').replace('\r', '<br>').replace('\n', '<br>')
    return s


def _write_row(f, cells, lineterminator):
    f.write('| ' + ' | '.join(cells) + ' |' + lineterminator)

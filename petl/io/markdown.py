# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import io
from itertools import chain
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

    Values are converted to text with the `vrepr` function. Backslashes and
    pipe characters are escaped and line breaks are replaced with ``<br>``,
    so that each row is written on a single line. If the table has an empty
    header, a blank header row is written, as Markdown tables require one.

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
            flds = [_escape(text_type(h)) for h in hdr]
            if not flds:
                # a pipe table needs a header row, so leave it blank
                try:
                    row = next(it)
                except StopIteration:
                    pass
                else:
                    flds = [''] * max(len(row), 1)
                    it = chain([row], it)
            if flds:
                _write_row(f, flds, lineterminator)
                _write_row(f, ['---'] * len(flds), lineterminator)
                for row in it:
                    cells = [_escape(vrepr(v)) for _, v in
                             izip_longest(flds, row, fillvalue=None)]
                    _write_row(f, cells, lineterminator)
            f.flush()

        finally:
            if not PY2:
                f.detach()


Table.tomarkdown = tomarkdown


def _escape(s):
    s = s.replace('\\', '\\\\').replace('|', '\\|')
    s = s.replace('\r\n', '<br>').replace('\r', '<br>').replace('\n', '<br>')
    return s


def _write_row(f, cells, lineterminator):
    f.write('| ' + ' | '.join(cells) + ' |' + lineterminator)

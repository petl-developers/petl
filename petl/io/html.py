# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# standard library dependencies
import io
from petl.compat import text_type, numeric_types, next, PY2, izip_longest, \
    string_types, callable


# internal dependencies
from petl.errors import ArgumentError
from petl.util.base import Table, Record
from petl.io.base import getcodec
from petl.io.sources import write_source_from_arg


def tohtml(table, source=None, encoding=None, errors='strict', caption=None,
           vrepr=text_type, lineterminator='\n', index_header=False,
           tr_style=None, td_styles=None, truncate=None):
    """
    Write the table as HTML to a file. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> etl.tohtml(table1, 'example.html', caption='example table')
        >>> print(open('example.html').read())
        <table class='petl'>
        <caption>example table</caption>
        <thead>
        <tr>
        <th>foo</th>
        <th>bar</th>
        </tr>
        </thead>
        <tbody>
        <tr>
        <td>a</td>
        <td style='text-align: right'>1</td>
        </tr>
        <tr>
        <td>b</td>
        <td style='text-align: right'>2</td>
        </tr>
        <tr>
        <td>c</td>
        <td style='text-align: right'>2</td>
        </tr>
        </tbody>
        </table>

    The `caption` keyword argument is used to provide a table caption
    in the output HTML.

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

            # write header
            try:
                hdr = next(it)
            except StopIteration:
                hdr = []
            _write_begin(f, hdr, lineterminator, caption, index_header,
                         truncate)

            # write body
            if tr_style and callable(tr_style):
                # wrap as records
                it = (Record(row, hdr) for row in it)
            for row in it:
                _write_row(f, hdr, row, lineterminator, vrepr,
                           tr_style, td_styles, truncate)

            # finish up
            _write_end(f, lineterminator)
            f.flush()

        finally:
            if not PY2:
                f.detach()


Table.tohtml = tohtml


def teehtml(table, source=None, encoding=None, errors='strict', caption=None,
            vrepr=text_type, lineterminator='\n', index_header=False,
            tr_style=None, td_styles=None, truncate=None):
    """
    Return a table that writes rows to a Unicode HTML file as they are
    iterated over.

    """

    source = write_source_from_arg(source)
    return TeeHTMLView(table, source=source, encoding=encoding, errors=errors,
                       caption=caption, vrepr=vrepr,
                       lineterminator=lineterminator, index_header=index_header,
                       tr_style=tr_style, td_styles=td_styles,
                       truncate=truncate)


Table.teehtml = teehtml


class TeeHTMLView(Table):
    def __init__(self, table, source=None, encoding=None, errors='strict',
                 caption=None, vrepr=text_type, lineterminator='\n',
                 index_header=False, tr_style=None, td_styles=None,
                 truncate=None):
        self.table = table
        self.source = source
        self.encoding = encoding
        self.errors = errors
        self.caption = caption
        self.vrepr = vrepr
        self.lineterminator = lineterminator
        self.index_header = index_header
        self.tr_style = tr_style
        self.td_styles = td_styles
        self.truncate = truncate

    def __iter__(self):
        table = self.table
        source = self.source
        encoding = self.encoding
        errors = self.errors
        lineterminator = self.lineterminator
        caption = self.caption
        index_header = self.index_header
        tr_style = self.tr_style
        td_styles = self.td_styles
        vrepr = self.vrepr
        truncate = self.truncate

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

                # write header
                try:
                    hdr = next(it)
                    yield hdr
                except StopIteration:
                    hdr = []
                _write_begin(f, hdr, lineterminator, caption, index_header,
                             truncate)

                # write body
                if tr_style and callable(tr_style):
                    # wrap as records
                    it = (Record(row, hdr) for row in it)
                for row in it:
                    _write_row(f, hdr, row, lineterminator, vrepr,
                               tr_style, td_styles, truncate)
                    yield row

                # finish up
                _write_end(f, lineterminator)
                f.flush()

            finally:
                if not PY2:
                    f.detach()


def _write_begin(f, flds, lineterminator, caption, index_header, truncate):
    f.write("<table class='petl'>" + lineterminator)
    if caption is not None:
        f.write(('<caption>%s</caption>' % caption) + lineterminator)
    if flds:
        f.write('<thead>' + lineterminator)
        f.write('<tr>' + lineterminator)
        for i, h in enumerate(flds):
            if index_header:
                h = '%s|%s' % (i, h)
            if truncate:
                h = h[:truncate]
            f.write(('<th>%s</th>' % h) + lineterminator)
        f.write('</tr>' + lineterminator)
        f.write('</thead>' + lineterminator)
    f.write('<tbody>' + lineterminator)


def _write_row(f, flds, row, lineterminator, vrepr, tr_style, td_styles,
               truncate):
    tr_css = _get_tr_css(row, tr_style)
    if tr_css:
        f.write(("<tr style='%s'>" % tr_css) + lineterminator)
    else:
        f.write("<tr>" + lineterminator)
    for h, v in izip_longest(flds, row, fillvalue=None):
        r = vrepr(v)
        if truncate:
            r = r[:truncate]
        td_css = _get_td_css(h, v, td_styles)
        if td_css:
            f.write(("<td style='%s'>%s</td>" % (td_css, r)) + lineterminator)
        else:
            f.write(("<td>%s</td>" % r) + lineterminator)
    f.write('</tr>' + lineterminator)


def _get_tr_css(row, tr_style):
    # check for user-provided style
    if tr_style:
        if isinstance(tr_style, string_types):
            return tr_style
        elif callable(tr_style):
            return tr_style(row)
        else:
            raise ArgumentError('expected string or callable, got %r'
                                % tr_style)
    # fall back to default style
    return ''


def _get_td_css(h, v, td_styles):
    # check for user-provided style
    if td_styles:
        if isinstance(td_styles, string_types):
            return td_styles
        elif callable(td_styles):
            return td_styles(v)
        elif isinstance(td_styles, dict):
            if h in td_styles:
                s = td_styles[h]
                if isinstance(s, string_types):
                    return s
                elif callable(s):
                    return s(v)
                else:
                    raise ArgumentError('expected string or callable, got %r'
                                        % s)
        else:
            raise ArgumentError('expected string, callable or dict, got %r'
                                % td_styles)
    # fall back to default style
    if isinstance(v, numeric_types) and not isinstance(v, bool):
        return 'text-align: right'
    else:
        return ''


def _write_end(f, lineterminator):
    f.write('</tbody>' + lineterminator)
    f.write('</table>' + lineterminator)

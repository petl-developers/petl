from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
import codecs
import io
from petl.compat import text_type, numeric_types, next, PY2, izip_longest, \
    string_types, callable


# internal dependencies
from petl.util.base import Table, Record
from petl.io.sources import write_source_from_arg


def tohtml(table, source=None, caption=None, vrepr=text_type,
           lineterminator='\r\n', index_header=False,
           tr_style=None, td_styles=None):
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

    The ``caption`` keyword argument is used to provide a table caption
    in the output HTML.

    """

    touhtml(table, source=source, caption=caption,
            vrepr=vrepr, lineterminator=lineterminator,
            encoding='ascii', index_header=index_header,
            tr_style=tr_style, td_styles=td_styles)


Table.tohtml = tohtml


def touhtml(table, source=None, caption=None, encoding='utf-8',
            vrepr=text_type, lineterminator='\r\n',
            index_header=False, tr_style=None, td_styles=None):
    """
    Write the table as HTML to a text file using the given encoding.

    """

    source = write_source_from_arg(source)
    with source.open_('wb') as f:
        if PY2:
            f = codecs.getwriter(encoding)(f)
        else:
            f = io.TextIOWrapper(f, encoding=encoding, newline='')
        try:
            it = iter(table)
            flds = next(it)
            _write_begin(f, flds, lineterminator, caption, index_header)
            if tr_style and callable(tr_style):
                # wrap as records
                it = (Record(row, flds) for row in it)
            for row in it:
                _write_row(f, flds, row, lineterminator, vrepr,
                           tr_style, td_styles)
            _write_end(f, lineterminator)
        finally:
            if not PY2:
                f.detach()


Table.touhtml = touhtml


def teeuhtml(table, source=None, caption=None,
             encoding='utf-8', vrepr=text_type,
             lineterminator='\r\n', index_header=False,
             tr_style=None, td_styles=None):
    """
    Return a table that writes rows to a Unicode HTML file as they are
    iterated over.

    """

    return TeeUnicodeHTMLView(table, source=source, caption=caption,
                              encoding=encoding, vrepr=vrepr,
                              lineterminator=lineterminator,
                              index_header=index_header,
                              tr_style=tr_style, td_styles=td_styles)


Table.teeuhtml = teeuhtml


class TeeUnicodeHTMLView(Table):

    def __init__(self, table, source=None, caption=None,
                 encoding='utf-8', vrepr=text_type,
                 lineterminator='\r\n', index_header=False,
                 tr_style=None, td_styles=None):
        self.table = table
        self.source = source
        self.caption = caption
        self.encoding = encoding
        self.vrepr = vrepr
        self.lineterminator = lineterminator
        self.index_header = index_header
        self.tr_style = tr_style
        self.td_styles = td_styles

    def __iter__(self):
        source = write_source_from_arg(self.source)
        lineterminator = self.lineterminator
        caption = self.caption
        vrepr = self.vrepr
        index_header = self.index_header
        tr_style = self.tr_style
        td_styles = self.td_styles
        with source.open_('wb') as f:
            if PY2:
                f = codecs.getwriter(self.encoding)(f)
            else:
                f = io.TextIOWrapper(f, encoding=self.encoding, newline='')
            try:
                it = iter(self.table)
                flds = next(it)
                _write_begin(f, flds, lineterminator, caption, index_header)
                yield flds
                if tr_style and callable(tr_style):
                    # wrap as records
                    it = (Record(row, flds) for row in it)
                for row in it:
                    _write_row(f, flds, row, lineterminator, vrepr,
                               tr_style, td_styles)
                    yield row
                _write_end(f, lineterminator)
            finally:
                if not PY2:
                    f.detach()


def teehtml(table, source=None, caption=None, vrepr=text_type,
            lineterminator='\r\n', index_header=False,
            tr_style=None, td_styles=None):
    """
    Return a table that writes rows to an HTML file as they are iterated
    over.

    """

    return TeeUnicodeHTMLView(table, source=source, caption=caption,
                              vrepr=vrepr,
                              lineterminator=lineterminator, encoding='ascii',
                              index_header=index_header,
                              tr_style=tr_style, td_styles=td_styles)


Table.teehtml = teehtml


def _write_begin(f, flds, lineterminator, caption, index_header):
    f.write("<table class='petl'>" + lineterminator)
    if caption is not None:
        f.write(('<caption>%s</caption>' % caption) + lineterminator)
    f.write('<thead>' + lineterminator)
    f.write('<tr>' + lineterminator)
    for i, h in enumerate(flds):
        if index_header:
            h = '%s|%s' % (i, h)
        f.write(('<th>%s</th>' % h) + lineterminator)
    f.write('</tr>' + lineterminator)
    f.write('</thead>' + lineterminator)
    f.write('<tbody>' + lineterminator)


def _write_row(f, flds, row, lineterminator, vrepr, tr_style, td_styles):
    tr_css = _get_tr_css(row, tr_style)
    if tr_css:
        f.write(("<tr style='%s'>" % tr_css) + lineterminator)
    else:
        f.write("<tr>" + lineterminator)
    for h, v in izip_longest(flds, row, fillvalue=None):
        r = vrepr(v)
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
            raise Exception('expected string or callable, got %r' % tr_style)
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
                    raise Exception('expected string or callable, got %r' % s)
        else:
            raise Exception('expected string, callable or dict, got %r'
                            % td_styles)
    # fall back to default style
    if isinstance(v, numeric_types) and not isinstance(v, bool):
        return 'text-align: right'
    else:
        return ''


def _write_end(f, lineterminator):
    f.write('</tbody>' + lineterminator)
    f.write('</table>' + lineterminator)

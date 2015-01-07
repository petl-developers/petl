from __future__ import absolute_import, print_function, division, \
    unicode_literals


# standard library dependencies
import codecs
import io
from petl.compat import text_type, numeric_types, next, PY2


# internal dependencies
from petl.util.base import Table
from petl.io.sources import write_source_from_arg


def tohtml(table, source=None, caption=None, representation=text_type,
           lineterminator='\r\n', index_header=False):
    """Write the table as HTML to a file. E.g.::

        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2],
        ...           ['c', 2]]
        >>> from petl import tohtml
        >>> tohtml(table1, 'example.html', caption='example table')
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
            representation=representation, lineterminator=lineterminator,
            encoding='ascii', index_header=index_header)


Table.tohtml = tohtml


def touhtml(table, source=None, caption=None, encoding='utf-8',
            representation=text_type, lineterminator='\r\n',
            index_header=False):
    """Write the table as HTML to a text file using the given encoding.

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
            for row in it:
                _write_row(f, row, lineterminator, representation)
            _write_end(f, lineterminator)
        finally:
            if not PY2:
                f.detach()


Table.touhtml = touhtml


def teeuhtml(table, source=None, caption=None,
             encoding='utf-8', representation=text_type,
             lineterminator='\r\n', index_header=False):
    """Return a table that writes rows to a Unicode HTML file as they are
    iterated over.

    """

    return TeeUnicodeHTMLView(table, source=source, caption=caption,
                              encoding=encoding, representation=representation,
                              lineterminator=lineterminator,
                              index_header=index_header)


Table.teeuhtml = teeuhtml


class TeeUnicodeHTMLView(Table):

    def __init__(self, table, source=None, caption=None,
                 encoding='utf-8', representation=text_type,
                 lineterminator='\r\n', index_header=False):
        self.table = table
        self.source = source
        self.caption = caption
        self.encoding = encoding
        self.representation = representation
        self.lineterminator = lineterminator
        self.index_header = index_header

    def __iter__(self):
        source = write_source_from_arg(self.source)
        lineterminator = self.lineterminator
        caption = self.caption
        representation = self.representation
        index_header = self.index_header
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
                for row in it:
                    _write_row(f, row, lineterminator, representation)
                    yield row
                _write_end(f, lineterminator)
            finally:
                if not PY2:
                    f.detach()


def teehtml(table, source=None, caption=None, representation=text_type,
            lineterminator='\r\n', index_header=False):
    """Return a table that writes rows to an HTML file as they are iterated
    over.

    """

    return TeeUnicodeHTMLView(table, source=source, caption=caption,
                              representation=representation,
                              lineterminator=lineterminator, encoding='ascii',
                              index_header=False)


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


def _write_row(f, row, lineterminator, representation):
    f.write('<tr>' + lineterminator)
    for v in row:
        r = representation(v)
        if isinstance(v, numeric_types) and not isinstance(v, bool):
            f.write(("<td style='text-align: right'>%s</td>" % r)
                    + lineterminator)
        else:
            f.write(('<td>%s</td>' % r) + lineterminator)
    f.write('</tr>' + lineterminator)


def _write_end(f, lineterminator):
    f.write('</tbody>' + lineterminator)
    f.write('</table>' + lineterminator)

from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


# standard library dependencies
import codecs


# internal dependencies
from petl.io.sources import write_source_from_arg


def tohtml(table, source=None, caption=None, representation=str,
           lineterminator='\r\n'):
    """
    Write the table as HTML to a file. E.g.::

        >>> from petl import tohtml, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

        >>> tohtml(table, 'test.html')

    .. versionadded:: 0.12

    .. versionchanged:: 0.17.1

    Added support for ``caption`` keyword argument to provide table caption
    in output.

    """

    source = write_source_from_arg(source)
    with source.open_('w') as f:
        it = iter(table)
        f.write("<table class='petl'>" + lineterminator)
        if caption is not None:
            f.write(('<caption>%s</caption>' % caption) + lineterminator)
        flds = it.next()
        f.write('<thead>' + lineterminator)
        f.write('<tr>' + lineterminator)
        for h in flds:
            f.write(('<th>%s</th>' % h) + lineterminator)
        f.write('</tr>' + lineterminator)
        f.write('</thead>' + lineterminator)
        f.write('<tbody>' + lineterminator)
        for row in it:
            f.write('<tr>' + lineterminator)
            for v in row:
                r = representation(v)
                if isinstance(v, (int, long, float)) and not isinstance(v, bool):
                    f.write(("<td style='text-align: right'>%s</td>" % r)
                            + lineterminator)
                else:
                    f.write(('<td>%s</td>' % r) + lineterminator)
            f.write('</tr>' + lineterminator)
        f.write('</tbody>' + lineterminator)
        f.write('</table>' + lineterminator)


def touhtml(table, source=None, caption=None, encoding='utf-8',
            representation=unicode, lineterminator=u'\r\n'):
    """
    Write the table as Unicode HTML to a file.

    .. versionadded:: 0.19
    """

    source = write_source_from_arg(source)
    with source.open_('w') as f:
        f = codecs.getwriter(encoding)(f)
        it = iter(table)
        f.write(u"<table class='petl'>" + lineterminator)
        if caption is not None:
            f.write((u'<caption>%s</caption>' % caption) + lineterminator)
        flds = it.next()
        f.write(u'<thead>' + lineterminator)
        f.write(u'<tr>' + lineterminator)
        for h in flds:
            f.write((u'<th>%s</th>' % h) + lineterminator)
        f.write(u'</tr>' + lineterminator)
        f.write(u'</thead>' + lineterminator)
        f.write(u'<tbody>' + lineterminator)
        for row in it:
            f.write(u'<tr>' + lineterminator)
            for v in row:
                r = representation(v)
                if isinstance(v, (int, long, float)) \
                        and not isinstance(v, bool):
                    f.write((u"<td style='text-align: right'>%s</td>" % r)
                            + lineterminator)
                else:
                    f.write((u'<td>%s</td>' % r) + lineterminator)
            f.write(u'</tr>' + lineterminator)
        f.write(u'</tbody>' + lineterminator)
        f.write(u'</table>' + lineterminator)

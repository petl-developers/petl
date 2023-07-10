from __future__ import absolute_import, print_function, division


import locale
from itertools import islice
from collections import defaultdict
from petl.compat import numeric_types, text_type


from petl import config
from petl.util.base import Table
from petl.io.sources import MemorySource
from petl.io.html import tohtml


def look(table, limit=0, vrepr=None, index_header=None, style=None,
         truncate=None, width=None):
    """
    Format a portion of the table as text for inspection in an interactive
    session. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar'],
        ...           ['a', 1],
        ...           ['b', 2]]
        >>> etl.look(table1)
        +-----+-----+
        | foo | bar |
        +=====+=====+
        | 'a' |   1 |
        +-----+-----+
        | 'b' |   2 |
        +-----+-----+

        >>> # alternative formatting styles
        ... etl.look(table1, style='simple')
        ===  ===
        foo  bar
        ===  ===
        'a'    1
        'b'    2
        ===  ===

        >>> etl.look(table1, style='minimal')
        foo  bar
        'a'    1
        'b'    2

        >>> # any irregularities in the length of header and/or data
        ... # rows will appear as blank cells
        ... table2 = [['foo', 'bar'],
        ...           ['a'],
        ...           ['b', 2, True]]
        >>> etl.look(table2)
        +-----+-----+------+
        | foo | bar |      |
        +=====+=====+======+
        | 'a' |     |      |
        +-----+-----+------+
        | 'b' |   2 | True |
        +-----+-----+------+

    Three alternative presentation styles are available: 'grid', 'simple' and
    'minimal', where 'grid' is the default. A different style can be specified
    using the `style` keyword argument. The default style can also be changed
    by setting ``petl.config.look_style``.

    """

    # determine defaults
    if limit == 0:
        limit = config.look_limit
    if vrepr is None:
        vrepr = config.look_vrepr
    if index_header is None:
        index_header = config.look_index_header
    if style is None:
        style = config.look_style
    if width is None:
        width = config.look_width

    return Look(table, limit=limit, vrepr=vrepr, index_header=index_header,
                style=style, truncate=truncate, width=width)


Table.look = look


class Look(object):

    def __init__(self, table, limit, vrepr, index_header, style, truncate,
                 width):
        self.table = table
        self.limit = limit
        self.vrepr = vrepr
        self.index_header = index_header
        self.style = style
        self.truncate = truncate
        self.width = width

    def __repr__(self):

        # determine if table overflows limit
        table, overflow = _vis_overflow(self.table, self.limit)

        # construct output
        style = self.style
        vrepr = self.vrepr
        index_header = self.index_header
        truncate = self.truncate
        width = self.width
        if style == 'simple':
            output = _look_simple(table, vrepr=vrepr,
                                  index_header=index_header,
                                  truncate=truncate, width=width)
        elif style == 'minimal':
            output = _look_minimal(table, vrepr=vrepr,
                                   index_header=index_header,
                                   truncate=truncate, width=width)
        else:
            output = _look_grid(table, vrepr=vrepr, index_header=index_header,
                                truncate=truncate, width=width)

        # add overflow indicator
        if overflow:
            output += '...\n'

        return output

    __str__ = __repr__
    __unicode__ = __repr__


def _table_repr(table):
    return str(look(table))


Table.__repr__ = _table_repr


def lookall(table, **kwargs):
    """
    Format the entire table as text for inspection in an interactive session.

    N.B., this will load the entire table into memory.

    See also :func:`petl.util.vis.look` and :func:`petl.util.vis.see`.

    """

    kwargs['limit'] = None
    return look(table, **kwargs)


def lookstr(table, limit=0, **kwargs):
    """Like :func:`petl.util.vis.look` but use str() rather than repr() for data
    values.

    """

    kwargs['vrepr'] = str
    return look(table, limit=limit, **kwargs)


Table.lookstr = lookstr


def _table_str(table):
    return str(lookstr(table))


Table.__str__ = _table_str
Table.__unicode__ = _table_str


def lookallstr(table, **kwargs):
    """
    Like :func:`petl.util.vis.lookall` but use str() rather than repr() for data
    values.

    """

    kwargs['vrepr'] = str
    return lookall(table, **kwargs)


Table.lookallstr = lookallstr


Table.lookall = lookall


def _look_grid(table, vrepr, index_header, truncate, width):
    it = iter(table)

    # fields representation
    try:
        hdr = next(it)
    except StopIteration:
        return ''
    flds = list(map(text_type, hdr))
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(flds)]
    else:
        fldsrepr = flds

    # rows representations
    rows = list(it)
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(hdr)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(hdr) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(hdr)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

    # truncate
    if truncate:
        fldsrepr = [x[:truncate] for x in fldsrepr]
        rowsrepr = [[x[:truncate] for x in valsrepr]
                    for valsrepr in rowsrepr]

    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen  # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)

    # construct a line separator
    sep = '+'
    for w in colwidths:
        sep += '-' * (w + 2)
        sep += '+'
    if width:
        sep = sep[:width]
    sep += '\n'

    # construct a header separator
    hedsep = '+'
    for w in colwidths:
        hedsep += '=' * (w + 2)
        hedsep += '+'
    if width:
        hedsep = hedsep[:width]
    hedsep += '\n'

    # construct a line for the header row
    fldsline = '|'
    for i, w in enumerate(colwidths):
        f = fldsrepr[i]
        fldsline += ' ' + f
        fldsline += ' ' * (w - len(f))  # padding
        fldsline += ' |'
    if width:
        fldsline = fldsline[:width]
    fldsline += '\n'

    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = '|'
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], numeric_types) \
                    and not isinstance(vals[i], bool):
                # left pad numbers
                rowline += ' ' * (w + 1 - len(vr))  # padding
                rowline += vr + ' |'
            else:
                # right pad everything else
                rowline += ' ' + vr
                rowline += ' ' * (w - len(vr))  # padding
                rowline += ' |'
        if width:
            rowline = rowline[:width]
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = sep + fldsline + hedsep
    for line in rowlines:
        output += line + sep

    return output


def _look_simple(table, vrepr, index_header, truncate, width):
    it = iter(table)

    # fields representation
    try:
        hdr = next(it)
    except StopIteration:
        return ''
    flds = list(map(text_type, hdr))
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(flds)]
    else:
        fldsrepr = flds

    # rows representations
    rows = list(it)
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(hdr)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(hdr) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(hdr)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

    # truncate
    if truncate:
        fldsrepr = [x[:truncate] for x in fldsrepr]
        rowsrepr = [[x[:truncate] for x in valsrepr]
                    for valsrepr in rowsrepr]

    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen  # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)

    # construct a header separator
    hedsep = '  '.join('=' * w for w in colwidths)
    if width:
        hedsep = hedsep[:width]
    hedsep += '\n'

    # construct a line for the header row
    fldsline = '  '.join(f.ljust(w) for f, w in zip(fldsrepr, colwidths))
    if width:
        fldsline = fldsline[:width]
    fldsline += '\n'

    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = ''
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], numeric_types) \
                    and not isinstance(vals[i], bool):
                # left pad numbers
                rowline += vr.rjust(w)
            else:
                # right pad everything else
                rowline += vr.ljust(w)
            if i < len(colwidths) - 1:
                rowline += '  '
        if width:
            rowline = rowline[:width]
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = hedsep + fldsline + hedsep
    for line in rowlines:
        output += line
    output += hedsep

    return output


def _look_minimal(table, vrepr, index_header, truncate, width):
    it = iter(table)

    # fields representation
    try:
        hdr = next(it)
    except StopIteration:
        return ''
    flds = list(map(text_type, hdr))
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(flds)]
    else:
        fldsrepr = flds

    # rows representations
    rows = list(it)
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(hdr)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(hdr) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(hdr)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

    # truncate
    if truncate:
        fldsrepr = [x[:truncate] for x in fldsrepr]
        rowsrepr = [[x[:truncate] for x in valsrepr]
                    for valsrepr in rowsrepr]

    # find longest representations so we know how wide to make cells
    colwidths = [0] * maxrowlen  # initialise to 0
    for i, fr in enumerate(fldsrepr):
        colwidths[i] = len(fr)
    for valsrepr in rowsrepr:
        for i, vr in enumerate(valsrepr):
            if len(vr) > colwidths[i]:
                colwidths[i] = len(vr)

    # construct a line for the header row
    fldsline = '  '.join(f.ljust(w) for f, w in zip(fldsrepr, colwidths))
    if width:
        fldsline = fldsline[:width]
    fldsline += '\n'

    # construct a line for each data row
    rowlines = list()
    for vals, valsrepr in zip(rows, rowsrepr):
        rowline = ''
        for i, w in enumerate(colwidths):
            vr = valsrepr[i]
            if i < len(vals) and isinstance(vals[i], numeric_types) \
                    and not isinstance(vals[i], bool):
                # left pad numbers
                rowline += vr.rjust(w)
            else:
                # right pad everything else
                rowline += vr.ljust(w)
            if i < len(colwidths) - 1:
                rowline += '  '
        if width:
            rowline = rowline[:width]
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = fldsline
    for line in rowlines:
        output += line

    return output


def see(table, limit=0, vrepr=None, index_header=None):
    """
    Format a portion of a table as text in a column-oriented layout for
    inspection in an interactive session. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> etl.see(table)
        foo: 'a', 'b'
        bar: 1, 2

    Useful for tables with a larger number of fields.


    """

    # determine defaults
    if limit == 0:
        limit = config.see_limit
    if vrepr is None:
        vrepr = config.see_vrepr
    if index_header is None:
        index_header = config.see_index_header

    return See(table, limit=limit, vrepr=vrepr, index_header=index_header)


class See(object):

    def __init__(self, table, limit, vrepr, index_header):
        self.table = table
        self.limit = limit
        self.vrepr = vrepr
        self.index_header = index_header

    def __repr__(self):

        # determine if table overflows limit
        table, overflow = _vis_overflow(self.table, self.limit)

        vrepr = self.vrepr
        index_header = self.index_header

        # construct output
        output = ''
        it = iter(table)
        try:
            flds = next(it)
        except StopIteration:
            return ''
        cols = defaultdict(list)
        for row in it:
            for i, f in enumerate(flds):
                try:
                    cols[str(i)].append(vrepr(row[i]))
                except IndexError:
                    cols[str(f)].append('')
        for i, f in enumerate(flds):
            if index_header:
                f = '%s|%s' % (i, f)
            output += '%s: %s' % (f, ', '.join(cols[str(i)]))
            if overflow:
                output += '...\n'
            else:
                output += '\n'

        return output

    __str__ = __repr__
    __unicode__ = __repr__


Table.see = see


def _vis_overflow(table, limit):
    overflow = False
    if limit:
        # try reading one more than the limit, to see if there are more rows
        table = list(islice(table, 0, limit+2))
        if len(table) > limit+1:
            overflow = True
            table = table[:-1]
    return table, overflow


def _display_html(table, limit=0, vrepr=None, index_header=None, caption=None,
                  tr_style=None, td_styles=None, encoding=None,
                  truncate=None, epilogue=None):

    # determine defaults
    if limit == 0:
        limit = config.display_limit
    if vrepr is None:
        vrepr = config.display_vrepr
    if index_header is None:
        index_header = config.display_index_header
    if encoding is None:
        encoding = locale.getpreferredencoding()

    table, overflow = _vis_overflow(table, limit)
    buf = MemorySource()
    tohtml(table, buf, encoding=encoding, index_header=index_header,
           vrepr=vrepr, caption=caption, tr_style=tr_style,
           td_styles=td_styles, truncate=truncate)
    output = text_type(buf.getvalue(), encoding)

    if epilogue:
        output += '<p>%s</p>' % epilogue
    elif overflow:
        output += '<p><strong>...</strong></p>'

    return output


Table._repr_html_ = _display_html


def display(table, limit=0, vrepr=None, index_header=None, caption=None,
            tr_style=None, td_styles=None, encoding=None, truncate=None,
            epilogue=None):
    """
    Display a table inline within an IPython notebook.

    """

    from IPython.core.display import display_html
    html = _display_html(table, limit=limit, vrepr=vrepr,
                         index_header=index_header, caption=caption,
                         tr_style=tr_style, td_styles=td_styles,
                         encoding=encoding, truncate=truncate,
                         epilogue=epilogue)
    display_html(html, raw=True)


Table.display = display


def displayall(table, **kwargs):
    """
    Display **all rows** from a table inline within an IPython notebook (use
    with caution, big tables will kill your browser).

    """

    kwargs['limit'] = None
    display(table, **kwargs)


Table.displayall = displayall

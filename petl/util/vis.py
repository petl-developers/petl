from __future__ import absolute_import, print_function, division, \
    unicode_literals


from itertools import islice
from collections import defaultdict
from petl.compat import numeric_types, text_type


from petl import config
from petl.util.base import Table
from petl.io.sources import MemorySource
from petl.io.html import touhtml


def look(table, *sliceargs, **kwargs):
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
    by setting ``petl.config.look_default_style``.

    Positional arguments can be used to slice the data rows. The `sliceargs` are
    passed to :func:`itertools.islice`. By default, the first 5 data rows are
    shown.

    The properties `n` and `p` can be used to look at the next and previous rows
    respectively. E.g., try ``>>> etl.look(table)`` then ``>>> _.n`` then
    ``>>> _.p``.

    See also :func:`petl.util.vis.lookall` and :func:`petl.util.vis.see`.

    """

    return Look(table, *sliceargs, **kwargs)


Table.look = look


def lookall(table, **kwargs):
    """
    Format the entire table as text for inspection in an interactive session.

    N.B., this will load the entire table into memory.

    See also :func:`petl.util.vis.look` and :func:`petl.util.vis.see`.

    """

    return look(table, 0, None, **kwargs)


Table.lookall = lookall


class Look(object):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (5,)
        else:
            self.sliceargs = sliceargs
        self.vrepr = kwargs.get('vrepr', repr)
        self.style = kwargs.get('style', config.look_default_style)
        self.index_header = kwargs.get('index_header', False)

    @property
    def n(self):
        if not self.sliceargs:
            sliceargs = (5,)
        elif len(self.sliceargs) == 1:
            stop = self.sliceargs[0]
            sliceargs = (stop, 2*stop)
        elif len(self.sliceargs) == 2:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            sliceargs = (stop, stop + page)
        else:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            step = self.sliceargs[2]
            sliceargs = (stop, stop + page, step)
        return Look(self.table, *sliceargs)

    @property
    def p(self):
        if not self.sliceargs:
            sliceargs = (5,)
        elif len(self.sliceargs) == 1:
            # already at the start, do nothing
            sliceargs = self.sliceargs
        elif len(self.sliceargs) == 2:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            if start - page < 0:
                sliceargs = (0, page)
            else:
                sliceargs = (start - page, start)
        else:
            start = self.sliceargs[0]
            stop = self.sliceargs[1]
            page = stop - start
            step = self.sliceargs[2]
            if start - page < 0:
                sliceargs = (0, page, step)
            else:
                sliceargs = (start - page, start, step)
        return Look(self.table, *sliceargs)

    def __repr__(self):
        if self.style == 'simple':
            return format_table_simple(self.table, self.vrepr, self.sliceargs,
                                       index_header=self.index_header)
        elif self.style == 'minimal':
            return format_table_minimal(self.table, self.vrepr, self.sliceargs,
                                        index_header=self.index_header)
        else:
            return format_table_grid(self.table, self.vrepr, self.sliceargs,
                                     index_header=self.index_header)

    def __str__(self):
        return repr(self)

    def __unicode__(self):
        return repr(self)


def format_table_grid(table, vrepr, sliceargs, index_header):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [str(f) for f in flds]
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(fldsrepr)]

    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

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
    sep += '\n'

    # construct a header separator
    hedsep = '+'
    for w in colwidths:
        hedsep += '=' * (w + 2)
        hedsep += '+'
    hedsep += '\n'

    # construct a line for the header row
    fldsline = '|'
    for i, w in enumerate(colwidths):
        f = fldsrepr[i]
        fldsline += ' ' + f
        fldsline += ' ' * (w - len(f))  # padding
        fldsline += ' |'
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
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = sep + fldsline + hedsep
    for line in rowlines:
        output += line + sep

    return output


def format_table_simple(table, vrepr, sliceargs, index_header):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [str(f) for f in flds]
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(fldsrepr)]

    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

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
    hedsep += '\n'

    # construct a line for the header row
    fldsline = '  '.join(f.ljust(w) for f, w in zip(fldsrepr, colwidths))
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
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = hedsep + fldsline + hedsep
    for line in rowlines:
        output += line
    output += hedsep

    return output


def format_table_minimal(table, vrepr, sliceargs, index_header):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [str(f) for f in flds]
    if index_header:
        fldsrepr = ['%s|%s' % (i, r) for (i, r) in enumerate(fldsrepr)]

    # rows representations
    rows = list(islice(it, *sliceargs))
    rowsrepr = [[vrepr(v) for v in row] for row in rows]

    # find maximum row length - may be uneven
    rowlens = [len(flds)]
    rowlens.extend([len(row) for row in rows])
    maxrowlen = max(rowlens)

    # pad short fields and rows
    if len(flds) < maxrowlen:
        fldsrepr.extend([''] * (maxrowlen - len(flds)))
    for valsrepr in rowsrepr:
        if len(valsrepr) < maxrowlen:
            valsrepr.extend([''] * (maxrowlen - len(valsrepr)))

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
        rowline += '\n'
        rowlines.append(rowline)

    # put it all together
    output = fldsline
    for line in rowlines:
        output += line

    return output


def lookstr(table, *sliceargs):
    """Like :func:`petl.util.vis.look` but use str() rather than repr() for data
    values.

    """

    return Look(table, *sliceargs, vrepr=str)


Table.lookstr = lookstr


def lookallstr(table):
    """
    Like :func:`petl.util.vis.lookall` but use str() rather than repr() for data
    values.

    """

    return lookstr(table, 0, None)


Table.lookallstr = lookallstr


def see(table, *sliceargs, **kwargs):
    """
    Format a portion of a table as text in a column-oriented layout for
    inspection in an interactive session. E.g.::

        >>> import petl as etl
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> etl.see(table)
        foo: 'a', 'b'
        bar: 1, 2

    Useful for tables with a larger number of fields.

    Positional arguments can be used to slice the data rows. The `sliceargs` are
    passed to :func:`itertools.islice`.

    """

    return See(table, *sliceargs, **kwargs)


Table.see = see


class See(object):

    def __init__(self, table, *sliceargs, **kwargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (5,)
        else:
            self.sliceargs = sliceargs
        self.vrepr = kwargs.get('vrepr', repr)
        self.index_header = kwargs.get('index_header', False)

    def __repr__(self):
        it = iter(self.table)
        flds = next(it)
        cols = defaultdict(list)
        for row in islice(it, *self.sliceargs):
            for i, f in enumerate(flds):
                try:
                    cols[str(i)].append(self.vrepr(row[i]))
                except IndexError:
                    cols[str(f)].append('')
        output = ''
        for i, f in enumerate(flds):
            if self.index_header:
                f = '%s|%s' % (i, f)
            output += '%s: %s\n' % (f, ', '.join(cols[str(i)]))
        return output


def _vis_overflow(table, limit):
    overflow = False
    if limit:
        # try reading one more than the limit, to see if there are more rows
        table = list(islice(table, 0, limit+2))
        if len(table) > limit+1:
            overflow = True
            table = table[:-1]
    return table, overflow


def _table_repr(table):
    limit = config.table_repr_limit
    index_header = config.table_repr_index_header
    vrepr = repr
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = str(l)
    if overflow:
        t += '...\n'
    return t


Table.__repr__ = _table_repr


def _table_str(table):
    limit = config.table_str_limit
    index_header = config.table_str_index_header
    vrepr = str
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = str(l)
    if overflow:
        t += '...\n'
    return t


Table.__str__ = _table_str


def _table_unicode(table):
    limit = config.table_str_limit
    index_header = config.table_str_index_header
    vrepr = text_type
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = text_type(l)
    if overflow:
        t += '...\n'
    return t


Table.__unicode__ = _table_unicode


def _table_html(table, limit=5, vrepr=text_type, index_header=False,
                caption=None, tr_style=None, td_styles=None):
    table, overflow = _vis_overflow(table, limit)
    buf = MemorySource()
    encoding = 'utf-8'
    touhtml(table, buf, encoding=encoding, index_header=index_header,
            vrepr=vrepr, caption=caption, tr_style=tr_style,
            td_styles=td_styles)
    s = text_type(buf.getvalue(), encoding)
    if overflow:
        s += '<p><strong>...</strong></p>'
    return s


def _table_repr_html(table):
    limit = config.table_repr_html_limit
    index_header = config.table_repr_html_index_header
    vrepr = text_type
    s = _table_html(table, limit=limit, vrepr=vrepr, index_header=index_header)
    return s


Table._repr_html_ = _table_repr_html


def display(table, limit=5, vrepr=text_type,
            index_header=False, caption=None, tr_style=None, td_styles=None):
    """
    Display a table inline within an IPython notebook.
    
    """

    from IPython.core.display import display_html
    html = _table_html(table, limit=limit, vrepr=vrepr, 
                       index_header=index_header, caption=caption,
                       tr_style=tr_style, td_styles=td_styles)
    display_html(html, raw=True)


Table.display = display


def displayall(table, vrepr=text_type,
               index_header=False, caption=None, tr_style=None, td_styles=None):
    """
    Display **all rows** from a table inline within an IPython notebook.

    """

    display(table, limit=None, vrepr=vrepr, index_header=index_header,
            caption=caption, tr_style=tr_style, td_styles=td_styles)


Table.displayall = displayall

from __future__ import absolute_import, print_function, division, \
    unicode_literals


from itertools import islice
from collections import defaultdict
from petl.compat import numeric_types


from petl.util.base import Table


def look(table, *sliceargs, **kwargs):
    """
    Format a portion of the table as text for inspection in an interactive
    session. E.g.::

        >>> from petl import look
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+

    Any irregularities in the length of header and/or data rows will appear as
    blank cells, e.g.::

        >>> table = [['foo', 'bar'], ['a'], ['b', 2, True]]
        >>> look(table)
        +-------+-------+------+
        | 'foo' | 'bar' |      |
        +=======+=======+======+
        | 'a'   |       |      |
        +-------+-------+------+
        | 'b'   | 2     | True |
        +-------+-------+------+

    .. versionchanged:: 0.3

    Positional arguments can be used to slice the data rows. The `sliceargs` are
    passed to :func:`itertools.islice`.

    .. versionchanged:: 0.8

    The properties `n` and `p` can be used to look at the next and previous rows
    respectively. I.e., try ``>>> look(table)`` then ``>>> _.n`` then
    ``>>> _.p``.

    .. versionchanged:: 0.13

    Three alternative presentation styles are available: 'grid', 'simple' and
    'minimal', where 'grid' is the default. A different style can be specified
    using the `style` keyword argument, e.g.::

        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> look(table, style='simple')
        =====  =====
        'foo'  'bar'
        =====  =====
        'a'        1
        'b'        2
        =====  =====

        >>> look(table, style='minimal')
        'foo'  'bar'
        'a'        1
        'b'        2

    The default style can also be changed, e.g.::

        >>> look.default_style = 'simple'
        >>> look(table)
        =====  =====
        'foo'  'bar'
        =====  =====
        'a'        1
        'b'        2
        =====  =====

        >>> look.default_style = 'grid'
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   |     1 |
        +-------+-------+
        | 'b'   |     2 |
        +-------+-------+

    See also :func:`lookall` and :func:`see`.

    """

    return Look(table, *sliceargs, **kwargs)


Table.look = look


# default limit for table representation
table_repr_limit = 5


# set True to display field indices
table_repr_index_header = False


# set to str or repr for different behaviour
table_repr_html_value = str


def repr_look(self):
    return repr(look(self, table_repr_limit, vrepr=repr))


def str_look(self):
    return str(look(self, table_repr_limit, vrepr=str))


Table.__repr__ = repr_look
Table.__str__ = str_look


def lookall(table, **kwargs):
    """
    Format the entire table as text for inspection in an interactive session.

    N.B., this will load the entire table into memory.
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
        self.style = kwargs.get('style', look.default_style)

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
            return format_table_simple(self.table, self.vrepr, self.sliceargs)
        elif self.style == 'minimal':
            return format_table_minimal(self.table, self.vrepr, self.sliceargs)
        else:
            return format_table_grid(self.table, self.vrepr, self.sliceargs)

    def __str__(self):
        return repr(self)


look.default_style = 'grid'


def format_table_grid(table, vrepr, sliceargs):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [vrepr(f) for f in flds]

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


def format_table_simple(table, vrepr, sliceargs):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [vrepr(f) for f in flds]

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


def format_table_minimal(table, vrepr, sliceargs):
    it = iter(table)

    # fields representation
    flds = next(it)
    fldsrepr = [vrepr(f) for f in flds]

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
    """
    Like :func:`look` but use str() rather than repr() for cell
    contents.

    .. versionadded:: 0.10

    """

    return Look(table, *sliceargs, vrepr=str)


Table.lookstr = lookstr


def lookallstr(table):
    """
    Like :func:`lookall` but use str() rather than repr() for cell
    contents.

    .. versionadded:: 0.10

    """

    return lookstr(table, 0, None)


Table.lookallstr = lookallstr


def see(table, *sliceargs):
    """
    Format a portion of a table as text in a column-oriented layout for
    inspection in an interactive session. E.g.::

        >>> from petl import see
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> see(table)
        'foo': 'a', 'b'
        'bar': 1, 2

    Useful for tables with a larger number of fields.

    .. versionchanged:: 0.3

    Positional arguments can be used to slice the data rows. The `sliceargs` are
    passed to :func:`itertools.islice`.

    """

    return See(table, *sliceargs)


Table.see = see


class See(object):

    def __init__(self, table, *sliceargs):
        self.table = table
        if not sliceargs:
            self.sliceargs = (5,)
        else:
            self.sliceargs = sliceargs

    def __repr__(self):
        it = iter(self.table)
        flds = next(it)
        cols = defaultdict(list)
        for row in islice(it, *self.sliceargs):
            for i, f in enumerate(flds):
                try:
                    cols[str(i)].append(repr(row[i]))
                except IndexError:
                    cols[str(f)].append('')
        output = ''
        for i, f in enumerate(flds):
            output += '%r: %s\n' % (f, ', '.join(cols[str(i)]))
        return output

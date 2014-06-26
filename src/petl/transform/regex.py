__author__ = 'aliman'


import re
import operator


from petl.util import RowContainer, asindices
from petl.transform.basics import TransformError
from petl.transform.conversions import convert


def capture(table, field, pattern, newfields=None, include_original=False,
            flags=0, fill=None):
    """
    Add one or more new fields with values captured from an
    existing field searched via a regular expression. E.g.::

        >>> from petl import capture, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | '1'  | 'A1'       | '12'    |
        +------+------------+---------+
        | '2'  | 'A2'       | '15'    |
        +------+------------+---------+
        | '3'  | 'B1'       | '18'    |
        +------+------------+---------+
        | '4'  | 'C12'      | '19'    |
        +------+------------+---------+

        >>> table2 = capture(table1, 'variable', '(\\w)(\\d+)', ['treat', 'time'])
        >>> look(table2)
        +------+---------+---------+--------+
        | 'id' | 'value' | 'treat' | 'time' |
        +======+=========+=========+========+
        | '1'  | '12'    | 'A'     | '1'    |
        +------+---------+---------+--------+
        | '2'  | '15'    | 'A'     | '2'    |
        +------+---------+---------+--------+
        | '3'  | '18'    | 'B'     | '1'    |
        +------+---------+---------+--------+
        | '4'  | '19'    | 'C'     | '12'   |
        +------+---------+---------+--------+

        >>> # using the include_original argument
        ... table3 = capture(table1, 'variable', '(\\w)(\\d+)', ['treat', 'time'], include_original=True)
        >>> look(table3)
        +------+------------+---------+---------+--------+
        | 'id' | 'variable' | 'value' | 'treat' | 'time' |
        +======+============+=========+=========+========+
        | '1'  | 'A1'       | '12'    | 'A'     | '1'    |
        +------+------------+---------+---------+--------+
        | '2'  | 'A2'       | '15'    | 'A'     | '2'    |
        +------+------------+---------+---------+--------+
        | '3'  | 'B1'       | '18'    | 'B'     | '1'    |
        +------+------------+---------+---------+--------+
        | '4'  | 'C12'      | '19'    | 'C'     | '12'   |
        +------+------------+---------+---------+--------+

    By default the field on which the capture is performed is omitted. It can
    be included using the `include_original` argument.

    See also :func:`split`, :func:`re.search`.

    .. versionchanged:: 0.18

    The ``fill`` parameter can be used to provide a list or tuple of values to use if the regular expression does not
    match. The ``fill`` parameter should contain as many values as there are capturing groups in the regular expression.
    If ``fill`` is ``None`` (default) then a ``petl.transform.TransformError`` will be raised on the first non-matching
    value.

    """

    return CaptureView(table, field, pattern,
                       newfields=newfields,
                       include_original=include_original,
                       flags=flags,
                       fill=fill)


class CaptureView(RowContainer):

    def __init__(self, source, field, pattern, newfields=None,
                 include_original=False, flags=0, fill=None):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.newfields = newfields
        self.include_original = include_original
        self.flags = flags
        self.fill = fill

    def __iter__(self):
        return itercapture(self.source, self.field, self.pattern, self.newfields,
                           self.include_original, self.flags, self.fill)


def itercapture(source, field, pattern, newfields, include_original, flags, fill):
    it = iter(source)
    prog = re.compile(pattern, flags)

    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
    else:
        raise Exception('field invalid: must be either field name or index')

    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if newfields:
        out_flds.extend(newfields)
    yield tuple(out_flds)

    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        match = prog.search(value)
        if match is None:
            if fill is not None:
                out_row.extend(fill)
            else:
                raise TransformError('value %r did not match pattern %r' % (value, pattern))
        else:
            out_row.extend(match.groups())
        yield tuple(out_row)


def split(table, field, pattern, newfields=None, include_original=False,
          maxsplit=0, flags=0):
    """
    Add one or more new fields with values generated by
    splitting an existing value around occurrences of a regular expression.
    E.g.::

        >>> from petl import split, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | '1'  | 'parad1'   | '12'    |
        +------+------------+---------+
        | '2'  | 'parad2'   | '15'    |
        +------+------------+---------+
        | '3'  | 'tempd1'   | '18'    |
        +------+------------+---------+
        | '4'  | 'tempd2'   | '19'    |
        +------+------------+---------+

        >>> table2 = split(table1, 'variable', 'd', ['variable', 'day'])
        >>> look(table2)
        +------+---------+------------+-------+
        | 'id' | 'value' | 'variable' | 'day' |
        +======+=========+============+=======+
        | '1'  | '12'    | 'para'     | '1'   |
        +------+---------+------------+-------+
        | '2'  | '15'    | 'para'     | '2'   |
        +------+---------+------------+-------+
        | '3'  | '18'    | 'temp'     | '1'   |
        +------+---------+------------+-------+
        | '4'  | '19'    | 'temp'     | '2'   |
        +------+---------+------------+-------+

    See also :func:`re.split`.

    """

    return SplitView(table, field, pattern, newfields, include_original, maxsplit,
                     flags)


class SplitView(RowContainer):

    def __init__(self, source, field, pattern, newfields=None,
                 include_original=False, maxsplit=0, flags=0):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.newfields = newfields
        self.include_original = include_original
        self.maxsplit = maxsplit
        self.flags = flags

    def __iter__(self):
        return itersplit(self.source, self.field, self.pattern, self.newfields,
                         self.include_original, self.maxsplit, self.flags)


def itersplit(source, field, pattern, newfields, include_original, maxsplit,
              flags):

    it = iter(source)
    prog = re.compile(pattern, flags)

    flds = it.next()
    if field in flds:
        field_index = flds.index(field)
    elif isinstance(field, int) and field < len(flds):
        field_index = field
        field = flds[field_index]
    else:
        raise Exception('field invalid: must be either field name or index')

    # determine output fields
    out_flds = list(flds)
    if not include_original:
        out_flds.remove(field)
    if newfields:
        out_flds.extend(newfields)
    yield tuple(out_flds)

    # construct the output data
    for row in it:
        value = row[field_index]
        if include_original:
            out_row = list(row)
        else:
            out_row = [v for i, v in enumerate(row) if i != field_index]
        out_row.extend(prog.split(value, maxsplit))
        yield tuple(out_row)


def sub(table, field, pattern, repl, count=0, flags=0):
    """
    Convenience function to convert values under the given field using a
    regular expression substitution. See also :func:`re.sub`.

    .. versionadded:: 0.5

    .. versionchanged:: 0.10

    Renamed 'resub' to 'sub'.

    """

    prog = re.compile(pattern, flags)
    conv = lambda v: prog.sub(repl, v, count=count)
    return convert(table, field, conv)


resub = sub  # backwards compatibility


def search(table, *args, **kwargs):
    """
    Perform a regular expression search, returning rows that match a given
    pattern, either anywhere in the row or within a specific field. E.g.::

        >>> from petl import search, look
        >>> look(table1)
        +------------+-------+--------------------------+
        | 'foo'      | 'bar' | 'baz'                    |
        +============+=======+==========================+
        | 'orange'   | 12    | 'oranges are nice fruit' |
        +------------+-------+--------------------------+
        | 'mango'    | 42    | 'I like them'            |
        +------------+-------+--------------------------+
        | 'banana'   | 74    | 'lovely too'             |
        +------------+-------+--------------------------+
        | 'cucumber' | 41    | 'better than mango'      |
        +------------+-------+--------------------------+

        >>> # search any field
        ... table2 = search(table1, '.g.')
        >>> look(table2)
        +------------+-------+--------------------------+
        | 'foo'      | 'bar' | 'baz'                    |
        +============+=======+==========================+
        | 'orange'   | 12    | 'oranges are nice fruit' |
        +------------+-------+--------------------------+
        | 'mango'    | 42    | 'I like them'            |
        +------------+-------+--------------------------+
        | 'cucumber' | 41    | 'better than mango'      |
        +------------+-------+--------------------------+

        >>> # search a specific field
        ... table3 = search(table1, 'foo', '.g.')
        >>> look(table3)
        +----------+-------+--------------------------+
        | 'foo'    | 'bar' | 'baz'                    |
        +==========+=======+==========================+
        | 'orange' | 12    | 'oranges are nice fruit' |
        +----------+-------+--------------------------+
        | 'mango'  | 42    | 'I like them'            |
        +----------+-------+--------------------------+


    .. versionadded:: 0.10

    """

    if len(args) == 1:
        field = None
        pattern = args[0]
    elif len(args) == 2:
        field = args[0]
        pattern = args[1]
    else:
        raise Exception('expected 1 or 2 arguments')
    return SearchView(table, pattern, field=field, **kwargs)


class SearchView(RowContainer):

    def __init__(self, table, pattern, field=None, flags=0):
        self.table = table
        self.pattern = pattern
        self.field = field
        self.flags = flags

    def __iter__(self):
        return itersearch(self.table, self.pattern, self.field, self.flags)


def itersearch(table, pattern, field, flags):
    prog = re.compile(pattern, flags)
    it = iter(table)
    fields = [str(f) for f in it.next()]
    yield tuple(fields)

    if field is None:
        # search whole row
        test = lambda row: any(prog.search(str(v)) for v in row)
    elif isinstance(field, basestring):
        # search single field
        index = fields.index(field)
        test = lambda row: prog.search(str(row[index]))
    else: # list or tuple or ...
        # search selection of fields
        indices = asindices(fields, field)
        getvals = operator.itemgetter(*indices)
        test = lambda row: any(prog.search(str(v)) for v in getvals(row))

    for row in it:
        if test(row):
            yield tuple(row)


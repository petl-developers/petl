from __future__ import absolute_import, print_function, division


from petl.compat import next, integer_types, string_types, text_type


import petl.config as config
from petl.errors import ArgumentError, FieldSelectionError
from petl.util.base import Table, expr, fieldnames, Record
from petl.util.parsers import numparser


def convert(table, *args, **kwargs):
    """Transform values under one or more fields via arbitrary functions, method
    invocations or dictionary translations. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', '2.4', 12],
        ...           ['B', '5.7', 34],
        ...           ['C', '1.2', 56]]
        >>> # using a built-in function:
        ... table2 = etl.convert(table1, 'bar', float)
        >>> table2
        +-----+-----+-----+
        | foo | bar | baz |
        +=====+=====+=====+
        | 'A' | 2.4 |  12 |
        +-----+-----+-----+
        | 'B' | 5.7 |  34 |
        +-----+-----+-----+
        | 'C' | 1.2 |  56 |
        +-----+-----+-----+

        >>> # using a lambda function::
        ... table3 = etl.convert(table1, 'baz', lambda v: v*2)
        >>> table3
        +-----+-------+-----+
        | foo | bar   | baz |
        +=====+=======+=====+
        | 'A' | '2.4' |  24 |
        +-----+-------+-----+
        | 'B' | '5.7' |  68 |
        +-----+-------+-----+
        | 'C' | '1.2' | 112 |
        +-----+-------+-----+

        >>> # a method of the data value can also be invoked by passing
        ... # the method name
        ... table4 = etl.convert(table1, 'foo', 'lower')
        >>> table4
        +-----+-------+-----+
        | foo | bar   | baz |
        +=====+=======+=====+
        | 'a' | '2.4' |  12 |
        +-----+-------+-----+
        | 'b' | '5.7' |  34 |
        +-----+-------+-----+
        | 'c' | '1.2' |  56 |
        +-----+-------+-----+

        >>> # arguments to the method invocation can also be given
        ... table5 = etl.convert(table1, 'foo', 'replace', 'A', 'AA')
        >>> table5
        +------+-------+-----+
        | foo  | bar   | baz |
        +======+=======+=====+
        | 'AA' | '2.4' |  12 |
        +------+-------+-----+
        | 'B'  | '5.7' |  34 |
        +------+-------+-----+
        | 'C'  | '1.2' |  56 |
        +------+-------+-----+

        >>> # values can also be translated via a dictionary
        ... table7 = etl.convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
        >>> table7
        +-----+-------+-----+
        | foo | bar   | baz |
        +=====+=======+=====+
        | 'Z' | '2.4' |  12 |
        +-----+-------+-----+
        | 'Y' | '5.7' |  34 |
        +-----+-------+-----+
        | 'C' | '1.2' |  56 |
        +-----+-------+-----+

        >>> # the same conversion can be applied to multiple fields
        ... table8 = etl.convert(table1, ('foo', 'bar', 'baz'), str)
        >>> table8
        +-----+-------+------+
        | foo | bar   | baz  |
        +=====+=======+======+
        | 'A' | '2.4' | '12' |
        +-----+-------+------+
        | 'B' | '5.7' | '34' |
        +-----+-------+------+
        | 'C' | '1.2' | '56' |
        +-----+-------+------+

        >>> # multiple conversions can be specified at the same time
        ... table9 = etl.convert(table1, {'foo': 'lower',
        ...                               'bar': float,
        ...                               'baz': lambda v: v * 2})
        >>> table9
        +-----+-----+-----+
        | foo | bar | baz |
        +=====+=====+=====+
        | 'a' | 2.4 |  24 |
        +-----+-----+-----+
        | 'b' | 5.7 |  68 |
        +-----+-----+-----+
        | 'c' | 1.2 | 112 |
        +-----+-----+-----+

        >>> # ...or alternatively via a list
        ... table10 = etl.convert(table1, ['lower', float, lambda v: v*2])
        >>> table10
        +-----+-----+-----+
        | foo | bar | baz |
        +=====+=====+=====+
        | 'a' | 2.4 |  24 |
        +-----+-----+-----+
        | 'b' | 5.7 |  68 |
        +-----+-----+-----+
        | 'c' | 1.2 | 112 |
        +-----+-----+-----+

        >>> # conversion can be conditional
        ... table11 = etl.convert(table1, 'baz', lambda v: v * 2,
        ...                       where=lambda r: r.foo == 'B')
        >>> table11
        +-----+-------+-----+
        | foo | bar   | baz |
        +=====+=======+=====+
        | 'A' | '2.4' |  12 |
        +-----+-------+-----+
        | 'B' | '5.7' |  68 |
        +-----+-------+-----+
        | 'C' | '1.2' |  56 |
        +-----+-------+-----+

        >>> # conversion can access other values from the same row
        ... table12 = etl.convert(table1, 'baz',
        ...                       lambda v, row: v * float(row.bar),
        ...                       pass_row=True)
        >>> table12
        +-----+-------+--------------------+
        | foo | bar   | baz                |
        +=====+=======+====================+
        | 'A' | '2.4' | 28.799999999999997 |
        +-----+-------+--------------------+
        | 'B' | '5.7' |              193.8 |
        +-----+-------+--------------------+
        | 'C' | '1.2' |               67.2 |
        +-----+-------+--------------------+
        >>> # conversion can use a custom function
        >>> def my_func(val, row):
        ...     return float(row.bar) + row.baz
        ... 
        >>> table13 = etl.convert(table1, 'foo', my_func, pass_row=True)
        >>> table13
        +------+-------+-----+
        | foo  | bar   | baz |
        +======+=======+=====+
        | 14.4 | '2.4' |  12 |
        +------+-------+-----+
        | 39.7 | '5.7' |  34 |
        +------+-------+-----+
        | 57.2 | '1.2' |  56 |
        +------+-------+-----+

    Note that either field names or indexes can be given.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    The ``pass_row`` keyword argument can be given, which if True will mean
    that both the value and the containing row will be passed as
    arguments to the conversion function (so, i.e., the conversion function
    should accept two arguments).

    When multiple fields are converted in a single call, the conversions
    are independent of each other. Each conversion sees the original row::

        >>> # multiple conversions do not affect each other
        ... table13 = etl.convert(table1, {
        ...                           "foo": lambda foo, row: row.bar,
        ...                           "bar": lambda bar, row: row.foo,
        ...                       }, pass_row=True)
        >>> table13
        +-------+-----+-----+
        | foo   | bar | baz |
        +=======+=====+=====+
        | '2.4' | 'A' |  12 |
        +-------+-----+-----+
        | '5.7' | 'B' |  34 |
        +-------+-----+-----+
        | '1.2' | 'C' |  56 |
        +-------+-----+-----+

    Also accepts `failonerror` and `errorvalue` keyword arguments,
    documented under :func:`petl.config.failonerror`

    """

    converters = None
    if len(args) == 0:
        # no conversion specified, can be set afterwards via suffix notation
        pass
    elif len(args) == 1:
        converters = args[0]
    elif len(args) > 1:
        converters = dict()
        # assume first arg is field name or spec
        field = args[0]
        if len(args) == 2:
            conv = args[1]
        else:
            conv = args[1:]
        if isinstance(field, (list, tuple)):  # allow for multiple fields
            for f in field:
                converters[f] = conv
        else:
            converters[field] = conv
    return FieldConvertView(table, converters, **kwargs)


Table.convert = convert


def convertall(table, *args, **kwargs):
    """
    Convenience function to convert all fields in the table using a common
    function or mapping. See also :func:`convert`.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    # TODO don't read the data twice!
    return convert(table, fieldnames(table), *args, **kwargs)


Table.convertall = convertall


def replace(table, field, a, b, **kwargs):
    """
    Convenience function to replace all occurrences of `a` with `b` under the
    given field. See also :func:`convert`.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    return convert(table, field, {a: b}, **kwargs)


Table.replace = replace


def replaceall(table, a, b, **kwargs):
    """
    Convenience function to replace all instances of `a` with `b` under all
    fields. See also :func:`convertall`.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    return convertall(table, {a: b}, **kwargs)


Table.replaceall = replaceall


def update(table, field, value, **kwargs):
    """
    Convenience function to convert a field to a fixed value. Accepts the
    ``where`` keyword argument. See also :func:`convert`.

    """

    return convert(table, field, lambda v: value, **kwargs)


Table.update = update


def convertnumbers(table, strict=False, **kwargs):
    """
    Convenience function to convert all field values to numbers where
    possible. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz', 'quux'],
        ...           ['1', '3.0', '9+3j', 'aaa'],
        ...           ['2', '1.3', '7+2j', None]]
        >>> table2 = etl.convertnumbers(table1)
        >>> table2
        +-----+-----+--------+-------+
        | foo | bar | baz    | quux  |
        +=====+=====+========+=======+
        |   1 | 3.0 | (9+3j) | 'aaa' |
        +-----+-----+--------+-------+
        |   2 | 1.3 | (7+2j) | None  |
        +-----+-----+--------+-------+

    """

    return convertall(table, numparser(strict), **kwargs)


Table.convertnumbers = convertnumbers


class FieldConvertView(Table):

    def __init__(self, source, converters=None, failonerror=None,
                 errorvalue=None, where=None, pass_row=False):
        self.source = source
        if converters is None:
            self.converters = dict()
        elif isinstance(converters, dict):
            self.converters = converters
        elif isinstance(converters, (tuple, list)):
            self.converters = dict([(i, v) for i, v in enumerate(converters)])
        else:
            raise ArgumentError('unexpected converters: %r' % converters)
        self.failonerror = (config.failonerror if failonerror is None
                                else failonerror)
        self.errorvalue = errorvalue
        self.where = where
        self.pass_row = pass_row

    def __iter__(self):
        return iterfieldconvert(self.source, self.converters, self.failonerror,
                                self.errorvalue, self.where, self.pass_row)

    def __setitem__(self, key, value):
        self.converters[key] = value


def iterfieldconvert(source, converters, failonerror, errorvalue, where,
                     pass_row):

    # grab the fields in the source table
    it = iter(source)
    try:
        hdr = next(it)
        flds = list(map(text_type, hdr))
        yield tuple(hdr)  # these are not modified
    except StopIteration:
        hdr = flds = []  # converters will fail selecting a field

    # build converter functions
    converter_functions = dict()
    for k, c in converters.items():

        # turn field names into row indices
        if not isinstance(k, integer_types):
            try:
                k = flds.index(k)
            except ValueError:  # not in list
                raise FieldSelectionError(k)
        assert isinstance(k, int), 'expected integer, found %r' % k

        # is converter a function?
        if callable(c):
            converter_functions[k] = c

        # is converter a method name?
        elif isinstance(c, string_types):
            converter_functions[k] = methodcaller(c)

        # is converter a method name with arguments?
        elif isinstance(c, (tuple, list)) and isinstance(c[0], string_types):
            methnm = c[0]
            methargs = c[1:]
            converter_functions[k] = methodcaller(methnm, *methargs)

        # is converter a dictionary?
        elif isinstance(c, dict):
            converter_functions[k] = dictconverter(c)

        # is it something else?
        elif c is None:
            pass  # ignore
        else:
            raise ArgumentError(
                'unexpected converter specification on field %r: %r' % (k, c)
            )

    # define a function to transform a value
    def transform_value(i, v, *args):
        if i not in converter_functions:
            # no converter defined on this field, return value as-is
            return v
        else:
            try:
                return converter_functions[i](v, *args)
            except Exception as e:
                if failonerror == 'inline':
                    return e
                elif failonerror:
                    raise e
                else:
                    return errorvalue

    # define a function to transform a row
    if pass_row:
        def transform_row(_row):
            return tuple(transform_value(i, v, _row)
                         for i, v in enumerate(_row))
    else:
        def transform_row(_row):
            return tuple(transform_value(i, v)
                         for i, v in enumerate(_row))

    # prepare where function
    if isinstance(where, string_types):
        where = expr(where)
    elif where is not None:
        assert callable(where), 'expected callable for "where" argument, ' \
                                'found %r' % where

    # prepare iterator
    if pass_row or where:
        # wrap rows as records
        it = (Record(row, flds) for row in it)

    # construct the data rows
    if where is None:
        # simple case, transform all rows
        for row in it:
            yield transform_row(row)
    else:
        # conditionally transform rows
        for row in it:
            if where(row):
                yield transform_row(row)
            else:
                yield row


def methodcaller(nm, *args):
    return lambda v: getattr(v, nm)(*args)


def dictconverter(d):
    def conv(v):
        try:
            if v in d:
                return d[v]
            else:
                return v
        except TypeError:
            # value is not hashable
            return v
    return conv


def format(table, field, fmt, **kwargs):
    """
    Convenience function to format all values in the given `field` using the
    `fmt` format string.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    conv = lambda v: fmt.format(v)
    return convert(table, field, conv, **kwargs)


Table.format = format


def formatall(table, fmt, **kwargs):
    """
    Convenience function to format all values in all fields using the
    `fmt` format string.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    conv = lambda v: fmt.format(v)
    return convertall(table, conv, **kwargs)


Table.formatall = formatall


def interpolate(table, field, fmt, **kwargs):
    """
    Convenience function to interpolate all values in the given `field` using
    the `fmt` string.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    conv = lambda v: fmt % v
    return convert(table, field, conv, **kwargs)


Table.interpolate = interpolate


def interpolateall(table, fmt, **kwargs):
    """
    Convenience function to interpolate all values in all fields using
    the `fmt` string.

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    """

    conv = lambda v: fmt % v
    return convertall(table, conv, **kwargs)


Table.interpolateall = interpolateall

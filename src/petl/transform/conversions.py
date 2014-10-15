from __future__ import absolute_import, print_function, division


from petl.util import numparser, RowContainer, FieldSelectionError, hybridrows,\
    expr, header


def convert(table, *args, **kwargs):
    """
    Transform values under one or more fields via arbitrary functions, method
    invocations or dictionary translations. E.g.::

        >>> from petl import convert, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | '2.4' |    12 |
        +-------+-------+-------+
        | 'B'   | '5.7' |    34 |
        +-------+-------+-------+
        | 'C'   | '1.2' |    56 |
        +-------+-------+-------+

        >>> # using the built-in float function:
        ... table2 = convert(table1, 'bar', float)
        >>> look(table2)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   |   2.4 |    12 |
        +-------+-------+-------+
        | 'B'   |   5.7 |    34 |
        +-------+-------+-------+
        | 'C'   |   1.2 |    56 |
        +-------+-------+-------+

        >>> # using a lambda function::
        ... table3 = convert(table1, 'baz', lambda v: v*2)
        >>> look(table3)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | '2.4' |    24 |
        +-------+-------+-------+
        | 'B'   | '5.7' |    68 |
        +-------+-------+-------+
        | 'C'   | '1.2' |   112 |
        +-------+-------+-------+

        >>> # a method of the data value can also be invoked by passing the method name
        ... table4 = convert(table1, 'foo', 'lower')
        >>> look(table4)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   | '2.4' |    12 |
        +-------+-------+-------+
        | 'b'   | '5.7' |    34 |
        +-------+-------+-------+
        | 'c'   | '1.2' |    56 |
        +-------+-------+-------+

        >>> # arguments to the method invocation can also be given
        ... table5 = convert(table1, 'foo', 'replace', 'A', 'AA')
        >>> look(table5)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'AA'  | '2.4' |    12 |
        +-------+-------+-------+
        | 'B'   | '5.7' |    34 |
        +-------+-------+-------+
        | 'C'   | '1.2' |    56 |
        +-------+-------+-------+

        >>> # values can also be translated via a dictionary
        ... table7 = convert(table1, 'foo', {'A': 'Z', 'B': 'Y'})
        >>> look(table7)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'Z'   | '2.4' |    12 |
        +-------+-------+-------+
        | 'Y'   | '5.7' |    34 |
        +-------+-------+-------+
        | 'C'   | '1.2' |    56 |
        +-------+-------+-------+

        >>> # the same conversion can be applied to multiple fields
        ... table8 = convert(table1, ('foo', 'bar', 'baz'), unicode)
        >>> look(table8)
        +-------+--------+-------+
        | 'foo' | 'bar'  | 'baz' |
        +=======+========+=======+
        | u'A'  | u'2.4' | u'12' |
        +-------+--------+-------+
        | u'B'  | u'5.7' | u'34' |
        +-------+--------+-------+
        | u'C'  | u'1.2' | u'56' |
        +-------+--------+-------+

        >>> # multiple conversions can be specified at the same time
        ... table9 = convert(table1, {'foo': 'lower', 'bar': float, 'baz': lambda v: v*2})
        >>> look(table9)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   |   2.4 |    24 |
        +-------+-------+-------+
        | 'b'   |   5.7 |    68 |
        +-------+-------+-------+
        | 'c'   |   1.2 |   112 |
        +-------+-------+-------+

        >>> # ...or alternatively via a list
        ... table10 = convert(table1, ['lower', float, lambda v: v*2])
        >>> look(table10)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   |   2.4 |    24 |
        +-------+-------+-------+
        | 'b'   |   5.7 |    68 |
        +-------+-------+-------+
        | 'c'   |   1.2 |   112 |
        +-------+-------+-------+

        >>> # ...or alternatively via suffix notation on the returned table object
        ... table11 = convert(table1)
        >>> table11['foo'] = 'lower'
        >>> table11['bar'] = float
        >>> table11['baz'] = lambda v: v*2
        >>> look(table11)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'a'   |   2.4 |    24 |
        +-------+-------+-------+
        | 'b'   |   5.7 |    68 |
        +-------+-------+-------+
        | 'c'   |   1.2 |   112 |
        +-------+-------+-------+

        >>> # conversion can be conditional
        ... table12 = convert(table1, 'baz', lambda v: v*2, where=lambda r: r.foo == 'B')
        >>> look(table12)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | '2.4' |    12 |
        +-------+-------+-------+
        | 'B'   | '5.7' |    68 |
        +-------+-------+-------+
        | 'C'   | '1.2' |    56 |
        +-------+-------+-------+

        >>> # conversion can access other values from the same row
        ... table14 = convert(table1, 'baz', lambda v, row: v * float(row.bar), pass_row=True)
        >>> look(table14)
        +-------+-------+--------------------+
        | 'foo' | 'bar' | 'baz'              |
        +=======+=======+====================+
        | 'A'   | '2.4' | 28.799999999999997 |
        +-------+-------+--------------------+
        | 'B'   | '5.7' |              193.8 |
        +-------+-------+--------------------+
        | 'C'   | '1.2' |               67.2 |
        +-------+-------+--------------------+

    Note that either field names or indexes can be given.

    .. versionchanged:: 0.11

    Now supports multiple field conversions.

    .. versionchanged:: 0.22

    The ``where`` keyword argument can be given with a callable or expression
    which is evaluated on each row and which should return True if the
    conversion should be applied on that row, else False.

    .. versionchanged:: 0.25

    The ``pass_row`` keyword argument can be given, which if True will mean
    that both the value and the containing row will be passed as
    arguments to the conversion function (so, i.e., the conversion function
    should accept two arguments).

    """

    if len(args) == 0:
        # no conversion specified, can be set afterwards via suffix notation
        converters = None
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


def convertall(table, *args, **kwargs):
    """
    Convenience function to convert all fields in the table using a common
    function or mapping. See also :func:`convert`.

    .. versionadded:: 0.4

    .. versionchanged:: 0.22

    The ``where`` keyword argument can be given with a callable or expression which is evaluated on each row
    and which should return True if the conversion should be applied on that row, else False.

    """

    # TODO don't read the data twice!

    return convert(table, header(table), *args, **kwargs)


def replace(table, field, a, b, **kwargs):
    """
    Convenience function to replace all occurrences of `a` with `b` under the
    given field. See also :func:`convert`.

    .. versionadded:: 0.5

    .. versionchanged:: 0.22

    The ``where`` keyword argument can be given with a callable or expression which is evaluated on each row
    and which should return True if the conversion should be applied on that row, else False.

    """

    return convert(table, field, {a: b}, **kwargs)


def replaceall(table, a, b, **kwargs):
    """
    Convenience function to replace all instances of `a` with `b` under all
    fields. See also :func:`convertall`.

    .. versionadded:: 0.5

    .. versionchanged:: 0.22

    The ``where`` keyword argument can be given with a callable or expression which is evaluated on each row
    and which should return True if the conversion should be applied on that row, else False.

    """

    return convertall(table, {a: b}, **kwargs)


def update(table, field, value, **kwargs):
    """
    Convenience function to convert a field to a fixed value. Accepts the ``where`` keyword argument. See also
    :func:`convert`.

    .. versionadded:: 0.23

    """

    return convert(table, field, lambda v: value, **kwargs)


def convertnumbers(table, strict=False, **kwargs):
    """
    Convenience function to convert all field values to numbers where possible.
    E.g.::

        >>> from petl import convertnumbers, look
        >>> look(table1)
        +-------+-------+--------+--------+
        | 'foo' | 'bar' | 'baz'  | 'quux' |
        +=======+=======+========+========+
        | '1'   | '3.0' | '9+3j' | 'aaa'  |
        +-------+-------+--------+--------+
        | '2'   | '1.3' | '7+2j' | None   |
        +-------+-------+--------+--------+

        >>> table2 = convertnumbers(table1)
        >>> look(table2)
        +-------+-------+--------+--------+
        | 'foo' | 'bar' | 'baz'  | 'quux' |
        +=======+=======+========+========+
        | 1     | 3.0   | (9+3j) | 'aaa'  |
        +-------+-------+--------+--------+
        | 2     | 1.3   | (7+2j) | None   |
        +-------+-------+--------+--------+

    .. versionadded:: 0.4

    """

    return convertall(table, numparser(strict), **kwargs)


def fieldconvert(table, converters=None, failonerror=False, errorvalue=None, **kwargs):
    """
    Transform values in one or more fields via functions or method invocations.

    .. deprecated:: 0.11

    Use :func:`convert` instead.

    """

    return FieldConvertView(table, converters, failonerror, errorvalue, **kwargs)


class FieldConvertView(RowContainer):

    def __init__(self, source, converters=None, failonerror=False,
                 errorvalue=None, where=None, pass_row=False):
        self.source = source
        if converters is None:
            self.converters = dict()
        elif isinstance(converters, dict):
            self.converters = converters
        elif isinstance(converters, (tuple, list)):
            self.converters = dict([(i, v) for i, v in enumerate(converters)])
        else:
            raise Exception('unexpected converters: %r' % converters)
        self.failonerror = failonerror
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
    flds = it.next()
    yield tuple(flds)  # these are not modified

    # build converter functions
    converter_functions = dict()
    for k, c in converters.items():

        # turn field names into row indices
        if isinstance(k, basestring):
            try:
                k = flds.index(k)
            except ValueError: # not in list
                raise FieldSelectionError(k)
        assert isinstance(k, int), 'expected integer, found %r' % k

        # is converter a function?
        if callable(c):
            converter_functions[k] = c

        # is converter a method name?
        elif isinstance(c, basestring):
            converter_functions[k] = methodcaller(c)

        # is converter a method name with arguments?
        elif isinstance(c, (tuple, list)) and isinstance(c[0], basestring):
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
            raise Exception('unexpected converter specification on field %r: %r' % (k, c))

    # define a function to transform a value
    def transform_value(i, v, *args):
        if i not in converter_functions:
            # no converter defined on this field, return value as-is
            return v
        else:
            try:
                return converter_functions[i](v, *args)
            except:
                if failonerror:
                    raise
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
    if isinstance(where, basestring):
        where = expr(where)
    elif where is not None:
        assert callable(where), 'expected callable for "where" argument, ' \
                                'found %r' % where

    # prepare iterator
    if pass_row or where:
        # use hybrid rows as more user-friendly, but N.B. has performance cost
        it = hybridrows(flds, it)

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

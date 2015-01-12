from __future__ import absolute_import, print_function, division


import datetime
from petl.compat import long


def datetimeparser(fmt, strict=True):
    """Return a function to parse strings as :class:`datetime.datetime` objects
    using a given format. E.g.::

        >>> from petl import datetimeparser
        >>> isodatetime = datetimeparser('%Y-%m-%dT%H:%M:%S')
        >>> isodatetime('2002-12-25T00:00:00')
        datetime.datetime(2002, 12, 25, 0, 0)
        >>> try:
        ...     isodatetime('2002-12-25T00:00:99')
        ... except ValueError as e:
        ...     print(e)
        ...
        unconverted data remains: 9

    If ``strict=False`` then if an error occurs when parsing, the original
    value will be returned as-is, and no error will be raised.

    """

    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt)
        except Exception as e:
            if strict:
                raise e
            else:
                return value
    return parser


def dateparser(fmt, strict=True):
    """Return a function to parse strings as :class:`datetime.date` objects
    using a given format. E.g.::

        >>> from petl import dateparser
        >>> isodate = dateparser('%Y-%m-%d')
        >>> isodate('2002-12-25')
        datetime.date(2002, 12, 25)
        >>> try:
        ...     isodate('2002-02-30')
        ... except ValueError as e:
        ...     print(e)
        ...
        day is out of range for month

    If ``strict=False`` then if an error occurs when parsing, the original
    value will be returned as-is, and no error will be raised.

    """

    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt).date()
        except Exception as e:
            if strict:
                raise e
            else:
                return value
    return parser


def timeparser(fmt, strict=True):
    """Return a function to parse strings as :class:`datetime.time` objects
    using a given format. E.g.::

        >>> from petl import timeparser
        >>> isotime = timeparser('%H:%M:%S')
        >>> isotime('00:00:00')
        datetime.time(0, 0)
        >>> isotime('13:00:00')
        datetime.time(13, 0)
        >>> try:
        ...     isotime('12:00:99')
        ... except ValueError as e:
        ...     print(e)
        ...
        unconverted data remains: 9
        >>> try:
        ...     isotime('25:00:00')
        ... except ValueError as e:
        ...     print(e)
        ...
        time data '25:00:00' does not match format '%H:%M:%S'

    If ``strict=False`` then if an error occurs when parsing, the original
    value will be returned as-is, and no error will be raised.

    """

    def parser(value):
        try:
            return datetime.datetime.strptime(value.strip(), fmt).time()
        except Exception as e:
            if strict:
                raise e
            else:
                return value
    return parser


def boolparser(true_strings=('true', 't', 'yes', 'y', '1'),
               false_strings=('false', 'f', 'no', 'n', '0'),
               case_sensitive=False,
               strict=True):
    """Return a function to parse strings as :class:`bool` objects using a
    given set of string representations for `True` and `False`. E.g.::

        >>> from petl import boolparser
        >>> mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
        >>> mybool('y')
        True
        >>> mybool('yes')
        True
        >>> mybool('Y')
        True
        >>> mybool('No')
        False
        >>> try:
        ...     mybool('foo')
        ... except ValueError as e:
        ...     print(e)
        ...
        value is not one of recognised boolean strings: 'foo'
        >>> try:
        ...     mybool('True')
        ... except ValueError as e:
        ...     print(e)
        ...
        value is not one of recognised boolean strings: 'true'

    If ``strict=False`` then if an error occurs when parsing, the original
    value will be returned as-is, and no error will be raised.

    """

    if not case_sensitive:
        true_strings = [s.lower() for s in true_strings]
        false_strings = [s.lower() for s in false_strings]

    def parser(value):
        value = value.strip()
        if not case_sensitive:
            value = value.lower()
        if value in true_strings:
            return True
        elif value in false_strings:
            return False
        elif strict:
            raise ValueError('value is not one of recognised boolean strings: '
                             '%r' % value)
        else:
            return value

    return parser


def numparser(strict=False):
    """Return a function that will attempt to parse the value as a number,
    trying :func:`int`, :func:`long`, :func:`float` and :func:`complex` in
    that order. If all fail, return the value as-is, unless ``strict=True``,
    in which case raise the underlying exception.

    """

    def f(v):
        try:
            return int(v)
        except (ValueError, TypeError):
            pass
        try:
            return long(v)
        except (ValueError, TypeError):
            pass
        try:
            return float(v)
        except (ValueError, TypeError):
            pass
        try:
            return complex(v)
        except (ValueError, TypeError) as e:
            if strict:
                raise e
        return v

    return f

from __future__ import absolute_import, print_function, division, \
    unicode_literals


import datetime
from petl.compat import long


def datetimeparser(fmt, strict=True):
    """Return a function to parse strings as :class:`datetime.datetime` objects
    using a given format. E.g.::

        >>> from petl import datetimeparser
        >>> isodatetime = datetimeparser('%Y-%m-%dT%H:%M:%S')
        >>> isodatetime('2002-12-25T00:00:00')
        datetime.datetime(2002, 12, 25, 0, 0)
        >>> isodatetime('2002-12-25T00:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1018, in parser
            return datetime.strptime(value.strip(), format)
          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9

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
        >>> isodate('2002-02-30')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1032, in parser
            return parser
          File "/usr/lib/python2.7/_strptime.py", line 440, in _strptime
            datetime_date(year, 1, 1).toordinal() + 1
        ValueError: day is out of range for month

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
        >>> isotime('12:00:99')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser

          File "/usr/lib/python2.7/_strptime.py", line 328, in _strptime
            data_string[found.end():])
        ValueError: unconverted data remains: 9
        >>> isotime('25:00:00')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1046, in parser

          File "/usr/lib/python2.7/_strptime.py", line 325, in _strptime
            (data_string, format))
        ValueError: time data '25:00:00' does not match format '%H:%M:%S'

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
        >>> mybool('Y')
        True
        >>> mybool('yes')
        True
        >>> mybool('No')
        False
        >>> mybool('nO')
        False
        >>> mybool('true')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'true'
        >>> mybool('foo')
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "petl/util.py", line 1175, in parser
            raise ValueError('value is not one of recognised boolean strings: %r' % value)
        ValueError: value is not one of recognised boolean strings: 'foo'

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
    that order. If all fail, return the value as-is, unless `strict`=`True`,
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

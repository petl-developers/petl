from __future__ import print_function, division, absolute_import


from datetime import datetime
from decimal import Decimal

import pytest

from petl.test.helpers import eq_, ieq
from petl.comparison import Comparable


def test_comparable():

    # bools
    d = [True, False]
    a = sorted(d, key=Comparable)
    e = [False, True]
    eq_(e, a)

    # ints
    d = [3, 1, 2]
    a = sorted(d, key=Comparable)
    e = [1, 2, 3]
    eq_(e, a)

    # floats
    d = [3., 1.2, 2.5]
    a = sorted(d, key=Comparable)
    e = [1.2, 2.5, 3.]
    eq_(e, a)

    # mixed numeric
    d = [3., 1, 2.5, Decimal('1.5')]
    a = sorted(d, key=Comparable)
    e = [1, Decimal('1.5'), 2.5, 3.]
    eq_(e, a)

    # mixed numeric and bool
    d = [True, False, -1.2, 2, .5]
    a = sorted(d, key=Comparable)
    e = [-1.2, False, .5, True, 2]
    eq_(e, a)

    # mixed numeric and None
    d = [3, None, 2.5]
    a = sorted(d, key=Comparable)
    e = [None, 2.5, 3.]
    eq_(e, a)

    # bytes
    d = [b'b', b'ccc', b'aa']
    a = sorted(d, key=Comparable)
    e = [b'aa', b'b', b'ccc']
    eq_(e, a)

    # text
    d = [u'b', u'ccc', u'aa']
    a = sorted(d, key=Comparable)
    e = [u'aa', u'b', u'ccc']
    eq_(e, a)

    # mixed bytes and text
    d = [u'b', b'ccc', b'aa']
    a = sorted(d, key=Comparable)
    # N.B., expect always bytes < unicode
    e = [b'aa', b'ccc', u'b']
    eq_(e, a)

    # mixed bytes and None
    d = [b'b', b'ccc', None, b'aa']
    a = sorted(d, key=Comparable)
    e = [None, b'aa', b'b', b'ccc']
    eq_(e, a)

    # mixed text and None
    d = [u'b', u'ccc', None, u'aa']
    a = sorted(d, key=Comparable)
    e = [None, u'aa', u'b', u'ccc']
    eq_(e, a)

    # mixed bytes, text and None
    d = [u'b', b'ccc', None, b'aa']
    a = sorted(d, key=Comparable)
    # N.B., expect always bytes < unicode
    e = [None, b'aa', b'ccc', u'b']
    eq_(e, a)

    # mixed bytes, text, numbers and None
    d = [u'b', True, b'ccc', False, None, b'aa', -1, 3.4]
    a = sorted(d, key=Comparable)
    e = [None, -1, False, True, 3.4, b'aa', b'ccc', u'b']
    eq_(e, a)


def test_comparable_datetime():

    dt = datetime.now().replace

    # datetimes
    d = [dt(hour=12), dt(hour=3), dt(hour=1)]
    a = sorted(d, key=Comparable)
    e = [dt(hour=1), dt(hour=3), dt(hour=12)]
    eq_(e, a)

    # mixed datetime and None
    d = [dt(hour=12), None, dt(hour=3), dt(hour=1)]
    a = sorted(d, key=Comparable)
    e = [None, dt(hour=1), dt(hour=3), dt(hour=12)]
    eq_(e, a)

    # mixed datetime, numbers, bytes, text and None
    d = [dt(hour=12), None, dt(hour=3), u'b', True, b'ccc', False, b'aa', -1,
         3.4]
    a = sorted(d, key=Comparable)
    # N.B., because bytes and unicode type names have changed in PY3,
    # petl uses PY2 type names to try and achieve consistent behaviour across
    # versions, i.e., 'datetime' < 'str' < 'unicode' rather than 'bytes' <
    # 'datetime' < 'str'
    e = [None, -1, False, True, 3.4, dt(hour=3), dt(hour=12), b'aa', b'ccc',
         u'b']
    eq_(e, a)


def test_comparable_nested():

    # lists
    d = [[3], [1], [2]]
    a = sorted(d, key=Comparable)
    e = [[1], [2], [3]]
    eq_(e, a)

    # tuples
    d = [(3,), (1,), (2,)]
    a = sorted(d, key=Comparable)
    e = [(1,), (2,), (3,)]
    eq_(e, a)

    # mixed lists and numeric
    d = [3, 1, [2]]
    a = sorted(d, key=Comparable)
    e = [1, 3, [2]]
    eq_(e, a)

    # lists containing None
    d = [[3], [None], [2]]
    a = sorted(d, key=Comparable)
    e = [[None], [2], [3]]
    eq_(e, a)

    # mixed lists and tuples
    d = [[3], [1], (2,)]
    a = sorted(d, key=Comparable)
    e = [[1], (2,), [3]]
    eq_(e, a)

    # length 2 lists
    d = [[3, 2], [3, 1], [2]]
    a = sorted(d, key=Comparable)
    e = [[2], [3, 1], [3, 2]]
    eq_(e, a)

    dt = datetime.now().replace

    # mixed everything
    d = [dt(hour=12),
         None,
         (dt(hour=3), 'b'),
         True,
         [b'aa', False],
         (b'aa', -1),
         3.4]
    a = sorted(d, key=Comparable)
    e = [None,
         True,
         3.4,
         dt(hour=12),
         (dt(hour=3), 'b'),
         (b'aa', -1),
         [b'aa', False]]
    eq_(e, a)


def test_comparable_ieq_table():
    rows = [[u'Bob', 42, 33],
          [u'Jim', 13, 69],
          [u'Joe', 86, 17],
          [u'Ted', 23, 51]]
    ieq(rows, rows)


def test_comparable_ieq_rows():
    rows = [['a', 'b', 'c'], [1, 2]]
    ieq(rows, rows)


def test_comparable_ieq_missing():
    x = ['a', 'b', 'c']
    y = ['a', 'b']
    with pytest.raises(AssertionError):
        ieq(x, y)
    with pytest.raises(AssertionError):
        ieq(y, x)

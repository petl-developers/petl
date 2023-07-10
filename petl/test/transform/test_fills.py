from __future__ import absolute_import, print_function, division


from petl.test.helpers import ieq
from petl.transform.fills import filldown, fillleft, fillright


def test_filldown():

    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (2, None, .56),
             (2, 'c', None),
             (None, 'c', .72))

    actual = filldown(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', .23),
              (2, 'b', .23),
              (2, 'b', .56),
              (2, 'c', .56),
              (2, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = filldown(table, 'bar')
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', None),
              (2, 'b', None),
              (2, 'b', .56),
              (2, 'c', None),
              (None, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = filldown(table, 'foo', 'bar')
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, 'a', .23),
              (1, 'b', None),
              (2, 'b', None),
              (2, 'b', .56),
              (2, 'c', None),
              (2, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)


def test_filldown_headerless():
    table = []
    actual = filldown(table, 'foo')
    expect = []
    ieq(expect, actual)


def test_fillright():

    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (2, None, .56),
             (2, 'c', None),
             (None, 'c', .72))

    actual = fillright(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', 'a'),
              (1, 1, .23),
              (1, 'b', 'b'),
              (2, 2, 2),
              (2, 2, .56),
              (2, 'c', 'c'),
              (None, 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fillright_headerless():
    table = []
    actual = fillright(table, 'foo')
    expect = []
    ieq(expect, actual)


def test_fillleft():

    table = (('foo', 'bar', 'baz'),
             (1, 'a', None),
             (1, None, .23),
             (1, 'b', None),
             (2, None, None),
             (None, None, .56),
             (2, 'c', None),
             (None, 'c', .72))

    actual = fillleft(table)
    expect = (('foo', 'bar', 'baz'),
              (1, 'a', None),
              (1, .23, .23),
              (1, 'b', None),
              (2, None, None),
              (.56, .56, .56),
              (2, 'c', None),
              ('c', 'c', .72))
    ieq(expect, actual)
    ieq(expect, actual)


def test_fillleft_headerless():
    table = []
    actual = fillleft(table, 'foo')
    expect = []
    ieq(expect, actual)

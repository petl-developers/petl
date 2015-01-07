from __future__ import absolute_import, print_function, division


from petl.test.helpers import eq_
from petl.compat import PY2
from petl.util.misc import typeset, diffvalues, diffheaders


def test_typeset():

    table = (('foo', 'bar', 'baz'),
             (b'A', 1, u'2'),
             (b'B', '2', u'3.4'),
             (b'B', '3', u'7.8', True),
             (u'D', u'xyz', 9.0),
             (b'E', 42))

    actual = typeset(table, 'foo')
    if PY2:
        expect = set(['str', 'unicode'])
    else:
        expect = set(['bytes', 'str'])
    eq_(expect, actual)


def test_diffheaders():

    table1 = (('foo', 'bar', 'baz'),
              ('a', 1, .3))

    table2 = (('baz', 'bar', 'quux'),
              ('a', 1, .3))

    add, sub = diffheaders(table1, table2)
    eq_(set(['quux']), add)
    eq_(set(['foo']), sub)


def test_diffvalues():

    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 3))

    table2 = (('bar', 'foo'),
              (1, 'a'),
              (3, 'c'))

    add, sub = diffvalues(table1, table2, 'foo')
    eq_(set(['c']), add)
    eq_(set(['b']), sub)



from __future__ import absolute_import, print_function, division


from petl.test.helpers import eq_
from petl.compat import PY2
from petl.util.misc import typeset, diffvalues, diffheaders, substringbefore, \
    substringafter


def test_typeset():

    table = (('foo', 'bar', 'baz'),
             (b'A', 1, u'2'),
             (b'B', '2', u'3.4'),
             (b'B', '3', u'7.8', True),
             (u'D', u'xyz', 9.0),
             (b'E', 42))

    actual = typeset(table, 'foo')
    if PY2:
        expect = {'str', 'unicode'}
    else:
        expect = {'bytes', 'str'}
    eq_(expect, actual)


def test_diffheaders():

    table1 = (('foo', 'bar', 'baz'),
              ('a', 1, .3))

    table2 = (('baz', 'bar', 'quux'),
              ('a', 1, .3))

    add, sub = diffheaders(table1, table2)
    eq_({'quux'}, add)
    eq_({'foo'}, sub)


def test_diffvalues():

    table1 = (('foo', 'bar'),
              ('a', 1),
              ('b', 3))

    table2 = (('bar', 'foo'),
              (1, 'a'),
              (3, 'c'))

    add, sub = diffvalues(table1, table2, 'foo')
    eq_({'c'}, add)
    eq_({'b'}, sub)


def test_substringbefore():

    before = substringbefore(':')
    eq_('foo', before('foo:bar:baz'))
    eq_('foo', before('foo'))
    eq_('', before(':foo'))

    before_bytes = substringbefore(b':')
    eq_(b'foo', before_bytes(b'foo:bar'))


def test_substringafter():

    after = substringafter(':')
    eq_('bar:baz', after('foo:bar:baz'))
    eq_('', after('foo'))
    eq_('foo', after(':foo'))

    after_bytes = substringafter(b':')
    eq_(b'bar', after_bytes(b'foo:bar'))


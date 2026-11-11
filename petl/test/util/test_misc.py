from __future__ import absolute_import, print_function, division


import pytest

import petl as etl
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


@pytest.mark.parametrize('missing, cell', [
    ('N/A', ''.join(['N', '/', 'A'])),
    (10 ** 20, int('100000000000000000000')),
    (['missing'], ['missing']),
])
def test_coalesce_matches_missing_values_by_equality(missing, cell):
    assert cell == missing
    assert cell is not missing
    table = (('a', 'b', 'c'),
             (cell, cell, 7),
             (cell, cell, cell),
             (5, 6, 7),
             (None, 7, 8))
    actual = etl.addfield(table, 'chosen',
                          etl.coalesce('a', 'b', 'c', missing=missing,
                                       default=0))
    expected = [('a', 'b', 'c', 'chosen'),
                (cell, cell, 7, 7),
                (cell, cell, cell, 0),
                (5, 6, 7, 5),
                (None, 7, 8, None)]
    eq_(expected, list(actual))
    eq_(expected, list(actual))


def test_coalesce_skips_identical_nan_marker():
    missing = float('nan')
    table = (('a', 'b'), (missing, 9), (missing, missing))
    actual = etl.addfield(table, 'chosen',
                          etl.coalesce('a', 'b', missing=missing, default=7))
    eq_([9, 7], [row[-1] for row in list(actual)[1:]])


@pytest.mark.parametrize('missing', [None, 'N/A'])
def test_coalesce_preserves_array_values(missing):
    np = pytest.importorskip('numpy')
    value = np.array([1, 2])
    table = (('a', 'b'), (value, 9))
    actual = etl.addfield(table, 'chosen',
                          etl.coalesce('a', 'b', missing=missing))
    assert list(actual)[1][-1] is value


def test_coalesce_skips_identical_array_marker():
    np = pytest.importorskip('numpy')
    missing = np.array([1, 2])
    table = (('a', 'b'), (missing, 9), (missing, missing))
    actual = etl.addfield(table, 'chosen',
                          etl.coalesce('a', 'b', missing=missing, default=7))
    eq_([9, 7], [row[-1] for row in list(actual)[1:]])


def test_coalesce_preserves_non_missing_false_values():
    table = (('a', 'b'),
             (0, 9),
             (False, 9),
             ('', 9),
             ([], 9),
             (None, 9),
             (None, None))
    actual = etl.addfield(table, 'chosen', etl.coalesce('a', 'b', default=7))
    eq_([('a', 'b', 'chosen'),
         (0, 9, 0),
         (False, 9, False),
         ('', 9, ''),
         ([], 9, []),
         (None, 9, 9),
         (None, None, 7)], list(actual))


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


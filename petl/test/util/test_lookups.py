from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import DuplicateKeyError, FieldSelectionError
from petl.test.helpers import eq_
from petl import cut, lookup, lookupone, dictlookup, dictlookupone, \
    recordlookup, recordlookupone


def test_lookup():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    # lookup one column on another
    actual = lookup(t1, 'foo', 'bar')
    expect = {'a': [1], 'b': [2, 3]}
    eq_(expect, actual)

    # test default value - tuple of whole row
    actual = lookup(t1, 'foo')  # no value selector
    expect = {'a': [('a', 1)], 'b': [('b', 2), ('b', 3)]}
    eq_(expect, actual)
    # test default value - key only
    actual = lookup(cut(t1, 'foo'), 'foo')
    expect = {'a': [('a',)], 'b': [('b',), ('b',)]}
    eq_(expect, actual)

    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))

    # test value selection
    actual = lookup(t2, 'foo', ('bar', 'baz'))
    expect = {'a': [(1, True)], 'b': [(2, False), (3, True), (3, False)]}
    eq_(expect, actual)

    # test compound key
    actual = lookup(t2, ('foo', 'bar'), 'baz')
    expect = {('a', 1): [True], ('b', 2): [False], ('b', 3): [True, False]}
    eq_(expect, actual)


def test_lookup_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        lookup(table, 'foo', 'bar')


def test_lookupone():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    # lookup one column on another under strict mode
    try:
        lookupone(t1, 'foo', 'bar', strict=True)
    except DuplicateKeyError:
        pass  # expected
    else:
        assert False, 'expected error'

    # lookup one column on another under, not strict
    actual = lookupone(t1, 'foo', 'bar', strict=False)
    expect = {'a': 1, 'b': 2}  # first value wins
    eq_(expect, actual)

    # test default value - tuple of whole row
    actual = lookupone(t1, 'foo', strict=False)  # no value selector
    expect = {'a': ('a', 1), 'b': ('b', 2)}  # first wins
    eq_(expect, actual)
    # test default value - key only
    actual = lookupone(cut(t1, 'foo'), 'foo')
    expect = {'a': ('a',), 'b': ('b',)}
    eq_(expect, actual)

    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))

    # test value selection
    actual = lookupone(t2, 'foo', ('bar', 'baz'), strict=False)
    expect = {'a': (1, True), 'b': (2, False)}
    eq_(expect, actual)

    # test compound key
    actual = lookupone(t2, ('foo', 'bar'), 'baz', strict=False)
    expect = {('a', 1): True, ('b', 2): False, ('b', 3): True}  # first wins
    eq_(expect, actual)


def test_lookupone_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        lookupone(table, 'foo', 'bar')


def test_dictlookup():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    actual = dictlookup(t1, 'foo')
    expect = {'a': [{'foo': 'a', 'bar': 1}],
              'b': [{'foo': 'b', 'bar': 2}, {'foo': 'b', 'bar': 3}]}
    eq_(expect, actual)
    # key only
    actual = dictlookup(cut(t1, 'foo'), 'foo')
    expect = {'a': [{'foo': 'a'}],
              'b': [{'foo': 'b'}, {'foo': 'b'}]}
    eq_(expect, actual)

    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))

    # test compound key
    actual = dictlookup(t2, ('foo', 'bar'))
    expect = {('a', 1): [{'foo': 'a', 'bar': 1, 'baz': True}],
              ('b', 2): [{'foo': 'b', 'bar': 2, 'baz': False}],
              ('b', 3): [{'foo': 'b', 'bar': 3, 'baz': True},
                         {'foo': 'b', 'bar': 3, 'baz': False}]}
    eq_(expect, actual)


def test_dictlookupone():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    try:
        dictlookupone(t1, 'foo', strict=True)
    except DuplicateKeyError:
        pass  # expected
    else:
        assert False, 'expected error'

    # relax
    actual = dictlookupone(t1, 'foo', strict=False)
    # first wins
    expect = {'a': {'foo': 'a', 'bar': 1}, 'b': {'foo': 'b', 'bar': 2}}
    eq_(expect, actual)
    # key only
    actual = dictlookupone(cut(t1, 'foo'), 'foo')
    expect = {'a': {'foo': 'a'},
              'b': {'foo': 'b'}}
    eq_(expect, actual)

    t2 = (('foo', 'bar', 'baz'),
          ('a', 1, True),
          ('b', 2, False),
          ('b', 3, True),
          ('b', 3, False))

    # test compound key
    actual = dictlookupone(t2, ('foo', 'bar'), strict=False)
    expect = {('a', 1): {'foo': 'a', 'bar': 1, 'baz': True},
              ('b', 2): {'foo': 'b', 'bar': 2, 'baz': False},
              ('b', 3): {'foo': 'b', 'bar': 3, 'baz': True}}  # first wins
    eq_(expect, actual)


def test_recordlookup():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    lkp = recordlookup(t1, 'foo')
    eq_(['a'], [r.foo for r in lkp['a']])
    eq_(['b', 'b'], [r.foo for r in lkp['b']])
    eq_([1], [r.bar for r in lkp['a']])
    eq_([2, 3], [r.bar for r in lkp['b']])

    # key only
    lkp = recordlookup(cut(t1, 'foo'), 'foo')
    eq_(['a'], [r.foo for r in lkp['a']])
    eq_(['b', 'b'], [r.foo for r in lkp['b']])


def test_recordlookupone():

    t1 = (('foo', 'bar'), ('a', 1), ('b', 2), ('b', 3))

    try:
        recordlookupone(t1, 'foo', strict=True)
    except DuplicateKeyError:
        pass  # expected
    else:
        assert False, 'expected error'

    # relax
    lkp = recordlookupone(t1, 'foo', strict=False)
    eq_('a', lkp['a'].foo)
    eq_('b', lkp['b'].foo)
    eq_(1, lkp['a'].bar)
    eq_(2, lkp['b'].bar)  # first wins

    # key only
    lkp = recordlookupone(cut(t1, 'foo'), 'foo', strict=False)
    eq_('a', lkp['a'].foo)
    eq_('b', lkp['b'].foo)

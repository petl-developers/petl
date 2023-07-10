from __future__ import absolute_import, print_function, division

import pytest

from petl.errors import FieldSelectionError
from petl.test.helpers import ieq, eq_
from petl.compat import next
from petl.util.base import header, fieldnames, data, dicts, records, \
    namedtuples, itervalues, values, rowgroupby


def test_header():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = header(table)
    expect = ('foo', 'bar')
    eq_(expect, actual)
    table = (['foo', 'bar'], ['a', 1], ['b', 2])
    actual = header(table)
    eq_(expect, actual)


def test_fieldnames():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = fieldnames(table)
    expect = ('foo', 'bar')
    eq_(expect, actual)

    class CustomField(object):

        def __init__(self, key, description):
            self.key = key
            self.description = description

        def __str__(self):
            return self.key

        def __repr__(self):
            return 'CustomField(%r, %r)' % (self.key, self.description)

    table = ((CustomField('foo', 'Get some foo.'),
              CustomField('bar', 'A lot of bar.')),
             ('a', 1),
             ('b', 2))
    actual = fieldnames(table)
    expect = ('foo', 'bar')
    eq_(expect, actual)


def test_data():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = data(table)
    expect = (('a', 1), ('b', 2))
    ieq(expect, actual)


def test_data_headerless():
    table = []
    actual = data(table)
    expect = []
    ieq(expect, actual)


def test_dicts():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = dicts(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': 2})
    ieq(expect, actual)


def test_dicts_headerless():
    table = []
    actual = dicts(table)
    expect = []
    ieq(expect, actual)


def test_dicts_shortrows():
    table = (('foo', 'bar'), ('a', 1), ('b',))
    actual = dicts(table)
    expect = ({'foo': 'a', 'bar': 1}, {'foo': 'b', 'bar': None})
    ieq(expect, actual)


def test_records():
    table = (('foo', 'bar'), ('a', 1), ('b', 2), ('c', 3))
    actual = records(table)
    # access items
    it = iter(actual)
    o = next(it)
    eq_('a', o['foo'])
    eq_(1, o['bar'])
    o = next(it)
    eq_('b', o['foo'])
    eq_(2, o['bar'])
    # access attributes
    it = iter(actual)
    o = next(it)
    eq_('a', o.foo)
    eq_(1, o.bar)
    o = next(it)
    eq_('b', o.foo)
    eq_(2, o.bar)
    # access with get() method
    it = iter(actual)
    o = next(it)
    eq_('a', o.get('foo'))
    eq_(1, o.get('bar'))
    eq_(None, o.get('baz'))
    eq_('qux', o.get('baz', default='qux'))


def test_records_headerless():
    table = []
    actual = records(table)
    expect = []
    ieq(expect, actual)


def test_records_errors():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = records(table)
    # access items
    it = iter(actual)
    o = next(it)
    try:
        o['baz']
    except KeyError:
        pass
    else:
        raise Exception('expected exception not raised')
    try:
        o.baz
    except AttributeError:
        pass
    else:
        raise Exception('expected exception not raised')


def test_records_unevenrows():
    table = (('foo', 'bar'), ('a', 1, True), ('b',))
    actual = records(table)
    # access items
    it = iter(actual)
    o = next(it)
    eq_('a', o['foo'])
    eq_(1, o['bar'])
    o = next(it)
    eq_('b', o['foo'])
    eq_(None, o['bar'])
    # access attributes
    it = iter(actual)
    o = next(it)
    eq_('a', o.foo)
    eq_(1, o.bar)
    o = next(it)
    eq_('b', o.foo)
    eq_(None, o.bar)


def test_namedtuples():
    table = (('foo', 'bar'), ('a', 1), ('b', 2))
    actual = namedtuples(table)
    it = iter(actual)
    o = next(it)
    eq_('a', o.foo)
    eq_(1, o.bar)
    o = next(it)
    eq_('b', o.foo)
    eq_(2, o.bar)


def test_namedtuples_headerless():
    table = []
    actual = namedtuples(table)
    expect = []
    ieq(expect, actual)


def test_namedtuples_unevenrows():
    table = (('foo', 'bar'), ('a', 1, True), ('b',))
    actual = namedtuples(table)
    it = iter(actual)
    o = next(it)
    eq_('a', o.foo)
    eq_(1, o.bar)
    o = next(it)
    eq_('b', o.foo)
    eq_(None, o.bar)


def test_itervalues():

    table = (('foo', 'bar', 'baz'),
             ('a', 1, True),
             ('b', 2),
             ('b', 7, False))

    actual = itervalues(table, 'foo')
    expect = ('a', 'b', 'b')
    ieq(expect, actual)

    actual = itervalues(table, 'bar')
    expect = (1, 2, 7)
    ieq(expect, actual)

    actual = itervalues(table, ('foo', 'bar'))
    expect = (('a', 1), ('b', 2), ('b', 7))
    ieq(expect, actual)

    actual = itervalues(table, 'baz')
    expect = (True, None, False)
    ieq(expect, actual)

    actual = itervalues(table, ('foo', 'baz'))
    expect = (('a', True), ('b', None), ('b', False))
    ieq(expect, actual)


def test_itervalues_headerless():
    table = []
    actual = itervalues(table, 'foo')
    with pytest.raises(FieldSelectionError):
        for i in actual:
            pass


def test_values():

    table = (('foo', 'bar', 'baz'),
             ('a', 1, True),
             ('b', 2),
             ('b', 7, False))

    actual = values(table, 'foo')
    expect = ('a', 'b', 'b')
    ieq(expect, actual)
    ieq(expect, actual)

    actual = values(table, 'bar')
    expect = (1, 2, 7)
    ieq(expect, actual)
    ieq(expect, actual)

    # old style signature for multiple fields, still supported
    actual = values(table, ('foo', 'bar'))
    expect = (('a', 1), ('b', 2), ('b', 7))
    ieq(expect, actual)
    ieq(expect, actual)

    # as of 0.24 new style signature for multiple fields
    actual = values(table, 'foo', 'bar')
    expect = (('a', 1), ('b', 2), ('b', 7))
    ieq(expect, actual)
    ieq(expect, actual)

    actual = values(table, 'baz')
    expect = (True, None, False)
    ieq(expect, actual)
    ieq(expect, actual)


def test_values_headerless():
    table = []
    actual = values(table, 'foo')
    with pytest.raises(FieldSelectionError):
        for i in actual:
            pass


def test_rowgroupby():

    table = (('foo', 'bar', 'baz'),
             ('a', 1, True),
             ('b', 2, True),
             ('b', 3))

    # simplest form

    g = rowgroupby(table, 'foo')

    key, vals = next(g)
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(('a', 1, True), vals[0])

    key, vals = next(g)
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(('b', 2, True), vals[0])
    eq_(('b', 3), vals[1])

    # specify value

    g = rowgroupby(table, 'foo', 'bar')

    key, vals = next(g)
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(1, vals[0])

    key, vals = next(g)
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(2, vals[0])
    eq_(3, vals[1])

    # callable key

    g = rowgroupby(table, lambda r: r['foo'], lambda r: r['baz'])

    key, vals = next(g)
    vals = list(vals)
    eq_('a', key)
    eq_(1, len(vals))
    eq_(True, vals[0])

    key, vals = next(g)
    vals = list(vals)
    eq_('b', key)
    eq_(2, len(vals))
    eq_(True, vals[0])
    eq_(None, vals[1])  # gets padded


def test_rowgroupby_headerless():
    table = []
    with pytest.raises(FieldSelectionError):
        rowgroupby(table, 'foo')

from __future__ import absolute_import, print_function, division

import pickle
import shelve
from contextlib import closing

import pytest

from petl.errors import FieldSelectionError
from petl.test.helpers import ieq, eq_
from petl.compat import PY3, next
from petl.util.base import header, fieldnames, data, dicts, records, \
    namedtuples, itervalues, values, rowgroupby, expr
from petl.util.lookups import recordlookup


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


@pytest.mark.parametrize('protocol', range(pickle.HIGHEST_PROTOCOL + 1))
@pytest.mark.parametrize('row', [('a',), ('a', 1), ('a', 1, True)])
def test_records_pickle(protocol, row):
    original = next(iter(records([['foo', 'bar'], row], missing='missing')))
    original.extra = {'nested': [1, 2]}
    restored = pickle.loads(pickle.dumps(original, protocol=protocol))
    eq_(type(original), type(restored))
    eq_(tuple(original), tuple(restored))
    eq_(original.flds, restored.flds)
    eq_(original.missing, restored.missing)
    eq_(original.extra, restored.extra)
    eq_(original[0], restored[0])
    eq_(original['bar'], restored['bar'])
    eq_(original.bar, restored.bar)
    with pytest.raises(KeyError):
        restored['unknown']
    with pytest.raises(AttributeError):
        restored.unknown


def test_records_legacy_pickle():
    # Protocol 0 output produced before Record defined its own reducer.
    payload = (b'ccopy_reg\n_reconstructor\np0\n(cpetl.util.base\nRecord\np1\n'
               b'c__builtin__\ntuple\np2\n(Va\np3\ntp4\ntp5\nRp6\n(dp7\n'
               b'Vflds\np8\n(lp9\nVfoo\np10\naVbar\np11\nasVmissing\np12\ng12\nsb.')
    restored = pickle.loads(payload)
    eq_(('a',), tuple(restored))
    eq_('a', restored.foo)
    eq_('missing', restored.bar)


def test_recordlookup_shelve(tmpdir):
    filename = str(tmpdir.join('records'))
    table = [['foo', 'bar'], ['a', 1], ['a', 2]]
    with closing(shelve.open(filename)) as database:
        recordlookup(table, 'foo', dictionary=database)
    with closing(shelve.open(filename)) as database:
        eq_([1, 2], [record.bar for record in database['a']])
        eq_(['a', 'a'], [record['foo'] for record in database['a']])


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


def test_expr_ok():
    fu = expr("{foo} * {bar}")
    res = fu({'foo': 3, 'bar': 2})
    eq_(6, res)


def test_expr_nok():
    fu = expr("{foo2} * {bar2}", trusted=True)
    with pytest.raises(KeyError) as exc_info:
        fu({'foo': 3, 'bar': 2})
        assert exc_info is not None


def test_expr_inject():
    with pytest.raises(Exception) as exc_info:
        # trick: using trusted=None will allow to skip using asteval
        fu = expr("__import__('os').system('ls')", trusted=None)
        fu({'foo': 3, 'bar': 2})
        assert exc_info is not None


def _has_asteval():
    if PY3:
        try:
            import asteval
            return True
        except ImportError:
            pass
    return False


def test_expr_untrusted():
    if _has_asteval():
        with pytest.raises((ValueError, TypeError)) as exc_info:
            _fu = expr("__import__('os').system('ls')", trusted=False)
            if _fu is not None:
                _fu({'foo': 3, 'bar': 2})
            assert exc_info is not None


def test_expr_ok_untrusted():
    if _has_asteval():
        fu = expr("{foo} * {bar}")
        res = fu({'foo': 3, 'bar': 2})
        eq_(6, res)


def test_expr_nok_untrusted():
    if _has_asteval():
        fu = expr("{foo2} * {bar2}", trusted=True)
        with pytest.raises(KeyError) as exc_info:
            fu({'foo': 3, 'bar': 2})
            assert exc_info is not None

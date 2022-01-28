# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import pytest

import petl as etl
from petl.test.helpers import ieq, eq_, assert_almost_equal
from petl.io.numpy import toarray, fromarray, torecarray


try:
    # noinspection PyUnresolvedReferences
    import numpy as np
except ImportError as e:
    pytest.skip('SKIP numpy tests: %s' % e, allow_module_level=True)
else:

    def test_toarray_nodtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = toarray(t)
        assert isinstance(a, np.ndarray)
        assert isinstance(a['foo'], np.ndarray)
        assert isinstance(a['bar'], np.ndarray)
        assert isinstance(a['baz'], np.ndarray)
        eq_('apples', a['foo'][0])
        eq_('oranges', a['foo'][1])
        eq_('pears', a['foo'][2])
        eq_(1, a['bar'][0])
        eq_(3, a['bar'][1])
        eq_(7, a['bar'][2])
        assert_almost_equal(2.5, a['baz'][0])
        assert_almost_equal(4.4, a['baz'][1])
        assert_almost_equal(.1, a['baz'][2])

    def test_toarray_lists():
        t = [['foo', 'bar', 'baz'],
             ['apples', 1, 2.5],
             ['oranges', 3, 4.4],
             ['pears', 7, .1]]
        a = toarray(t)
        assert isinstance(a, np.ndarray)
        assert isinstance(a['foo'], np.ndarray)
        assert isinstance(a['bar'], np.ndarray)
        assert isinstance(a['baz'], np.ndarray)
        eq_('apples', a['foo'][0])
        eq_('oranges', a['foo'][1])
        eq_('pears', a['foo'][2])
        eq_(1, a['bar'][0])
        eq_(3, a['bar'][1])
        eq_(7, a['bar'][2])
        assert_almost_equal(2.5, a['baz'][0], places=6)
        assert_almost_equal(4.4, a['baz'][1], places=6)
        assert_almost_equal(.1, a['baz'][2], places=6)

    def test_torecarray():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = torecarray(t)
        assert isinstance(a, np.ndarray)
        assert isinstance(a.foo, np.ndarray)
        assert isinstance(a.bar, np.ndarray)
        assert isinstance(a.baz, np.ndarray)
        eq_('apples', a.foo[0])
        eq_('oranges', a.foo[1])
        eq_('pears', a.foo[2])
        eq_(1, a.bar[0])
        eq_(3, a.bar[1])
        eq_(7, a.bar[2])
        assert_almost_equal(2.5, a.baz[0], places=6)
        assert_almost_equal(4.4, a.baz[1], places=6)
        assert_almost_equal(.1, a.baz[2], places=6)

    def test_toarray_stringdtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = toarray(t, dtype='U4, i2, f4')
        assert isinstance(a, np.ndarray)
        assert isinstance(a['foo'], np.ndarray)
        assert isinstance(a['bar'], np.ndarray)
        assert isinstance(a['baz'], np.ndarray)
        eq_('appl', a['foo'][0])
        eq_('oran', a['foo'][1])
        eq_('pear', a['foo'][2])
        eq_(1, a['bar'][0])
        eq_(3, a['bar'][1])
        eq_(7, a['bar'][2])
        assert_almost_equal(2.5, a['baz'][0], places=6)
        assert_almost_equal(4.4, a['baz'][1], places=6)
        assert_almost_equal(.1, a['baz'][2], places=6)

    def test_toarray_dictdtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = toarray(t, dtype={'foo': 'U4'})  # specify partial dtype
        assert isinstance(a, np.ndarray)
        assert isinstance(a['foo'], np.ndarray)
        assert isinstance(a['bar'], np.ndarray)
        assert isinstance(a['baz'], np.ndarray)
        eq_('appl', a['foo'][0])
        eq_('oran', a['foo'][1])
        eq_('pear', a['foo'][2])
        eq_(1, a['bar'][0])
        eq_(3, a['bar'][1])
        eq_(7, a['bar'][2])
        assert_almost_equal(2.5, a['baz'][0])
        assert_almost_equal(4.4, a['baz'][1])
        assert_almost_equal(.1, a['baz'][2])

    def test_toarray_explicitdtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = toarray(t, dtype=[('A', 'U4'), ('B', 'i2'), ('C', 'f4')])
        assert isinstance(a, np.ndarray)
        assert isinstance(a['A'], np.ndarray)
        assert isinstance(a['B'], np.ndarray)
        assert isinstance(a['C'], np.ndarray)
        eq_('appl', a['A'][0])
        eq_('oran', a['A'][1])
        eq_('pear', a['A'][2])
        eq_(1, a['B'][0])
        eq_(3, a['B'][1])
        eq_(7, a['B'][2])
        assert_almost_equal(2.5, a['C'][0], places=6)
        assert_almost_equal(4.4, a['C'][1], places=6)
        assert_almost_equal(.1, a['C'][2], places=6)

    def test_fromarray():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        a = toarray(t)
        u = fromarray(a)
        ieq(t, u)

    def test_integration():
        t = etl.wrap([('foo', 'bar', 'baz'),
                      ('apples', 1, 2.5),
                      ('oranges', 3, 4.4),
                      ('pears', 7, .1)])
        a = t.toarray()
        u = etl.fromarray(a).convert('bar', int)
        ieq(t, u)

    def test_valuesarray_no_dtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]

        expect = np.array([1, 3, 7])
        actual = etl.wrap(t).values('bar').array()
        eq_(expect.dtype, actual.dtype)
        assert np.all(expect == actual)

    def test_valuesarray_explicit_dtype():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]
        expect = np.array([1, 3, 7], dtype='i2')
        actual = etl.wrap(t).values('bar').array(dtype='i2')
        eq_(expect.dtype, actual.dtype)
        assert np.all(expect == actual)

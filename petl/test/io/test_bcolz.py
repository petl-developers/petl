# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
import tempfile

import pytest

from petl.test.helpers import ieq, eq_
from petl.io.bcolz import frombcolz, tobcolz, appendbcolz


try:
    import bcolz
except ImportError as e:
    pytest.skip('SKIP bcolz tests: %s' % e, allow_module_level=True)
else:

    def test_frombcolz():

        cols = [
            ['apples', 'oranges', 'pears'],
            [1, 3, 7],
            [2.5, 4.4, .1]
        ]
        names = ('foo', 'bar', 'baz')
        rootdir = tempfile.mkdtemp()
        ctbl = bcolz.ctable(cols, names=names, rootdir=rootdir, mode='w')
        ctbl.flush()

        expect = [names] + list(zip(*cols))

        # from ctable object
        actual = frombcolz(ctbl)
        ieq(expect, actual)
        ieq(expect, actual)

        # from rootdir
        actual = frombcolz(rootdir)
        ieq(expect, actual)
        ieq(expect, actual)

    def test_tobcolz():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]

        ctbl = tobcolz(t)
        assert isinstance(ctbl, bcolz.ctable)
        eq_(t[0], tuple(ctbl.names))
        ieq(t[1:], (tuple(r) for r in ctbl.iter()))

        ctbl = tobcolz(t, chunklen=2)
        assert isinstance(ctbl, bcolz.ctable)
        eq_(t[0], tuple(ctbl.names))
        ieq(t[1:], (tuple(r) for r in ctbl.iter()))
        eq_(2, ctbl.cols[ctbl.names[0]].chunklen)

    def test_appendbcolz():
        t = [('foo', 'bar', 'baz'),
             ('apples', 1, 2.5),
             ('oranges', 3, 4.4),
             ('pears', 7, .1)]

        # append to in-memory ctable
        ctbl = tobcolz(t)
        appendbcolz(t, ctbl)
        eq_(t[0], tuple(ctbl.names))
        ieq(t[1:] + t[1:], (tuple(r) for r in ctbl.iter()))

        # append to on-disk ctable
        rootdir = tempfile.mkdtemp()
        tobcolz(t, rootdir=rootdir)
        appendbcolz(t, rootdir)
        ctbl = bcolz.open(rootdir, mode='r')
        eq_(t[0], tuple(ctbl.names))
        ieq(t[1:] + t[1:], (tuple(r) for r in ctbl.iter()))

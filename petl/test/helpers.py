from __future__ import absolute_import, print_function, division

import sys

from nose.tools import eq_, assert_almost_equal

from petl.compat import izip_longest


def ieq(expect, actual, cast=None):
    '''test when values are equals for eacfh row and column'''
    ie = iter(expect)
    ia = iter(actual)
    for re, ra in izip_longest(ie, ia, fillvalue=None):
        if cast:
            ra = cast(ra)
        if re is None and ra is None:
            continue
        if type(re) in (int, float, bool, str):
            eq_(re, ra)
            continue
        for ve, va in izip_longest(re, ra, fillvalue=None):
            if isinstance(ve, list):
                for je, ja in izip_longest(ve, va, fillvalue=None):
                    _eq_print(je, ja, re, ra)
            elif not isinstance(ve, dict):
                _eq_print(ve, va, re, ra)


def _eq_print(ve, va, re, ra):
    try:
        eq_(ve, va)
    except AssertionError as ea:
        print('\nrow: ', re, ' != ', ra, file=sys.stderr)
        print('val: ', ve, ' != ', va, file=sys.stderr)
        raise ea


def ieq2(expect, actual, cast=None):
    '''test twice when values are equals for eacfh row and column'''
    ieq(expect, actual, cast)
    ieq(expect, actual, cast)


def test_iassertequal():
    x = ['a', 'b']
    y = ['a', 'b', 'c']
    try:
        ieq(x, y)
    except AssertionError:
        pass
    else:
        assert False, 'did not catch actual item left over'
    try:
        ieq(y, x)
    except AssertionError:
        pass
    else:
        assert False, 'did not catch expected item left over'

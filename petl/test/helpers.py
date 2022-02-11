from __future__ import absolute_import, print_function, division

import os
import sys

import pytest

from petl.compat import izip_longest


def eq_(expect, actual, msg=None):
    """Test when two values from a python variable are exactly equals (==)"""
    assert expect == actual, msg


def assert_almost_equal(first, second, places=None, msg=None):
    """Test when the values are aproximatedly equals by a places exponent"""
    vabs = None if places is None else 10 ** (- places)
    assert pytest.approx(first, second, abs=vabs), msg


def ieq(expect, actual, cast=None):
    """Test when values of a iterable are equals for each row and column"""
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
    """Print two values when they aren't exactly equals (==)"""
    try:
        eq_(ve, va)
    except AssertionError as ea:
        # Show the values but only when they differ
        print('\nrow: ', re, ' != ', ra, file=sys.stderr)
        print('val: ', ve, ' != ', va, file=sys.stderr)
        raise ea


def ieq2(expect, actual, cast=None):
    """Test when iterables values are equals twice looking for side effects"""
    ieq(expect, actual, cast)
    ieq(expect, actual, cast)

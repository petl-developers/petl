from __future__ import absolute_import, print_function, division


from petl.compat import izip_longest
from nose.tools import eq_, assert_almost_equal


def ieq(expect, actual, cast=None):
    ie = iter(expect)
    ia = iter(actual)
    for e, a in izip_longest(ie, ia, fillvalue=None):
        if cast:
            a = cast(a)
        eq_(e, a)
        
    
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

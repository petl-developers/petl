"""
Common test functions.

"""


from itertools import izip_longest
from nose.tools import eq_


assertequal = eq_ # backwards compatibility


def ieq(expect, actual, cast=None):
    ie = iter(expect)
    ia = iter(actual)
    for e, a in izip_longest(ie, ia, fillvalue=None):
#        if isinstance(e, list):
#            e = tuple(e)
#        if isinstance(a, list):
#            a = tuple(a)
        if cast:
            a = cast(a)
        eq_(e, a)
        
    
iassertequal = ieq # backwards compatibility


def test_iassertequal():
    x = ['a', 'b']
    y = ['a', 'b', 'c']
    try:
        iassertequal(x, y)
    except AssertionError:
        pass
    else:
        assert False, 'did not catch actual item left over'
    try:
        iassertequal(y, x)
    except AssertionError:
        pass
    else:
        assert False, 'did not catch expected item left over'
    
    


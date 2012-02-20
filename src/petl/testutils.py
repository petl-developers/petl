"""
Common test functions.

"""


from itertools import izip_longest


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    ie = iter(expect)
    ia = iter(actual)
    for e, a in izip_longest(ie, ia, fillvalue=None):
#        if isinstance(e, list):
#            e = tuple(e)
#        if isinstance(a, list):
#            a = tuple(a)
        assert e == a, (e, a)


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
    
    


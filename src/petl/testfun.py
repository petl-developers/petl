"""
Common test functions.

"""


def assertequal(expect, actual):
    assert expect == actual, (expect, actual)


def iassertequal(expect, actual):
    ie = iter(expect)
    ia = iter(actual)
    for e, a in zip(ie, ia):
        if isinstance(e, list):
            e = tuple(e)
        if isinstance(a, list):
            a = tuple(a)
        assert e == a, (e, a)
    try:
        v = ie.next()
    except StopIteration:
        pass
    else:
        assert False, 'expected item left over: %r' % v
    try:
        v = ia.next()
    except StopIteration:
        pass
    else:
        assert False, 'actual item left over: %r' % v



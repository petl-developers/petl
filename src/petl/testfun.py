"""
Common test functions.

"""


from itertools import izip_longest
import random

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
    
    
class dummytable(object):
    
    def __init__(self, numflds, numrows):
        self.numflds = numflds
        self.numrows = numrows
        self.seed = random.random()
        
    def __iter__(self):
        # we want this to be stable, i.e., same data each time
        nf = self.numflds
        nr = self.numrows
        seed = self.seed
        flds = ['f%s' % n for n in range(nf)]
        yield flds
        for i in xrange(nr):
            row = [hash((seed, i, n)) for n in range(nf)]
            yield tuple(row)
            
    def reseed(self):
        self.seed = random.random()
        
    def cachetag(self):
        return hash((self.numflds, self.numrows, self.seed))
        


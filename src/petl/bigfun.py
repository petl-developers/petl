"""
Profiling utilities.

"""
import random

class bigtable(object):
    
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
            yield row
            
    def cachetag(self):
        return hash((self.numflds, self.numrows, self.seed))
        


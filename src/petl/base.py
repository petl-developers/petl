"""
Base classes.

"""


from itertools import islice


class IterContainer(object):
    
    def __contains__(self, item):
        for o in self:
            if o == item:
                return True
        return False
        
    def __len__(self):
        l = 0
        for o in self:
            l += 1
        return l
        
    def __getitem__(self, item):
        if isinstance(item, int):
            try:
                return islice(self, item, item+1).next()
            except StopIteration:
                raise IndexError('index out of range')
        elif isinstance(item, slice):
            return islice(self, item.start, item.stop, item.step)

    def index(self, item):
        for i, o in enumerate(self):
            if o == item:
                return i
        raise ValueError('%s is not in container' % item)



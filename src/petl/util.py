"""
TODO doc me

"""


from operator import itemgetter


def closeit(iterator):
    """
    TODO doc me
    
    """
    if hasattr(iterator, 'close') and callable(getattr(iterator, 'close')):
        iterator.close()
        
        
def asindices(fields, selection):
    """
    TODO doc me
    
    """
    indices = list()
    for s in selection:
        # selection could be a field name
        if s in fields:
            indices.append(fields.index(s))
        # or selection could be a field index
        elif isinstance(s, int) and s - 1 < len(fields):
            indices.append(s - 1) # index fields from 1, not 0
        else:
            raise FieldSelectionError(s)
    return indices
        
        
class FieldSelectionError(Exception):
    """
    TODO doc me
    
    """
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return 'selection is not a field or valid field index: %s' % self.value
    
    
def rowgetter(*indices):
    """
    TODO doc me
    
    """
    
    # guard condition
    assert len(indices) > 0, 'indices is empty'

    # if only one index, we cannot use itemgetter, because we want a singleton 
    # sequence to be returned, but itemgetter with a single argument returns the 
    # value itself, so let's define a function
    if len(indices) == 1:
        index = indices[0]
        return lambda row: (row[index],) # note comma - singleton tuple!
    # if more than one index, use itemgetter, it should be the most efficient
    else:
        return itemgetter(*indices)
    
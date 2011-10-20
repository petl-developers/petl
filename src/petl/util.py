"""
TODO doc me

"""


from itertools import islice


__all__ = ['fields', 'data', 'records', 'count', 'look', 'see', 'values', 
           'types', 'parsetypes', 'stats', 'rowlengths']


def fields(table):
    """
    Return the header row for the given table. E.g.::
    
        >>> from petl import fields
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> fields(table)
        ['foo', 'bar']
    
    """
    
    it = iter(table)
    return it.next()

    
def data(table):
    """
    Return an iterator over the data rows for the given table. E.g.::
    
        >>> from petl import data
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = data(table)
        >>> it.next()
        ['a', 1]
        >>> it.next()
        ['b', 2]
    
    """
    
    return islice(table, 1, None)

    
def records(table, missing=None):
    """
    Return an iterator over the data in the table as a sequence of dictionaries.
    E.g.::
    
        >>> from petl import records
        >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]
        >>> it = records(table)
        >>> it.next()
        {'foo': 'a', 'bar': 1}
        >>> it.next()
        {'foo': 'b', 'bar': 2}

    """
    
    it = iter(table)
    fields = it.next()
    for row in it:
        try:
            # list comprehension should be faster
            items = [(fields[i], row[i]) for i in range(len(fields))]
        except IndexError:
            # short row, fall back to slower for loop
            items = list()
            for i, f in enumerate(fields):
                try:
                    v = row[i]
                except IndexError:
                    v = missing
                items.append((f, v))
        yield dict(items)
    
    
def count(table):
    """TODO doc me"""
    
    
def look(table):
    """TODO doc me"""
    
    
def see(table):
    """TODO doc me"""
    
    
def values(table):
    """TODO doc me"""
    
    
def types(table):
    """TODO doc me"""
    
    
def parsetypes(table):
    """TODO doc me"""
    
    
def stats(table):
    """TODO doc me"""
    
    
def rowlengths(table):
    """TODO doc me"""
    

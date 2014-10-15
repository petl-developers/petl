"""
The `petl` module.

"""


from __future__ import absolute_import, print_function, division


from petl.util import header, fieldnames, data, records, rowcount, look, see, \
    itervalues, values, iterdata, valuecounter, valuecounts, \
    valueset, isunique, lookup, lookupone, recordlookup, recordlookupone, \
    typecounter, typecounts, typeset, parsecounter, parsecounts, \
    stats, rowlengths, DuplicateKeyError, datetimeparser, dateparser, \
    timeparser, boolparser, \
    expr, limits, strjoin, valuecount, lookall, dataslice, parsenumber, \
    stringpatterns, stringpatterncounter, randomtable, dummytable, \
    diffheaders, diffvalues, columns, facetcolumns, heapqmergesorted, \
    shortlistmergesorted, progress, clock, isordered, rowgroupby, nrows, \
    nthword, lookstr, listoflists, tupleoftuples, listoftuples, tupleoflists, \
    lol, tot, tol, lot, iternamedtuples, namedtuples, iterrecords, dicts, \
    iterdicts, dictlookup, dictlookupone, cache, empty, numparser, coalesce

from petl.io import *

from petl.transform import *


__version__ = VERSION = '0.26'


def lenstats(table, field):
    """
    Convenience function to report statistics on value lengths under the given
    field. E.g.::

        >>> from petl import lenstats    
        >>> table1 = [['foo', 'bar'],
        ...           [1, 'a'],
        ...           [2, 'aaa'],
        ...           [3, 'aa'],
        ...           [4, 'aaa'],
        ...           [5, 'aaaaaaaaaaa']]
        >>> lenstats(table1, 'bar')
        {'count': 5, 'errors': 0, 'min': 1.0, 'max': 11.0, 'sum': 20.0, 'mean': 4.0}

    """

    return stats(convert(table, field, lambda v: len(v)), field)


# convenience aliases
eq = selecteq
ne = selectne
lt = selectlt
gt = selectgt
le = selectle
ge = selectge
true = selecttrue
false = selectfalse
none = selectnone
notnone = selectnotnone
counts = valuecounts
move = movefield

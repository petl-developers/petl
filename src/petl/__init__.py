"""
The `petl` module.

"""


from __future__ import absolute_import, print_function, division

from petl.comparison import Comparable

from petl.util import header, fieldnames, data, records, look, see, \
    itervalues, values, iterdata, valuecounter, valuecounts, \
    isunique, lookup, lookupone, recordlookup, recordlookupone, \
    typecounter, typecounts, typeset, parsecounter, parsecounts, \
    stats, rowlengths, DuplicateKeyError, datetimeparser, dateparser, \
    timeparser, boolparser, expr, limits, strjoin, valuecount, lookall, \
    stringpatterns, stringpatterncounter, randomtable, dummytable, \
    diffheaders, diffvalues, columns, facetcolumns, heapqmergesorted, \
    shortlistmergesorted, progress, clock, isordered, rowgroupby, nrows, \
    nthword, lookstr, listoflists, tupleoftuples, listoftuples, tupleoflists, \
    lol, tot, tol, lot, iternamedtuples, namedtuples, iterrecords, dicts, \
    iterdicts, dictlookup, dictlookupone, cache, empty, numparser, coalesce

from petl.io import *

from petl.transform import *


__version__ = VERSION = '1.0a1.dev0'


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

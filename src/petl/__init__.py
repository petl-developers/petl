"""
The `petl` module.

"""

from petl.util import header, fieldnames, data, records, rowcount, look, see, \
           itervalues, values, valuecounter, valuecounts, \
           valueset, unique, lookup, lookupone, recordlookup, recordlookupone, \
           typecounter, typecounts, typeset, parsecounter, parsecounts, \
           stats, rowlengths, DuplicateKeyError, datetimeparser, dateparser, timeparser, boolparser, \
           expr, limits, strjoin, valuecount, lookall, dataslice, parsenumber, \
           stringpatterns, stringpatterncounter, randomtable, dummytable, \
           diffheaders, diffvalues, columns

from petl.io import fromcsv, frompickle, fromsqlite3, tocsv, topickle, \
           tosqlite3, crc32sum, adler32sum, statsum, fromdb, \
           appendcsv, appendpickle, appendsqlite3, todb, appenddb, fromtext, \
           totext, appendtext, fromxml, Uncacheable, fromjson, fromdicts, tojson

from petl.transform import rename, cut, cat, convert, fieldconvert, extend, rowslice, \
           head, tail, sort, melt, recast, duplicates, conflicts, \
           mergereduce, select, complement, diff, capture, \
           split, fieldmap, facet, selecteq, rowreduce, merge, aggregate, recordreduce, \
           rowmap, recordmap, rowmapmany, recordmapmany, setheader, pushheader, skip, \
           extendheader, unpack, join, leftjoin, rightjoin, outerjoin, crossjoin, \
           antijoin, rangeaggregate, rangecounts, selectop, selectne, selectgt, \
           selectge, selectlt, selectle, rangefacet, selectrangeopenleft, \
           selectrangeopenright, selectrangeopen, selectrangeclosed, rangerowreduce, \
           rangerecordreduce, selectin, selectnotin, selectre, rowselect, recordselect, \
           fieldselect, rowlenselect, selectis, selectisnot, selectisinstance, transpose, \
           intersection, pivot, recordcomplement, recorddiff, cutout, skipcomments, \
           convertall, convertnumbers, hashjoin, hashleftjoin, hashrightjoin, \
           hashantijoin, hashcomplement, hashintersection, replace, replaceall, \
           resub, flatten, unflatten
           
           
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
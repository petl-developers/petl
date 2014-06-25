"""
Functions for transforming tables.

"""

from petl.transform.misc import cut, cutout, cat, \
    addfield, rowslice, head, tail, melt, recast, duplicates, unique, \
    conflicts, complement, recordcomplement, diff, recorddiff, \
    fieldmap, facet, rangefacet, \
    rowmap, recordmap, rowmapmany, recordmapmany, skipcomments, movefield, \
    unpack, unpackdict, transpose, intersection, pivot, flatten, unflatten, \
    annex, fold, addrownumbers, addcolumn, rowgroupmap, \
    distinct, coalesce, addfieldusingcontext

from petl.transform.headers import rename, setheader, extendheader, \
    pushheader, skip, prefixheader, suffixheader

from petl.transform.conversions import convert, convertall, replace, \
    replaceall, update, convertnumbers, fieldconvert

from petl.transform.sorts import sort, mergesort

from petl.transform.selects import select, selectop, selectcontains, selecteq, \
    selectfalse, selectge, selectgt, selectin, selectis, selectisinstance, \
    selectisnot, selectle, selectlt, selectne, selectnone, selectnotin, \
    selectnotnone, selectrangeclosed, selectrangeopen, selectrangeopenleft, \
    selectrangeopenright, selectre, selecttrue, selectusingcontext, \
    recordselect, rowselect, rowlenselect, fieldselect

from petl.transform.joins import join, leftjoin, rightjoin, outerjoin, \
    crossjoin, antijoin, lookupjoin, unjoin

from petl.transform.hashjoins import hashjoin, hashleftjoin, hashrightjoin, \
    hashantijoin, hashlookupjoin, hashintersection, hashcomplement

from petl.transform.reductions import rowreduce, recordreduce, mergeduplicates,\
    aggregate, rangeaggregate, rangecounts, rangerecordreduce, rangerowreduce, \
    groupcountdistinctvalues, groupselectfirst, groupselectmax, groupselectmin,\
    mergereduce, merge, multirangeaggregate

from petl.transform.fills import filldown, fillright, fillleft

from petl.transform.regex import capture, split, search, sub, resub


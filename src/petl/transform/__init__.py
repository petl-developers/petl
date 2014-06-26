"""
Functions for transforming tables.

"""

from petl.transform.basics import cut, cutout, movefield, cat, annex, \
    addfield, addfieldusingcontext, addrownumbers, addcolumn, rowslice, head, \
    tail, skipcomments

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
    recordselect, rowselect, rowlenselect, fieldselect, facet, rangefacet

from petl.transform.joins import join, leftjoin, rightjoin, outerjoin, \
    crossjoin, antijoin, lookupjoin, unjoin

from petl.transform.hashjoins import hashjoin, hashleftjoin, hashrightjoin, \
    hashantijoin, hashlookupjoin

from petl.transform.reductions import rowreduce, recordreduce, mergeduplicates,\
    aggregate, rangeaggregate, rangecounts, rangerecordreduce, rangerowreduce, \
    groupcountdistinctvalues, groupselectfirst, groupselectmax, groupselectmin,\
    mergereduce, merge, multirangeaggregate, fold

from petl.transform.fills import filldown, fillright, fillleft

from petl.transform.regex import capture, split, search, sub, resub

from petl.transform.reshape import melt, recast, transpose, pivot, flatten, \
    unflatten

from petl.transform.maps import fieldmap, rowmap, recordmap, rowmapmany, \
    recordmapmany, rowgroupmap

from petl.transform.unpacks import unpack, unpackdict

from petl.transform.dedup import duplicates, unique, distinct, conflicts

from petl.transform.setops import complement, intersection, \
    recordcomplement, diff, recorddiff, hashintersection, hashcomplement
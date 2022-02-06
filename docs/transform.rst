.. module:: petl.transform

Usage - transforming rows and columns
=====================================

.. module:: petl.transform.basics
.. _transform_basics:

Basic transformations
---------------------

.. autofunction:: petl.transform.basics.head
.. autofunction:: petl.transform.basics.tail
.. autofunction:: petl.transform.basics.rowslice
.. autofunction:: petl.transform.basics.cut
.. autofunction:: petl.transform.basics.cutout
.. autofunction:: petl.transform.basics.movefield
.. autofunction:: petl.transform.basics.cat
.. autofunction:: petl.transform.basics.stack
.. autofunction:: petl.transform.basics.skipcomments
.. autofunction:: petl.transform.basics.addfield
.. autofunction:: petl.transform.basics.addfields
.. autofunction:: petl.transform.basics.addcolumn
.. autofunction:: petl.transform.basics.addrownumbers
.. autofunction:: petl.transform.basics.addfieldusingcontext
.. autofunction:: petl.transform.basics.annex


.. module:: petl.transform.headers
.. _transform_headers:

Header manipulations
--------------------

.. autofunction:: petl.transform.headers.rename
.. autofunction:: petl.transform.headers.setheader
.. autofunction:: petl.transform.headers.extendheader
.. autofunction:: petl.transform.headers.pushheader
.. autofunction:: petl.transform.headers.prefixheader
.. autofunction:: petl.transform.headers.suffixheader
.. autofunction:: petl.transform.headers.sortheader
.. autofunction:: petl.transform.headers.skip


.. module:: petl.transform.conversions
.. _transform_conversions:

Converting values
-----------------

.. autofunction:: petl.transform.conversions.convert
.. autofunction:: petl.transform.conversions.convertall
.. autofunction:: petl.transform.conversions.convertnumbers
.. autofunction:: petl.transform.conversions.replace
.. autofunction:: petl.transform.conversions.replaceall
.. autofunction:: petl.transform.conversions.format
.. autofunction:: petl.transform.conversions.formatall
.. autofunction:: petl.transform.conversions.interpolate
.. autofunction:: petl.transform.conversions.interpolateall
.. autofunction:: petl.transform.conversions.update


.. module:: petl.transform.selects
.. _transform_selects:

Selecting rows
--------------

.. autofunction:: petl.transform.selects.select
.. autofunction:: petl.transform.selects.selectop
.. autofunction:: petl.transform.selects.selecteq
.. autofunction:: petl.transform.selects.selectne
.. autofunction:: petl.transform.selects.selectlt
.. autofunction:: petl.transform.selects.selectle
.. autofunction:: petl.transform.selects.selectgt
.. autofunction:: petl.transform.selects.selectge
.. autofunction:: petl.transform.selects.selectrangeopen
.. autofunction:: petl.transform.selects.selectrangeopenleft
.. autofunction:: petl.transform.selects.selectrangeopenright
.. autofunction:: petl.transform.selects.selectrangeclosed
.. autofunction:: petl.transform.selects.selectcontains
.. autofunction:: petl.transform.selects.selectin
.. autofunction:: petl.transform.selects.selectnotin
.. autofunction:: petl.transform.selects.selectis
.. autofunction:: petl.transform.selects.selectisnot
.. autofunction:: petl.transform.selects.selectisinstance
.. autofunction:: petl.transform.selects.selecttrue
.. autofunction:: petl.transform.selects.selectfalse
.. autofunction:: petl.transform.selects.selectnone
.. autofunction:: petl.transform.selects.selectnotnone
.. autofunction:: petl.transform.selects.selectusingcontext
.. autofunction:: petl.transform.selects.rowlenselect
.. autofunction:: petl.transform.selects.facet
.. autofunction:: petl.transform.selects.biselect


.. module:: petl.transform.regex
.. _transform_regex:

Regular expressions
-------------------

.. autofunction:: petl.transform.regex.search
.. autofunction:: petl.transform.regex.searchcomplement
.. autofunction:: petl.transform.regex.sub
.. autofunction:: petl.transform.regex.split
.. autofunction:: petl.transform.regex.splitdown
.. autofunction:: petl.transform.regex.capture


.. module:: petl.transform.unpacks
.. _transform_unpacks:

Unpacking compound values
-------------------------

.. autofunction:: petl.transform.unpacks.unpack
.. autofunction:: petl.transform.unpacks.unpackdict


.. module:: petl.transform.maps
.. _transform_maps:

Transforming rows
-----------------

.. autofunction:: petl.transform.maps.fieldmap
.. autofunction:: petl.transform.maps.rowmap
.. autofunction:: petl.transform.maps.rowmapmany
.. autofunction:: petl.transform.maps.rowgroupmap


.. module:: petl.transform.sorts
.. _transform_sorts:

Sorting
-------

.. autofunction:: petl.transform.sorts.sort
.. autofunction:: petl.transform.sorts.mergesort
.. autofunction:: petl.transform.sorts.issorted


.. module:: petl.transform.joins
.. _transform_joins:

Joins
-----

.. autofunction:: petl.transform.joins.join
.. autofunction:: petl.transform.joins.leftjoin
.. autofunction:: petl.transform.joins.lookupjoin
.. autofunction:: petl.transform.joins.rightjoin
.. autofunction:: petl.transform.joins.outerjoin
.. autofunction:: petl.transform.joins.crossjoin
.. autofunction:: petl.transform.joins.antijoin
.. autofunction:: petl.transform.joins.unjoin
.. autofunction:: petl.transform.hashjoins.hashjoin
.. autofunction:: petl.transform.hashjoins.hashleftjoin
.. autofunction:: petl.transform.hashjoins.hashlookupjoin
.. autofunction:: petl.transform.hashjoins.hashrightjoin
.. autofunction:: petl.transform.hashjoins.hashantijoin


.. module:: petl.transform.setops
.. _transform_setops:

Set operations
--------------

.. autofunction:: petl.transform.setops.complement
.. autofunction:: petl.transform.setops.diff
.. autofunction:: petl.transform.setops.recordcomplement
.. autofunction:: petl.transform.setops.recorddiff
.. autofunction:: petl.transform.setops.intersection
.. autofunction:: petl.transform.setops.hashcomplement
.. autofunction:: petl.transform.setops.hashintersection


.. module:: petl.transform.dedup
.. _transform_dedup:

Deduplicating rows
------------------

.. autofunction:: petl.transform.dedup.duplicates
.. autofunction:: petl.transform.dedup.unique
.. autofunction:: petl.transform.dedup.conflicts
.. autofunction:: petl.transform.dedup.distinct
.. autofunction:: petl.transform.dedup.isunique


.. module:: petl.transform.reductions
.. _transform_reductions:

Reducing rows (aggregation)
---------------------------

.. autofunction:: petl.transform.reductions.aggregate
.. autofunction:: petl.transform.reductions.rowreduce
.. autofunction:: petl.transform.reductions.mergeduplicates
.. autofunction:: petl.transform.reductions.merge
.. autofunction:: petl.transform.reductions.fold
.. autofunction:: petl.transform.reductions.groupcountdistinctvalues
.. autofunction:: petl.transform.reductions.groupselectfirst
.. autofunction:: petl.transform.reductions.groupselectlast
.. autofunction:: petl.transform.reductions.groupselectmin
.. autofunction:: petl.transform.reductions.groupselectmax


.. module:: petl.transform.reshape
.. _transform_reshape:

Reshaping tables
----------------

.. autofunction:: petl.transform.reshape.melt
.. autofunction:: petl.transform.reshape.recast
.. autofunction:: petl.transform.reshape.transpose
.. autofunction:: petl.transform.reshape.pivot
.. autofunction:: petl.transform.reshape.flatten
.. autofunction:: petl.transform.reshape.unflatten


.. module:: petl.transform.fills
.. _transform_fills:

Filling missing values
----------------------

.. autofunction:: petl.transform.fills.filldown
.. autofunction:: petl.transform.fills.fillright
.. autofunction:: petl.transform.fills.fillleft


.. module:: petl.transform.validation
.. _transform_validation:

Validation
----------

.. autofunction:: petl.transform.validation.validate


.. module:: petl.transform.intervals
.. _transform_intervals:

Intervals (intervaltree)
------------------------

.. note::

    The following functions require the package `intervaltree
    <https://github.com/chaimleib/intervaltree>`_ to be installed, e.g.::

        $ pip install intervaltree

.. autofunction:: petl.transform.intervals.intervaljoin
.. autofunction:: petl.transform.intervals.intervalleftjoin
.. autofunction:: petl.transform.intervals.intervaljoinvalues
.. autofunction:: petl.transform.intervals.intervalantijoin
.. autofunction:: petl.transform.intervals.intervallookup
.. autofunction:: petl.transform.intervals.intervallookupone
.. autofunction:: petl.transform.intervals.intervalrecordlookup
.. autofunction:: petl.transform.intervals.intervalrecordlookupone
.. autofunction:: petl.transform.intervals.facetintervallookup
.. autofunction:: petl.transform.intervals.facetintervallookupone
.. autofunction:: petl.transform.intervals.facetintervalrecordlookup
.. autofunction:: petl.transform.intervals.facetintervalrecordlookupone
.. autofunction:: petl.transform.intervals.intervalsubtract
.. autofunction:: petl.transform.intervals.collapsedintervals

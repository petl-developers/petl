.. module:: petl.transform

Transform - transforming tables
===============================


Basic transformations
---------------------

.. autofunction:: petl.transform.basics.head
.. autofunction:: petl.transform.basics.tail
.. autofunction:: petl.transform.basics.rowslice
.. autofunction:: petl.transform.basics.cut
.. autofunction:: petl.transform.basics.cutout
.. autofunction:: petl.transform.basics.movefield
.. autofunction:: petl.transform.basics.cat
.. autofunction:: petl.transform.basics.skipcomments
.. autofunction:: petl.transform.basics.addfield
.. autofunction:: petl.transform.basics.addcolumn
.. autofunction:: petl.transform.basics.addrownumbers
.. autofunction:: petl.transform.basics.addfieldusingcontext
.. autofunction:: petl.transform.basics.annex


Header manipulations
--------------------

.. autofunction:: petl.transform.headers.rename
.. autofunction:: petl.transform.headers.setheader
.. autofunction:: petl.transform.headers.extendheader
.. autofunction:: petl.transform.headers.pushheader
.. autofunction:: petl.transform.headers.prefixheader
.. autofunction:: petl.transform.headers.suffixheader
.. autofunction:: petl.transform.headers.skip


Converting values
-----------------

.. autofunction:: petl.transform.conversions.convert
.. autofunction:: petl.transform.conversions.convertall
.. autofunction:: petl.transform.conversions.convertnumbers
.. autofunction:: petl.transform.conversions.replace
.. autofunction:: petl.transform.conversions.replaceall
.. autofunction:: petl.transform.conversions.update


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
.. autofunction:: petl.transform.selects.selectre
.. autofunction:: petl.transform.selects.selecttrue
.. autofunction:: petl.transform.selects.selectfalse
.. autofunction:: petl.transform.selects.selectnone
.. autofunction:: petl.transform.selects.selectnotnone
.. autofunction:: petl.transform.selects.selectusingcontext
.. autofunction:: petl.transform.selects.rowlenselect
.. autofunction:: petl.transform.selects.facet


Regular expressions
-------------------

.. autofunction:: petl.transform.regex.search
.. autofunction:: petl.transform.regex.searchcomplement
.. autofunction:: petl.transform.regex.sub
.. autofunction:: petl.transform.regex.split
.. autofunction:: petl.transform.regex.capture


Unpacking compound values
-------------------------

.. autofunction:: petl.transform.unpacks.unpack
.. autofunction:: petl.transform.unpacks.unpackdict


Transforming rows
-----------------

.. autofunction:: petl.transform.maps.fieldmap
.. autofunction:: petl.transform.maps.rowmap
.. autofunction:: petl.transform.maps.rowmapmany
.. autofunction:: petl.transform.maps.rowgroupmap


Sorting
-------

.. autofunction:: petl.transform.sorts.sort
.. autofunction:: petl.transform.sorts.mergesort
.. autofunction:: petl.transform.sorts.issorted


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


Set operations
--------------

.. autofunction:: petl.transform.setops.complement
.. autofunction:: petl.transform.setops.diff
.. autofunction:: petl.transform.setops.recordcomplement
.. autofunction:: petl.transform.setops.recorddiff
.. autofunction:: petl.transform.setops.intersection
.. autofunction:: petl.transform.setops.hashcomplement
.. autofunction:: petl.transform.setops.hashintersection


Deduplicating rows
------------------

.. autofunction:: petl.transform.dedup.duplicates
.. autofunction:: petl.transform.dedup.unique
.. autofunction:: petl.transform.dedup.conflicts
.. autofunction:: petl.transform.dedup.distinct
.. autofunction:: petl.transform.dedup.isunique


Reducing rows
-------------

.. autofunction:: petl.transform.reductions.aggregate
.. autofunction:: petl.transform.reductions.rowreduce
.. autofunction:: petl.transform.reductions.mergeduplicates
.. autofunction:: petl.transform.reductions.merge
.. autofunction:: petl.transform.reductions.fold
.. autofunction:: petl.transform.reductions.groupcountdistinctvalues
.. autofunction:: petl.transform.reductions.groupselectfirst
.. autofunction:: petl.transform.reductions.groupselectmin
.. autofunction:: petl.transform.reductions.groupselectmax


Reshaping tables
----------------

.. autofunction:: petl.transform.reshape.melt
.. autofunction:: petl.transform.reshape.recast
.. autofunction:: petl.transform.reshape.transpose
.. autofunction:: petl.transform.reshape.pivot
.. autofunction:: petl.transform.reshape.flatten
.. autofunction:: petl.transform.reshape.unflatten


Filling missing values
----------------------

.. autofunction:: petl.transform.fills.filldown
.. autofunction:: petl.transform.fills.fillright
.. autofunction:: petl.transform.fills.fillleft

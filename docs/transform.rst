.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Transform - transforming tables
===============================

Basic transformations
---------------------

.. autofunction:: petl.head
.. autofunction:: petl.tail
.. autofunction:: petl.rowslice
.. autofunction:: petl.cut
.. autofunction:: petl.cutout
.. autofunction:: petl.movefield
.. autofunction:: petl.cat
.. autofunction:: petl.skipcomments
.. autofunction:: petl.addfield
.. autofunction:: petl.addcolumn
.. autofunction:: petl.addrownumbers
.. autofunction:: petl.addfieldusingcontext
.. autofunction:: petl.annex

Header manipulations
--------------------

.. autofunction:: petl.rename
.. autofunction:: petl.setheader
.. autofunction:: petl.extendheader
.. autofunction:: petl.pushheader
.. autofunction:: petl.prefixheader
.. autofunction:: petl.suffixheader
.. autofunction:: petl.skip

Converting values
-----------------

.. autofunction:: petl.convert
.. autofunction:: petl.convertall
.. autofunction:: petl.convertnumbers
.. autofunction:: petl.replace
.. autofunction:: petl.replaceall
.. autofunction:: petl.update
.. autofunction:: petl.fieldconvert

Selecting rows
--------------

.. autofunction:: petl.select
.. autofunction:: petl.selectop
.. autofunction:: petl.selecteq
.. autofunction:: petl.selectne
.. autofunction:: petl.selectlt
.. autofunction:: petl.selectle
.. autofunction:: petl.selectgt
.. autofunction:: petl.selectge
.. autofunction:: petl.selectrangeopen
.. autofunction:: petl.selectrangeopenleft
.. autofunction:: petl.selectrangeopenright
.. autofunction:: petl.selectrangeclosed
.. autofunction:: petl.selectcontains
.. autofunction:: petl.selectin
.. autofunction:: petl.selectnotin
.. autofunction:: petl.selectis
.. autofunction:: petl.selectisnot
.. autofunction:: petl.selectisinstance
.. autofunction:: petl.selectre
.. autofunction:: petl.selecttrue
.. autofunction:: petl.selectfalse
.. autofunction:: petl.selectnone
.. autofunction:: petl.selectnotnone
.. autofunction:: petl.selectusingcontext
.. autofunction:: petl.rowselect
.. autofunction:: petl.recordselect
.. autofunction:: petl.rowlenselect
.. autofunction:: petl.fieldselect
.. autofunction:: petl.facet
.. autofunction:: petl.rangefacet

Regular expressions
-------------------

.. autofunction:: petl.search
.. autofunction:: petl.sub
.. autofunction:: petl.split
.. autofunction:: petl.capture

Deduplicating rows
-------------------

.. autofunction:: petl.duplicates
.. autofunction:: petl.unique
.. autofunction:: petl.conflicts
.. autofunction:: petl.distinct

Unpacking compound values
-------------------------

.. autofunction:: petl.unpack
.. autofunction:: petl.unpackdict

Transforming rows
-----------------

.. autofunction:: petl.fieldmap
.. autofunction:: petl.rowmap
.. autofunction:: petl.recordmap
.. autofunction:: petl.rowmapmany
.. autofunction:: petl.recordmapmany
.. autofunction:: petl.rowgroupmap

Sorting
-------

.. autofunction:: petl.sort
.. autofunction:: petl.mergesort

Joins
-----

.. autofunction:: petl.join
.. autofunction:: petl.leftjoin
.. autofunction:: petl.lookupjoin
.. autofunction:: petl.rightjoin
.. autofunction:: petl.outerjoin
.. autofunction:: petl.crossjoin
.. autofunction:: petl.antijoin
.. autofunction:: petl.unjoin
.. autofunction:: petl.hashjoin
.. autofunction:: petl.hashleftjoin
.. autofunction:: petl.hashlookupjoin
.. autofunction:: petl.hashrightjoin
.. autofunction:: petl.hashantijoin

Set operations
--------------

.. autofunction:: petl.complement
.. autofunction:: petl.diff
.. autofunction:: petl.recordcomplement
.. autofunction:: petl.recorddiff
.. autofunction:: petl.intersection
.. autofunction:: petl.hashcomplement
.. autofunction:: petl.hashintersection

Reducing rows
-------------

.. autofunction:: petl.aggregate
.. autofunction:: petl.rangeaggregate
.. autofunction:: petl.rangecounts
.. autofunction:: petl.rowreduce
.. autofunction:: petl.recordreduce
.. autofunction:: petl.rangerowreduce
.. autofunction:: petl.rangerecordreduce
.. autofunction:: petl.mergeduplicates
.. autofunction:: petl.merge
.. autofunction:: petl.fold
.. autofunction:: petl.multirangeaggregate
.. autofunction:: petl.groupcountdistinctvalues
.. autofunction:: petl.groupselectfirst
.. autofunction:: petl.groupselectmin
.. autofunction:: petl.groupselectmax

Reshaping tables
----------------

.. autofunction:: petl.melt
.. autofunction:: petl.recast
.. autofunction:: petl.transpose
.. autofunction:: petl.pivot
.. autofunction:: petl.flatten
.. autofunction:: petl.unflatten

Filling missing values
----------------------

.. autofunction:: petl.filldown
.. autofunction:: petl.fillright
.. autofunction:: petl.fillleft



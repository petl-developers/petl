.. module:: petl.util

Utility functions
=================


Basic utilities
---------------

.. autofunction:: petl.util.base.header
.. autofunction:: petl.util.base.fieldnames
.. autofunction:: petl.util.base.data
.. autofunction:: petl.util.base.values
.. autofunction:: petl.util.base.dicts
.. autofunction:: petl.util.base.namedtuples
.. autofunction:: petl.util.base.records
.. autofunction:: petl.util.base.expr
.. autofunction:: petl.util.base.rowgroupby
.. autofunction:: petl.util.base.empty


Visualising tables
------------------

.. autofunction:: petl.util.vis.look
.. autofunction:: petl.util.vis.lookall
.. autofunction:: petl.util.vis.see
.. autofunction:: petl.util.vis.display
.. autofunction:: petl.util.vis.displayall


Lookup data structures
----------------------

.. autofunction:: petl.util.lookups.lookup
.. autofunction:: petl.util.lookups.lookupone
.. autofunction:: petl.util.lookups.dictlookup
.. autofunction:: petl.util.lookups.dictlookupone
.. autofunction:: petl.util.lookups.recordlookup
.. autofunction:: petl.util.lookups.recordlookupone


Parsing string/text values
--------------------------

.. autofunction:: petl.util.parsers.dateparser
.. autofunction:: petl.util.parsers.timeparser
.. autofunction:: petl.util.parsers.datetimeparser
.. autofunction:: petl.util.parsers.boolparser
.. autofunction:: petl.util.parsers.numparser


Counting
--------

.. autofunction:: petl.util.counting.nrows
.. autofunction:: petl.util.counting.valuecount
.. autofunction:: petl.util.counting.valuecounter
.. autofunction:: petl.util.counting.valuecounts
.. autofunction:: petl.util.counting.stringpatterncounter
.. autofunction:: petl.util.counting.stringpatterns
.. autofunction:: petl.util.counting.rowlengths
.. autofunction:: petl.util.counting.typecounter
.. autofunction:: petl.util.counting.typecounts
.. autofunction:: petl.util.counting.parsecounter
.. autofunction:: petl.util.counting.parsecounts


Timing
------

.. autofunction:: petl.util.timing.progress
.. autofunction:: petl.util.timing.log_progress
.. autofunction:: petl.util.timing.clock


Statistics
----------

.. autofunction:: petl.util.statistics.limits
.. autofunction:: petl.util.statistics.stats


Materialising tables
--------------------

.. autofunction:: petl.util.materialise.columns
.. autofunction:: petl.util.materialise.facetcolumns
.. autofunction:: petl.util.materialise.listoflists
.. autofunction:: petl.util.materialise.listoftuples
.. autofunction:: petl.util.materialise.tupleoflists
.. autofunction:: petl.util.materialise.tupleoftuples
.. autofunction:: petl.util.materialise.cache


Randomly generated tables
-------------------------

.. autofunction:: petl.util.random.randomtable
.. autofunction:: petl.util.random.dummytable


Miscellaneous
-------------

.. autofunction:: petl.util.misc.typeset
.. autofunction:: petl.util.misc.diffheaders
.. autofunction:: petl.util.misc.diffvalues
.. autofunction:: petl.util.misc.strjoin
.. autofunction:: petl.util.misc.nthword
.. autofunction:: petl.util.misc.coalesce

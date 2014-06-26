.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Load - writing tables to files and databases
============================================

The following functions write data from a table to a file-like source
or database. For functions that accept a ``source`` argument, if the
``source`` argument is ``None`` or a string it is interpreted as
follows:

* ``None`` - write to stdout
* string ending with '.gz' or '.bgz' - write to file via gzip decompression
* string ending with '.bz2' - write to file via bz2 decompression
* any other string - write directly to file

Some helper classes are also available for writing to other types of
file-like sources, e.g., writing to a Zip file or string buffer, see
the section on `I/O helper classes <iohelp.html>`_ for more information.

Delimited files
---------------

.. autofunction:: petl.tocsv
.. autofunction:: petl.appendcsv
.. autofunction:: petl.totsv
.. autofunction:: petl.appendtsv
.. autofunction:: petl.toucsv
.. autofunction:: petl.appenducsv
.. autofunction:: petl.toutsv
.. autofunction:: petl.appendutsv

Pickle files
------------

.. autofunction:: petl.topickle
.. autofunction:: petl.appendpickle

Text files
----------

.. autofunction:: petl.totext
.. autofunction:: petl.appendtext
.. autofunction:: petl.toutext
.. autofunction:: petl.appendutext

JSON files
----------

.. autofunction:: petl.tojson
.. autofunction:: petl.tojsonarrays

HTML files
----------

.. autofunction:: petl.tohtml
.. autofunction:: petl.touhtml

Databases
---------

.. autofunction:: petl.tosqlite3
.. autofunction:: petl.appendsqlite3
.. autofunction:: petl.todb
.. autofunction:: petl.appenddb


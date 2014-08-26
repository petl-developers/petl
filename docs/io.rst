.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Extract/Load - reading/writing tables from files, databases and other sources
=============================================================================

Extract (read)
--------------

The "from..." functions read a table from a file-like source or
database. For everything except :func:`fromdb` the ``source`` argument
provides information about where to read the underlying data from. If
the ``source`` argument is ``None`` or a string it is interpreted as
follows:

* ``None`` - read from stdin
* string starting with 'http://', 'https://' or 'ftp://' - read from URL
* string ending with '.gz' or '.bgz' - read from file via gzip decompression
* string ending with '.bz2' - read from file via bz2 decompression
* any other string - read directly from file

Some helper classes are also available for reading from other types of file-like
sources, e.g., reading data from a Zip file, a string or a subprocess,
see the section on I/O helper classes below for more information.

Load (write)
------------

The "to..." functions write data from a table to a file-like source
or database. For functions that accept a ``source`` argument, if the
``source`` argument is ``None`` or a string it is interpreted as
follows:

* ``None`` - write to stdout
* string ending with '.gz' or '.bgz' - write to file via gzip decompression
* string ending with '.bz2' - write to file via bz2 decompression
* any other string - write directly to file

Some helper classes are also available for writing to other types of
file-like sources, e.g., writing to a Zip file or string buffer, see
the section on I/O helper classes below for more information.

Delimited files
---------------

.. autofunction:: petl.fromcsv
.. autofunction:: petl.tocsv
.. autofunction:: petl.appendcsv
.. autofunction:: petl.teecsv
.. autofunction:: petl.fromtsv
.. autofunction:: petl.totsv
.. autofunction:: petl.appendtsv
.. autofunction:: petl.teetsv
.. autofunction:: petl.fromucsv
.. autofunction:: petl.toucsv
.. autofunction:: petl.appenducsv
.. autofunction:: petl.teeucsv
.. autofunction:: petl.fromutsv
.. autofunction:: petl.toutsv
.. autofunction:: petl.appendutsv
.. autofunction:: petl.teeutsv

Pickle files
------------

.. autofunction:: petl.frompickle
.. autofunction:: petl.topickle
.. autofunction:: petl.appendpickle
.. autofunction:: petl.teepickle

Text files
----------

.. autofunction:: petl.fromtext
.. autofunction:: petl.totext
.. autofunction:: petl.appendtext
.. autofunction:: petl.teetext
.. autofunction:: petl.fromutext
.. autofunction:: petl.toutext
.. autofunction:: petl.appendutext
.. autofunction:: petl.teeutext

XML files
---------

.. autofunction:: petl.fromxml

For writing to an XML file, see :func:`petl.totext`.

HTML files
----------

.. autofunction:: petl.tohtml
.. autofunction:: petl.teehtml
.. autofunction:: petl.touhtml
.. autofunction:: petl.teeuhtml

JSON files
----------

.. autofunction:: petl.fromjson
.. autofunction:: petl.fromdicts
.. autofunction:: petl.tojson
.. autofunction:: petl.tojsonarrays

Databases
---------
.. autofunction:: petl.fromsqlite3
.. autofunction:: petl.tosqlite3
.. autofunction:: petl.appendsqlite3
.. autofunction:: petl.fromdb
.. autofunction:: petl.todb
.. autofunction:: petl.appenddb

I/O helper classes
------------------

The following classes are helpers for extract (``from...()``) and load
(``to...()``) functions that use a file-like data source. An instance
of any of the following classes can be used as the ``source`` argument
to data extraction functions like :func:`fromcsv` etc., with the
exception of :class:`StdoutSource` which is write-only. An instance of
any of the following classes can also be used as the ``source``
argument to data loading functions like :func:`tocsv` etc., with the
exception of :class:`StdinSource`, :class:`URLSource` and
:class:`PopenSource` which are read-only. The behaviour of each source
can usually be configured by passing arguments to the constructor, see
the source code of the :mod:`petl.io` module for full details.

.. autoclass:: petl.FileSource
.. autoclass:: petl.GzipSource
.. autoclass:: petl.BZ2Source
.. autoclass:: petl.ZipSource
.. autoclass:: petl.StdinSource
.. autoclass:: petl.StdoutSource
.. autoclass:: petl.URLSource
.. autoclass:: petl.StringSource
.. autoclass:: petl.PopenSource







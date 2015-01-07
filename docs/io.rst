.. module:: petl.io

Extract/Load - reading/writing tables from files, databases and other sources
=============================================================================


Extract (read)
--------------

The "from..." functions read a table from a file-like source or
database. For everything except :func:`petl.io.db.fromdb` the ``source``
argument provides information about where to read the underlying data from. If
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

.. autofunction:: petl.io.csv.fromcsv
.. autofunction:: petl.io.csv.tocsv
.. autofunction:: petl.io.csv.appendcsv
.. autofunction:: petl.io.csv.teecsv
.. autofunction:: petl.io.csv.fromtsv
.. autofunction:: petl.io.csv.totsv
.. autofunction:: petl.io.csv.appendtsv
.. autofunction:: petl.io.csv.teetsv
.. autofunction:: petl.io.csv.fromucsv
.. autofunction:: petl.io.csv.toucsv
.. autofunction:: petl.io.csv.appenducsv
.. autofunction:: petl.io.csv.teeucsv
.. autofunction:: petl.io.csv.fromutsv
.. autofunction:: petl.io.csv.toutsv
.. autofunction:: petl.io.csv.appendutsv
.. autofunction:: petl.io.csv.teeutsv


Pickle files
------------

.. autofunction:: petl.io.pickle.frompickle
.. autofunction:: petl.io.pickle.topickle
.. autofunction:: petl.io.pickle.appendpickle
.. autofunction:: petl.io.pickle.teepickle


Text files
----------

.. autofunction:: petl.io.text.fromtext
.. autofunction:: petl.io.text.totext
.. autofunction:: petl.io.text.appendtext
.. autofunction:: petl.io.text.teetext
.. autofunction:: petl.io.text.fromutext
.. autofunction:: petl.io.text.toutext
.. autofunction:: petl.io.text.appendutext
.. autofunction:: petl.io.text.teeutext


XML files
---------

.. autofunction:: petl.io.xml.fromxml

For writing to an XML file, see :func:`petl.io.text.totext`.


HTML files
----------

.. autofunction:: petl.io.html.tohtml
.. autofunction:: petl.io.html.teehtml
.. autofunction:: petl.io.html.touhtml
.. autofunction:: petl.io.html.teeuhtml


JSON files
----------

.. autofunction:: petl.io.json.fromjson
.. autofunction:: petl.io.json.fromdicts
.. autofunction:: petl.io.json.tojson
.. autofunction:: petl.io.json.tojsonarrays


Databases
---------
.. autofunction:: petl.io.sqlite3.fromsqlite3
.. autofunction:: petl.io.sqlite3.tosqlite3
.. autofunction:: petl.io.sqlite3.appendsqlite3
.. autofunction:: petl.io.db.fromdb
.. autofunction:: petl.io.db.todb
.. autofunction:: petl.io.db.appenddb


I/O helper classes
------------------

The following classes are helpers for extract (``from...()``) and load
(``to...()``) functions that use a file-like data source. An instance
of any of the following classes can be used as the ``source`` argument
to data extraction functions like :func:`petl.io.csv.fromcsv` etc., with the
exception of :class:`petl.io.sources.StdoutSource` which is write-only. An
instance of
any of the following classes can also be used as the ``source``
argument to data loading functions like :func:`tocsv` etc., with the
exception of :class:`petl.io.sources.StdinSource`,
:class:`petl.io.sources.URLSource` and :class:`petl.io.sources.PopenSource`
which are read-only. The behaviour of each source can usually be configured
by passing arguments to the constructor, see the source code of the
:mod:`petl.io.sources` module for full details.

.. autoclass:: petl.io.sources.FileSource
.. autoclass:: petl.io.sources.GzipSource
.. autoclass:: petl.io.sources.BZ2Source
.. autoclass:: petl.io.sources.ZipSource
.. autoclass:: petl.io.sources.StdinSource
.. autoclass:: petl.io.sources.StdoutSource
.. autoclass:: petl.io.sources.URLSource
.. autoclass:: petl.io.sources.MemorySource
.. autoclass:: petl.io.sources.PopenSource

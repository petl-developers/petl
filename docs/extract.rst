.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Extract - reading tables from files, databases and other sources
================================================================

The following functions extract a table from a file-like source or
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
see the section on `I/O helper classes <iohelp.html>`_ for more information.

Delimited files
---------------

.. autofunction:: petl.fromcsv
.. autofunction:: petl.fromtsv
.. autofunction:: petl.fromucsv
.. autofunction:: petl.fromutsv

Pickle files
------------

.. autofunction:: petl.frompickle

Text files
----------

.. autofunction:: petl.fromtext
.. autofunction:: petl.fromutext

XML files
---------

.. autofunction:: petl.fromxml

JSON files
----------

.. autofunction:: petl.fromjson
.. autofunction:: petl.fromdicts

Databases
---------
.. autofunction:: petl.fromsqlite3
.. autofunction:: petl.fromdb



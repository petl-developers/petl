.. module:: petl.io

Extract/Load - reading/writing tables from files, databases and other sources
=============================================================================

.. _io_extract:

Extract (read)
--------------

The "from..." functions extract a table from a file-like source or
database. For everything except :func:`petl.io.db.fromdb` the
``source`` argument provides information about where to extract the
underlying data from. If the ``source`` argument is ``None`` or a
string it is interpreted as follows:

* ``None`` - read from stdin
* string starting with `http://`, `https://` or `ftp://` - read from URL
* string ending with `.gz` or `.bgz` - read from file via gzip decompression
* string ending with `.bz2` - read from file via bz2 decompression
* any other string - read directly from file

Some helper classes are also available for reading from other types of
file-like sources, e.g., reading data from a Zip file, a string or a
subprocess, see the section on :ref:`io_helpers` below for more
information.

Be aware that loading data from stdin breaks the table container
convention, because data can usually only be read once. If you are
sure that data will only be read once in your script or interactive
session then this may not be a problem, however note that some
:mod:`petl` functions do access the underlying data source more than
once and so will not work as expected with data from stdin.

.. _io_load:

Load (write)
------------

The "to..." functions load data from a table into a file-like source
or database. For functions that accept a ``source`` argument, if the
``source`` argument is ``None`` or a string it is interpreted as
follows:

* ``None`` - write to stdout
* string ending with `.gz` or `.bgz` - write to file via gzip decompression
* string ending with `.bz2` - write to file via bz2 decompression
* any other string - write directly to file

Some helper classes are also available for writing to other types of
file-like sources, e.g., writing to a Zip file or string buffer, see
the section on :ref:`io_helpers` below for more information.

.. module:: petl.io.csv
.. _io_csv:

Python objects
--------------

.. autofunction:: petl.io.base.fromcolumns

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


.. module:: petl.io.pickle
.. _io_pickle:

Pickle files
------------

.. autofunction:: petl.io.pickle.frompickle
.. autofunction:: petl.io.pickle.topickle
.. autofunction:: petl.io.pickle.appendpickle
.. autofunction:: petl.io.pickle.teepickle


.. module:: petl.io.text
.. _io_text:

Text files
----------

.. autofunction:: petl.io.text.fromtext
.. autofunction:: petl.io.text.totext
.. autofunction:: petl.io.text.appendtext
.. autofunction:: petl.io.text.teetext


.. module:: petl.io.xml
.. _io_xml:

XML files
---------

.. autofunction:: petl.io.xml.fromxml

For writing to an XML file, see :func:`petl.io.text.totext`.


.. module:: petl.io.html
.. _io_html:

HTML files
----------

.. autofunction:: petl.io.html.tohtml
.. autofunction:: petl.io.html.teehtml


.. module:: petl.io.json
.. _io_json:

JSON files
----------

.. autofunction:: petl.io.json.fromjson
.. autofunction:: petl.io.json.fromdicts
.. autofunction:: petl.io.json.tojson
.. autofunction:: petl.io.json.tojsonarrays


.. module:: petl.io.db
.. _io_db:

Databases
---------

.. note::

    The automatic table creation feature of :func:`petl.io.db.todb`
    requires `SQLAlchemy <http://www.sqlalchemy.org/>`_ to be installed, e.g.::

        $ pip install sqlalchemy


.. autofunction:: petl.io.db.fromdb
.. autofunction:: petl.io.db.todb
.. autofunction:: petl.io.db.appenddb


.. module:: petl.io.xls
.. _io_xls:

Excel .xls files (xlrd/xlwt)
----------------------------

.. note::

    The following functions require `xlrd
    <https://pypi.python.org/pypi/xlrd>`_ and `xlwt
    <https://pypi.python.org/pypi/xlwt-future>`_ to be installed,
    e.g.::

        $ pip install xlrd xlwt-future

.. autofunction:: petl.io.xls.fromxls
.. autofunction:: petl.io.xls.toxls


.. module:: petl.io.xlsx
.. _io_xlsx:

Excel .xlsx files (openpyxl)
----------------------------

.. note::

    The following functions require `openpyxl
    <https://bitbucket.org/ericgazoni/openpyxl/wiki/Home>`_ to be
    installed, e.g.::

        $ pip install openpyxl

.. autofunction:: petl.io.xlsx.fromxlsx
.. autofunction:: petl.io.xlsx.toxlsx
.. autofunction:: petl.io.xlsx.appendxlsx


.. module:: petl.io.numpy
.. _io_numpy:

Arrays (NumPy)
--------------

.. note::

    The following functions require `numpy <http://www.numpy.org/>`_
    to be installed, e.g.::

        $ pip install numpy

.. autofunction:: petl.io.numpy.fromarray
.. autofunction:: petl.io.numpy.toarray
.. autofunction:: petl.io.numpy.torecarray
.. autofunction:: petl.io.numpy.valuestoarray


.. module:: petl.io.pandas
.. _io_pandas:

DataFrames (pandas)
-------------------

.. note::

    The following functions require `pandas
    <http://pandas.pydata.org/>`_ to be installed, e.g.::

        $ pip install pandas

.. autofunction:: petl.io.pandas.fromdataframe
.. autofunction:: petl.io.pandas.todataframe


.. module:: petl.io.pytables
.. _io_pytables:

HDF5 files (PyTables)
---------------------

.. note::

    The following functions require `PyTables
    <https://pytables.github.io/index.html>`_ to be installed, e.g.::

        $ # install HDF5
        $ apt-get install libhdf5-7 libhdf5-dev
        $ # install other prerequisites
        $ pip install cython
        $ pip install numpy
        $ pip install numexpr
        $ # install PyTables
        $ pip install tables

.. autofunction:: petl.io.pytables.fromhdf5
.. autofunction:: petl.io.pytables.fromhdf5sorted
.. autofunction:: petl.io.pytables.tohdf5
.. autofunction:: petl.io.pytables.appendhdf5


.. module:: petl.io.bcolz
.. _io_bcolz:

Bcolz ctables
-------------

.. note::

    The following functions require `bcolz <http://bcolz.blosc.org>`_
    to be installed, e.g.::

        $ pip install bcolz

.. autofunction:: petl.io.bcolz.frombcolz
.. autofunction:: petl.io.bcolz.tobcolz
.. autofunction:: petl.io.bcolz.appendbcolz

.. module:: petl.io.whoosh
.. _io_whoosh:

Text indexes (Whoosh)
---------------------

.. note::

    The following functions require
    `Whoosh <https://pypi.python.org/pypi/Whoosh/>`_
    to be installed, e.g.::

        $ pip install whoosh

.. autofunction:: petl.io.whoosh.fromtextindex
.. autofunction:: petl.io.whoosh.searchtextindex
.. autofunction:: petl.io.whoosh.searchtextindexpage
.. autofunction:: petl.io.whoosh.totextindex
.. autofunction:: petl.io.whoosh.appendtextindex

.. module:: petl.io.sources
.. _io_helpers:

I/O helper classes
------------------

The following classes are helpers for extract (``from...()``) and load
(``to...()``) functions that use a file-like data source.

An instance of any of the following classes can be used as the ``source``
argument to data extraction functions like :func:`petl.io.csv.fromcsv` etc.,
with the exception of :class:`petl.io.sources.StdoutSource` which is
write-only.

An instance of any of the following classes can also be used as the ``source``
argument to data loading functions like :func:`petl.io.csv.tocsv` etc., with the
exception of :class:`petl.io.sources.StdinSource`,
:class:`petl.io.sources.URLSource` and :class:`petl.io.sources.PopenSource`
which are read-only.

The behaviour of each source can usually be configured by passing arguments
to the constructor, see the source code of the :mod:`petl.io.sources` module
for full details.

.. autoclass:: petl.io.sources.FileSource
.. autoclass:: petl.io.sources.GzipSource
.. autoclass:: petl.io.sources.BZ2Source
.. autoclass:: petl.io.sources.ZipSource
.. autoclass:: petl.io.sources.StdinSource
.. autoclass:: petl.io.sources.StdoutSource
.. autoclass:: petl.io.sources.URLSource
.. autoclass:: petl.io.sources.MemorySource
.. autoclass:: petl.io.sources.PopenSource

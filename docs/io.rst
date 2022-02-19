.. module:: petl.io
.. _io_usage:

Usage - reading/writing tables
==============================

`petl` uses simple python functions for providing a rows and columns abstraction
for reading and writing data from files, databases, and other sources.

The main features that `petl` was designed are:

- Pure python implementation based on `streams <https://docs.python.org/3/library/io.html>`,
  `iterators <https://docs.python.org/3/library/stdtypes.html?highlight=iterator#iterator-types>`
  , and other python types.
- Extensible approach, only requiring package dependencies when using their 
  functionality.
- Use a Dataframe/Table like paradigm similar of Pandas, R, and others
- Lightweight alternative to develop and maintain compared to heavier, 
  full-featured frameworks, like PySpark, PyArrow and other ETL tools.

.. _io_overview:

Brief Overview
--------------

.. _io_extract:

Extract (read)
^^^^^^^^^^^^^^

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

.. _io_extract_codec:

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
^^^^^^^^^^^^

The "to..." functions load data from a table into a file-like source
or database. For functions that accept a ``source`` argument, if the
``source`` argument is ``None`` or a string it is interpreted as
follows:

* ``None`` - write to stdout
* string ending with `.gz` or `.bgz` - write to file via gzip decompression
* string ending with `.bz2` - write to file via bz2 decompression
* any other string - write directly to file

.. _io_load_codec:

Some helper classes are also available for writing to other types of
file-like sources, e.g., writing to a Zip file or string buffer, see
the section on :ref:`io_helpers` below for more information.

.. _io_builtin_formats:

Built-in File Formats
---------------------

.. module:: petl.io.csv
.. _io_csv:

Python objects
^^^^^^^^^^^^^^

.. autofunction:: petl.io.base.fromcolumns

Delimited files
^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^

.. autofunction:: petl.io.pickle.frompickle
.. autofunction:: petl.io.pickle.topickle
.. autofunction:: petl.io.pickle.appendpickle
.. autofunction:: petl.io.pickle.teepickle


.. module:: petl.io.text
.. _io_text:

Text files
^^^^^^^^^^

.. autofunction:: petl.io.text.fromtext
.. autofunction:: petl.io.text.totext
.. autofunction:: petl.io.text.appendtext
.. autofunction:: petl.io.text.teetext


.. module:: petl.io.xml
.. _io_xml:

XML files
^^^^^^^^^

.. autofunction:: petl.io.xml.fromxml
.. autofunction:: petl.io.xml.toxml


.. module:: petl.io.html
.. _io_html:

HTML files
^^^^^^^^^^

.. autofunction:: petl.io.html.tohtml
.. autofunction:: petl.io.html.teehtml


.. module:: petl.io.json
.. _io_json:

JSON files
^^^^^^^^^^

.. autofunction:: petl.io.json.fromjson
.. autofunction:: petl.io.json.fromdicts
.. autofunction:: petl.io.json.tojson
.. autofunction:: petl.io.json.tojsonarrays

.. module:: petl.io.streams
.. _io_helpers:

Python I/O streams
^^^^^^^^^^^^^^^^^^

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

.. autoclass:: petl.io.sources.StdinSource
.. autoclass:: petl.io.sources.StdoutSource
.. autoclass:: petl.io.sources.MemorySource
.. autoclass:: petl.io.sources.PopenSource

.. module:: petl.io.register
.. _io_register:

Custom I/O streams
^^^^^^^^^^^^^^^^^^

For creating custom helpers for :ref:`remote I/O <io_remotes>` or
`compression` use the following functions:

.. autofunction:: petl.io.sources.register_reader
.. autofunction:: petl.io.sources.register_writer
.. autofunction:: petl.io.sources.get_reader
.. autofunction:: petl.io.sources.get_writer

See the source code of the classes in :mod:`petl.io.sources` module for
more details.

.. _io_extended_formats:

Supported File Formats
----------------------

.. module:: petl.io.xls
.. _io_xls:

Excel .xls files (xlrd/xlwt)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^

.. note::

    The following functions require `pandas
    <http://pandas.pydata.org/>`_ to be installed, e.g.::

        $ pip install pandas

.. autofunction:: petl.io.pandas.fromdataframe
.. autofunction:: petl.io.pandas.todataframe


.. module:: petl.io.pytables
.. _io_pytables:

HDF5 files (PyTables)
^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^

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

.. module:: petl.io.avro
.. _io_avro:

Avro files (fastavro)
^^^^^^^^^^^^^^^^^^^^^

.. note::

    The following functions require `fastavro
    <https://github.com/fastavro/fastavro>`_ to be
    installed, e.g.::

        $ pip install fastavro

.. autofunction:: petl.io.avro.fromavro
.. autofunction:: petl.io.avro.toavro
.. autofunction:: petl.io.avro.appendavro

.. literalinclude:: ../petl/test/io/test_avro_schemas.py
   :name: logical_schema
   :language: python
   :caption: Avro schema for logical types 
   :start-after: begin_logical_schema
   :end-before: end_logical_schema

.. literalinclude:: ../petl/test/io/test_avro_schemas.py
   :name: nullable_schema
   :language: python
   :caption: Avro schema with nullable fields
   :start-after: begin_nullable_schema
   :end-before: end_nullable_schema

.. literalinclude:: ../petl/test/io/test_avro_schemas.py
   :name: array_schema
   :language: python
   :caption: Avro schema with array values in fields
   :start-after: begin_array_schema
   :end-before: end_array_schema

.. literalinclude:: ../petl/test/io/test_avro_schemas.py
   :name: complex_schema
   :language: python
   :caption: Example of recursive complex Avro schema
   :start-after: begin_complex_schema
   :end-before: end_complex_schema

.. module:: petl.io.gsheet
.. _io_gsheet:

Google Sheets (gspread)
^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    This is a experimental feature. API and behavior may change between releases
    with some possible breaking changes.

.. note::

    The following functions require `gspread
    <https://github.com/burnash/gspread>`_  to be installed,
    e.g.::

        $ pip install gspread

.. autofunction:: petl.io.gsheet.fromgsheet
.. autofunction:: petl.io.gsheet.togsheet
.. autofunction:: petl.io.gsheet.appendgsheet

.. module:: petl.io.db
.. _io_db:

Databases
---------

.. note::

    For reading and writing to databases, the following functions require
    `SQLAlchemy <http://www.sqlalchemy.org/>` and the database specific driver
    to be installed along petl, e.g.::

        $ pip install sqlalchemy
        $ pip install sqlite3
        $ pip install pymysql

.. autofunction:: petl.io.db.fromdb
.. autofunction:: petl.io.db.todb
.. autofunction:: petl.io.db.appenddb

.. module:: petl.io.remote
.. _io_remotes:

Remote and Cloud Filesystems
----------------------------

The following classes are helpers for reading (``from...()``) and writing
(``to...()``) functions transparently as a file-like source.

There are no need to instantiate them. They are used in the mecanism described
in :ref:`Extract <io_extract>` and :ref:`Load <io_load>`.

It's possible to read and write just by prefixing the protocol (e.g: `s3://`)
in the source path of the file.

.. note::

    For reading and writing to remote filesystems, the following functions 
    requires `fsspec <https://filesystem-spec.readthedocs.io/>` to be installed 
    along petl package e.g.::

        $ pip install fsspec

The supported filesystems with their URI formats can be found in fsspec 
documentation:

- `Built-in Implementations <https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations>`__
- `Other Known Implementations <https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations>`__

Remote sources
^^^^^^^^^^^^^^

.. autoclass:: petl.io.remotes.RemoteSource
.. autoclass:: petl.io.remotes.SMBSource

.. _io_deprecated:

Deprecated I/O sources
^^^^^^^^^^^^^^^^^^^^^^

The following helpers are deprecated and will be removed in a future version.

It's functionality was replaced by helpers in :ref:`Remote helpers <io_remotes>`.

.. autoclass:: petl.io.sources.FileSource
.. autoclass:: petl.io.sources.GzipSource
.. autoclass:: petl.io.sources.BZ2Source
.. autoclass:: petl.io.sources.ZipSource
.. autoclass:: petl.io.sources.URLSource

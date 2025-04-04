Changes
=======

Version 1.7.16
--------------

* CI: added jobs for testing petl with python 3.13 and macos-13 on Intel platform
  By :user:`juarezr`, :issue:`675`.

* CI: workaround for actions/setup-python as Github removed support for python 3.7
  By :user:`juarezr`, :issue:`675`.

* Fix: Joining tables with uneven rows gives wrong result.
  By :user:`MichalKarol`.

Version 1.7.15
--------------

* Add unit tests for randomtable, dummytable, and their supporting functions and classes.
  By :user:`bmos`, :issue:`657`.

* Fix: DeprecationWarning: Seeding based on hashing is deprecated since Python 3.9 and will be removed in a subsequent version.
  By :user:`bmos`, :issue:`657`.

Version 1.7.14
--------------

* Enhancement: Fix other functions to conform with PEP 479
  By :user:`augustomen`, :issue:`645`.

* CI: fix build as SQLAlchemy 2 is not supported yet
  By :user:`juarezr`, :issue:`635`.

* CI: workaround for actions/setup-python#672 as Github removed python 2.7 and 3.6
  By :user:`juarezr`, :issue:`649`.

* CI: Gh actions upgrade
  By :user:`juarezr`, :issue:`639`.

Version 1.7.13
--------------

* Fix in case a custom protocol was registered in fsspec
  By :user:`timheb`, :issue:`647`.

Version 1.7.12
--------------

* Fix: calling functions to*() should output by default to stdout
  By :user:`juarezr`, :issue:`632`.

* Add python3.11 for the build and testing
  By :user:`juarezr`, :issue:`635`.

* Add support for writing to JSONL files
  By :user:`mzaeemz`, :issue:`524`.

Version 1.7.11
--------------

* Fix generator support in fromdicts to use file cache
  By :user:`arturponinski`, :issue:`625`.

Version 1.7.10
--------------

* Fix fromtsv() to pass on header argument
  By :user:`jfitzell`, :issue:`622`.

Version 1.7.9
-------------

* Feature: Add improved support for working with Google Sheets 
  By :user:`juarezr`, :issue:`615`.

* Maintanance: Improve test helpers testing
  By :user:`juarezr`, :issue:`614`.

Version 1.7.8
-------------

* Fix iterrowslice() to conform with PEP 479
  By :user:`arturponinski`, :issue:`575`.

* Cleanup and unclutter old and unused files in repository
  By :user:`juarezr`, :issue:`606`.

* Add tohtml with css styles test case
  By :user:`juarezr`, :issue:`609`.

* Fix sortheader() to not overwrite data for duplicate column names
  By :user:`arturponinski`, :issue:`392`.

* Add NotImplementedError to IterContainer's __iter__
  By :user:`arturponinski`, :issue:`483`.

* Add casting of headers to strings in toxlsx and appendxlsx
  By :user:`arturponinski`, :issue:`530`.

* Fix sorting of rows with different length
  By :user:`arturponinski`, :issue:`385`.

Version 1.7.7
-------------

* New pull request template. No python changes.
  By :user:`juarezr`, :issue:`594`.

Version 1.7.6
-------------

* Fix convertall does not work when table header has non-string elements
  By :user:`dnicolodi`, :issue:`579`.

* Fix todataframe() to do not iterate the table multiple times
  By :user:`dnicolodi`, :issue:`578`.

* Fix broken aggregate when supplying single key
  By :user:`MalayGoel`, :issue:`552`.

* Migrated to pytest
  By :user:`arturponinski`, :issue:`584`.

* Testing python 3.10 on Github Actions. No python changes.
  By :user:`juarezr`, :issue:`591`.

* codacity: upgrade to latest/main github action version. No python changes.
  By :user:`juarezr`, :issue:`585`.

* Publish releases to PyPI with Github Actions. No python changes.
  By :user:`juarezr`, :issue:`593`.

Version 1.7.5
-------------

* Added Decimal to numeric types
  By :user:`blas`, :issue:`573`.

* Add support for ignore_workbook_corruption parameter in xls
  By :user:`arturponinski`, :issue:`572`.

* Add support for generators in the petl.fromdicts
  By :user:`arturponinski`, :issue:`570`.

* Add function to support fromdb, todb, appenddb via clickhouse_driver
  By :user:`superjcd`, :issue:`566`.

* Fix fromdicts(...).header() raising TypeError
  By :user:`romainernandez`, :issue:`555`.

Version 1.7.4
-------------

* Use python 3.6 instead of 2.7 for deploy on travis-ci. No python changes.
  By :user:`juarezr`, :issue:`550`.

Version 1.7.3
-------------

* Fixed SQLAlchemy 1.4 removed the Engine.contextual_connect method
  By :user:`juarezr`, :issue:`545`.

* How to use convert with custom function and reference row
  By :user:`javidy`, :issue:`542`.

Version 1.7.2
-------------

* Allow aggregation over the entire table (without a key)
  By :user:`bmaggard`, :issue:`541`.

* Allow specifying output field name for simple aggregation
  By :user:`bmaggard`, :issue:`370`.

* Bumped version of package dependency on lxml from 4.4.0 to 4.6.2
  By :user:`juarezr`, :issue:`536`.

Version 1.7.1
-------------

* Fixing conda packaging failures.
  By :user:`juarezr`, :issue:`534`.


Version 1.7.0
-------------

* Added `toxml()` as convenience wrapper over `totext()`.
  By :user:`juarezr`, :issue:`529`.

* Document behavior of multi-field convert-with-row.
  By :user:`chrullrich`, :issue:`532`.

* Allow user defined sources from fsspec for :ref:`remote I/O <io_remotes>`.
  By :user:`juarezr`, :issue:`533`.


Version 1.6.8
-------------

* Allow using a custom/restricted xml parser in `fromxml()`.
  By :user:`juarezr`, :issue:`527`.


Version 1.6.7
-------------

* Reduced memory footprint for JSONL files, huge improvement.
  By :user:`fahadsiddiqui`, :issue:`522`.


Version 1.6.6
-------------

* Added python version 3.8 and 3.9 to tox.ini for using in newer distros.
  By :user:`juarezr`, :issue:`517`.

* Fixed compatibility with python3.8 in `petl.timings.clock()`.
  By :user:`juarezr`, :issue:`484`.

* Added json lines support in `fromjson()`. 
  By :user:`fahadsiddiqui`, :issue:`521`.


Version 1.6.5
-------------

* Fixed `fromxlsx()` with read_only crashes.
  By :user:`juarezr`, :issue:`514`.


Version 1.6.4
-------------

* Fixed exception when writing to S3 with ``fsspec`` ``auto_mkdir=True``.
  By :user:`juarezr`, :issue:`512`.


Version 1.6.3
-------------

* Allowed reading and writing Excel files in remote sources.
  By :user:`juarezr`, :issue:`506`.

* Allow `toxlsx()` to add or replace a worksheet. 
  By :user:`churlrich`, :issue:`502`.

* Improved avro: improve message on schema or data mismatch. 
  By :user:`juarezr`, :issue:`507`.

* Fixed build for failed test case. By :user:`juarezr`, :issue:`508`.


Version 1.6.2
-------------

* Fixed boolean type detection in toavro(). By :user:`juarezr`, :issue:`504`.

* Fix unavoidable warning if fsspec is installed but some optional package is
  not installed.
  By :user:`juarezr`, :issue:`503`.


Version 1.6.1
-------------

* Added `extras_require` for the `petl` pip package.
  By :user:`juarezr`, :issue:`501`.

* Fix unavoidable warning if fsspec is not installed.
  By :user:`juarezr`, :issue:`500`.


Version 1.6.0
-------------

* Added class :class:`petl.io.remotes.RemoteSource` using package **fsspec**
  for reading and writing files in remote servers by using the protocol in the
  url for selecting the implementation.
  By :user:`juarezr`, :issue:`494`.

* Removed classes :class:`petl.io.source.s3.S3Source` as it's handled by fsspec
  By :user:`juarezr`, :issue:`494`.

* Removed classes :class:`petl.io.codec.xz.XZCodec`, :class:`petl.io.codec.xz.LZ4Codec`
  and :class:`petl.io.codec.zstd.ZstandardCodec` as it's handled by fsspec.
  By :user:`juarezr`, :issue:`494`.

* Fix bug in connection to a JDBC database using jaydebeapi.
  By :user:`miguelosana`, :issue:`497`.


Version 1.5.0
-------------

* Added functions :func:`petl.io.sources.register_reader` and
  :func:`petl.io.sources.register_writer` for registering custom source helpers for
  hanlding I/O from remote protocols.
  By :user:`juarezr`, :issue:`491`.

* Added function :func:`petl.io.sources.register_codec` for registering custom
  helpers for compressing and decompressing files with other algorithms.
  By :user:`juarezr`, :issue:`491`.

* Added classes :class:`petl.io.codec.xz.XZCodec`, :class:`petl.io.codec.xz.LZ4Codec`
  and :class:`petl.io.codec.zstd.ZstandardCodec` for compressing files with `XZ` and
  the "state of art"  `LZ4` and `Zstandard` algorithms.
  By :user:`juarezr`, :issue:`491`.

* Added classes :class:`petl.io.source.s3.S3Source` and
  :class:`petl.io.source.smb.SMBSource` reading and writing files to remote
  servers using int url the protocols `s3://` and `smb://`.
  By :user:`juarezr`, :issue:`491`.


Version 1.4.0
-------------

* Added functions :func:`petl.io.avro.fromavro`, :func:`petl.io.avro.toavro`,
  and :func:`petl.io.avro.appendavro` for reading and writing to 
  `Apache Avro <https://avro.apache.org/docs/current/spec.html>` files. Avro
  generally is faster and safer than text formats like Json, XML or CSV.
  By :user:`juarezr`, :issue:`490`.


Version 1.3.0
-------------

.. note::
    The parameters to the :func:`petl.io.xlsx.fromxlsx` function have changed
    in this release. The parameters ``row_offset`` and ``col_offset`` are no longer
    supported. Please use ``min_row``, ``min_col``, ``max_row`` and ``max_col`` instead.

* A new configuration option `failonerror` has been added to the :mod:`petl.config` 
  module. This option affects various transformation functions including 
  :func:`petl.transform.conversions.convert`, :func:`petl.transform.maps.fieldmap`, 
  :func:`petl.transform.maps.rowmap` and :func:`petl.transform.maps.rowmapmany`. 
  The option can have values `True` (raise any exceptions encountered during conversion), 
  `False` (silently use a given `errorvalue` if any exceptions arise during conversion) or 
  `"inline"` (use any exceptions as the output value). The default value is `False` which 
  maintains compatibility with previous releases. By :user:`bmaggard`, :issue:`460`, 
  :issue:`406`, :issue:`365`.
  
* A new function :func:`petl.util.timing.log_progress` has been added, which behaves
  in a similar way to :func:`petl.util.timing.progress` but writes to a Python logger.
  By :user:`dusktreader`, :issue:`408`, :issue:`407`.

* Added new function :func:`petl.transform.regex.splitdown` for splitting a value into
  multiple rows. By :user:`John-Dennert`, :issue:`430`, :issue:`386`.

* Added new function :func:`petl.transform.basics.addfields` to add multiple new fields
  at a time. By :user:`mjumbewu`, :issue:`417`.

* Pass through keyword arguments to :func:`xlrd.open_workbook`. By :user:`gjunqueira`,
  :issue:`470`, :issue:`473`.

* Added new function :func:`petl.io.xlsx.appendxlsx`. By :user:`victormpa` and :user:`alimanfoo`,
  :issue:`424`, :issue:`475`.

* Fixes for upstream API changes in openpyxl and intervaltree modules. N.B., the arguments
  to :func:`petl.io.xlsx.fromxlsx` have changed for specifying row and column offsets
  to match openpyxl. (:issue:`472` - :user:`alimanfoo`).
  
* Exposed `read_only` argument in :func:`petl.io.xlsx.fromxlsx` and set default to 
  False to prevent truncation of files created by LibreOffice. By :user:`mbelmadani`, 
  :issue:`457`.

* Added support for reading from remote sources with gzip or bz2 compression 
  (:issue:`463` - :user:`H-Max`).

* The function :func:`petl.transform.dedup.distinct` has been fixed for the case
  where ``None`` values appear in the table. By :user:`bmaggard`, :issue:`414`,
  :issue:`412`.
  
* Changed keyed sorts so that comparisons are only by keys. By :user:`DiegoEPaez`, 
  :issue:`466`.

* Documentation improvements by :user:`gamesbook` (:issue:`458`).


Version 1.2.0
-------------

Please note that this version drops support for Python 2.6 (:issue:`443`,
:issue:`444` - :user:`hugovk`).

* Function :func:`petl.transform.basics.addrownumbers` now supports a "field"
  argument to allow specifying the name of the new field to be added
  (:issue:`366`, :issue:`367` - :user:`thatneat`).
* Fix to :func:`petl.io.xlsx.fromxslx` to ensure that the underlying workbook is
  closed after iteration is complete (:issue:`387` - :user:`mattkatz`).
* Resolve compatibility issues with newer versions of openpyxl
  (:issue:`393`, :issue:`394` - :user:`henryrizzi`).
* Fix deprecation warnings from openpyxl (:issue:`447`, :issue:`445` -
  :user:`scardine`; :issue:`449` - :user:`alimanfoo`).
* Changed exceptions to use standard exception classes instead of ArgumentError
  (:issue:`396` - :user:`bmaggard`).
* Add support for non-numeric quoting in CSV files (:issue:`377`, :issue:`378`
  - :user:`vilos`).
* Fix bug in handling of mode in MemorySource (:issue:`403` - :user:`bmaggard`).
* Added a get() method to the Record class (:issue:`401`, :issue:`402` -
  :user:`dusktreader`).
* Added ability to make constraints optional, i.e., support validation on
  optional fields (:issue:`399`, :issue:`400` - :user:`dusktreader`).
* Added support for CSV files without a header row (:issue:`421` -
  :user:`LupusUmbrae`).
* Documentation fixes (:issue:`379` - :user:`DeanWay`; :issue:`381` -
  :user:`PabloCastellano`).

Version 1.1.0
-------------

* Fixed :func:`petl.transform.reshape.melt` to work with non-string key
  argument (`#209 <https://github.com/petl-developers/petl/issues/209>`_).
* Added example to docstring of :func:`petl.transform.dedup.conflicts` to
  illustrate how to analyse the source of conflicts when rows are merged from
  multiple tables
  (`#256 <https://github.com/petl-developers/petl/issues/256>`_).
* Added functions for working with bcolz ctables, see :mod:`petl.io.bcolz`
  (`#310 <https://github.com/petl-developers/petl/issues/310>`_).
* Added :func:`petl.io.base.fromcolumns`
  (`#316 <https://github.com/petl-developers/petl/issues/316>`_).
* Added :func:`petl.transform.reductions.groupselectlast`.
  (`#319 <https://github.com/petl-developers/petl/issues/319>`_).
* Added example in docstring for :class:`petl.io.sources.MemorySource`
  (`#323 <https://github.com/petl-developers/petl/issues/323>`_).
* Added function :func:`petl.transform.basics.stack` as a simpler
  alternative to :func:`petl.transform.basics.cat`. Also behaviour of
  :func:`petl.transform.basics.cat` has changed for tables where the header
  row contains duplicate fields. This was part of addressing a bug in
  :func:`petl.transform.basics.addfield` for tables where the header
  contains duplicate fields
  (`#327 <https://github.com/petl-developers/petl/issues/327>`_).
* Change in behaviour of :func:`petl.io.json.fromdicts` to preserve
  ordering of keys if ordered dicts are used. Also added
  :func:`petl.transform.headers.sortheader` to deal with unordered
  cases
  (`#332 <https://github.com/petl-developers/petl/issues/332>`_).
* Added keyword `strict` to functions in the :mod:`petl.transform.setops`
  module to enable users to enforce strict set-like behaviour if desired
  (`#333 <https://github.com/petl-developers/petl/issues/333>`_).
* Added `epilogue` argument to :func:`petl.util.vis.display` to enable further
  customisation of content of table display in Jupyter notebooks
  (`#337 <https://github.com/petl-developers/petl/issues/337>`_).
* Added :func:`petl.transform.selects.biselect` as a convenience for
  obtaining two tables, one with rows matching a condition, the other with
  rows not matching the condition
  (`#339 <https://github.com/petl-developers/petl/issues/339>`_).
* Changed :func:`petl.io.json.fromdicts` to avoid making two passes through
  the data
  (`#341 <https://github.com/petl-developers/petl/issues/341>`_).
* Changed :func:`petl.transform.basics.addfieldusingcontext` to enable
  running calculations
  (`#343 <https://github.com/petl-developers/petl/issues/343>`_).
* Fix behaviour of join functions when tables have no non-key fields
  (`#345 <https://github.com/petl-developers/petl/issues/345>`_).
* Fix incorrect default value for 'errors' argument when using codec module
  (`#347 <https://github.com/petl-developers/petl/issues/347>`_).
* Added some documentation on how to write extension classes, see :doc:`intro`
  (`#349 <https://github.com/petl-developers/petl/issues/349>`_).
* Fix issue with unicode field names
  (`#350 <https://github.com/petl-developers/petl/issues/350>`_).

Version 1.0
-----------

Version 1.0 is a new major release of :mod:`petl`. The main purpose of
version 1.0 is to introduce support for Python 3.4, in addition to the
existing support for Python 2.6 and 2.7. Much of the functionality
available in :mod:`petl` versions 0.x has remained unchanged in
version 1.0, and most existing code that uses :mod:`petl` should work
unchanged with version 1.0 or with minor changes. However there have
been a number of API changes, and some functionality has been migrated
from the `petlx`_ package, described below.

If you have any questions about migrating to version 1.0 or find any
problems or issues please email python-etl@googlegroups.com.

Text file encoding
~~~~~~~~~~~~~~~~~~

Version 1.0 unifies the API for working with ASCII and non-ASCII
encoded text files, including CSV and HTML.

The following functions now accept an 'encoding' argument, which
defaults to the value of ``locale.getpreferredencoding()`` (usually
'utf-8'): `fromcsv`, `tocsv`, `appendcsv`, `teecsv`, `fromtsv`,
`totsv`, `appendtsv`, `teetsv`, `fromtext`, `totext`, `appendtext`,
`tohtml`, `teehtml`.

The following functions have been removed as they are now redundant:
`fromucsv`, `toucsv`, `appenducsv`, `teeucsv`, `fromutsv`, `toutsv`,
`appendutsv`, `teeutsv`, `fromutext`, `toutext`, `appendutext`,
`touhtml`, `teeuhtml`.

To migrate code, in most cases it should be possible to simply replace
'fromucsv' with 'fromcsv', etc.

`pelt.fluent` and `petl.interactive`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The functionality previously available through the `petl.fluent` and
`petl.interactive` modules is now available through the root petl
module.

This means two things.

First, is is now possible to use either functional or fluent (i.e.,
object-oriented) styles of programming with the root :mod:`petl`
module, as described in introductory section on
:ref:`intro_programming_styles`.

Second, the default representation of table objects uses the
:func:`petl.util.vis.look` function, so you can simply return a table
from the prompt to inspect it, as described in the introductory
section on :ref:`intro_interactive_use`.

The `petl.fluent` and `petl.interactive` modules have been removed as
they are now redundant.

To migrate code, it should be possible to simply replace "import
petl.fluent as etl" or "import petl.interactive as etl" with "import
petl as etl".

Note that the automatic caching behaviour of the `petl.interactive`
module has **not** been retained. If you want to enable caching
behaviour for a particular table, make an explicit call to the
:func:`petl.util.materialise.cache` function. See also
:ref:`intro_caching`.

IPython notebook integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In version 1.0 :mod:`petl` table container objects implement
`_repr_html_()` so can be returned from a cell in an IPython notebook
and will automatically format as an HTML table.

Also, the :func:`petl.util.vis.display` and
:func:`petl.util.vis.displayall` functions have been migrated across
from the `petlx.ipython` package. If you are working within the
IPython notebook these functions give greater control over how tables
are rendered. For some examples, see:

  http://nbviewer.ipython.org/github/petl-developers/petl/blob/v1.0/repr_html.ipynb

Database extract/load functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`petl.io.db.todb` function now supports automatic table
creation, inferring a schema from data in the table to be loaded. This
functionality has been migrated across from the `petlx`_ package, and
requires `SQLAlchemy <http://www.sqlalchemy.org/>`_ to be installed.

The functions `fromsqlite3`, `tosqlite3` and `appendsqlite3` have been
removed as they duplicate functionality available from the existing
functions :func:`petl.io.db.fromdb`, :func:`petl.io.db.todb` and
:func:`petl.io.db.appenddb`. These existing functions have been
modified so that if a string is provided as the `dbo` argument it is
interpreted as the name of an :mod:`sqlite3` file. It should be
possible to migrate code by simply replacing 'fromsqlite3' with
'fromdb', etc.

Other functions removed or renamed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following functions have been removed because they are overly
complicated and/or hardly ever used. If you use any of these functions
and would like to see them re-instated then please email
python-etl@googlegroups.com: `rangefacet`, `rangerowreduce`,
`rangeaggregate`, `rangecounts`, `multirangeaggregate`, `lenstats`.

The following functions were marked as deprecated in petl 0.x and have
been removed in version 1.0: `dataslice` (use `data` instead),
`fieldconvert` (use `convert` instead), `fieldselect` (use `select` instead),
`parsenumber` (use `numparser` instead), `recordmap` (use `rowmap` instead),
`recordmapmany` (use `rowmapmany` instead), `recordreduce` (use `rowreduce`
instead), `recordselect` (use `rowselect` instead), `valueset` (use
``table.values(‘foo’).set()`` instead).

The following functions are no longer available in the root
:mod:`petl` namespace, but are still available from a subpackage if
you really need them: `iterdata` (use `data` instead), `iterdicts`
(use `dicts` instead), `iternamedtuples` (use `namedtuples` instead),
`iterrecords` (use `records` instead), `itervalues` (use `values`
instead).

The following functions have been renamed: `isordered` (renamed to
`issorted`), `StringSource` (renamed to `MemorySource`).

The function `selectre` has been removed as it duplicates
functionality, use `search` instead.

Sorting and comparison
~~~~~~~~~~~~~~~~~~~~~~

A major difference between Python 2 and Python 3 involves comparison
and sorting of objects of different types. Python 3 is a lot stricter
about what you can compare with what, e.g., ``None < 1 < 'foo'`` works
in Python 2.x but raises an exception in Python 3. The strict
comparison behaviour of Python 3 is generally a problem for typical
usages of :mod:`petl`, where data can be highly heterogeneous and a
column in a table may have a mixture of values of many different
types, including `None` for missing.

To maintain the usability of :mod:`petl` in this type of scenario, and
to ensure that the behaviour of :mod:`petl` is as consistent as
possible across different Python versions, the
:func:`petl.transform.sorts.sort` function and anything that depends
on it (as well as any other functions making use of rich comparisons)
emulate the relaxed comparison behaviour that is available under
Python 2.x. In fact :mod:`petl` goes further than this, allowing
comparison of a wider range of types than is possible under Python 2.x
(e.g., ``datetime`` with ``None``).

As the underlying code to achieve this has been completely reworked,
there may be inconsistencies or unexpected behaviour, so it's worth
testing carefully the results of any code previously run using
:mod:`petl` 0.x, especially if you are also migrating from Python 2 to
Python 3.

The different comparison behaviour under different Python versions may
also give unexpected results when selecting rows of a table. E.g., the
following will work under Python 2.x but raise an exception under
Python 3.4::

    >>> import petl as etl
    >>> table = [['foo', 'bar'],
    ...          ['a', 1],
    ...          ['b', None]]
    >>> # raises exception under Python 3
    ... etl.select(table, 'bar', lambda v: v > 0)

To get the more relaxed behaviour under Python 3.4,
use the :mod:`petl.transform.selects.selectgt` function, or wrap
values with :class:`petl.comparison.Comparable`, e.g.::

    >>> # works under Python 3
    ... etl.selectgt(table, 'bar', 0)
    +-----+-----+
    | foo | bar |
    +=====+=====+
    | 'a' |   1 |
    +-----+-----+

    >>> # or ...
    ... etl.select(table, 'bar', lambda v: v > etl.Comparable(0))
    +-----+-----+
    | foo | bar |
    +=====+=====+
    | 'a' |   1 |
    +-----+-----+

New extract/load modules
~~~~~~~~~~~~~~~~~~~~~~~~

Several new extract/load modules have been added, migrating
functionality previously available from the `petlx`_ package:

* :ref:`io_xls`
* :ref:`io_xlsx`
* :ref:`io_numpy`
* :ref:`io_pandas`
* :ref:`io_pytables`
* :ref:`io_whoosh`

These modules all have dependencies on third party packages, but these
have been kept as optional dependencies so are not required for
installing :mod:`petl`.

New validate function
~~~~~~~~~~~~~~~~~~~~~

A new :func:`petl.transform.validation.validate` function has been
added to provide a convenient interface when validating a table
against a set of constraints.

New intervals module
~~~~~~~~~~~~~~~~~~~~

A new module has been added providing transformation functions based
on intervals, migrating functionality previously available from the
`petlx`_ package:

* :ref:`transform_intervals`

This module requires the `intervaltree
<https://github.com/chaimleib/intervaltree>`_ module.

New configuration module
~~~~~~~~~~~~~~~~~~~~~~~~

All configuration variables have been brought together into a new
:mod:`petl.config` module. See the source code for the variables
available, they should be self-explanatory.


:mod:`petl.push` moved to :mod:`petlx`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :mod:`petl.push` module remains in an experimental state and has
been moved to the `petlx`_ extensions project.

Argument names and other minor changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Argument names for a small number of functions have been changed to
create consistency across the API.

There are some other minor changes as well. If you are migrating from
:mod:`petl` version 0.x the best thing is to run your code and inspect
any errors. Email python-etl@googlegroups.com if you have any
questions.

Source code reorganisation
~~~~~~~~~~~~~~~~~~~~~~~~~~

The source code has been substantially reorganised. This should not
affect users of the :mod:`petl` package however as all functions in
the public API are available through the root :mod:`petl` namespace.

.. _petlx: http://petlx.readthedocs.org

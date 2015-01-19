Changes
=======

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

  http://nbviewer.ipython.org/github/alimanfoo/petl/blob/v1.0/repr_html.ipynb

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

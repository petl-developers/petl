What's new
==========

Version 1.0
-----------

Version 1.0 is a new major release. Although much of the functionality
available in the 0.x series is still available in 1.0, there have been
a number of substantive changes which are not backwards-compatible.

If you have any questions about migrating to version 1.0 or find any
problems or issues please email python-etl@googlegroups.com.

Python 3 compatibility
~~~~~~~~~~~~~~~~~~~~~~

As of version 1.0 :mod:`petl` can be used with Python 2.6, Python 2.7
and Python 3.4.

Efforts have been made to ensure that the behaviour of :mod:`petl` is
consistent across these different Python versions. However, there are
two areas in particular where behaviour may be inconsistent or
unexpected, and you are advised to check the results of your code
carefully.

The first area concerns reading and writing to text files, including
CSV and HTML. The :mod:`petl` API for reading and writing these files
maintains a separation between functions working with ascii text only
(e.g., :func:`petl.io.csv.fromcsv`) and functions capable of working
with any character encoding (e.g.,
:func:`petl.io.csv.fromucsv`). These functions **should** behave in a
consistent way across Python versions, however the underlying
implementations are quite different depending on which Python version
is being used, and therefore there is a greater risk of
inconsistencies arising.

The second area concerns comparison behaviour between objects of
different types (e.g., ``None < 1 < 'foo'``), which is substantially
different in Python 3.4. This has consequences mainly for functions
which involve sorting, e.g., :func:`petl.transform.sorts.sort` and
other functions which depend on it. Python 3.4 is much stricter and
raises an Exception whenever objects of different types are compared,
whereas Python 2.x is more relaxed and allows comparisons between
objects of different types.

:mod:`petl` is designed to work well especially in situations
involving highly heterogeneous data, where values within a single
table column could objects of any type and include missing values
represented as ``None`` or something else. In these situations it is
generally preferable to allow sorting of heterogeneously typed
objects, even if the ordering is not meaningful. Therefore :mod:`petl`
tries to emulate the relaxed comparison behaviour of Python 2.x under
all Python versions. It actually goes further than this, to allow
comparison of a wider range of types than is possible under Python 2.x
(e.g., ``datetime`` with ``None``).

Again, this should mean that code using :mod:`petl` and involving
sorting that ran previously under Python 2.x should return the same
results under Python 3.4. However, because this has required some
re-engineering of :mod:`petl` internals, this is another area where
there may be inconsistencies or unexpected results.

The different comparison behaviour may also give unexpected results
when selecting rows of a table. E.g., the following will work
under Python 2.x but raise an exception under Python 3.4::

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


Functionalities removed
~~~~~~~~~~~~~~~~~~~~~~~

A number of functions that provided some substantive functionality
(i.e., were not simply convenience) have been removed in version 1.0
because they were overly complicated and not frequently used. The list
of these functions is below:

* rangefacet()
* rangerowreduce()
* rangerecordreduce()
* rangeaggregate()
* rangecounts()
* multirangeaggregate()
* lenstats()

If you have a concrete use case for any of these functions and would
like to see them re-instated then please email
python-etl@googlegroups.com.

The following functions were marked as deprecated in petl 0.x and have
been removed from version 1.0:

* dataslice() - use data() instead
* fieldconvert() - use convert() instead
* fieldselect() - use select() instead
* parsenumber() - use numparser() instead
* recordmap() - use rowmap() instead
* recordmapmany() - use rowmapmany() instead
* recordreduce() - use rowreduce() instead
* recordselect() - use rowselect() instead
* valueset() - use table.values('foo').set() instead

The following functions are no longer available in the root
:mod:`petl` namespace, but are still available from a subpackage if
you really need them:

* iterdata() - use data() instead
* iterdicts() - use dicts() instead
* iternamedtuples() - use namedtuples() instead
* iterrecords() - use records() instead
* itervalues() - use values() instead

The following functions have been renamed:

* isordered() - renamed to issorted()
* StringSource() - renamed to MemorySource()

:mod:`petl.fluent` and :mod:`petl.interactive`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The functionalities previously available in the :mod:`petl.fluent` and
:mod:`petl.interactive` modules have been integrated into the root
:mod:`petl` module. The :mod:`petl.fluent` and :mod:`petl.interactive`
modules have been removed.

This means that the root :mod:`petl` module now supports both the
functional and fluent (i.e., object-oriented) usage styles as
described above.

It also means that table objects have a natural representation when
returned from the prompt in an interactive Python session.

Note that the universal caching behaviour of the
:mod:`petl.interactive` module has been dropped. If you need caching
behaviour, use the :func:`petl.util.materialise.cache` function explicitly.

IPython notebook integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The functionalities previously available through the
:mod:`petlx.ipython` module have been integrated into
:mod:`petl`. This means that table objects will render as HTML tables
when returned from a cell in an IPython notebook. The functions
:mod:`petl.util.vis.display` and :mod:`petl.util.vis.displayall` are
also available for finer control over how tables are rendered in a
notebook.

:mod:`petl.push` moved to :mod:`petlx`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :mod:`petl.push` module has been moved to the
`petlx <http://petlx.readthedocs.org>`_ extensions project.

Source code reorganisation
~~~~~~~~~~~~~~~~~~~~~~~~~~

The source code has been substantially reorganised. This should not
affect users of the :mod:`petl` package as all functions are available
through the root :mod:`petl` namespace, however, anyone developing
code for contribution to :mod:`petl` will have to rebase.

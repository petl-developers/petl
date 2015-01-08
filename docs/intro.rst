.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Introduction
============


Installation
------------

This package is available from the `Python Package Index
<http://pypi.python.org/pypi/petl>`_. On Linux distributions you
should be able to do::

    $ pip install petl

On other platforms you can download manually, extract and run ``python
setup.py install``.

To verify the installation, the test suite can be run with :mod:`nose`::

    $ pip install nose
    $ nosetests -v petl


Dependencies and extensions
---------------------------

This package has been written with no dependencies other than the
Python core modules, for ease of installation and
maintenance. However, there are many third party packages which could
usefuly be used with :mod:`petl`, e.g., providing access to data from
Excel or other file types. Some extensions with these additional
dependencies are provided by the `petlx
<http://petlx.readthedocs.org>`_ package, a companion package to
:mod:`petl`.


ETL pipelines
-------------

This package makes extensive use of lazy evaluation and iterators. This
means, generally, that a pipeline will not actually be executed until
data is requested.

E.g., given a file at 'example.csv' in the current working directory::

	>>> example_data = """foo,bar,baz
	... a,1,3.4
	... b,2,7.4
	... c,6,2.2
	... d,9,8.1
	... """
	>>> with open('example.csv', 'w') as f:
	...     f.write(example_data)
	...

...the following code **does not** actually read the file or load any of its
contents into memory::

	>>> import petl as etl
	>>> table1 = etl.fromcsv('example.csv')

Rather, `table1` is a **row container** (see Conventions below), which can be
iterated over, extracting data from the underlying file on demand.

Similarly, if one or more transformation functions are applied, e.g.::

	>>> table2 = etl.convert(table1, 'foo', 'upper')
	>>> table3 = etl.convert(table2, 'bar', int)
	>>> table4 = etl.convert(table3, 'baz', float)
	>>> table5 = etl.addfield(table4, 'quux', lambda row: row.bar * row.baz)

...no actual transformation work will be done until data are
requested from `table5` or any of the other tables returned by
the intermediate steps. 

So in effect, a 5 step pipeline has been set up, and rows will pass through
the pipeline on demand, as they are pulled from the end of the pipeline via
iteration.

A call to a function like :func:`petl.util.vis.look`, or any of the functions
which write data to a file or database (e.g., :func:`petl.io.csv.tocsv`,
:func:`petl.io.text.totext`, :func:`petl.io.sqlite3.tosqlite3`,
:func:`petl.io.db.todb`), will pull data through the pipeline
and cause all of the transformation steps to be executed on the
requested rows, e.g.::

	>>> etl.look(table5)
	+-----+-----+-----+--------------------+
	| foo | bar | baz | quux               |
	+=====+=====+=====+====================+
	| 'A' |   1 | 3.4 |                3.4 |
	+-----+-----+-----+--------------------+
	| 'B' |   2 | 7.4 |               14.8 |
	+-----+-----+-----+--------------------+
	| 'C' |   6 | 2.2 | 13.200000000000001 |
	+-----+-----+-----+--------------------+
	| 'D' |   9 | 8.1 |  72.89999999999999 |
	+-----+-----+-----+--------------------+

...although note that :func:`petl.util.vis.look` will by default only request
the first 5 rows, and so the minimum amount of processing will be done to
produce 5 rows.


Functional and object-oriented programming styles
-------------------------------------------------

The :mod:`petl` package supports both functional and object-oriented
programming styles. For example, the example transformation pipeline in the
section above could also be written as::

        >>> import petl as etl
	>>> table = (
	...     etl
	...     .fromcsv('example.csv')
	...     .convert('foo', 'upper')
	...     .convert('bar', int)
	...     .convert('baz', float)
	...     .addfield('quux', lambda row: row.bar * row.baz)
	... )
	>>> table.look()
	+-----+-----+-----+--------------------+
	| foo | bar | baz | quux               |
	+=====+=====+=====+====================+
	| 'A' |   1 | 3.4 |                3.4 |
	+-----+-----+-----+--------------------+
	| 'B' |   2 | 7.4 |               14.8 |
	+-----+-----+-----+--------------------+
	| 'C' |   6 | 2.2 | 13.200000000000001 |
	+-----+-----+-----+--------------------+
	| 'D' |   9 | 8.1 |  72.89999999999999 |
	+-----+-----+-----+--------------------+

A ``wrap()`` function is also provided to use the object-oriented style with
any valid row container object, e.g.::

	>>> l = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
	>>> table = etl.wrap(l)
	>>> table.look()
	+-----+-----+
	| foo | bar |
	+=====+=====+
	| 'a' |   1 |
	+-----+-----+
	| 'b' |   2 |
	+-----+-----+
	| 'c' |   2 |
	+-----+-----+


Interactive use
---------------

When using :mod:`petl` from within an interactive Python session, the
default representation for table objects uses the :func:`petl.util.vis.look()`
function, so a table object can be returned at the prompt to inspect it, e.g.::

	>>> l = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
	>>> table = etl.wrap(l)
	>>> table
	+-------+-------+
	| 0|foo | 1|bar |
	+=======+=======+
	| 'a'   |     1 |
	+-------+-------+
	| 'b'   |     2 |
	+-------+-------+
	| 'c'   |     2 |
	+-------+-------+

By default the fields in the header are numbered for convenience, this can be
turned off via the :mod:`petl.config` module, e.g.::

	>>> etl.config.table_repr_index_header = False

By default data values are rendered using the built-in :func:`repr` function.
To see the string (:func:`str`) values instead, :func:`print` the table, e.g.:

	>>> print(table)
	+-----+-----+
	| foo | bar |
	+=====+=====+
	| a   |   1 |
	+-----+-----+
	| b   |   2 |
	+-----+-----+
	| c   |   2 |
	+-----+-----+

Table objects also implement ``_repr_html_()`` and so will be displayed as an
HTML table if returned from a cell in an IPython notebook. The functions
:func:`petl.util.viz.display` and :func:`petl.util.viz.displayall` also
provide more control over rendering of tables within an IPython notebook. For
examples of usage see the `repr_html notebook <http://nbviewer.ipython.org/github/alimanfoo/petl/blob/v1.0/repr_html.ipynb>`_.


``petl`` executable
-------------------

Also included in the ``petl`` distribution is a script to execute
simple transformation pipelines directly from the operating system
shell. E.g.::

	$ petl "dummytable().tocsv()" > dummy.csv
	$ cat dummy.csv | petl "fromcsv().cut('foo', 'baz').selectgt('baz', 0.5).head().data().totsv()"

The ``petl`` script is extremely simple, it expects a single
positional argument, which is evaluated as Python code but with all of
the functions in the :mod:`petl` namespace imported.


Conventions - row containers and row iterators
----------------------------------------------

This package defines the following convention for objects acting as
containers of tabular data and supporting row-oriented iteration over
the data.

A **row container** (also referred to here as a **table**) is
any object which satisfies the following:

1. implements the `__iter__` method

2. `__iter__` returns a **row iterator** (see below)

3. all row iterators returned by `__iter__` are independent, i.e., consuming items from one iterator will not affect any other iterators

A **row iterator** is an iterator which satisfies the following:

4. each item returned by the iterator is a sequence (e.g., tuple or list)

5. the first item returned by the iterator is a **header row** comprising a sequence of **header values**

6. each subsequent item returned by the iterator is a **data row** comprising a sequence of **data values**

7. a **header value** is typically a string (`str`) but may be an object of any type as long as it implements `__str__` and is pickleable

8. a **data value** is any pickleable object

So, for example, a list of lists is a valid row container::

    >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]

Note that an object returned by the :func:`csv.reader` function from the
standard Python :mod:`csv` module is a row iterator and **not** a row
container, because it can only be iterated over once. However, it is
straightforward to define functions that support the row container convention
and provide access to data from CSV or other types of file or data source, see
e.g. the :func:`petl.io.csv.fromcsv` function.

The main reason for requiring that row containers support independent
row iterators (point 3) is that data from a table may need to be
iterated over several times within the same program or interactive
session. E.g., when using :mod:`petl` in an interactive session to build up
a sequence of data transformation steps, the user might want to
examine outputs from several intermediate steps, before all of the
steps are defined and the transformation is executed in full.

Note that this convention does not place any restrictions on the
lengths of header and data rows. A table may contain a header row
and/or data rows of varying lengths.


Caching
-------

This package tries to make efficient use of memory by using iterators
and lazy evaluation where possible. However, some transformations
cannot be done without building data structures, either in memory or
on disk.

An example is the :func:`petl.transform.sorts.sort` function, which will either
sort a table entirely in memory, or will sort the table in memory in chunks,
writing chunks to disk and performing a final merge sort on the
chunks. Which strategy is used will depend on the arguments passed
into the :func:`petl.transform.sorts.sort` function when it is called.

In either case, the sorting can take some time, and if the sorted data
will be used more than once, it is undesirable to start again from
scratch each time. It is better to cache the sorted data, if possible,
so it can be re-used.

The :func:`petl.transform.sorts.sort` function, and all functions which use
it internally, provide a `cache` keyword argument which can be used to
turn on or off the caching of sorted data.

There is also an explicit :func:`petl.util.materialise.cache` function, which
can be used to cache in memory up to a configurable number of rows from any
table.


Changes in version 1.0
----------------------

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
	+-------+-------+
	| 0|foo | 1|bar |
	+=======+=======+
	| 'a'   |     1 |
	+-------+-------+

	>>> # or ...
	... etl.select(table, 'bar', lambda v: v > etl.Comparable(0))
	+-------+-------+
	| 0|foo | 1|bar |
	+=======+=======+
	| 'a'   |     1 |
	+-------+-------+


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

:mod:`petl.fluent` and :mod:`petl.interactive` functionalities merged into root :mod:`petl` module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Acknowledgments
---------------

This package is primarily developed and maintained by Alistair Miles
<alimanfoo@googlemail.com> with funding from the `MRC Centre for
Genomics and Global Health <http://www.cggh.org>`_.

The following people have also contributed to the development of this
package:

* Alexander Stauber
* Andrew Kim (`andrewakim <https://github.com/andrewakim>`_)
* Caleb Lloyd (`caleblloyd <https://github.com/caleblloyd>`_)
* Florent Xicluna (`florentx <https://github.com/florentx>`_)
* Jonathan Camile (`deytao <https://github.com/deytao>`_)
* Kenneth Borthwick
* Michael Rea (`rea725 <https://github.com/rea725>`_)
* Olivier Macchioni (`omacchioni <https://github.com/omacchioni>`_)
* Olivier Poitrey (`rs <https://github.com/rs>`_)
* Peder Jakobsen (`pjakobsen <https://github.com/pjakobsen>`_)
* Phillip Knaus (`phillipknaus <https://github.com/phillipknauss>`_)
* Richard Pearson (`podpearson <https://github.com/podpearson>`_)
* Roger Woodley (`rogerkwoodley <https://github.com/rogerkwoodley>`_)
* `adamsdarlingtower <https://github.com/adamsdarlingtower>`_
* `imazor <https://github.com/imazor>`_
* `james-unified <https://github.com/james-unified>`_
* `shayh <https://github.com/shayh>`_
* `thatneat <https://github.com/thatneat>`_
* `titusz <https://github.com/titusz>`_
* `zigen <https://github.com/zigen>`_

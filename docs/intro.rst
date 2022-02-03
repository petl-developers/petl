Introduction
============

.. _intro_design_goals:

Design goals
------------

This package is designed primarily for convenience and ease of use,
especially when working interactively with data that are unfamiliar,
heterogeneous and/or of mixed quality.

:mod:`petl` transformation pipelines make minimal use of system memory
and can scale to millions of rows if speed is not a priority. However
if you are working with very large datasets and/or performance-critical
applications then other packages may be more suitable, e.g., see
`pandas <http://pandas.pydata.org/>`_, `pytables
<https://pytables.github.io/>`_, `bcolz <http://bcolz.blosc.org/>`_
and `blaze <http://blaze.pydata.org/>`_. See also :doc:`related_work`.

.. _intro_pipelines:

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

Rather, `table1` is a **table container** (see :ref:`intro_conventions`
below) which can be iterated over, extracting data from the
underlying file on demand.

Similarly, if one or more transformation functions are applied, e.g.::

    >>> table2 = etl.convert(table1, 'foo', 'upper')
    >>> table3 = etl.convert(table2, 'bar', int)
    >>> table4 = etl.convert(table3, 'baz', float)
    >>> table5 = etl.addfield(table4, 'quux', lambda row: row.bar * row.baz)

...no actual transformation work will be done until data are
requested from `table5` (or any of the other tables returned by
the intermediate steps).

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

.. _intro_programming_styles:

Functional and object-oriented programming styles
-------------------------------------------------

The :mod:`petl` package supports both functional and object-oriented
programming styles. For example, the example in the section on
:ref:`intro_pipelines` above could also be written as::

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
any valid table container object, e.g.::

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

.. _intro_interactive_use:

Interactive use
---------------

When using :mod:`petl` from within an interactive Python session, the
default representation for table objects uses the :func:`petl.util.vis.look()`
function, so a table object can be returned at the prompt to inspect it, e.g.::

    >>> l = [['foo', 'bar'], ['a', 1], ['b', 2], ['c', 2]]
    >>> table = etl.wrap(l)
    >>> table
    +-----+-----+
    | foo | bar |
    +=====+=====+
    | 'a' |   1 |
    +-----+-----+
    | 'b' |   2 |
    +-----+-----+
    | 'c' |   2 |
    +-----+-----+

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

.. _intro_ipython_notebook:

IPython notebook integration
----------------------------

Table objects also implement ``_repr_html_()`` and so will be displayed as an
HTML table if returned from a cell in an IPython notebook. The functions
:func:`petl.util.vis.display` and :func:`petl.util.vis.displayall` also
provide more control over rendering of tables within an IPython notebook.

For examples of usage see the `repr_html notebook <https://nbviewer.jupyter.org/github/petl-developers/petl/blob/master/repr_html.ipynb>`_.

.. _intro_executable:

``petl`` executable
-------------------

Also included in the ``petl`` distribution is a script to execute
simple transformation pipelines directly from the operating system
shell. E.g.::

    $ petl "dummytable().tocsv()" > example.csv
    $ cat example.csv | petl "fromcsv().cut('foo', 'baz').convert('baz', float).selectgt('baz', 0.5).head().data().totsv()"

The ``petl`` script is extremely simple, it expects a single
positional argument, which is evaluated as Python code but with all of
the functions in the :mod:`petl` namespace imported.

.. _intro_conventions:

Conventions - table containers and table iterators
--------------------------------------------------

This package defines the following convention for objects acting as
containers of tabular data and supporting row-oriented iteration over
the data.

A **table container** (also referred to here as a **table**) is
any object which satisfies the following:

1. implements the `__iter__` method

2. `__iter__` returns a **table iterator** (see below)

3. all table iterators returned by `__iter__` are independent, i.e., consuming items from one iterator will not affect any other iterators

A **table iterator** is an iterator which satisfies the following:

4. each item returned by the iterator is a sequence (e.g., tuple or list)

5. the first item returned by the iterator is a **header row** comprising a sequence of **header values**

6. each subsequent item returned by the iterator is a **data row** comprising a sequence of **data values**

7. a **header value** is typically a string (`str`) but may be an object of any type as long as it implements `__str__` and is pickleable

8. a **data value** is any pickleable object

So, for example, a list of lists is a valid table container::

    >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]

Note that an object returned by the :func:`csv.reader` function from the
standard Python :mod:`csv` module is a table iterator and **not** a table
container, because it can only be iterated over once. However, it is
straightforward to define functions that support the table container convention
and provide access to data from CSV or other types of file or data source, see
e.g. the :func:`petl.io.csv.fromcsv` function.

The main reason for requiring that table containers support independent
table iterators (point 3) is that data from a table may need to be
iterated over several times within the same program or interactive
session. E.g., when using :mod:`petl` in an interactive session to build up
a sequence of data transformation steps, the user might want to
examine outputs from several intermediate steps, before all of the
steps are defined and the transformation is executed in full.

Note that this convention does not place any restrictions on the
lengths of header and data rows. A table may contain a header row
and/or data rows of varying lengths.

.. _intro_extending:

Extensions - integrating custom data sources
--------------------------------------------

The :mod:`petl.io` module has functions for extracting data from a number of
well-known data sources. However, it is also straightforward to write an
extension that enables integration with other data sources. For an object to
be usable as a :mod:`petl` table it has to implement the **table container**
convention described above. Below is the source code for an
:class:`ArrayView` class which allows integration of :mod:`petl` with numpy
arrays. This class is included within the :mod:`petl.io.numpy`
module but also provides an example of how other data sources might be
integrated::

    >>> import petl as etl
    >>> class ArrayView(etl.Table):
    ...     def __init__(self, a):
    ...         # assume that a is a numpy array
    ...         self.a = a
    ...     def __iter__(self):
    ...         # yield the header row
    ...         header = tuple(self.a.dtype.names)
    ...         yield header
    ...         # yield the data rows
    ...         for row in self.a:
    ...             yield tuple(row)
    ...

Now this class enables the use of numpy arrays with :mod:`petl` functions,
e.g.::

    >>> import numpy as np
    >>> a = np.array([('apples', 1, 2.5),
    ...               ('oranges', 3, 4.4),
    ...               ('pears', 7, 0.1)],
    ...              dtype='U8, i4,f4')
    >>> t1 = ArrayView(a)
    >>> t1
    +-----------+----+-----------+
    | f0        | f1 | f2        |
    +===========+====+===========+
    | 'apples'  | 1  | 2.5       |
    +-----------+----+-----------+
    | 'oranges' | 3  | 4.4000001 |
    +-----------+----+-----------+
    | 'pears'   | 7  | 0.1       |
    +-----------+----+-----------+

    >>> t2 = t1.cut('f0', 'f2').convert('f0', 'upper').addfield('f3', lambda row: row.f2 * 2)
    >>> t2
    +-----------+-----------+---------------------+
    | f0        | f2        | f3                  |
    +===========+===========+=====================+
    | 'APPLES'  | 2.5       |                 5.0 |
    +-----------+-----------+---------------------+
    | 'ORANGES' | 4.4000001 |  8.8000001907348633 |
    +-----------+-----------+---------------------+
    | 'PEARS'   | 0.1       | 0.20000000298023224 |
    +-----------+-----------+---------------------+

If you develop an extension for a data source that you think would also be
useful for others, please feel free to submit a PR to the
`petl GitHub repository <https://github.com/petl-developers/petl>`_, or if it
is a domain-specific data source, the
`petlx GitHub repository <https://github.com/petl-developers/petlx>`_.

.. _intro_caching:

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

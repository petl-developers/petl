.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

Introduction
============

Installation
------------

This module is available from the `Python Package Index
<http://pypi.python.org/pypi/petl>`_. On Linux distributions you
should be able to do ``easy_install petl`` or ``pip install petl``. On
other platforms you can download manually, extract and run ``python
setup.py install``.


Dependencies and extensions
---------------------------

This package has been written with no dependencies other than the
Python core modules, for ease of installation and
maintenance. However, there are many third party packages which could
usefuly be used with :mod:`petl`, e.g., providing access to data from
Excel or other file types. Some extensions with these additional
dependencies are provided by the `petlx
<http://pypi.python.org/pypi/petlx>`_ package, a companion package to
:mod:`petl`.


Conventions - row containers and row iterators
----------------------------------------------

This package defines the following convention for objects acting as
containers of tabular data and supporting row-oriented iteration over
the data.

A *row container* (also referred to here informally as a *table*) is
any object which satisfies the following:

1. implements the `__iter__` method

2. `__iter__` returns a *row iterator* (see below)

3. all row iterators returned by `__iter__` are independent, i.e., consuming items from one iterator will not affect any other iterators

A *row iterator* is an iterator which satisfies the following:

4. each item returned by the iterator is either a list or a tuple

5. the first item returned by the iterator is a *header row* comprising a list or tuple of *fields*

6. each subsequent item returned by the iterator is a *data row* comprising a list or tuple of *data values*

7. a *field* is typically a string (`str` or `unicode`) but may be an object of any type as long as it implements `__str__` and is pickleable

8. a *data value* is any pickleable object that supports rich comparison operators

So, for example, the list of lists shown below is a row container::

    >>> table = [['foo', 'bar'], ['a', 1], ['b', 2]]

Note that, under this convention, an object returned by the
:func:`csv.reader` function from the standard Python :mod:`csv` module
is a row iterator and *not* a row container, because it can only be
iterated over once, e.g.::

    >>> from StringIO import StringIO
    >>> import csv
    >>> csvdata = """foo,bar
    ... a,1
    ... b,2
    ... """
    >>> rowiterator = csv.reader(StringIO(csvdata))
    >>> for row in rowiterator:
    ...     print row
    ...
    ['foo', 'bar']
    ['a', '1']
    ['b', '2']
    >>> for row in rowiterator:
    ...     print row
    ...
    >>> # can only iterate once

However, it is straightforward to define functions that support the
above convention for row containers and provide access to data from
CSV or other types of file or data source, see e.g. the
:func:`fromcsv` function in this package.

The main reason for requiring that row containers support independent
row iterators (point 3) is that data from a table may need to be
iterated over several times within the same program or interactive
session. E.g., when using `petl` in an interactive session to build up
a sequence of data transformation steps, the user might want to
examine outputs from several intermediate steps, before all of the
steps are defined and the transformation is executed in full.

Note that this convention does not place any restrictions on the
lengths of header and data rows. A table may return a header row
and/or data rows of varying lengths. 

Note also that many features of :mod:`petl` depend on sorting which
will only work if the data values support rich comparison operators.

Transformation pipelines
------------------------

This package makes extensive use of lazy evaluation and
iterators. This means, generally, that a transformation will not
actually be executed until data is requested.

E.g., given the following data in a file at 'example1.csv' in the
current working directory::

	foo,bar,baz
	a,1,3.4
	b,2,7.4
	c,6,2.2
	d,9,8.1

...the following code does not actually read the file, nor does it
load any of its contents into memory::

	>>> from petl import *
	>>> table1 = fromcsv('example1.csv')

Rather, `table1` is a row container object, which can be iterated over. 

Similarly, if one or more transformation functions are applied, e.g.:::

	>>> table2 = convert(table1, 'foo', 'upper')
	>>> table3 = convert(table2, 'bar', int)
	>>> table4 = convert(table3, 'baz', float)
	>>> table5 = addfield(table4, 'quux', expr('{bar} * {baz}'))

...no actual transformation work will be done, until data are
requested from `table5` or any of the other row containers returned by
the intermediate steps. 

So in effect, a 5 step transformation pipeline has been set up, and
rows will pass through the pipeline on demand, as they are pulled from
the end of the pipeline via iteration.

A call to a function like :func:`look`, or any of the functions which
write data to a file or database (e.g., :func:`tocsv`, :func:`totext`,
:func:`tosqlite3`, :func:`todb`), will pull data through the pipeline
and cause all of the transformation steps to be executed on the
requested rows, e.g.::

	>>> look(table5)
	+-------+-------+-------+--------------------+
	| 'foo' | 'bar' | 'baz' | 'quux'             |
	+=======+=======+=======+====================+
	| 'A'   | 1     | 3.4   | 3.4                |
	+-------+-------+-------+--------------------+
	| 'B'   | 2     | 7.4   | 14.8               |
	+-------+-------+-------+--------------------+
	| 'C'   | 6     | 2.2   | 13.200000000000001 |
	+-------+-------+-------+--------------------+
	| 'D'   | 9     | 8.1   | 72.89999999999999  |
	+-------+-------+-------+--------------------+

...although note that :func:`look` will by default only request the
first 10 rows, and so at most only 10 rows will be processed. Calling
:func:`look` to inspect the first few rows of a table is often an
efficient way to examine the output of a transformation pipeline,
without having to execute the transformation over all of the input
data.


Caching
-------

This package tries to make efficient use of memory by using iterators
and lazy evaluation where possible. However, some transformations
cannot be done without building data structures, either in memory or
on disk.

An example is the :func:`sort` function, which will either sort a
table entirely in memory, or will sort the table in memory in chunks,
writing chunks to disk and performing a final merge sort on the
chunks. (Which strategy is used will depend on the arguments passed
into the :func:`sort` function when it is called.)

In either case, the sorting can take some time, and if the sorted data
will be used more than once, it is obviously undesirable to throw away
the sorted data and start again from scratch each time. It is better
to cache the sorted data, if possible, so it can be re-used.

The :func:`sort` function and all functions which use :func:`sort`
internally provide a `cache` keyword argument, which can be used to
turn on or off the caching of sorted data.

There is also an explicit :func:`cache` function, which can be used to
cache in memory up to a configurable number of rows from a table.

.. versionchanged:: 0.16

Use of the cachetag() method is now deprecated. 

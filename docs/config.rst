Configuration
=============

The :mod:`petl.config` module contains process-wide defaults for table
inspection, sorting and error handling. Change an attribute on the module
to change a default, for example::

    import petl as etl
    etl.config.look_limit = 10

Prefer a function's explicit keyword arguments when a setting should apply
to only one operation. These defaults are shared, not thread-local or
specific to a table. Restore a changed default when it is no longer needed.

Table inspection
----------------

The following settings affect :func:`petl.util.vis.look`,
:func:`petl.util.vis.see` and :func:`petl.util.vis.display`, respectively.
The ``display_*`` settings also apply to a table's HTML representation in
a notebook.

.. list-table:: Inspection defaults
    :header-rows: 1
    :widths: 30 25 45

    * - Setting
      - Default
      - Meaning
    * - ``look_style``
      - ``'grid'``
      - Text layout: ``'grid'``, ``'simple'`` or ``'minimal'``.
    * - ``look_limit``
      - ``5``
      - Maximum number of data rows shown by ``look()``.
    * - ``look_index_header``
      - ``False``
      - Include zero-based field indices in the header.
    * - ``look_vrepr``
      - ``repr``
      - Callable used to format each value as text.
    * - ``look_width``
      - ``None``
      - Maximum text output line width; ``None`` means no width limit.
    * - ``see_limit``
      - ``5``
      - Maximum number of data rows shown by ``see()``.
    * - ``see_index_header``
      - ``False``
      - Include zero-based field indices in the column-oriented output.
    * - ``see_vrepr``
      - ``repr``
      - Callable used to format each value as text.
    * - ``display_limit``
      - ``5``
      - Maximum number of data rows shown in HTML output.
    * - ``display_index_header``
      - ``False``
      - Include zero-based field indices in the HTML header.
    * - ``display_vrepr``
      - ``petl.compat.text_type``
      - Callable used to format each value before HTML escaping
        (``str`` on Python 3).

An omitted ``limit``, or ``limit=0``, uses the corresponding configured
limit. Pass ``limit=None`` to show all rows, or a positive integer to
override the default. Showing all rows of a large table can be expensive.
For the other inspection arguments listed above, ``None`` means use the
configured default; an explicit ``False`` for ``index_header`` overrides
a configured ``True``.

Sorting
-------

``sort_buffersize`` defaults to ``100000`` data rows, not bytes. It controls
the default chunk size for :func:`petl.transform.sorts.sort`. Larger tables
are sorted using temporary files. Set ``etl.config.sort_buffersize = None``
to sort entirely in memory, which requires enough memory for the table.

An omitted ``buffersize``, or ``buffersize=None``, uses the configured
default. Pass an explicit positive integer to override it for one sort.
In particular, passing ``buffersize=None`` alone does not force an
in-memory sort when the configured default is an integer.

Error handling
--------------

``failonerror`` defaults to ``False``. It supplies the default error policy
for transformations that accept a ``failonerror`` argument, including
:func:`petl.transform.conversions.convert` and
:func:`petl.transform.maps.fieldmap`. The supported policies are documented
below. Use ``True`` when unexpected conversion errors should stop a
pipeline instead of being replaced by an error value.

An omitted ``failonerror``, or ``failonerror=None``, uses the configured
default. Passing ``False`` explicitly still overrides a global ``True``.

When defaults are read
----------------------

``look()`` and ``see()`` capture their defaults when the inspection object
is created. Sorting and conversion views likewise capture the defaults
described above when the view is created, even though data processing is
lazy. Changing the configuration later does not update these existing
objects. HTML display reads its defaults when the HTML is rendered.

For example, an explicit inspection limit overrides the configured one::

    >>> import petl as etl
    >>> table = [('value',), ('1',), ('bad',)]
    >>> previous_limit = etl.config.see_limit
    >>> try:
    ...     etl.config.see_limit = 1
    ...     str(etl.see(table))
    ...     str(etl.see(table, limit=None))
    ... finally:
    ...     etl.config.see_limit = previous_limit
    "value: '1'...\n"
    "value: '1', 'bad'\n"

The conversion policy is captured before the view is iterated::

    >>> previous_policy = etl.config.failonerror
    >>> try:
    ...     etl.config.failonerror = True
    ...     strict = etl.convert(table, 'value', int)
    ...     tolerant = etl.convert(table, 'value', int,
    ...                            failonerror=False, errorvalue='invalid')
    ...     etl.config.failonerror = False
    ...     try:
    ...         list(strict)
    ...     except ValueError:
    ...         print('The existing view still raises conversion errors')
    ...     list(tolerant)
    ... finally:
    ...     etl.config.failonerror = previous_policy
    The existing view still raises conversion errors
    [('value',), (1,), ('invalid',)]

Error policy reference
----------------------

.. automodule:: petl.config
    :members:


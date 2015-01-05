petl.fluent - Alternative notation for combining transformations
================================================================

.. automodule:: petl.fluent


``petl`` executable
-------------------

Also included in the ``petl`` distribution is a script to execute
simple transformation pipelines directly from the operating system
shell. E.g.::

    $ virtualenv petl
    $ . petl/bin/activate
    $ pip install petl
    $ petl "dummytable().tocsv()" > dummy.csv
    $ cat dummy.csv | petl "fromcsv().cut('foo', 'baz').selectgt('baz', 0.5).head().data().totsv()"

The ``petl`` script is extremely simple, it expects a single
positional argument, which is evaluated as Python code but with all of
the petl.fluent functions imported.



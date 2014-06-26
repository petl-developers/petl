.. module:: petl.fluent
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

petl.fluent - Alternative notation for combining transformations
================================================================

.. versionadded:: 0.6

The module :mod:`petl.fluent` provides all of the functions present in
the root :mod:`petl` module, but with modifications to allow them to
be used in a fluent style. E.g.::

   >>> from petl.fluent import *
   >>> t0 = dummytable()
   >>> t0.look()
   +-------+-----------+----------------------+
   | 'foo' | 'bar'     | 'baz'                |
   +=======+===========+======================+
   | 61    | 'oranges' | 0.41684297441746143  |
   +-------+-----------+----------------------+
   | 42    | 'bananas' | 0.5424838757229734   |
   +-------+-----------+----------------------+
   | 55    | 'pears'   | 0.044730394239418825 |
   +-------+-----------+----------------------+
   | 63    | 'apples'  | 0.6553751878324998   |
   +-------+-----------+----------------------+
   | 57    | 'pears'   | 0.33151097448517963  |
   +-------+-----------+----------------------+
   | 57    | 'apples'  | 0.2152565282912028   |
   +-------+-----------+----------------------+
   | 45    | 'bananas' | 0.1478840303008977   |
   +-------+-----------+----------------------+
   | 79    | 'pears'   | 0.14301990499723238  |
   +-------+-----------+----------------------+
   | 11    | 'pears'   | 0.16801320344526383  |
   +-------+-----------+----------------------+
   | 96    | 'oranges' | 0.3004187573856759   |
   +-------+-----------+----------------------+
   
   >>> t1 = (t0
   ...     .convert('bar', 'upper')
   ...     .addfield('quux', 42)
   ...     .addfield('spong', lambda row: row.foo * row.quux)
   ...     .selecteq('bar', 'APPLES')
   ... )
   >>> t1.look()
   +-------+----------+----------------------+--------+---------+
   | 'foo' | 'bar'    | 'baz'                | 'quux' | 'spong' |
   +=======+==========+======================+========+=========+
   | 63    | 'APPLES' | 0.6553751878324998   | 42     | 2646    |
   +-------+----------+----------------------+--------+---------+
   | 57    | 'APPLES' | 0.2152565282912028   | 42     | 2394    |
   +-------+----------+----------------------+--------+---------+
   | 87    | 'APPLES' | 0.9045902500660937   | 42     | 3654    |
   +-------+----------+----------------------+--------+---------+
   | 5     | 'APPLES' | 0.6915135568859515   | 42     | 210     |
   +-------+----------+----------------------+--------+---------+
   | 28    | 'APPLES' | 0.8440288073976338   | 42     | 1176    |
   +-------+----------+----------------------+--------+---------+
   | 32    | 'APPLES' | 0.047452310539432774 | 42     | 1344    |
   +-------+----------+----------------------+--------+---------+
   | 93    | 'APPLES' | 0.8100969279893147   | 42     | 3906    |
   +-------+----------+----------------------+--------+---------+
   | 94    | 'APPLES' | 0.8216793407511486   | 42     | 3948    |
   +-------+----------+----------------------+--------+---------+
   | 94    | 'APPLES' | 0.7911584363109934   | 42     | 3948    |
   +-------+----------+----------------------+--------+---------+
   | 34    | 'APPLES' | 0.18846546302867728  | 42     | 1428    |
   +-------+----------+----------------------+--------+---------+

Alternatively, if you don't want to import all petl function names
into the root namespace, you can just import the module and use the
``wrap()`` function, e.g.::

     >>> import petl.fluent as etl
     >>> l = [['foo', 'bar'], ['a', 1], ['b', 3]]
     >>> tbl = etl.wrap(l)
     >>> tbl.look()
     +-------+-------+
     | 'foo' | 'bar' |
     +=======+=======+
     | 'a'   | 1     |
     +-------+-------+
     | 'b'   | 3     |
     +-------+-------+
     
     >>> tbl.cut('foo').look()
     +-------+
     | 'foo' |
     +=======+
     | 'a'   |
     +-------+
     | 'b'   |
     +-------+
     
     >>> tbl.tocsv('test.csv')
     >>> etl.fromcsv('test.csv').look()
     +-------+-------+
     | 'foo' | 'bar' |
     +=======+=======+
     | 'a'   | '1'   |
     +-------+-------+
     | 'b'   | '3'   |
     +-------+-------+

.. versionchanged:: 0.21

The recommended import and wrapping pattern (described above) has changed, see https://github.com/alimanfoo/petl/issues/230 for more details.


``petl`` executable
-------------------

.. versionadded:: 0.10

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



.. module:: petl.interactive
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

petl.interactive - Optimisations for Use in Interactive Mode
============================================================

.. versionadded:: 0.5 

The module :mod:`petl.interactive` provides all of the functions
present in the root :mod:`petl` module, but with a couple of
optimisations for use within an interactive session.

The main optimisation is that some caching is done by default,
such that the first 10000 rows of any table are cached in
memory the first time they are requested. This usually provides a
better experience when building up a transformation pipeline one step
at a time, where you are examining the outputs of each intermediate
step as its written via :func:`look` or :func:`see`. I.e., as each new
step is added and the output examined, as long as less than 10000 rows
are requested, only that new step will actually be executed, and none
of the upstream transformations will be repeated, because the outputs
from previous steps will have been cached. 

The default cache size can be changed by setting
``petl.interactive.cachesize`` to an integer value.

Also, by default, the :func:`look` function is used to generate a
representation of tables. So you don't need to type, e.g., ``>>>
look(mytable)``, you can just type ``>>> mytable``. The default
representation function can be changed by setting
``petl.interactive.representation``, e.g.,
``petl.interactive.representation = petl.see``, or
``petl.interactive.representation = None`` to disable this behaviour.

Finally, this module extends :mod:`petl.fluent` so you can use the
fluent style if you wish, e.g.::

     >>> import petl.interactive as etl
     >>> l = [['foo', 'bar'], ['a', 1], ['b', 3]]
     >>> tbl = etl.wrap(l)
     >>> tbl
     +-------+-------+
     | 'foo' | 'bar' |
     +=======+=======+
     | 'a'   | 1     |
     +-------+-------+
     | 'b'   | 3     |
     +-------+-------+

     >>> tbl.cut('foo')
     +-------+
     | 'foo' |
     +=======+
     | 'a'   |
     +-------+
     | 'b'   |
     +-------+

     >>> tbl.tocsv('test.csv')
     >>> etl.fromcsv('test.csv')
     +-------+-------+
     | 'foo' | 'bar' |
     +=======+=======+
     | 'a'   | '1'   |
     +-------+-------+
     | 'b'   | '3'   |
     +-------+-------+

.. versionchanged:: 0.21

The recommended import and wrapping pattern (described above) has changed, see https://github.com/alimanfoo/petl/issues/230 for more details.

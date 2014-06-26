.. module:: petl.push
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>

petl.push - Branching Pipelines
===============================

.. versionadded:: 0.10

Introduction
------------

This module provides some functions for setting up branching data
transformation pipelines.

The general pattern is to define the pipeline, connecting components
together via the ``pipe()`` method call, then pushing data through the
pipeline via the ``push()`` method call at the top of the
pipeline. E.g.::

    >>> from petl import fromcsv
    >>> source = fromcsv('fruit.csv')
    >>> from petl.push import *
    >>> p = partition('fruit')
    >>> p.pipe('orange', tocsv('oranges.csv'))
    >>> p.pipe('banana', tocsv('bananas.csv'))
    >>> p.push(source)

The pipe operator can also be used to connect components in the
pipeline, by analogy with the use of the pipe character in unix/linux
shells, e.g.::

    >>> from petl import fromcsv
    >>> source = fromcsv('fruit.csv')
    >>> from petl.push import *
    >>> p = partition('fruit')
    >>> p | ('orange', tocsv('oranges.csv')
    >>> p | ('banana', tocsv('bananas.csv')
    >>> p.push(source)

Push Functions
--------------

.. autofunction:: petl.push.partition
.. autofunction:: petl.push.sort
.. autofunction:: petl.push.duplicates
.. autofunction:: petl.push.unique
.. autofunction:: petl.push.diff
.. autofunction:: petl.push.tocsv
.. autofunction:: petl.push.totsv
.. autofunction:: petl.push.topickle


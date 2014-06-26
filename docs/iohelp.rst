.. module:: petl
.. moduleauthor:: Alistair Miles <alimanfoo@googlemail.com>


I/O helper classes
==================

The following classes are helpers for extract (``from...()``) and load
(``to...()``) functions that use a file-like data source. An instance
of any of the following classes can be used as the ``source`` argument
to data extraction functions like :func:`fromcsv` etc., with the
exception of :class:`StdoutSource` which is write-only. An instance of
any of the following classes can also be used as the ``source``
argument to data loading functions like :func:`tocsv` etc., with the
exception of :class:`StdinSource`, :class:`URLSource` and
:class:`PopenSource` which are read-only. The behaviour of each source
can usually be configured by passing arguments to the constructor, see
the source code of the :mod:`petl.io` module for full details.

.. autoclass:: petl.FileSource
.. autoclass:: petl.GzipSource
.. autoclass:: petl.BZ2Source
.. autoclass:: petl.ZipSource
.. autoclass:: petl.StdinSource
.. autoclass:: petl.StdoutSource
.. autoclass:: petl.URLSource
.. autoclass:: petl.StringSource
.. autoclass:: petl.PopenSource



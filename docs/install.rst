Installation
============

.. _intro_installation:

Getting Started
---------------

This package is available from the `Python Package Index
<http://pypi.python.org/pypi/petl>`_. If you have `pip
<https://pip.pypa.io/>`_ you should be able to do::

    $ pip install petl

You can also download manually, extract and run ``python setup.py
install``.

To verify the installation, the test suite can be run with `pytest
<https://docs.pytest.org/>`_, e.g.::

    $ pip install pytest
    $ pytest -v petl

:mod:`petl` has been tested with Python versions 2.7 and 3.6-3.13
under Linux, MacOS, and Windows operating systems.

.. _intro_dependencies:

Dependencies and extensions
---------------------------

This package is written in pure Python and has no installation requirements
other than the Python core modules.

Some domain-specific and/or experimental extensions to :mod:`petl` are
available from the petlx_ package.

.. _petlx: http://petlx.readthedocs.org

Some of the functions in this package require installation of third party
packages. These packages are indicated in the relevant parts of the 
documentation for each file format.

Also is possible to install some of dependencies when installing `petl` by
specifying optional extra features, e.g.::

    $ pip install petl['avro', 'interval', 'remote']

The available extra features are:

db
    For using records from :ref:`Databases <io_db>` with `SQLAlchemy`.

    Note that is also required installing the package for the desired database.

interval
    For using :ref:`Interval transformations <transform_intervals>`
    with `intervaltree`

avro
  For using :ref:`Avro files <io_avro>` with `fastavro`

pandas
  For using :ref:`DataFrames <io_pandas>` with `pandas`

numpy
  For using :ref:`Arrays <io_numpy>` with `numpy`

xls
  For using :ref:`Excel/LO files <io_xls>` with `xlrd`/`xlwt`

xlsx
  For using :ref:`Excel/LO files <io_xlsx>` with `openpyxl`

xpath
  For using :ref:`XPath expressions <io_xml>` with `lxml`

bcolz
  For using :ref:`Bcolz ctables <io_bcolz>` with `bcolz`

whoosh
  For using :ref:`Text indexes <io_whoosh>` with `whoosh`

hdf5
  For using :ref:`HDF5 files <io_pytables>` with `PyTables`.

  Note that also are additional software to be installed.

remote
  For reading and writing from :ref:`Remote Sources <io_remotes>` with `fsspec`.

  Note that `fsspec` also depends on other packages for providing support for 
  each protocol as described in :class:`petl.io.remotes.RemoteSource`.

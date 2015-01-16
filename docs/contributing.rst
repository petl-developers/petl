Contributing
============

Contributions to :mod:`petl` are welcome in any form, please feel free
to email the python-etl@googlegroups.com mailing list if you have some
code or ideas you'd like to discuss.

Please note that the :mod:`petl` package is intended as a stable,
general purpose library for ETL work. If you would like to extend
:mod:`petl` with functionality that is domain-specific, or if you have
an experimental or tentative feature that is not yet ready for
inclusion in the core :mod:`petl` package but you would like to
distribute it, please contribute to the `petlx
<http://petlx.readthedocs.org>`_ project instead, or distribute your
code as a separate package.

If you are thinking of developing or modifying the :mod:`petl` code base in
any way, here is some information on how to set up your development
environment to run tests etc.

Running the test suite
----------------------

The main :mod:`petl` test suite can be run with `nose
<https://nose.readthedocs.org/>`_. E.g., assuming you have the source code
repository cloned to the current working directory, you can run the test
suite with::

    $ pip install nose
    $ nosetests -v

Currently :mod:`petl` supports Python 2.6, Python 2.7 and Python 3.4,
so the tests should pass under all three Python versions.

Dependencies
------------

To keep installation as simple as possible on different platforms,
:mod:`petl` has no installation dependencies. Most functionality also
depends only on the Python core libraries.

Some :mod:`petl` functions depend on third party packages, however
these should be kept as optional requirements. Any tests for modules
requiring third party packages should be written so that they are
skipped if the packages are not available. See the existing tests for
examples of how to do this.


Running database tests
----------------------

There are some additional tests within the test suite that require
database servers to be setup correctly on the local host. To run these
additional tests, make sure you have both MySQL and PostgreSQL servers
running locally, and have created a user "petl" with password "test"
and all permissions granted on a database called "petl".  Install
dependencies::

    $ pip install pymysql psycopg2 sqlalchemy

If these dependencies are not installed, or if a local database server
is not found, these tests are skipped.

Running doctests
----------------

Doctests in docstrings should (almost) all be runnable, and should
pass if run with Python 3.4. Doctests can be run with `nose
<https://nose.readthedocs.org/>`_. See the tox.ini file for example
doctest commands.

Building the documentation
--------------------------

Documentation is built with `sphinx <http://sphinx-doc.org/>`_. To build::

    $ pip install sphinx
    $ cd docs
    $ make html

Built docs can then be found in the ``docs/_build/html/`` directory.

Automatically running all tests
-------------------------------

All of the above tests can be run automatically using `tox
<https://tox.readthedocs.org/>`_. You will need binaries for Python
2.6, Python 2.7 and Python 3.4 available on your system.

To run all tests **without** installing any of the optional
dependencies, do::

    $ tox -e py26,py27,py34,doctests

To run the entire test suite, including installation of **all**
optional dependencies, do::

    $ tox

The first time you run this it will take some while all the optional
dependencies are installed in each environment.

Contributing code via GitHub
----------------------------

The best way to contribute code is via a GitHub pull request.

Please include unit tests with any code contributed.

If you are able, please run tox and ensure that all the above tests pass
before making a pull request.

Thanks!

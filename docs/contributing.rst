Contributing
============

Contributions to :mod:`petl` are welcome in any form, please feel free to
email the python-etl@googlegroups.com mailing list or Alistair Miles
<alimanfoo@googlemail.com> if you have some code or ideas you'd like to discuss.

Please note that the :mod:`petl` module should be kept free from any
dependencies other than the core Python libraries. If you would like to
contribute code that has additional dependencies, or is somewhat
experimental or tentative, please contribute to the
`petlx <http://petlx.readthedocs.org>`_ project instead.

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

Currently :mod:`petl` supports Python 2.6, Python 2.7 and Python 3.4, so the
tests should pass under all three Python versions. See the section below for
suggestions on how to automatically run the test suite under different Python
versions.

Running database tests
----------------------

There are some additional tests within the test suite that do not run by
default, because they require database servers to be setup correctly on the
local host. To run these additional tests, make sure you have both MySQL and
PostgreSQL servers running locally, and have created a user "petl" with
password "test" and all permissions granted on a database called "petl".
Install dependencies::

    $ pip install pymysql psycopg2 sqlalchemy

...then run these additional tests::

    $ nosetests -v petl.test.io.test_db_server:dbtest_mysql
    $ nosetests -v petl.test.io.test_db_server:dbtest_postgresql

Running doctests
----------------

Doctests in docstrings should (almost) all be runnable, and should pass if
run with Python 3.4 only. Doctests can be run with `nose
<https://nose.readthedocs.org/>`_, e.g.::

    $ nosetests -v --with-doctest --doctest-options=+NORMALIZE_WHITESPACE petl/io --stop --ignore-files=csv_py2 --ignore-files=db
    $ nosetests -v --with-doctest --doctest-options=+NORMALIZE_WHITESPACE petl/transform --stop
    $ nosetests -v --with-doctest --doctest-options=+NORMALIZE_WHITESPACE petl/util --stop --ignore-files=timing

Building the documentation
--------------------------

Documentation is built with `sphinx <http://sphinx-doc.org/>`_. To build::

    $ pip install sphinx
    $ cd docs
    $ make html

Built docs can then be found in the ``docs/_build/html/`` directory.

Automatically running all tests
-------------------------------

All of the above tests can be run automatically using
`tox <https://tox.readthedocs.org/>`_. You will need binaries for Python 2.6,
Python 2.7 and Python 3.4 available on your system. Then you can do::

    $ pip install tox
    $ tox

Contributing code via GitHub
----------------------------

The best way to contribute code is via a GitHub pull request.

Please include unit tests with any code contributed.

If you are able, please run tox and ensure that all the above tests pass
before making a pull request.

Thanks!

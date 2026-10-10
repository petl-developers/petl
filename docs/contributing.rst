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

The main :mod:`petl` test suite can be run with `pytest
<https://docs.pytest.org/en/stable/>`_. E.g., assuming you have the source code
repository cloned to the current working directory, you can run the test
suite with::

    $ pip install -r requirements-tests.txt
    $ pytest -v petl

The test environments and their Python interpreters are defined in
``tox.ini``.

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

Doctests in docstrings can be run with pytest's ``--doctest-modules``
option. For example, the Python 3.14 doctest environment runs these
examples with coverage::

    $ tox -e py314-doctest

Building the documentation
--------------------------

Documentation is built with `sphinx <http://sphinx-doc.org/>`_. To build::

    $ pip install -r requirements-docs.txt
    $ cd docs
    $ make html

Built docs can then be found in the ``docs/_build/html/`` directory.

Automatically running all tests
-------------------------------

The test suite and documentation builds can be run automatically using
`tox <https://tox.readthedocs.org/>`_. Install the Python interpreter
for each selected environment. For example, with Python 3.14 installed::

    $ tox -e py314,py314-docs

The test environment installs ``requirements-tests.txt`` and
``requirements-formats.txt``. The documentation environment installs
``requirements-docs.txt``. Tox creates these environments and installs
their dependencies on the first run.

Running ``tox`` without ``-e`` selects the default environments listed
in ``tox.ini``. Database and remote filesystem tests have separate
``database``, ``remote`` and ``compose`` environments; see ``tox.ini``
for their dependencies and service setup commands.

Contributing code via GitHub
----------------------------

The best way to contribute code is via a GitHub pull request.

Please include unit tests with any code contributed.

If you are able, please run tox and ensure that all the above tests pass
before making a pull request.

Thanks!

Guidelines for core developers
------------------------------

Before merging a pull request that includes new or modified code, all
items in the `PR checklist <https://github.com/petl-developers/petl/blob/master/.github/PULL_REQUEST_TEMPLATE.md>`_
should be complete.

Pull requests containing new and/or modified code that is anything
other than a trivial bug fix should be approved by at least one core
developer before being merged. If a core developer is making a PR
themselves, it is OK to merge their own PR if they first allow some
reasonable time (e.g., at least one working day) for other core devs
to raise any objections, e.g., by posting a comment like "merging soon
if no objections" on the PR. If the PR contains substantial new
features or modifications, the PR author might want to allow a little
more time to ensure other core devs have an opportunity to see it.

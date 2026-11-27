.. module:: petl

petl - Extract, Transform and Load
===================================

:mod:`petl` is a general purpose Python package for extracting, transforming
and loading tables of data.

.. image:: petl-architecture.png
    :width: 750
    :align: center
    :alt: petl use cases diagram

Resources
---------

- Documentation: http://petl.readthedocs.org/
- Mailing List: http://groups.google.com/group/python-etl
- Source Code: https://github.com/petl-developers/petl
- Download: 
  - PyPI: http://pypi.python.org/pypi/petl
  - Conda Forge:https://anaconda.org/conda-forge/petl

.. note::

   - Version 2.0 will be a major milestone for :mod:`petl`.
   - This version will introduce some changes that could affect current behaviour.
   - We will try to keep compatibility to the maximum possible, except 
     when the current behavior is inconsistent or have shortcomings.
   - The minimum supported version is Python 3.6.
   - Version 1.8.x drops support for Python 2.7.
   - Bugfixes for users who still need Python 2.7 continue on the ``1.7.x``
     maintenance branch.

Getting Help
-------------

Please feel free to ask questions via the mailing list
(python-etl@googlegroups.com).

To report installation problems, bugs or any other issues please email
python-etl@googlegroups.com or `raise an issue on GitHub
<https://github.com/petl-developers/petl/issues/new>`_.

For an example of :mod:`petl` in use, see the `case study on comparing tables
<http://nbviewer.ipython.org/github/petl-developers/petl/tree/master/notes/case_study_1.ipynb>`_.

The `ETL with Python tutorial
<https://github.com/dimgold/ETL_with_Python/blob/master/ETL_with_Python.ipynb>`_
uses a Python 2 notebook to walk through reading JSON, transforming tables and
loading the results into MySQL.

Contents
--------

For an alphabetic list of all functions in the package,
see the :ref:`genindex`.

.. toctree::
   :maxdepth: 2

   install
   intro
   io
   transform
   util
   config
   contributing
   acknowledgments
   related_work
   changes

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

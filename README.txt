petl - Extract, Transform and Load (Tables of Data)
===================================================

`petl` is a tentative Python module for extracting, transforming and
loading tables of data.

- Download: http://pypi.python.org/pypi/petl
- Source: https://github.com/alimanfoo/petl
- Docs: http://readthedocs.org/docs/petl

There are already many Python modules in this space, but none (that
I've found) seem to cover the full range of transformations required,
and many use different data structures to represent tables of data,
meaning that functions from different modules cannot easily be
mixed. It would be magnificent if there was a single Python package
that brought some of this together, or at least if there were some
consistency across relevant packages so that, over time, the Python
ETL toolbox would grow into something even more awesome than it
already is.

Currently, this module is somewhere between pipe dream, fantasy,
wishlist, experiment and placeholder (until someone with far more
talent, time and energy than me comes along and takes on the
mantle). It's not realistic to expect that I alone will be able to
come close to covering the full range of ETL functionality required by
a diverse range of projects in any reasonable space of time. However,
I would like to do at least the following:

- Try to compile a consistent glossary/terminology for describing
  transformations on tables of data (e.g., what do "map", "cut",
  "slice", , "join", "merge", "fold", "transpose", "split", "melt",
  "cast", ..., mean?) with cross-references to what terminology is
  used in other tools/modules.

- Work towards an interface/protocol for objects that can consume
  and/or generate tabular data, that is (a) as bloody simple as
  possible, so that writing transformation scripts, and developing new
  transformation functions, is possible for people like me (who aren't
  Python gods), (b) compatible with the full range of transformations
  identified in the wish list, (c) compatible with chaining/pipelining
  multiple transformations, and (d) workable for transformations on
  big(-ish) datasets.

- Explore and develop a deeper understanding of implementation
  strategies for a range of tabular data transformations.

If any of this is in any way interesting to you, then I've set up a
mailing list at http://groups.google.com/group/python-etl - not just
for this package, but for general discussion about Pythonic ways of
working with table-shaped data. Feel free to jump in, or to redirect
me to other projects or forums that have already got this all covered.

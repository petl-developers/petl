# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import


import inspect


from petl.util.base import Table


def todataframe(table, index=None, exclude=None, columns=None,
                coerce_float=False, nrows=None):
    """
    Load data from the given `table` into a
    `pandas <http://pandas.pydata.org/>`_ DataFrame. E.g.::

        >>> import petl as etl
        >>> table = [('foo', 'bar', 'baz'),
        ...          ('apples', 1, 2.5),
        ...          ('oranges', 3, 4.4),
        ...          ('pears', 7, .1)]
        >>> df = etl.todataframe(table)
        >>> df
               foo  bar  baz
        0   apples    1  2.5
        1  oranges    3  4.4
        2    pears    7  0.1

    """
    import pandas as pd
    it = iter(table)
    try:
        header = next(it)
    except StopIteration:
        header = None  # Will create an Empty DataFrame
    if columns is None:
        columns = header
    return pd.DataFrame.from_records(it, index=index, exclude=exclude,
                                     columns=columns, coerce_float=coerce_float,
                                     nrows=nrows)


Table.todataframe = todataframe
Table.todf = todataframe


def fromdataframe(df, include_index=False):
    """
    Extract a table from a `pandas <http://pandas.pydata.org/>`_ DataFrame.
    E.g.::

        >>> import petl as etl
        >>> import pandas as pd
        >>> records = [('apples', 1, 2.5), ('oranges', 3, 4.4), ('pears', 7, 0.1)]
        >>> df = pd.DataFrame.from_records(records, columns=('foo', 'bar', 'baz'))
        >>> table = etl.fromdataframe(df)
        >>> table
        +-----------+-----+-----+
        | foo       | bar | baz |
        +===========+=====+=====+
        | 'apples'  |   1 | 2.5 |
        +-----------+-----+-----+
        | 'oranges' |   3 | 4.4 |
        +-----------+-----+-----+
        | 'pears'   |   7 | 0.1 |
        +-----------+-----+-----+

    """

    return DataFrameView(df, include_index=include_index)


class DataFrameView(Table):

    def __init__(self, df, include_index=False):
        assert hasattr(df, 'columns') \
            and hasattr(df, 'iterrows') \
            and inspect.ismethod(df.iterrows), \
            'bad argument, expected pandas.DataFrame, found %r' % df
        self.df = df
        self.include_index = include_index

    def __iter__(self):
        if self.include_index:
            yield ('index',) + tuple(self.df.columns)
            for i, row in self.df.iterrows():
                yield (i,) + tuple(row)
        else:
            yield tuple(self.df.columns)
            for _, row in self.df.iterrows():
                yield tuple(row)

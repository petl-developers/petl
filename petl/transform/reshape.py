from __future__ import absolute_import, print_function, division


import itertools
import collections
import operator
from petl.compat import next, text_type


from petl.comparison import comparable_itemgetter
from petl.util.base import Table, rowgetter, values, itervalues, \
    header, data, asindices
from petl.transform.sorts import sort


def melt(table, key=None, variables=None, variablefield='variable',
         valuefield='value'):
    """
    Reshape a table, melting fields into data. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'gender', 'age'],
        ...           [1, 'F', 12],
        ...           [2, 'M', 17],
        ...           [3, 'M', 16]]
        >>> table2 = etl.melt(table1, 'id')
        >>> table2.lookall()
        +----+----------+-------+
        | id | variable | value |
        +====+==========+=======+
        |  1 | 'gender' | 'F'   |
        +----+----------+-------+
        |  1 | 'age'    |    12 |
        +----+----------+-------+
        |  2 | 'gender' | 'M'   |
        +----+----------+-------+
        |  2 | 'age'    |    17 |
        +----+----------+-------+
        |  3 | 'gender' | 'M'   |
        +----+----------+-------+
        |  3 | 'age'    |    16 |
        +----+----------+-------+

        >>> # compound keys are supported
        ... table3 = [['id', 'time', 'height', 'weight'],
        ...           [1, 11, 66.4, 12.2],
        ...           [2, 16, 53.2, 17.3],
        ...           [3, 12, 34.5, 9.4]]
        >>> table4 = etl.melt(table3, key=['id', 'time'])
        >>> table4.lookall()
        +----+------+----------+-------+
        | id | time | variable | value |
        +====+======+==========+=======+
        |  1 |   11 | 'height' |  66.4 |
        +----+------+----------+-------+
        |  1 |   11 | 'weight' |  12.2 |
        +----+------+----------+-------+
        |  2 |   16 | 'height' |  53.2 |
        +----+------+----------+-------+
        |  2 |   16 | 'weight' |  17.3 |
        +----+------+----------+-------+
        |  3 |   12 | 'height' |  34.5 |
        +----+------+----------+-------+
        |  3 |   12 | 'weight' |   9.4 |
        +----+------+----------+-------+

        >>> # a subset of variable fields can be selected
        ... table5 = etl.melt(table3, key=['id', 'time'],
        ...                   variables=['height'])
        >>> table5.lookall()
        +----+------+----------+-------+
        | id | time | variable | value |
        +====+======+==========+=======+
        |  1 |   11 | 'height' |  66.4 |
        +----+------+----------+-------+
        |  2 |   16 | 'height' |  53.2 |
        +----+------+----------+-------+
        |  3 |   12 | 'height' |  34.5 |
        +----+------+----------+-------+

    See also :func:`petl.transform.reshape.recast`.

    """

    return MeltView(table, key=key, variables=variables,
                    variablefield=variablefield,
                    valuefield=valuefield)


Table.melt = melt


class MeltView(Table):

    def __init__(self, source, key=None, variables=None,
                 variablefield='variable', valuefield='value'):
        self.source = source
        self.key = key
        self.variables = variables
        self.variablefield = variablefield
        self.valuefield = valuefield

    def __iter__(self):
        return itermelt(self.source, self.key, self.variables,
                        self.variablefield, self.valuefield)


def itermelt(source, key, variables, variablefield, valuefield):
    if key is None and variables is None:
        raise ValueError('either key or variables must be specified')

    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return

    # determine key and variable field indices
    key_indices = variables_indices = None
    if key is not None:
        key_indices = asindices(hdr, key)
    if variables is not None:
        if not isinstance(variables, (list, tuple)):
            variables = (variables,)
        variables_indices = asindices(hdr, variables)

    if key is None:
        # assume key is fields not in variables
        key_indices = [i for i in range(len(hdr))
                       if i not in variables_indices]
    if variables is None:
        # assume variables are fields not in key
        variables_indices = [i for i in range(len(hdr))
                             if i not in key_indices]
        variables = [hdr[i] for i in variables_indices]

    getkey = rowgetter(*key_indices)

    # determine the output fields
    outhdr = [hdr[i] for i in key_indices]
    outhdr.append(variablefield)
    outhdr.append(valuefield)
    yield tuple(outhdr)

    # construct the output data
    for row in it:
        k = getkey(row)
        for v, i in zip(variables, variables_indices):
            try:
                o = list(k)  # populate with key values initially
                o.append(v)  # add variable
                o.append(row[i])  # add value
                yield tuple(o)
            except IndexError:
                # row is missing this value, and melt() should yield no row
                pass


def recast(table, key=None, variablefield='variable', valuefield='value',
           samplesize=1000, reducers=None, missing=None):
    """
    Recast molten data. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'variable', 'value'],
        ...           [3, 'age', 16],
        ...           [1, 'gender', 'F'],
        ...           [2, 'gender', 'M'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
        >>> table2 = etl.recast(table1)
        >>> table2
        +----+-----+--------+
        | id | age | gender |
        +====+=====+========+
        |  1 |  12 | 'F'    |
        +----+-----+--------+
        |  2 |  17 | 'M'    |
        +----+-----+--------+
        |  3 |  16 | 'M'    |
        +----+-----+--------+

        >>> # specifying variable and value fields
        ... table3 = [['id', 'vars', 'vals'],
        ...           [3, 'age', 16],
        ...           [1, 'gender', 'F'],
        ...           [2, 'gender', 'M'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
        >>> table4 = etl.recast(table3, variablefield='vars', valuefield='vals')
        >>> table4
        +----+-----+--------+
        | id | age | gender |
        +====+=====+========+
        |  1 |  12 | 'F'    |
        +----+-----+--------+
        |  2 |  17 | 'M'    |
        +----+-----+--------+
        |  3 |  16 | 'M'    |
        +----+-----+--------+

        >>> # if there are multiple values for each key/variable pair, and no
        ... # reducers function is provided, then all values will be listed
        ... table6 = [['id', 'time', 'variable', 'value'],
        ...           [1, 11, 'weight', 66.4],
        ...           [1, 14, 'weight', 55.2],
        ...           [2, 12, 'weight', 53.2],
        ...           [2, 16, 'weight', 43.3],
        ...           [3, 12, 'weight', 34.5],
        ...           [3, 17, 'weight', 49.4]]
        >>> table7 = etl.recast(table6, key='id')
        >>> table7
        +----+--------------+
        | id | weight       |
        +====+==============+
        |  1 | [66.4, 55.2] |
        +----+--------------+
        |  2 | [53.2, 43.3] |
        +----+--------------+
        |  3 | [34.5, 49.4] |
        +----+--------------+

        >>> # multiple values can be reduced via an aggregation function
        ... def mean(values):
        ...     return float(sum(values)) / len(values)
        ...
        >>> table8 = etl.recast(table6, key='id', reducers={'weight': mean})
        >>> table8
        +----+--------------------+
        | id | weight             |
        +====+====================+
        |  1 | 60.800000000000004 |
        +----+--------------------+
        |  2 |              48.25 |
        +----+--------------------+
        |  3 |              41.95 |
        +----+--------------------+

        >>> # missing values are padded with whatever is provided via the
        ... # missing keyword argument (None by default)
        ... table9 = [['id', 'variable', 'value'],
        ...           [1, 'gender', 'F'],
        ...           [2, 'age', 17],
        ...           [1, 'age', 12],
        ...           [3, 'gender', 'M']]
        >>> table10 = etl.recast(table9, key='id')
        >>> table10
        +----+------+--------+
        | id | age  | gender |
        +====+======+========+
        |  1 |   12 | 'F'    |
        +----+------+--------+
        |  2 |   17 | None   |
        +----+------+--------+
        |  3 | None | 'M'    |
        +----+------+--------+

    Note that the table is scanned once to discover variables, then a second
    time to reshape the data and recast variables as fields. How many rows are
    scanned in the first pass is determined by the `samplesize` argument.

    See also :func:`petl.transform.reshape.melt`.

    """

    return RecastView(table, key=key, variablefield=variablefield,
                      valuefield=valuefield, samplesize=samplesize,
                      reducers=reducers, missing=missing)


Table.recast = recast


class RecastView(Table):

    def __init__(self, source, key=None, variablefield='variable',
                 valuefield='value', samplesize=1000, reducers=None,
                 missing=None):
        self.source = source
        self.key = key
        self.variablefield = variablefield
        self.valuefield = valuefield
        self.samplesize = samplesize
        if reducers is None:
            self.reducers = dict()
        else:
            self.reducers = reducers
        self.missing = missing

    def __iter__(self):
        return iterrecast(self.source, self.key, self.variablefield,
                          self.valuefield, self.samplesize, self.reducers,
                          self.missing)


def iterrecast(source, key, variablefield, valuefield,
               samplesize, reducers, missing):

    # TODO only make one pass through the data

    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))

    # normalise some stuff
    keyfields = key
    variablefields = variablefield  # N.B., could be more than one

    # normalise key fields
    if keyfields and not isinstance(keyfields, (list, tuple)):
        keyfields = (keyfields,)

    # normalise variable fields
    if variablefields:
        if isinstance(variablefields, dict):
            pass  # handle this later
        elif not isinstance(variablefields, (list, tuple)):
            variablefields = (variablefields,)

    # infer key fields
    if not keyfields:
        # assume keyfields is fields not in variables
        keyfields = [f for f in flds
                     if f not in variablefields and f != valuefield]

    # infer key fields
    if not variablefields:
        # assume variables are fields not in keyfields
        variablefields = [f for f in flds
                          if f not in keyfields and f != valuefield]

    # sanity checks
    assert valuefield in flds, 'invalid value field: %s' % valuefield
    assert valuefield not in keyfields, 'value field cannot be keyfields'
    assert valuefield not in variablefields, \
        'value field cannot be variable field'
    for f in keyfields:
        assert f in flds, 'invalid keyfields field: %s' % f
    for f in variablefields:
        assert f in flds, 'invalid variable field: %s' % f

    # we'll need these later
    valueindex = flds.index(valuefield)
    keyindices = [flds.index(f) for f in keyfields]
    variableindices = [flds.index(f) for f in variablefields]

    # determine the actual variable names to be cast as fields
    if isinstance(variablefields, dict):
        # user supplied dictionary
        variables = variablefields
    else:
        variables = collections.defaultdict(set)
        # sample the data to discover variables to be cast as fields
        for row in itertools.islice(it, 0, samplesize):
            for i, f in zip(variableindices, variablefields):
                variables[f].add(row[i])
        for f in variables:
            # turn from sets to sorted lists
            variables[f] = sorted(variables[f])

    # finished the first pass

    # determine the output fields
    outhdr = list(keyfields)
    for f in variablefields:
        outhdr.extend(variables[f])
    yield tuple(outhdr)

    # output data

    source = sort(source, key=keyfields)
    it = itertools.islice(source, 1, None)  # skip header row
    getsortablekey = comparable_itemgetter(*keyindices)
    getactualkey = operator.itemgetter(*keyindices)

    # process sorted data in newfields
    groups = itertools.groupby(it, key=getsortablekey)
    for _, group in groups:
        # may need to iterate over the group more than once
        group = list(group)
        # N.B., key returned by groupby may be wrapped as SortableItem, we want
        # to output the actual key value, get it from the first row in the group
        key_value = getactualkey(group[0])
        if len(keyfields) > 1:
            out_row = list(key_value)
        else:
            out_row = [key_value]
        for f, i in zip(variablefields, variableindices):
            for variable in variables[f]:
                # collect all values for the current variable
                vals = [r[valueindex] for r in group if r[i] == variable]
                if len(vals) == 0:
                    val = missing
                elif len(vals) == 1:
                    val = vals[0]
                else:
                    if variable in reducers:
                        redu = reducers[variable]
                    else:
                        redu = list  # list all values
                    val = redu(vals)
                out_row.append(val)
        yield tuple(out_row)


def transpose(table):
    """
    Transpose rows into columns. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'colour'],
        ...           [1, 'blue'],
        ...           [2, 'red'],
        ...           [3, 'purple'],
        ...           [5, 'yellow'],
        ...           [7, 'orange']]
        >>> table2 = etl.transpose(table1)
        >>> table2
        +----------+--------+-------+----------+----------+----------+
        | id       | 1      | 2     | 3        | 5        | 7        |
        +==========+========+=======+==========+==========+==========+
        | 'colour' | 'blue' | 'red' | 'purple' | 'yellow' | 'orange' |
        +----------+--------+-------+----------+----------+----------+

    See also :func:`petl.transform.reshape.recast`.

    """

    return TransposeView(table)


Table.transpose = transpose


class TransposeView(Table):

    def __init__(self, source):
        self.source = source

    def __iter__(self):
        return itertranspose(self.source)


def itertranspose(source):
    hdr = header(source)
    its = [iter(source) for _ in hdr]
    for i in range(len(hdr)):
        yield tuple(row[i] for row in its[i])


def pivot(table, f1, f2, f3, aggfun, missing=None,
          presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Construct a pivot table. E.g.::

        >>> import petl as etl
        >>> table1 = [['region', 'gender', 'style', 'units'],
        ...           ['east', 'boy', 'tee', 12],
        ...           ['east', 'boy', 'golf', 14],
        ...           ['east', 'boy', 'fancy', 7],
        ...           ['east', 'girl', 'tee', 3],
        ...           ['east', 'girl', 'golf', 8],
        ...           ['east', 'girl', 'fancy', 18],
        ...           ['west', 'boy', 'tee', 12],
        ...           ['west', 'boy', 'golf', 15],
        ...           ['west', 'boy', 'fancy', 8],
        ...           ['west', 'girl', 'tee', 6],
        ...           ['west', 'girl', 'golf', 16],
        ...           ['west', 'girl', 'fancy', 1]]
        >>> table2 = etl.pivot(table1, 'region', 'gender', 'units', sum)
        >>> table2
        +--------+-----+------+
        | region | boy | girl |
        +========+=====+======+
        | 'east' |  33 |   29 |
        +--------+-----+------+
        | 'west' |  35 |   23 |
        +--------+-----+------+

        >>> table3 = etl.pivot(table1, 'region', 'style', 'units', sum)
        >>> table3
        +--------+-------+------+-----+
        | region | fancy | golf | tee |
        +========+=======+======+=====+
        | 'east' |    25 |   22 |  15 |
        +--------+-------+------+-----+
        | 'west' |     9 |   31 |  18 |
        +--------+-------+------+-----+

        >>> table4 = etl.pivot(table1, 'gender', 'style', 'units', sum)
        >>> table4
        +--------+-------+------+-----+
        | gender | fancy | golf | tee |
        +========+=======+======+=====+
        | 'boy'  |    15 |   29 |  24 |
        +--------+-------+------+-----+
        | 'girl' |    19 |   24 |   9 |
        +--------+-------+------+-----+

    See also :func:`petl.transform.reshape.recast`.

    """

    return PivotView(table, f1, f2, f3, aggfun, missing=missing,
                     presorted=presorted, buffersize=buffersize,
                     tempdir=tempdir, cache=cache)


Table.pivot = pivot


class PivotView(Table):

    def __init__(self, source, f1, f2, f3, aggfun, missing=None,
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key=(f1, f2), buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
        self.f1, self.f2, self.f3 = f1, f2, f3
        self.aggfun = aggfun
        self.missing = missing

    def __iter__(self):
        return iterpivot(self.source, self.f1, self.f2, self.f3, self.aggfun,
                         self.missing)


def iterpivot(source, f1, f2, f3, aggfun, missing):

    # first pass - collect fields
    f2vals = set(itervalues(source, f2))  # TODO only make one pass
    f2vals = list(f2vals)
    f2vals.sort()
    outhdr = [f1]
    outhdr.extend(f2vals)
    yield tuple(outhdr)

    # second pass - generate output
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        hdr = []
    flds = list(map(text_type, hdr))
    f1i = flds.index(f1)
    f2i = flds.index(f2)
    f3i = flds.index(f3)
    for v1, v1rows in itertools.groupby(it, key=operator.itemgetter(f1i)):
        outrow = [v1] + [missing] * len(f2vals)
        for v2, v12rows in itertools.groupby(v1rows,
                                             key=operator.itemgetter(f2i)):
            aggval = aggfun([row[f3i] for row in v12rows])
            outrow[1 + f2vals.index(v2)] = aggval
        yield tuple(outrow)


def flatten(table):
    """
    Convert a table to a sequence of values in row-major order. E.g.::

        >>> import petl as etl
        >>> table1 = [['foo', 'bar', 'baz'],
        ...           ['A', 1, True],
        ...           ['C', 7, False],
        ...           ['B', 2, False],
        ...           ['C', 9, True]]
        >>> list(etl.flatten(table1))
        ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9, True]

    See also :func:`petl.transform.reshape.unflatten`.

    """

    return FlattenView(table)


Table.flatten = flatten


class FlattenView(Table):

    def __init__(self, table):
        self.table = table

    def __iter__(self):
        for row in data(self.table):
            for value in row:
                yield value


def unflatten(*args, **kwargs):
    """
    Convert a sequence of values in row-major order into a table. E.g.::

        >>> import petl as etl
        >>> a = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
        >>> table1 = etl.unflatten(a, 3)
        >>> table1
        +-----+----+-------+
        | f0  | f1 | f2    |
        +=====+====+=======+
        | 'A' |  1 | True  |
        +-----+----+-------+
        | 'C' |  7 | False |
        +-----+----+-------+
        | 'B' |  2 | False |
        +-----+----+-------+
        | 'C' |  9 | None  |
        +-----+----+-------+

        >>> # a table and field name can also be provided as arguments
        ... table2 = [['lines'],
        ...           ['A'],
        ...           [1],
        ...           [True],
        ...           ['C'],
        ...           [7],
        ...           [False],
        ...           ['B'],
        ...           [2],
        ...           [False],
        ...           ['C'],
        ...           [9]]
        >>> table3 = etl.unflatten(table2, 'lines', 3)
        >>> table3
        +-----+----+-------+
        | f0  | f1 | f2    |
        +=====+====+=======+
        | 'A' |  1 | True  |
        +-----+----+-------+
        | 'C' |  7 | False |
        +-----+----+-------+
        | 'B' |  2 | False |
        +-----+----+-------+
        | 'C' |  9 | None  |
        +-----+----+-------+

    See also :func:`petl.transform.reshape.flatten`.

    """

    return UnflattenView(*args, **kwargs)


Table.unflatten = unflatten


class UnflattenView(Table):

    def __init__(self, *args, **kwargs):
        if len(args) == 2:
            self.input = args[0]
            self.period = args[1]
        elif len(args) == 3:
            self.input = values(args[0], args[1])
            self.period = args[2]
        else:
            assert False, 'invalid arguments'
        self.missing = kwargs.get('missing', None)

    def __iter__(self):
        inpt = self.input
        period = self.period
        missing = self.missing

        # generate header row
        outhdr = tuple('f%s' % i for i in range(period))
        yield outhdr

        # generate data rows
        row = list()
        for v in inpt:
            if len(row) < period:
                row.append(v)
            else:
                yield tuple(row)
                row = [v]

        # deal with last row
        if len(row) > 0:
            if len(row) < period:
                row.extend([missing] * (period - len(row)))
            yield tuple(row)

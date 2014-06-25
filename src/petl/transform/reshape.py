__author__ = 'aliman'


import itertools
import collections
import operator


from petl.util import RowContainer, rowgetter, sortable_itemgetter, values, \
    itervalues, header, data
from petl.transform.sorts import sort


def melt(table, key=None, variables=None, variablefield='variable', valuefield='value'):
    """
    Reshape a table, melting fields into data. E.g.::

        >>> from petl import melt, look
        >>> look(table1)
        +------+----------+-------+
        | 'id' | 'gender' | 'age' |
        +======+==========+=======+
        | 1    | 'F'      | 12    |
        +------+----------+-------+
        | 2    | 'M'      | 17    |
        +------+----------+-------+
        | 3    | 'M'      | 16    |
        +------+----------+-------+

        >>> table2 = melt(table1, 'id')
        >>> look(table2)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 2    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 3    | 'age'      | 16      |
        +------+------------+---------+

        >>> # compound keys are supported
        ... look(table3)
        +------+--------+----------+----------+
        | 'id' | 'time' | 'height' | 'weight' |
        +======+========+==========+==========+
        | 1    | 11     | 66.4     | 12.2     |
        +------+--------+----------+----------+
        | 2    | 16     | 53.2     | 17.3     |
        +------+--------+----------+----------+
        | 3    | 12     | 34.5     | 9.4      |
        +------+--------+----------+----------+

        >>> table4 = melt(table3, key=['id', 'time'])
        >>> look(table4)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'height'   | 66.4    |
        +------+--------+------------+---------+
        | 1    | 11     | 'weight'   | 12.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'height'   | 53.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'weight'   | 17.3    |
        +------+--------+------------+---------+
        | 3    | 12     | 'height'   | 34.5    |
        +------+--------+------------+---------+
        | 3    | 12     | 'weight'   | 9.4     |
        +------+--------+------------+---------+

        >>> # a subset of variable fields can be selected
        ... table5 = melt(table3, key=['id', 'time'], variables=['height'])
        >>> look(table5)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'height'   | 66.4    |
        +------+--------+------------+---------+
        | 2    | 16     | 'height'   | 53.2    |
        +------+--------+------------+---------+
        | 3    | 12     | 'height'   | 34.5    |
        +------+--------+------------+---------+

    See also :func:`recast`.

    """

    return MeltView(table, key=key, variables=variables,
                    variablefield=variablefield,
                    valuefield=valuefield)


class MeltView(RowContainer):

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
    it = iter(source)

    # normalise some stuff
    flds = it.next()
    if isinstance(key, basestring):
        key = (key,) # normalise to a tuple
    if isinstance(variables, basestring):
        # shouldn't expect this, but ... ?
        variables = (variables,) # normalise to a tuple
    if not key:
        # assume key is fields not in variables
        key = [f for f in flds if f not in variables]
    if not variables:
        # assume variables are fields not in key
        variables = [f for f in flds if f not in key]

    # determine the output fields
    out_flds = list(key)
    out_flds.append(variablefield)
    out_flds.append(valuefield)
    yield tuple(out_flds)

    key_indices = [flds.index(k) for k in key]
    getkey = rowgetter(*key_indices)
    variables_indices = [flds.index(v) for v in variables]

    # construct the output data
    for row in it:
        k = getkey(row)
        for v, i in zip(variables, variables_indices):
            o = list(k) # populate with key values initially
            o.append(v) # add variable
            o.append(row[i]) # add value
            yield tuple(o)


def recast(table, key=None, variablefield='variable', valuefield='value',
           samplesize=1000, reducers=None, missing=None):
    """
    Recast molten data. E.g.::

        >>> from petl import recast, look
        >>> look(table1)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 3    | 'age'      | 16      |
        +------+------------+---------+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 2    | 'gender'   | 'M'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+

        >>> table2 = recast(table1)
        >>> look(table2)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | 'M'      |
        +------+-------+----------+
        | 3    | 16    | 'M'      |
        +------+-------+----------+

        >>> # specifying variable and value fields
        ... look(table3)
        +------+----------+--------+
        | 'id' | 'vars'   | 'vals' |
        +======+==========+========+
        | 3    | 'age'    | 16     |
        +------+----------+--------+
        | 1    | 'gender' | 'F'    |
        +------+----------+--------+
        | 2    | 'gender' | 'M'    |
        +------+----------+--------+
        | 2    | 'age'    | 17     |
        +------+----------+--------+
        | 1    | 'age'    | 12     |
        +------+----------+--------+
        | 3    | 'gender' | 'M'    |
        +------+----------+--------+

        >>> table4 = recast(table3, variablefield='vars', valuefield='vals')
        >>> look(table4)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | 'M'      |
        +------+-------+----------+
        | 3    | 16    | 'M'      |
        +------+-------+----------+

        >>> # if there are multiple values for each key/variable pair, and no reducers
        ... # function is provided, then all values will be listed
        ... look(table6)
        +------+--------+------------+---------+
        | 'id' | 'time' | 'variable' | 'value' |
        +======+========+============+=========+
        | 1    | 11     | 'weight'   | 66.4    |
        +------+--------+------------+---------+
        | 1    | 14     | 'weight'   | 55.2    |
        +------+--------+------------+---------+
        | 2    | 12     | 'weight'   | 53.2    |
        +------+--------+------------+---------+
        | 2    | 16     | 'weight'   | 43.3    |
        +------+--------+------------+---------+
        | 3    | 12     | 'weight'   | 34.5    |
        +------+--------+------------+---------+
        | 3    | 17     | 'weight'   | 49.4    |
        +------+--------+------------+---------+

        >>> table7 = recast(table6, key='id')
        >>> look(table7)
        +------+--------------+
        | 'id' | 'weight'     |
        +======+==============+
        | 1    | [66.4, 55.2] |
        +------+--------------+
        | 2    | [53.2, 43.3] |
        +------+--------------+
        | 3    | [34.5, 49.4] |
        +------+--------------+

        >>> # multiple values can be reduced via an aggregation function
        ... def mean(values):
        ...     return float(sum(values)) / len(values)
        ...
        >>> table8 = recast(table6, key='id', reducers={'weight': mean})
        >>> look(table8)
        +------+--------------------+
        | 'id' | 'weight'           |
        +======+====================+
        | 1    | 60.800000000000004 |
        +------+--------------------+
        | 2    | 48.25              |
        +------+--------------------+
        | 3    | 41.95              |
        +------+--------------------+

        >>> # missing values are padded with whatever is provided via the missing
        ... # keyword argument (None by default)
        ... look(table9)
        +------+------------+---------+
        | 'id' | 'variable' | 'value' |
        +======+============+=========+
        | 1    | 'gender'   | 'F'     |
        +------+------------+---------+
        | 2    | 'age'      | 17      |
        +------+------------+---------+
        | 1    | 'age'      | 12      |
        +------+------------+---------+
        | 3    | 'gender'   | 'M'     |
        +------+------------+---------+

        >>> table10 = recast(table9, key='id')
        >>> look(table10)
        +------+-------+----------+
        | 'id' | 'age' | 'gender' |
        +======+=======+==========+
        | 1    | 12    | 'F'      |
        +------+-------+----------+
        | 2    | 17    | None     |
        +------+-------+----------+
        | 3    | None  | 'M'      |
        +------+-------+----------+

    Note that the table is scanned once to discover variables, then a second
    time to reshape the data and recast variables as fields. How many rows are
    scanned in the first pass is determined by the `samplesize` argument.

    See also :func:`melt`.

    """

    return RecastView(table, key=key, variablefield=variablefield,
                      valuefield=valuefield, samplesize=samplesize,
                      reducers=reducers, missing=missing)


class RecastView(RowContainer):

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
    #
    # TODO implementing this by making two passes through the data is a bit
    # ugly, and could be costly if there are several upstream transformations
    # that would need to be re-executed each pass - better to make one pass,
    # caching the rows sampled to discover variables to be recast as fields?
    #


    it = iter(source)
    fields = it.next()

    # normalise some stuff
    keyfields = key
    variablefields = variablefield # N.B., could be more than one
    if isinstance(keyfields, basestring):
        keyfields = (keyfields,)
    if isinstance(variablefields, basestring):
        variablefields = (variablefields,)
    if not keyfields:
        # assume keyfields is fields not in variables
        keyfields = [f for f in fields
                     if f not in variablefields and f != valuefield]
    if not variablefields:
        # assume variables are fields not in keyfields
        variablefields = [f for f in fields
                          if f not in keyfields and f != valuefield]

    # sanity checks
    assert valuefield in fields, 'invalid value field: %s' % valuefield
    assert valuefield not in keyfields, 'value field cannot be keyfields'
    assert valuefield not in variablefields, \
        'value field cannot be variable field'
    for f in keyfields:
        assert f in fields, 'invalid keyfields field: %s' % f
    for f in variablefields:
        assert f in fields, 'invalid variable field: %s' % f

    # we'll need these later
    valueindex = fields.index(valuefield)
    keyindices = [fields.index(f) for f in keyfields]
    variableindices = [fields.index(f) for f in variablefields]

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
            variables[f] = sorted(variables[f]) # turn from sets to sorted lists

    # finished the first pass

    # determine the output fields
    outfields = list(keyfields)
    for f in variablefields:
        outfields.extend(variables[f])
    yield tuple(outfields)

    # output data

    source = sort(source, key=keyfields)
    it = itertools.islice(source, 1, None) # skip header row
    getsortablekey = sortable_itemgetter(*keyindices)
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
                values = [r[valueindex] for r in group if r[i] == variable]
                if len(values) == 0:
                    value = missing
                elif len(values) == 1:
                    value = values[0]
                else:
                    if variable in reducers:
                        redu = reducers[variable]
                    else:
                        redu = list # list all values
                    value = redu(values)
                out_row.append(value)
        yield tuple(out_row)


def transpose(table):
    """
    Transpose rows into columns. E.g.::

        >>> from petl import transpose, look
        >>> look(table1)
        +------+----------+
        | 'id' | 'colour' |
        +======+==========+
        | 1    | 'blue'   |
        +------+----------+
        | 2    | 'red'    |
        +------+----------+
        | 3    | 'purple' |
        +------+----------+
        | 5    | 'yellow' |
        +------+----------+
        | 7    | 'orange' |
        +------+----------+

        >>> table2 = transpose(table1)
        >>> look(table2)
        +----------+--------+-------+----------+----------+----------+
        | 'id'     | 1      | 2     | 3        | 5        | 7        |
        +==========+========+=======+==========+==========+==========+
        | 'colour' | 'blue' | 'red' | 'purple' | 'yellow' | 'orange' |
        +----------+--------+-------+----------+----------+----------+

    See also :func:`recast`.

    """

    return TransposeView(table)


class TransposeView(RowContainer):

    def __init__(self, source):
        self.source = source

    def __iter__(self):
        return itertranspose(self.source)


def itertranspose(source):
    fields = header(source)
    its = [iter(source) for _ in fields]
    for i in range(len(fields)):
        yield tuple(row[i] for row in its[i])


def pivot(table, f1, f2, f3, aggfun, missing=None,
          presorted=False, buffersize=None, tempdir=None, cache=True):
    """
    Construct a pivot table. E.g.::

        >>> from petl import pivot, look
        >>> look(table1)
        +----------+----------+---------+---------+
        | 'region' | 'gender' | 'style' | 'units' |
        +==========+==========+=========+=========+
        | 'east'   | 'boy'    | 'tee'   | 12      |
        +----------+----------+---------+---------+
        | 'east'   | 'boy'    | 'golf'  | 14      |
        +----------+----------+---------+---------+
        | 'east'   | 'boy'    | 'fancy' | 7       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'tee'   | 3       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'golf'  | 8       |
        +----------+----------+---------+---------+
        | 'east'   | 'girl'   | 'fancy' | 18      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'tee'   | 12      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'golf'  | 15      |
        +----------+----------+---------+---------+
        | 'west'   | 'boy'    | 'fancy' | 8       |
        +----------+----------+---------+---------+
        | 'west'   | 'girl'   | 'tee'   | 6       |
        +----------+----------+---------+---------+

        >>> table2 = pivot(table1, 'region', 'gender', 'units', sum)
        >>> look(table2)
        +----------+-------+--------+
        | 'region' | 'boy' | 'girl' |
        +==========+=======+========+
        | 'east'   | 33    | 29     |
        +----------+-------+--------+
        | 'west'   | 35    | 23     |
        +----------+-------+--------+

        >>> table3 = pivot(table1, 'region', 'style', 'units', sum)
        >>> look(table3)
        +----------+---------+--------+-------+
        | 'region' | 'fancy' | 'golf' | 'tee' |
        +==========+=========+========+=======+
        | 'east'   | 25      | 22     | 15    |
        +----------+---------+--------+-------+
        | 'west'   | 9       | 31     | 18    |
        +----------+---------+--------+-------+

        >>> table4 = pivot(table1, 'gender', 'style', 'units', sum)
        >>> look(table4)
        +----------+---------+--------+-------+
        | 'gender' | 'fancy' | 'golf' | 'tee' |
        +==========+=========+========+=======+
        | 'boy'    | 15      | 29     | 24    |
        +----------+---------+--------+-------+
        | 'girl'   | 19      | 24     | 9     |
        +----------+---------+--------+-------+

    See also :func:`recast`.

    """

    return PivotView(table, f1, f2, f3, aggfun, missing=missing,
                     presorted=presorted, buffersize=buffersize, tempdir=tempdir,
                     cache=cache)


class PivotView(RowContainer):

    def __init__(self, source, f1, f2, f3, aggfun, missing=None,
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key=(f1, f2), buffersize=buffersize, tempdir=tempdir, cache=cache)
        self.f1, self.f2, self.f3 = f1, f2, f3
        self.aggfun = aggfun
        self.missing = missing

    def __iter__(self):
        return iterpivot(self.source, self.f1, self.f2, self.f3, self.aggfun, self.missing)


def iterpivot(source, f1, f2, f3, aggfun, missing):

    # first pass - collect fields
    f2vals = set(itervalues(source, f2)) # TODO sampling
    f2vals = list(f2vals)
    f2vals.sort()
    outflds = [f1]
    outflds.extend(f2vals)
    yield tuple(outflds)

    # second pass - generate output
    it = iter(source)
    srcflds = it.next()
    f1i = srcflds.index(f1)
    f2i = srcflds.index(f2)
    f3i = srcflds.index(f3)
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

        >>> from petl import flatten, look
        >>> look(table1)
        +-------+-------+-------+
        | 'foo' | 'bar' | 'baz' |
        +=======+=======+=======+
        | 'A'   | 1     | True  |
        +-------+-------+-------+
        | 'C'   | 7     | False |
        +-------+-------+-------+
        | 'B'   | 2     | False |
        +-------+-------+-------+
        | 'C'   | 9     | True  |
        +-------+-------+-------+

        >>> list(flatten(table1))
        ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9, True]

    See also :func:`unflatten`.

    .. versionadded:: 0.7

    """

    return FlattenView(table)


class FlattenView(RowContainer):

    def __init__(self, table):
        self.table = table

    def __iter__(self):
        for row in data(self.table):
            for value in row:
                yield value


def unflatten(*args, **kwargs):
    """
    Convert a sequence of values in row-major order into a table. E.g.::

        >>> from petl import unflatten, look
        >>> input = ['A', 1, True, 'C', 7, False, 'B', 2, False, 'C', 9]
        >>> table = unflatten(input, 3)
        >>> look(table)
        +------+------+-------+
        | 'f0' | 'f1' | 'f2'  |
        +======+======+=======+
        | 'A'  | 1    | True  |
        +------+------+-------+
        | 'C'  | 7    | False |
        +------+------+-------+
        | 'B'  | 2    | False |
        +------+------+-------+
        | 'C'  | 9    | None  |
        +------+------+-------+

        >>> # a table and field name can also be provided as arguments
        ... look(table1)
        +---------+
        | 'lines' |
        +=========+
        | 'A'     |
        +---------+
        | 1       |
        +---------+
        | True    |
        +---------+
        | 'C'     |
        +---------+
        | 7       |
        +---------+
        | False   |
        +---------+
        | 'B'     |
        +---------+
        | 2       |
        +---------+
        | False   |
        +---------+
        | 'C'     |
        +---------+

        >>> table2 = unflatten(table1, 'lines', 3)
        >>> look(table2)
        +------+------+-------+
        | 'f0' | 'f1' | 'f2'  |
        +======+======+=======+
        | 'A'  | 1    | True  |
        +------+------+-------+
        | 'C'  | 7    | False |
        +------+------+-------+
        | 'B'  | 2    | False |
        +------+------+-------+
        | 'C'  | 9    | None  |
        +------+------+-------+

    See also :func:`flatten`.

    .. versionadded:: 0.7

    """

    return UnflattenView(*args, **kwargs)


class UnflattenView(RowContainer):

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
        fields = tuple('f%s' % i for i in range(period))
        yield fields

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



from __future__ import absolute_import, print_function, division


import operator
from collections import OrderedDict
from petl.compat import next, string_types, text_type


import petl.config as config
from petl.errors import ArgumentError
from petl.util.base import Table, expr, rowgroupby, Record
from petl.transform.sorts import sort


def fieldmap(table, mappings=None, failonerror=None, errorvalue=None):
    """
    Transform a table, mapping fields arbitrarily between input and output.
    E.g.::

        >>> import petl as etl
        >>> from collections import OrderedDict
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, 'female', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33, 45.2],
        ...           [5, '-', 25, 1.65, 51.9]]
        >>> mappings = OrderedDict()
        >>> # rename a field
        ... mappings['subject_id'] = 'id'
        >>> # translate a field
        ... mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
        >>> # apply a calculation to a field
        ... mappings['age_months'] = 'age', lambda v: v * 12
        >>> # apply a calculation to a combination of fields
        ... mappings['bmi'] = lambda rec: rec['weight'] / rec['height']**2
        >>> # transform and inspect the output
        ... table2 = etl.fieldmap(table1, mappings)
        >>> table2
        +------------+--------+------------+--------------------+
        | subject_id | gender | age_months | bmi                |
        +============+========+============+====================+
        |          1 | 'M'    |        192 |  29.48870392390012 |
        +------------+--------+------------+--------------------+
        |          2 | 'F'    |        228 |   30.8531967030519 |
        +------------+--------+------------+--------------------+
        |          3 | 'F'    |        204 | 23.481883600555488 |
        +------------+--------+------------+--------------------+
        |          4 | 'M'    |        252 |  25.55260331279326 |
        +------------+--------+------------+--------------------+
        |          5 | '-'    |        300 |   19.0633608815427 |
        +------------+--------+------------+--------------------+

    Note also that the mapping value can be an expression string, which will be
    converted to a lambda function via :func:`petl.util.base.expr`.

    The `failonerror` and `errorvalue` keyword arguments are documented
    under :func:`petl.config.failonerror`
    """

    return FieldMapView(table, mappings=mappings, failonerror=failonerror,
                        errorvalue=errorvalue)


Table.fieldmap = fieldmap


class FieldMapView(Table):

    def __init__(self, source, mappings=None, failonerror=None,
                 errorvalue=None):
        self.source = source
        if mappings is None:
            self.mappings = OrderedDict()
        else:
            self.mappings = mappings
        self.failonerror = (config.failonerror if failonerror is None
                                else failonerror)
        self.errorvalue = errorvalue

    def __setitem__(self, key, value):
        self.mappings[key] = value

    def __iter__(self):
        return iterfieldmap(self.source, self.mappings, self.failonerror,
                            self.errorvalue)


def iterfieldmap(source, mappings, failonerror, errorvalue):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
    outhdr = mappings.keys()
    yield tuple(outhdr)

    mapfuns = dict()
    for outfld, m in mappings.items():
        if m in hdr:
            mapfuns[outfld] = operator.itemgetter(m)
        elif isinstance(m, int) and m < len(hdr):
            mapfuns[outfld] = operator.itemgetter(m)
        elif isinstance(m, string_types):
            mapfuns[outfld] = expr(m)
        elif callable(m):
            mapfuns[outfld] = m
        elif isinstance(m, (tuple, list)) and len(m) == 2:
            srcfld = m[0]
            fm = m[1]
            if callable(fm):
                mapfuns[outfld] = composefun(fm, srcfld)
            elif isinstance(fm, dict):
                mapfuns[outfld] = composedict(fm, srcfld)
            else:
                raise ArgumentError('expected callable or dict')
        else:
            raise ArgumentError('invalid mapping %r: %r' % (outfld, m))

    # wrap rows as records
    it = (Record(row, flds) for row in it)
    for row in it:
        outrow = list()
        for outfld in outhdr:
            try:
                val = mapfuns[outfld](row)
            except Exception as e:
                if failonerror == 'inline':
                    val = e
                elif failonerror:
                    raise e
                else:
                    val = errorvalue
            outrow.append(val)
        yield tuple(outrow)


def composefun(f, srcfld):
    def g(rec):
        return f(rec[srcfld])
    return g


def composedict(d, srcfld):
    def g(rec):
        k = rec[srcfld]
        if k in d:
            return d[k]
        else:
            return k
    return g


def rowmap(table, rowmapper, header, failonerror=None):
    """
    Transform rows via an arbitrary function. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, 'female', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33, 45.2],
        ...           [5, '-', 25, 1.65, 51.9]]
        >>> def rowmapper(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     return [row[0],
        ...             transmf[row['sex']] if row['sex'] in transmf else None,
        ...             row.age * 12,
        ...             row.height / row.weight ** 2]
        ...
        >>> table2 = etl.rowmap(table1, rowmapper,
        ...                     header=['subject_id', 'gender', 'age_months',
        ...                             'bmi'])
        >>> table2
        +------------+--------+------------+-----------------------+
        | subject_id | gender | age_months | bmi                   |
        +============+========+============+=======================+
        |          1 | 'M'    |        192 | 0.0003772112382934443 |
        +------------+--------+------------+-----------------------+
        |          2 | 'F'    |        228 | 0.0004366015456998006 |
        +------------+--------+------------+-----------------------+
        |          3 | 'F'    |        204 | 0.0003215689675106949 |
        +------------+--------+------------+-----------------------+
        |          4 | 'M'    |        252 | 0.0006509906805544679 |
        +------------+--------+------------+-----------------------+
        |          5 | None   |        300 | 0.0006125608384287258 |
        +------------+--------+------------+-----------------------+

    The `rowmapper` function should accept a single row and return a single
    row (list or tuple).

    The `failonerror` keyword argument is documented under
    :func:`petl.config.failonerror`
    """

    return RowMapView(table, rowmapper, header, failonerror=failonerror)


Table.rowmap = rowmap


class RowMapView(Table):

    def __init__(self, source, rowmapper, header, failonerror=None):
        self.source = source
        self.rowmapper = rowmapper
        self.header = header
        self.failonerror = (config.failonerror if failonerror is None
                                else failonerror)

    def __iter__(self):
        return iterrowmap(self.source, self.rowmapper, self.header,
                          self.failonerror)


def iterrowmap(source, rowmapper, header, failonerror):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
    yield tuple(header)
    it = (Record(row, flds) for row in it)
    for row in it:
        try:
            outrow = rowmapper(row)
            yield tuple(outrow)
        except Exception as e:
            if failonerror == 'inline':
                yield tuple([e])
            elif failonerror:
                raise e


def rowmapmany(table, rowgenerator, header, failonerror=None):
    """
    Map each input row to any number of output rows via an arbitrary
    function. E.g.::

        >>> import petl as etl
        >>> table1 = [['id', 'sex', 'age', 'height', 'weight'],
        ...           [1, 'male', 16, 1.45, 62.0],
        ...           [2, 'female', 19, 1.34, 55.4],
        ...           [3, '-', 17, 1.78, 74.4],
        ...           [4, 'male', 21, 1.33]]
        >>> def rowgenerator(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     yield [row[0], 'gender',
        ...            transmf[row['sex']] if row['sex'] in transmf else None]
        ...     yield [row[0], 'age_months', row.age * 12]
        ...     yield [row[0], 'bmi', row.height / row.weight ** 2]
        ...
        >>> table2 = etl.rowmapmany(table1, rowgenerator,
        ...                         header=['subject_id', 'variable', 'value'])
        >>> table2.lookall()
        +------------+--------------+-----------------------+
        | subject_id | variable     | value                 |
        +============+==============+=======================+
        |          1 | 'gender'     | 'M'                   |
        +------------+--------------+-----------------------+
        |          1 | 'age_months' |                   192 |
        +------------+--------------+-----------------------+
        |          1 | 'bmi'        | 0.0003772112382934443 |
        +------------+--------------+-----------------------+
        |          2 | 'gender'     | 'F'                   |
        +------------+--------------+-----------------------+
        |          2 | 'age_months' |                   228 |
        +------------+--------------+-----------------------+
        |          2 | 'bmi'        | 0.0004366015456998006 |
        +------------+--------------+-----------------------+
        |          3 | 'gender'     | None                  |
        +------------+--------------+-----------------------+
        |          3 | 'age_months' |                   204 |
        +------------+--------------+-----------------------+
        |          3 | 'bmi'        | 0.0003215689675106949 |
        +------------+--------------+-----------------------+
        |          4 | 'gender'     | 'M'                   |
        +------------+--------------+-----------------------+
        |          4 | 'age_months' |                   252 |
        +------------+--------------+-----------------------+

    The `rowgenerator` function should accept a single row and yield zero or
    more rows (lists or tuples).

    The `failonerror` keyword argument is documented under
    :func:`petl.config.failonerror`

    See also the :func:`petl.transform.reshape.melt` function.

    """

    return RowMapManyView(table, rowgenerator, header, failonerror=failonerror)


Table.rowmapmany = rowmapmany


class RowMapManyView(Table):

    def __init__(self, source, rowgenerator, header, failonerror=None):
        self.source = source
        self.rowgenerator = rowgenerator
        self.header = header
        self.failonerror = (config.failonerror if failonerror is None
                                else failonerror)

    def __iter__(self):
        return iterrowmapmany(self.source, self.rowgenerator, self.header,
                              self.failonerror)


def iterrowmapmany(source, rowgenerator, header, failonerror):
    it = iter(source)
    try:
        hdr = next(it)
    except StopIteration:
        return
    flds = list(map(text_type, hdr))
    yield tuple(header)
    it = (Record(row, flds) for row in it)
    for row in it:
        try:
            for outrow in rowgenerator(row):
                yield tuple(outrow)
        except Exception as e:
            if failonerror == 'inline':
                yield tuple([e])
            elif failonerror:
                raise e
            else:
                pass


def rowgroupmap(table, key, mapper, header=None, presorted=False,
                buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply `mapper` to yield zero or more
    output rows for each input group of rows.

    """

    return RowGroupMapView(table, key, mapper, header=header,
                           presorted=presorted,
                           buffersize=buffersize, tempdir=tempdir, cache=cache)


Table.rowgroupmap = rowgroupmap


class RowGroupMapView(Table):

    def __init__(self, source, key, mapper, header=None,
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
        self.key = key
        self.header = header
        self.mapper = mapper

    def __iter__(self):
        return iterrowgroupmap(self.source, self.key, self.mapper, self.header)


def iterrowgroupmap(source, key, mapper, header):
    yield tuple(header)
    for key, rows in rowgroupby(source, key):
        for row in mapper(key, rows):
            yield row

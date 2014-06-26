__author__ = 'aliman'


import operator


from petl.compat import OrderedDict
from petl.util import RowContainer, hybridrows, expr, rowgroupby
from petl.transform.sorts import sort


def fieldmap(table, mappings=None, failonerror=False, errorvalue=None):
    """
    Transform a table, mapping fields arbitrarily between input and output. E.g.::

        >>> from petl import fieldmap, look
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | 'female' | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     | 45.2     |
        +------+----------+-------+----------+----------+
        | 5    | '-'      | 25    | 1.65     | 51.9     |
        +------+----------+-------+----------+----------+

        >>> from collections import OrderedDict
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
        ... table2 = fieldmap(table1, mappings)
        >>> look(table2)
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+

        >>> # field mappings can also be added and/or updated after the table is created
        ... # via the suffix notation
        ... table3 = fieldmap(table1)
        >>> table3['subject_id'] = 'id'
        >>> table3['gender'] = 'sex', {'male': 'M', 'female': 'F'}
        >>> table3['age_months'] = 'age', lambda v: v * 12
        >>> # use an expression string this time
        ... table3['bmi'] = '{weight} / {height}**2'
        >>> look(table3)
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+

    Note also that the mapping value can be an expression string, which will be
    converted to a lambda function via :func:`expr`.

    """

    return FieldMapView(table, mappings=mappings, failonerror=failonerror,
                        errorvalue=errorvalue)


class FieldMapView(RowContainer):

    def __init__(self, source, mappings=None, failonerror=False, errorvalue=None):
        self.source = source
        if mappings is None:
            self.mappings = OrderedDict()
        else:
            self.mappings = mappings
        self.failonerror = failonerror
        self.errorvalue = errorvalue

    def __setitem__(self, key, value):
        self.mappings[key] = value

    def __iter__(self):
        return iterfieldmap(self.source, self.mappings, self.failonerror, self.errorvalue)


def iterfieldmap(source, mappings, failonerror, errorvalue):
    it = iter(source)
    flds = it.next()
    outflds = mappings.keys()
    yield tuple(outflds)

    mapfuns = dict()
    for outfld, m in mappings.items():
        if m in flds:
            mapfuns[outfld] = operator.itemgetter(m)
        elif isinstance(m, int) and m < len(flds):
            mapfuns[outfld] = operator.itemgetter(m)
        elif isinstance(m, basestring):
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
                raise Exception('expected callable or dict') # TODO better error
        else:
            raise Exception('invalid mapping', outfld, m) # TODO better error

    for row in hybridrows(flds, it):
        try:
            # use list comprehension if possible
            outrow = [mapfuns[outfld](row) for outfld in outflds]
        except:
            # fall back to doing it one field at a time
            outrow = list()
            for outfld in outflds:
                try:
                    val = mapfuns[outfld](row)
                except:
                    if failonerror:
                        raise
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


def rowmap(table, rowmapper, fields, failonerror=False, missing=None):
    """
    Transform rows via an arbitrary function. E.g.::

        >>> from petl import rowmap, look
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | 'female' | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     | 45.2     |
        +------+----------+-------+----------+----------+
        | 5    | '-'      | 25    | 1.65     | 51.9     |
        +------+----------+-------+----------+----------+

        >>> def rowmapper(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     return [row[0],
        ...             transmf[row[1]] if row[1] in transmf else row[1],
        ...             row[2] * 12,
        ...             row[4] / row[3] ** 2]
        ...
        >>> table2 = rowmap(table1, rowmapper, fields=['subject_id', 'gender', 'age_months', 'bmi'])
        >>> look(table2)
        +--------------+----------+--------------+--------------------+
        | 'subject_id' | 'gender' | 'age_months' | 'bmi'              |
        +==============+==========+==============+====================+
        | 1            | 'M'      | 192          | 29.48870392390012  |
        +--------------+----------+--------------+--------------------+
        | 2            | 'F'      | 228          | 30.8531967030519   |
        +--------------+----------+--------------+--------------------+
        | 3            | 'F'      | 204          | 23.481883600555488 |
        +--------------+----------+--------------+--------------------+
        | 4            | 'M'      | 252          | 25.55260331279326  |
        +--------------+----------+--------------+--------------------+
        | 5            | '-'      | 300          | 19.0633608815427   |
        +--------------+----------+--------------+--------------------+

    The `rowmapper` function should return a single row (list or tuple).

    .. versionchanged:: 0.9

    Hybrid row objects supporting data value access by either position or by
    field name are now passed to the `rowmapper` function.

    """

    return RowMapView(table, rowmapper, fields, failonerror=failonerror,
                      missing=missing)


class RowMapView(RowContainer):

    def __init__(self, source, rowmapper, fields, failonerror=False, missing=None):
        self.source = source
        self.rowmapper = rowmapper
        self.fields = fields
        self.failonerror = failonerror
        self.missing = missing

    def __iter__(self):
        return iterrowmap(self.source, self.rowmapper, self.fields, self.failonerror,
                          self.missing)


def iterrowmap(source, rowmapper, fields, failonerror, missing):
    it = iter(source)
    srcflds = it.next()
    yield tuple(fields)
    for row in hybridrows(srcflds, it, missing):
        try:
            outrow = rowmapper(row)
            yield tuple(outrow)
        except:
            if failonerror:
                raise


def recordmap(table, recmapper, fields, failonerror=False):
    """
    Transform records via an arbitrary function.

    .. deprecated:: 0.9

    Use :func:`rowmap` insteand.

    """

    return rowmap(table, recmapper, fields, failonerror=failonerror)


def rowmapmany(table, rowgenerator, fields, failonerror=False, missing=None):
    """
    Map each input row to any number of output rows via an arbitrary function.
    E.g.::

        >>> from petl import rowmapmany, look
        >>> look(table1)
        +------+----------+-------+----------+----------+
        | 'id' | 'sex'    | 'age' | 'height' | 'weight' |
        +======+==========+=======+==========+==========+
        | 1    | 'male'   | 16    | 1.45     | 62.0     |
        +------+----------+-------+----------+----------+
        | 2    | 'female' | 19    | 1.34     | 55.4     |
        +------+----------+-------+----------+----------+
        | 3    | '-'      | 17    | 1.78     | 74.4     |
        +------+----------+-------+----------+----------+
        | 4    | 'male'   | 21    | 1.33     |          |
        +------+----------+-------+----------+----------+

        >>> def rowgenerator(row):
        ...     transmf = {'male': 'M', 'female': 'F'}
        ...     yield [row[0], 'gender', transmf[row[1]] if row[1] in transmf else row[1]]
        ...     yield [row[0], 'age_months', row[2] * 12]
        ...     yield [row[0], 'bmi', row[4] / row[3] ** 2]
        ...
        >>> table2 = rowmapmany(table1, rowgenerator, fields=['subject_id', 'variable', 'value'])
        >>> look(table2)
        +--------------+--------------+--------------------+
        | 'subject_id' | 'variable'   | 'value'            |
        +==============+==============+====================+
        | 1            | 'gender'     | 'M'                |
        +--------------+--------------+--------------------+
        | 1            | 'age_months' | 192                |
        +--------------+--------------+--------------------+
        | 1            | 'bmi'        | 29.48870392390012  |
        +--------------+--------------+--------------------+
        | 2            | 'gender'     | 'F'                |
        +--------------+--------------+--------------------+
        | 2            | 'age_months' | 228                |
        +--------------+--------------+--------------------+
        | 2            | 'bmi'        | 30.8531967030519   |
        +--------------+--------------+--------------------+
        | 3            | 'gender'     | '-'                |
        +--------------+--------------+--------------------+
        | 3            | 'age_months' | 204                |
        +--------------+--------------+--------------------+
        | 3            | 'bmi'        | 23.481883600555488 |
        +--------------+--------------+--------------------+
        | 4            | 'gender'     | 'M'                |
        +--------------+--------------+--------------------+

    The `rowgenerator` function should yield zero or more rows (lists or tuples).

    See also the :func:`melt` function.

    .. versionchanged:: 0.9

    Hybrid row objects supporting data value access by either position or by
    field name are now passed to the `rowgenerator` function.

    """

    return RowMapManyView(table, rowgenerator, fields, failonerror=failonerror,
                          missing=missing)


class RowMapManyView(RowContainer):

    def __init__(self, source, rowgenerator, fields, failonerror=False, missing=None):
        self.source = source
        self.rowgenerator = rowgenerator
        self.fields = fields
        self.failonerror = failonerror
        self.missing = missing

    def __iter__(self):
        return iterrowmapmany(self.source, self.rowgenerator, self.fields,
                              self.failonerror, self.missing)


def iterrowmapmany(source, rowgenerator, fields, failonerror, missing):
    it = iter(source)
    srcflds = it.next()
    yield tuple(fields)
    for row in hybridrows(srcflds, it, missing):
        try:
            for outrow in rowgenerator(row):
                yield tuple(outrow)
        except:
            if failonerror:
                raise


def recordmapmany(table, rowgenerator, fields, failonerror=False):
    """
    Map each input row (as a record) to any number of output rows via an
    arbitrary function.

    .. deprecated:: 0.9

    Use :func:`rowmapmany` instead.

    """

    return rowmapmany(table, rowgenerator, fields, failonerror=failonerror)


def rowgroupmap(table, key, mapper, fields=None, missing=None, presorted=False,
                buffersize=None, tempdir=None, cache=True):
    """
    Group rows under the given key then apply `mapper` to yield zero or more
    output rows for each input group of rows.

    .. versionadded:: 0.12

    """

    return RowGroupMapView(table, key, mapper, fields=fields,
                           presorted=presorted,
                           buffersize=buffersize, tempdir=tempdir, cache=cache)


class RowGroupMapView(RowContainer):

    def __init__(self, source, key, mapper, fields=None,
                 presorted=False, buffersize=None, tempdir=None, cache=True):
        if presorted:
            self.source = source
        else:
            self.source = sort(source, key, buffersize=buffersize,
                               tempdir=tempdir, cache=cache)
        self.key = key
        self.fields = fields
        self.mapper = mapper

    def __iter__(self):
        return iterrowgroupmap(self.source, self.key, self.mapper, self.fields)


def iterrowgroupmap(source, key, mapper, fields):
    yield tuple(fields)
    for key, rows in rowgroupby(source, key):
        for row in mapper(key, rows):
            yield row



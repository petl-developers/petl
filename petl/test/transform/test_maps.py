from __future__ import absolute_import, print_function, division


from petl.compat import OrderedDict
from petl.test.helpers import ieq, eq_
from petl.transform.maps import fieldmap, rowmap, rowmapmany


def test_fieldmap():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))

    mappings = OrderedDict()
    mappings['subject_id'] = 'id'
    mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    mappings['age_months'] = 'age', lambda v: v * 12
    mappings['bmi'] = lambda rec: rec['weight'] / rec['height'] ** 2
    actual = fieldmap(table, mappings)
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2),
              (5, '-', 25 * 12, 51.9 / 1.65 ** 2))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?

    # do it with suffix
    actual = fieldmap(table)
    actual['subject_id'] = 'id'
    actual['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    actual['age_months'] = 'age', lambda v: v * 12
    actual['bmi'] = '{weight} / {height}**2'
    ieq(expect, actual)

    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2),
              (5, '-', 25 * 12, None))
    actual = fieldmap(table2, mappings)
    ieq(expect, actual)


def test_fieldmap_record_access():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))

    mappings = OrderedDict()
    mappings['subject_id'] = 'id'
    mappings['gender'] = 'sex', {'male': 'M', 'female': 'F'}
    mappings['age_months'] = 'age', lambda v: v * 12
    mappings['bmi'] = lambda rec: rec.weight / rec.height ** 2
    actual = fieldmap(table, mappings)
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2),
              (5, '-', 25 * 12, 51.9 / 1.65 ** 2))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?


def test_fieldmap_empty():
    table = (('foo', 'bar'),)
    expect = (('foo', 'baz'),)
    mappings = OrderedDict()
    mappings['foo'] = 'foo'
    mappings['baz'] = 'bar', lambda v: v * 2
    actual = fieldmap(table, mappings)
    ieq(expect, actual)


def test_rowmap():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))

    def rowmapper(row):
        transmf = {'male': 'M', 'female': 'F'}
        return [row[0],
                transmf[row[1]] if row[1] in transmf else row[1],
                row[2] * 12,
                row[4] / row[3] ** 2]

    actual = rowmap(table, rowmapper, header=['subject_id', 'gender',
                                              'age_months', 'bmi'])
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2),
              (5, '-', 25 * 12, 51.9 / 1.65 ** 2))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?

    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2))
    actual = rowmap(table2, rowmapper, header=['subject_id', 'gender',
                                               'age_months', 'bmi'])
    ieq(expect, actual)


def test_rowmap_empty():
    table = (('id', 'sex', 'age', 'height', 'weight'),)

    def rowmapper(row):
        transmf = {'male': 'M', 'female': 'F'}
        return [row[0],
                transmf[row[1]] if row[1] in transmf else row[1],
                row[2] * 12,
                row[4] / row[3] ** 2]

    actual = rowmap(table, rowmapper, header=['subject_id', 'gender',
                                              'age_months', 'bmi'])
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),)
    ieq(expect, actual)


def test_recordmap():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, 'female', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33, 45.2),
             (5, '-', 25, 1.65, 51.9))

    def recmapper(rec):
        transmf = {'male': 'M', 'female': 'F'}
        return [rec['id'],
                transmf[rec['sex']] if rec['sex'] in transmf else rec['sex'],
                rec['age'] * 12,
                rec['weight'] / rec['height'] ** 2]

    actual = rowmap(table, recmapper, header=['subject_id', 'gender',
                                              'age_months', 'bmi'])
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2),
              (5, '-', 25 * 12, 51.9 / 1.65 ** 2))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?

    # test short rows
    table2 = (('id', 'sex', 'age', 'height', 'weight'),
              (1, 'male', 16, 1.45, 62.0),
              (2, 'female', 19, 1.34, 55.4),
              (3, 'female', 17, 1.78, 74.4),
              (4, 'male', 21, 1.33, 45.2),
              (5, '-', 25, 1.65))
    expect = (('subject_id', 'gender', 'age_months', 'bmi'),
              (1, 'M', 16 * 12, 62.0 / 1.45 ** 2),
              (2, 'F', 19 * 12, 55.4 / 1.34 ** 2),
              (3, 'F', 17 * 12, 74.4 / 1.78 ** 2),
              (4, 'M', 21 * 12, 45.2 / 1.33 ** 2))
    actual = rowmap(table2, recmapper, header=['subject_id', 'gender',
                                               'age_months', 'bmi'])
    ieq(expect, actual)


def test_rowmapmany():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, '-', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33))

    def rowgenerator(row):
        transmf = {'male': 'M', 'female': 'F'}
        yield [row[0], 'gender',
               transmf[row[1]] if row[1] in transmf else row[1]]
        yield [row[0], 'age_months', row[2] * 12]
        yield [row[0], 'bmi', row[4] / row[3] ** 2]

    actual = rowmapmany(table, rowgenerator, header=['subject_id', 'variable',
                                                     'value'])
    expect = (('subject_id', 'variable', 'value'),
              (1, 'gender', 'M'),
              (1, 'age_months', 16 * 12),
              (1, 'bmi', 62.0 / 1.45 ** 2),
              (2, 'gender', 'F'),
              (2, 'age_months', 19 * 12),
              (2, 'bmi', 55.4 / 1.34 ** 2),
              (3, 'gender', '-'),
              (3, 'age_months', 17 * 12),
              (3, 'bmi', 74.4 / 1.78 ** 2),
              (4, 'gender', 'M'),
              (4, 'age_months', 21 * 12))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?


def test_recordmapmany():
    table = (('id', 'sex', 'age', 'height', 'weight'),
             (1, 'male', 16, 1.45, 62.0),
             (2, 'female', 19, 1.34, 55.4),
             (3, '-', 17, 1.78, 74.4),
             (4, 'male', 21, 1.33))

    def rowgenerator(rec):
        transmf = {'male': 'M', 'female': 'F'}
        yield [rec['id'], 'gender',
               transmf[rec['sex']] if rec['sex'] in transmf else rec['sex']]
        yield [rec['id'], 'age_months', rec['age'] * 12]
        yield [rec['id'], 'bmi', rec['weight'] / rec['height'] ** 2]

    actual = rowmapmany(table, rowgenerator, header=['subject_id', 'variable',
                                                     'value'])
    expect = (('subject_id', 'variable', 'value'),
              (1, 'gender', 'M'),
              (1, 'age_months', 16 * 12),
              (1, 'bmi', 62.0 / 1.45 ** 2),
              (2, 'gender', 'F'),
              (2, 'age_months', 19 * 12),
              (2, 'bmi', 55.4 / 1.34 ** 2),
              (3, 'gender', '-'),
              (3, 'age_months', 17 * 12),
              (3, 'bmi', 74.4 / 1.78 ** 2),
              (4, 'gender', 'M'),
              (4, 'age_months', 21 * 12))
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?


'''
def test_fieldmap_errors_default_config():
    table1 = (('foo',), ('A',), (1,))
    mappings = {}
    mappings['bar'] = 'foo', lambda v: v.lower()

    # test the default config setting: failonerror == False
    eq_(config.failonerror, False)

    # test that by default, a bad fieldmap does not raise an exception,
    # and that values for the failed fieldmap are returned as None
    table2 = fieldmap(table1, mappings)
    expect2 = (('bar',), ('a',), (None,))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # test that when called with failonerror=False, a bad fieldmap does
    # not raise an exception, and that values for the failed fieldmap
    # are returned as None
    table3 = fieldmap(table1, mappings, failonerror=False)
    ieq(expect2, table3)
    ieq(expect2, table3)

    # test that when called with failonerror=True, a bad fieldmap
    # raises an exception
    try:
        table4 = fieldmap(table1, mappings, failonerror=True)
        table4.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # test that when called with failonerror='yield_exceptions', a bad
    # fieldmap does not raise an exception, and an Exception for the
    # failed fieldmap is returned in the result.
    table5 = fieldmap(table1, mappings, failonerror='yield_exceptions')
    expect5 = (('bar',), ('a',))
    ieq(expect5, table5.head(1))
    ieq(expect5, table5.head(1))
    d = table5.dicts()
    assert isinstance(list(d)[1]['bar'], AttributeError)


def test_fieldmap_errors_config():
    # save config setting
    saved_config_failonerror = config.failonerror

    table1 = (('foo',), ('A',), (1,))
    mappings = {}
    mappings['bar'] = 'foo', lambda v: v.lower()

    # when config failonerror == True, and neither failonerror nor
    # errorvalue are provided, a bad conversion raises an exception
    config.failonerror = True
    try:
        table2 = fieldmap(table1, mappings)
        table2.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # when config failonerror == 'yield_exceptions', and neither
    # failonerror nor errorvalue are provided, a bad conversion does not
    # raise an exception, and an Exception for the failed conversion is
    # returned in the result.
    config.failonerror = 'yield_exceptions'
    table3 = fieldmap(table1, mappings)
    expect3 = (('bar',), ('a',))
    ieq(expect3, table3.head(1))
    ieq(expect3, table3.head(1))
    d = table3.dicts()
    assert isinstance(list(d)[1]['bar'], AttributeError)

    # when config failonerror is an invalid value, but still truthy, it
    # behaves the same as if == True
    config.failonerror = 'bogus'
    try:
        table4 = fieldmap(table1, mappings)
        table4.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # when config failonerror is None, it behaves the same as if ==
    # False
    config.failonerror = None
    table5 = fieldmap(table1, mappings)
    expect5 = (('bar',),
              ('a',),
              (None,))
    ieq(expect5, table5)
    ieq(expect5, table5)

    # restore config setting
    config.failonerror = saved_config_failonerror

'''
'''

def test_rowmap_errors_default_config():
    table1 = (('foo',), ('A',), (1,))
    rowmapper = lambda r: [[r[0].lower()]]
    # test the default config setting: failonerror == False
    eq_(config.failonerror, False)

    # test that by default, a bad rowmap does not raise an exception,
    # and that values for the failed rowmap are returned as None
    table2 = rowmap(table1, rowmapper, header=['bar'])
    expect2 = (('bar',), ('a',), (None,))
    ieq(expect2, table2)
    ieq(expect2, table2)

    # test that when called with failonerror=False, a bad fieldmap does
    # not raise an exception, and that values for the failed fieldmap
    # are returned as None
    table3 = fieldmap(table1, mappings, failonerror=False)
    ieq(expect2, table3)
    ieq(expect2, table3)

    # test that when called with failonerror=True, a bad fieldmap
    # raises an exception
    try:
        table4 = fieldmap(table1, mappings, failonerror=True)
        table4.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # test that when called with failonerror='yield_exceptions', a bad
    # fieldmap does not raise an exception, and an Exception for the
    # failed fieldmap is returned in the result.
    table5 = fieldmap(table1, mappings, failonerror='yield_exceptions')
    expect5 = (('bar',), ('a',))
    ieq(expect5, table5.head(1))
    ieq(expect5, table5.head(1))
    d = table5.dicts()
    assert isinstance(list(d)[1]['bar'], AttributeError)


def test_fieldmap_errors_config():
    # save config setting
    saved_config_failonerror = config.failonerror

    table1 = (('foo',), ('A',), (1,))
    mappings = {}
    mappings['bar'] = 'foo', lambda v: v.lower()

    # when config failonerror == True, and neither failonerror nor
    # errorvalue are provided, a bad conversion raises an exception
    config.failonerror = True
    try:
        table2 = fieldmap(table1, mappings)
        table2.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # when config failonerror == 'yield_exceptions', and neither
    # failonerror nor errorvalue are provided, a bad conversion does not
    # raise an exception, and an Exception for the failed conversion is
    # returned in the result.
    config.failonerror = 'yield_exceptions'
    table3 = fieldmap(table1, mappings)
    expect3 = (('bar',), ('a',))
    ieq(expect3, table3.head(1))
    ieq(expect3, table3.head(1))
    d = table3.dicts()
    assert isinstance(list(d)[1]['bar'], AttributeError)

    # when config failonerror is an invalid value, but still truthy, it
    # behaves the same as if == True
    config.failonerror = 'bogus'
    try:
        table4 = fieldmap(table1, mappings)
        table4.nrows()
    except AttributeError:
        pass
    else:
        assert False, 'exception expected'

    # when config failonerror is None, it behaves the same as if ==
    # False
    config.failonerror = None
    table5 = fieldmap(table1, mappings)
    expect5 = (('bar',),
              ('a',),
              (None,))
    ieq(expect5, table5)
    ieq(expect5, table5)

    # restore config setting
    config.failonerror = saved_config_failonerror
'''
'''

def rowmap(table, rowmapper, header, failonerror=False):
    return RowMapView(table, rowmapper, header, failonerror=failonerror)

def rowmapmany(table, rowgenerator, header, failonerror=False):
    return RowMapManyView(table, rowgenerator, header, failonerror=failonerror)


table2 = fieldmap(table1, mappings)
def test_fieldmap_errors_default_config():
    table1 = (('foo',), ('A',), (1,))
    mappings = {}
    mappings['bar'] = 'foo', lambda v: v.lower()

table2 = rowmap(table1, rowmapper, header=['bar'])
    table1 = (('foo',), ('A',), (1,))
    rowmapper = lambda r: [[r[0].lower()]]

table2 = rowmapmany(table1, rowgenerator, header=['bar'])
'''

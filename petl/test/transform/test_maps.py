from __future__ import absolute_import, print_function, division

from collections import OrderedDict

from petl.test.failonerror import assert_failonerror
from petl.test.helpers import ieq
from petl.transform.maps import fieldmap, rowmap, rowmapmany

from functools import partial


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


def test_fieldmap_headerless():
    table = []
    expect = []
    mappings = OrderedDict()
    mappings['foo'] = 'foo'
    mappings['baz'] = 'bar', lambda v: v * 2
    actual = fieldmap(table, mappings)
    ieq(expect, actual)


def test_fieldmap_failonerror():
    input_  = (('foo',), ('A',), (1,))
    mapper_ = {'bar': ('foo', lambda v: v.lower())}
    expect_ = (('bar',), ('a',), (None,))

    assert_failonerror(
            input_fn=partial(fieldmap, input_, mapper_),
            expected_output=expect_)


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


def test_rowmap_headerless():
    table = []

    def rowmapper(row):
        return row

    actual = rowmap(table, rowmapper, header=['subject_id', 'gender'])
    expect = []
    ieq(expect, actual)


def test_rowmap_failonerror():
    input_  = (('foo',), ('A',), (1,), ('B',))
    mapper  = lambda r: [r[0].lower()]
    # exceptions in rowmappers do not generate an output row
    expect_ = (('foo',), ('a',), ('b',))

    assert_failonerror(
            input_fn=partial(rowmap, input_, mapper, header=('foo',)),
            expected_output=expect_)


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


def test_rowmapmany_failonerror():
    input_  = (('foo',), ('A',), (1,), ('B',))
    mapper  = lambda r: [r[0].lower()]
    expect_ = (('foo',), ('a',), ('b',),)

    assert_failonerror(
            input_fn=partial(rowmapmany, input_, mapper, header=('foo',)),
            expected_output=expect_)


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


def test_recordmapmany_headerless():
    table = []

    def duplicate(rec):
        yield rec
        yield rec

    actual = rowmapmany(table, duplicate, header=['subject_id', 'variable'])
    expect = []
    ieq(expect, actual)
    ieq(expect, actual)  # can iteratate twice?

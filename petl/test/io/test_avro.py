# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import math

from datetime import datetime, date
from decimal import Decimal
from tempfile import NamedTemporaryFile

import pytest

from petl.compat import PY3
from petl.transform.basics import cat
from petl.util.base import dicts
from petl.util.vis import look

from petl.test.helpers import ieq

from petl.io.avro import fromavro, toavro, appendavro

from petl.test.io.test_avro_schemas import schema0, schema1, schema2, \
    schema3, schema4, schema5, schema6

if PY3:
    from datetime import timezone

try:
    import fastavro
    # import fastavro dependencies
    import pytz
except ImportError as e:
    pytest.skip('SKIP avro tests: %s' % e, allow_module_level=True)
else:
    # region Test Cases

    def test_fromavro11():
        _read_from_mavro_file(table1, schema1)

    def test_fromavro22():
        _read_from_mavro_file(table2, schema2)

    def test_fromavro33():
        _read_from_mavro_file(table3, schema3)

    def test_toavro11():
        _write_to_avro_file(table1, schema1)

    def test_toavro22():
        _write_to_avro_file(table2, schema2)

    def test_toavro33():
        _write_to_avro_file(table3, schema3)

    def test_toavro10():
        _write_to_avro_file(table1, None)

    def test_toavro13():
        _write_to_avro_file(table01, schema0, table10)

    def test_toavro20():
        _write_to_avro_file(table2, None)

    def test_toavro30():
        _write_to_avro_file(table3, None)

    def test_toavro44():
        _write_to_avro_file(table4, schema4)

    def test_toavro55():
        _write_to_avro_file(table5, schema5)

    def test_toavro50():
        _write_to_avro_file(table5, None)

    def test_toavro70():
        _write_to_avro_file(table71, None)

    def test_toavro80():
        _write_to_avro_file(table8, None)

    def test_toavro90():
        _write_to_avro_file(table9, None)

    def test_toavro61():
        _write_to_avro_file(table61, schema6, print_tables=False)

    def test_toavro62():
        _write_to_avro_file(table62, schema6, print_tables=False)

    def test_toavro63():
        _write_to_avro_file(table63, schema6, print_tables=False)

    def test_toavro60():
        _write_to_avro_file(table60, schema6, print_tables=False)

    def test_appendavro11():
        _append_to_avro_file(table11, table12, schema1, table1)

    def test_appendavro22():
        _append_to_avro_file(table21, table22, schema2, table2)

    def test_appendavro10():
        _append_to_avro_file(table11, table12, schema1)

    def test_toavro_troubleshooting10():
        nullable_schema = dict(schema0)
        schema_fields = nullable_schema['fields']
        for field in schema_fields:
            field['type'] = ['null', 'string']
        try:
            _write_temp_avro_file(table1, nullable_schema)
        except ValueError as vex:
            bob = "%s" % vex
            assert 'Bob' in bob
            return
        assert False, 'Failed schema conversion'

    def test_toavro_troubleshooting11():
        table0 = list(table1)
        table0[3][1] = None
        try:
            _write_temp_avro_file(table0, schema1)
        except TypeError as tex:
            joe = "%s" % tex
            assert 'Joe' in joe
            return
        assert False, 'Failed schema conversion'

    # endregion

    # region Execution

    def _read_from_mavro_file(test_rows, test_schema, test_expect=None, print_tables=True):
        _show__expect_rows(test_rows, print_tables)
        test_filename = _create_avro_example(test_schema, test_rows)
        test_actual = fromavro(test_filename)
        test_expect2 = test_rows if test_expect is None else test_expect
        _assert_rows_are_equals(test_expect2, test_actual, print_tables)
        return test_filename

    def _write_temp_avro_file(test_rows, test_schema):
        test_filename = _get_tempfile_path()
        print("Writing avro file:", test_filename)
        toavro(test_rows, test_filename, schema=test_schema)
        return test_filename

    def _write_to_avro_file(test_rows, test_schema, test_expect=None, print_tables=True):
        _show__expect_rows(test_rows, print_tables)
        test_filename = _write_temp_avro_file(test_rows, test_schema)
        test_actual = fromavro(test_filename)
        test_expect2 = test_rows if test_expect is None else test_expect
        _assert_rows_are_equals(test_expect2, test_actual, print_tables)

    def _append_to_avro_file(test_rows1, test_rows2, test_schema, test_expect=None, print_tables=True):
        _show__expect_rows(test_rows1, print_tables)
        _show__expect_rows(test_rows2, print_tables)
        test_filename = _get_tempfile_path()
        toavro(test_rows1, test_filename, schema=test_schema)
        appendavro(test_rows2, test_filename, schema=test_schema)

        test_actual = fromavro(test_filename)
        if test_expect is not None:
            test_expect2 = test_expect
        else:
            test_expect2 = cat(test_rows1, test_rows2)
        _assert_rows_are_equals(test_expect2, test_actual, print_tables)

    # endregion

    # region Helpers

    def _assert_rows_are_equals(test_expect, test_actual, print_tables=True):
        if print_tables:
            _show__rows_from('Actual:', test_actual)
            avro_schema = test_actual.get_avro_schema()
            print('\nSchema:\n', avro_schema)
        ieq(test_expect, test_actual)
        ieq(test_expect, test_actual)  # verify can iterate twice

    def _show__expect_rows(test_rows, print_tables=True, limit=0):
        if print_tables:
            _show__rows_from('\nExpected:', test_rows, limit)

    def _show__rows_from(label, test_rows, limit=0):
        print(label)
        print(look(test_rows, limit=limit))

    def _decs(float_value, rounding=12):
        return Decimal(str(round(float_value, rounding)))

    def _utc(year, month, day, hour=0, minute=0, second=0, microsecond=0):
        u = datetime(year, month, day, hour, minute, second, microsecond)
        if PY3:
            return u.replace(tzinfo=timezone.utc)
        return u.replace(tzinfo=pytz.utc)

    def _get_tempfile_path(delete_on_close=False):
        f = NamedTemporaryFile(delete=delete_on_close, mode='r')
        test_filename = f.name
        f.close()
        return test_filename

    def _create_avro_example(test_schema, test_table):
        parsed_schema = fastavro.parse_schema(test_schema)
        rows = dicts(test_table)
        with NamedTemporaryFile(delete=False, mode='wb') as fo:
            fastavro.writer(fo, parsed_schema, rows)
            return fo.name

    # endregion

    # region Mockup data

    header1 = [u'name', u'friends', u'age']

    rows1 = [[u'Bob', 42, 33],
             [u'Jim', 13, 69],
             [u'Joe', 86, 17],
             [u'Ted', 23, 51]]

    table1 = [header1] + rows1

    table11 = [header1] + rows1[0:2]
    table12 = [header1] + rows1[2:]

    table01 = [header1[0:2]] + [item[0:2] for item in rows1]
    table10 = [header1] + [item[0:2] + [None] for item in rows1]

    table2 = [[u'name', u'age', u'birthday', u'death', u'insurance', u'deny'],
              [u'pete', 17, date(2012, 10, 11),
                  _utc(2018, 10, 14, 15, 16, 17, 18000), Decimal('1.100'), False],
              [u'mike', 27, date(2002, 11, 12),
                  _utc(2015, 12, 13, 14, 15, 16, 17000), Decimal('1.010'), False],
              [u'zack', 37, date(1992, 12, 13),
                  _utc(2010, 11, 12, 13, 14, 15, 16000), Decimal('123.456'), True],
              [u'gene', 47, date(1982, 12, 25),
                  _utc(2009, 10, 11, 12, 13, 14, 15000), Decimal('-1.010'), False]]

    table21 = table2[0:3]
    table22 = [table2[0]] + table2[3:]

    table3 = [[u'name', u'age', u'birthday', u'death'],
              [u'pete', 17, date(2012, 10, 11),
                  _utc(2018, 10, 14, 15, 16, 17, 18000)],
              [u'mike', 27, date(2002, 11, 12),
                  _utc(2015, 12, 13, 14, 15, 16, 17000)],
              [u'zack', 37, date(1992, 12, 13),
                  _utc(2010, 11, 12, 13, 14, 15, 16000)],
              [u'gene', 47, date(1982, 12, 25),
                  _utc(2009, 10, 11, 12, 13, 14, 15000)]]

    table4 = [[u'name', u'friends', u'age', u'birthday'],
              [u'Bob', 42, 33, date(2012, 10, 11)],
              [u'Jim', 13, 69, None],
              [None, 86, 17, date(1992, 12, 13)],
              [u'Ted', 23, None, date(1982, 12, 25)]]

    table5 = [[u'palette', u'colors'],
              [u'red', [u'red', u'salmon', u'crimson', u'firebrick', u'coral']],
              [u'pink', [u'pink', u'rose']],
              [u'purple', [u'purple', u'violet', u'fuchsia',
                           u'magenta', u'indigo', u'orchid', u'lavender']],
              [u'green', [u'green', u'lime', u'seagreen',
                          u'grass', u'olive', u'forest', u'teal']],
              [u'blue', [u'blue', u'cyan', u'aqua', u'aquamarine',
                         u'turquoise', u'royal', u'sky', u'navy']],
              [u'gold',  [u'gold', u'yellow', u'khaki',
                          u'mocassin', u'papayawhip', u'lemonchiffon']],
              [u'black',  None]]

    header6 = [u'array_string', u'array_record', u'nulable_date',
               u'multi_union_time', u'array_bytes_decimal', u'array_fixed_decimal']

    rows61 = [[u'a', u'b', u'c'],
              [{u'f1': u'1', u'f2': Decimal('654.321')}],
              date(2020, 1, 10),
              _utc(2020, 12, 19, 18, 17, 16, 15000),
              [Decimal('123.456')],
              [Decimal('987.654')], ]

    rows62 = [[u'a', u'b', u'c'],
              [{u'f1': u'1', u'f2': Decimal('654.321')}],
              date(2020, 1, 10),
              _utc(2020, 12, 19, 18, 17, 16, 15000),
              [Decimal('123.456'), Decimal('456.789')],
              [Decimal('987.654'), Decimal('321.123'), Decimal('456.654')]]

    table61 = [header6, rows61]

    table62 = [header6, rows62]

    table63 = [header6, rows61, rows62]

    table60 = [header6, [rows61[0], rows61[1], ]]

    header7 = [u'col', u'sqrt_pow_ij']

    rows70 = [[j, [round(math.sqrt(math.pow(i*j, i+j)), 9)
                   for i in range(1, j+1)]] for j in range(1, 7)]

    rows71 = [[j, [Decimal(str(round(math.sqrt(math.pow(i*j, i+j)), 9)))
                   for i in range(1, j+1)]] for j in range(1, 7)]

    table70 = [header7] + rows70
    table71 = [header7] + rows71

    header8 = [u'number', u'properties']

    rows8 = [[_decs(x), { 
                    u'atan': _decs(math.atan(x)),
                    u'sin': math.sin(x), 
                    u'cos': math.cos(x), 
                    u'tan': math.tan(x), 
                    u'square': x*x, 
                    u'sqrt': math.sqrt(x), 
                    u'log': math.log(x), 
                    u'log10': math.log10(x), 
                    u'exp': math.log10(x), 
                    u'power_x': x**x, 
                    u'power_minus_x': x**-x, 
                }] for x in range(1, 12)]

    table8 = [header8] + rows8

    rows9 = [[1, { u'name': u'Bob', u'age': 20 }],
             [2, { u'name': u'Ted', u'budget': _decs(54321.25) }],
             [2, { u'name': u'Jim', u'color': u'blue' }],
             [2, { u'name': u'Joe', u'alias': u'terminator' }]]

    table9 = [header8] + rows9

    # endregion

    # region testing

    # endregion

# end of tests #

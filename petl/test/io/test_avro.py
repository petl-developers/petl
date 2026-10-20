# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division

import math

from collections import OrderedDict
from datetime import datetime, date
from decimal import Decimal, localcontext
from tempfile import NamedTemporaryFile

import pytest

from petl.compat import PY3
from petl.transform.basics import cat
from petl.util.base import dicts
from petl.util.vis import look

from petl.test.helpers import ieq

from petl.io.avro import fromavro, toavro, appendavro
from petl.io.avro import _get_definition_from_type_of, precision_and_scale

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

    def test_toavro_header_only():
        _write_to_avro_file([('name', 'friends', 'age')], None)

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

    # region Decimal schema inference

    def test_toavro_decimal_zero():
        # a zero has no digits to take the logarithm of
        _write_to_avro_file(table_dec_zero, None)
        _assert_decimal_type(table_dec_zero, 8, 2)

    def test_toavro_decimal_small():
        # more fractional digits than the default precision of 8
        _write_to_avro_file(table_dec_small, None)
        _assert_decimal_type(table_dec_small, 9, 9)

    def test_toavro_decimal_integral():
        # positive exponents are trailing zeros, not fractional digits
        _write_to_avro_file(table_dec_integral, None)
        _assert_decimal_type(table_dec_integral, 8, 0)

    def test_toavro_decimal_mixed_scales():
        # writing rescales a value to the scale of the schema, so the
        # precision has to cover the widest unscaled integer of the column
        _write_to_avro_file(table_dec_mixed, None)
        _assert_decimal_type(table_dec_mixed, 20, 11)

    def test_toavro_decimal_nested():
        # arrays and records reach the same inference helper
        _write_to_avro_file(table_dec_nested, None)

    def test_precision_and_scale_of_each_shape():
        for value, prec, scale in decimal_shapes:
            actual = precision_and_scale(value)[:2]
            assert actual == (prec, scale), \
                'precision_and_scale(%s): got %r, expected %r' \
                % (value, actual, (prec, scale))

    def test_inferred_decimal_type_obeys_avro_spec():
        # the spec asks for a precision "greater than zero" and a scale
        # "zero or a positive integer less than or equal to the precision"
        for column in _all_decimal_columns():
            prec, scale = _infer_decimal_type(column)
            assert prec > 0, \
                'column %r got precision %d' % (_labels(column), prec)
            assert 0 <= scale <= prec, \
                'column %r got scale %d > precision %d' \
                % (_labels(column), scale, prec)

    def test_inferred_decimal_type_fits_every_value():
        # a decimal is `unscaled * 10 ** -scale`, so at the scale of the
        # schema each value must stay within the declared precision
        for column in _all_decimal_columns():
            prec, scale = _infer_decimal_type(column)
            for value in column:
                if value is None:
                    continue
                digits = _unscaled_digits(value, scale)
                assert digits <= prec, \
                    'column %r: %s needs %d digits, precision is %d' \
                    % (_labels(column), value, digits, prec)

    def test_precision_and_scale_of_unscaled_value():
        for value, _, _ in decimal_shapes:
            _, scale, bytes_req, unscaled = precision_and_scale(value)
            assert (unscaled < 0) == (value < 0), \
                '%s came back as %d' % (value, unscaled)
            # the defining identity: unscaled * 10**-scale must equal value,
            # trailing zeros included (a plain digit-string compare doesn't
            # catch a positive-exponent value coming back short of them);
            # a wide context avoids scaleb rounding the widest fixtures
            with localcontext() as ctx:
                ctx.prec = 60
                assert Decimal(unscaled).scaleb(-scale) == value, \
                    '%s came back as %d at scale %d' % (value, unscaled, scale)
            assert _significand('{0:f}'.format(value)) \
                == _significand('%d' % unscaled), \
                '%s came back as %d' % (value, unscaled)
            # enough room for the two's complement of the unscaled integer
            assert 2 ** (8 * bytes_req - 1) > abs(unscaled), \
                '%s needs more than %d bytes' % (value, bytes_req)
            assert bytes_req == 1 or 2 ** (8 * bytes_req - 9) <= abs(unscaled), \
                '%s does not need %d bytes' % (value, bytes_req)

    def test_precision_and_scale_of_non_finite():
        for value in [Decimal('NaN'), Decimal('Infinity'), Decimal('-Inf')]:
            try:
                precision_and_scale(value)
            except ValueError as vex:
                # the message has to name the value that cannot be written
                assert str(value) in '%s' % vex, \
                    '%s is not reported by: %s' % (value, vex)
                continue
            assert False, 'built a decimal schema for %s' % value

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

    def _assert_decimal_type(test_rows, prec, scale):
        column = [row[1] for row in test_rows[1:]]
        actual = _infer_decimal_type(column)
        assert actual == (prec, scale), \
            'column %r got %r, expected %r' \
            % (_labels(column), actual, (prec, scale))

    def _all_decimal_columns():
        return [[value] for value, _, _ in decimal_shapes] + decimal_columns

    def _labels(column):
        return [str(value) for value in column]

    def _infer_decimal_type(column):
        '''the precision and scale petl infers for a column of decimals'''
        prev = None
        tdef = None
        for value in column:
            curr, dcurr = _get_definition_from_type_of(u'amount', value, prev)
            # a None row leaves the definition built so far untouched
            if curr is not None:
                tdef = curr
            if dcurr is not None:
                prev = dcurr
        return tdef['precision'], tdef['scale']

    def _significand(text):
        '''the digits of a number, without sign, point or padding zeros'''
        return text.replace('-', '').replace('.', '').strip('0')

    def _unscaled_digits(value, scale):
        '''digit count of `value * 10 ** scale`, taken from its text form

        Deliberately built from the fixed point representation instead of
        `Decimal.as_tuple()`, which is what the code under test reads, and
        without arithmetic, which would round on the decimal context.
        '''
        fixed = '{0:f}'.format(value)
        fraction = fixed.split('.')[1] if '.' in fixed else ''
        assert len(fraction) <= scale, \
            '%s does not fit a scale of %d' % (value, scale)
        text = fixed.replace('-', '').replace('.', '')
        text = text + '0' * (scale - len(fraction))
        return len(text.lstrip('0')) or 1

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

    # (value, precision, scale) of the avro decimal logicalType, where the
    # precision is the digit count of the unscaled integer and the scale the
    # count of fractional digits
    decimal_shapes = [
        # zeros, in every shape Decimal can hold one
        (Decimal('0'), 1, 0),
        (Decimal('-0'), 1, 0),
        (Decimal('0.0'), 1, 1),
        (Decimal('0.00'), 2, 2),
        # a zero keeps the width implied by its own exponent
        (Decimal('0E+2'), 3, 0),
        (Decimal('0E-7'), 7, 7),
        # integers, including the exact powers of ten
        (Decimal('1'), 1, 0),
        (Decimal('42'), 2, 0),
        (Decimal('-99'), 2, 0),
        (Decimal('10'), 2, 0),
        (Decimal('100'), 3, 0),
        (Decimal('12345'), 5, 0),
        # ordinary fractions
        (Decimal('1.5'), 2, 1),
        (Decimal('99.99'), 4, 2),
        (Decimal('123.45'), 5, 2),
        (Decimal('100.00'), 5, 2),
        (Decimal('-3.14159'), 6, 5),
        # fewer digits than fractional places
        (Decimal('0.1'), 1, 1),
        (Decimal('0.001'), 3, 3),
        (Decimal('0.00000001'), 8, 8),
        (Decimal('1E-9'), 9, 9),
        (Decimal('1E-12'), 12, 12),
        # positive exponents: trailing zeros are part of the integer
        (Decimal('1E+2'), 3, 0),
        (Decimal('1.5E+3'), 4, 0),
        (Decimal('-2E+5'), 6, 0),
        (Decimal('1E+20'), 21, 0),
        # more digits than the decimal context holds by default
        (Decimal('12345678901234.5678'), 18, 4),
        (Decimal('0.123456789012345678'), 18, 18),
        (Decimal('123456789012345678901234567890'), 30, 0),
    ]

    # columns whose values disagree on scale, precision or both
    decimal_columns = [
        [Decimal('0.00'), Decimal('12.34')],
        [Decimal('1'), Decimal('0'), Decimal('-0'), Decimal('0.00')],
        [Decimal('1.5'), Decimal('0.001')],
        [Decimal('123456789.1'), Decimal('0.12345678901')],
        [Decimal('1E-12'), Decimal('1.5')],
        [Decimal('0.001'), Decimal('1E-12')],
        [Decimal('1E+7'), Decimal('0.5')],
        [Decimal('9.99'), Decimal('1E-9'), Decimal('123456.789')],
        [Decimal('1E+20'), Decimal('0.000001')],
        [Decimal('0.5'), Decimal('1E+2'), Decimal('1E-8')],
        [Decimal('1.100'), None, Decimal('0')],
    ]

    header_dec = [u'name', u'amount']

    table_dec_zero = [header_dec,
                      [u'pete', Decimal('0.00')],
                      [u'mike', Decimal('12.34')],
                      [u'zack', Decimal('-0')]]

    table_dec_small = [header_dec,
                       [u'pete', Decimal('1E-9')],
                       [u'mike', Decimal('0.000000025')]]

    table_dec_integral = [header_dec,
                          [u'pete', Decimal('1E+2')],
                          [u'mike', Decimal('-2E+5')]]

    table_dec_mixed = [header_dec,
                       [u'pete', Decimal('123456789.1')],
                       [u'mike', Decimal('0.12345678901')]]

    table_dec_nested = [[u'name', u'amounts', u'totals'],
                        [u'pete', [Decimal('0.00'), Decimal('1E-9')],
                         OrderedDict([(u'due', Decimal('0')),
                                      (u'paid', Decimal('1E+2'))])]]

    # endregion

    # region testing

    # endregion

# end of tests #

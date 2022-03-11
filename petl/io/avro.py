# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import sys
import math
from collections import OrderedDict
from datetime import datetime, date, time
from decimal import Decimal

from petl.compat import izip_longest, text_type, string_types, PY3
from petl.io.sources import read_source_from_arg, write_source_from_arg
from petl.transform.headers import skip, setheader
from petl.util.base import Table, dicts, fieldnames, iterpeek, wrap

# region API


def fromavro(source, limit=None, skips=0, **avro_args):
    """Extract a table from the records of a avro file.

    The `source` argument (string or file-like or fastavro.reader) can either
    be  the path of the file, a file-like input stream or a instance from
    fastavro.reader.

    The `limit` and `skip` arguments can be used to limit the range of rows 
    to extract.

    The `sample` argument (int, optional) defines how many rows are inspected
    for discovering the field types and building a schema for the avro file 
    when the `schema` argument is not passed.

    The rows fields read from file can have scalar values like int, string,
    float, datetime, date and decimal but can also have compound types like 
    enum, :ref:`array <array_schema>`, map, union and record. 
    The fields types can also have recursive structures defined 
    in :ref:`complex schemas <complex_schema>`.

    Also types with :ref:`logical types <logical_schema>` types are read and 
    translated to coresponding python types: long timestamp-millis and 
    long timestamp-micros: datetime.datetime, int date: datetime.date, 
    bytes decimal and fixed decimal: Decimal, int time-millis and 
    long time-micros: datetime.time.

    Example usage for reading files::

        >>> # set up a Avro file to demonstrate with
        ...
        >>> schema1 = {
        ...     'doc': 'Some people records.',
        ...     'name': 'People',
        ...     'namespace': 'test',
        ...     'type': 'record',
        ...     'fields': [
        ...         {'name': 'name', 'type': 'string'},
        ...         {'name': 'friends', 'type': 'int'},
        ...         {'name': 'age', 'type': 'int'},
        ...     ]
        ... }
        ...
        >>> records1 = [
        ...     {'name': 'Bob', 'friends': 42, 'age': 33},
        ...     {'name': 'Jim', 'friends': 13, 'age': 69},
        ...     {'name': 'Joe', 'friends': 86, 'age': 17},
        ...     {'name': 'Ted', 'friends': 23, 'age': 51}
        ... ]
        ...
        >>> import fastavro
        >>> parsed_schema1 = fastavro.parse_schema(schema1)
        >>> with open('example.file1.avro', 'wb') as f1:
        ...     fastavro.writer(f1, parsed_schema1, records1)
        ...
        >>> # now demonstrate the use of fromavro()
        >>> import petl as etl
        >>> tbl1 = etl.fromavro('example.file1.avro')
        >>> tbl1
        +-------+---------+-----+
        | name  | friends | age |
        +=======+=========+=====+
        | 'Bob' |      42 |  33 |
        +-------+---------+-----+
        | 'Jim' |      13 |  69 |
        +-------+---------+-----+
        | 'Joe' |      86 |  17 |
        +-------+---------+-----+
        | 'Ted' |      23 |  51 |
        +-------+---------+-----+

    .. versionadded:: 1.4.0

    """

    source2 = read_source_from_arg(source)
    return AvroView(source=source2,
                    limit=limit,
                    skips=skips,
                    **avro_args)


def toavro(table, target, schema=None, sample=9,
           codec='deflate', compression_level=None, **avro_args):
    """
    Write the table into a new avro file according to schema passed.

    This method assume that each column has values with the same type 
    for all rows of the source `table`.

    `Apache Avro`_ is a data
    serialization framework. It is used in data serialization (especially in
    Hadoop ecosystem), for dataexchange for databases (Redshift) and RPC 
    protocols (like in Kafka). It has libraries to support many languages and
    generally is faster and safer than text formats like Json, XML or CSV.

    The `target` argument is the file path for creating the avro file.
    Note that if a file already exists at the given location, it will be
    overwritten.

    The `schema` argument (dict) defines the rows field structure of the file.
    Check fastavro `documentation`_ and Avro schema `reference`_ for details.

    The `sample` argument (int, optional) defines how many rows are inspected
    for discovering the field types and building a schema for the avro file 
    when the `schema` argument is not passed.

    The `codec` argument (string, optional) sets the compression codec used to
    shrink data in the file. It can be 'null', 'deflate' (default), 'bzip2' or
    'snappy', 'zstandard', 'lz4', 'xz' (if installed)

    The `compression_level` argument (int, optional) sets the level of 
    compression to use with the specified codec (if the codec supports it)

    Additionally there are support for passing extra options in the 
    argument `**avro_args` that are fowarded directly to fastavro. Check the
    fastavro `documentation`_ for reference.

    The avro file format preserves type information, i.e., reading and writing
    is round-trippable for tables with non-string data values. However the
    conversion from Python value types to avro fields is not perfect. Use the
    `schema` argument to define proper type to the conversion.

    The following avro types are supported by the schema: null, boolean, 
    string, int, long, float, double, bytes, fixed, enum, 
    :ref:`array <array_schema>`, map, union, record, and recursive types 
    defined in :ref:`complex schemas <complex_schema>`.

    Also :ref:`logical types <logical_schema>` are supported and translated to 
    coresponding python types: long timestamp-millis, long timestamp-micros, int date, 
    bytes decimal, fixed decimal, string uuid, int time-millis, long time-micros.

    Example usage for writing files::

        >>> # set up a Avro file to demonstrate with
        >>> table2 = [['name', 'friends', 'age'],
        ...           ['Bob', 42, 33],
        ...           ['Jim', 13, 69],
        ...           ['Joe', 86, 17],
        ...           ['Ted', 23, 51]]
        ...
        >>> schema2 = {
        ...     'doc': 'Some people records.',
        ...     'name': 'People',
        ...     'namespace': 'test',
        ...     'type': 'record',
        ...     'fields': [
        ...         {'name': 'name', 'type': 'string'},
        ...         {'name': 'friends', 'type': 'int'},
        ...         {'name': 'age', 'type': 'int'},
        ...     ]
        ... }
        ...
        >>> # now demonstrate what writing with toavro()
        >>> import petl as etl
        >>> etl.toavro(table2, 'example.file2.avro', schema=schema2)
        ...
        >>> # this was what was saved above
        >>> tbl2 = etl.fromavro('example.file2.avro')
        >>> tbl2
        +-------+---------+-----+
        | name  | friends | age |
        +=======+=========+=====+
        | 'Bob' |      42 |  33 |
        +-------+---------+-----+
        | 'Jim' |      13 |  69 |
        +-------+---------+-----+
        | 'Joe' |      86 |  17 |
        +-------+---------+-----+
        | 'Ted' |      23 |  51 |
        +-------+---------+-----+

    .. versionadded:: 1.4.0

    .. _Apache Avro: https://avro.apache.org/docs/current/spec.html
    .. _reference: https://avro.apache.org/docs/current/spec.html#schemas
    .. _documentation : https://fastavro.readthedocs.io/en/latest/writer.html

    """
    _write_toavro(table,
                  target=target,
                  mode='wb',
                  schema=schema,
                  sample=sample,
                  codec=codec,
                  compression_level=compression_level,
                  **avro_args)


def appendavro(table, target, schema=None, sample=9, **avro_args):
    """
    Append rows into a avro existing avro file or create a new one.

    The `target` argument can be either an existing avro file or the file 
    path for creating new one.

    The `schema` argument is checked against the schema of the existing file.
    So it must be the same schema as used by `toavro()` or the schema of the
    existing file.

    The `sample` argument (int, optional) defines how many rows are inspected
    for discovering the field types and building a schema for the avro file 
    when the `schema` argument is not passed.

    Additionally there are support for passing extra options in the 
    argument `**avro_args` that are fowarded directly to fastavro. Check the
    fastavro documentation for reference.

    See :meth:`petl.io.avro.toavro` method for more information and examples.

    .. versionadded:: 1.4.0

    """
    _write_toavro(table,
                  target=target,
                  mode='a+b',
                  schema=schema,
                  sample=sample,
                  **avro_args)

# endregion API

# region Implementation


class AvroView(Table):
    '''Read rows from avro file with their types and logical types'''

    def __init__(self, source, limit, skips, **avro_args):
        self.source = source
        self.limit = limit
        self.skip = skips
        self.avro_args = avro_args
        self.avro_schema = None

    def get_avro_schema(self):
        '''gets the schema stored in avro file header'''
        return self.avro_schema

    def __iter__(self):
        with self.source.open('rb') as source_file:
            avro_reader = self._open_reader(source_file)
            header = self._decode_schema(avro_reader)
            yield header
            for row in self._read_rows_from(avro_reader, header):
                yield row

    def _open_reader(self, source_file):
        '''This could raise a error when the file is corrupt or is not avro'''
        # delay the import of fastavro for not breaking when unused
        import fastavro
        avro_reader = fastavro.reader(source_file, **self.avro_args)
        return avro_reader

    def _decode_schema(self, avro_reader):
        '''extract the header from schema stored in avro file header'''
        self.avro_schema = avro_reader.writer_schema
        if self.avro_schema is None:
            return None, None
        schema_fields = self.avro_schema['fields']
        header = tuple(col['name'] for col in schema_fields)
        return header

    def _read_rows_from(self, avro_reader, header):
        count = 0
        maximum = self.limit if self.limit is not None else sys.maxsize
        for i, record in enumerate(avro_reader):
            if i < self.skip:
                continue
            if count >= maximum:
                break
            count += 1
            row = self._map_row_from(header, record)
            yield row

    def _map_row_from(self, header, record):
        '''
        fastavro auto converts logical types defined in avro schema to 
        correspoding python types. E.g: 
        - avro type: long logicalType: timestamp-millis -> python datetime
        - avro type: int logicalType: date              -> python date
        - avro type: bytes logicalType: decimal         -> python Decimal
        '''
        if header is None or PY3:
            r = tuple(record.values())
        else:
            # fastavro on python2 does not respect dict order
            r = tuple(record.get(col) for col in header)
        return r


def _write_toavro(table, target, mode, schema, sample,
                  codec='deflate', compression_level=None, **avro_args):
    if table is None:
        return
    # build a schema when not defined by user
    if not schema:
        schema, table2 = _build_schema_from_values(table, sample)
    else:
        table2 = _fix_missing_headers(table, schema)
    # fastavro expects a iterator of dicts
    rows = dicts(table2) if PY3 else _ordered_dict_iterator(table2)

    target2 = write_source_from_arg(target, mode=mode)
    with target2.open(mode) as target_file:
        # delay the import of fastavro for not breaking when unused
        from fastavro import parse_schema
        from fastavro.write import Writer

        parsed_schema = parse_schema(schema)
        writer = Writer(fo=target_file,
                        schema=parsed_schema,
                        codec=codec,
                        compression_level=compression_level,
                        **avro_args)
        num = 1
        for record in rows:
            try:
                writer.write(record)
                num = num + 1
            except ValueError as verr:
                vmsg = _get_error_details(target, num, verr, record, schema)
                _raise_error(ValueError, vmsg)
            except TypeError as terr:
                tmsg = _get_error_details(target, num, terr, record, schema)
                _raise_error(TypeError, tmsg)
        # finish writing
        writer.flush()

# endregion Implementation

# region Helpers


def _build_schema_from_values(table, sample):
    # table2: try not advance iterators
    samples, table2 = iterpeek(table, sample + 1)
    props = fieldnames(samples)
    peek = skip(samples, 1)
    schema_fields = _build_schema_fields_from_values(peek, props)
    schema_source = _build_schema_with(schema_fields)
    return schema_source, table2


def _build_schema_with(schema_fields):
    schema = {
        'type': 'record',
        'name': 'output',
        'namespace': 'avro',
        'doc': 'generated by petl',
        'fields': schema_fields,
    }
    return schema


def _build_schema_fields_from_values(peek, props):
    # store the previous for calculate max precision and max scale
    previous = OrderedDict()
    # set a default when value is None in the first row  but allow override after
    fill_missing = True
    fields = OrderedDict()
    # iterate on sample rows for dealing with columns with None values
    for row in peek:
        _update_field_defs_from(props, row, fields, previous, fill_missing)
        fill_missing = False

    schema_fields = [item for item in fields.values()]
    return schema_fields


def _update_field_defs_from(props, row, fields, previous, fill_missing):
    for prop, val in izip_longest(props, row):
        if prop is None:
            break
        dprev = previous.get(prop + '_prec')
        fprev = previous.get(prop + '_prop')
        fcurr = None
        if isinstance(val, dict):
            # get the fields from a recursive definition of record inside this field
            tdef, dcurr, fcurr = _get_definition_from_record(prop, val, fprev, dprev, fill_missing)
        else:
            # get the field definition for building the schema
            tdef, dcurr = _get_definition_from_type_of(prop, val, dprev)

        if tdef is not None:
            fields[prop] = {'name': prop, 'type': ['null', tdef]}
        elif fill_missing:
            fields[prop] = {'name': prop, 'type': ['null', 'string']}
        if dcurr is not None:
            previous[prop + '_prec'] = dcurr
        if fcurr is not None:
            previous[prop + '_prop'] = fcurr


def _get_definition_from_type_of(prop, val, prev):
    # TODO: get type for enum, map and other python types
    tdef = None
    curr = None
    if isinstance(val, datetime):
        tdef = {'type': 'long', 'logicalType': 'timestamp-millis'}
    elif isinstance(val, time):
        tdef = {'type': 'int', 'logicalType': 'time-millis'}
    elif isinstance(val, date):
        tdef = {'type': 'int', 'logicalType': 'date'}
    elif isinstance(val, Decimal):
        curr, precision, scale = _get_precision_from_decimal(curr, val, prev)
        tdef = {'type': 'bytes', 'logicalType': 'decimal',
                'precision': precision, 'scale': scale, }
    elif isinstance(val, bytes):
        tdef = 'bytes'
    elif isinstance(val, list):
        tdef, curr = _get_definition_from_array(prop, val, prev)
    elif isinstance(val, bool):
        tdef = 'boolean'
    elif isinstance(val, float):
        tdef = 'double'
    elif isinstance(val, int):
        tdef = 'long'
    elif val is not None:
        tdef = 'string'
    else:
        return None, None
    return tdef, curr


def _get_definition_from_array(prop, val, prev):
    afield = None
    for item in iter(val):
        if item is None:
            continue
        field2, curr2 = _get_definition_from_type_of(prop, item, prev)
        if field2 is not None:
            afield = field2
        if curr2 is not None:
            prev = curr2

    bfield = 'string' if afield is None else afield
    tdef = {'type': 'array', 'items': bfield}
    return tdef, prev


def _get_definition_from_record(prop, val, fprev, dprev, fill_missing):
    if fprev is None:
        fprev = OrderedDict()
    if dprev is None:
        dprev = OrderedDict()
    props = list(val.keys())
    row = list(val.values())

    _update_field_defs_from(props, row, fprev, dprev, fill_missing)

    schema_fields = [item for item in fprev.values()]
    tdef = {
        'type': 'record',
        'name': prop + '_record',
        'namespace': 'avro',
        'fields': schema_fields,
    }
    return tdef, dprev, fprev


def _get_precision_from_decimal(curr, val, prev):
    if val is None:
        prec = scale = 0
    else:
        prec, scale, _, _ = precision_and_scale(val)
    if prev is not None:
        # get the greatests precision and scale of the sample
        prec0, scale0 = prev.get('precision'), prev.get('scale')
        prec, scale = max(prec, prec0), max(scale, scale0)
    prec = max(prec, 8)
    curr = {'precision': prec, 'scale': scale, }
    return curr, prec, scale


def precision_and_scale(numeric_value):
    sign, digits, exp = numeric_value.as_tuple()
    number = 0
    for digit in digits:
        number = (number * 10) + digit
    # delta = exp + scale
    delta = 1
    number = 10 ** delta * number
    inumber = int(number)

    bits_req = inumber.bit_length() + 1
    bytes_req = (bits_req + 8) // 8
    if sign:
        inumber = - inumber
    prec = int(math.ceil(math.log10(abs(inumber))))
    scale = abs(exp)
    return prec, scale, bytes_req, inumber


def _fix_missing_headers(table, schema):
    '''add missing columns headers from schema'''
    if schema is None or 'fields' not in schema:
        return table
    # table2: try not advance iterators
    sample, table2 = iterpeek(table, 2)
    cols = fieldnames(sample)
    headers = _get_schema_header_names(schema)
    if len(cols) >= len(headers):
        return table2
    table3 = setheader(table2, headers)
    return table3


def _get_error_details(target, num, err, record, schema):
    '''show last row when failed writing for throubleshooting'''
    headers = _get_schema_header_names(schema)
    if isinstance(record, dict):
        table = [headers, list(record.values())]
    else:
        table = [headers, record]
    example = wrap(table).look()
    dest = " output: %s" % target if isinstance(target, string_types) else ''
    printed = "failed writing on row #%d: %s\n%s\n schema: %s\n%s"
    details = printed % (num, err, dest, schema, example)
    return details


def _get_schema_header_names(schema):
    fields = schema.get('fields')
    if fields is None:
        return []
    header = [field.get('name') for field in fields]
    return header


def _raise_error(ErrorType, new_message):
    """Works like raise Excetion(msg) from prev_exp in python3."""
    exinf = sys.exc_info()
    tracebk = exinf[2]
    try:
        if PY3:
            raise ErrorType(new_message).with_traceback(tracebk)
        # Python2 compatibility workaround
        exec('raise ErrorType, new_message, tracebk')
    finally:
        exinf = None
        tracebk = None  # noqa: F841


def _ordered_dict_iterator(table):
    it = iter(table)
    hdr = next(it)
    flds = [text_type(f) for f in hdr]
    for row in it:
        items = list()
        for i, f in enumerate(flds):
            try:
                v = row[i]
            except IndexError:
                v = None
            items.append((f, v))
        yield OrderedDict(items)


Table.toavro = toavro
Table.appendavro = appendavro

# endregion

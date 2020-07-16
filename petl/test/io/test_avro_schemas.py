# -*- coding: utf-8 -*-

# begin_nullable_schema

schema0 = {
    'doc': 'Nullable records.',
    'name': 'anyone',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': ['null', 'string']},
        {'name': 'friends', 'type': ['null', 'int']},
        {'name': 'age', 'type': ['null', 'int']},
    ],
}

# end_nullable_schema

# begin_basic_schema

schema1 = {
    'doc': 'Some people records.',
    'name': 'People',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'friends', 'type': 'int'},
        {'name': 'age', 'type': 'int'},
    ],
}

# end_basic_schema

# begin_logicalType_schema

schema2 = {
    'doc': 'Some random people.',
    'name': 'Crowd',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'},
        {'name': 'birthday', 'type': {
            'type': 'int',
            'logicalType': 'date'
        }},
        {'name': 'death', 'type': {
            'type': 'long',
            'logicalType': 'timestamp-millis'
        }},
        {'name': 'insurance', 'type': {
            'type': 'bytes',
            'logicalType': 'decimal',
            'precision': 12,
            'scale': 3
        }},
        {'name': 'deny', 'type': 'boolean'},
    ],
}

# end_logicalType_schema

# begin_micros_schema

schema3 = {
    'doc': 'Some random people.',
    'name': 'Crowd',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'},
        {'name': 'birthday', 'type': {
            'type': 'int',
            'logicalType': 'date'
        }},
        {'name': 'death', 'type': {
            'type': 'long',
            'logicalType': 'timestamp-micros'
        }},
    ],
}

# end_micros_schema

# begin_mixed_schema

schema4 = {
    'doc': 'Some people records.',
    'name': 'People',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'name', 'type': ['null', 'string']},
        {'name': 'friends', 'type':  ['null', 'long']},
        {'name': 'age', 'type': ['null', 'int']},
        {'name': 'birthday',
            'type': ['null', {'type': 'int', 'logicalType': 'date'}]
         }
    ],
}

# end_mixed_schema

# begin_array_schema

schema5 = {
    'name': 'palettes',
    'namespace': 'color',
    'type': 'record',
    'fields': [
        {'name': 'palette', 'type': 'string'},
        {'name': 'colors',
            'type': ['null', {'type': 'array', 'items': 'string'}]
         }
    ],
}

# end_array_schema

# begin_complex_schema

schema6 = {
    'fields': [
        {
            'name': 'array_string',
            'type': {'type': 'array', 'items': 'string'}
        },
        {
            'name': 'array_record',
            'type': {'type': 'array', 'items': {
                'type': 'record',
                'name': 'some_record',
                'fields': [
                    {
                        'name': 'f1',
                        'type': 'string'
                    },
                    {
                        'name': 'f2',
                        'type': {'type': 'bytes',
                                 'logicalType': 'decimal',
                                 'precision': 18,
                                 'scale': 6, }
                    }
                ]
            }
            }
        },
        {
            'name': 'nulable_date',
            'type': ['null', {'type': 'int',
                              'logicalType': 'date'}]
        },
        {
            'name': 'multi_union_time',
            'type': ['null', 'string', {'type': 'long',
                                        'logicalType': 'timestamp-micros'}]
        },
        {
            'name': 'array_bytes_decimal',
            'type': ['null', {'type': 'array',
                              'items': {'type': 'bytes',
                                        'logicalType': 'decimal',
                                        'precision': 18,
                                        'scale': 6, }
                              }]
        },
        {
            'name': 'array_fixed_decimal',
            'type': ['null', {'type': 'array',
                              'items': {'type': 'fixed',
                                        'name': 'FixedDecimal',
                                        'size': 8,
                                        'logicalType': 'decimal',
                                        'precision': 18,
                                        'scale': 6, }
                              }]
        },
    ],
    'namespace': 'namespace',
    'name': 'name',
    'type': 'record'
}

# end_complex_schema

# begin_logical_schema

logical_schema = {
    'fields': [
        {
            'name': 'date',
            'type': {'type': 'int', 'logicalType': 'date'}
        },
        {
            'name': 'datetime',
            'type': {'type': 'long', 'logicalType': 'timestamp-millis'}
        },
        {
            'name': 'datetime2',
            'type': {'type': 'long', 'logicalType': 'timestamp-micros'}
        },
        {
            'name': 'uuid',
            'type': {'type': 'string', 'logicalType': 'uuid'}
        },
        {
            'name': 'time',
            'type': {'type': 'int', 'logicalType': 'time-millis'}
        },
        {
            'name': 'time2',
            'type': {'type': 'long', 'logicalType': 'time-micros'}
        },
        {
            'name': 'Decimal',
            'type': 
                {
                   'type': 'bytes', 'logicalType': 'decimal',
                   'precision': 15, 'scale': 6
                }
        },
        {
            'name': 'Decimal2',
            'type': 
                {
                   'type': 'fixed', 'size': 8,
                   'logicalType': 'decimal', 'precision': 15, 'scale': 3
                }
        }
    ],
    'namespace': 'namespace',
    'name': 'name',
    'type': 'record'
}

# end_logical_schema

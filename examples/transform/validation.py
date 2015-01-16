# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


# validate()
############

import petl as etl
# define some validation constraints
header = ('foo', 'bar', 'baz')
constraints = [
    dict(name='foo_int', field='foo', test=int),
    dict(name='bar_date', field='bar', test=etl.dateparser('%Y-%m-%d')),
    dict(name='baz_enum', field='baz', assertion=lambda v: v in ['Y', 'N']),
    dict(name='not_none', assertion=lambda row: None not in row)
]
# now validate a table
table = (('foo', 'bar', 'bazzz'),
         (1, '2000-01-01', 'Y'),
         ('x', '2010-10-10', 'N'),
         (2, '2000/01/01', 'Y'),
         (3, '2015-12-12', 'x'),
         (4, None, 'N'),
         ('y', '1999-99-99', 'z'),
         (6, '2000-01-01'),
         (7, '2001-02-02', 'N', True))
problems = etl.validate(table, constraints=constraints, header=header)
problems.lookall()




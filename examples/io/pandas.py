from __future__ import division, print_function, absolute_import


# todataframe()
###############

import petl as etl
table = [('foo', 'bar', 'baz'),
         ('apples', 1, 2.5),
         ('oranges', 3, 4.4),
         ('pears', 7, .1)]
df = etl.todataframe(table)
df


# fromdataframe()
#################

import petl as etl
import pandas as pd
records = [('apples', 1, 2.5), ('oranges', 3, 4.4), ('pears', 7, 0.1)]
df = pd.DataFrame.from_records(records, columns=('foo', 'bar', 'baz'))
table = etl.fromdataframe(df)
table

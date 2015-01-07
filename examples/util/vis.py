from __future__ import division, print_function, absolute_import


# look()
########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2]]
etl.look(table1)
# alternative formatting styles
etl.look(table1, style='simple')
etl.look(table1, style='minimal')
# any irregularities in the length of header and/or data
# rows will appear as blank cells
table2 = [['foo', 'bar'],
          ['a'],
          ['b', 2, True]]
etl.look(table2)


# see()
#######

import petl as etl
table = [['foo', 'bar'], ['a', 1], ['b', 2]]
etl.see(table)

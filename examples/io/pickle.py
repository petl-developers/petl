from __future__ import division, print_function, absolute_import


# frompickle()
##############

import petl as etl
import pickle
# set up a file to demonstrate with
with open('example.p', 'wb') as f:
    pickle.dump(['foo', 'bar'], f)
    pickle.dump(['a', 1], f)
    pickle.dump(['b', 2], f)
    pickle.dump(['c', 2.5], f)

# demonstrate the use of frompickle()
table1 = etl.frompickle('example.p')
table1


# topickle()
############

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
etl.topickle(table1, 'example.p')
# look what it did
table2 = etl.frompickle('example.p')
table2



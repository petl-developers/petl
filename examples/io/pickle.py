from __future__ import division, print_function, absolute_import


# frompickle()
##############

# set up a file to demonstrate with
import pickle
with open('example.p', 'wb') as f:
    pickle.dump(['foo', 'bar'], f)
    pickle.dump(['a', 1], f)
    pickle.dump(['b', 2], f)
    pickle.dump(['c', 2.5], f)

# now demonstrate the use of petl.frompickle
from petl import frompickle, look
table1 = frompickle('example.p')
look(table1)


# topickle()
############

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import topickle, look
topickle(table1, 'example.p')
# look what it did
from petl import frompickle
table2 = frompickle('example.p')
look(table2)



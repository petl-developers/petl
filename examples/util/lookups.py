from __future__ import absolute_import, division, print_function

# lookup()
##########
import petl as etl

table1 = [['foo', 'bar'], 
          ['a', 1], 
          ['b', 2], 
          ['b', 3]]
lkp = etl.lookup(table1, 'foo', 'bar')
lkp['a']
lkp['b']
# if no valuespec argument is given, defaults to the whole
# row (as a tuple)
lkp = etl.lookup(table1, 'foo')
lkp['a']
lkp['b']
# compound keys are supported
table2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
lkp = etl.lookup(table2, ('foo', 'bar'), 'baz')
lkp[('a', 1)]
lkp[('b', 2)]
lkp[('b', 3)]
# data can be loaded into an existing dictionary-like 
# object, including persistent dictionaries created via the 
# shelve module
import shelve

lkp = shelve.open('example1.dat', flag='n')
lkp = etl.lookup(table1, 'foo', 'bar', lkp)
lkp.close()
lkp = shelve.open('example1.dat', flag='r')
lkp['a']
lkp['b']


# lookupone()
#############

import petl as etl

table1 = [['foo', 'bar'], 
          ['a', 1], 
          ['b', 2], 
          ['b', 3]]
# if the specified key is not unique and strict=False (default),
# the first value wins
lkp = etl.lookupone(table1, 'foo', 'bar')
lkp['a']
lkp['b']
# if the specified key is not unique and strict=True, will raise
# DuplicateKeyError
try:
    lkp = etl.lookupone(table1, 'foo', strict=True)
except etl.errors.DuplicateKeyError as e:
    print(e)

# compound keys are supported
table2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
lkp = etl.lookupone(table2, ('foo', 'bar'), 'baz')
lkp[('a', 1)]
lkp[('b', 2)]
lkp[('b', 3)]
# data can be loaded into an existing dictionary-like 
# object, including persistent dictionaries created via the 
# shelve module
import shelve

lkp = shelve.open('example2.dat', flag='n')
lkp = etl.lookupone(table1, 'foo', 'bar', lkp)
lkp.close()
lkp = shelve.open('example2.dat', flag='r')
lkp['a']
lkp['b']


# dictlookup()
##############

import petl as etl

table1 = [['foo', 'bar'], 
          ['a', 1], 
          ['b', 2], 
          ['b', 3]]
lkp = etl.dictlookup(table1, 'foo')
lkp['a']
lkp['b']
# compound keys are supported
table2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
lkp = etl.dictlookup(table2, ('foo', 'bar'))
lkp[('a', 1)]
lkp[('b', 2)]
lkp[('b', 3)]
# data can be loaded into an existing dictionary-like 
# object, including persistent dictionaries created via the 
# shelve module
import shelve

lkp = shelve.open('example3.dat', flag='n')
lkp = etl.dictlookup(table1, 'foo', lkp)
lkp.close()
lkp = shelve.open('example3.dat', flag='r')
lkp['a']
lkp['b']


# dictlookupone()
#################

import petl as etl

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['b', 3]]
# if the specified key is not unique and strict=False (default),
# the first value wins
lkp = etl.dictlookupone(table1, 'foo')
lkp['a']
lkp['b']
# if the specified key is not unique and strict=True, will raise
# DuplicateKeyError
try:
    lkp = etl.dictlookupone(table1, 'foo', strict=True)
except etl.errors.DuplicateKeyError as e:
    print(e)

# compound keys are supported
table2 = [['foo', 'bar', 'baz'],
          ['a', 1, True],
          ['b', 2, False],
          ['b', 3, True],
          ['b', 3, False]]
lkp = etl.dictlookupone(table2, ('foo', 'bar'))
lkp[('a', 1)]
lkp[('b', 2)]
lkp[('b', 3)]
# data can be loaded into an existing dictionary-like
# object, including persistent dictionaries created via the
# shelve module
import shelve

lkp = shelve.open('example4.dat', flag='n')
lkp = etl.dictlookupone(table1, 'foo', lkp)
lkp.close()
lkp = shelve.open('example4.dat', flag='r')
lkp['a']
lkp['b']



"""
The `petl` module.

"""


from __future__ import absolute_import, print_function, division


from petl.comparison import Comparable
from petl.util import *
from petl.io import *
from petl.transform import *


__version__ = VERSION = '1.0a1.dev0'


# convenience aliases
eq = selecteq
ne = selectne
lt = selectlt
gt = selectgt
le = selectle
ge = selectge
true = selecttrue
false = selectfalse
none = selectnone
notnone = selectnotnone
counts = valuecounts
move = movefield
lol = listoflists
tot = tupleoftuples
lot = listoftuples
tol = tupleoflists

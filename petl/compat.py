from __future__ import absolute_import, print_function, division

import pickle
import sys

from decimal import Decimal
from functools import reduce
from io import StringIO, BytesIO
from itertools import filterfalse as ifilterfalse
from itertools import zip_longest as izip_longest
from urllib.request import urlopen


# TODO: Aliases retained to limit call-site churn; removal deferred to a later change.
ifilter = filter
imap = map
izip = zip
xrange = range
maketrans = str.maketrans
string_types = str,
integer_types = int,
numeric_types = bool, int, float, Decimal
text_type = str
binary_type = bytes
long = int
maxint = sys.maxsize
next = next
callable = callable

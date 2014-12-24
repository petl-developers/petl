from __future__ import absolute_import, print_function, division, \
    unicode_literals


import sys


############################
# Python 2.6 compatibility #
############################

try:
    from collections import Counter, OrderedDict
except ImportError:
    from .py26_backports import Counter, OrderedDict

try:
    from itertools import count, compress, combinations_with_replacement
except ImportError:
    from .py26_backports import count, compress, combinations_with_replacement


##########################
# Python 3 compatibility #
##########################

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    ifilter = filter
    imap = map
    izip = zip
    xrange = range
    from itertools import filterfalse as ifilterfalse
    from itertools import zip_longest as izip_longest
    from functools import reduce
    maketrans = str.maketrans
    string_types = str,
    integer_types = int,
    number_types = int, float
    class_types = type,
    text_type = str
    binary_type = bytes
    sortable_types = set()
    long = int
    from urllib.request import urlopen
    from io import StringIO, BytesIO
    import pickle
    maxint = sys.maxsize

else:
    from itertools import ifilter, ifilterfalse, imap, izip, izip_longest
    from string import maketrans
    string_types = basestring,
    integer_types = int, long
    number_types = int, long, float
    class_types = type, types.ClassType
    text_type = unicode
    binary_type = str
    sortable_types = set([complex, float, int, long, str, unicode])
    from urllib2 import urlopen
    from cStringIO import StringIO
    BytesIO = StringIO
    import cPickle as pickle
    maxint = sys.maxint

try:
    advance_iterator = next
except NameError:
    def advance_iterator(it):
        return it.next()
next = advance_iterator

try:
    callable = callable
except NameError:
    def callable(obj):
        return any("__call__" in klass.__dict__ for klass in type(obj).__mro__)







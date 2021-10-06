from __future__ import absolute_import, print_function, division


import sys



##########################
# Python 3 compatibility #
##########################

PY2 = sys.version_info.major == 2
PY3 = sys.version_info.major == 3

if PY2:
    from itertools import ifilter, ifilterfalse, imap, izip, izip_longest
    from string import maketrans
    from decimal import Decimal
    string_types = basestring,
    integer_types = int, long
    numeric_types = bool, int, long, float, Decimal
    text_type = unicode
    binary_type = str
    from urllib2 import urlopen
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    BytesIO = StringIO
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    maxint = sys.maxint
    long = long
    xrange = xrange
    reduce = reduce

else:
    ifilter = filter
    imap = map
    izip = zip
    xrange = range
    from decimal import Decimal
    from itertools import filterfalse as ifilterfalse
    from itertools import zip_longest as izip_longest
    from functools import reduce
    maketrans = str.maketrans
    string_types = str,
    integer_types = int,
    numeric_types = bool, int, float, Decimal
    class_types = type,
    text_type = str
    binary_type = bytes
    long = int
    from urllib.request import urlopen
    from io import StringIO, BytesIO
    import pickle
    maxint = sys.maxsize

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

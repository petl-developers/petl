"""
The `petl` module.

"""


from __future__ import absolute_import, print_function, division


from itertools import islice
from petl.compat import text_type as _text_type


from petl import comparison
from petl.comparison import Comparable
from petl import util
from petl.util import *
from petl import io
from petl.io import *
from petl import transform
from petl.transform import *
from petl import config
from petl import errors


__version__ = VERSION = '1.0a1.dev0'

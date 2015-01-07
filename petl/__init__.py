"""
The `petl` module.

"""


from __future__ import absolute_import, print_function, division


from itertools import islice


from petl.comparison import Comparable
from petl.util import *
from petl.io import *
from petl.transform import *
from petl import config
from petl.compat import text_type as _text_type


__version__ = VERSION = '1.0a1.dev0'


def _vis_overflow(table, limit):
    overflow = False
    if limit > 0:
        # try reading one more than the limit, to see if there are more rows
        table = list(islice(table, 0, limit+2))
        if len(table) > limit+1:
            overflow = True
            table = table[:-1]
    return table, overflow


def _table_repr(table):
    limit = config.table_repr_limit
    index_header = config.table_repr_index_header
    vrepr = repr
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = str(l)
    if overflow:
        t += '...\n'
    return t


def _table_str(table):
    limit = config.table_str_limit
    index_header = config.table_str_index_header
    vrepr = str
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = str(l)
    if overflow:
        t += '...\n'
    return t


def _table_unicode(table):
    limit = config.table_str_limit
    index_header = config.table_str_index_header
    vrepr = _text_type
    table, overflow = _vis_overflow(table, limit)
    l = look(table, limit, vrepr=vrepr, index_header=index_header)
    t = _text_type(l)
    if overflow:
        t += '...\n'
    return t


def _table_repr_html(table):
    limit = config.table_repr_html_limit
    index_header = config.table_repr_html_index_header
    vrepr = _text_type
    table, overflow = _vis_overflow(table, limit)
    buf = MemorySource()
    encoding = 'utf-8'
    touhtml(table, buf, encoding=encoding, index_header=index_header,
            representation=vrepr)
    s = _text_type(buf.getvalue(), encoding)
    if overflow:
        s += '<p><strong>...</strong></p>'
    return s


Table.__repr__ = _table_repr
Table.__str__ = _table_str
Table.__unicode__ = _table_unicode
Table._repr_html_ = _table_repr_html

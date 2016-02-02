from __future__ import absolute_import, print_function, division


from petl.test.helpers import ieq
from petl import join, leftjoin, rightjoin, outerjoin, crossjoin, antijoin, \
    lookupjoin, hashjoin, hashleftjoin, hashrightjoin, hashantijoin, \
    hashlookupjoin, unjoin, sort, cut


def _test_join_basic(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))

    # normal inner join
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = join_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)
    ieq(expect4, table4)  # check twice

    # multiple rows for each key
    table5 = (('id', 'colour'),
              (1, 'blue'),
              (1, 'red'),
              (2, 'purple'))
    table6 = (('id', 'shape'),
              (1, 'circle'),
              (1, 'square'),
              (2, 'ellipse'))
    table7 = join_impl(table5, table6, key='id')
    expect7 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (1, 'blue', 'square'),
               (1, 'red', 'circle'),
               (1, 'red', 'square'),
               (2, 'purple', 'ellipse'))
    ieq(expect7, table7)


def _test_join_compound_keys(join_impl):

    # compound keys
    table8 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table9 = (('id', 'time', 'weight'),
              (1, 2, 4.5),
              (2, 1, 6.7),
              (2, 2, 8.9))
    table10 = join_impl(table8, table9, key=['id', 'time'])
    expect10 = (('id', 'time', 'height', 'weight'),
                (1, 2, 34.5, 4.5),
                (2, 1, 56.7, 6.7))
    ieq(expect10, table10)

    # natural join on compound key
    table11 = join_impl(table8, table9)
    expect11 = expect10
    ieq(expect11, table11)


def _test_join_string_key(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('id', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    # normal inner join
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def _test_join_empty(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),)
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)

    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = join_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),)
    ieq(expect3, table3)


def _test_join_novaluefield(join_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))

    expect = (('id', 'colour', 'shape'),
              (1, 'blue', 'circle'),
              (3, 'purple', 'square'))

    actual = join_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = join_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = join_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = join_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_join_prefix(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('id', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    table3 = join_impl(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)


def _test_join_lrkey(join_impl):

    table1 = (('id', 'colour'),
              ('aa', 'blue'),
              ('bb', 'red'),
              ('cc', 'purple'))
    table2 = (('identifier', 'shape'),
              ('aa', 'circle'),
              ('cc', 'square'),
              ('dd', 'ellipse'))

    table3 = join_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               ('aa', 'blue', 'circle'),
               ('cc', 'purple', 'square'))
    ieq(expect3, table3)


def _test_join_multiple(join_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = join_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'))
    ieq(expect, actual)


def _test_join(join_impl):
    _test_join_basic(join_impl)
    _test_join_compound_keys(join_impl)
    _test_join_string_key(join_impl)
    _test_join_empty(join_impl)
    _test_join_novaluefield(join_impl)
    _test_join_prefix(join_impl)
    _test_join_lrkey(join_impl)
    _test_join_multiple(join_impl)


def test_join():
    _test_join(join)


def _test_leftjoin_1(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_2(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_3(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = leftjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_leftjoin_compound_keys(leftjoin_impl):

    # compound keys
    table5 = (('id', 'time', 'height'),
              (1, 1, 12.3),
              (1, 2, 34.5),
              (2, 1, 56.7))
    table6 = (('id', 'time', 'weight', 'bp'),
              (1, 2, 4.5, 120),
              (2, 1, 6.7, 110),
              (2, 2, 8.9, 100))
    table7 = leftjoin_impl(table5, table6, key=['id', 'time'])
    expect7 = (('id', 'time', 'height', 'weight', 'bp'),
               (1, 1, 12.3, None, None),
               (1, 2, 34.5, 4.5, 120),
               (2, 1, 56.7, 6.7, 110))
    ieq(expect7, table7)


def _test_leftjoin_empty(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),)
    table3 = leftjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin_novaluefield(leftjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    expect = (('id', 'colour', 'shape'),
              (1, 'blue', 'circle'),
              (2, 'red', None),
              (3, 'purple', 'square'),
              (5, 'yellow', None,),
              (7, 'orange', None))
    
    actual = leftjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = leftjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = leftjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = leftjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_leftjoin_multiple(leftjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = leftjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (4, 'chartreuse', 42, None, None))
    ieq(expect, actual)


def _test_leftjoin_prefix(leftjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin_lrkey(leftjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'orange'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = leftjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (5, 'yellow', None,),
               (7, 'orange', None))
    ieq(expect3, table3)


def _test_leftjoin(leftjoin_impl):
    _test_leftjoin_1(leftjoin_impl)
    _test_leftjoin_2(leftjoin_impl)
    _test_leftjoin_3(leftjoin_impl)
    _test_leftjoin_compound_keys(leftjoin_impl)
    _test_leftjoin_empty(leftjoin_impl)
    _test_leftjoin_novaluefield(leftjoin_impl)
    _test_leftjoin_multiple(leftjoin_impl)
    _test_leftjoin_prefix(leftjoin_impl)
    _test_leftjoin_lrkey(leftjoin_impl)


def test_leftjoin():
    _test_leftjoin(leftjoin)


def _test_rightjoin_1(rightjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_2(rightjoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_3(rightjoin_impl):

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (4, 'orange'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (5, 'ellipse'),
              (7, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (5, None, 'ellipse'),
               (7, None, 'pentagon'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = rightjoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_rightjoin_empty(rightjoin_impl):

    table1 = (('id', 'colour'),)
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, None, 'circle'),
               (3, None, 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_novaluefield(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    expect = (('id', 'colour', 'shape'),
              (0, None, 'triangle'),
              (1, 'blue', 'circle'),
              (3, 'purple', 'square'),
              (4, None, 'ellipse'),
              (5, None, 'pentagon'))

    actual = rightjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = rightjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = rightjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = rightjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_rightjoin_prefix(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, key='id', lprefix='l_',
                            rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_lrkey(rightjoin_impl):
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('identifier', 'shape'),
              (0, 'triangle'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'pentagon'))
    table3 = rightjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'triangle'),
               (1, 'blue', 'circle'),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'pentagon'))
    ieq(expect3, table3)


def _test_rightjoin_multiple(rightjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = rightjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (5, None, None, 'didodecahedron', 3.14159265))

    # N.B., need to sort because hash and sort implementations will return
    # rows in a different order
    ieq(sort(expect), sort(actual))


def _test_rightjoin(rightjoin_impl):
    _test_rightjoin_1(rightjoin_impl)
    _test_rightjoin_2(rightjoin_impl)
    _test_rightjoin_3(rightjoin_impl)
    _test_rightjoin_empty(rightjoin_impl)
    _test_rightjoin_novaluefield(rightjoin_impl)
    _test_rightjoin_prefix(rightjoin_impl)
    _test_rightjoin_lrkey(rightjoin_impl)
    _test_rightjoin_multiple(rightjoin_impl)


def test_rightjoin():
    _test_rightjoin(rightjoin)


def test_outerjoin():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def test_outerjoin_2():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'))
    table2 = (('id', 'shape'),
              (0, 'pentagon'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'),
              (5, 'triangle'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, None, 'pentagon'),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, None, 'triangle'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice

    # natural join
    table4 = outerjoin(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def test_outerjoin_fieldorder():

    table1 = (('colour', 'id'),
              ('blue', 1),
              ('red', 2),
              ('purple', 3))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('colour', 'id', 'shape'),
               ('blue', 1, 'circle'),
               ('red', 2, None),
               ('purple', 3, 'square'),
               (None, 4, 'ellipse'))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_empty():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),)
    table3 = outerjoin(table1, table2, key='id')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', None),
               (2, 'red', None),
               (3, 'purple', None),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)


def test_outerjoin_novaluefield():
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    expect = (('id', 'colour', 'shape'),
              (0, 'black', None),
              (1, 'blue', 'circle'),
              (2, 'red', None),
              (3, 'purple', 'square'),
              (4, None, 'ellipse'),
              (5, 'yellow', None),
              (7, 'white', None))
    actual = outerjoin(table1, table2, key='id')
    ieq(expect, actual)
    actual = outerjoin(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape'), actual)
    actual = outerjoin(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'colour'), actual)
    actual = outerjoin(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def test_outerjoin_prefix():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, key='id', lprefix='l_', rprefix='r_')
    expect3 = (('l_id', 'l_colour', 'r_shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_lrkey():

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (3, 'purple'),
              (5, 'yellow'),
              (7, 'white'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'),
              (4, 'ellipse'))
    table3 = outerjoin(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour', 'shape'),
               (0, 'black', None),
               (1, 'blue', 'circle'),
               (2, 'red', None),
               (3, 'purple', 'square'),
               (4, None, 'ellipse'),
               (5, 'yellow', None),
               (7, 'white', None))
    ieq(expect3, table3)
    ieq(expect3, table3)  # check twice


def test_outerjoin_multiple():

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (1, 'red', 8),
              (2, 'yellow', 15),
              (2, 'orange', 5),
              (3, 'purple', 4),
              (4, 'chartreuse', 42))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'),
              (5, 'didodecahedron', 3.14159265))

    actual = outerjoin(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (1, 'red', 8, 'circle', 'big'),
              (2, 'yellow', 15, 'square', 'tiny'),
              (2, 'yellow', 15, 'square', 'big'),
              (2, 'orange', 5, 'square', 'tiny'),
              (2, 'orange', 5, 'square', 'big'),
              (3, 'purple', 4, 'ellipse', 'small'),
              (3, 'purple', 4, 'ellipse', 'tiny'),
              (4, 'chartreuse', 42, None, None),
              (5, None, None, 'didodecahedron', 3.14159265))

    ieq(expect, actual)


def test_crossjoin():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),
               (1, 'blue', 1, 'circle'),
               (1, 'blue', 3, 'square'),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, 'square'))
    ieq(expect3, table3)


def test_crossjoin_empty():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),)
    table3 = crossjoin(table1, table2)
    expect3 = (('id', 'colour', 'id', 'shape'),)
    ieq(expect3, table3)


def test_crossjoin_novaluefield():
    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    expect = (('id', 'colour', 'id', 'shape'),
              (1, 'blue', 1, 'circle'),
              (1, 'blue', 3, 'square'),
              (2, 'red', 1, 'circle'),
              (2, 'red', 3, 'square'))
    actual = crossjoin(table1, table2, key='id')
    ieq(expect, actual)
    actual = crossjoin(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 0, 2, 'shape'), actual)
    actual = crossjoin(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 0, 'colour', 2), actual)
    actual = crossjoin(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 0, 2), actual)


def test_crossjoin_prefix():

    table1 = (('id', 'colour'),
              (1, 'blue'),
              (2, 'red'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = crossjoin(table1, table2, prefix=True)
    expect3 = (('1_id', '1_colour', '2_id', '2_shape'),
               (1, 'blue', 1, 'circle'),
               (1, 'blue', 3, 'square'),
               (2, 'red', 1, 'circle'),
               (2, 'red', 3, 'square'))
    ieq(expect3, table3)


def _test_antijoin_basics(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = antijoin_impl(table1, table2, key='id')
    expect3 = (('id', 'colour'),
               (0, 'black'),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    ieq(expect3, table3)

    table4 = antijoin_impl(table1, table2)
    expect4 = expect3
    ieq(expect4, table4)


def _test_antijoin_empty(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),)
    actual = antijoin_impl(table1, table2, key='id')
    expect = table1
    ieq(expect, actual)


def _test_antijoin_novaluefield(antijoin_impl):
    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('id', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    expect = (('id', 'colour'),
              (0, 'black'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    actual = antijoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = antijoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id'), actual)
    actual = antijoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(expect, actual)
    actual = antijoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_antijoin_lrkey(antijoin_impl):

    table1 = (('id', 'colour'),
              (0, 'black'),
              (1, 'blue'),
              (2, 'red'),
              (4, 'yellow'),
              (5, 'white'))
    table2 = (('identifier', 'shape'),
              (1, 'circle'),
              (3, 'square'))
    table3 = antijoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect3 = (('id', 'colour'),
               (0, 'black'),
               (2, 'red'),
               (4, 'yellow'),
               (5, 'white'))
    ieq(expect3, table3)


def _test_antijoin(antijoin_impl):
    _test_antijoin_basics(antijoin_impl)
    _test_antijoin_empty(antijoin_impl)
    _test_antijoin_novaluefield(antijoin_impl)
    _test_antijoin_lrkey(antijoin_impl)


def test_antijoin():
    _test_antijoin(antijoin)


def _test_lookupjoin_1(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)

    # natural join
    actual = lookupjoin_impl(table1, table2)
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin_2(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (1, 'circle', 'small'),
              (2, 'square', 'tiny'),
              (2, 'square', 'big'),
              (3, 'ellipse', 'small'),
              (3, 'ellipse', 'tiny'))

    actual = lookupjoin_impl(table1, table2, key='id')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)
    ieq(expect, actual)


def _test_lookupjoin_prefix(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, key='id', lprefix='l_',
                             rprefix='r_')
    expect = (('l_id', 'l_color', 'l_cost', 'r_shape', 'r_size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)


def _test_lookupjoin_lrkey(lookupjoin_impl):

    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))

    table2 = (('identifier', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))

    actual = lookupjoin_impl(table1, table2, lkey='id', rkey='identifier')
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    ieq(expect, actual)


def _test_lookupjoin_novaluefield(lookupjoin_impl):
    table1 = (('id', 'color', 'cost'),
              (1, 'blue', 12),
              (2, 'red', 8),
              (3, 'purple', 4))
    table2 = (('id', 'shape', 'size'),
              (1, 'circle', 'big'),
              (2, 'square', 'tiny'),
              (3, 'ellipse', 'small'))
    expect = (('id', 'color', 'cost', 'shape', 'size'),
              (1, 'blue', 12, 'circle', 'big'),
              (2, 'red', 8, 'square', 'tiny'),
              (3, 'purple', 4, 'ellipse', 'small'))
    actual = lookupjoin_impl(table1, table2, key='id')
    ieq(expect, actual)
    actual = lookupjoin_impl(cut(table1, 'id'), table2, key='id')
    ieq(cut(expect, 'id', 'shape', 'size'), actual)
    actual = lookupjoin_impl(table1, cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id', 'color', 'cost'), actual)
    actual = lookupjoin_impl(cut(table1, 'id'), cut(table2, 'id'), key='id')
    ieq(cut(expect, 'id'), actual)


def _test_lookupjoin(lookupjoin_impl):
    _test_lookupjoin_1(lookupjoin_impl)
    _test_lookupjoin_2(lookupjoin_impl)
    _test_lookupjoin_prefix(lookupjoin_impl)
    _test_lookupjoin_lrkey(lookupjoin_impl)
    _test_lookupjoin_novaluefield(lookupjoin_impl)


def test_lookupjoin():
    _test_lookupjoin(lookupjoin)


def test_hashjoin():
    _test_join(hashjoin)


def test_hashleftjoin():
    _test_leftjoin(hashleftjoin)


def test_hashrightjoin():
    _test_rightjoin(hashrightjoin)


def test_hashantijoin():
    _test_antijoin(hashantijoin)


def test_hashlookupjoin():
    _test_lookupjoin(hashlookupjoin)


def test_unjoin_implicit_key():

    # test the case where the join key needs to be reconstructed
        
    table1 = (('foo', 'bar'),
              (1, 'apple'),
              (2, 'apple'),
              (3, 'orange'))
    
    expect_left = (('foo', 'bar_id'),
                   (1, 1),
                   (2, 1),
                   (3, 2))
    expect_right = (('id', 'bar'),
                    (1, 'apple'),
                    (2, 'orange'))
    
    left, right = unjoin(table1, 'bar')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)

    
def test_unjoin_explicit_key():

    # test the case where the join key is still present
    
    table2 = (('Customer ID', 'First Name', 'Surname', 'Telephone Number'),
              (123, 'Robert', 'Ingram', '555-861-2025'),
              (456, 'Jane', 'Wright', '555-403-1659'),
              (456, 'Jane', 'Wright', '555-776-4100'),
              (789, 'Maria', 'Fernandez', '555-808-9633'))
    
    expect_left = (('Customer ID', 'First Name', 'Surname'),
                   (123, 'Robert', 'Ingram'),
                   (456, 'Jane', 'Wright'),
                   (789, 'Maria', 'Fernandez'))
    expect_right = (('Customer ID', 'Telephone Number'),
                    (123, '555-861-2025'),
                    (456, '555-403-1659'),
                    (456, '555-776-4100'),
                    (789, '555-808-9633'))
    left, right = unjoin(table2, 'Telephone Number', key='Customer ID')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_2():
    
    table3 = (('Employee', 'Skill', 'Current Work Location'),
              ('Jones', 'Typing', '114 Main Street'),
              ('Jones', 'Shorthand', '114 Main Street'),
              ('Jones', 'Whittling', '114 Main Street'),
              ('Bravo', 'Light Cleaning', '73 Industrial Way'),
              ('Ellis', 'Alchemy', '73 Industrial Way'),
              ('Ellis', 'Flying', '73 Industrial Way'),
              ('Harrison', 'Light Cleaning', '73 Industrial Way'))
    # N.B., we do expect rows will get sorted
    expect_left = (('Employee', 'Current Work Location'),
                   ('Bravo', '73 Industrial Way'),
                   ('Ellis', '73 Industrial Way'),
                   ('Harrison', '73 Industrial Way'),
                   ('Jones', '114 Main Street'))
    expect_right = (('Employee', 'Skill'),
                    ('Bravo', 'Light Cleaning'),
                    ('Ellis', 'Alchemy'),
                    ('Ellis', 'Flying'),
                    ('Harrison', 'Light Cleaning'),
                    ('Jones', 'Shorthand'),
                    ('Jones', 'Typing'),
                    ('Jones', 'Whittling'))
    left, right = unjoin(table3, 'Skill', key='Employee')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_3():
    
    table4 = (('Tournament', 'Year', 'Winner', 'Date of Birth'),
              ('Indiana Invitational', 1998, 'Al Fredrickson', '21 July 1975'),
              ('Cleveland Open', 1999, 'Bob Albertson', '28 September 1968'),
              ('Des Moines Masters', 1999, 'Al Fredrickson', '21 July 1975'),
              ('Indiana Invitational', 1999, 'Chip Masterson', '14 March 1977'))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Tournament', 'Year', 'Winner'),
                   ('Cleveland Open', 1999, 'Bob Albertson'),
                   ('Des Moines Masters', 1999, 'Al Fredrickson'),
                   ('Indiana Invitational', 1998, 'Al Fredrickson'),
                   ('Indiana Invitational', 1999, 'Chip Masterson'))    
    expect_right = (('Winner', 'Date of Birth'),
                    ('Al Fredrickson', '21 July 1975'),
                    ('Bob Albertson', '28 September 1968'),
                    ('Chip Masterson', '14 March 1977'))
    left, right = unjoin(table4, 'Date of Birth', key='Winner')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_4():
    
    table5 = (('Restaurant', 'Pizza Variety', 'Delivery Area'),
              ('A1 Pizza', 'Thick Crust', 'Springfield'),
              ('A1 Pizza', 'Thick Crust', 'Shelbyville'),
              ('A1 Pizza', 'Thick Crust', 'Capital City'),
              ('A1 Pizza', 'Stuffed Crust', 'Springfield'),
              ('A1 Pizza', 'Stuffed Crust', 'Shelbyville'),
              ('A1 Pizza', 'Stuffed Crust', 'Capital City'),
              ('Elite Pizza', 'Thin Crust', 'Capital City'),
              ('Elite Pizza', 'Stuffed Crust', 'Capital City'),
              ("Vincenzo's Pizza", "Thick Crust", "Springfield"),
              ("Vincenzo's Pizza", "Thick Crust", "Shelbyville"),
              ("Vincenzo's Pizza", "Thin Crust", "Springfield"),
              ("Vincenzo's Pizza", "Thin Crust", "Shelbyville"))
    
    # N.B., we do expect rows will get sorted
    expect_left = (('Restaurant', 'Pizza Variety'),
                   ('A1 Pizza', 'Stuffed Crust'),
                   ('A1 Pizza', 'Thick Crust'),
                   ('Elite Pizza', 'Stuffed Crust'),
                   ('Elite Pizza', 'Thin Crust'),
                   ("Vincenzo's Pizza", "Thick Crust"),
                   ("Vincenzo's Pizza", "Thin Crust"))
    expect_right = (('Restaurant', 'Delivery Area'),
                    ('A1 Pizza', 'Capital City'),
                    ('A1 Pizza', 'Shelbyville'),
                    ('A1 Pizza', 'Springfield'),
                    ('Elite Pizza', 'Capital City'),
                    ("Vincenzo's Pizza", "Shelbyville"),
                    ("Vincenzo's Pizza", "Springfield"))
    left, right = unjoin(table5, 'Delivery Area', key='Restaurant')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)


def test_unjoin_explicit_key_5():
    
    table6 = (('ColA', 'ColB', 'ColC'),
              ('A', 1, 'apple'),
              ('B', 1, 'apple'),
              ('C', 2, 'orange'),
              ('D', 3, 'lemon'),
              ('E', 3, 'lemon'))

    # N.B., we do expect rows will get sorted
    expect_left = (('ColA', 'ColB'),
                   ('A', 1),
                   ('B', 1),
                   ('C', 2),
                   ('D', 3),
                   ('E', 3))
    expect_right = (('ColB', 'ColC'),
                    (1, 'apple'),
                    (2, 'orange'),
                    (3, 'lemon'))
    left, right = unjoin(table6, 'ColC', key='ColB')
    ieq(expect_left, left)
    ieq(expect_left, left)
    ieq(expect_right, right)
    ieq(expect_right, right)

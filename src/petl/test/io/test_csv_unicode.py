# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division, unicode_literals


import codecs
from nose.tools import eq_


from petl.testutils import ieq
from petl.io.csv import fromucsv, toucsv, appenducsv


def test_fromucsv():

    data = '''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_fromucsv.csv', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    actual = fromucsv('tmp/test_fromucsv.csv')
    expect = (('name', 'id'),
              ('Արամ Խաչատրյան', '1'),
              ('Johann Strauß', '2'),
              ('Вагиф Сәмәдоғлу', '3'),
              ('章子怡', '4'),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromucsv_lineterminators():
    data = ('name,id',
            'Արամ Խաչատրյան,1',
            'Johann Strauß,2',
            'Вагиф Сәмәдоғлу,3',
            '章子怡,4')
    expect = (('name', 'id'),
              ('Արամ Խաչատրյան', '1'),
              ('Johann Strauß', '2'),
              ('Вагиф Сәмәдоғлу', '3'),
              ('章子怡', '4'))

    for lt in '\r', '\n', '\r\n':
        f = codecs.open('tmp/test_fromucsv.csv', encoding='utf-8', mode='w')
        f.write(lt.join(data))
        f.close()
        actual = fromucsv('tmp/test_fromucsv.csv')
        ieq(expect, actual)


def test_toucsv():

    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    toucsv(tbl, 'tmp/test_toucsv.csv', lineterminator='\n')

    expect = '''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_toucsv.csv', encoding='utf-8', mode='r')
    actual = f.read()
    eq_(expect, actual)

    # Test with write_header=False
    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    toucsv(tbl, 'tmp/test_toucsv.csv', lineterminator='\n', write_header=False)

    expect = '''Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_toucsv.csv', encoding='utf-8', mode='r')
    actual = f.read()
    eq_(expect, actual)


def test_appenducsv():

    data = '''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_appenducsv.csv', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    tbl = (('name', 'id'),
           ('ኃይሌ ገብረሥላሴ', 5),
           ('ედუარდ შევარდნაძე', 6),
           )
    appenducsv(tbl, 'tmp/test_appenducsv.csv', lineterminator='\n')

    expect = '''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
ኃይሌ ገብረሥላሴ,5
ედუარდ შევარდნაძე,6
'''
    f = codecs.open('tmp/test_appenducsv.csv', encoding='utf-8', mode='r')
    actual = f.read()
    eq_(expect, actual)

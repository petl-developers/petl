# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


import codecs
from nose.tools import eq_


from petl.testutils import ieq
from petl.io.csv import fromucsv, toucsv, appenducsv


def test_fromucsv():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_fromucsv.csv', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    actual = fromucsv('tmp/test_fromucsv.csv')
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', u'1'),
              (u'Johann Strauß', u'2'),
              (u'Вагиф Сәмәдоғлу', u'3'),
              (u'章子怡', u'4'),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_toucsv():

    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    toucsv(tbl, 'tmp/test_toucsv.csv', lineterminator='\n')

    expect = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_toucsv.csv', encoding='utf-8', mode='r')
    actual = f.read()
    eq_(expect, actual)

    # Test with write_header=False
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    toucsv(tbl, 'tmp/test_toucsv.csv', lineterminator='\n', write_header=False)

    expect = u'''Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_toucsv.csv', encoding='utf-8', mode='r')
    actual = f.read()
    eq_(expect, actual)


def test_appenducsv():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_appenducsv.csv', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    tbl = ((u'name', u'id'),
           (u'ኃይሌ ገብረሥላሴ', 5),
           (u'ედუარდ შევარდნაძე', 6),
           )
    appenducsv(tbl, 'tmp/test_appenducsv.csv', lineterminator='\n')

    expect = u'''name,id
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

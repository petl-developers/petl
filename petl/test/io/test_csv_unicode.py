# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


import codecs
from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq, eq_
from petl.io.csv import fromucsv, toucsv, appenducsv


def test_fromucsv():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    fn = NamedTemporaryFile().name
    uf = codecs.open(fn, encoding='utf-8', mode='w')
    uf.write(data)
    uf.close()

    actual = fromucsv(fn)
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', u'1'),
              (u'Johann Strauß', u'2'),
              (u'Вагиф Сәмәдоғлу', u'3'),
              (u'章子怡', u'4'),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromucsv_lineterminators():
    data = (u'name,id',
            u'Արամ Խաչատրյան,1',
            u'Johann Strauß,2',
            u'Вагиф Сәмәдоғлу,3',
            u'章子怡,4')
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', u'1'),
              (u'Johann Strauß', u'2'),
              (u'Вагиф Сәмәдоғлу', u'3'),
              (u'章子怡', u'4'))

    for lt in u'\r', u'\n', u'\r\n':
        fn = NamedTemporaryFile().name
        uf = codecs.open(fn, encoding='utf-8', mode='w')
        uf.write(lt.join(data))
        uf.close()
        actual = fromucsv(fn)
        ieq(expect, actual)


def test_toucsv():

    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    fn = NamedTemporaryFile().name
    toucsv(tbl, fn, lineterminator='\n')

    expect = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    uf = codecs.open(fn, encoding='utf-8', mode='r')
    actual = uf.read()
    eq_(expect, actual)

    # Test with write_header=False
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    toucsv(tbl, fn, lineterminator='\n', write_header=False)

    expect = u'''Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    uf = codecs.open(fn, encoding='utf-8', mode='r')
    actual = uf.read()
    eq_(expect, actual)


def test_appenducsv():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    fn = NamedTemporaryFile().name
    uf = codecs.open(fn, encoding='utf-8', mode='w')
    uf.write(data)
    uf.close()

    tbl = ((u'name', u'id'),
           (u'ኃይሌ ገብረሥላሴ', 5),
           (u'ედუარდ შევარდნაძე', 6),
           )
    appenducsv(tbl, fn, lineterminator='\n')

    expect = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
ኃይሌ ገብረሥላሴ,5
ედუარდ შევარდნაძე,6
'''
    uf = codecs.open(fn, encoding='utf-8', mode='r')
    actual = uf.read()
    eq_(expect, actual)

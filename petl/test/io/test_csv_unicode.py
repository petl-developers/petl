# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import io
from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq, eq_
from petl.io.csv import fromcsv, tocsv, appendcsv


def test_fromcsv():

    data = (
        u"name,id\n"
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    uf = io.open(fn, encoding='utf-8', mode='wt')
    uf.write(data)
    uf.close()

    actual = fromcsv(fn, encoding='utf-8')
    expect = ((u'name', u'id'),
              (u'Արամ Խաչատրյան', u'1'),
              (u'Johann Strauß', u'2'),
              (u'Вагиф Сәмәдоғлу', u'3'),
              (u'章子怡', u'4'))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromcsv_lineterminators():
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
        uf = io.open(fn, encoding='utf-8', mode='wt', newline='')
        uf.write(lt.join(data))
        uf.close()
        actual = fromcsv(fn, encoding='utf-8')
        ieq(expect, actual)


def test_tocsv():

    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4))
    fn = NamedTemporaryFile().name
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        u"name,id\n"
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
    )
    uf = io.open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)

    # Test with write_header=False
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4))
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n', write_header=False)

    expect = (
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
    )
    uf = io.open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)


def test_appendcsv():

    data = (
        u"name,id\n"
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    uf = io.open(fn, encoding='utf-8', mode='wt')
    uf.write(data)
    uf.close()

    tbl = ((u'name', u'id'),
           (u'ኃይሌ ገብረሥላሴ', 5),
           (u'ედუარდ შევარდნაძე', 6))
    appendcsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        u"name,id\n"
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
        u"ኃይሌ ገብረሥላሴ,5\n"
        u"ედუარდ შევარდნაძე,6\n"
    )
    uf = io.open(fn, encoding='utf-8', mode='rt')
    actual = uf.read()
    eq_(expect, actual)


def test_tocsv_none():

    tbl = ((u'col1', u'colNone'),
           (u'a', 1),
           (u'b', None),
           (u'c', None),
           (u'd', 4))
    fn = NamedTemporaryFile().name
    tocsv(tbl, fn, encoding='utf-8', lineterminator='\n')

    expect = (
        u'col1,colNone\n'
        u'a,1\n'
        u'b,\n'
        u'c,\n'
        u'd,4\n'
    )

    uf = io.open(fn, encoding='utf-8', mode='rt', newline='')
    actual = uf.read()
    eq_(expect, actual)

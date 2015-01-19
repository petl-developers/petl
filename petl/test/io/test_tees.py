# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile


from petl.test.helpers import ieq
import petl as etl


def test_teepickle():

    t1 = (('foo', 'bar'),
          ('a', 2),
          ('b', 1),
          ('c', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    etl.wrap(t1).teepickle(f1.name).selectgt('bar', 1).topickle(f2.name)

    ieq(t1, etl.frompickle(f1.name))
    ieq(etl.wrap(t1).selectgt('bar', 1), etl.frompickle(f2.name))


def test_teecsv():

    t1 = (('foo', 'bar'),
          ('a', 2),
          ('b', 1),
          ('c', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .teecsv(f1.name, encoding='ascii')
     .selectgt('bar', 1)
     .tocsv(f2.name, encoding='ascii'))

    ieq(t1,
        etl.fromcsv(f1.name, encoding='ascii').convertnumbers())
    ieq(etl.wrap(t1).selectgt('bar', 1),
        etl.fromcsv(f2.name, encoding='ascii').convertnumbers())


def test_teetsv():

    t1 = (('foo', 'bar'),
          ('a', 2),
          ('b', 1),
          ('c', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .teetsv(f1.name, encoding='ascii')
     .selectgt('bar', 1)
     .totsv(f2.name, encoding='ascii'))

    ieq(t1,
        etl.fromtsv(f1.name, encoding='ascii').convertnumbers())
    ieq(etl.wrap(t1).selectgt('bar', 1),
        etl.fromtsv(f2.name, encoding='ascii').convertnumbers())


def test_teecsv_write_header():

    t1 = (('foo', 'bar'),
          ('a', '2'),
          ('b', '1'),
          ('c', '3'))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .convertnumbers()
     .teecsv(f1.name, write_header=False, encoding='ascii')
     .selectgt('bar', 1)
     .tocsv(f2.name, encoding='ascii'))

    ieq(t1[1:],
        etl.fromcsv(f1.name, encoding='ascii'))
    ieq(etl.wrap(t1).convertnumbers().selectgt('bar', 1),
        etl.fromcsv(f2.name, encoding='ascii').convertnumbers())


def test_teecsv_unicode():

    t1 = ((u'name', u'id'),
          (u'Արամ Խաչատրյան', 1),
          (u'Johann Strauß', 2),
          (u'Вагиф Сәмәдоғлу', 3),
          (u'章子怡', 4))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .teecsv(f1.name, encoding='utf-8')
     .selectgt('id', 1)
     .tocsv(f2.name, encoding='utf-8'))

    ieq(t1,
        etl.fromcsv(f1.name, encoding='utf-8').convertnumbers())
    ieq(etl.wrap(t1).selectgt('id', 1),
        etl.fromcsv(f2.name, encoding='utf-8').convertnumbers())


def test_teecsv_unicode_write_header():

    t1 = ((u'name', u'id'),
          (u'Արամ Խաչատրյան', u'1'),
          (u'Johann Strauß', u'2'),
          (u'Вагиф Сәмәдоғлу', u'3'),
          (u'章子怡', u'4'))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .convertnumbers()
     .teecsv(f1.name, write_header=False, encoding='utf-8')
     .selectgt('id', 1)
     .tocsv(f2.name, encoding='utf-8'))

    ieq(t1[1:],
        etl.fromcsv(f1.name, encoding='utf-8'))
    ieq(etl.wrap(t1).convertnumbers().selectgt('id', 1),
        etl.fromcsv(f2.name, encoding='utf-8').convertnumbers())


def test_teetsv_unicode():

    t1 = ((u'name', u'id'),
          (u'Արամ Խաչատրյան', 1),
          (u'Johann Strauß', 2),
          (u'Вагиф Сәмәдоғлу', 3),
          (u'章子怡', 4),)

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    (etl
     .wrap(t1)
     .teetsv(f1.name, encoding='utf-8')
     .selectgt('id', 1)
     .totsv(f2.name, encoding='utf-8'))

    ieq(t1,
        etl.fromtsv(f1.name, encoding='utf-8').convertnumbers())
    ieq(etl.wrap(t1).selectgt('id', 1),
        etl.fromtsv(f2.name, encoding='utf-8').convertnumbers())


def test_teetext():

    t1 = (('foo', 'bar'),
          ('a', 2),
          ('b', 1),
          ('c', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    prologue = 'foo,bar\n'
    template = '{foo},{bar}\n'
    epilogue = 'd,4'
    (etl
     .wrap(t1)
     .teetext(f1.name,
              template=template,
              prologue=prologue,
              epilogue=epilogue)
     .selectgt('bar', 1)
     .topickle(f2.name))

    ieq(t1 + (('d', 4),),
        etl.fromcsv(f1.name).convertnumbers())
    ieq(etl.wrap(t1).selectgt('bar', 1),
        etl.frompickle(f2.name))


def test_teetext_unicode():

    t1 = ((u'foo', u'bar'),
          (u'Արամ Խաչատրյան', 2),
          (u'Johann Strauß', 1),
          (u'Вагиф Сәмәдоғлу', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)

    prologue = u'foo,bar\n'
    template = u'{foo},{bar}\n'
    epilogue = u'章子怡,4'
    (etl
     .wrap(t1)
     .teetext(f1.name,
              template=template,
              prologue=prologue,
              epilogue=epilogue,
              encoding='utf-8')
     .selectgt('bar', 1)
     .topickle(f2.name))

    ieq(t1 + ((u'章子怡', 4),),
        etl.fromcsv(f1.name, encoding='utf-8').convertnumbers())
    ieq(etl.wrap(t1).selectgt('bar', 1),
        etl.frompickle(f2.name))


def test_teehtml():

    t1 = (('foo', 'bar'),
          ('a', 2),
          ('b', 1),
          ('c', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    etl.wrap(t1).teehtml(f1.name).selectgt('bar', 1).topickle(f2.name)

    ieq(t1, etl.fromxml(f1.name, './/tr', ('th', 'td')).convertnumbers())
    ieq(etl.wrap(t1).selectgt('bar', 1), etl.frompickle(f2.name))


def test_teehtml_unicode():

    t1 = ((u'foo', u'bar'),
          (u'Արամ Խաչատրյան', 2),
          (u'Johann Strauß', 1),
          (u'Вагиф Сәмәдоғлу', 3))

    f1 = NamedTemporaryFile(delete=False)
    f2 = NamedTemporaryFile(delete=False)
    (etl
     .wrap(t1)
     .teehtml(f1.name, encoding='utf-8')
     .selectgt('bar', 1)
     .topickle(f2.name))

    ieq(t1,
        (etl
         .fromxml(f1.name, './/tr', ('th', 'td'), encoding='utf-8')
         .convertnumbers()))
    ieq(etl.wrap(t1).selectgt('bar', 1), etl.frompickle(f2.name))

# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import io
from tempfile import NamedTemporaryFile
from petl.test.helpers import ieq, eq_


from petl.io.text import fromtext, totext


def test_fromtext():
    data = (
        u"name,id\n"
        u"Արամ Խաչատրյան,1\n"
        u"Johann Strauß,2\n"
        u"Вагиф Сәмәдоғлу,3\n"
        u"章子怡,4\n"
    )
    fn = NamedTemporaryFile().name
    f = io.open(fn, encoding='utf-8', mode='wt')
    f.write(data)
    f.close()

    actual = fromtext(fn, encoding='utf-8')
    expect = ((u'lines',),
              (u'name,id',),
              (u'Արամ Խաչատրյան,1',),
              (u'Johann Strauß,2',),
              (u'Вагиф Сәмәдоғлу,3',),
              (u'章子怡,4',),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_totext():

    # exercise function
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    prologue = (
        u"{| class='wikitable'\n"
        u"|-\n"
        u"! name\n"
        u"! id\n"
    )
    template = (
        u"|-\n"
        u"| {name}\n"
        u"| {id}\n"
    )
    epilogue = u"|}\n"
    fn = NamedTemporaryFile().name
    totext(tbl, fn, template=template, prologue=prologue,
           epilogue=epilogue, encoding='utf-8')

    # check what it did
    f = io.open(fn, encoding='utf-8', mode='rt')
    actual = f.read()
    expect = (
        u"{| class='wikitable'\n"
        u"|-\n"
        u"! name\n"
        u"! id\n"
        u"|-\n"
        u"| Արամ Խաչատրյան\n"
        u"| 1\n"
        u"|-\n"
        u"| Johann Strauß\n"
        u"| 2\n"
        u"|-\n"
        u"| Вагиф Сәмәдоғлу\n"
        u"| 3\n"
        u"|-\n"
        u"| 章子怡\n"
        u"| 4\n"
        u"|}\n"
    )
    eq_(expect, actual)

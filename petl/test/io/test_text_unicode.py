# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import io
from tempfile import NamedTemporaryFile
from petl.test.helpers import ieq, eq_


from petl.io.text import fromtext, totext


def test_fromtext():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    fn = NamedTemporaryFile().name
    f = io.open(fn, encoding='utf-8', mode='wt')
    f.write(data)
    f.close()

    actual = fromtext(fn)
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
    prologue = u"""{| class="wikitable"
|-
! name
! id
"""
    template = u"""|-
| {name}
| {id}
"""
    epilogue = u"|}"
    fn = NamedTemporaryFile().name
    totext(tbl, fn, template=template, prologue=prologue,
           epilogue=epilogue)

    # check what it did
    f = io.open(fn, encoding='utf-8', mode='rt')
    actual = f.read()
    expect = u"""{| class="wikitable"
|-
! name
! id
|-
| Արամ Խաչատրյան
| 1
|-
| Johann Strauß
| 2
|-
| Вагиф Сәмәдоғлу
| 3
|-
| 章子怡
| 4
|}"""
    eq_(expect, actual)

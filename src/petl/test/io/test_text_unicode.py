# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division
# N.B., do not import unicode_literals in tests


import codecs
from petl.testutils import ieq, eq_


from petl.io.text import fromutext, toutext


def test_fromutext():

    data = u'''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_fromutext.txt', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    actual = fromutext('tmp/test_fromutext.txt')
    expect = ((u'lines',),
              (u'name,id',),
              (u'Արամ Խաչատրյան,1',),
              (u'Johann Strauß,2',),
              (u'Вагиф Сәмәдоғлу,3',),
              (u'章子怡,4',),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_toutext():

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
    toutext(tbl, 'tmp/test_toutext.txt', template=template, prologue=prologue,
            epilogue=epilogue)

    # check what it did
    f = codecs.open('tmp/test_toutext.txt', encoding='utf-8', mode='r')
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

# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division, unicode_literals


import codecs
from nose.tools import eq_
from petl.testutils import ieq


from petl.io.text import fromutext, toutext


def test_fromutext():

    data = '''name,id
Արամ Խաչատրյան,1
Johann Strauß,2
Вагиф Сәмәдоғлу,3
章子怡,4
'''
    f = codecs.open('tmp/test_fromutext.txt', encoding='utf-8', mode='w')
    f.write(data)
    f.close()

    actual = fromutext('tmp/test_fromutext.txt')
    expect = (('lines',),
              ('name,id',),
              ('Արամ Խաչատրյան,1',),
              ('Johann Strauß,2',),
              ('Вагиф Сәмәдоғлу,3',),
              ('章子怡,4',),
              )
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_toutext():

    # exercise function
    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    prologue = """{| class="wikitable"
|-
! name
! id
"""
    template = """|-
| {name}
| {id}
"""
    epilogue = "|}"
    toutext(tbl, 'tmp/test_toutext.txt', template=template, prologue=prologue, epilogue=epilogue)

    # check what it did
    f = codecs.open('tmp/test_toutext.txt', encoding='utf-8', mode='r')
    actual = f.read()
    expect = """{| class="wikitable"
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

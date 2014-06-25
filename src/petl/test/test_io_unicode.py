# -*- coding: utf-8 -*-


"""
Tests for the petl.io module unicode support.

"""


import codecs
import json
from nose.tools import eq_
from petl.testutils import ieq


from petl import fromucsv, toucsv, appenducsv, fromutext, toutext, touhtml, tojson, fromjson


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


def test_touhtml():

    # exercise function
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    touhtml(tbl, 'tmp/test_touhtml.html', lineterminator='\n')

    # check what it did
    f = codecs.open('tmp/test_touhtml.html', mode='r', encoding='utf-8')
    actual = f.read()
    expect = u"""<table class='petl'>
<thead>
<tr>
<th>name</th>
<th>id</th>
</tr>
</thead>
<tbody>
<tr>
<td>Արամ Խաչատրյան</td>
<td style='text-align: right'>1</td>
</tr>
<tr>
<td>Johann Strauß</td>
<td style='text-align: right'>2</td>
</tr>
<tr>
<td>Вагиф Сәмәдоғлу</td>
<td style='text-align: right'>3</td>
</tr>
<tr>
<td>章子怡</td>
<td style='text-align: right'>4</td>
</tr>
</tbody>
</table>
"""
    eq_(expect, actual)


def test_json_unicode():

    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4),
           )
    tojson(tbl, 'tmp/test_tojson_utf8.json')

    result = json.load(open('tmp/test_tojson_utf8.json'))
    assert len(result) == 4
    for a, b in zip(tbl[1:], result):
        assert a[0] == b['name']
        assert a[1] == b['id']

    actual = fromjson('tmp/test_tojson_utf8.json')
    ieq(tbl, actual)






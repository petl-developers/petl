# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function, division, \
    unicode_literals


import codecs
from nose.tools import eq_


from petl.io.html import touhtml


def test_touhtml():

    # exercise function
    tbl = (('name', 'id'),
           ('Արամ Խաչատրյան', 1),
           ('Johann Strauß', 2),
           ('Вагиф Сәмәдоғлу', 3),
           ('章子怡', 4),
           )
    touhtml(tbl, 'tmp/test_touhtml.html', lineterminator='\n')

    # check what it did
    f = codecs.open('tmp/test_touhtml.html', mode='r', encoding='utf-8')
    actual = f.read()
    expect = """<table class='petl'>
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

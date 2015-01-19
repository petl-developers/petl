# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


import io
from tempfile import NamedTemporaryFile
from petl.test.helpers import eq_


from petl.io.html import tohtml


def test_tohtml():

    # exercise function
    tbl = ((u'name', u'id'),
           (u'Արամ Խաչատրյան', 1),
           (u'Johann Strauß', 2),
           (u'Вагиф Сәмәдоғлу', 3),
           (u'章子怡', 4))
    fn = NamedTemporaryFile().name
    tohtml(tbl, fn, encoding='utf-8', lineterminator='\n')

    # check what it did
    f = io.open(fn, mode='rt', encoding='utf-8', newline='')
    actual = f.read()
    expect = (
        u"<table class='petl'>\n"
        u"<thead>\n"
        u"<tr>\n"
        u"<th>name</th>\n"
        u"<th>id</th>\n"
        u"</tr>\n"
        u"</thead>\n"
        u"<tbody>\n"
        u"<tr>\n"
        u"<td>Արամ Խաչատրյան</td>\n"
        u"<td style='text-align: right'>1</td>\n"
        u"</tr>\n"
        u"<tr>\n"
        u"<td>Johann Strauß</td>\n"
        u"<td style='text-align: right'>2</td>\n"
        u"</tr>\n"
        u"<tr>\n"
        u"<td>Вагиф Сәмәдоғлу</td>\n"
        u"<td style='text-align: right'>3</td>\n"
        u"</tr>\n"
        u"<tr>\n"
        u"<td>章子怡</td>\n"
        u"<td style='text-align: right'>4</td>\n"
        u"</tr>\n"
        u"</tbody>\n"
        u"</table>\n"
    )
    eq_(expect, actual)

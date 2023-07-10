# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import io
from petl.test.helpers import eq_


from petl.io.html import tohtml


def test_tohtml():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)),
             ('c', False))

    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = (
            u"<table class='petl'>\n"
            u"<thead>\n"
            u"<tr>\n"
            u"<th>foo</th>\n"
            u"<th>bar</th>\n"
            u"</tr>\n"
            u"</thead>\n"
            u"<tbody>\n"
            u"<tr>\n"
            u"<td>a</td>\n"
            u"<td style='text-align: right'>1</td>\n"
            u"</tr>\n"
            u"<tr>\n"
            u"<td>b</td>\n"
            u"<td>(1, 2)</td>\n"
            u"</tr>\n"
            u"<tr>\n"
            u"<td>c</td>\n"
            u"<td>False</td>\n"
            u"</tr>\n"
            u"</tbody>\n"
            u"</table>\n"
        )
        eq_(expect, actual)


def test_tohtml_caption():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)))
    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', caption='my table',
           lineterminator='\n')

    # check what it did
    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = (
            u"<table class='petl'>\n"
            u"<caption>my table</caption>\n"
            u"<thead>\n"
            u"<tr>\n"
            u"<th>foo</th>\n"
            u"<th>bar</th>\n"
            u"</tr>\n"
            u"</thead>\n"
            u"<tbody>\n"
            u"<tr>\n"
            u"<td>a</td>\n"
            u"<td style='text-align: right'>1</td>\n"
            u"</tr>\n"
            u"<tr>\n"
            u"<td>b</td>\n"
            u"<td>(1, 2)</td>\n"
            u"</tr>\n"
            u"</tbody>\n"
            u"</table>\n"
        )
        eq_(expect, actual)


def test_tohtml_with_style():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1))

    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', lineterminator='\n',
        tr_style='text-align: right', td_styles='text-align: center')

    # check what it did
    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = (
            u"<table class='petl'>\n"
            u"<thead>\n"
            u"<tr>\n"
            u"<th>foo</th>\n"
            u"<th>bar</th>\n"
            u"</tr>\n"
            u"</thead>\n"
            u"<tbody>\n"
            u"<tr style='text-align: right'>\n"
            u"<td style='text-align: center'>a</td>\n"
            u"<td style='text-align: center'>1</td>\n"
            u"</tr>\n"
            u"</tbody>\n"
            u"</table>\n"
        )
        eq_(expect, actual)


def test_tohtml_headerless():
    table = []

    f = NamedTemporaryFile(delete=False)
    tohtml(table, f.name, encoding='ascii', lineterminator='\n')

    # check what it did
    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = (
            u"<table class='petl'>\n"
            u"<tbody>\n"
            u"</tbody>\n"
            u"</table>\n"
        )
        eq_(expect, actual)

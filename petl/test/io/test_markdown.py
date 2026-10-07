# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import io
from petl.test.helpers import eq_


from petl.io.markdown import tomarkdown
from petl.io.sources import MemorySource


def _read(fn, encoding='ascii'):
    with io.open(fn, mode='rt', encoding=encoding, newline='') as o:
        return o.read()


def test_tomarkdown():

    table = (('foo', 'bar'),
             ('a', 1),
             ('b', (1, 2)),
             ('c', False))

    f = NamedTemporaryFile(delete=False)
    tomarkdown(table, f.name, encoding='ascii')
    expect = (
        u"| foo | bar |\n"
        u"| --- | --- |\n"
        u"| a | 1 |\n"
        u"| b | (1, 2) |\n"
        u"| c | False |\n"
    )
    eq_(expect, _read(f.name))


def test_tomarkdown_escape():

    table = (('foo|bar', 'baz'),
             ('a|b', 'line1\nline2'),
             ('c', 'line1\r\nline2\rline3'))

    f = NamedTemporaryFile(delete=False)
    tomarkdown(table, f.name, encoding='ascii')
    expect = (
        u"| foo\\|bar | baz |\n"
        u"| --- | --- |\n"
        u"| a\\|b | line1<br>line2 |\n"
        u"| c | line1<br>line2<br>line3 |\n"
    )
    eq_(expect, _read(f.name))


def test_tomarkdown_irregular_rows():

    table = (('foo', 'bar'),
             ('a',),
             ('b', 2, True))

    f = NamedTemporaryFile(delete=False)
    tomarkdown(table, f.name, encoding='ascii')
    expect = (
        u"| foo | bar |\n"
        u"| --- | --- |\n"
        u"| a | None |\n"
        u"| b | 2 | True |\n"
    )
    eq_(expect, _read(f.name))


def test_tomarkdown_vrepr_lineterminator():

    table = (('foo', 'bar'),
             ('a', None),
             ('b', 2))

    f = NamedTemporaryFile(delete=False)
    tomarkdown(table, f.name, encoding='ascii', lineterminator='\r\n',
               vrepr=lambda v: u'' if v is None else u'%r' % (v,))
    expect = (
        u"| foo | bar |\r\n"
        u"| --- | --- |\r\n"
        u"| 'a' |  |\r\n"
        u"| 'b' | 2 |\r\n"
    )
    eq_(expect, _read(f.name))


def test_tomarkdown_unicode():

    table = ((u'name', u'id'),
             (u'Արամ Խաչատրյան', 1),
             (u'Johann Strauß', 2))

    f = NamedTemporaryFile(delete=False)
    tomarkdown(table, f.name, encoding='utf-8')
    expect = (
        u"| name | id |\n"
        u"| --- | --- |\n"
        u"| Արամ Խաչատրյան | 1 |\n"
        u"| Johann Strauß | 2 |\n"
    )
    eq_(expect, _read(f.name, encoding='utf-8'))


def test_tomarkdown_empty():

    buf = MemorySource()
    tomarkdown([], buf)
    eq_(b'', buf.getvalue())

    buf = MemorySource()
    tomarkdown([('foo', 'bar')], buf)
    eq_(b'| foo | bar |\n| --- | --- |\n', buf.getvalue())


def test_tomarkdown_fluent():

    import petl as etl
    buf = MemorySource()
    etl.wrap([('foo',), ('a',)]).tomarkdown(buf)
    eq_(b'| foo |\n| --- |\n| a |\n', buf.getvalue())

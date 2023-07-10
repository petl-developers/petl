# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import gzip
import os
import io


from petl.test.helpers import ieq, eq_
from petl.io.text import fromtext, totext


def test_fromtext():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wb')
    f.write(b'foo\tbar\n')
    f.write(b'a\t1\n')
    f.write(b'b\t2\n')
    f.write(b'c\t3\n')
    f.close()

    actual = fromtext(f.name, encoding='ascii')
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_fromtext_lineterminators():

    data = [b'foo,bar',
            b'a,1',
            b'b,2',
            b'c,2']

    expect = (('lines',),
              ('foo,bar',),
              ('a,1',),
              ('b,2',),
              ('c,2',))

    for lt in b'\r', b'\n', b'\r\n':
        f = NamedTemporaryFile(mode='wb', delete=False)
        f.write(lt.join(data))
        f.close()
        actual = fromtext(f.name, encoding='ascii')
        ieq(expect, actual)


def test_totext():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    prologue = (
        "{| class='wikitable'\n"
        "|-\n"
        "! foo\n"
        "! bar\n"
    )
    template = (
        "|-\n"
        "| {foo}\n"
        "| {bar}\n"
    )
    epilogue = "|}\n"
    totext(table, f.name, encoding='ascii', template=template,
           prologue=prologue, epilogue=epilogue)

    # check what it did
    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = (
            "{| class='wikitable'\n"
            "|-\n"
            "! foo\n"
            "! bar\n"
            "|-\n"
            "| a\n"
            "| 1\n"
            "|-\n"
            "| b\n"
            "| 2\n"
            "|-\n"
            "| c\n"
            "| 2\n"
            "|}\n"
        )
        eq_(expect, actual)


def test_fromtext_gz():

    # initial data
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)
    f = gzip.open(fn, 'wb')
    try:
        f.write(b'foo\tbar\n')
        f.write(b'a\t1\n')
        f.write(b'b\t2\n')
        f.write(b'c\t3\n')
    finally:
        f.close()

    actual = fromtext(fn, encoding='ascii')
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_totext_gz():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)
    prologue = (
        "{| class='wikitable'\n"
        "|-\n"
        "! foo\n"
        "! bar\n"
    )
    template = (
        "|-\n"
        "| {foo}\n"
        "| {bar}\n"
    )
    epilogue = "|}\n"
    totext(table, fn, encoding='ascii', template=template, prologue=prologue,
           epilogue=epilogue)

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        actual = o.read()
        expect = (
            b"{| class='wikitable'\n"
            b"|-\n"
            b"! foo\n"
            b"! bar\n"
            b"|-\n"
            b"| a\n"
            b"| 1\n"
            b"|-\n"
            b"| b\n"
            b"| 2\n"
            b"|-\n"
            b"| c\n"
            b"| 2\n"
            b"|}\n"
        )
        eq_(expect, actual)
    finally:
        o.close()


def test_totext_headerless():
    table = []
    f = NamedTemporaryFile(delete=False)
    prologue = "-- START\n"
    template = "+ {f1}\n"
    epilogue = "-- END\n"
    totext(table, f.name, encoding='ascii', template=template,
           prologue=prologue, epilogue=epilogue)

    with io.open(f.name, mode='rt', encoding='ascii', newline='') as o:
        actual = o.read()
        expect = prologue + epilogue
        eq_(expect, actual)

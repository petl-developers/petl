from __future__ import absolute_import, print_function, division


__author__ = 'Alistair Miles <alimanfoo@googlemail.com>'


from tempfile import NamedTemporaryFile
import gzip
import os
from nose.tools import eq_


from petl.testutils import ieq
from petl.io.text import fromtext, totext, appendtext


def test_fromtext():

    # initial data
    f = NamedTemporaryFile(delete=False)
    f.write('foo\tbar\n')
    f.write('a\t1\n')
    f.write('b\t2\n')
    f.write('c\t3\n')
    f.close()

    actual = fromtext(f.name)
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual)  # verify can iterate twice


def test_totext():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    prologue = """{| class="wikitable"
|-
! foo
! bar
"""
    template = """|-
| {foo}
| {bar}
"""
    epilogue = "|}"
    totext(table, f.name, template, prologue, epilogue)

    # check what it did
    with open(f.name, 'rb') as o:
        actual = o.read()
        expect = """{| class="wikitable"
|-
! foo
! bar
|-
| a
| 1
|-
| b
| 2
|-
| c
| 2
|}"""
        eq_(expect, actual)


def test_fromtext_gz():

    # initial data
    f = NamedTemporaryFile(delete=False)
    f.close()
    fn = f.name + '.gz'
    os.rename(f.name, fn)
    f = gzip.open(fn, 'wb')
    try:
        f.write('foo\tbar\n')
        f.write('a\t1\n')
        f.write('b\t2\n')
        f.write('c\t3\n')
    finally:
        f.close()

    actual = fromtext(fn)
    expect = (('lines',),
              ('foo\tbar',),
              ('a\t1',),
              ('b\t2',),
              ('c\t3',))
    ieq(expect, actual)
    ieq(expect, actual) # verify can iterate twice


def test_totext_gz():

    # exercise function
    table = (('foo', 'bar'),
             ('a', 1),
             ('b', 2),
             ('c', 2))
    f = NamedTemporaryFile(delete=False)
    fn = f.name + '.gz'
    f.close()
    os.rename(f.name, fn)
    prologue = """{| class="wikitable"
|-
! foo
! bar
"""
    template = """|-
| {foo}
| {bar}
"""
    epilogue = "|}"
    totext(table, fn, template, prologue, epilogue)

    # check what it did
    o = gzip.open(fn, 'rb')
    try:
        actual = o.read()
        expect = """{| class="wikitable"
|-
! foo
! bar
|-
| a
| 1
|-
| b
| 2
|-
| c
| 2
|}"""
        eq_(expect, actual)
    finally:
        o.close()

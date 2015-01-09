from __future__ import absolute_import, print_function, division


from tempfile import NamedTemporaryFile
import gzip
import os


from petl.test.helpers import ieq, eq_
from petl.io.text import fromtext, totext


def test_fromtext():

    # initial data
    f = NamedTemporaryFile(delete=False, mode='wt')
    f.write('foo\tbar\n')
    f.write('a\t1\n')
    f.write('b\t2\n')
    f.write('c\t3\n')
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

    data = ['foo,bar',
            'a,1',
            'b,2',
            'c,2']

    expect = (('lines',),
              ('foo,bar',),
              ('a,1',),
              ('b,2',),
              ('c,2',))

    for lt in '\r', '\n', '\r\n':
        f = NamedTemporaryFile(mode='wt', delete=False)
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
    totext(table, f.name, encoding='ascii', template=template,
           prologue=prologue, epilogue=epilogue)

    # check what it did
    with open(f.name, 'r') as o:
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
    f = gzip.open(fn, 'wt')
    try:
        f.write('foo\tbar\n')
        f.write('a\t1\n')
        f.write('b\t2\n')
        f.write('c\t3\n')
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
    totext(table, fn, encoding='ascii', template=template, prologue=prologue,
           epilogue=epilogue)

    # check what it did
    o = gzip.open(fn, 'rt')
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

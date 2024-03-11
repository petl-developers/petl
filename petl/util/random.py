from __future__ import absolute_import, print_function, division

import hashlib
import random as pyrandom
import time
from collections import OrderedDict
from functools import partial

from petl.compat import xrange, text_type
from petl.util.base import Table


def randomseed():
    """
    Obtain the hex digest of a sha256 hash of the
    current epoch time in nanoseconds.
    """

    time_ns = str(time.time()).encode()
    hash_time = hashlib.sha256(time_ns).hexdigest()
    return hash_time


def randomtable(numflds=5, numrows=100, wait=0, seed=None):
    """
    Construct a table with random numerical data. Use `numflds` and `numrows` to
    specify the number of fields and rows respectively. Set `wait` to a float
    greater than zero to simulate a delay on each row generation (number of
    seconds per row). E.g.::

        >>> import petl as etl
        >>> table = etl.randomtable(3, 100, seed=42)
        >>> table
        +----------------------+----------------------+---------------------+
        | f0                   | f1                   | f2                  |
        +======================+======================+=====================+
        |   0.6394267984578837 | 0.025010755222666936 | 0.27502931836911926 |
        +----------------------+----------------------+---------------------+
        |  0.22321073814882275 |   0.7364712141640124 |  0.6766994874229113 |
        +----------------------+----------------------+---------------------+
        |   0.8921795677048454 |  0.08693883262941615 |  0.4219218196852704 |
        +----------------------+----------------------+---------------------+
        | 0.029797219438070344 |  0.21863797480360336 |  0.5053552881033624 |
        +----------------------+----------------------+---------------------+
        | 0.026535969683863625 |   0.1988376506866485 |  0.6498844377795232 |
        +----------------------+----------------------+---------------------+
        ...
        <BLANKLINE>

    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.
    The only supported seed types are: None, int, float, str, bytes, and bytearray.

    """

    return RandomTable(numflds, numrows, wait=wait, seed=seed)


class RandomTable(Table):
    def __init__(self, numflds=5, numrows=100, wait=0, seed=None):
        self.numflds = numflds
        self.numrows = numrows
        self.wait = wait
        if seed is None:
            self.seed = randomseed()
        else:
            self.seed = seed

    def __iter__(self):
        nf = self.numflds
        nr = self.numrows
        seed = self.seed

        # N.B., we want this to be stable, i.e., same data each time
        pyrandom.seed(seed)

        # construct fields
        flds = ["f%s" % n for n in range(nf)]
        yield tuple(flds)

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(pyrandom.random() for n in range(nf))

    def reseed(self):
        self.seed = randomseed()


def dummytable(
        numrows=100,
        fields=(
                ('foo', partial(pyrandom.randint, 0, 100)),
                ('bar', partial(pyrandom.choice, ('apples', 'pears', 'bananas', 'oranges'))),
                ('baz', pyrandom.random),
        ),
        wait=0,
        seed=None,
):
    """
    Construct a table with dummy data. Use `numrows` to specify the number of
    rows. Set `wait` to a float greater than zero to simulate a delay on each
    row generation (number of seconds per row). E.g.::

        >>> import petl as etl
        >>> table1 = etl.dummytable(100, seed=42)
        >>> table1
        +-----+----------+----------------------+
        | foo | bar      | baz                  |
        +=====+==========+======================+
        |  81 | 'apples' | 0.025010755222666936 |
        +-----+----------+----------------------+
        |  35 | 'pears'  |  0.22321073814882275 |
        +-----+----------+----------------------+
        |  94 | 'apples' |   0.6766994874229113 |
        +-----+----------+----------------------+
        |  69 | 'apples' |   0.5904925124490397 |
        +-----+----------+----------------------+
        |   4 | 'apples' |  0.09369523986159245 |
        +-----+----------+----------------------+
        ...
        <BLANKLINE>

        >>> import random as pyrandom
        >>> from functools import partial
        >>> fields = [('foo', pyrandom.random),
        ...           ('bar', partial(pyrandom.randint, 0, 500)),
        ...           ('baz', partial(pyrandom.choice, ['chocolate', 'strawberry', 'vanilla']))]
        >>> table2 = etl.dummytable(100, fields=fields, seed=42)
        >>> table2
        +---------------------+-----+-------------+
        | foo                 | bar | baz         |
        +=====================+=====+=============+
        |  0.6394267984578837 |  12 | 'vanilla'   |
        +---------------------+-----+-------------+
        | 0.27502931836911926 | 114 | 'chocolate' |
        +---------------------+-----+-------------+
        |  0.7364712141640124 | 346 | 'vanilla'   |
        +---------------------+-----+-------------+
        |  0.8921795677048454 |  44 | 'vanilla'   |
        +---------------------+-----+-------------+
        |  0.4219218196852704 |  15 | 'chocolate' |
        +---------------------+-----+-------------+
        ...
        <BLANKLINE>

        >>> table3_1 = etl.dummytable(50)
        >>> table3_2 = etl.dummytable(100)
        >>> table3_1[5] == table3_2[5]
        False

    Data generation functions can be specified via the `fields` keyword
    argument.

    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.
    The only supported seed types are: None, int, float, str, bytes, and bytearray.

    """

    return DummyTable(numrows=numrows, fields=fields, wait=wait, seed=seed)


class DummyTable(Table):
    def __init__(self, numrows=100, fields=None, wait=0, seed=None):
        self.numrows = numrows
        self.wait = wait
        if fields is None:
            self.fields = OrderedDict()
        else:
            self.fields = OrderedDict(fields)
        if seed is None:
            self.seed = randomseed()
        else:
            self.seed = seed

    def __setitem__(self, item, value):
        self.fields[text_type(item)] = value

    def __iter__(self):
        nr = self.numrows
        seed = self.seed
        fields = self.fields.copy()

        # N.B., we want this to be stable, i.e., same data each time
        pyrandom.seed(seed)

        # construct header row
        hdr = tuple(text_type(f) for f in fields.keys())
        yield hdr

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(fields[f]() for f in fields)

    def reseed(self):
        self.seed = randomseed()

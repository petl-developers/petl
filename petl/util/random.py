from __future__ import absolute_import, print_function, division, \
    unicode_literals


import datetime
import random
import time
from functools import partial
from petl.compat import xrange, OrderedDict


from petl.util.base import RowContainer


def randomtable(numflds=5, numrows=100, wait=0):
    """
    Construct a table with random numerical data. Use `numflds` and `numrows` to
    specify the number of fields and rows respectively. Set `wait` to a float
    greater than zero to simulate a delay on each row generation (number of
    seconds per row). E.g.::

        >>> from petl import randomtable, look
        >>> t = randomtable(5, 10000)
        >>> look(t)
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 'f0'                | 'f1'                | 'f2'                | 'f3'                 | 'f4'                 |
        +=====================+=====================+=====================+======================+======================+
        | 0.37981479583619415 | 0.5651754962690851  | 0.5219839418441516  | 0.400507081757018    | 0.18772722969580335  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.8523718373108918  | 0.9728988775985702  | 0.539819811070272   | 0.5253127991162814   | 0.032332586052070345 |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.15767415808765595 | 0.8723372406647985  | 0.8116271113050197  | 0.19606663402788693  | 0.02917384287810021  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.29027126477145737 | 0.9458013821235983  | 0.0558711583090582  | 0.8388382491420909   | 0.533855533396786    |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.7299727877963395  | 0.7293822340944851  | 0.953624640847381   | 0.7161554959575555   | 0.8681001821667421   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.7057077618876934  | 0.5222733323906424  | 0.26527912571554013 | 0.41069309093677264  | 0.7062831671289698   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.9447075997744453  | 0.3980291877822444  | 0.5748113148854611  | 0.037655670603881974 | 0.30826709590498524  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.21559911346698513 | 0.8353039675591192  | 0.5558847892537019  | 0.8561403358605812   | 0.01109608253313421  |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.27334411287843097 | 0.10064946027523636 | 0.7476185996637322  | 0.26201984851765325  | 0.6303996377010502   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+
        | 0.8348722928576766  | 0.40319578510057763 | 0.3658094978577834  | 0.9829576880714145   | 0.6170025401631835   |
        +---------------------+---------------------+---------------------+----------------------+----------------------+

    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.

    """

    return RandomTable(numflds, numrows, wait=wait)


class RandomTable(RowContainer):

    def __init__(self, numflds=5, numrows=100, wait=0):
        self.numflds = numflds
        self.numrows = numrows
        self.wait = wait
        self.seed = datetime.datetime.now()

    def __iter__(self):

        nf = self.numflds
        nr = self.numrows
        seed = self.seed

        # N.B., we want this to be stable, i.e., same data each time
        random.seed(seed)

        # construct fields
        flds = ['f%s' % n for n in range(nf)]
        yield tuple(flds)

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(random.random() for n in range(nf))

    def reseed(self):
        self.seed = datetime.datetime.now()


def dummytable(numrows=100,
               fields=(('foo', partial(random.randint, 0, 100)),
                       ('bar', partial(random.choice, ('apples', 'pears',
                                                       'bananas', 'oranges'))),
                       ('baz', random.random)),
               wait=0):
    """
    Construct a table with dummy data. Use `numrows` to specify the number of
    rows. Set `wait` to a float greater than zero to simulate a delay on each
    row generation (number of seconds per row). E.g.::

        >>> from petl import dummytable, look
        >>> t1 = dummytable(10000)
        >>> look(t1)
        +-------+-----------+----------------------+
        | 'foo' | 'bar'     | 'baz'                |
        +=======+===========+======================+
        | 98    | 'oranges' | 0.017443519200384117 |
        +-------+-----------+----------------------+
        | 85    | 'pears'   | 0.6126183086894914   |
        +-------+-----------+----------------------+
        | 43    | 'apples'  | 0.8354915052285888   |
        +-------+-----------+----------------------+
        | 32    | 'pears'   | 0.9612740566307508   |
        +-------+-----------+----------------------+
        | 35    | 'bananas' | 0.4845179128370132   |
        +-------+-----------+----------------------+
        | 16    | 'pears'   | 0.150174888085586    |
        +-------+-----------+----------------------+
        | 98    | 'bananas' | 0.22592589109877748  |
        +-------+-----------+----------------------+
        | 82    | 'bananas' | 0.4887849296756226   |
        +-------+-----------+----------------------+
        | 75    | 'apples'  | 0.8414305202212253   |
        +-------+-----------+----------------------+
        | 78    | 'bananas' | 0.025845900016858714 |
        +-------+-----------+----------------------+

    Note that the data are generated on the fly and are not stored in memory,
    so this function can be used to simulate very large tables.

    Data generation functions can be specified via the `fields` keyword
    argument, or set on the table via the suffix notation, e.g.::

        >>> import random
        >>> from functools import partial
        >>> t2 = dummytable(10000, fields=[('foo', random.random), ('bar', partial(random.randint, 0, 500))])
        >>> t2['baz'] = partial(random.choice, ['chocolate', 'strawberry', 'vanilla'])
        >>> look(t2)
        +---------------------+-------+--------------+
        | 'foo'               | 'bar' | 'baz'        |
        +=====================+=======+==============+
        | 0.04595169186388326 | 370   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.29252999472988905 | 90    | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.7939324498894116  | 146   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.4964898678468417  | 123   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.26250784199548494 | 327   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.748470693146964   | 275   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.8995553034254133  | 151   | 'strawberry' |
        +---------------------+-------+--------------+
        | 0.26331484411715367 | 211   | 'chocolate'  |
        +---------------------+-------+--------------+
        | 0.4740252948218193  | 364   | 'vanilla'    |
        +---------------------+-------+--------------+
        | 0.166428545780258   | 59    | 'vanilla'    |
        +---------------------+-------+--------------+

    .. versionchanged:: 0.6

    Now supports different field types, e.g., non-numeric. Previous functionality
    is available as :func:`randomtable`.

    """

    return DummyTable(numrows=numrows, fields=fields, wait=wait)


class DummyTable(RowContainer):

    def __init__(self, numrows=100, fields=None, wait=0):
        self.numrows = numrows
        self.wait = wait
        if fields is None:
            self.fields = OrderedDict()
        else:
            self.fields = OrderedDict(fields)
        self.seed = datetime.datetime.now()

    def __setitem__(self, item, value):
        self.fields[str(item)] = value

    def __iter__(self):
        nr = self.numrows
        seed = self.seed
        fields = self.fields.copy()

        # N.B., we want this to be stable, i.e., same data each time
        random.seed(seed)

        # construct header row
        hdr = tuple(str(f) for f in fields.keys())
        yield hdr

        # construct data rows
        for _ in xrange(nr):
            # artificial delay
            if self.wait:
                time.sleep(self.wait)
            yield tuple(fields[f]() for f in fields)

    def reseed(self):
        self.seed = datetime.datetime.now()

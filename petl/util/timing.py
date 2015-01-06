from __future__ import absolute_import, print_function, division, \
    unicode_literals


import sys
import time


from petl.util.base import RowContainer


def progress(table, batchsize=1000, prefix="", out=sys.stderr):
    """
    Report progress on rows passing through. E.g.::

        >>> from petl import dummytable, progress, tocsv
        >>> d = dummytable(100500)
        >>> p = progress(d, 10000)
        >>> tocsv(p, 'output.csv')
        10000 rows in 0.57s (17574 rows/second); batch in 0.57s (17574 rows/second)
        20000 rows in 1.13s (17723 rows/second); batch in 0.56s (17876 rows/second)
        30000 rows in 1.69s (17732 rows/second); batch in 0.56s (17749 rows/second)
        40000 rows in 2.27s (17652 rows/second); batch in 0.57s (17418 rows/second)
        50000 rows in 2.83s (17679 rows/second); batch in 0.56s (17784 rows/second)
        60000 rows in 3.39s (17694 rows/second); batch in 0.56s (17769 rows/second)
        70000 rows in 3.96s (17671 rows/second); batch in 0.57s (17534 rows/second)
        80000 rows in 4.53s (17677 rows/second); batch in 0.56s (17720 rows/second)
        90000 rows in 5.09s (17681 rows/second); batch in 0.56s (17715 rows/second)
        100000 rows in 5.66s (17675 rows/second); batch in 0.57s (17625 rows/second)
        100500 rows in 5.69s (17674 rows/second)

    See also :func:`clock`.

    .. versionadded:: 0.10

    """

    return ProgressView(table, batchsize, prefix, out)


class ProgressView(RowContainer):

    def __init__(self, wrapped, batchsize, prefix, out):
        self.wrapped = wrapped
        self.batchsize = batchsize
        self.prefix = prefix
        self.out = out

    def __iter__(self):
        start = time.time()
        batchstart = start
        for n, r in enumerate(self.wrapped):
            if n % self.batchsize == 0 and n > 0:
                batchend = time.time()
                batchtime = batchend - batchstart
                elapsedtime = batchend - start
                try:
                    rate = int(n / elapsedtime)
                except ZeroDivisionError:
                    rate = 0
                try:
                    batchrate = int(self.batchsize / batchtime)
                except ZeroDivisionError:
                    batchrate = 0
                v = (n, elapsedtime, rate, batchtime, batchrate)
                message = self.prefix + \
                    '%s rows in %.2fs (%s row/s); ' \
                    'batch in %.2fs (%s row/s)' % v
                print(message, file=self.out)
                if hasattr(self.out, 'flush'):
                    self.out.flush()
                batchstart = batchend
            yield r
        end = time.time()
        elapsedtime = end - start
        try:
            rate = int(n / elapsedtime)
        except ZeroDivisionError:
            rate = 0
        v = (n, elapsedtime, rate)
        message = self.prefix + '%s rows in %.2fs (%s row/s)' % v
        print(message, file=self.out)
        if hasattr(self.out, 'flush'):
            self.out.flush()


def clock(table):
    """
    Time how long is spent retrieving rows from the wrapped container. Enables
    diagnosis of which steps in a pipeline are taking the most time. E.g.::

        >>> from petl import dummytable, clock, convert, progress, tocsv
        >>> t1 = dummytable(100000)
        >>> c1 = clock(t1)
        >>> t2 = convert(c1, 'foo', lambda v: v**2)
        >>> c2 = clock(t2)
        >>> p = progress(c2, 10000)
        >>> tocsv(p, 'dummy.csv')
        10000 rows in 1.17s (8559 rows/second); batch in 1.17s (8559 rows/second)
        20000 rows in 2.34s (8548 rows/second); batch in 1.17s (8537 rows/second)
        30000 rows in 3.51s (8547 rows/second); batch in 1.17s (8546 rows/second)
        40000 rows in 4.68s (8541 rows/second); batch in 1.17s (8522 rows/second)
        50000 rows in 5.89s (8483 rows/second); batch in 1.21s (8261 rows/second)
        60000 rows in 7.30s (8221 rows/second); batch in 1.40s (7121 rows/second)
        70000 rows in 8.59s (8144 rows/second); batch in 1.30s (7711 rows/second)
        80000 rows in 9.78s (8182 rows/second); batch in 1.18s (8459 rows/second)
        90000 rows in 10.98s (8193 rows/second); batch in 1.21s (8279 rows/second)
        100000 rows in 12.30s (8132 rows/second); batch in 1.31s (7619 rows/second)
        100000 rows in 12.30s (8132 rows/second)
        >>> # time consumed retrieving rows from t1
        ... c1.time
        5.4099999999999895
        >>> # time consumed retrieving rows from t2
        ... c2.time
        8.740000000000006
        >>> # actual time consumed by the convert step
        ... c2.time - c1.time
        3.330000000000016

    See also :func:`progress`.

    .. versionadded:: 0.10

    """

    return ClockView(table)


class ClockView(RowContainer):

    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __iter__(self):
        self.time = 0
        it = iter(self.wrapped)
        while True:
            before = time.clock()
            row = next(it)
            after = time.clock()
            self.time += (after - before)
            yield row

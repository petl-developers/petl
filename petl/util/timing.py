from __future__ import absolute_import, print_function, division


import sys
import time


from petl.util.base import Table


def progress(table, batchsize=1000, prefix="", out=sys.stderr):
    """
    Report progress on rows passing through. E.g.::

        >>> import petl as etl
        >>> table = etl.dummytable(100000)
        >>> table.progress(10000).tocsv('example.csv')
        10000 rows in 0.14s (72628 row/s); batch in 0.14s (72628 row/s)
        20000 rows in 0.24s (82657 row/s); batch in 0.10s (95900 row/s)
        30000 rows in 0.34s (87189 row/s); batch in 0.10s (97928 row/s)
        40000 rows in 0.45s (89315 row/s); batch in 0.10s (96365 row/s)
        50000 rows in 0.55s (90791 row/s); batch in 0.10s (97213 row/s)
        60000 rows in 0.65s (91671 row/s); batch in 0.10s (96340 row/s)
        70000 rows in 0.76s (92128 row/s); batch in 0.11s (94974 row/s)
        80000 rows in 0.86s (92547 row/s); batch in 0.10s (95589 row/s)
        90000 rows in 0.97s (92864 row/s); batch in 0.10s (95476 row/s)
        100000 rows in 1.08s (92813 row/s); batch in 0.11s (92363 row/s)
        100000 rows in 1.08s (92808 row/s)

    See also :func:`petl.util.timing.clock`.

    """

    return ProgressView(table, batchsize, prefix, out)


Table.progress = progress


class ProgressView(Table):

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

        >>> import petl as etl
        >>> t1 = etl.dummytable(100000)
        >>> c1 = etl.clock(t1)
        >>> t2 = etl.convert(c1, 'foo', lambda v: v**2)
        >>> c2 = etl.clock(t2)
        >>> p = etl.progress(c2, 10000)
        >>> etl.tocsv(p, 'example.csv')
        10000 rows in 0.17s (59406 row/s); batch in 0.17s (59406 row/s)
        20000 rows in 0.34s (59270 row/s); batch in 0.17s (59136 row/s)
        30000 rows in 0.51s (59185 row/s); batch in 0.17s (59014 row/s)
        40000 rows in 0.68s (59097 row/s); batch in 0.17s (58834 row/s)
        50000 rows in 0.85s (59075 row/s); batch in 0.17s (58989 row/s)
        60000 rows in 1.02s (59045 row/s); batch in 0.17s (58895 row/s)
        70000 rows in 1.19s (58990 row/s); batch in 0.17s (58661 row/s)
        80000 rows in 1.36s (59001 row/s); batch in 0.17s (59083 row/s)
        90000 rows in 1.52s (59043 row/s); batch in 0.17s (59373 row/s)
        100000 rows in 1.69s (59043 row/s); batch in 0.17s (59050 row/s)
        100000 rows in 1.69s (59041 row/s)
        >>> # time consumed retrieving rows from t1
        ... c1.time
        0.7471600000000382
        >>> # time consumed retrieving rows from t2
        ... c2.time
        1.187450000000062
        >>> # actual time consumed by the convert step
        ... c2.time - c1.time
        0.44029000000002383

    See also :func:`petl.util.timing.progress`.

    """

    return ClockView(table)


Table.clock = clock


class ClockView(Table):

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

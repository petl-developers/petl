from __future__ import absolute_import, print_function, division


import abc
import logging
import sys
import time


from petl.compat import PY3
from petl.util.base import Table
from petl.util.statistics import onlinestats


def progress(table, batchsize=1000, prefix="", out=None):
    """
    Report progress on rows passing through to a file or file-like object
    (defaults to sys.stderr). E.g.::

        >>> import petl as etl
        >>> table = etl.dummytable(100000)
        >>> table.progress(10000).tocsv('example.csv')  # doctest: +SKIP
        10000 rows in 0.13s (78363 row/s); batch in 0.13s (78363 row/s)
        20000 rows in 0.22s (91679 row/s); batch in 0.09s (110448 row/s)
        30000 rows in 0.31s (96573 row/s); batch in 0.09s (108114 row/s)
        40000 rows in 0.40s (99535 row/s); batch in 0.09s (109625 row/s)
        50000 rows in 0.49s (101396 row/s); batch in 0.09s (109591 row/s)
        60000 rows in 0.59s (102245 row/s); batch in 0.09s (106709 row/s)
        70000 rows in 0.68s (103221 row/s); batch in 0.09s (109498 row/s)
        80000 rows in 0.77s (103810 row/s); batch in 0.09s (108126 row/s)
        90000 rows in 0.90s (99465 row/s); batch in 0.13s (74516 row/s)
        100000 rows in 1.02s (98409 row/s); batch in 0.11s (89821 row/s)
        100000 rows in 1.02s (98402 row/s); batches in 0.10 +/- 0.02s [0.09-0.13] (100481 +/- 13340 rows/s [74516-110448])

    See also :func:`petl.util.timing.clock`.

    """

    return ProgressView(table, batchsize, prefix, out)


def log_progress(table, batchsize=1000, prefix="", logger=None, level=logging.INFO):
    """
    Report progress on rows passing through to a python logger. If logger is
    none, a new logger will be created that, by default, streams to stdout. E.g.::

        >>> import petl as etl
        >>> table = etl.dummytable(100000)
        >>> table.log_progress(10000).tocsv('example.csv')  # doctest: +SKIP
        10000 rows in 0.13s (78363 row/s); batch in 0.13s (78363 row/s)
        20000 rows in 0.22s (91679 row/s); batch in 0.09s (110448 row/s)
        30000 rows in 0.31s (96573 row/s); batch in 0.09s (108114 row/s)
        40000 rows in 0.40s (99535 row/s); batch in 0.09s (109625 row/s)
        50000 rows in 0.49s (101396 row/s); batch in 0.09s (109591 row/s)
        60000 rows in 0.59s (102245 row/s); batch in 0.09s (106709 row/s)
        70000 rows in 0.68s (103221 row/s); batch in 0.09s (109498 row/s)
        80000 rows in 0.77s (103810 row/s); batch in 0.09s (108126 row/s)
        90000 rows in 0.90s (99465 row/s); batch in 0.13s (74516 row/s)
        100000 rows in 1.02s (98409 row/s); batch in 0.11s (89821 row/s)
        100000 rows in 1.02s (98402 row/s); batches in 0.10 +/- 0.02s [0.09-0.13] (100481 +/- 13340 rows/s [74516-110448])

    See also :func:`petl.util.timing.clock`.

    """

    return LoggingProgressView(table, batchsize, prefix, logger, level=level)


Table.progress = progress
Table.log_progress = log_progress


class ProgressViewBase(Table):
    """
    Abstract base class for reporting on proecessing status
    """

    def __init__(self, inner, batchsize, prefix):
        self.inner = inner
        self.batchsize = batchsize
        self.prefix = prefix

    @abc.abstractmethod
    def print_message(self, message):
        pass

    def __iter__(self):
        start = time.time()
        batchstart = start
        batchn = 0
        batchtimemin, batchtimemax = None, None
        batchtimemean, batchtimevar = 0, 0
        batchratemean, batchratevar = 0, 0

        for n, r in enumerate(self.inner):
            if n % self.batchsize == 0 and n > 0:
                batchn += 1
                batchend = time.time()
                batchtime = batchend - batchstart
                if batchtimemin is None or batchtime < batchtimemin:
                    batchtimemin = batchtime
                if batchtimemax is None or batchtime > batchtimemax:
                    batchtimemax = batchtime
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
                self.print_message(message)
                batchstart = batchend
                batchtimemean, batchtimevar = \
                    onlinestats(batchtime, batchn, mean=batchtimemean,
                                 variance=batchtimevar)
                batchratemean, batchratevar = \
                    onlinestats(batchrate, batchn, mean=batchratemean,
                                 variance=batchratevar)
            yield r

        # compute total elapsed time and rate
        end = time.time()
        elapsedtime = end - start
        try:
            rate = int(n / elapsedtime)
        except ZeroDivisionError:
            rate = 0

        # construct the final message
        if batchn > 1:
            if batchtimemin is None:
                batchtimemin = 0
            if batchtimemax is None:
                batchtimemax = 0
            try:
                batchratemin = int(self.batchsize / batchtimemax)
            except ZeroDivisionError:
                batchratemin = 0
            try:
                batchratemax = int(self.batchsize / batchtimemin)
            except ZeroDivisionError:
                batchratemax = 0
            v = (n, elapsedtime, rate, batchtimemean, batchtimevar**.5,
                 batchtimemin, batchtimemax, int(batchratemean),
                 int(batchratevar**.5), int(batchratemin), int(batchratemax))
            message = self.prefix + '%s rows in %.2fs (%s row/s); batches in ' \
                                    '%.2f +/- %.2fs [%.2f-%.2f] ' \
                                    '(%s +/- %s rows/s [%s-%s])' % v
        else:
            v = (n, elapsedtime, rate)
            message = self.prefix + '%s rows in %.2fs (%s row/s)' % v

        self.print_message(message)


class ProgressView(ProgressViewBase):
    """
    Reports progress to a file_object like sys.stdout or a file handler
    """

    def __init__(self, inner, batchsize, prefix, out):
        if out is None:
            self.file_object = sys.stderr
        else:
            self.file_object = out
        super(ProgressView, self).__init__(inner, batchsize, prefix)

    def print_message(self, message):
        print(message, file=self.file_object)
        if hasattr(self.file_object, 'flush'):
            self.file_object.flush()


class LoggingProgressView(ProgressViewBase):
    """
    Reports progress to a logger, log handler, or log adapter
    """

    def __init__(self, inner, batchsize, prefix, logger, level=logging.INFO):
        if logger is None:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(level)
        else:
            self.logger = logger
        self.level = level
        super(LoggingProgressView, self).__init__(inner, batchsize, prefix)

    def print_message(self, message):
        self.logger.log(self.level, message)


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
        >>> etl.tocsv(p, 'example.csv')  # doctest: +SKIP
        10000 rows in 0.23s (44036 row/s); batch in 0.23s (44036 row/s)
        20000 rows in 0.38s (52167 row/s); batch in 0.16s (63979 row/s)
        30000 rows in 0.54s (55749 row/s); batch in 0.15s (64624 row/s)
        40000 rows in 0.69s (57765 row/s); batch in 0.15s (64793 row/s)
        50000 rows in 0.85s (59031 row/s); batch in 0.15s (64707 row/s)
        60000 rows in 1.00s (59927 row/s); batch in 0.15s (64847 row/s)
        70000 rows in 1.16s (60483 row/s); batch in 0.16s (64051 row/s)
        80000 rows in 1.31s (61008 row/s); batch in 0.15s (64953 row/s)
        90000 rows in 1.47s (61356 row/s); batch in 0.16s (64285 row/s)
        100000 rows in 1.62s (61703 row/s); batch in 0.15s (65012 row/s)
        100000 rows in 1.62s (61700 row/s); batches in 0.16 +/- 0.02s [0.15-0.23] (62528 +/- 6173 rows/s [44036-65012])
        >>> # time consumed retrieving rows from t1
        ... c1.time  # doctest: +SKIP
        0.7243089999999492
        >>> # time consumed retrieving rows from t2
        ... c2.time  # doctest: +SKIP
        1.1704209999999766
        >>> # actual time consumed by the convert step
        ... c2.time - c1.time  # doctest: +SKIP
        0.4461120000000274

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
            before = time.perf_counter() if PY3 else time.clock()
            try:
                row = next(it)
            except StopIteration:
                return
            after = time.perf_counter() if PY3 else time.clock()
            self.time += (after - before)
            yield row

"""
TODO doc me

"""


import logging
from itertools import islice
from collections import defaultdict, Counter
from datetime import datetime, date, time


logger = logging.getLogger('petl')
d, i, w, e = logger.debug, logger.info, logger.warn, logger.error # abbreviations


class Profiler(object):
    """
    TODO doc me

    """


    def __init__(self, table=None, max_sample_size=0):
        assert isinstance(max_sample_size, int), 'max_sample_size must be an int'
        self._table = table
        self._max_sample_size = max_sample_size
        self._analyses = list()


    def add(self, analysis, field=None):
        self._analyses.append((analysis, field))


    def profile(self):
        
        d('setup the report')
        report = dict()
        report['table'] = dict()
        report['field'] = dict()
        it = iter(self._table)
        fields = it.next()
        report['table']['default'] = dict()
        report['table']['default']['fields'] = tuple(fields)
        for f in fields:
            report['field'][f] = dict()

        d('setup analyses')
        table_analyses = [cls() for cls, field in self._analyses if hasattr(cls, 'accept_row')]
        field_analyses = defaultdict(list)
        for cls, field in self._analyses:
            if hasattr(cls, 'accept_value'):
                if field is not None:
                    # add analysis on a single field
                    field_analyses[field].append(cls())
                else:
                    # add analysis on all fields (not field specified)
                    for field in fields:
                        field_analyses[field].append(cls())
        
        d('deal with sampling')
        if self._max_sample_size:
            it = islice(it, 0, self._max_sample_size-1)

        d('profile the data')
        sample_size = 0
        for row in it:
            sample_size += 1
            for analysis in table_analyses:
                analysis.accept_row(row)
            for field in fields:
                field_index = fields.index(field)
                try:
                    value = row[field_index]
                except IndexError:
                    value = Ellipsis
                for analysis in field_analyses[field]:
                    analysis.accept_value(value)
        
        d('build the report')
        report['table']['default']['sample_size'] = sample_size
        for analysis in table_analyses:
            key, data = analysis.report()
            d(key)
            d(data)
            report['table'][key] = data
        for field in fields:
            for analysis in field_analyses[field]:
                key, data = analysis.report()
                d(field)
                d(key)
                d(data)
                report['field'][field][key] = data

        d('clean up')
        if hasattr(it, 'close'):
            it.close()
        
        return report


class RowLengths(object):

    def __init__(self):
        self._row_lengths_sum = 0
        self._row_count = 0
        self._min_row_length = None
        self._max_row_length = None
        
    def accept_row(self, row):
        n = len(row)
        self._row_count += 1
        self._row_lengths_sum += n
        if self._min_row_length is None or n < self._min_row_length:
            self._min_row_length = n
        if self._max_row_length is None or n > self._max_row_length:
            self._max_row_length = n

    def report(self):
        d('build a report of row lengths data')
        data = {
                'row_count': self._row_count,
                'row_lengths_sum': self._row_lengths_sum,
                'min_row_length': self._min_row_length,
                'max_row_length': self._max_row_length,
                'mean_row_length': self._row_lengths_sum / self._row_count
                }
        return ('row_lengths', data)
    

class DistinctValues(object):
    
    def __init__(self):
        self._counter = Counter()

    def accept_value(self, value):
        self._counter[value] += 1

    def report(self):
        d('build a report of distinct values')
        return ('distinct_values', self._counter)


class BasicStatistics(object):

    def __init__(self):
        self._count = 0
        self._errors = 0
        self._sum = 0
        self._max = None
        self._min = None

    def accept_value(self, value):
        try:
            value = float(value)
        except ValueError:
            self._errors += 1
        except TypeError:
            self._errors += 1
        else:
            self._count += 1
            self._sum += value
            if self._min is None or value < self._min:
                self._min = value
            if self._max is None or value > self._max:
                self._max = value

    def report(self):
        d('build a report of basic statistics')
        data = {
                'min': self._min,
                'max': self._max,
                'sum': self._sum,
                'count': self._count,
                'errors': self._errors
                }
        if self._count > 0:
            data['mean'] = self._sum / self._count

        return ('basic_statistics', data)
    
    
# N.B., this list of date, time and datetime formats is far inferior to the set
# of formats recognised by the Python dateutil package. If you want to do any
# serious profiling of datetimes, use the Types analysis from petlx (TODO) 


date_formats = {
                '%x',
                '%d/%m/%y',
                '%d/%m/%Y',
                '%d %b %y',
                '%d %b %Y',
                '%d. %b. %Y',
                '%d %B %Y',
                '%d. %B %Y',
                '%a %d %b %y',
                '%a %d/%b %y',
                '%a %d %B %Y',
                '%A %d %B %Y',
                '%m-%d',
                '%y-%m-%d',
                '%Y-%m-%d',
                '%m/%y',
                '%d/%b',
                '%m/%d/%y',
                '%m/%d/%Y',
                '%b %d, %y',
                '%b %d, %Y',
                '%B %d, %Y',
                '%a, %b %d, %y',
                '%a, %B %d, %Y',
                '%A, %d. %B %Y'
                }


time_formats = {
                '%X',
                '%H:%M',
                '%H:%M:%S',
                '%I:%M %p',
                '%I:%M:%S %p',
                '%M:%S.%f',
                '%H:%M:%S.%f',
                '%I:%M:%S.%f %p',
                '%I:%M%p',
                '%I:%M:%S%p',
                '%I:%M:%S.%f%p'
                }


datetime_formats = {
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S.%f'
                    }


def datetime_cls(value):
    formats = {datetime: datetime_formats,
               date: date_formats,
               time: time_formats}
    value = value.strip()
    for cls in formats:
        for fmt in formats[cls]:
            try:
                datetime.strptime(value, fmt)
            except ValueError:
                pass
            except TypeError:
                pass
            else:
                return cls
    return None


true_strings = {'true', 't', 'yes', 'y', '1'}
false_strings = {'false', 'f', 'no', 'n', '0'}


def bool_(value):
    value = value.strip().lower()
    if value in true_strings:
        return True
    elif value in false_strings:
        return False
    else:
        raise ValueError('value is not one of recognised boolean representations: %s' % value)
    

class Types(object):
    
    def __init__(self, threshold=0.5):
        self._threshold = threshold
        self._count = 0
        self._actual_types = Counter()
        self._parse_types = Counter()
        self._consensus_types = Counter()

    def accept_value(self, value):
        self._count += 1
        cls = value.__class__
        self._actual_types[cls.__name__] += 1
        self._consensus_types[cls.__name__] += 1
        if isinstance(value, basestring):
            for cls in (int, float): 
                try:
                    cls(value)
                except ValueError:
                    pass
                except TypeError:
                    pass
                else:
                    self._parse_types[cls.__name__] += 1
                    self._consensus_types[cls.__name__] += 1
            dt_cls = datetime_cls(value)
            if dt_cls is not None:
                self._parse_types[dt_cls.__name__] += 1
                self._consensus_types[dt_cls.__name__] += 1
            try:
                bool_(value)
            except ValueError:
                pass
            except TypeError:
                pass
            else:
                self._parse_types[bool.__name__] += 1
                self._consensus_types[bool.__name__] += 1

    def report(self):

        d('build report')
        data = {
                'actual_types': self._actual_types,
                'parse_types': self._parse_types,
                'consensus_types': self._consensus_types
                }

        return ('types', data)
    

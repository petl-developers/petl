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


    def __init__(self, table=None, start=0, stop=None, step=1):
        self.table = table
        self.start = start
        self.stop = stop
        self.step = step
        self.analysis_classes = set()


    def add(self, cls, field=None):
        self.analysis_classes.add((cls, field))


    def profile(self):
        
        d('create an iterator')
        it = iter(self.table)
        
        try:
            
            d('get field names')
            fields = it.next()

            d('adapt iterator to sampling parameters')
            it = islice(it, self.start, self.stop, self.step)
            
            d('normalise analysis classes, now we know the field names')
            row_analysis_classes = set()
            value_analysis_classes = dict()
            for f in fields:
                value_analysis_classes[f] = set()
            for cls, field in self.analysis_classes:
                if hasattr(cls, 'accept_row'):
                    # ignore the field name, this is a row analysis
                    row_analysis_classes.add(cls)
                elif hasattr(cls, 'accept_value'):
                    if field is None:
                        # add on all fields
                        for f in fields:
                            value_analysis_classes[f].add(cls)
                    else:
                        value_analysis_classes[field].add(cls)
                else:
                    it.close()
                    raise Exception('analysis class has neither accept_row or accept_value method: %(0)s' % cls)
            
            d('instantiate analysis_classes')
            row_analyses = [cls() for cls in row_analysis_classes]
            value_analyses = defaultdict(list)
            for f in fields:
                for cls in value_analysis_classes[f]:
                    value_analyses[f].append(cls())
                    
            d('set up containers for example data')
            example_data = dict()
            for f in fields:
                example_data[f] = list()

            d('profile the data')
            sample_size = 0
            store_examples = True
            for row in it:
                sample_size += 1
                if store_examples:
                    if sample_size > 5:
                        store_examples = False
                for analysis in row_analyses:
                    analysis.accept_row(row)
                for f in fields:
                    index = fields.index(f)
                    try:
                        value = row[index]
                    except IndexError:
                        value = Ellipsis
                    for analysis in value_analyses[f]:
                        analysis.accept_value(value)
                    if store_examples:
                        example_data[f].append(value)

            d('construct the report')
            report = ProfilingReport(fields=fields, 
                                     sample_size=sample_size, 
                                     start=self.start, 
                                     stop=self.stop, 
                                     step=self.step,
                                     row_analyses=row_analyses,
                                     value_analyses=value_analyses,
                                     example_data=example_data)
            return report
            
        except:
            raise
        finally:
            # make sure iterator is closed, even if not exhausted
            if hasattr(it, 'close'):
                it.close()
            

class ProfilingReport(object):
    
    def __init__(self, fields, sample_size, start, stop, step, row_analyses, 
                 value_analyses, example_data):
        self.fields = fields
        self.sample_size = sample_size
        self.start = start
        self.stop = stop
        self.step = step
        self.row_analyses = row_analyses
        self.value_analyses = value_analyses
        self.example_data = example_data
        
    def __str__(self):
        return repr(self)

    def __repr__(self):
        
        r = """
================
Profiling Report
================
"""
        if self.stop is None:
            preamble = """
Profiling was carried out on %(sample_size)s rows in total. Rows were selected 
by sampling 1 in every %(step)s rows, starting from row %(start)s.
""" 
        else:
            preamble = """
Profiling was carried out on %(sample_size)s rows in total. Rows were selected 
by sampling 1 in every %(step)s rows, starting from row %(start)s and ending at
row %(stop)s.
""" 
        preamble = preamble % {'sample_size': self.sample_size, 
                               'step': self.step,
                               'start': self.start,
                               'stop': self.stop}
        r += preamble
        
        r += "\nFields:\n"
        for i, f in enumerate(self.fields):
            r += "%(index)s. %(field)r\n" % {'index': i+1,
                                             'field': f}

        for analysis in self.row_analyses:
            r += repr(analysis)

        for i, f in enumerate(self.fields):
            heading = '\n%(index)s. Field: %(field)r\n' % {'index': i+1,
                                                           'field': f}
            r += heading
            r += '=' * len(heading)
            r += '\n'
            r += '\nExample data: %s\n' % ", ".join(map(repr, self.example_data[f])) 
            
            for analysis in self.value_analyses[f]:
                r += repr(analysis)
                
        return r


def repr_counter(counter):
    r = ", ".join('%r (%s)' % (k, counter[k]) for k in sorted(counter))
    return r
    
    
class RowLengths(object):

    def __init__(self):
        self.counter = Counter()
        
    def accept_row(self, row):
        n = len(row)
        self.counter[n] += 1

    def __repr__(self):
        r = """
Row lengths: %s
""" % repr_counter(self.counter)
        return r


class DistinctValues(object):
    
    def __init__(self):
        self.counter = Counter()

    def accept_value(self, value):
        self.counter[value] += 1
        
    def __repr__(self):
        r = """
Distinct values (most common): %s
""" % ", ".join(('%r (%s)' % item for item in self.counter.most_common(20)))
        return r


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
    

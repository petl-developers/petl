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


    def __init__(self):
        self.row_analysis_classes = set()
        self.value_analysis_classes = set()


    def add(self, cls, field=None):
        if hasattr(cls, 'accept_row'):
            # ignore the field, this analysis accepts whole rows
            self.row_analysis_classes.add(cls)
        elif hasattr(cls, 'accept_value'):
            # N.B., field=None implies the analysis should be done on all fields
            self.value_analysis_classes.add((cls, field))
        else:
            raise Exception('analysis class has neither accept_row or accept_value method: %(0)s' % cls)


    def profile(self, table, start=1, stop=None, step=1):
        """
        TODO doc me
        
        """
        
        d('create an iterator')
        table_iterator = iter(table)
        
        try:
            
            d('get field names')
            fields = table_iterator.next()

            d('adapt iterator to sampling parameters')
            # N.B., index rows from 1
            table_iterator = islice(table_iterator, 
                                    start - 1, 
                                    stop - 1 if stop is not None else stop, 
                                    step)
            
            d('normalise value analysis classes, now we know the field names')
            value_analysis_classes = dict()
            for f in fields:
                # use a set for each field to ensure the same analysis is not 
                # duplicated
                value_analysis_classes[f] = set()
            for cls, field in self.value_analysis_classes:
                if field is None:
                    # add on all fields
                    for f in fields:
                        value_analysis_classes[f].add(cls)
                else:
                    value_analysis_classes[field].add(cls)
            
            d('instantiate analysis_classes')
            row_analyses = [cls() for cls in self.row_analysis_classes]
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
            for row in table_iterator:
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
            report = Report(fields=fields, 
                                     sample_size=sample_size, 
                                     start=start, 
                                     stop=stop, 
                                     step=step,
                                     row_analyses=row_analyses,
                                     value_analyses=value_analyses,
                                     example_data=example_data)
            return report
            
        except:
            raise
        finally:
            # make sure iterator is closed, even if not exhausted
            if hasattr(table_iterator, 'closeit'):
                table_iterator.closeit()
            

class Report(object):
    
    def __init__(self, fields, sample_size, start, stop, step, row_analyses, 
                 value_analyses, example_data):
        self.fields = tuple(fields)
        self.sample_size = sample_size
        self.start = start
        self.stop = stop
        self.step = step
        self.row_analyses = row_analyses
        self.value_analyses = value_analyses
        self.example_data = example_data
        
    def get_analysis(self, cls, field=None):
        if hasattr(cls, 'accept_row'):
            # ignore field argument
            l = [a for a in self.row_analyses if isinstance(a, cls)]
            if len(l) > 0:
                return l[0]
            return None
        elif hasattr(cls, 'accept_value'):
            if field in self.value_analyses:
                l = [a for a in self.value_analyses[field] if isinstance(a, cls)]
                if len(l) > 0:
                    return l[0]
            return None
        
    def __str__(self):
        return repr(self)

    def __repr__(self):
        
        r = """
================
Profiling Report
================
"""
        # construct the preamble
        if self.stop is None:
            preamble = """
Profiling was carried out on %(sample_size)s rows in total. Rows were selected 
by sampling 1 in every %(step)s rows, starting from row %(start)s.
""" 
        else:
            preamble = """
Profiling was carried out on %(sample_size)s rows in total. Rows were selected 
by sampling 1 in every %(step)s rows, starting from row %(start)s and ending at
row %(stop)s (or the last row, whichever comes sooner).
""" 
        preamble = preamble % {'sample_size': self.sample_size, 
                               'step': self.step,
                               'start': self.start,
                               'stop': self.stop}
        r += preamble
        
        # output a summary list of fields
        r += "\nFields:\n"
        for i, f in enumerate(self.fields):
            r += "%(index)s. %(field)r\n" % {'index': i+1,
                                             'field': f}

        # output results from row analyses
        for analysis in self.row_analyses:
            r += repr(analysis)

        # construct a subsection for each field
        for i, f in enumerate(self.fields):
            heading = '%(index)s. Field: %(field)r' % {'index': i+1,
                                                       'field': f}
            underline = '=' * len(heading)
            r += '\n' + heading + '\n' + underline + '\n'
            r += '\nExample data: %s\n' % ", ".join(map(repr, self.example_data[f])) 
            
            # output results of value analyses for the current field
            for analysis in self.value_analyses[f]:
                r += repr(analysis)
                
        return r


def repr_counter(counter):
    r = ", ".join('%r (%s)' % (k, counter[k]) for k in sorted(counter))
    return r
    
    
def str_counter(counter):
    r = ", ".join('%s (%s)' % (k, counter[k]) for k in sorted(counter))
    return r
    
def repr_counter_items(items):
    return ", ".join(('%r (%s)' % item for item in items))

    
def str_counter_items(items):
    return ", ".join(('%s (%s)' % item for item in items))

    
class Analysis(object):
    
    @classmethod
    def profile(cls, table, *field_selection):
        source_iterator = iter(table)
        try:
            flds = source_iterator.next()
            
            # if this is a row analyser, ignore the field selection
            if hasattr(cls, 'accept_row'):
                analysis = cls()
                for row in source_iterator:
                    analysis.accept_row(row)
                return analysis
            
            elif hasattr(cls, 'accept_value'):
            
                # convert field selection into field indices
                indices = list()
                if len(field_selection) > 0:
                    for selection in field_selection:
                        # selection could be a field name
                        if selection in flds:
                            indices.append(flds.index(selection))
                        # or selection could be a field index
                        elif isinstance(selection, int) and selection - 1 < len(flds):
                            indices.append(selection - 1) # index fields from 1, not 0
                        else:
                            # TODO error?
                            pass
                else:
                    indices = range(len(flds)) # analyse all fields
                    print indices

                # instantiate an analysis for each selected field
                analyses = [cls() for i in indices]
            
                # iterate through the data
                for row in source_iterator:
                    for i, a in zip(indices, analyses):
                        try:
                            a.accept_value(row[i])
                        except IndexError:
                            # TODO what now?
                            pass
                        
                if len(analyses) > 1:
                    # return a dictionary of analyses
                    d = dict()
                    for i, a in zip(indices, analyses):
                        d[flds[i]] = a
                    return d
                else:
                    return analyses[0] 
            
        except:
            raise
        finally:
            if hasattr(source_iterator, 'closeit'):
                source_iterator.closeit()


def rowlengths(table, limit=None):
    it = iter(table)
    it = islice(it, 1, None) # skip header row
    
        
class RowLengths(Analysis):

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


class DistinctValues(Analysis):
    """
    TODO doc me
    
    """
    limit = 20
    
    def __init__(self):
        self.counter = Counter()

    def accept_value(self, value):
        self.counter[value] += 1
        
    def __repr__(self):
        n = len(self.counter)
        
        if n <= self.limit:
            # order by count
            items = self.counter.most_common()
            r = """
Distinct values: %s
""" % repr_counter_items(items)

        else:
            # limit the number we report, so the report doesn't get filled up
            # with too many values
            most_common = self.counter.most_common(self.limit)
            least_common = self.counter.most_common()
            least_common.reverse()
            least_common = least_common[:self.limit]
            r = """
Distinct values (%s most common): %s

Distinct values (%s least common): %s
""" % (len(most_common), 
       repr_counter_items(most_common),
       len(least_common), 
       repr_counter_items(least_common))
        
        return r


class BasicStatistics(Analysis):

    def __init__(self):
        self.count = 0
        self.errors = 0
        self.sum = 0
        self.max = None
        self.min = None

    def accept_value(self, value):
        try:
            value = float(value)
        except ValueError:
            self.errors += 1
        except TypeError:
            self.errors += 1
        else:
            self.count += 1
            self.sum += value
            if self.min is None or value < self.min:
                self.min = value
            if self.max is None or value > self.max:
                self.max = value
                
    def mean(self):
        return float(self.sum) / self.count
                
    def __repr__(self):
        if self.count == 0:
            r = """
Basic statistics could not be evaluated, as there were no valid numeric values.
"""
        else:
            r = """
Basic statistics were evaluated on %s values, with %s values being excluded due 
to errors converting data values to numbers. The maximum was %s, the minimum was 
%s, the sum was %s and the mean was %s. 
""" % (self.count, self.errors, self.max, self.min, self.sum, self.mean())
        return r
    
    
# N.B., this list of date, time and datetime formats is far inferior to the set
# of formats recognised by the Python dateutil package. If you want to do any
# serious profiling of datetimes, use the DataTypes analysis from petlx (TODO) 


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


def date_or_time(value):
    formats = {date: date_formats,
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
    

class DataTypes(Analysis):
    
    def __init__(self, threshold=0.5):
        self._threshold = threshold
        self.count = 0
        self.actual_types = Counter()
        self.parse_types = Counter()
        self.combined_types = Counter()

    def accept_value(self, value):
        self.count += 1
        cls = value.__class__
        self.actual_types[cls.__name__] += 1
        self.combined_types[cls.__name__] += 1
        if isinstance(value, basestring):
            for cls in (int, float): 
                try:
                    cls(value)
                except ValueError:
                    pass
                except TypeError:
                    pass
                else:
                    self.parse_types[cls.__name__] += 1
                    self.combined_types[cls.__name__] += 1
            dt_cls = date_or_time(value)
            if dt_cls is not None:
                self.parse_types[dt_cls.__name__] += 1
                self.combined_types[dt_cls.__name__] += 1
            try:
                bool_(value)
            except ValueError:
                pass
            except TypeError:
                pass
            else:
                self.parse_types[bool.__name__] += 1
                self.combined_types[bool.__name__] += 1

    def __repr__(self):
        r = """
Actual data types: %s
""" % str_counter_items(self.actual_types.most_common())
        str_count = self.actual_types['str']
        unicode_count = self.actual_types['unicode']
        strings_count = str_count + unicode_count
        if strings_count > 0 and len(self.parse_types) > 0:
            r += """
Of %s string values (%s ASCII, %s unicode), the following data types could be 
obtained by parsing: %s
""" % (strings_count, 
       str_count, 
       unicode_count, 
       str_counter_items(self.parse_types.most_common()))
            r += """
Combined data type counts (actual + parsed): %s
""" % str_counter_items(self.combined_types.most_common())
        return r

    
# TODO string lengths, string patterns, ...


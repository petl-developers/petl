"""
TODO doc me

"""


import logging
from itertools import islice
from collections import defaultdict, Counter


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
        table_analyses = [cls() for cls, field in self._analyses if issubclass(cls, TableAnalysis)]
        field_analyses = defaultdict(list)
        for cls, field in self._analyses:
            if issubclass(cls, FieldAnalysis):
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
                analysis.accept(row)
            for field in fields:
                field_index = fields.index(field)
                try:
                    value = row[field_index]
                except IndexError:
                    pass # TODO
                else:
                    for analysis in field_analyses[field]:
                        analysis.accept(value)
        
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

        return report


class Analysis(object):
    pass


class TableAnalysis(Analysis):
    pass


class FieldAnalysis(Analysis):
    pass


class RowLengths(TableAnalysis):

    def __init__(self):
        self._row_lengths_sum = 0
        self._row_count = 0
        self._min_row_length = None
        self._max_row_length = None
        
    def accept(self, row):
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
    

class DistinctValues(FieldAnalysis):
    
    def __init__(self):
        self._counter = Counter()

    def accept(self, value):
        self._counter[value] += 1

    def report(self):
        d('build a report of distinct values')
        return ('distinct_values', self._counter)


class BasicStatistics(FieldAnalysis):

    def __init__(self):
        self._count = 0
        self._errors = 0
        self._sum = 0
        self._max = None
        self._min = None

    def accept(self, value):
        try:
            value = float(value)
        except ValueError:
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
    

class Types(FieldAnalysis):
    
    def __init__(self):
        self._actual_types = Counter()
        self._applicable_types = Counter()
        self._inferred_type = None

    def accept(self, value):
        # TODO
        pass

    def report(self):
        data = {
                'actual_types': self._actual_types,
                'applicable_types': self._applicable_types,
                'inferred_type': self._inferred_type
                }
        return ('types', data)
    

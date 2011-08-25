"""
TODO doc me

"""


from itertools import islice
from collections import defaultdict


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
        
        report = dict()
        report['table'] = dict()
        report['field'] = dict()

        it = iter(self._table)
        fields = it.next()
        report['table']['default'] = dict()
        report['table']['default']['fields'] = tuple(fields)
        for f in fields:
            report['field'][f] = dict()

        table_analyses = [cls() for cls, field in self._analyses if isinstance(cls, TableAnalysis)]
        field_analyses = defaultdict(list)
        for cls, field in self._analyses:
            if isinstance(cls, FieldAnalysis):
                if field is not None:
                    # add analysis on a single field
                    field_analyses[field].append(cls())
                else:
                    # add analysis on all fields (not field specified)
                    for field in fields:
                        field_analyses[field].append(cls())
        
        if self._max_sample_size:
            it = islice(it, 0, self._max_sample_size-1)

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
        
        report['table']['default']['sample_size'] = sample_size
        for analysis in table_analyses:
            analysis.report(report['table'])
        for field in fields:
            for analysis in field_analyses[field]:
                analysis.report(report['field'][field])

        return report


class Analysis(object):
    pass


class TableAnalysis(Analysis):
    pass


class FieldAnalysis(Analysis):
    pass


class RowLengths(TableAnalysis):

    def accept(self, row):
        pass

    def report(self, report):
        pass
    

class DistinctValues(FieldAnalysis):

    def accept(self, value):
        pass

    def report(self, report):
        pass


class BasicStatistics(FieldAnalysis):

    def accept(self, value):
        pass

    def report(self, report):
        pass
    

class Types(FieldAnalysis):

    def accept(self, value):
        pass

    def report(self, report):
        pass
    

"""
TODO doc me

"""


from itertools import islice


class Profiler(object):
    """
    TODO doc me

    """


    def __init__(self, table=None, max_sample_size=0):
        assert isinstance(max_sample_size, int), 'max_sample_size must be an int'
        self._table = table
        self._max_sample_size = max_sample_size
        self._profile_functions = list()


    def add(self, profile_function, field=None):
        self._profile_functions.append((profile_function, field))


    def profile(self):
        profile = dict()
        profile['general'] = dict()
        profile['fields'] = dict()
        it = iter(self._table)
        field_names = it.next()
        profile['general']['field_names'] = tuple(field_names)
        for f in field_names:
            profile['fields'][f] = dict()
        if self._max_sample_size:
            it = islice(it, 0, self._max_sample_size-1)
        sample_size = 0
        row_length_sum = 0
        min_row_length = None
        max_row_length = None
        for index, row in enumerate(it):
            sample_size += 1
            row_length = len(row)
            row_length_sum += row_length
            if min_row_length is None or row_length < min_row_length :
                min_row_length = row_length
            if max_row_length is None or row_length > max_row_length :
                max_row_length = row_length
        
        profile['general']['sample_size'] = sample_size
        profile['general']['min_row_length'] = min_row_length
        profile['general']['max_row_length'] = max_row_length
        profile['general']['mean_row_length'] = row_length_sum / sample_size

        return profile


def record_lengths():
    pass


def distinct_values():
    pass

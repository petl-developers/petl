"""
TODO doc me

"""
import csv
import logging
import os


logger = logging.getLogger('petl')
d, i, w, e = logger.debug, logger.info, logger.warn, logger.error # abbreviations


class ExtractCsv(object):
    
    def __init__(self, path, *args, **kwargs):
        assert os.path.isfile(path), 'file not found: %s' % path
        self._path = path
        self._args = args
        self._kwargs = kwargs
        
    def __iter__(self):
        with open(self._path, 'rb') as file:
            reader = csv.reader(file, *self._args, **self._kwargs)
            for row in reader:
                yield row
        
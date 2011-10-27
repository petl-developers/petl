"""
TODO doc me

"""


import csv
import os
import zlib


__all__ = ['fromcsv', 'frompickle', 'fromsqlite3', 'tocsv', 'topickle', 'tosqlite3']


class Uncacheable(Exception):
    pass # TODO


def crc32(file_path):
    """
    Compute the CRC32 checksum of the file at the given location. Returns
    the checksum as an integer, use hex(result) to view as hexadecimal.
    
    """
    
    checksum = None
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(8192)
            if data is None:
                break
            if checksum is None:
                checksum = zlib.crc32(data) & 0xffffffffL # deal with signed integer
            else:
                checksum = zlib.crc32(data, checksum) & 0xffffffffL # deal with signed integer
    return checksum


def adler32(file_path):
    """
    Compute the adler 32 checksum of the file at the given location. Returns
    the checksum as an integer, use hex(result) to view as hexadecimal.
    
    """
    
    checksum = None
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            if checksum is None:
                checksum = zlib.adler32(data) & 0xffffffffL # deal with signed integer
            else:
                checksum = zlib.adler32(data, checksum) & 0xffffffffL # deal with signed integer
    return checksum


def fromcsv(path, *args, **kwargs):
    """
    Wrapper for the standard `csv.reader` function. Returns a table providing
    access to the data in the given delimited file. The `path` argument is the
    path of the delimited file, all other positional and/or keyword arguments
    are passed to `csv.reader`. E.g.::

        >>> import csv
        >>> import tempfile
        >>> # set up a temporary CSV file to demonstrate with
        ... f = tempfile.NamedTemporaryFile(delete=False)
        >>> writer = csv.writer(f)
        >>> writer.writerow(['foo', 'bar'])
        >>> writer.writerow(['a', 1])
        >>> writer.writerow(['b', 2])
        >>> writer.writerow(['c', 2])
        >>> f.close()
        >>> # now demonstrate the use of petl.fromcsv
        ... from petl import fromcsv, look
        >>> table = fromcsv(f.name)
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also `petl.convert`.
    
    """

    return CSVView(path, *args, **kwargs)


class CSVView(object):
    
    def __init__(self, path, *args, **kwargs):
        self.path = path
        self.args = args
        self.kwargs = kwargs
        
    def __iter__(self):
        with open(self.path, 'rb') as file:
            reader = csv.reader(file, *self.args, **self.kwargs)
            for row in reader:
                yield row
                
    def cachetag(self):
        p = self.path
        if os.path.isfile(p):
            checksum = adler32(p)
            return hash((checksum, self.args, tuple(self.kwargs.items()))) 
        else:
            raise Uncacheable
                
    
def frompickle(path):
    """
    TODO doc me
    
    """
    

def fromsqlite3():
    """
    TODO doc me
    
    """
    

def tocsv(table, path, *args, **kwargs):
    """
    TODO doc me
    
    """
    

def topickle(table, path):
    """
    TODO doc me
    
    """
    

def tosqlite3(table):
    """
    TODO doc me
    
    """
    
    
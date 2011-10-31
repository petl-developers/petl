"""
TODO doc me

"""


import csv
import os
import zlib
import cPickle as pickle


__all__ = ['fromcsv', 'frompickle', 'fromsqlite3', 'tocsv', 'topickle', \
           'tosqlite3', 'crc32sum', 'adler32sum', 'statsum']


class Uncacheable(Exception):
    pass # TODO


def crc32sum(filename):
    """
    Compute the CRC32 checksum of the file at the given location. Returns
    the checksum as an integer, use hex(result) to view as hexadecimal.
    
    """
    
    checksum = None
    with open(filename, 'rb') as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            if checksum is None:
                checksum = zlib.crc32(data) & 0xffffffffL # deal with signed integer
            else:
                checksum = zlib.crc32(data, checksum) & 0xffffffffL # deal with signed integer
    return checksum


def adler32sum(filename):
    """
    Compute the Adler 32 checksum of the file at the given location. Returns
    the checksum as an integer, use hex(result) to view as hexadecimal.
    
    """
    
    checksum = None
    with open(filename, 'rb') as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            if checksum is None:
                checksum = zlib.adler32(data) & 0xffffffffL # deal with signed integer
            else:
                checksum = zlib.adler32(data, checksum) & 0xffffffffL # deal with signed integer
    return checksum


def statsum(filename):
    """
    Compute a crude checksum of the file by hashing the file's absolute path
    name, the file size, and the file's time of last modification. N.B., on
    some systems this will give a 1s resolution, i.e., any changes to a file
    within the same second that preserve the file size will *not* change the
    result.
    
    """
    
    return hash((os.path.abspath(filename), 
                 os.path.getsize(filename), 
                 os.path.getmtime(filename)))
        

def fromcsv(path, checksumfun=statsum, **kwargs):
    """
    Wrapper for the standard `csv.reader` function. Returns a table providing
    access to the data in the given delimited file. The `path` argument is the
    path of the delimited file, all other keyword arguments are passed to 
    `csv.reader`. E.g.::

        >>> import csv
        >>> import tempfile
        >>> # set up a temporary CSV file to demonstrate with
        ... f = tempfile.NamedTemporaryFile(delete=False)
        >>> writer = csv.writer(f, delimiter='\t')
        >>> writer.writerow(['foo', 'bar'])
        >>> writer.writerow(['a', 1])
        >>> writer.writerow(['b', 2])
        >>> writer.writerow(['c', 2])
        >>> f.close()
        >>> # now demonstrate the use of petl.fromcsv
        ... from petl import fromcsv, look
        >>> table = fromcsv(f.name, delimiter='\t')
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
    
    The returned table object implements the `cachetag` method, which by default
    uses the `statsum` function to generate a checksum of the underlying file.
    Note that `statsum` is cheap to compute but crude as it relies on file size 
    and time of modification, and on some systems this will not reveal changes 
    within the same second that preserve file size. If you need a finer level
    of granularity, use either `adler32sum` or `crc32sum` instead.
    
    """

    return CSVView(path, checksumfun=checksumfun, **kwargs)


class CSVView(object):
    
    def __init__(self, path, checksumfun=statsum, **kwargs):
        self.path = path
        self.checksumfun = checksumfun
        self.kwargs = kwargs
        
    def __iter__(self):
        with open(self.path, 'rb') as file:
            reader = csv.reader(file, **self.kwargs)
            for row in reader:
                yield row
                
    def cachetag(self):
        p = self.path
        if os.path.isfile(p):
            checksum = self.checksumfun(p)
            return hash((checksum, tuple(self.kwargs.items()))) 
        else:
            raise Uncacheable
                
    
def frompickle(path, checksumfun=statsum):
    """
    Returns a table providing access to the data pickled in the given file. The 
    rows in the table should have been pickled to the file one at a time. E.g.::

        >>> import pickle
        >>> import tempfile
        >>> # set up a temporary file to demonstrate with
        ... f = tempfile.NamedTemporaryFile(delete=False)
        >>> pickle.dump(['foo', 'bar'], f)
        >>> pickle.dump(['a', 1], f)
        >>> pickle.dump(['b', 2], f)
        >>> pickle.dump(['c', 2.5], f)
        >>> f.close()
        >>> # now demonstrate the use of petl.frompickle
        ... from petl import frompickle, look
        >>> table = frompickle(f.name)
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2.5   |
        +-------+-------+

    The returned table object implements the `cachetag` method, which by default
    uses the `statsum` function to generate a checksum of the underlying file.
    Note that `statsum` is cheap to compute but crude as it relies on file size 
    and time of modification, and on some systems this will not reveal changes 
    within the same second that preserve file size. If you need a finer level
    of granularity, use either `adler32sum` or `crc32sum` instead.
    
    """
    
    return PickleView(path, checksumfun=checksumfun)
    
    
class PickleView(object):

    def __init__(self, path, checksumfun=statsum):
        self.path = path
        self.checksumfun = checksumfun
        
    def __iter__(self):
        with open(self.path, 'rb') as file:
            try:
                while True:
                    yield pickle.load(file)
            except EOFError:
                pass
                
    def cachetag(self):
        p = self.path
        if os.path.isfile(p):
            return self.checksumfun(p)
        else:
            raise Uncacheable
    

def fromsqlite3(connection, query):
    """
    Provides access to data from an sqlite3 connection via a given query. E.g.::
    
        >>> # set up a demonstration sqlite3 database
        ... import sqlite3
        >>> data = [['a', 1],
        ...         ['b', 2],
        ...         ['c', 2.0]]
        >>> connection = sqlite3.connect(':memory:')
        >>> c = connection.cursor()
        >>> c.execute('create table foobar (foo, bar)')
        <sqlite3.Cursor object at 0xb77f56e0>
        >>> for row in data:
        ...     c.execute('insert into foobar values (?, ?)', row)
        ... 
        <sqlite3.Cursor object at 0xb77f56e0>
        <sqlite3.Cursor object at 0xb77f56e0>
        <sqlite3.Cursor object at 0xb77f56e0>
        >>> connection.commit()
        >>> c.close()
        >>> # now demonstrate the use of petl.fromsqlite3
        ... from petl import look, fromsqlite3
        >>> table = fromsqlite3(connection, 'select * from foobar')
        >>> look(table)    
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | u'a'  | 1     |
        +-------+-------+
        | u'b'  | 2     |
        +-------+-------+
        | u'c'  | 2.0   |
        +-------+-------+

    Returned table objects do not implement the cachetag method.
    """
    
    return Sqlite3View(connection, query)


class Sqlite3View(object):

    def __init__(self, connection, query):
        self.connection = connection
        self.query = query
        
    def __iter__(self):
        c = self.connection.cursor()
        c.execute(self.query)
        fields = [d[0] for d in c.description]
        yield fields
        for result in c:
            yield result
                

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
    
    
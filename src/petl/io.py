"""
TODO doc me

"""


import csv
import os
import zlib
import cPickle as pickle
import sqlite3


__all__ = ['fromcsv', 'frompickle', 'fromsqlite3', 'tocsv', 'topickle', \
           'tosqlite3', 'crc32sum', 'adler32sum', 'statsum', 'fromdb']


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


defaultsumfun = statsum
"""
Default checksum function used when generating cachetags for file-backed tables.

To change the default globally, e.g.::

    >>> import petl.io
    >>> petl.io.defaultsumfun = petl.io.adler32sum
    
"""
        

def fromcsv(filename, checksumfun=None, **kwargs):
    """
    Wrapper for the standard :func:`csv.reader` function. Returns a table providing
    access to the data in the given delimited file. The `filename` argument is the
    path of the delimited file, all other keyword arguments are passed to 
    :func:`csv.reader`. E.g.::

        >>> import csv
        >>> # set up a CSV file to demonstrate with
        ... with open('test.csv', 'wb') as f:
        ...     writer = csv.writer(f, delimiter='\\t')
        ...     writer.writerow(['foo', 'bar'])
        ...     writer.writerow(['a', 1])
        ...     writer.writerow(['b', 2])
        ...     writer.writerow(['c', 2])
        ...
        >>> # now demonstrate the use of petl.fromcsv
        ... from petl import fromcsv, look
        >>> table = fromcsv('test.csv', delimiter='\\t')
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
    need to be converted, see also :func:`convert`.
    
    The returned table object implements the `cachetag()` method.
    
    """

    return CSVView(filename, checksumfun=checksumfun, **kwargs)


class CSVView(object):
    
    def __init__(self, filename, checksumfun=None, **kwargs):
        self.filename = filename
        self.checksumfun = checksumfun
        self.kwargs = kwargs
        
    def __iter__(self):
        with open(self.filename, 'rb') as file:
            reader = csv.reader(file, **self.kwargs)
            for row in reader:
                yield row
                
    def cachetag(self):
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return hash((checksum, tuple(self.kwargs.items()))) 
        else:
            raise Uncacheable
                
    
def frompickle(filename, checksumfun=None):
    """
    Returns a table providing access to the data pickled in the given file. The 
    rows in the table should have been pickled to the file one at a time. E.g.::

        >>> import pickle
        >>> # set up a file to demonstrate with
        ... with open('test.dat', 'wb') as f:
        ...     pickle.dump(['foo', 'bar'], f)
        ...     pickle.dump(['a', 1], f)
        ...     pickle.dump(['b', 2], f)
        ...     pickle.dump(['c', 2.5], f)
        ...
        >>> # now demonstrate the use of petl.frompickle
        ... from petl import frompickle, look
        >>> table = frompickle('test.dat')
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

    The returned table object implements the `cachetag()` method.
    
    """
    
    return PickleView(filename, checksumfun=checksumfun)
    
    
class PickleView(object):

    def __init__(self, filename, checksumfun=None):
        self.filename = filename
        self.checksumfun = checksumfun
        
    def __iter__(self):
        with open(self.filename, 'rb') as file:
            try:
                while True:
                    yield pickle.load(file)
            except EOFError:
                pass
                
    def cachetag(self):
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return checksum
        else:
            raise Uncacheable
    

def fromsqlite3(filename, query, checksumfun=None):
    """
    Provides access to data from an :mod:`sqlite3` connection via a given query. E.g.::

        >>> import sqlite3
        >>> from petl import look, fromsqlite3    
        >>> # initial data
        >>> data = [['a', 1],
        ...         ['b', 2],
        ...         ['c', 2.0]]
        >>> connection = sqlite3.connect('test.db')
        >>> c = connection.cursor()
        >>> c.execute('create table foobar (foo, bar)')
        <sqlite3.Cursor object at 0x2240b90>
        >>> for row in data:
        ...     c.execute('insert into foobar values (?, ?)', row)
        ... 
        <sqlite3.Cursor object at 0x2240b90>
        <sqlite3.Cursor object at 0x2240b90>
        <sqlite3.Cursor object at 0x2240b90>
        >>> connection.commit()
        >>> c.close()
        >>> # demonstrate the petl.fromsqlite3 function
        ... table = fromsqlite3('test.db', 'select * from foobar')
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

    The returned table object implements the `cachetag()` method.

    """
    
    return Sqlite3View(filename, query, checksumfun)


class Sqlite3View(object):

    def __init__(self, filename, query, checksumfun=None):
        self.filename = filename
        self.query = query
        self.checksumfun = checksumfun
        
    def __iter__(self):
        connection = sqlite3.connect(self.filename)
        cursor = connection.execute(self.query)
        fields = [d[0] for d in cursor.description]
        yield fields
        for result in cursor:
            yield result
        connection.close()

    def cachetag(self):
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return hash((checksum, self.query))
        else:
            raise Uncacheable
                
    
def fromdb(connection, query):
    """
    Provides access to data from any DB-API 2.0 connection via a given query. 
    E.g., using `sqlite3`::

        >>> import sqlite3
        >>> from petl import look, fromdb
        >>> connection = sqlite3.connect('test.db')
        >>> table = fromdb(connection, 'select * from foobar')
        >>> look(table)
        
    E.g., using `psycopg2` (assuming you've installed it first)::
    
        >>> import psycopg2
        >>> from petl import look, fromdb
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> table = fromdb(connection, 'select * from test')
        >>> look(table)
        
    E.g., using `MySQLdb` (assuming you've installed it first)::
    
        >>> import MySQLdb
        >>> from petl import look, fromdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> table = fromdb(connection, 'select * from test')
        >>> look(table)
        
    The returned table object does not implement the `cachetag()` method.
        
    """
    
    return DbView(connection, query)


class DbView(object):

    def __init__(self, connection, query):
        self.connection = connection
        self.query = query
        
    def __iter__(self):
        cursor = self.connection.execute(self.query)
        fields = [d[0] for d in cursor.description]
        yield fields
        for result in cursor:
            yield result

    
def tocsv(table, filename, *args, **kwargs):
    """
    TODO doc me
    
    """
    

def topickle(table, filename):
    """
    TODO doc me
    
    """
    

def tosqlite3(table):
    """
    TODO doc me
    
    """
    
    
"""
Extract and load data to/from files, databases, etc.

"""


import csv
import os
import zlib
import cPickle as pickle
import sqlite3


from petl.util import data, header, fieldnames, asdict, records
from xml.etree import ElementTree
from operator import attrgetter
import json
from json.encoder import JSONEncoder
from petl.util import RowContainer
import gzip
import sys
import bz2
import zipfile
import urllib2
from contextlib import contextmanager


class Uncacheable(Exception):
    
    def __init__(self, nested=None):
        self.nested = nested


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


class FileSource(object):
    
    def __init__(self, filename, checksumfun=None):
        self.filename = filename
        self.checksumfun = checksumfun
        
    def checksum(self):        
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return checksum 
        else:
            raise Uncacheable

    def open_(self, *args):
        return open(self.filename, *args)


class GzipSource(FileSource):
    
    def __init__(self, filename, checksumfun=None):
        super(GzipSource, self).__init__(filename, checksumfun)

    def open_(self, *args):
        return gzip.open(self.filename, *args)


class BZ2Source(FileSource):

    def __init__(self, filename, checksumfun=None):
        super(BZ2Source, self).__init__(filename, checksumfun)

    def open_(self, *args):
        return bz2.BZ2File(self.filename, *args)


class ZipSource(object):
    
    def __init__(self, filename, membername):
        self.filename = filename
        self.membername = membername
        
    def checksum(self):
        try:
            zf = zipfile.ZipFile(self.filename)
            info = zf.getinfo(self.membername)
            return info.CRC
        except Exception as e:
            raise Uncacheable(e)

    def open_(self, *args):
        zf = zipfile.ZipFile(self.filename, *args)
        if args:
            return zf.open(self.membername, args[0])
        else:
            return zf.open(self.membername)


class StdinSource(object):

    def open_(self, *args):
        return sys.stdin
    

class StdoutSource(object):

    def open_(self, *args):
        return sys.stdout
    

class URLSource(object):
    
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        
    @contextmanager
    def open_(self, *args):
        try:
            f = urllib2.urlopen(*self.args, **self.kwargs) 
            yield f
        finally:
            f.close()
    
    
def _read_source_from_arg(source):
    if source is None:
        return StdinSource()
    elif isinstance(source, basestring):
        if any(map(source.startswith, ['http://', 'https://', 'ftp://'])):
            return URLSource(source)
        elif source.endswith('.gz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        return source
    
    
def _write_source_from_arg(source):
    if source is None:
        return StdoutSource()
    elif isinstance(source, basestring):
        if source.endswith('.gz'):
            return GzipSource(source)
        elif source.endswith('.bz2'):
            return BZ2Source(source)
        else:
            return FileSource(source)
    else:
        return source
    
    
def fromcsv(source=None, dialect=csv.excel, **kwargs):
    """
    Wrapper for the standard :func:`csv.reader` function. Returns a table providing
    access to the data in the given delimited file. E.g.::

        >>> import csv
        >>> # set up a CSV file to demonstrate with
        ... with open('test.csv', 'wb') as f:
        ...     writer = csv.writer(f)
        ...     writer.writerow(['foo', 'bar'])
        ...     writer.writerow(['a', 1])
        ...     writer.writerow(['b', 2])
        ...     writer.writerow(['c', 2])
        ...
        >>> # now demonstrate the use of petl.fromcsv
        ... from petl import fromcsv, look
        >>> testcsv = fromcsv('test.csv')
        >>> look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.reader`. So, e.g., to override the delimiter
    from the default CSV dialect, provide the `delimiter` keyword argument.
     
    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.
    
    The returned table object implements the `cachetag()` method. If the 
    `checksumfun` argument is not given, the default checksum function (whatever
    `petl.io.defaultsumfun` is currently set to) will be used to calculate 
    cachetag values.
    
    """

    source = _read_source_from_arg(source)
    return CSVView(source=source, dialect=dialect, **kwargs)


class CSVView(RowContainer):
    
    def __init__(self, source=None, checksumfun=None, dialect=csv.excel, **kwargs):
        self.source = source
        self.dialect = dialect
        self.kwargs = kwargs
        
    def __iter__(self):
        with self.source.open_() as f:
            reader = csv.reader(f, dialect=self.dialect, **self.kwargs)
            for row in reader:
                yield tuple(row)
                
    def cachetag(self):
        try:
            return hash((self.source.checksum(), 
                         self.dialect, 
                         tuple(self.kwargs.items()))) 
        except Exception as e:
            raise Uncacheable(e)    
                
    
def frompickle(source=None):
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
        >>> testdat = frompickle('test.dat')
        >>> look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2.5   |
        +-------+-------+

    The returned table object implements the `cachetag()` method. If the 
    `checksumfun` argument is not given, the default checksum function (whatever
    `petl.io.defaultsumfun` is currently set to) will be used to calculate 
    cachetag values.
    
    """
    
    source = _read_source_from_arg(source)
    return PickleView(source)
    
    
class PickleView(RowContainer):

    def __init__(self, source):
        self.source = source
        
    def __iter__(self):
        with self.source.open_() as f:
            try:
                while True:
                    yield tuple(pickle.load(f))
            except EOFError:
                pass
                
    def cachetag(self):
        try:
            return self.source.checksum()
        except Exception as e:
            raise Uncacheable(e)    
                

def fromsqlite3(source, query, checksumfun=None):
    """
    Provides access to data from an :mod:`sqlite3` database file via a given query. E.g.::

        >>> import sqlite3
        >>> from petl import look, fromsqlite3    
        >>> # set up a database to demonstrate with
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
        >>>
        >>> # now demonstrate the petl.fromsqlite3 function
        ... foobar = fromsqlite3('test.db', 'select * from foobar')
        >>> look(foobar)    
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | u'a'  | 1     |
        +-------+-------+
        | u'b'  | 2     |
        +-------+-------+
        | u'c'  | 2.0   |
        +-------+-------+

    The returned table object implements the `cachetag()` method. If the 
    `checksumfun` argument is not given, the default checksum function (whatever
    `petl.io.defaultsumfun` is currently set to) will be used to calculate 
    cachetag values.
    
    .. versionchanged:: 0.10.2
    
    Either a database file name or a connection object can be given as the
    first argument. (Note that `cachetag()` is only implemented if a file name
    is given.)
    
    """
    
    return Sqlite3View(source, query, checksumfun)


class Sqlite3View(RowContainer):

    def __init__(self, source, query, checksumfun=None):
        self.source = source
        self.query = query
        self.checksumfun = checksumfun
        # setup the connection
        if isinstance(self.source, basestring):
            self.connection = sqlite3.connect(self.source)
            self.connection.row_factory = sqlite3.Row
        elif isinstance(self.source, sqlite3.Connection):
            self.connection = self.source
        else:
            raise Exception('source argument must be filename or connection; found %r' % self.source)
        
    def __iter__(self):

        cursor = self.connection.cursor()
        cursor.execute(self.query)
        fields = [d[0] for d in cursor.description]
        yield tuple(fields)
        for result in cursor:
            yield result
            
        # tidy up
        cursor.close()

    def cachetag(self):
        # will only work if we've been given a reference to the file, not the connection
        p = self.source
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
        
    .. versionchanged:: 0.10.2
    
    The first argument may also be a function that creates a cursor. E.g.::
    
        >>> import psycopg2
        >>> from petl import look, fromdb
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> mkcursor = lambda: connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        >>> table = fromdb(mkcursor, 'select * from test')
        >>> look(table)
    
    N.B., each call to the function should return a new cursor.
    
    """
    
    return DbView(connection, query)


class DbView(RowContainer):

    def __init__(self, connection, query):
        self.connection = connection
        self.query = query
        
    def __iter__(self):
        # N.B., each iterator needs its own cursor so iterators are independent

        # support either connection object or cursor factory function
        if hasattr(self.connection, 'cursor') and callable(self.connection.cursor):
            cursor = self.connection.cursor()
        elif callable(self.connection):
            cursor = self.connection()
            assert hasattr(cursor, 'execute'), 'not a cursor: %r' % cursor
        else:
            raise Exception('connection arg must have cursor method or be callable')
        
        try:
            cursor.execute(self.query)
            fields = [d[0] for d in cursor.description]
            yield tuple(fields)
            for result in cursor:
                yield tuple(result)
        except:
            raise
        finally:
            cursor.close()
    
            
def fromtext(source=None, header=['lines'], strip=None):
    """
    Construct a table from lines in the given text file. E.g.::

        >>> # example data
        ... with open('test.txt', 'w') as f:
        ...     f.write('a\\t1\\n')
        ...     f.write('b\\t2\\n')
        ...     f.write('c\\t3\\n')
        ... 
        >>> from petl import fromtext, look
        >>> table1 = fromtext('test.txt')
        >>> look(table1)
        +--------------+
        | 'lines'      |
        +==============+
        | 'a\\t1'      |
        +--------------+
        | 'b\\t2'      |
        +--------------+
        | 'c\\t3'      |
        +--------------+

    The :func:`fromtext` function provides a starting point for custom handling of 
    text files. E.g., using :func:`capture`::
    
        >>> from petl import capture
        >>> table2 = capture(table1, 'lines', '(.*)\\\\t(.*)$', ['foo', 'bar'])
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '3'   |
        +-------+-------+

    .. versionchanged:: 0.4
    
    The strip() function is called on each line, which by default will remove 
    leading and trailing whitespace, including the end-of-line character - use 
    the `strip` keyword argument to specify alternative characters to strip.    
    
    """

    source = _read_source_from_arg(source)
    return TextView(source, header, strip=strip)


class TextView(RowContainer):
    
    def __init__(self, source, header=['lines'], strip=None):
        self.source = source
        self.header = header
        self.strip = strip
        
    def __iter__(self):
        with self.source.open_() as f:
            if self.header is not None:
                yield tuple(self.header)
            s = self.strip
            for line in f:
                yield (line.strip(s),)
                
    def cachetag(self):
        try:
            return hash((self.source.checksum(), 
                         tuple(self.header), 
                         self.strip)) 
        except Exception as e:
            raise Uncacheable(e)    


def fromxml(source, *args, **kwargs):
    """
    Access data in an XML file. E.g.::

        >>> from petl import fromxml, look
        >>> data = \"""<table>
        ...     <tr>
        ...         <td>foo</td><td>bar</td>
        ...     </tr>
        ...     <tr>
        ...         <td>a</td><td>1</td>
        ...     </tr>
        ...     <tr>
        ...         <td>b</td><td>2</td>
        ...     </tr>
        ...     <tr>
        ...         <td>c</td><td>2</td>
        ...     </tr>
        ... </table>\"""
        >>> with open('example1.xml', 'w') as f:    
        ...     f.write(data)
        ...     f.close()
        ... 
        >>> table1 = fromxml('example1.xml', 'tr', 'td')
        >>> look(table1)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+
        
    If the data values are stored in an attribute, provide the attribute name
    as an extra positional argument, e.g.:

        >>> data = \"""<table>
        ...     <tr>
        ...         <td v='foo'/><td v='bar'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='a'/><td v='1'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='b'/><td v='2'/>
        ...     </tr>
        ...     <tr>
        ...         <td v='c'/><td v='2'/>
        ...     </tr>
        ... </table>\"""
        >>> with open('example2.xml', 'w') as f:    
        ...     f.write(data)
        ...     f.close()
        ... 
        >>> table2 = fromxml('example2.xml', 'tr', 'td', 'v')
        >>> look(table2)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+
        
    Data values can also be extracted by providing a mapping of field names
    to element paths, e.g.::
    
        >>> data = \"""<table>
        ...     <row>
        ...         <foo>a</foo><baz><bar v='1'/><bar v='3'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>b</foo><baz><bar v='2'/></baz>
        ...     </row>
        ...     <row>
        ...         <foo>c</foo><baz><bar v='2'/></baz>
        ...     </row>
        ... </table>\"""
        >>> with open('example3.xml', 'w') as f:    
        ...     f.write(data)
        ...     f.close()
        ... 
        >>> table3 = fromxml('example3.xml', 'row', {'foo': 'foo', 'bar': ('baz/bar', 'v')})
        >>> look(table3)
        +-------+------------+
        | 'foo' | 'bar'      |
        +=======+============+
        | 'a'   | ('1', '3') |
        +-------+------------+
        | 'b'   | '2'        |
        +-------+------------+
        | 'c'   | '2'        |
        +-------+------------+

    Note that the implementation is currently *not*
    streaming, i.e., the whole document is loaded into memory.
    
    .. versionadded:: 0.4
    
    .. versionchanged:: 0.6 If multiple elements match a given field, all values are reported as a tuple.
    
    """

    source = _read_source_from_arg(source)
    return XmlView(source, *args, **kwargs)


class XmlView(RowContainer):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        if len(args) == 2 and isinstance(args[1], basestring):
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = None
        elif len(args) == 2 and isinstance(args[1], dict):
            self.rmatch = args[0]
            self.vmatch = None
            self.vdict = args[1]
            self.attr = None
        elif len(args) == 3:
            self.rmatch = args[0]
            self.vmatch = args[1]
            self.vdict = None
            self.attr = args[2]
        else:
            assert False, 'bad parameters'
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = None
        
    def __iter__(self):
        tree = ElementTree.parse(self.source.open_())
        
        if self.vmatch is not None:
            # simple case, all value paths are the same
            for rowelm in tree.iterfind(self.rmatch):
                if self.attr is None:
                    getv = attrgetter('text')
                else:
                    getv = lambda e: e.get(self.attr)
                yield tuple(getv(velm) for velm in rowelm.findall(self.vmatch))

        else:
            # difficult case, deal with different paths for each field
            fields = tuple(self.vdict.keys())
            yield fields
            vmatches = dict()
            vgetters = dict()
            for f in fields:
                vmatch = self.vdict[f]
                if isinstance(vmatch, basestring):
                    # match element path
                    vmatches[f] = vmatch
                    vgetters[f] = lambda v: tuple(e.text for e in v) if len(v) > 1 else v[0].text if len(v) == 1 else self.missing
                else:
                    # match element path and attribute name
                    vmatches[f] = vmatch[0]
                    attr = vmatch[1]
                    vgetters[f] = lambda v: tuple(e.get(attr) for e in v) if len(v) > 1 else v[0].get(attr) if len(v) == 1 else self.missing
            for rowelm in tree.iterfind(self.rmatch):
                yield tuple(vgetters[f](rowelm.findall(vmatches[f])) for f in fields)
            
                    
    def cachetag(self):
        try:
            return hash((self.source.checksum(), self.args, self.missing))
        except Exception as e:
            raise Uncacheable(e)
        

def fromjson(source, *args, **kwargs):
    """
    Extract data from a JSON file. The file must contain a JSON array as the top
    level object, and each member of the array will be treated as a row of data.
    E.g.::

        >>> from petl import fromjson, look
        >>> data = '[{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]'
        >>> with open('example1.json', 'w') as f:
        ...     f.write(data)
        ... 
        >>> table1 = fromjson('example1.json')
        >>> look(table1)
        +--------+--------+
        | u'foo' | u'bar' |
        +========+========+
        | u'a'   | 1      |
        +--------+--------+
        | u'b'   | 2      |
        +--------+--------+
        | u'c'   | 2      |
        +--------+--------+
        
    If your JSON file does not fit this structure, you will need to parse it
    via :func:`json.load` and select the array to treat as the data, see also 
    :func:`fromdicts`.

    .. versionadded:: 0.5
    
    """

    source = _read_source_from_arg(source)
    return JsonView(source, *args, **kwargs)


class JsonView(RowContainer):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.missing = None
        self.header = None
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
            del self.kwargs['missing']
        if 'header' in kwargs:
            self.header = kwargs['header']
            del self.kwargs['header']
        
    def __iter__(self):
        with self.source.open_() as f:
            result = json.load(f, *self.args, **self.kwargs)
            if self.header is None:
                # determine fields
                header = list()
                for o in result:
                    if hasattr(o, 'keys'):
                        header.extend(k for k in o.keys() if k not in header)
            else:
                header = self.header
            yield tuple(header)
            # output data rows
            for o in result:
                row = tuple(o[f] if f in o else None for f in header)
                yield row
                    
    def cachetag(self):
        try:
            return hash((self.source.checksum(), self.args, tuple(self.kwargs.items())))
        except Exception as e:
            raise Uncacheable(e)


def fromdicts(dicts, header=None):
    """
    View a sequence of Python :class:`dict` as a table. E.g.::
    
        >>> from petl import fromdicts, look
        >>> dicts = [{"foo": "a", "bar": 1}, {"foo": "b", "bar": 2}, {"foo": "c", "bar": 2}]
        >>> table = fromdicts(dicts)
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
    See also :func:`fromjson`.

    .. versionadded:: 0.5
    
    """

    return DictsView(dicts, header=header)


class DictsView(RowContainer):
    
    def __init__(self, dicts, header=None):
        self.dicts = dicts
        self.header = header
        
    def __iter__(self):
        result = self.dicts
        if self.header is None:
            # determine fields
            header = list()
            for o in result:
                if hasattr(o, 'keys'):
                    header.extend(k for k in o.keys() if k not in header)
        else:
            header = self.header
        yield tuple(header)
        # output data rows
        for o in result:
            row = tuple(o[f] if f in o else None for f in header)
            yield row
                    
    def cachetag(self):
        raise Uncacheable


def tocsv(table, source=None, dialect=csv.excel, **kwargs):
    """
    Write the table to a CSV file. E.g.::

        >>> from petl import tocsv, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> tocsv(table, 'test.csv')
        >>> # look what it did
        ... from petl import fromcsv
        >>> look(fromcsv('test.csv'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.writer`. So, e.g., to override the delimiter
    from the default CSV dialect, provide the `delimiter` keyword argument.
     
    Note that if a file already exists at the given location, it will be overwritten.
    
    """
    
    source = _write_source_from_arg(source)
    with source.open_('wb') as f:
        writer = csv.writer(f, dialect=dialect, **kwargs)
        for row in table:
            writer.writerow(row)


def appendcsv(table, source=None, dialect=csv.excel, **kwargs):
    """
    Append data rows to an existing CSV file. E.g.::

        >>> # look at an existing CSV file
        ... from petl import look, fromcsv
        >>> testcsv = fromcsv('test.csv')
        >>> look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+
        
        >>> # append some data
        ... look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+
        
        >>> from petl import appendcsv 
        >>> appendcsv(table, 'test.csv')
        >>> # look what it did
        ... look(testcsv)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+
        | 'd'   | '7'   |
        +-------+-------+
        | 'e'   | '42'  |
        +-------+-------+
        | 'f'   | '12'  |
        +-------+-------+

    The `filename` argument is the path of the delimited file, all other keyword
    arguments are passed to :func:`csv.writer`. So, e.g., to override the delimiter
    from the default CSV dialect, provide the `delimiter` keyword argument.
     
    Note that no attempt is made to check that the fields or row lengths are 
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.
    
    """
    
    source = _write_source_from_arg(source)
    with source.open_('ab') as f:
        writer = csv.writer(f, dialect=dialect, **kwargs)
        for row in data(table):
            writer.writerow(row)


def topickle(table, source=None, protocol=-1):
    """
    Write the table to a pickle file. E.g.::

        >>> from petl import topickle, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> topickle(table, 'test.dat')
        >>> # look what it did
        ... from petl import frompickle
        >>> look(frompickle('test.dat'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

    Note that if a file already exists at the given location, it will be overwritten.

    The pickle file format preserves type information, i.e., reading and writing 
    is round-trippable.
    
    """
    
    source = _write_source_from_arg(source)
    with source.open_('wb') as f:
        for row in table:
            pickle.dump(row, f, protocol)
    

def appendpickle(table, source=None, protocol=-1):
    """
    Append data to an existing pickle file. E.g.::

        >>> from petl import look, frompickle
        >>> # inspect an existing pickle file
        ... testdat = frompickle('test.dat')
        >>> look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> # append some data
        ... from petl import appendpickle
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+
        
        >>> appendpickle(table, 'test.dat')
        >>> # look what it did
        ... look(testdat)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 42    |
        +-------+-------+
        | 'f'   | 12    |
        +-------+-------+

    Note that no attempt is made to check that the fields or row lengths are 
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.
    
    """
    
    source = _write_source_from_arg(source)
    with source.open_('ab') as f:
        for row in data(table):
            pickle.dump(row, f, protocol)
    

def tosqlite3(table, filename_or_connection, tablename, create=True, commit=True):
    """
    Load data into a table in an :mod:`sqlite3` database. Note that if
    the database table exists, it will be truncated, i.e., all
    existing rows will be deleted prior to inserting the new
    data. E.g.::

        >>> from petl import tosqlite3, look
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> # by default, if the table does not already exist, it will be created
        ... tosqlite3(table, 'test.db', 'foobar')
        >>> # look what it did
        ... from petl import fromsqlite3
        >>> look(fromsqlite3('test.db', 'select * from foobar'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | u'a'  | 1     |
        +-------+-------+
        | u'b'  | 2     |
        +-------+-------+
        | u'c'  | 2     |
        +-------+-------+

    .. versionchanged:: 0.10.2
    
    Either a database file name or a connection object can be given as the
    second argument. (Note that `cachetag()` is only implemented if a file name
    is given.)

    """
    
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]

    if isinstance(filename_or_connection, basestring):
        conn = sqlite3.connect(filename_or_connection)
    elif isinstance(filename_or_connection, sqlite3.Connection):
        conn = filename_or_connection
    else:
        raise Exception('filename_or_connection argument must be filename or connection; found %r' % filename_or_connection)
    
    cursor = conn.cursor()
    
    if create: # force table creation
        cursor.execute('DROP TABLE IF EXISTS %s' % tablename)
        cursor.execute('CREATE TABLE %s (%s)' % (tablename, ', '.join(names)))
    
    # truncate table
    cursor.execute('DELETE FROM %s' % tablename)
    
    placeholders = ', '.join(['?'] * len(names))
    _insert(cursor, tablename, placeholders, table)

    # tidy up
    cursor.close()
    if commit:
        conn.commit()

    return conn # in case people want to close it
    
    
def appendsqlite3(table, filename_or_connection, tablename, commit=True):
    """
    Load data into an existing table in an :mod:`sqlite3`
    database. Note that the database table will be appended, i.e., the
    new data will be inserted into the table, and any existing rows
    will remain. E.g.::
    
        >>> from petl import appendsqlite3, look
        >>> look(moredata)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'd'   | 7     |
        +-------+-------+
        | 'e'   | 9     |
        +-------+-------+
        | 'f'   | 1     |
        +-------+-------+
        
        >>> appendsqlite3(moredata, 'test.db', 'foobar') 
        >>> # look what it did
        ... from petl import look, fromsqlite3
        >>> look(fromsqlite3('test.db', 'select * from foobar'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | u'a'  | 1     |
        +-------+-------+
        | u'b'  | 2     |
        +-------+-------+
        | u'c'  | 2     |
        +-------+-------+
        | u'd'  | 7     |
        +-------+-------+
        | u'e'  | 9     |
        +-------+-------+
        | u'f'  | 1     |
        +-------+-------+

    .. versionchanged:: 0.10.2
    
    Either a database file name or a connection object can be given as the
    second argument. (Note that `cachetag()` is only implemented if a file name
    is given.)

    """

    # sanitise table name
    tablename = _quote(tablename)

    if isinstance(filename_or_connection, basestring):
        conn = sqlite3.connect(filename_or_connection)
    elif isinstance(filename_or_connection, sqlite3.Connection):
        conn = filename_or_connection
    else:
        raise Exception('filename_or_connection argument must be filename or connection; found %r' % filename_or_connection)

    cursor = conn.cursor()
    
    flds = header(table) # just need to know how many fields there are
    placeholders = ', '.join(['?'] * len(flds))
    _insert(cursor, tablename, placeholders, table)

    # tidy up
    cursor.close()
    if commit:
        conn.commit()

    return conn # in case people want to close it
    
    
def todb(table, connection_or_cursor, tablename, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection or cursor. Note that the database table will be truncated, 
    i.e., all existing rows will be deleted prior to inserting the new data.
    
    E.g.::

        >>> from petl import look, todb
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
    ... using :mod:`sqlite3`::
    
        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    
        
    ... using :mod:`psycopg2`::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    
        
    ... using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    

    .. versionchanged:: 0.10.2
    
    A cursor can also be provided instead of a connection, e.g.::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> cursor = connection.cursor()
        >>> todb(table, cursor, 'foobar')    
    
    """

    # deal with polymorphic arg
    if hasattr(connection_or_cursor, 'cursor'):
        connection = connection_or_cursor
        cursor = connection.cursor()
    elif hasattr(connection_or_cursor, 'execute'):
        cursor = connection_or_cursor
        if hasattr(cursor, 'connection'):
            connection = cursor.connection
        else:
            connection = None
    else:
        raise Exception('expected connection or cursor, found: %r' % connection_or_cursor)
            
    # sanitise table and field names
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]
    placeholders = _placeholders(connection, names)

    try:
        # truncate the table
        query = 'TRUNCATE %s' % tablename
        cursor.execute(query)
    except:
        # TRUNCATE is not supported in some databases
        query = 'DELETE FROM %s' % tablename
        cursor.execute(query)
    
    # insert some data
    _insert(cursor, tablename, placeholders, table)

    # finish up
    if hasattr(connection_or_cursor, 'cursor'):
        # close the cursor we created
        cursor.close()
    if commit and connection is not None:
        connection.commit()
    
    
def appenddb(table, connection_or_cursor, tablename, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection or cursor. Note that the database table will be appended, 
    i.e., the new data will be inserted into the table, and any existing
    rows will remain.
    
    E.g.::
    
        >>> from petl import look, appenddb
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+

    ... using :mod:`sqlite3`::
    
        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    ... using :mod:`psycopg2`::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    ... using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    .. versionchanged:: 0.10.2
    
    A cursor can also be provided instead of a connection, e.g.::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> cursor = connection.cursor()
        >>> appenddb(table, cursor, 'foobar')    
    
    """

    # deal with polymorphic arg
    if hasattr(connection_or_cursor, 'cursor'):
        connection = connection_or_cursor
        cursor = connection.cursor()
    elif hasattr(connection_or_cursor, 'execute'):
        cursor = connection_or_cursor
        if hasattr(cursor, 'connection'):
            connection = cursor.connection
        else:
            connection = None
    else:
        raise Exception('expected connection or cursor, found: %r' % connection_or_cursor)
            
    # sanitise table and field names
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]
    placeholders = _placeholders(connection, names)
    
    # insert some data
    _insert(cursor, tablename, placeholders, table)

    # finish up
    if hasattr(connection_or_cursor, 'cursor'):
        # close the cursor we created
        cursor.close()
    if commit and connection is not None:
        connection.commit()


def _quote(s):
    # crude way to sanitise table and field names
    return '`%s`' % s.replace('`', '')


def _insert(cursor, tablename, placeholders, table):    
    insertquery = 'INSERT INTO %s VALUES (%s)' % (tablename, placeholders)
    for row in data(table):
        cursor.execute(insertquery, row)

    
def _placeholders(connection, names):    
    # discover the paramstyle
    mod = __import__(connection.__class__.__module__)
    if mod.paramstyle == 'qmark':
        placeholders = ', '.join(['?'] * len(names))
    elif mod.paramstyle in ('format', 'pyformat'):
        # TODO test this!
        placeholders = ', '.join(['%s'] * len(names))
    elif mod.paramstyle == 'numeric':
        # TODO test this!
        placeholders = ', '.join([':' + str(i + 1) for i in range(len(names))])
    else:
        raise Exception('TODO')
    return placeholders


def totext(table, source=None, template=None, prologue=None, epilogue=None):
    """
    Write the table to a text file. E.g.::

        >>> from petl import totext, look    
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> prologue = \"\"\"{| class="wikitable"
        ... |-
        ... ! foo
        ... ! bar
        ... \"\"\"
        >>> template = \"\"\"|-
        ... | {foo}
        ... | {bar}
        ... \"\"\"
        >>> epilogue = "|}"
        >>> totext(table, 'test.txt', template, prologue, epilogue)
        >>> 
        >>> # see what we did
        ... with open('test.txt') as f:
        ...     print f.read()
        ...     
        {| class="wikitable"
        |-
        ! foo
        ! bar
        |-
        | a
        | 1
        |-
        | b
        | 2
        |-
        | c
        | 2
        |}
        
    The `template` will be used to format each row via `str.format <http://docs.python.org/library/stdtypes.html#str.format>`_.

    """
    
    source = _write_source_from_arg(source)
    with source.open_('wb') as f:
        if prologue is not None:
            f.write(prologue)
        it = iter(table)
        flds = it.next()
        for row in it:
            rec = asdict(flds, row)
            s = template.format(**rec)
            f.write(s)
        if epilogue is not None:
            f.write(epilogue)
            
    
def appendtext(table, source=None, template=None, prologue=None, epilogue=None):
    """
    As :func:`totext` but the file is opened in append mode.
    
    """

    source = _write_source_from_arg(source)
    with source.open_('ab') as f:
        if prologue is not None:
            f.write(prologue)
        it = iter(table)
        flds = it.next()
        for row in it:
            rec = asdict(flds, row)
            s = template.format(**rec)
            f.write(s)
        if epilogue is not None:
            f.write(epilogue)
            
            
def tojson(table, source=None, *args, **kwargs):
    """
    Write a table in JSON format. E.g.::

        >>> from petl import tojson, look    
        >>> look(table)
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | 1     |
        +-------+-------+
        | 'b'   | 2     |
        +-------+-------+
        | 'c'   | 2     |
        +-------+-------+
        
        >>> tojson(table, 'example.json')
        >>> # check what it did
        ... import json
        >>> with open('example.json') as f:
        ...     json.load(f)
        ... 
        [{u'foo': u'a', u'bar': 1}, {u'foo': u'b', u'bar': 2}, {u'foo': u'c', u'bar': 2}]
    
    Note that this is currently not streaming, all data is loaded into memory
    before being written to the file.
    
    .. versionadded:: 0.5
    
    """
    
    encoder = JSONEncoder(*args, **kwargs)
    source = _write_source_from_arg(source)
    with source.open_('wb') as f:
        for chunk in encoder.iterencode(list(records(table))):
            f.write(chunk)
            

def fromtsv(source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`fromcsv` but with different default dialect
    (tab delimited).
    
    .. versionadded:: 0.9
        
    """
    
    return fromcsv(source, dialect=dialect, **kwargs)


def totsv(table, source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`tocsv` but with different default dialect
    (tab delimited).
    
    .. versionadded:: 0.9
        
    """    

    return tocsv(table, source=source, dialect=dialect, **kwargs)


def appendtsv(table, source=None, dialect=csv.excel_tab, **kwargs):
    """
    Convenience function, as :func:`appendcsv` but with different default dialect
    (tab delimited).
    
    .. versionadded:: 0.9
        
    """    

    return appendcsv(table, source=source, dialect=dialect, **kwargs)



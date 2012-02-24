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
from petl.base import RowContainer

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
        >>> testcsv = fromcsv('test.csv', delimiter='\\t')
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

    Note that all data values are strings, and any intended numeric values will
    need to be converted, see also :func:`convert`.
    
    The returned table object implements the `cachetag()` method. If the 
    `checksumfun` argument is not given, the default checksum function (whatever
    `petl.io.defaultsumfun` is currently set to) will be used to calculate 
    cachetag values.
    
    """

    return CSVView(filename, checksumfun=checksumfun, **kwargs)


class CSVView(RowContainer):
    
    def __init__(self, filename, checksumfun=None, **kwargs):
        self.filename = filename
        self.checksumfun = checksumfun
        self.kwargs = kwargs
        
    def __iter__(self):
        with open(self.filename, 'rb') as file:
            reader = csv.reader(file, **self.kwargs)
            for row in reader:
                yield tuple(row)
                
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
    
    return PickleView(filename, checksumfun=checksumfun)
    
    
class PickleView(RowContainer):

    def __init__(self, filename, checksumfun=None):
        self.filename = filename
        self.checksumfun = checksumfun
        
    def __iter__(self):
        with open(self.filename, 'rb') as file:
            try:
                while True:
                    yield tuple(pickle.load(file))
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
    Provides access to data from an :mod:`sqlite3` database file via a given query. E.g.::

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
    
    """
    
    return Sqlite3View(filename, query, checksumfun)


class Sqlite3View(RowContainer):

    def __init__(self, filename, query, checksumfun=None):
        self.filename = filename
        self.query = query
        self.checksumfun = checksumfun
        
    def __iter__(self):
        connection = sqlite3.connect(self.filename)
        cursor = connection.execute(self.query)
        fields = [d[0] for d in cursor.description]
        yield tuple(fields)
        for result in cursor:
            yield tuple(result)
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


class DbView(RowContainer):

    def __init__(self, connection, query):
        self.connection = connection
        self.query = query
        
    def __iter__(self):
        cursor = self.connection.execute(self.query)
        fields = [d[0] for d in cursor.description]
        yield tuple(fields)
        for result in cursor:
            yield tuple(result)
            
            
def fromtext(filename, header=['lines'], strip=None, checksumfun=None):
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
        | 'a\\t1'     |
        +--------------+
        | 'b\\t2'     |
        +--------------+
        | 'c\\t3'     |
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

    return TextView(filename, header, strip=strip, checksumfun=checksumfun)


class TextView(RowContainer):
    
    def __init__(self, filename, header=['lines'], strip=None, checksumfun=None):
        self.filename = filename
        self.header = header
        self.strip = strip
        self.checksumfun = checksumfun
        
    def __iter__(self):
        with open(self.filename, 'rU') as file:
            if self.header is not None:
                yield tuple(self.header)
            s = self.strip
            for line in file:
                yield (line.strip(s),)
                
    def cachetag(self):
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return checksum
        else:
            raise Uncacheable


def fromxml(filename, *args, **kwargs):
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

    return XmlView(filename, *args, **kwargs)


class XmlView(RowContainer):
    
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
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
        if 'checksumfun' in kwargs:
            self.checksumfun = kwargs['checksumfun']
        else:
            self.checksumfun = None
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = None
        
    def __iter__(self):
        tree = ElementTree.parse(self.filename)
        
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
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return hash((checksum, self.args))
        else:
            raise Uncacheable


def fromjson(filename, *args, **kwargs):
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

    return JsonView(filename, *args, **kwargs)


class JsonView(RowContainer):
    
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
        self.args = args
        self.kwargs = kwargs
        self.checksumfun = None
        self.missing = None
        self.header = None
        if 'checksumfun' in kwargs:
            self.checksumfun = kwargs['checksumfun']
            del self.kwargs['checksumfun']
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
            del self.kwargs['missing']
        if 'header' in kwargs:
            self.header = kwargs['header']
            del self.kwargs['header']
        
    def __iter__(self):
        with open(self.filename) as f:
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
        p = self.filename
        if os.path.isfile(p):
            sumfun = self.checksumfun if self.checksumfun is not None else defaultsumfun
            checksum = sumfun(p)
            return hash((checksum, self.args, tuple(self.kwargs.items())))
        else:
            raise Uncacheable


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


def tocsv(table, filename, **kwargs):
    """
    Write the table to a CSV file. E.g.::

        >>> from petl import tocsv
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> tocsv(table, 'test.csv', delimiter='\\t')
        >>> # look what it did
        ... from petl import look, fromcsv
        >>> look(fromcsv('test.csv', delimiter='\\t'))
        +-------+-------+
        | 'foo' | 'bar' |
        +=======+=======+
        | 'a'   | '1'   |
        +-------+-------+
        | 'b'   | '2'   |
        +-------+-------+
        | 'c'   | '2'   |
        +-------+-------+

    Note that if a file already exists at the given location, it will be overwritten.
    
    """
    
    with open(filename, 'wb') as f:
        writer = csv.writer(f, **kwargs)
        for row in table:
            writer.writerow(row)


def appendcsv(table, filename, **kwargs):
    """
    Append data rows to an existing CSV file. E.g.::

        >>> # look at an existing CSV file
        ... from petl import look, fromcsv
        >>> testcsv = fromcsv('test.csv', delimiter='\\t')
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
        ... from petl import appendcsv 
        >>> table = [['foo', 'bar'],
        ...          ['d', 7],
        ...          ['e', 42],
        ...          ['f', 12]]
        >>> appendcsv(table, 'test.csv', delimiter='\\t')
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

    Note that no attempt is made to check that the fields or row lengths are 
    consistent with the existing data, the data rows from the table are simply
    appended to the file. See also the :func:`cat` function.
    
    """
    
    with open(filename, 'ab') as f:
        writer = csv.writer(f, **kwargs)
        for row in data(table):
            writer.writerow(row)


def topickle(table, filename, protocol=-1):
    """
    Write the table to a pickle file. E.g.::

        >>> from petl import topickle
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> topickle(table, 'test.dat')
        >>> # look what it did
        ... from petl import look, frompickle
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
    
    with open(filename, 'wb') as file:
        for row in table:
            pickle.dump(row, file, protocol)
    

def appendpickle(table, filename, protocol=-1):
    """
    Append data to an existing pickle file. E.g.::

        >>> # inspect an existing pickle file
        ... from petl import look, frompickle
        >>> testdat = frompickle('test.dat')
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
        >>> table = [['foo', 'bar'],
        ...          ['d', 7],
        ...          ['e', 42],
        ...          ['f', 12]]
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
    
    with open(filename, 'ab') as file:
        for row in data(table):
            pickle.dump(row, file, protocol)
    

def tosqlite3(table, filename, tablename, create=True):
    """
    Load data into a table in an :mod:`sqlite3` database. Note that if
    the database table exists, it will be truncated, i.e., all
    existing rows will be deleted prior to inserting the new
    data. E.g.::

        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import tosqlite3
        >>> # by default, if the table does not already exist, it will be created
        ... tosqlite3(table, 'test.db', 'foobar')
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

    """
    
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]

    conn = sqlite3.connect(filename)
    if create:
        conn.execute('create table if not exists %s (%s)' % (tablename, ', '.join(names)))
    conn.execute('delete from %s' % tablename)
    placeholders = ', '.join(['?'] * len(names))
    _insert(conn, tablename, placeholders, table)
    conn.commit()
    
    
def appendsqlite3(table, filename, tablename):
    """
    Load data into an existing table in an :mod:`sqlite3`
    database. Note that the database table will be appended, i.e., the
    new data will be inserted into the table, and any existing rows
    will remain. E.g.::
    
        >>> moredata = [['foo', 'bar'],
        ...             ['d', 7],
        ...             ['e', 9],
        ...             ['f', 1]]
        >>> from petl import appendsqlite3
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

    """

    # sanitise table name
    tablename = _quote(tablename)

    conn = sqlite3.connect(filename)
    flds = header(table) # just need to know how many fields there are
    placeholders = ', '.join(['?'] * len(flds))
    _insert(conn, tablename, placeholders, table)
    conn.commit()
    
    
    
def todb(table, connection, tablename, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection. Note that the database table will be truncated, i.e.,
    all existing rows will be deleted prior to inserting the new data.
    
    E.g., using :mod:`sqlite3`::
    
        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import todb
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    
        
    E.g., using :mod:`psycopg2`::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import todb
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    
        
    E.g., using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import todb
        >>> # assuming table "foobar" already exists in the database
        ... todb(table, connection, 'foobar')    
        
    """

    # sanitise table and field names
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]
    placeholders = _placeholders(connection, names)

    # truncate the table
    c = connection.cursor()
    c.execute('delete from %s' % tablename)
    
    # insert some data
    _insert(c, tablename, placeholders, table)

    # finish up
    if commit:
        connection.commit()
    c.close()
    
    
def appenddb(table, connection, tablename, commit=True):
    """
    Load data into an existing database table via a DB-API 2.0
    connection. Note that the database table will be appended, i.e.,
    the new data will be inserted into the table, and any existing
    rows will remain.
    
    E.g., using :mod:`sqlite3`::
    
        >>> import sqlite3
        >>> connection = sqlite3.connect('test.db')
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import appenddb
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    E.g., using :mod:`psycopg2`::

        >>> import psycopg2 
        >>> connection = psycopg2.connect("dbname=test user=postgres")
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import appenddb
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    E.g., using :mod:`MySQLdb`::

        >>> import MySQLdb
        >>> connection = MySQLdb.connect(passwd="moonpie", db="thangs")
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
        >>> from petl import appenddb
        >>> # assuming table "foobar" already exists in the database
        ... appenddb(table, connection, 'foobar')    
        
    """

    # sanitise table and field names
    tablename = _quote(tablename)
    names = [_quote(n) for n in fieldnames(table)]
    placeholders = _placeholders(connection, names)
    
    # insert some data
    c = connection.cursor()
    _insert(c, tablename, placeholders, table)

    # finish up
    if commit:
        connection.commit()
    c.close()


def _quote(s):
    # crude way to sanitise table and field names
    return '"%s"' % s.replace('"', '')


def _insert(cursor, tablename, placeholders, table):    
    insertquery = 'insert into %s values (%s)' % (tablename, placeholders)
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


def totext(table, filename, template, prologue=None, epilogue=None):
    """
    Write the table to a text file. E.g.::

        >>> from petl import totext    
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
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
    
    with open(filename, 'w') as f:
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
            
    
def appendtext(table, filename, template, prologue=None, epilogue=None):
    """
    As :func:`totext` but the file is opened in append mode.
    
    """

    with open(filename, 'a') as f:
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
            
            
def tojson(table, filename, *args, **kwargs):
    """
    Write a table in JSON format. E.g.::

        >>> from petl import tojson    
        >>> table = [['foo', 'bar'],
        ...          ['a', 1],
        ...          ['b', 2],
        ...          ['c', 2]]
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
    with open(filename, 'w') as f:
        for chunk in encoder.iterencode(list(records(table))):
            f.write(chunk)
            


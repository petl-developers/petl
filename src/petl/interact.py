"""
TODO doc me

"""


from itertools import islice
from collections import defaultdict

from petl.prettytable import PrettyTable
from petl.util import closeit


def look(table, start=1, stop=21, step=1):
    
    table_iterator = iter(table)
    try:
        fields = table_iterator.next()
        pretty = PrettyTable()
        pretty.field_names = fields
        for row in islice(table_iterator, start - 1, stop - 1, step):
            # need to normalize row length, otherwise prettytable chokes
            difference = len(fields) - len(row)
            if difference < 0:
                row = row[:len(fields)]
            elif difference > 0:
                for i in xrange(difference):
                    row.append(Ellipsis)
            pretty.add_row(row)
        pretty.printt()
    except:
        raise
    finally:
        closeit(table_iterator)
        

def see(source, limit=20):
    it = iter(source)
    try:
        fields = it.next()
        data = defaultdict(list)
        indices = [fields.index(f) for f in fields]
        for row in islice(it, limit):
            for f, i in zip(fields, indices):
                try:
                    data[f].append(repr(row[i]))
                except IndexError:
                    data[f].append(repr(Ellipsis))
        closeit(it)
        for f in fields:
            print '%r: %s' % (f, ', '.join(data[f]))
    except:
        raise
    finally:
        closeit(it)
    


    
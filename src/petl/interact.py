"""
TODO doc me

"""


from prettytable import PrettyTable
from itertools import islice


def look(table, start=1, stop=10, step=1):
    
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
        if hasattr(table_iterator, 'close'):
            table_iterator.close()
    
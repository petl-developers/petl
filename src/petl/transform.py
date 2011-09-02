"""
TODO doc me

"""


class Cut(object):
    """
    TODO doc me
    
    """
    
    def __init__(self, table, *args):
        self.table = table
        self.args = args
    
    def __iter__(self):
        indexes = list()
        it = iter(self.table)
        try:
            fields = it.next()
            
            # convert arguments into field indexes
            for a in self.args:
                # argument could be a field name
                if a in fields:
                    indexes.append(fields.index(a))
                # or argument could be a field index
                elif isinstance(a, int) and a - 1 < len(fields):
                    indexes.append(a - 1) # index fields from 1

            # construct the transformed fields
            yield [fields[i] for i in indexes]
            # construct the transformed data
            for row in it:
                yield [row[i] if i < len(row) else Ellipsis for i in indexes]

        except:
            raise
        finally:
            # make sure the iterator is closed
            if hasattr(it, 'close'):
                it.close()


class Cat(object):
    """
    TODO doc me
    
    """
    
    def __init__(self, *tables, **kwargs):
        self.tables = tables
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
    
    def __iter__(self):
        
        iters = [iter(t) for t in self.tables]
        try:
            # build field names
            in_fieldses = [it.next() for it in iters]
            out_fields = list()
            for fields in in_fieldses:
                for f in fields:
                    if f not in out_fields:
                        out_fields.append(f)
            yield out_fields
            # now build data
            for i, it in enumerate(iters):
                fields = in_fieldses[i]
                def value(row, f):
                    if f in fields:
                        value = row[fields.index(f)]
                    else:
                        value = self.missing
                    return value
                for row in it:
                    out_row = [value(row, f) for f in out_fields]
                    yield out_row
        except:
            raise
        finally:
            # make sure the iterator is closed
            if hasattr(it, 'close'):
                it.close()
        

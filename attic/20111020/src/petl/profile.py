"""
TODO doc me

"""


import logging
from itertools import islice
from collections import Counter


from petl.util import closeit


logger = logging.getLogger('petl')
d, i, w, e = logger.debug, logger.info, logger.warn, logger.error # abbreviations


def rowlengths(table, limit=None):
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        it = islice(it, 1, limit) # skip header row
        counter = Counter()
        for row in it:
            counter[len(row)] += 1
        output = [('length', 'count')]
        output.extend(counter.most_common())
        closeit(it)
        return output
    except:
        raise
    finally:
        closeit(it)


def values(table, field, limit=None):    
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        if limit is not None:
            it = islice(it, 0, limit - 1) # index rows from 1
        counter = Counter()
        for row in it:
            counter[row[field_index]] += 1
        output = [('value', 'count')]
        output.extend(counter.most_common())
        closeit(it)
        return output
    except:
        raise
    finally:
        closeit(it)
        
        
def valueset(table, field, limit=None):
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        if limit is not None:
            it = islice(it, 0, limit - 1) # index rows from 1
        vals = set()
        for row in it:
            vals.add(row[field_index])
        closeit(it)
        return vals
    except:
        raise
    finally:
        closeit(it)
        
        
def unique(table, field):
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        vals = set()
        unique = True
        for row in it:
            val = row[field_index]
            if val in vals:
                unique = False
                break
            else:
                vals.add(val)
        closeit(it)
        return unique
    except:
        raise
    finally:
        closeit(it)
        

def types(table, field, limit=None):    
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        if limit is not None:
            it = islice(it, 0, limit - 1) # index rows from 1
        counter = Counter()
        for row in it:
            counter[row[field_index].__class__.__name__] += 1
        output = [('type', 'count')]
        output.extend(counter.most_common())
        closeit(it)
        return output
    except:
        raise
    finally:
        closeit(it)


def parsetypes(table, field, parsers={'int': int, 'float': float}, limit=None):    
    """
    TODO doc me
    
    """
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        if limit is not None:
            it = islice(it, 0, limit - 1) # index rows from 1
        counter = Counter()
        for row in it:
            value = row[field_index]
            if isinstance(value, basestring):
                for name, parser in parsers.items():
                    try:
                        parser(value)
                    except ValueError:
                        pass
                    except TypeError:
                        pass
                    else:
                        counter[name] += 1
        output = [('type', 'count')]
        output.extend(counter.most_common())
        closeit(it)
        return output
    except:
        raise
    finally:
        closeit(it)


def stats(table, field, limit=None):
    """
    TODO doc me
    
    """
    output = {'min': None, 
              'max': None,
              'sum': None, 
              'mean': None, 
              'count': 0, 
              'errors': 0}
    it = iter(table)
    try:
        fields = it.next()
        assert field in fields, 'field not found: %s' % field
        field_index = fields.index(field)
        if limit is not None:
            it = islice(it, 0, limit - 1) # index rows from 1
        for row in it:
            value = row[field_index]
            try:
                value = float(value)
            except:
                output['errors'] += 1
            else:
                if output['min'] is None or value < output['min']:
                    output['min'] = value
                if output['max'] is None or value > output['max']:
                    output['max'] = value
                if output['sum'] is None:
                    output['sum'] = value
                else:
                    output['sum'] += value
                output['count'] += 1
        if output['count'] > 0:
            output['mean'] = output['sum'] / output['count']
        closeit(it)
        return output
    except:
        raise
    finally:
        closeit(it)
        
# TODO string lengths, string patterns, ...


"""
TODO doc me

"""


import logging
from itertools import islice
from collections import Counter
from datetime import datetime


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
        

# N.B., this list of date, time and datetime formats is far inferior to the set
# of formats recognised by the Python dateutil package. If you want to do any
# serious profiling of datetimes, use the DataTypes analysis from petlx (TODO) 


date_formats = {
                '%x',
                '%d/%m/%y',
                '%d/%m/%Y',
                '%d %b %y',
                '%d %b %Y',
                '%d. %b. %Y',
                '%d %B %Y',
                '%d. %B %Y',
                '%a %d %b %y',
                '%a %d/%b %y',
                '%a %d %B %Y',
                '%A %d %B %Y',
                '%m-%d',
                '%y-%m-%d',
                '%Y-%m-%d',
                '%m/%y',
                '%d/%b',
                '%m/%d/%y',
                '%m/%d/%Y',
                '%b %d, %y',
                '%b %d, %Y',
                '%B %d, %Y',
                '%a, %b %d, %y',
                '%a, %B %d, %Y',
                '%A, %d. %B %Y'
                }


time_formats = {
                '%X',
                '%H:%M',
                '%H:%M:%S',
                '%I:%M %p',
                '%I:%M:%S %p',
                '%M:%S.%f',
                '%H:%M:%S.%f',
                '%I:%M:%S.%f %p',
                '%I:%M%p',
                '%I:%M:%S%p',
                '%I:%M:%S.%f%p'
                }


def parsedate(value):
    for fmt in date_formats:
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except:
            pass
    raise ValueError('could not parse value as date: %s' % value)


def parsetime(value):
    value = value.strip()
    for fmt in time_formats:
        try:
            return datetime.strptime(value.strip(), fmt).time()
        except:
            pass
    raise ValueError('could not parse value as time: %s' % value)


def datetimeparser(format):
    def parser(value):
        return datetime.strptime(value.strip(), format)
    

def dateparser(format):
    def parser(value):
        return datetime.strptime(value.strip(), format).date()
    

def timeparser(format):
    def parser(value):
        return datetime.strptime(value.strip(), format).time()
    

true_strings = {'true', 't', 'yes', 'y', '1'}
false_strings = {'false', 'f', 'no', 'n', '0'}


def parsebool(value):
    value = value.strip().lower()
    if value in true_strings:
        return True
    elif value in false_strings:
        return False
    else:
        raise ValueError('value is not one of recognised boolean strings: %s' % value)
    

def boolparser(true_strings=true_strings, false_strings=false_strings):
    def parser(value):
        value = value.strip().lower()
        if value in true_strings:
            return True
        elif value in false_strings:
            return False
        else:
            raise ValueError('value is not one of recognised boolean strings: %s' % value)
    return parser
    
# TODO string lengths, string patterns, ...


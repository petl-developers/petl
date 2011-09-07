"""
TODO doc me

"""


from operator import itemgetter
from collections import defaultdict
from itertools import islice, groupby
import re
import logging
from petl.util import asindices, rowgetter, closeit


logger = logging.getLogger('petl')
def d(*args):
    logger.debug(repr(args))
def i(*args):
    logger.info(repr(args))
def w(*args):
    logger.warn(repr(args))
def e(*args):
    logger.error(repr(args))


class cut(object):
    """
    TODO doc me
    
    """
    
    def __init__(self, source, *args):
        self.source = source
        self.selection = args # selection can be either field names or indices
    
    def __iter__(self):
        source_iterator = iter(self.source)
        try:
            
            # convert field selection into field indices
            fields = source_iterator.next()
            indices = asindices(fields, self.selection)

            # define a function to transform each row in the source data 
            # according to the field selection
            transform = rowgetter(*indices)
            
            # yield the transformed field names
            yield transform(fields)
            
            # construct the transformed data
            for row in source_iterator:
                try:
                    yield transform(row) 
                except IndexError:
                    # row is short, let's be kind and fill in any missing fields
                    yield [row[i] if i < len(row) else Ellipsis for i in indices]

        except:
            raise
        finally:
            closeit(source_iterator)


class cat(object):
    """
    TODO doc me
    
    """
    
    def __init__(self, *sources, **kwargs):
        self.sources = sources
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
    
    def __iter__(self):
        source_iterators = [iter(t) for t in self.sources]
        try:
            
            # determine output fields by gathering all fields found in the sources
            source_fields_lists = [source_iterator.next() for source_iterator in source_iterators]
            out_fields = list()
            for fields in source_fields_lists:
                for f in fields:
                    if f not in out_fields:
                        # add any new fields as we find them
                        out_fields.append(f)
            yield out_fields

            # output data rows
            for source_index, source_iterator in enumerate(source_iterators):
                fields = source_fields_lists[source_index]
                
                # let's define a function which will, for any row and field name,
                # return the corresponding value, or fill in any missing values
                def get_value(row, f):
                    try:
                        value = row[fields.index(f)]
                    except ValueError: # source does not have f in fields
                        value = self.missing
                    except IndexError: # row is short
                        value = self.missing
                    return value
                
                # now construct and yield the data rows
                for row in source_iterator:
                    out_row = [get_value(row, f) for f in out_fields]
                    yield out_row

        except:
            raise
        finally:
            # make sure all iterators are closed
            for source_iterator in source_iterators:
                closeit(source_iterator)
        

class convert(object):
    
    def __init__(self, source, conversion=dict(), error_value=None):
        self.source = source
        self.conversion = conversion
        self.error_value = error_value
        
    def set(self, field, converter):
        self.conversion[field] = converter
        
    def __setitem__(self, key, value):
        self.set(key, value)
        
    def __iter__(self):
        source_iterator = iter(self.source)
        try:
            
            # grab the fields in the source table
            fields = source_iterator.next()
            yield fields # these are not modified
            
            # define a function to transform a value
            def transform_value(i, v):
                try:
                    f = fields[i]
                except IndexError:
                    # row is long, just return value as-is
                    return v
                else:
                    try:
                        c = self.conversion[f]
                    except KeyError:
                        # no converter defined on this field, return value as-is
                        return v
                    else:
                        try:
                            return c(v)
                        except ValueError:
                            return self.error_value
                        except TypeError:
                            return self.error_value

            # construct the data rows
            for row in source_iterator:
                yield [transform_value(i, v) for i, v in enumerate(row)]

        except:
            raise
        finally:
            closeit(source_iterator)
                
                
class sort(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.selection = args
        if 'reverse' in kwargs:
            self.reverse = kwargs['reverse']
        else:
            self.reverse=False
        self.kwargs = kwargs
        
    def __iter__(self):
        source_iterator = iter(self.source)
        try:
            fields = source_iterator.next()
            yield fields
            
            # convert field selection into field indices
            indices = asindices(fields, self.selection)
             
            # now use field indices to construct a getkey function
            # N.B., this will probably raise an exception on short rows
            getkey = itemgetter(*indices)

            # TODO merge sort on large dataset!!!
            rows = list(source_iterator)
            rows.sort(key=getkey, reverse=self.reverse)
            for row in rows:
                yield row
            
        except:
            raise
        finally:
            closeit(source_iterator)

                  
class filterduplicates(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.selection = args
        self.kwargs = kwargs
        
    def __iter__(self):
        
        # first need to sort the data
        source = sort(self.source, *self.selection)
        source_iterator = iter(source)

        try:
            fields = source_iterator.next()
            yield fields

            # convert field selection into field indices
            indices = asindices(fields, self.selection)
                
            # now use field indices to construct a getkey function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            getkey = itemgetter(*indices)
            
            previous = None
            previous_yielded = False
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = getkey(previous)
                    kcurr = getkey(row)
                    if kprev == kcurr:
                        if not previous_yielded:
                            yield previous
                            previous_yielded = True
                        yield row
                    else:
                        # reset
                        previous_yielded = False
                    previous = row
            
        except:
            raise
        finally:
            closeit(source_iterator)


class filterconflicts(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.selection = args
        self.kwargs = kwargs
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
        
    def __iter__(self):
        # first need to sort the data
        source = sort(self.source, *self.selection)
        source_iterator = iter(source)

        try:
            fields = source_iterator.next()
            yield fields

            # convert field selection into field indices
            indices = asindices(fields, self.selection)
                            
            # now use field indices to construct a getkey function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            getkey = itemgetter(*indices)
            
            previous = None
            previous_yielded = False
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = getkey(previous)
                    kcurr = getkey(row)
                    if kprev == kcurr:
                        # is there a conflict?
                        conflict = False
                        for x, y in zip(previous, row):
                            if self.missing not in (x, y) and x != y:
                                conflict = True
                                break
                        if conflict:
                            if not previous_yielded:
                                yield previous
                                previous_yielded = True
                            yield row
                    else:
                        # reset
                        previous_yielded = False
                    previous = row
            
        except:
            raise
        finally:
            closeit(source_iterator)


class mergeduplicates(object):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.selection = args
        self.kwargs = kwargs
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
        
    def __iter__(self):
        # first need to sort the data
        source = sort(self.source, *self.selection)
        source_iterator = iter(source)

        try:
            fields = source_iterator.next()
            yield fields

            # convert field selection into field indices
            indices = asindices(fields, self.selection)
            
            # now use field indices to construct a getkey function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            getkey = itemgetter(*indices)
            
            previous = None
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = getkey(previous)
                    kcurr = getkey(row)
                    if kprev == kcurr:
                        merge = list()
                        for i, v in enumerate(row):
                            try:
                                if v is not self.missing:
                                    # last wins
                                    merge.append(v)
                                else:
                                    merge.append(previous[i])
                            except IndexError: # previous row is short
                                merge.append(v)
                        previous = merge
                    else:
                        yield previous
                        previous = row
            # return the last one
            yield previous
            
        except:
            raise
        finally:
            closeit(source_iterator)


class Melt(object):
    
    def __init__(self, source, key=None, variables=None, 
                 variable_field='variable', value_field='value'):
        assert key is not None or variables is not None, 'supply either key or variables (or both)'
        self.source = source
        self.key = key
        self.variables = variables
        self.variable_field = variable_field
        self.value_field = value_field
        
    def __iter__(self):
        sit = iter(self.source)
        try:
            
            # normalise some stuff
            flds = sit.next()
            key = self.key
            variables = self.variables
            if isinstance(key, basestring):
                key = (key,) # normalise to a tuple
            if isinstance(variables, basestring):
                # shouldn't expect this, but ... ?
                variables = (variables,) # normalise to a tuple
            if key is None:
                # assume key is fields not in variables
                key = [f for f in flds if f not in variables]
            if variables is None:
                # assume variables are fields not in key
                variables = [f for f in flds if f not in key]
            
            # determine the output fields
            out_fields = list(key)
            out_fields.append(self.variable_field)
            out_fields.append(self.value_field)
            yield out_fields
            
            key_indices = [flds.index(k) for k in key]
            if len(key) > 1:
                get_key = itemgetter(*key_indices)
            elif len(key) == 1:
                key_index = key_indices[0]
                get_key = lambda row: (row[key_index],)
            variables_indices = [flds.index(v) for v in variables]
            
            # construct the output data
            for row in sit:
                k = get_key(row)
                for v, i in zip(variables, variables_indices):
                    o = list(k) # populate with key values initially
                    o.append(v)
                    o.append(row[i])
                    yield o
                    
        except:
            raise
        finally:
            if hasattr(sit, 'closeit'):
                sit.closeit()


class Recast(object):
    
    def __init__(self, source, key=None, variable_field='variable', 
                 value_field='value', sample_size=1000, reduce=dict(), 
                 missing=None):
        self.source = source
        self.key = key
        self.variable_field = variable_field
        self.value_field = value_field
        self.sample_size = 1000
        self.reduce = reduce
        self.missing = missing
    
    def __iter__(self):

        #
        # TODO implementing this by making two passes through the data is a bit
        # ugly, and could be costly if there are several upstream transformations
        # that would need to be re-executed each pass - better to make one pass,
        # caching the rows sampled to discover variables to be recast as fields?
        #
        
        try:
            
            source_iterator = iter(self.source)
            flds = source_iterator.next()
            
            # normalise some stuff
            value_field = self.value_field
            key_fields = self.key
            variable_fields = self.variable_field # N.B., could be more than one
            if isinstance(key_fields, basestring):
                key_fields = (key_fields,)
            if isinstance(variable_fields, basestring):
                variable_fields = (variable_fields,)
            if key_fields is None:
                # assume key_fields is fields not in variables
                key_fields = [f for f in flds if f not in variable_fields and f != value_field]
            if variable_fields is None:
                # assume variables are fields not in key_fields
                variable_fields = [f for f in flds if f not in key_fields and f != value_field]
            
            # sanity checks
            assert value_field in flds, 'invalid value field: %s' % value_field
            assert value_field not in key_fields, 'value field cannot be key_fields'
            assert value_field not in variable_fields, 'value field cannot be variable field'
            for f in key_fields:
                assert f in flds, 'invalid key_fields field: %s' % f
            for f in variable_fields:
                assert f in flds, 'invalid variable field: %s' % f

            # we'll need these later
            value_index = flds.index(value_field)
            key_indices = [flds.index(f) for f in key_fields]
            variable_indices = [flds.index(f) for f in variable_fields]
            
            # determine the actual variable names to be cast as fields
            if isinstance(variable_fields, dict):
                # user supplied dictionary
                variables = variable_fields
            else:
                variables = defaultdict(set)
                # sample the data to discover variables to be cast as fields
                for row in islice(source_iterator, 0, self.sample_size):
                    for i, f in zip(variable_indices, variable_fields):
                        variables[f].add(row[i])
                for f in variables:
                    variables[f] = sorted(variables[f]) # turn from sets to sorted lists
            
            if hasattr(source_iterator, 'closeit'):
                source_iterator.closeit() # finished the first pass
            
            # determine the output fields
            out_fields = list(key_fields)
            for f in variable_fields:
                out_fields.extend(variables[f])
            yield out_fields
            
            # output data
            
            source = sort(self.source, *key_fields)
            source_iterator = iter(source)
            source_iterator.next() # skip header row, don't know why islice doesn't work?
            get_key = itemgetter(*key_indices)
            
            # process sorted data in groups
            groups = groupby(source_iterator, key=get_key)
            for key_value, group in groups:
                group = list(group) # may need to iterate over the group more than once
                if len(key_fields) > 1:
                    out_row = list(key_value)
                else:
                    out_row = [key_value]
                for f, i in zip(variable_fields, variable_indices):
                    for variable in variables[f]:
                        # collect all values for the current variable
                        values = [r[value_index] for r in group if r[i] == variable]
                        if variable in self.reduce:
                            redu = self.reduce[variable]
                        else:
                            redu = itemgetter(0) # pick first item arbitrarily
                        if values:
                            value = redu(values)
                        else:
                            value = self.missing
                        out_row.append(value)
                yield out_row
                        
        except:
            raise
        finally:
            if hasattr(source_iterator, 'closeit'):
                source_iterator.closeit()


class StringCapture(object):
    
    def __init__(self, source, field, pattern, groups, include_original=False):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.groups = groups
        self.include_original = include_original
        
    def __iter__(self):
        sit = iter(self.source)
        try:
            prog = re.compile(self.pattern)
            
            flds = sit.next()
            assert self.field in flds, 'field not found: %s' % self.field
            field_index = flds.index(self.field)
            
            # determine output fields
            if self.include_original:
                out_fields = list(flds)
            else:
                out_fields = [f for f in flds if f != self.field]
            out_fields.extend(self.groups)
            yield out_fields
            
            # construct the output data
            for row in sit:
                value = row[field_index]
                if self.include_original:
                    out_row = list(row)
                else:
                    out_row = [v for i, v in enumerate(row) if i != field_index]
                out_row.extend(prog.search(value).groups())
                yield out_row
                
        except:
            raise
        finally:
            if hasattr(sit, 'closeit'):
                sit.closeit()
        

class StringSplit(object):
    def __init__(self, source, field, pattern, groups, include_original=False):
        self.source = source
        self.field = field
        self.pattern = pattern
        self.groups = groups
        self.include_original = include_original
        
    def __iter__(self):
        sit = iter(self.source)
        try:
            
            flds = sit.next()
            assert self.field in flds, 'field not found: %s' % self.field
            field_index = flds.index(self.field)
            
            # determine output fields
            if self.include_original:
                out_fields = list(flds)
            else:
                out_fields = [f for f in flds if f != self.field]
            out_fields.extend(self.groups)
            yield out_fields
            
            # construct the output data
            for row in sit:
                value = row[field_index]
                if self.include_original:
                    out_row = list(row)
                else:
                    out_row = [v for i, v in enumerate(row) if i != field_index]
                out_row.extend(value.split(self.pattern))
                yield out_row
                
        except:
            raise
        finally:
            if hasattr(sit, 'closeit'):
                sit.closeit()
        

def mean(values):
    return float(sum(values)) / len(values)


def meanf(precision=2):
    def f(values):
        v = mean(values)
        v = round(v, precision)
        return v
    return f
    
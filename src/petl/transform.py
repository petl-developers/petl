"""
TODO doc me

"""


from operator import itemgetter
from collections import defaultdict
from itertools import islice, groupby
import re

class Cut(object):
    """
    TODO doc me
    
    """
    
    def __init__(self, source, *args):
        self.source = source
        self.field_selection = args
    
    def __iter__(self):

        source_iterator = iter(self.source)
        try:
            
            # convert field selection into field indices
            indices = list()
            source_fields = source_iterator.next()
            for selection in self.field_selection:
                # selection could be a field name
                if selection in source_fields:
                    indices.append(source_fields.index(selection))
                # or selection could be a field index
                elif isinstance(selection, int) and selection - 1 < len(source_fields):
                    indices.append(selection - 1) # index fields from 1, not 0
                else:
                    # TODO raise?
                    pass

            # define a function to transform each row in the source data 
            # according to the field selection
            #
            # if more than one field selected, use itemgetter, it should be the
            # most efficient
            if len(indices) > 1:
                transform = itemgetter(*indices)
            # 
            # if only one field is selected, we cannot use itemgetter, because
            # we want a singleton sequence to be returned, but itemgetter with
            # a single argument returns the value itself, so let's define a 
            # custom transform function
            elif len(indices) == 1:
                index = indices[0]
                transform = lambda row: (row[index],) # note comma - singleton tuple!
            #
            # no fields selected, should probably raise an error
            else:
                # TODO raise?
                pass
            
            # yield the transformed field names
            yield transform(source_fields)
            
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
            # make sure the iterator is closed
            if hasattr(source_iterator, 'close'):
                source_iterator.close()


class Cat(object):
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
                if hasattr(source_iterator, 'close'):
                    source_iterator.close()
        

class Convert(object):
    
    def __init__(self, source, conversion=dict(), error_value=None):
        self.source = source
        self.conversion = conversion
        self.error_value = error_value
        
    def add(self, field, converter):
        self.conversion[field] = converter
        
    def __iter__(self):
        source_iterator = iter(self.source)
        
        try:
            
            # grab the fields in the source table
            flds = source_iterator.next()
            yield flds # these are not modified
            
            # define a function to transform a value
            def transform_value(i, v):
                try:
                    f = flds[i]
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
            if hasattr(source_iterator, 'close'):
                source_iterator.close()
                
                
class Sort(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.field_selection = args
        if 'reverse' in kwargs:
            self.reverse = kwargs['reverse']
        else:
            self.reverse=False
        self.kwargs = kwargs
        
    def __iter__(self):
        # TODO merge sort on large dataset
        source_iterator = iter(self.source)

        try:
            flds = source_iterator.next()
            yield flds
            
            # convert field selection into field indices
            indices = list()
            for selection in self.field_selection:
                # selection could be a field name
                if selection in flds:
                    indices.append(flds.index(selection))
                # or selection could be a field index
                elif isinstance(selection, int) and selection - 1 < len(flds):
                    indices.append(selection - 1) # index fields from 1, not 0
                else:
                    # TODO raise?
                    pass
                
            # now use field indices to construct a key function
            # N.B., this will probably raise an exception on short rows
            key = itemgetter(*indices)
            rows = list(source_iterator)
            rows.sort(key=key, reverse=self.reverse)
            for row in rows:
                yield row
            
        except:
            raise
        finally:
            if hasattr(source_iterator, 'close'):
                source_iterator.close()

                  
class FilterDuplicates(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.field_selection = args
        self.kwargs = kwargs
        
    def __iter__(self):
        
        # first need to sort the data
        source = Sort(self.source, *self.field_selection)
        source_iterator = iter(source)

        try:
            flds = source_iterator.next()
            yield flds

            # convert field selection into field indices
            indices = list()
            for selection in self.field_selection:
                # selection could be a field name
                if selection in flds:
                    indices.append(flds.index(selection))
                # or selection could be a field index
                elif isinstance(selection, int) and selection - 1 < len(flds):
                    indices.append(selection - 1) # index fields from 1, not 0
                else:
                    # TODO raise?
                    pass
                
            # now use field indices to construct a key function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            key = itemgetter(*indices)
            
            previous = None
            previous_yielded = False
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = key(previous)
                    kcurr = key(row)
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
            if hasattr(source_iterator, 'close'):
                source_iterator.close()


class FilterConflicts(object):
    
    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.field_selection = args
        self.kwargs = kwargs
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
        
    def __iter__(self):
        # first need to sort the data
        source = Sort(self.source, *self.field_selection)
        source_iterator = iter(source)

        try:
            flds = source_iterator.next()
            yield flds

            # convert field selection into field indices
            indices = list()
            for selection in self.field_selection:
                # selection could be a field name
                if selection in flds:
                    indices.append(flds.index(selection))
                # or selection could be a field index
                elif isinstance(selection, int) and selection - 1 < len(flds):
                    indices.append(selection - 1) # index fields from 1, not 0
                else:
                    # TODO raise?
                    pass
                
            # now use field indices to construct a key function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            key = itemgetter(*indices)
            
            previous = None
            previous_yielded = False
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = key(previous)
                    kcurr = key(row)
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
            if hasattr(source_iterator, 'close'):
                source_iterator.close()


class MergeDuplicates(object):

    def __init__(self, source, *args, **kwargs):
        self.source = source
        self.field_selection = args
        self.kwargs = kwargs
        if 'missing' in kwargs:
            self.missing = kwargs['missing']
        else:
            self.missing = Ellipsis
        
    def __iter__(self):
        # first need to sort the data
        source = Sort(self.source, *self.field_selection)
        source_iterator = iter(source)

        try:
            flds = source_iterator.next()
            yield flds

            # convert field selection into field indices
            indices = list()
            for selection in self.field_selection:
                # selection could be a field name
                if selection in flds:
                    indices.append(flds.index(selection))
                # or selection could be a field index
                elif isinstance(selection, int) and selection - 1 < len(flds):
                    indices.append(selection - 1) # index fields from 1, not 0
                else:
                    # TODO raise?
                    pass
                
            # now use field indices to construct a key function
            # N.B., this may raise an exception on short rows, depending on
            # the field selection
            key = itemgetter(*indices)
            
            previous = None
            
            for row in source_iterator:
                if previous is None:
                    previous = row
                else:
                    kprev = key(previous)
                    kcurr = key(row)
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
            if hasattr(source_iterator, 'close'):
                source_iterator.close()


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
            if hasattr(sit, 'close'):
                sit.close()


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
        try:
            
            sit = iter(self.source)
            flds = sit.next()
            
            # normalise some stuff
            valf = self.value_field
            keyf = self.key
            varf = self.variable_field
            if isinstance(keyf, basestring):
                keyf = (keyf,)
            if isinstance(varf, basestring):
                varf = (varf,)
            if keyf is None:
                # assume keyf is fields not in variables
                keyf = [f for f in flds if f not in varf and f != valf]
            if varf is None:
                # assume variables are fields not in keyf
                varf = [f for f in flds if f not in keyf and f != valf]
            
            # sanity checks
            assert valf in flds, 'invalid value field: %s' % valf
            assert valf not in keyf, 'value field cannot be keyf'
            assert valf not in varf, 'value field cannot be variable field'
            for f in keyf:
                assert f in flds, 'invalid keyf field: %s' % f
            for f in varf:
                assert f in flds, 'invalid variable field: %s' % f

            # we'll need these later
            vali = flds.index(valf)
            keyi = [flds.index(k) for k in keyf]
            vari = [flds.index(v) for v in varf]
            
            # determine the actual variable names to be cast as fields
            if isinstance(varf, dict):
                vard = varf
            else:
                vard = defaultdict(set)
                # sample the data to collect variables to be cast as fields
                for row in islice(sit, 0, self.sample_size):
                    for i, f in zip(vari, varf):
                        vard[f].add(row[i])
                for f in vard:
                    vard[f] = sorted(vard[f])
            
            if hasattr(sit, 'close'):
                sit.close() # finished the first pass
            
            # determine the output fields
            out_fields = list(keyf)
            for f in varf:
                out_fields.extend(vard[f])
            yield out_fields
            
            # output data
            
            source = Sort(self.source, *keyf)
            sit = iter(source)
            key = itemgetter(*keyi)
            
            # process sorted data in groups
            groups = groupby(islice(sit, 1), key=key)
            for kval, group in groups:
                if len(kval) > 1:
                    out_row = list(kval)
                else:
                    out_row = [kval]
                group = list(group) # may need to iterate over the group more than once
                for v, i in zip(varf, vari):
                    for f in vard[v]:
                        values = [r[vali] for r in group if r[i] == f]
                        if f in self.reduce:
                            redu = self.reduce[f]
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
            if hasattr(sit, 'close'):
                sit.close()


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
            if hasattr(sit, 'close'):
                sit.close()
        

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
            if hasattr(sit, 'close'):
                sit.close()
        

    
    
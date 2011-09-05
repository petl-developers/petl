"""
TODO doc me

"""


from operator import itemgetter

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
            # a single argument returns the get_value itself, so let's define a 
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
                # return the corresponding get_value, or fill in any missing values
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

                  
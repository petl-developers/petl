# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

data = """type,price,quantity
Apples
Cortland,0.30,24
Red Delicious,0.40,24
Oranges
Navel,0.50,12
"""

# <codecell>

import petl.interactive as etl
from petl.io import StringSource

# <codecell>

tbl1 = (etl
    .fromcsv(StringSource(data))
)
tbl1

# <headingcell level=2>

# Option 1 - using existing petl functions

# <codecell>

def make_room_for_category(row):
    if len(row) == 1:
        return (row[0], 'X', 'X', 'X')
    else:
        return (None,) + tuple(row)

tbl2 = tbl1.rowmap(make_room_for_category, fields=['category', 'type', 'price', 'quantity'])
tbl2

# <codecell>

tbl3 = tbl2.filldown()
tbl3

# <codecell>

tbl4 = tbl3.ne('type', 'X')
tbl4

# <headingcell level=2>

# Option 2 - custom transformer

# <codecell>

class CustomTransformer(object):
    
    def __init__(self, source):
        self.source = source
        
    def __iter__(self):
        it = iter(self.source)
        
        # construct new header
        source_fields = it.next()
        out_fields = ('category',) + tuple(source_fields)
        yield out_fields
        
        # transform data
        current_category = None
        for row in it:
            if len(row) == 1:
                current_category = row[0]
            else:
                yield (current_category,) + tuple(row)

# <codecell>

tbl5 = CustomTransformer(tbl1)

# <codecell>

# just so it formats nicely as HTML in the notebook...
etl.wrap(tbl5)


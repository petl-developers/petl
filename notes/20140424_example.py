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

# <codecell>



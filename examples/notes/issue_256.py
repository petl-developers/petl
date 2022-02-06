# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# Notes supporting [issue #256](https://github.com/alimanfoo/petl/issues/256).

# <codecell>

import petl.interactive as etl

# <codecell>

t1 = etl.wrap([['foo', 'bar'], [1, 'a'], [2, 'b']])
t1

# <codecell>

t2 = etl.wrap([['foo', 'bar'], [1, 'a'], [2, 'c']])
t2

# <codecell>

t3 = etl.merge(t1, t2, key='foo')
t3

# <markdowncell>

# The problem with the above is that you cannot tell from inspecting *t3* alone which conflicting value comes from which source.
# 
# A workaround as suggested by [@pawl](https://github.com/pawl) is to use the [*conflicts()*](http://petl.readthedocs.org/en/latest/#petl.conflicts) function, e.g.: 

# <codecell>

t4 = (etl
    .cat(
        t1.addfield('source', 1),
        t2.addfield('source', 2)
    )
    .conflicts(key='foo', exclude='source')
)
t4


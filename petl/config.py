from __future__ import division, print_function, absolute_import


from petl.compat import text_type


look_style = 'grid'  # alternatives: 'simple', 'minimal'
look_limit = 5
look_index_header = False
look_vrepr = repr
look_width = None
see_limit = 5
see_index_header = False
see_vrepr = repr
display_limit = 5
display_index_header = False
display_vrepr = text_type
sort_buffersize = 100000
failonerror=False # False, True, 'inline'
"""
Controls what happens when unhandled exceptions are raised in a
transformation:

    - If `False`, exceptions are suppressed.  If present, the value
      provided in the `errorvalue` argument is returned.

    - If `True`, the first unhandled exception is raised.

    - If `'inline'`, unhandled exceptions are returned.
"""

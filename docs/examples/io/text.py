from __future__ import division, print_function, absolute_import


# fromtext()
############

# example data
text = 'a,1\nb,2\nc,2\n'
with open('test.txt', 'w') as f:
    f.write(text)

from petl import fromtext, look
table1 = fromtext('test.txt')
look(table1)
# now post-process,e.g., with capture()
from petl import capture
table2 = capture(table1, 'lines', '(.*),(.*)$', ['foo', 'bar'])
look(table2)


# totext()
##########

table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
from petl import totext, look
prologue = '''{| class="wikitable"
|-
! foo
! bar
'''
template = '''|-
| {foo}
| {bar}
'''
epilogue = '|}'
totext(table1, 'test.txt', template, prologue, epilogue)
# see what we did
print(open('test.txt').read())

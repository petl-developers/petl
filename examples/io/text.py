from __future__ import division, print_function, absolute_import


# fromtext()
############

import petl as etl
# setup example file
text = 'a,1\nb,2\nc,2\n'
with open('example.txt', 'w') as f:
    f.write(text)

table1 = etl.fromtext('example.txt')
table1
# post-process, e.g., with capture()
table2 = table1.capture('lines', '(.*),(.*)$', ['foo', 'bar'])
table2


# totext()
##########

import petl as etl
table1 = [['foo', 'bar'],
          ['a', 1],
          ['b', 2],
          ['c', 2]]
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
etl.totext(table1, 'example.txt', template, prologue, epilogue)
# see what we did
print(open('example.txt').read())

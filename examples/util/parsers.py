from __future__ import division, print_function, absolute_import


# datetimeparser()
##################

from petl import datetimeparser
isodatetime = datetimeparser('%Y-%m-%dT%H:%M:%S')
isodatetime('2002-12-25T00:00:00')
try:
    isodatetime('2002-12-25T00:00:99')
except ValueError as e:
    print(e)


# dateparser()
##############

from petl import dateparser
isodate = dateparser('%Y-%m-%d')
isodate('2002-12-25')
try:
    isodate('2002-02-30')
except ValueError as e:
    print(e)


# timeparser()
##############

from petl import timeparser
isotime = timeparser('%H:%M:%S')
isotime('00:00:00')
isotime('13:00:00')
try:
    isotime('12:00:99')
except ValueError as e:
    print(e)

try:
    isotime('25:00:00')
except ValueError as e:
    print(e)


# boolparser()
##############

from petl import boolparser
mybool = boolparser(true_strings=['yes', 'y'], false_strings=['no', 'n'])
mybool('y')
mybool('yes')
mybool('Y')
mybool('No')
try:
    mybool('foo')
except ValueError as e:
    print(e)

try:
    mybool('True')
except ValueError as e:
    print(e)

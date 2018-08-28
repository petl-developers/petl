import math
import petl
import openpyxl
import time
import sys

if len(sys.argv) < 3:
    ITERATIONS = 1
else:
    ITERATIONS = int(sys.argv[2])

if len(sys.argv) >= 2:
    FILENAME = sys.argv[1]
else:
    FILENAME = "100krow.xlsx"

def doThingsWith(data):
    start = time.time()

    L = len(data)
    _x = data[0]
    _x = data[1]
    _x =  data[L-2]
    _x =  data[L-1]

    _x =  data[ int(math.floor(L/2)) ]

    end = time.time()
    print "ACCESS TIME: doThingsWith() executed in ", end - start, "s."
    
print "Benchmarking on file", FILENAME, "for", ITERATIONS, "iterations."
print ""
print "petl version", petl.__version__
print "openpyxl version", openpyxl.__version__

print "TESTING read_only=True"
data = None
# Default settings
start = time.time()
for _ in xrange(ITERATIONS):
    data = petl.io.xlsx.fromxlsx(FILENAME, read_only=True)
end = time.time()
print "LOAD TIME:", (end - start)
doThingsWith(data)
print "Number of lines captured with default settings:", len(data)

print ""
print "================================================="
print ""

print "TESTING read_only=False"
data = None
# Read only set to false
start = time.time()
for _ in xrange(ITERATIONS):
    data = petl.io.xlsx.fromxlsx(FILENAME, read_only=False)
end = time.time()
print "LOAD TIME:", (end - start)
doThingsWith(data)
print "Number of lines captured with `read_only=False` settings:", len(data)

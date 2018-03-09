import petl
import openpyxl

print "petl version", petl.__version__
print "openpyxl version", openpyxl.__version__

# Default settings
data = petl.io.xlsx.fromxlsx("100krow-nofunc.xlsx")
print "Number of lines with default settings:", len(data)

# Read only set to false
data = petl.io.xlsx.fromxlsx("100krow-nofunc.xlsx", read_only=False)
print "Number of lines with `read_only=False` settings:", len(data)

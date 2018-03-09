import petl

# Default settings
data = petl.io.xlsx.fromxlsx("100krow.xlsx")
print "Number of lines with default settings:", len(data)

# Read only set to false
data = petl.io.xlsx.fromxlsx("100krow.xlsx", read_only=False)
print "Number of lines with default settings:", len(data)

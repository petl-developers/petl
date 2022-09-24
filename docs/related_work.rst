Related Work
============

**continuum.io**

- http://continuum.io

In development, a major revision of NumPy to better support a range of
data integration and processing use cases.

**pandas (Python package)**

- http://pandas.sourceforge.net/
- http://pypi.python.org/pypi/pandas
- http://github.com/wesm/pandas

A Python library for analysis of relational/tabular data, built on
NumPy, and inspired by R's dataframe concept. Functionality includes
support for missing data, inserting and deleting columns, group
by/aggregation, merging, joining, reshaping, pivoting.

**tabular (Python package)**

- http://pypi.python.org/pypi/tabular
- http://packages.python.org/tabular/html/

A Python package for working with tabular data. The `tabarray` class
supports both row-oriented and column-oriented access to data,
including selection and filtering of rows/columns, matrix math
(tabular extends NumPy), sort, aggregate, join, transpose,
comparisons. 

Does require a uniform datatype for each column. All data is handled
in memory.

**datarray (Python package)**

- http://pypi.python.org/pypi/datarray
- http://github.com/fperez/datarray
- http://fperez.github.com/datarray-doc

Datarray provides a subclass of Numpy ndarrays that support individual
dimensions (axes) being labeled with meaningful descriptions labeled
'ticks' along each axis indexing and slicing by named axis indexing on
any axis with the tick labels instead of only integers reduction
operations (like .sum, .mean, etc) support named axis arguments
instead of only integer indices.

**pydataframe (Python package)**

- http://code.google.com/p/pydataframe/

An implemention of an almost R like DataFrame object.

**larry (Python package)**

- http://pypi.python.org/pypi/la

The main class of the la package is a labeled array, larry. A larry
consists of data and labels. The data is stored as a NumPy array and
the labels as a list of lists (one list per dimension). larry has
built-in methods such as ranking, merge, shuffle, move_sum, zscore,
demean, lag as well as typical Numpy methods like sum, max, std, sign,
clip. NaNs are treated as missing data.

**picalo (Python package)**

- http://www.picalo.org/ 
- http://pypi.python.org/pypi/picalo/ 
- http://www.picalo.org/download/api/

A GUI application and Python library primarily aimed at data analysis
for auditors & fraud examiners, but has a number of general purpose
data mining and transformation capabilities like filter, join,
transpose, crosstable/pivot.

Does not rely on streaming/iterative processing of data, and has a
persistence capability based on zodb for handling larger datasets.

**csvkit (Python package)**

- http://pypi.python.org/pypi/picalo/
- http://csvkit.rtfd.org/

A set of command-line utilities for transforming tabular data from CSV
(delimited) files. Includes csvclean, csvcut, csvjoin, csvsort,
csvstack, csvstat, csvgrep, csvlook.

**csvutils (Python package)**

- http://pypi.python.org/pypi/csvutils

**python-pipeline (Python package)**

- http://code.google.com/p/python-pipeline/

**Google Refine**

- http://code.google.com/p/google-refine/

A web application for exploring, filtering, cleaning and transforming
a table of data. Some excellent functionality for finding and fixing
problems in data. Does have the capability to join two tables, but
generally it's one table at a time. Some question marks over ability
to handle larger datasets. 

Has an extension capability, two third party extensions known at the
time of writing, including a `stats extension
<http://blog.apps.chicagotribune.com/2010/11/18/sprint-our-first-google-refine-extension-refine-stats/>`_.

**Data Wrangler**

- http://vis.stanford.edu/wrangler/
- http://vis.stanford.edu/papers/wrangler
- http://pypi.python.org/pypi/DataWrangler

A web application for exploring, transforming and cleaning tabular
data, in a similar vein to Google Refine but with a strong focus on
usability, and more capabilities for transforming tables, including
folding/unfolding (similar to R reshape's melt/cast) and
cross-tabulation.

Currently a client-side only web application, not available for
download. There is also a Python library providing data transformation
functions as found in the GUI. The research paper has a good
discussion of data transformation and quality issues, esp. w.r.t. tool
usability.

**Pentaho Data Integration (a.k.a. Kettle)**

- http://kettle.pentaho.com/
- http://wiki.pentaho.com/display/EAI/Getting+Started
- http://wiki.pentaho.com/display/EAI/Pentaho+Data+Integration+Steps

**SnapLogic**

- http://www.snaplogic.com
- https://www.snaplogic.org/Documentation/3.2/ComponentRef/index.html

A data integration platform, where ETL components are web resources
with a RESTful interface. Standard components for transforms like
filter, join and sort.

**Talend**

- http://www.talend.com

**Jaspersoft ETL**

- http://www.jaspersoft.com/jasperetl

**CloverETL**

- http://www.cloveretl.com/

**Apatar**

- http://apatar.com/

**Jitterbit**

- http://www.jitterbit.com/

**Scriptella**

- http://scriptella.javaforge.com/

**Kapow Katalyst**

- http://kapowsoftware.com/products/kapow-katalyst-platform/index.php
- http://kapowsoftware.com/products/kapow-katalyst-platform/extraction-browser.php
- http://kapowsoftware.com/products/kapow-katalyst-platform/transformation-normalization.php

**Flat File Checker (FlaFi)**

- http://www.flat-file.net/
 
**Orange**

- http://orange.biolab.si/

**North Concepts Data Pipeline**

- http://northconcepts.com/data-pipeline/

**SAS Clinical Data Integration**

- http://www.sas.com/industry/pharma/cdi/index.html

**R Reshape Package**

- http://had.co.nz/reshape/

**TableFu**

- http://propublica.github.com/table-fu/

**python-tablefu**

- https://github.com/eyeseast/python-tablefu

**pygrametl (Python package)**

- http://www.pygrametl.org/
- http://people.cs.aau.dk/~chr/pygrametl/pygrametl.html
- http://dbtr.cs.aau.dk/DBPublications/DBTR-25.pdf

**etlpy (Python package)**

- http://sourceforge.net/projects/etlpy/
- http://etlpy.svn.sourceforge.net/viewvc/etlpy/source/samples/

Looks abandoned since 2009, but there is some code.

**OpenETL**

- https://launchpad.net/openetl
- http://bazaar.launchpad.net/~openerp-commiter/openetl/OpenETL/files/head:/lib/openetl/component/transform/

**Data River**

- http://www.datariver.it/

**Ruffus**

- http://www.ruffus.org.uk/

**PyF**

- http://pyfproject.org/

**PyDTA**

- http://presbrey.mit.edu/PyDTA

**Google Fusion Tables**

- http://www.google.com/fusiontables/Home/

**pivottable (Python package)**

- http://pypi.python.org/pypi/pivottable/0.8

**PrettyTable (Python package)**

- http://pypi.python.org/pypi/PrettyTable

**PyTables (Python package)**

- http://www.pytables.org/

**plyr**

- http://plyr.had.co.nz/

**Tablib**

- https://github.com/jazzband/tablib
- https://tablib.readthedocs.io

Tablib is an MIT Licensed format-agnostic tabular dataset library, written in
Python. It allows you to import, export, and manipulate tabular data sets.
Advanced features include segregation, dynamic columns, tags & filtering, and
seamless format import & export.

**PowerShell**

- http://technet.microsoft.com/en-us/library/ee176874.aspx - Import-Csv
- http://technet.microsoft.com/en-us/library/ee176955.aspx - Select-Object
- http://technet.microsoft.com/en-us/library/ee176968.aspx - Sort-Object
- http://technet.microsoft.com/en-us/library/ee176864.aspx - Group-Object

**SwiftRiver**

- http://ushahidi.com/products/swiftriver-platform

**Data Science Toolkit**

- http://www.datasciencetoolkit.org/about

**IncPy**

- http://www.stanford.edu/~pgbovine/incpy.html

Doesn't have any ETL functionality, but possibly (enormously) relevant
to exploratory development of a transformation pipeline, because you
could avoid having to rerun the whole pipeline every time you add a
new step.

**Articles, Blogs, Other**

- http://metadeveloper.blogspot.com/2008/02/iron-python-dsl-for-etl.html
- http://www.cs.uoi.gr/~pvassil/publications/2009_IJDWM/IJDWM_2009.pdf
- http://web.tagus.ist.utl.pt/~helena.galhardas/ajax.html
- http://stackoverflow.com/questions/1321396/what-are-the-required-functionnalities-of-etl-frameworks
- http://stackoverflow.com/questions/3762199/etl-using-python
- http://www.jonathanlevin.co.uk/2008/03/open-source-etl-tools-vs-commerical-etl.html
- http://www.quora.com/ETL/Why-should-I-use-an-existing-ETL-vs-writing-my-own-in-Python-for-my-data-warehouse-needs
- http://synful.us/archives/41/the-poor-mans-etl-python
- http://www.gossamer-threads.com/lists/python/python/418041?do=post_view_threaded#418041
- http://code.activestate.com/lists/python-list/592134/
- http://fuzzytolerance.info/code/open-source-etl-tools/
- http://www.protocolostomy.com/2009/12/28/codekata-4-data-munging/
- http://www.hanselman.com/blog/ParsingCSVsAndPoorMansWebLogAnalysisWithPowerShell.aspx - nice example of a data transformation problem, done in PowerShell
- http://www.datascience.co.nz/blog/2011/04/01/the-science-of-data-munging/
- http://wesmckinney.com/blog/?p=8 - on grouping with pandas
- http://stackoverflow.com/questions/4341756/data-recognition-parsing-filtering-and-transformation-gui

On memoization...

- http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
- http://code.activestate.com/recipes/577219-minimalistic-memoization/
- http://ubuntuforums.org/showthread.php?t=850487

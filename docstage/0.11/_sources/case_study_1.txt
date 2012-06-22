.. currentmodule:: petl

Case Study 1 - Comparing Tables
===============================

This case study illustrates some of the :mod:`petl` functions
available for doing some simple profiling and comparison of data from
two tables.

.. raw:: html

   <iframe width="853" height="480" src="http://www.youtube.com/embed/Y0FleNEcO9I" frameborder="0" allowfullscreen></iframe>

Introduction
------------

The files used in this case study can be downloaded from the following
link:

* http://aliman.s3.amazonaws.com/petl/petl-case-study-1-files.zip

Download and unzip the files::

$ wget http://aliman.s3.amazonaws.com/petl/petl-case-study-1-files.zip
$ unzip petl-case-study-1-files.zip

The first file (`snpdata.csv`) contains a list of locations in the
genome of the malaria parasite `P. falciparum`, along with some basic
data about genetic variations found at those locations.

The second file (`popdata.csv`) is supposed to contain the same list
of genome locations, along with some additional data such as allele
frequencies in different populations.

The main point for this case study is that the first file
(`snpdata.csv`) contains the canonical list of genome locations, and
the second file (`popdata.csv`) contains some additional data about
the same genome locations and therefore should be consistent with the
first file. We want to check whether this second file is in fact
consistent with the first file.

Preparing the data
------------------

Start the Python interpreter and import :mod:`petl` functions::

	$ python
	Python 2.7.2+ (default, Oct  4 2011, 20:03:08) 
	[GCC 4.6.1] on linux2
	Type "help", "copyright", "credits" or "license" for more information.
	>>> from petl import *

To save some typing, let `a` be the table of data extracted from the
first file (`snpdata.csv`), and let `b` be the table of data extracted
from the second file (`popdata.csv`), using the :func:`fromcsv`
function::

	>>> a = fromcsv('snpdata.csv', delimiter='\t')
	>>> b = fromcsv('popdata.csv', delimiter='\t')

Examine the header from each file using the :func:`header`
function::

	>>> header(a)
	('Chr', 'Pos', 'Ref', 'Nref', 'Der', 'Mut', 'isTypable', 'GeneId', 'GeneAlias', 'GeneDescr')
	>>> header(b)
	('Chromosome', 'Coordinates', 'Ref. Allele', 'Non-Ref. Allele', 'Outgroup Allele', 'Ancestral Allele', 'Derived Allele', 'Ref. Aminoacid', 'Non-Ref. Aminoacid', 'Private Allele', 'Private population', 'maf AFR', 'maf PNG', 'maf SEA', 'daf AFR', 'daf PNG', 'daf SEA', 'nraf AFR', 'nraf PNG', 'nraf SEA', 'Mutation type', 'Gene', 'Gene Aliases', 'Gene Description', 'Gene Information')

There is a common set of 9 fields that is present in both tables, and
we would like focus on comparing these common fields, however
different field names have been used in the two files. To simplify
comparison, use :func:`rename` to rename some fields in the
second file::

	>>> b_renamed = rename(b, {'Chromosome': 'Chr', 'Coordinates': 'Pos', 'Ref. Allele': 'Ref', 'Non-Ref. Allele': 'Nref', 'Derived Allele': 'Der', 'Mutation type': 'Mut', 'Gene': 'GeneId', 'Gene Aliases': 'GeneAlias', 'Gene Description': 'GeneDescr'})
	>>> header(b_renamed)
	('Chr', 'Pos', 'Ref', 'Nref', 'Outgroup Allele', 'Ancestral Allele', 'Der', 'Ref. Aminoacid', 'Non-Ref. Aminoacid', 'Private Allele', 'Private population', 'maf AFR', 'maf PNG', 'maf SEA', 'daf AFR', 'daf PNG', 'daf SEA', 'nraf AFR', 'nraf PNG', 'nraf SEA', 'Mut', 'GeneId', 'GeneAlias', 'GeneDescr', 'Gene Information')

Use :func:`cut` to extract only the fields we're interested in
from both tables::

	>>> common_fields = ['Chr', 'Pos', 'Ref', 'Nref', 'Der', 'Mut', 'GeneId', 'GeneAlias', 'GeneDescr']
	>>> a_common = cut(a, common_fields)
	>>> b_common = cut(b_renamed, common_fields)

Inspect the data::

	>>> look(a_common)
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'Chr'  | 'Pos'   | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'   | 'GeneAlias' | 'GeneDescr'                                        |
	+========+=========+=======+========+=======+=======+============+=============+====================================================+
	| 'MAL1' | '91099' | 'G'   | 'A'    | '-'   | 'S'   | 'PFA0095c' | 'MAL1P1.10' | 'rifin'                                            |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '91104' | 'A'   | 'T'    | '-'   | 'N'   | 'PFA0095c' | 'MAL1P1.10' | 'rifin'                                            |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93363' | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93382' | 'T'   | 'G'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93384' | 'G'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93390' | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93439' | 'T'   | 'C'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '93457' | 'C'   | 'T'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '94008' | 'T'   | 'C'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | '94035' | 'C'   | 'T'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+---------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	
	>>> look(b_common)
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'Chr'  | 'Pos'   | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'   | 'GeneAlias'     | 'GeneDescr'                                              |
	+========+=========+=======+========+=======+=======+============+=================+==========================================================+
	| 'MAL1' | '91099' | 'G'   | 'A'    | '-'   | 'SYN' | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '91104' | 'A'   | 'T'    | '-'   | 'NON' | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93363' | 'T'   | 'A'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93382' | 'T'   | 'G'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93384' | 'G'   | 'A'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93390' | 'T'   | 'A'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93439' | 'T'   | 'C'    | '-'   | 'SYN' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '93457' | 'C'   | 'T'    | '-'   | 'SYN' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '94008' | 'T'   | 'C'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | '94035' | 'C'   | 'T'    | '-'   | 'NON' | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+---------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+

The :func:`fromcsv` function does not attempt to parse any of the
values from the underlying CSV file, so all values are reported as
strings. However, the 'Pos' field should be interpreted as an integer.

Also, the 'Mut' field has a different representation in the two
tables, which needs to be converted before the data can be compared.

Use the :func:`convert` function to convert the type of the 'Pos'
field in both tables and the representation of the 'Mut' field in
table `b`::

	>>> a_conv = convert(a_common, 'Pos', int)
	>>> b_conv1 = convert(b_common, 'Pos', int)
	>>> b_conv2 = convert(b_conv1, 'Mut', {'SYN': 'S', 'NON': 'N'})
	>>> b_conv = b_conv2
	>>> look(a_conv)
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'Chr'  | 'Pos' | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'   | 'GeneAlias' | 'GeneDescr'                                        |
	+========+=======+=======+========+=======+=======+============+=============+====================================================+
	| 'MAL1' | 91099 | 'G'   | 'A'    | '-'   | 'S'   | 'PFA0095c' | 'MAL1P1.10' | 'rifin'                                            |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 91104 | 'A'   | 'T'    | '-'   | 'N'   | 'PFA0095c' | 'MAL1P1.10' | 'rifin'                                            |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93363 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93382 | 'T'   | 'G'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93384 | 'G'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93390 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93439 | 'T'   | 'C'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 93457 | 'C'   | 'T'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 94008 | 'T'   | 'C'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	| 'MAL1' | 94035 | 'C'   | 'T'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum' |
	+--------+-------+-------+--------+-------+-------+------------+-------------+----------------------------------------------------+
	
	>>> look(b_conv)
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'Chr'  | 'Pos' | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'   | 'GeneAlias'     | 'GeneDescr'                                              |
	+========+=======+=======+========+=======+=======+============+=================+==========================================================+
	| 'MAL1' | 91099 | 'G'   | 'A'    | '-'   | 'S'   | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 91104 | 'A'   | 'T'    | '-'   | 'N'   | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93363 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93382 | 'T'   | 'G'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93384 | 'G'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93390 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93439 | 'T'   | 'C'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93457 | 'C'   | 'T'    | '-'   | 'S'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 94008 | 'T'   | 'C'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 94035 | 'C'   | 'T'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+

Now the tables are ready for comparison.

Looking for missing or unexpected rows
--------------------------------------

Because both tables should contain the same list of genome locations,
they should have the same number of rows. Use :func:`rowcount` to
compare::

	>>> rowcount(a_conv)
	103647
	>>> rowcount(b_conv)
	103618
	>>> 103647-103618
	29

There is some discrepancy. First investigate by comparing just the
genomic locations, defined by the 'Chr' and 'Pos' fields, using
:func:`complement`::

	>>> a_locs = cut(a_conv, 'Chr', 'Pos')
	>>> b_locs = cut(b_conv, 'Chr', 'Pos')
	>>> locs_only_in_a = complement(a_locs, b_locs)
	>>> rowcount(locs_only_in_a)
	29
	>>> look(locs_only_in_a)
	+---------+---------+
	| 'Chr'   | 'Pos'   |
	+=========+=========+
	| 'MAL1'  | 216961  |
	+---------+---------+
	| 'MAL10' | 538210  |
	+---------+---------+
	| 'MAL10' | 548779  |
	+---------+---------+
	| 'MAL10' | 1432969 |
	+---------+---------+
	| 'MAL11' | 500289  |
	+---------+---------+
	| 'MAL11' | 1119809 |
	+---------+---------+
	| 'MAL11' | 1278859 |
	+---------+---------+
	| 'MAL12' | 51827   |
	+---------+---------+
	| 'MAL13' | 183727  |
	+---------+---------+
	| 'MAL13' | 398404  |
	+---------+---------+
	
	>>> locs_only_in_b = complement(b_locs, a_locs)
	>>> rowcount(locs_only_in_b)
	0

So it appears that 29 locations are missing from table `b`. Export
these missing locations to a CSV file using :func:`tocsv`::

	>>> tocsv(locs_only_in_a, 'missing_locations.csv', delimiter='\t')

A shorthand for finding the difference between two tables is the
:func:`diff` function::

	>>> locs_only_in_b, locs_only_in_a = diff(a_locs, b_locs)

An alternative method for finding rows in one table where some key
value is not present in another table is to use the :func:`antijoin`
function::

	>>> locs_only_in_a = antijoin(a_conv, b_conv, key=('Chr', 'Pos'))
	>>> rowcount(locs_only_in_a)
	29
	>>> look(locs_only_in_a)
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'Chr'   | 'Pos'   | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'     | 'GeneAlias'             | 'GeneDescr'                                    |
	+=========+=========+=======+========+=======+=======+==============+=========================+================================================+
	| 'MAL1'  | 216961  | 'T'   | 'C'    | 'C'   | 'S'   | 'PFA0245w'   | 'MAL1P1.40'             | 'novel putative transporter, PfNPT'            |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL10' | 538210  | 'T'   | 'A'    | '-'   | 'N'   | 'PF10_0133'  | '-'                     | 'hypothetical protein'                         |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL10' | 548779  | 'A'   | 'C'    | '-'   | 'N'   | 'PF10_0136'  | '-'                     | 'Initiation factor 2 subunit family, putative' |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL10' | 1432969 | 'T'   | 'C'    | '-'   | 'N'   | 'PF10_0355'  | '-'                     | 'Erythrocyte membrane protein, putative'       |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL11' | 500289  | 'C'   | 'A'    | 'A'   | 'N'   | 'PF11_0528'  | '-'                     | 'hypothetical protein'                         |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL11' | 1119809 | 'G'   | 'C'    | 'C'   | 'N'   | 'PF11_0300'  | '-'                     | 'hypothetical protein'                         |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL11' | 1278859 | 'A'   | 'G'    | '-'   | 'N'   | 'PF11_0341'  | '-'                     | 'hypothetical protein'                         |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL12' | 51827   | 'T'   | 'G'    | '-'   | 'N'   | 'PFL0030c'   | '2277.t00006,MAL12P1.6' | 'erythrocyte membrane protein 1 (PfEMP1)'      |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL13' | 183727  | 'G'   | 'C'    | '-'   | 'N'   | 'MAL13P1.18' | '-'                     | 'hypothetical protein, conserved'              |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+
	| 'MAL13' | 398404  | 'G'   | 'T'    | '-'   | 'S'   | 'MAL13P1.41' | '-'                     | 'hypothetical protein, conserved'              |
	+---------+---------+-------+--------+-------+-------+--------------+-------------------------+------------------------------------------------+

Finding conflicts
-----------------

We'd also like to compare the values given in the other fields, to
find any discrepancies between the two tables.

First, examine all fields for discrepancies, using the :func:`cat` and
:func:`conflicts` functions::

	>>> ab = cat(a_conv, b_conv)
	>>> ab_conflicts = conflicts(ab, key=('Chr', 'Pos'))
	>>> rowcount(ab_conflicts)
	185978
	>>> look(ab_conflicts)
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'Chr'  | 'Pos' | 'Ref' | 'Nref' | 'Der' | 'Mut' | 'GeneId'   | 'GeneAlias'     | 'GeneDescr'                                              |
	+========+=======+=======+========+=======+=======+============+=================+==========================================================+
	| 'MAL1' | 91099 | 'G'   | 'A'    | '-'   | 'S'   | 'PFA0095c' | 'MAL1P1.10'     | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 91099 | 'G'   | 'A'    | '-'   | 'S'   | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 91104 | 'A'   | 'T'    | '-'   | 'N'   | 'PFA0095c' | 'MAL1P1.10'     | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 91104 | 'A'   | 'T'    | '-'   | 'N'   | 'PFA0095c' | 'MAL1P1.10,RIF' | 'rifin'                                                  |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93363 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93363 | 'T'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93382 | 'T'   | 'G'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93382 | 'T'   | 'G'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93384 | 'G'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+
	| 'MAL1' | 93384 | 'G'   | 'A'    | '-'   | 'N'   | 'PFA0100c' | 'MAL1P1.11'     | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+-------+--------+-------+-------+------------+-----------------+----------------------------------------------------------+

From a glance at the conflicts above, it appears there are
discrepancies in the 'GeneAlias' and 'GeneDescr' fields. There may
also be conflicts in other fields, so we need to investigate further.

Find conflicts, one field at a time, starting with the 'Ref', 'Nref',
'Der' and 'Mut' fields::

	>>> ab_ref = cut(ab, 'Chr', 'Pos', 'Ref')
	>>> ab_ref_conflicts = conflicts(ab_ref, key=('Chr', 'Pos'))
	>>> rowcount(ab_ref_conflicts)
	0
	>>> ab_nref = cut(ab, 'Chr', 'Pos', 'Nref')
	>>> ab_nref_conflicts = conflicts(ab_nref, key=('Chr', 'Pos'))
	>>> rowcount(ab_nref_conflicts)
	0
	>>> ab_der = cut(ab, 'Chr', 'Pos', 'Der')
	>>> ab_der_conflicts = conflicts(ab_der, key=('Chr', 'Pos'))
	>>> rowcount(ab_der_conflicts)
	0
	>>> ab_mut = cut(ab, 'Chr', 'Pos', 'Mut')
	>>> ab_mut_conflicts = conflicts(ab_mut, key=('Chr', 'Pos'))
	>>> rowcount(ab_mut_conflicts)
	3592
	>>> look(ab_mut_conflicts)
	+--------+--------+-------+
	| 'Chr'  | 'Pos'  | 'Mut' |
	+========+========+=======+
	| 'MAL1' | 99099  | '-'   |
	+--------+--------+-------+
	| 'MAL1' | 99099  | 'N'   |
	+--------+--------+-------+
	| 'MAL1' | 99211  | '-'   |
	+--------+--------+-------+
	| 'MAL1' | 99211  | 'N'   |
	+--------+--------+-------+
	| 'MAL1' | 197903 | 'N'   |
	+--------+--------+-------+
	| 'MAL1' | 197903 | 'S'   |
	+--------+--------+-------+
	| 'MAL1' | 384429 | 'N'   |
	+--------+--------+-------+
	| 'MAL1' | 384429 | 'S'   |
	+--------+--------+-------+
	| 'MAL1' | 513268 | 'N'   |
	+--------+--------+-------+
	| 'MAL1' | 513268 | 'S'   |
	+--------+--------+-------+
	
	>>> tocsv(ab_mut_conflicts, 'mut_conflicts.csv', delimiter='\t')

So there are some conflicts in the 'Mut' field, and we've saved them to
a file for later reference.

We'd like also to find conflicts in the 'GeneId', 'GeneAlias' and
'GeneDescr' fields. Here, while we're focusing on finding conflicts in
one field at a time, we'll include information from the other fields
in the output, to provide extra diagnostic information when
investigating the cause of any conflicts, making use of the `include`
keyword argument to the :func:`conflicts` function::

	>>> ab_gene = cut(ab, 'Chr', 'Pos', 'GeneId', 'GeneAlias', 'GeneDescr')
	>>> ab_geneid_conflicts = conflicts(ab_gene, key=('Chr', 'Pos'), include='GeneId')
	>>> rowcount(ab_geneid_conflicts)
	5434
	>>> look(ab_geneid_conflicts)
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'Chr'  | 'Pos'  | 'GeneId'   | 'GeneAlias'           | 'GeneDescr'                                       |
	+========+========+============+=======================+===================================================+
	| 'MAL1' | 190628 | 'PFA0215w' | 'MAL1P1.70'           | 'hypothetical protein, conserved'                 |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190628 | 'PFA0220w' | 'PFA0215w,MAL1P1.34b' | 'ubiquitin carboxyl-terminal hydrolase, putative' |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190668 | 'PFA0215w' | 'MAL1P1.70'           | 'hypothetical protein, conserved'                 |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190668 | 'PFA0220w' | 'PFA0215w,MAL1P1.34b' | 'ubiquitin carboxyl-terminal hydrolase, putative' |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190786 | 'PFA0215w' | 'MAL1P1.70'           | 'hypothetical protein, conserved'                 |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190786 | 'PFA0220w' | 'PFA0215w,MAL1P1.34b' | 'ubiquitin carboxyl-terminal hydrolase, putative' |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190808 | 'PFA0215w' | 'MAL1P1.70'           | 'hypothetical protein, conserved'                 |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190808 | 'PFA0220w' | 'PFA0215w,MAL1P1.34b' | 'ubiquitin carboxyl-terminal hydrolase, putative' |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190854 | 'PFA0215w' | 'MAL1P1.70'           | 'hypothetical protein, conserved'                 |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	| 'MAL1' | 190854 | 'PFA0220w' | 'PFA0215w,MAL1P1.34b' | 'ubiquitin carboxyl-terminal hydrolase, putative' |
	+--------+--------+------------+-----------------------+---------------------------------------------------+
	
	>>> tocsv(ab_geneid_conflicts, 'geneid_conflicts.csv', delimiter='\t')
	>>> ab_genealias_conflicts = conflicts(ab_gene, key=('Chr', 'Pos'), include='GeneAlias')
	>>> rowcount(ab_genealias_conflicts)
	39144
	>>> look(ab_genealias_conflicts)
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'Chr'  | 'Pos'  | 'GeneId'   | 'GeneAlias'                              | 'GeneDescr'                                 |
	+========+========+============+==========================================+=============================================+
	| 'MAL1' | 91099  | 'PFA0095c' | 'MAL1P1.10'                              | 'rifin'                                     |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 91099  | 'PFA0095c' | 'MAL1P1.10,RIF'                          | 'rifin'                                     |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 91104  | 'PFA0095c' | 'MAL1P1.10'                              | 'rifin'                                     |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 91104  | 'PFA0095c' | 'MAL1P1.10,RIF'                          | 'rifin'                                     |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 99099  | 'PFA0110w' | 'MAL1P1.13,Pf155'                        | 'ring-infected erythrocyte surface antigen' |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 99099  | 'PFA0110w' | 'MAL1P1.13,Pf155,RESA'                   | 'ring-infected erythrocyte surface antigen' |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 99211  | 'PFA0110w' | 'MAL1P1.13,Pf155'                        | 'ring-infected erythrocyte surface antigen' |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 99211  | 'PFA0110w' | 'MAL1P1.13,Pf155,RESA'                   | 'ring-infected erythrocyte surface antigen' |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 111583 | 'PFA0125c' | 'JESEBL,jesebl,eba-181,EBA181,MAL1P1.16' | 'erythrocyte binding antigen-181'           |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	| 'MAL1' | 111583 | 'PFA0125c' | 'jesebl,JESEBL,eba-181,MAL1P1.16'        | 'erythrocyte binding antigen-181'           |
	+--------+--------+------------+------------------------------------------+---------------------------------------------+
	
	>>> tocsv(ab_genealias_conflicts, 'genealias_conflicts.csv', delimiter='\t')
	>>> ab_genedescr_conflicts = conflicts(ab_gene, key=('Chr', 'Pos'), include='GeneDescr')
	>>> rowcount(ab_genedescr_conflicts)
	177730
	>>> look(ab_genedescr_conflicts)
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'Chr'  | 'Pos' | 'GeneId'   | 'GeneAlias' | 'GeneDescr'                                              |
	+========+=======+============+=============+==========================================================+
	| 'MAL1' | 93363 | 'PFA0100c' | 'MAL1P1.11' | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93363 | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93382 | 'PFA0100c' | 'MAL1P1.11' | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93382 | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93384 | 'PFA0100c' | 'MAL1P1.11' | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93384 | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93390 | 'PFA0100c' | 'MAL1P1.11' | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93390 | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93439 | 'PFA0100c' | 'MAL1P1.11' | 'Plasmodium exported protein (PHISTa), unknown function' |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	| 'MAL1' | 93439 | 'PFA0100c' | 'MAL1P1.11' | 'hypothetical protein, conserved in P. falciparum'       |
	+--------+-------+------------+-------------+----------------------------------------------------------+
	
	>>> tocsv(ab_genedescr_conflicts, 'genedescr_conflicts.csv', delimiter='\t')

So each of these three fields has a different set of conflicts, and
we've saved the output to CSV files for later reference.

	

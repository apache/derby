This directory contains classes for testing SQL statements invoked
from static initializers.  When these classes are referred to during
binding, they are loaded, causing their static initializers to run.
This can cause nested binding.  Also, DDL is not allowed while binding
is going on.

InsertInStaticInitializer inserts a row into a table from a static Initializer;
i.e
	select staticInitializer.f() from ....;  [q1]
	staticInitialzer.f() executes SQL [q2] which does an insert.
We test that 

1. Locks are held on the table on which the insert is done.
2. Locks are *not* held in the system catalogs which are read for compiling
Q1 and Q2.


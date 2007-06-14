Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

--------------------------------------------------------------------------------------------


			Network Server System Test Readme
			
1) Description
2) Packaging
3) How to run
4) Settings
5) Known issues
6) Future work

DESCRIPTION:
============
The Network Server System Test (NsTest) is a configurable long running system test for
various Derby features running under the network server(or embedded) mode. The test uses
a single table having all the supported data types as columns with multiple clients
performing Insert/Update/Delete and Select operations. The table has a delete trigger 
which populates a similar table when fired. On regular intervals the test also performs 
back-up/restore and re-encryption. The test follows the suggested usage of JDBC with
liberal use of PreparedStatements and closing of respective objects when not in use. 
The test starts off by using preset number of Initialzer threads (INIT_THREADS=6) to begin
loading the tables. Once loaded, each invocation of the test uses 71 clients with the following 
distribution to peform various operations on the database:

1  - A single back-up/restore/re-encrypt thread
15 - Tester1 threads keeps the connection to the database open forever
45 - Tester2 threads frequently opens and closed based on a random choice 
10 - Tester3 threads opens/closes the connection to the database after each query

To increase the number of client either:
(a) Update the above settings to teh desired number
(b) Invoke multiple instances of the test in separate VMs with the System property 
    'derby.nstest.backupRestore' set to 'false' for all the instances except one. This
    will set the Backup/Restore/Re-Encryption thread to be started only once.

Option (b) works best to avoid any single point of test failure and losing the entire test
run artifacts.

The test has been particularly useful in detecting increased memory usage, locking etc. and 
also verifies the expected behaviour when all the different Derby features are used in conjunction.

PACKAGING:
==========
The test resides under the org.apache.derbyTesting.systen.nstest package. The main
class to invoke is org.apache.derbyTesting.system.nstest.NsTest. Currently all the
variable settings used in the test are set in this class. The user threads reside in the
'tester' package while some initializer and generic utility classes belong to 
'init' and 'utils' package.

HOW TO RUN:
===========
Usage:
java org.apache.derbyTesting.system.nstest.NsTest DerbyClient|Embedded

The main class to invoke is org.apache.derbyTesting.system.nstest.NsTest. This class
takes a String argument of "DerbyClient"/"Embedded", default is DerbyClient. The test requires
the Network Server to be started on port 1900 to begin the run. 

To start the NW Server as a thread in the same VM, set the START_SERVER_IN_SAME_VM to true, 
useful for a small test setup.

To turn off Backup/Restore/Re-Encryption, set the System property 'derby.nstest.backupRestore'
to 'false', default is 'true'.

EXIT CRITERIA
=============

1. There should not be any fatal failures
2. No unwanted lock time outs or deadlocks

SETTINGS:
=========
Almost all the variable settings for this test reside in the org.apache.derbyTesting.system.nstest.NsTest
class. The main ones are as follows:
	
INIT_THREADS = Initializer threads
MAX_INITIAL_ROWS = Initial set of rows inserted before test begins
MAX_ITERATIONS = Each client does these many transactions in the test
MAX_LOW_STRESS_ROWS = Num of rows worked over in a transaction 
MAX_OPERATIONS_PER_CONN = Num of transaction batches made by a client before closing the connection
NUMTESTER1 = Number of Tester1 testers
NUMTESTER2 = Number of Tester2 testers
NUMTESTER3 = Number of Tester3 testers
NUM_HIGH_STRESS_ROWS = Maximum rows to be selected
NUM_UNTOUCHED_ROWS = Number of rows on which Update/Delete operations are not performed

START_SERVER_IN_SAME_VM= Set to true/false to start Network Server in the same VM.

The System property 'derby.nstest.backupRestore' can be set to false for turning off 
Backup/Restore/Re-Encryption. By default the Backup/Restore/Re-Encryption thread is
always started.

KNOWN ISSUES:
=============

With the addition of the Backup/Restore/Re-Encryption thread, long runs of this test are blocked
by:
DERBY-1947 - OOM after repeated calls to boot and shutdown a database

FUTURE WORK:
============

- Move location of test variables from NsTest.java to a common area
- Ability to start the NW Server as a separate process passing independent VM args 
- Ability to run using JUnit test runners
- Use larger BLOB/CLOB sizes
- Other Derby feature integration testing
- Work on reporting


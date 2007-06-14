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

--------------------------------------------------------------------------------------------------


			Optimizer Test
			
1) Description
2) Packaging
3) How to run
4) Settings
5) Known issues
6) Future work

DESCRIPTION:
============
This test was written to run complex queries using combination of views (single and multi-level)
and aggregates and test the recent optimizer changes as a part of DERBY-805 and DERBY-1205. 
A default set of queries are available to run (classes org.apache.derbyTesting.system.langtest.
query.Query1 - Query6):
Class Query1: 	Returns a list of queries that Selects from a single view
Class Query2: 	Returns a list of queries that Selects from multiple views using joins
Class Query3: 	Returns a list of queries that Selects from multiple views with
			  	joins on columns having indexes
Class Query4: 	Returns a list of queries that Selects from multiple views with
				joins on columns having no indexes
Class Query5: 	Returns a list of queries that Selects from multiple
 				views with joins on columns, one with index and one without index				
Class Query6: 	Returns a list of queries that Selects from multiple
 				views with combination of nested views and aggregate views
 				
A custom list of queries to be executed can be also provided via a 'query.list' file. 
Each query is run for StaticValues.ITER  iterations and timed using the 
java.sql.Statement and java.sql.PreparedStatement to identify the query preparation and 
execution times.
 
The test settings are located on the org.apache.derbyTesting.system.optimizer.StaticValues
class. The test creates 64 tables with different column types and populates them with 
the a specified number of rows (org.apache.derbyTesting.system.langtest.StaticValues.NUM_OF_ROWS).

PACKAGING:
==========
The test resides under the org.apache.derbyTesting.system.optimizer package. The main
class to invoke is org.apache.derbyTesting.system.optimizer.RunOptimizerTest. All the
variable settings used in the test are set in the org.apache.derbyTesting.system.optimizer.StaticValues 
class. The query classes reside in the 'query' package while some initializer and generic utility 
classes belong to the 'utils' package.

HOW TO RUN:
===========
Usage:

java org.apache.derbyTesting.system.optimizer.RunOptimizerTest -reset|-qlist
-reset = Reset the database and begin run
-qlist = Run only test queries from the 'query.list' file provided
-verbose = Run in the verbose mode to print all the queries being run

The query.list file should be in the format of <qeury name>=query //no ";" at the end

example:
Query1=select col1, max_view_8bc1 from sum_view_8a right join max_view_8b on col1=max_view_8bc1 where col1 <100 union all select col1, min_view_8bc1 from sum_view_8a right join min_view_8b on col1=min_view_8bc1 where col1 <100

No arguments will run all the default test queries, provided via classes
 
Set the 'derby.optimizertest.mode' to 'client' to run this test using the 
DerbyClient against a Derby Network Server running on port 1527

SETTINGS:
=========
The test settings are located on the org.apache.derbyTesting.system.optimizer.StaticValues
class:
NUM_OF_ROWS=1000; 	//Total number of rows expected in each table
NUM_OF_TABLES=64; 	//Total number of tables to be created. This value should not get changed since all the views are depended 
			//these 64 tables
ITER=2; 	    	//Number of iterations of each query

The test also takes the following arguments:

-reset = Reset the database and begin run
-qlist = Run only test queries from the 'query.list' file provided
-verbose = Run in the verbose mode to print all the queries being run

No arguments will run all the default test queries, provided via classes
 
To run the test using the DerbyClient against a Derby Network Server running on port 1527
set the 'derby.optimizertest.mode' System property to 'client' .

KNOWN ISSUES:
=============
The test results shows some improvement from 10.1 to 10.2 . But the difference is not very big. Also we should add some more 
complex queries. 

FUTURE WORK:
============

- Improve on the infrastructure to add more test queries
- Provide a JUnit wrapper to facilitate test runs within harnesses/IDEs that support JUnit



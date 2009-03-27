           Licensed to the Apache Software Foundation (ASF) under one
           or more contributor license agreements.  See the NOTICE file
           distributed with this work for additional information
           regarding copyright ownership.  The ASF licenses this file
           to you under the Apache License, Version 2.0 (the
           "License"); you may not use this file except in compliance
           with the License.  You may obtain a copy of the License at

             http://www.apache.org/licenses/LICENSE-2.0

           Unless required by applicable law or agreed to in writing,
           software distributed under the License is distributed on an
           "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
           KIND, either express or implied.  See the License for the
           specific language governing permissions and limitations
           under the License.    

Sttest package - Single Table Test.

Contents

1. About the Sttest
2. Design
3. Schema
4. Test goals
5. Exit criteria
6. How to run the test

1. About the Sttest

A derby database test which creates a single table, and runs updates against that table for a long time, printing a log of its actions to stdout.

2. Design

It is a multi threaded application where each thread acts as a different connection to the database. The  number of threads can be changed.
It can be changed in the Sttest.java To change the number of connection search for the following string in the Sttest.java file "static int connections_to_make"  and make the changes.
Also other values like maximum size and minimum size can be changed.  The database has only one table with all the Derby supported datatypes.
And they are indexed. The test will insert rows till it reaches a particular size and then it does inserts/updates and deletes. It checks whether the table has the max rows and if so will delete 
some rows which are selected randomly. Similarly the test will check whether the number of rows are less than the minimum number
then it will insert some rows.


3. Schema

The schema for this test is as follows

Table

1.table Datatypes 
	id int not null
	t_char char(100)
	t_blob blob(100K)
	t_clob clob(100K)
	t_date date
	t_decimal decimal
	t_decimal_nn decimal(10,10)
	t_double double precision
	t_float float
	t_int int
	t_longint bigint
	t_numeric_large numeric(31,0)
	t_real real
	t_smallint smallint
	t_time time
	t_timestamp timestamp
	t_varchar varchar(100)
	serialkey bigint generated always as identity (start with 1,increment by 1)
			
4. Test goals

The usual duration of this test is  2 weeks time without any major issues. You can  increase/decrease the stress by changing the test values

5. Exit criteria

1. There should not be any fatal failures
2. No unwanted lock time outs or deadlocks
3. No NPEs
4. No unwanted Exceptions
5. No Out of memory Errors

6. How to run the test

1. Make sure you have java in your environment and put the Derby jars including the derbyTesting.jar in your classpath
2. Then run 
          java org.apache.derbyTesting.system.sttest.Sttest 
   If you want to get the test output to a file then run 
          java org.apache.derbyTesting.system.sttest.Sttest>sttest.out 2>&1
   Which will print the output to a file called sttest.out including warnings and errors
 
There is no explicit exit condition.  This test usually runs for more than 2 weeks. Check for ERROR and Exception in both the out files and derby.log.

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

mailjdbc package - mail client scenario test

Contents

1. About the Mailjdbc test
2. Design
3. Schema
4. Test goals
5. Exit criteria
6. How to run the test

1. About the Mailjdbc test

 This test will mimic how a typical mail client can  use Derby as their database

2. Design

There will be a total of 4 threads to mimic a single users mail client activity
1. User activity on the inbox
	How the user uses his inbox. Like reading, deleting, moving to folders etc.
2. Purging  ( deleting the mails which are older than the specified date)
	This thread will delete the mails which are older than n days, (where n can be any number of days)
3. Backup
	Back up thread will back up the mail db at every day or so
4. Syncing (adding random number of rows on a particular time interval)
	This thread will be mimicking the refresh mechanism on the inbox. After each refresh a random number of rows will be added to the db.


The test will also do periodic dump of the database size  using the size of the files
Start with no data and grow up to the maximum size  and then at some point the db size should flatten out 

3. Schema

The schema for this test is as follows

Tables

1.Table name : 	Inbox
	Columns: 	id (generated key) Primary key
			From (string)
			To (String)
			Date (String)
			Message (CLOB)
			folder_id (Integer)
2.Table name : 	Attachment
	Columns: 	id (Integer) foreign key
			Attach_id(Integer)
			attachment (Blob)
Id + attach_id - Primary key

3.Table name : 	Folder
	Columns: 	folderid (generated key)
			foldername(string)
			
4. Test goals

Since the test is going to run continuously with the 4 threads , the duration of about 2 weeks will mimic more or less of about 4 months of user activity on a mail client. The stress on the test can be increased by  increasing the size of the data instead of increasing the number of connections. These are the two methods that the test is going to use for increasing the stress

Increasing the size of the file attachments
Reducing the size of the virtual memory

For both conditions the performance of the database should be about the same. 

5. Exit criteria

1. There should not be any fatal failures
2. No unwanted lock time outs or deadlocks


6. How to run the test

1. Make sure you have java in your environment and put the Derby jars including the derbyTesting.jar in your classpath
2. Make sure to copy the derby.properties file provided under the directory org/apache/derbyTesting/system/mailjdbc
3. Run java org.apache.derbyTesting.system.mailjdbc.MailJdbc embedded
 or
 java org.apache.derbyTesting.system.mailjdbc.MailJdbc NetworkServer (To run in the NetworkServer mode you should start the n/w server manually)

There are 2 output files. 
1. Activity.out - which will give all the activities done by the test
2. Performance.out - which will give you the time for each transaction.
 
There is no explicit exit condition.  This test usually runs for more than 2 weeks. Check for ERROR and Exception in both the out files
and derby.log.

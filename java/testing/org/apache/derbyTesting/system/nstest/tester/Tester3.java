/*

 Derby - Class org.apache.derbyTesting.system.nstest.tester.Tester3

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

package org.apache.derbyTesting.system.nstest.tester;

import java.sql.Connection;

import org.apache.derbyTesting.system.nstest.NsTest;

/**
 * Tester3 - Threads that opens/closes the connection to the database after each query
 */
public class Tester3 extends TesterObject {

	//*******************************************************************************
	//
	// Constructor. Get's the name of the thread running this for use in messages
	//
	//*******************************************************************************
	public Tester3(String name) {
		super(name);
	}

	//*********************************************************************************
	//
	// This starts the acutal test operations.  Overrides the startTesting() of parent.
	// Tester3 profile -
	//     Query only kind of client that deals with a large result
	//     set based on a select query that returns a large number of
	//     rows (stress condition).  Connection is closed after each
	//     query. The query will need to run in a DIRTY_READ mode, i.e.
	//     READ UNCOMMITTED isolation level.  We work over the untouched
	//		portion of rows in the table (i.e. serialkey 1 to NUM_UNTOUCHED_ROWS)
	//
	//*********************************************************************************
	public void startTesting() {

		//The following loop will be done nstest.MAX_ITERATIONS times after which we exit the thread
		// Note that a different connection is used for each operation.  The purpose of this client is
		// to work on a large set of data as defined by the parameter NUM_HIGH_STRESS_ROWS
		// This thread could be made to pause (sleep) for a bit between each iteration.
		for (int i = 0; i < NsTest.MAX_ITERATIONS; i++) {
			//Get the connection.  It will be closed at the end of the loop
			connex = getConnection();
			if (connex == null) {
				System.out.println("FAIL: " + getThread_id()
						+ " could not get the database connection");
				return; //quit
			}

			// set isolation level to Connection.TRANSACTION_READ_UNCOMMITTED to reduce number of
			// deadlocks/lock issues
			setIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED);

			//Now select nstest.NUM_HIGH_STRESS_ROWS number of rows
			try {
				int numSelected = doSelectOperation(NsTest.NUM_HIGH_STRESS_ROWS);
				System.out.println(getThread_id()+" Tester3: Rows selected "+numSelected);
			} catch (Exception e) {
				System.out.println("doSelect in thread " + getThread_id()
						+ " threw ");
				printException("doSelectOperation() in Tester3 of "+getThread_id(), e);
			}

			//close the connection
			closeConnection();

			//Thread.sleep(10 * 60000); //10 minutes

		}//end of for (int i=0;...)

		System.out.println("Thread " + getThread_id() + " is now terminating");

	}//end of startTesting()

}

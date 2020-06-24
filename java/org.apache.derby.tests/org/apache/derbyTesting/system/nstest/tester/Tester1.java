/*

 Derby - Class org.apache.derbyTesting.system.nstest.tester.Tester1

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
 * Tester1 - Threads that keep the connection to the database open forever
 */
public class Tester1 extends TesterObject {

	// *******************************************************************************
	//
	// Constructor. Get's the name of the thread running this for use in
	// messages
	//
	// *******************************************************************************
	public Tester1(String name) {
		super(name);
	}

	// *********************************************************************************
	//
	// This starts the acutal test operations. Overrides the startTesting() of
	// parent.
	// Tester1 profile -
	// The connection to the database is open forever. This client will do 
	// Insert/Update/Delete and simple Select queries over a small to medium 
	// set of data determined randomly over MAX_LOW_STRESS_ROWS rows. 
	// Autocommit is left on else too many deadlocks occur and the goal is to
	// test the data flow and connection management of the network server, not
	// the transaction management of the database.
	//
	// *********************************************************************************
	public void startTesting() {

		// this connection will remain open forever.
		connex = getConnection();
		if (connex == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println("FAIL: " + getThread_id()
					+ " could not get the database connection");
			return; // quit
		}

		// set autocommit to false to keep transaction control in your hand
		// Too many deadlocks amd locking issues if this is not commented out
		try {
			connex.setAutoCommit(false);
		} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println("FAIL: " + getThread_id()
					+ "'s setAutoCommit() failed:");
			printException("setting AutoCommit", e);

            // if you can't change the autocommit state, the connection is unusable.
            // get out of here.
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
            return;
		}

		// also set isolation level to Connection.TRANSACTION_READ_UNCOMMITTED
		// to reduce number of deadlocks
		setIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED);

		// The following loop will be done nstest.MAX_ITERATIONS times after
		// which we exit the thread.
		// Note that the same connection is used for each operation. The
		// connection is only closed outside the loop. Since autocommit is on,
		// we make an interation work over MAX_LOW_STRESS_ROWS number of rows.
		// This thread could be made to pause (sleep) for a bit between each
		// iteration.
		for (int i = 0; i < NsTest.MAX_ITERATIONS; i++) {

			// Now loop through nstest.MAX_LOW_STRESS_ROWS number of times/rows
			// before committing.
			// Here, we do randomly do either insert/update/delete operations or
			// one select
			int rnum = (int) (Math.random() * 100) % 4; // returns 0, 1, 2, 3
			switch (rnum) {
			case 0: // do a select operation
				try {
					int numSelected = doSelectOperation(NsTest.MAX_LOW_STRESS_ROWS);
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
					NsTest.logger.println(getThread_id() + " selected "
							+ numSelected + " rows");
				} catch (Exception e) {
					NsTest.logger
							.println("--> Isolation Level is TRANSACTION_READ_UNCOMMITTED, hence SHOULD NOT FAIL ********* doSelect in thread "
									+ getThread_id() + " threw " + e);
					printException("doSelectOperation()", e);
                    if ( NsTest.justCountErrors() ) { NsTest.printException( Tester1.class.getName(), e ); }
					else { e.printStackTrace( NsTest.logger ); }

                    // if the connection is dead, there's no point in hanging around
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
                    if ( NsTest.deadConnection( e ) ) { return; }
				}
				break;

			case 1: // do Insert/Update/Delete operations
			case 2: // do Insert/Update/Delete operations
			case 3: // do Insert/Update/Delete operations
				for (int j = 0; j < NsTest.MAX_LOW_STRESS_ROWS; j++) {
					doIUDOperation();
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
                    if ( deadConnection() ) { return; }
				}
				break;
			}

			// Letting this be even though autocommit is on so that if later on
			// if we decide to turn autocommit off, this automatically takes
            // effect.
			// commit
			try {
				connex.commit();
			} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
				NsTest.logger
						.println("FAIL: " + getThread_id() + "'s commit() failed:");
				printException("committing Xn in Tester1", e);

                // if you can't commit, the connection is unusable.
                // get out of here.
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
                return;
			}
		}// end of for (int i=0;...)

		// close the connection before the thread terminates
		closeConnection();
		NsTest.logger.println("Thread " + getThread_id()+ " is now terminating");
//IC see: https://issues.apache.org/jira/browse/DERBY-6533

	}//end of startTesting()

}

/*

 Derby - Class org.apache.derbyTesting.system.nstest.init.Initializer

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

package org.apache.derbyTesting.system.nstest.init;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;

import org.apache.derbyTesting.system.nstest.NsTest;
import org.apache.derbyTesting.system.nstest.utils.DbUtil;

/**
 * Initializer: Main Class that populates the tables needed for the test
 */
public class Initializer {

	private String thread_id;

	private DbUtil dbutil;

	public Initializer(String name) {
		this.thread_id = name;
		dbutil = new DbUtil(this.thread_id);
	}

	// This starts the acutal inserts
	public void startInserts() {

		Connection conn = null;
		int insertsRemaining = NsTest.MAX_INITIAL_ROWS;

		// The JDBC driver should have been loaded by nstest.java at this
		// point, we just need to get a connection to the database
		try {

//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println(thread_id
					+ " is getting a connection to the database...");

			if (NsTest.embeddedMode) {
				conn = DriverManager.getConnection(NsTest.embedDbURL,
						NsTest.prop);
			} else {
				if(NsTest.driver_type.equalsIgnoreCase("DerbyClient")) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
					NsTest.logger.println("-->Using derby client url");
					conn = DriverManager.getConnection(NsTest.clientDbURL,
							NsTest.prop);
				}
			}
		} catch (Exception e) {
			NsTest.logger.println("FAIL: " + thread_id
					+ " could not get the database connection");
			printException("getting database connection in startInserts()", e);
		}

		// add one to the statistics of client side connections made per jvm
		NsTest.addStats(NsTest.CONNECTIONS_MADE, 1);
		NsTest.logger.println("Connection number: " + NsTest.numConnections);

		// set autocommit to false to keep transaction control in your hand
		if (NsTest.AUTO_COMMIT_OFF) {
			try {

				conn.setAutoCommit(false);
			} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
				NsTest.logger.println("FAIL: " + thread_id
						+ "'s setAutoCommit() failed:");
				printException("setAutoCommit() in Initializer", e);
			}
		}

		while (insertsRemaining-- >= 0) {
			try {
				int numInserts = dbutil.add_one_row(conn, thread_id);
				//NsTest.logger.println("Intializer.java: exited add_one_row: "
				//		+ numInserts + " rows");
			} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
				NsTest.logger.println(" FAIL: " + thread_id
						+ " unexpected exception:");
				printException("add_one_row() in Initializer", e);
				break;
			}
		}// end of while(insertsRemaning-- > 0)

		// commit the huge bulk Insert!
		if (NsTest.AUTO_COMMIT_OFF) {
			try {
				conn.commit();
			} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
				NsTest.logger
						.println("FAIL: " + thread_id + "'s commit() failed:");
				printException("commit in Initializer", e);
			}
		}

	}// end of startInserts()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public synchronized void printException(String where, Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
        if ( NsTest.justCountErrors() )
        {
            NsTest.addError( e );
            return;
        }

		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;

			if (se.getSQLState().equals("40001"))
				NsTest.logger.println("deadlocked detected");
			if (se.getSQLState().equals("40XL1"))
				NsTest.logger.println(" lock timeout exception");
			if (se.getSQLState().equals("23500"))
				NsTest.logger.println(" duplicate key violation");
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				NsTest.logger.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-5465
		if (e.getMessage() == null) {
			e.printStackTrace( NsTest.logger );
		}
		NsTest.logger.println("During - " + where
				+ ", the exception thrown was : " + e.getMessage());
	}

}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.RelativeTest

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

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.*;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

/**
 * Tests relative scrolling of a resultset. This is the JUnit conversion of
 * jdbcapi/testRelative test.
 */
public class RelativeTest extends BaseJDBCTestCase {

	public RelativeTest(String name) {
		super(name);
	}

	/**
	 * Test relative scrolling of ResultSet with concurrency set to
	 * CONCUR_READ_ONLY.
	 */
	public void testScrolling_CONCUR_READ_ONLY() throws SQLException {
		int concurrency = ResultSet.CONCUR_READ_ONLY;
		Statement stmt1 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				concurrency);
		ResultSet rs = stmt1.executeQuery("select * from testRelative");

		rs.next(); // First Record
		assertEquals("work1", rs.getString("name"));
		rs.relative(2);
		assertEquals("work3", rs.getString("name"));
		assertEquals(false, rs.isFirst());
		assertEquals(false, rs.isLast());
		assertEquals(false, rs.isAfterLast());
		rs.relative(-2);
		assertEquals("work1", rs.getString("name"));

		rs.relative(10);
		try {
			/*
			 * Attempting to move beyond the first/last row in the result set
			 * positions the cursor before/after the the first/last row.
			 * Therefore, attempting to get value will throw an exception.
			 */
			rs.getString("name");
			fail("FAIL - Attempting to read from an invalid row should have " +
					"thrown an exception");
		} catch (SQLException sqle) {
			/**
			 * sets the expected sql state for the expected exceptions,
			 * according to return value of usingDerbyNetClient().
			 */
			String NO_CURRENT_ROW_SQL_STATE = "";
			if (usingDerbyNetClient()) {
				NO_CURRENT_ROW_SQL_STATE = "XJ121";
			} else {
				NO_CURRENT_ROW_SQL_STATE = "24000";
			}
			assertSQLState(NO_CURRENT_ROW_SQL_STATE, sqle);
		}
	}

	/**
	 * Test relative scrolling of ResultSet with concurrency set to
	 * CONCUR_UPDATABLE.
	 */
	public void testScrolling_CONCUR_UPDATABLE() throws SQLException {
		int concurrency = ResultSet.CONCUR_UPDATABLE;
		Statement stmt1 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				concurrency);
		ResultSet rs = stmt1.executeQuery("select * from testRelative");

		rs.next(); // First Record
		assertEquals("work1", rs.getString("name"));
		rs.relative(2);
		assertEquals("work3", rs.getString("name"));
		assertEquals(false, rs.isFirst());
		assertEquals(false, rs.isLast());
		assertEquals(false, rs.isAfterLast());
		rs.relative(-2);
		assertEquals("work1", rs.getString("name"));

		rs.relative(10);
		try {
			/*
			 * Attempting to move beyond the first/last row in the result set
			 * positions the cursor before/after the the first/last row.
			 * Therefore, attempting to get value now will throw an exception.
			 */
			rs.getString("name");
			fail("FAIL - Attempting to read from an invalid row should have " +
				"thrown an exception");
		} catch (SQLException sqle) {
			/**
			 * sets the expected sql state for the expected exceptions,
			 * according to return value of usingDerbyNetClient().
			 */
			String NO_CURRENT_ROW_SQL_STATE = "";
			if (usingDerbyNetClient()) {
				NO_CURRENT_ROW_SQL_STATE = "XJ121";
			} else {
				NO_CURRENT_ROW_SQL_STATE = "24000";
			}
			assertSQLState(NO_CURRENT_ROW_SQL_STATE, sqle);
		}
	}

	/**
	 * Runs the test fixtures in embedded and client.
	 * 
	 * @return test suite
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("RelativeTest");
		suite.addTest(baseSuite("RelativeTest:embedded"));
		suite.addTest(TestConfiguration
				.clientServerDecorator(baseSuite("RelativeTest:client")));
		return suite;
	}

	/**
	 * Base suite of tests that will run in both embedded and client.
	 * 
	 * @param name
	 *            Name for the suite.
	 */
	private static Test baseSuite(String name) {
		TestSuite suite = new TestSuite(name);
		suite.addTestSuite(RelativeTest.class);
		return new CleanDatabaseTestSetup(DatabasePropertyTestSetup
				.setLockTimeouts(suite, 2, 4)) {

			/**
			 * Creates the tables used in the test cases.
			 * 
			 * @exception SQLException
			 *                if a database error occurs
			 */
			protected void decorateSQL(Statement stmt) throws SQLException {
				stmt.execute("create table testRelative("
						+ "name varchar(10), i int)");

				stmt.execute("insert into testRelative values ("
						+ "'work1', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work2', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work3', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work4', NULL)");
			}
		};
	}
}

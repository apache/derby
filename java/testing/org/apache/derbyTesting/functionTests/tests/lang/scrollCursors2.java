/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.scrollCursors2

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.IOException;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * Test of scroll cursors.
 *
 * @author Jerry Brenner
 */

public class scrollCursors2 { 

        private static boolean isDerbyNetClient = false;
    
	public static void main(String[] args) {
		boolean		passed = true;
		Connection	conn = null;
		Statement	s_i_r = null;

		/* Run all parts of this test, and catch any exceptions */
		try {
			System.out.println("Test scrollCurors2 starting");

                        isDerbyNetClient = TestUtil.isDerbyNetClientFramework();
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();
			cleanUp(conn);	
			conn.setAutoCommit(false);

			/* Create the table and do any other set-up */
			passed = passed && setUpTest(conn);

			// Negative tests with forward only cursors.
			passed = passed && forwardOnlyNegative(conn);

			// Positive tests with forward only cursors.
			passed = passed && forwardOnlyPositive(conn);

			// Tests with scroll sensitive cursors
			passed = passed && scrollSensitiveTest(conn);

			// Positive tests for scroll insensitive cursors
			passed = passed && scrollInsensitivePositive(conn);

			// Negative tests for scroll insensitive cursors
			passed = passed && scrollInsensitiveNegative(conn);

			// "test" scrolling and CallableStatements
			passed = passed && testCallableStatements(conn);

			// tests for PreparedStatement.getMetaData()
			passed = passed && getMetaDataTests(conn);
                        
                        // test scrollable with different maxRows and fetchSize
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 10, 10);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 10, 5);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 10, 0);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 0, 0);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 0, 5);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 0, 10);
                        passed = passed && scrollVerifyMaxRowWithFetchSize(conn, 0, 15);
                        
		} 
		catch (SQLException se) 
		{
			passed = false;
			dumpSQLExceptions(se);
		} 
		catch (Throwable e) 
		{
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
			passed = false;
		} 
		finally 
		{

			/* Test is finished - clean up after ourselves */
			passed = passed && cleanUp(conn, s_i_r);
		}

		if (passed)
			System.out.println("PASS");

		System.out.println("Test scrollCursors2 finished");
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace();
			se = se.getNextException();
		}
	}

	/**
	 * Set up the test.
	 *
	 * This method creates the table used by the rest of the test.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean setUpTest(Connection conn)
					throws SQLException 
	{
		boolean	passed = true;
		int		rows;
		PreparedStatement	ps;
		Statement			s_i_r;

		s_i_r = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_READ_ONLY);

		/* Create a table */
		s_i_r.execute("create table t (i int, c50 char(50))");

	    /* Populate the table */
		s_i_r.execute("insert into t (i) values (2), (3), (4), (5), (6)");
		s_i_r.execute("update t set c50 = RTRIM(CAST (i AS CHAR(50)))");
		s_i_r.close();

		return passed;
	}

	/**
	 * Negative tests for forward only cursors.
	 *
	 * This method tests forward only cursors.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean forwardOnlyNegative( Connection conn)
		throws SQLException 
	{
		boolean		passed = true;
		PreparedStatement	ps_f_r = null;
		ResultSet	rs;
		SQLWarning	warning;
		Statement	s_f_r = null;

		s_f_r = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
									 ResultSet.CONCUR_READ_ONLY);
		// We should have gotten no warnings and a read only forward only cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();

		// Verify that setMaxRows(-1) fails
		try
		{
			s_f_r.setMaxRows(-1);
			// Should never get here
			System.out.println("setMaxRows(-1) expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ063");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}
		// Verify maxRows still 0
		if (s_f_r.getMaxRows() != 0)
		{
			System.out.println("getMaxRows() expected to return 0");
			passed = false;
		}

		// Verify that result set from statement is 
		// scroll insensitive and read only
		rs = s_f_r.executeQuery("select * from t");
		if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY)
		{
			System.out.println("cursor type = " + rs.getType() +
							   ", not " + ResultSet.TYPE_FORWARD_ONLY);
		}
		if (rs.getConcurrency() != ResultSet.CONCUR_READ_ONLY)
		{
			System.out.println("concurrency = " + rs.getConcurrency() +
							   ", not " + ResultSet.CONCUR_READ_ONLY);
		}

		// Verify that first(), etc. don't work
		try
		{
			rs.first();
			// Should never get here
			System.out.println("first() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		try
		{
			rs.beforeFirst();
			// Should never get here
			System.out.println("beforeFirst() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		try
		{
			rs.isBeforeFirst();
			// Should never get here
			System.out.println("isBeforeFirst() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		try
		{
			rs.isAfterLast();
			// Should never get here
			System.out.println("isAfterLast() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		try
		{
			rs.isFirst();
			// Should never get here
			System.out.println("isFirst() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}
		try
		{
			rs.isLast();
			// Should never get here
			System.out.println("isLast() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}
		try
		{
			rs.absolute(1);
			// Should never get here
			System.out.println("absolute() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}
		try
		{
			rs.relative(1);
			// Should never get here
			System.out.println("relative() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}

		// setFetchDirection should fail
		try
		{
			rs.setFetchDirection(ResultSet.FETCH_FORWARD);
			// Should never get here
			System.out.println("setFetchDirection() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }
		}

		/* Book says that getFetchDirection(), getFetchSize() and
		 * setFetchSize() are all okay.
		 */
		if ((rs.getFetchSize() != 1 && !isDerbyNetClient) || (rs.getFetchSize() != 0 && isDerbyNetClient))
	 	{
                        if (!isDerbyNetClient) {
                            System.out.println("getFetchSize() expected to return 1");
                        } else {
                            System.out.println("getFetchSize() expected to return 0");
                        }
                        passed = false;
		}
		rs.setFetchSize(5);
		if (rs.getFetchSize() != 5)
	 	{
			System.out.println("getFetchSize() expected to return 5");
			passed = false;
		}

		if (rs.getFetchDirection() != ResultSet.FETCH_FORWARD)
		{
			System.out.println(
				"getFetchDirection() expected to return FETCH_FORWARD, not " +
				rs.getFetchDirection());
			passed = false;
		}

		rs.close();
		s_f_r.close();

		ps_f_r = conn.prepareStatement(
									 "select * from t",
									 ResultSet.TYPE_FORWARD_ONLY,
									 ResultSet.CONCUR_READ_ONLY);
		// We should have gotten no warnings and a read only forward only cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();

		// Verify that result set from statement is 
		// scroll insensitive and read only
		rs = ps_f_r.executeQuery();
		if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY)
		{
			System.out.println("cursor type = " + rs.getType() +
							   ", not " + ResultSet.TYPE_FORWARD_ONLY);
		}
		if (rs.getConcurrency() != ResultSet.CONCUR_READ_ONLY)
		{
			System.out.println("concurrency = " + rs.getConcurrency() +
							   ", not " + ResultSet.CONCUR_READ_ONLY);
		}

		// Verify that first() doesn't work
		try
		{
			rs.first();
			// Should never get here
			System.out.println("first() expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ061");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		rs.close();
		ps_f_r.close();

		return passed;
	}

	/**
	 * Positive tests for forward only cursors.
	 *
	 * This method tests forward only cursors.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean forwardOnlyPositive( Connection conn)
		throws SQLException 
	{
		boolean		passed = true;
		ResultSet	rs;
		SQLWarning	warning;
		Statement	s_f_r = null;

		s_f_r = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
									 ResultSet.CONCUR_READ_ONLY);
		// We should have gotten no warnings and a read only forward only cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();

		// Verify that setMaxRows(4) succeeds
		s_f_r.setMaxRows(5);
		if (s_f_r.getMaxRows() != 5)
		{
			System.out.println("getMaxRows() expected to return 5");
			passed = false;
		}
		rs = s_f_r.executeQuery("values 1, 2, 3, 4, 5, 6");
		if (rs == null)
		{
			System.out.println("rs expected to be non-null.");
			passed = false;
		}
		// Iterate straight thru RS, expect only 5 rows.
		for (int index = 1; index < 6; index++)
		{
			if (! rs.next())
			{
				System.out.println("rs.next() failed, index = " + index);
				passed = false;
				break;
			}
		}
		// We should not see another row (only 5, not 6)
		if (rs.next())
		{
			System.out.println("rs.next() failed, should not have seen 6th row.");
			passed = false;
		}
		rs.close();
		s_f_r.close();
		return passed;
	}

	/**
	 * Scroll sensitive cursor tests
	 *
	 * This method tests scroll sensitive cursors.
	 * (Not implemented, so we should get back
	 * scroll insensitive curors with read only concurrency.) 
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean scrollSensitiveTest( Connection conn)
		throws SQLException 
	{
		ResultSet	rs;
		SQLWarning	warning;
		Statement	s_s_r = null; // sensitive, read only
		Statement	s_s_u = null; // sensitive, updatable


		s_s_r = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
									 ResultSet.CONCUR_READ_ONLY);

		// We should have gotten a warning and a scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();

		// Verify that result set from statement is 
		// scroll insensitive and read only
		rs = s_s_r.executeQuery("select * from t");
		if (rs.getType() != ResultSet.TYPE_SCROLL_INSENSITIVE)
		{
			System.out.println("cursor type = " + rs.getType() +
							   ", not " + ResultSet.TYPE_SCROLL_INSENSITIVE);
		}
		if (rs.getConcurrency() != ResultSet.CONCUR_READ_ONLY)
		{
			System.out.println("concurrency = " + rs.getConcurrency() +
							   ", not " + ResultSet.CONCUR_READ_ONLY);
		}
		rs.close();

		// Close the statement
		s_s_r.close();

		s_s_u = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
									 ResultSet.CONCUR_UPDATABLE);
		// We should have gotten 2 warnings and a read only scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();

		// Verify that result set from statement is 
		// scroll insensitive and read only
		rs = s_s_u.executeQuery("select * from t");
		if (rs.getType() != ResultSet.TYPE_SCROLL_INSENSITIVE)
		{
			System.out.println("cursor type = " + rs.getType() +
							   ", not " + ResultSet.TYPE_SCROLL_INSENSITIVE);
		}
		if (rs.getConcurrency() != ResultSet.CONCUR_READ_ONLY)
		{
			System.out.println("concurrency = " + rs.getConcurrency() +
							   ", not " + ResultSet.CONCUR_READ_ONLY);
		}
		rs.close();


		return true;
	}

	/**
 	 * Positive tests for scroll insensitive cursor.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean scrollInsensitivePositive( Connection conn)
		throws SQLException 
	{
		boolean 	passed = true;
		PreparedStatement ps_i_r = null;
		PreparedStatement ps_i_u = null;
		ResultSet	rs;
		SQLWarning	warning;
		Statement	s_i_r = null; // insensitive, read only
		Statement	s_i_u = null; // insensitive, updatable


		s_i_r = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_READ_ONLY);

		// We should not have gotten any warnings 
		// and should have gotten a scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("unexpected warning = " + warning);
			warning = warning.getNextWarning();
			passed = false;
		}
		conn.clearWarnings();

		// run a query
		rs = s_i_r.executeQuery("select * from t");
		// verify scroll insensitive and read only
		if (rs.getType() != ResultSet.TYPE_SCROLL_INSENSITIVE)
		{
			System.out.println(
				"rs.getType() expected to return TYPE_SCROLL_INSENSITIVE, not " +
				rs.getType());
			passed = false;
		}
		if (rs.getConcurrency() != ResultSet.CONCUR_READ_ONLY)
		{
			System.out.println(
				"rs.getConcurrency() expected to return CONCUR_READ_ONLY, not " +
				rs.getConcurrency());
			passed = false;
		}
		
		// We should be positioned before the 1st row
		if (! rs.isBeforeFirst())
		{
			System.out.println("expected to be before the 1st row");
			passed = false;
		}
                if (rs.absolute(0))
                {
			System.out.println("absolute(0) expected to return false");
			passed = false;
                }
		if (! rs.isBeforeFirst())
		{
			System.out.println("still expected to be before the 1st row");
			passed = false;
		}
		// go to first row
		if (! rs.first())
		{
			System.out.println("expected first() to succeed");
			passed = false;
		}
		if (rs.getInt(1) != 2)
		{
			System.out.println(
				"rs.getInt(1) expected to return 2, not " + rs.getInt(1));
			passed = false;
		}
		if (! rs.isFirst())
		{
			System.out.println("expected to be on the 1st row");
			passed = false;
		}
		// move to before first
		rs.beforeFirst();
		if (! rs.isBeforeFirst())
		{
			System.out.println("expected to be before the 1st row");
			passed = false;
		}
		// move to last row
		if (! rs.last())
		{
			System.out.println("expected last() to succeed");
			passed = false;
		}
		if (! rs.isLast())
		{
			System.out.println("expected to be on the last row");
			passed = false;
		}
		if (rs.isAfterLast())
		{
			System.out.println("not expected to be after the last row");
			passed = false;
		}
		if (rs.getInt(1) != 6)
		{
			System.out.println(
				"rs.getInt(1) expected to return 6, not " + rs.getInt(1));
			passed = false;
		}
		if (rs.next())
		{
			System.out.println("not expected to find another row");
			passed = false;
		}
		if (! rs.isAfterLast())
		{
			System.out.println("expected to be after the last row");
			passed = false;
		}

		// We're after the last row, verify that only isAfterLast()
		// returns true
		if (rs.isLast())
		{
			System.out.println("not expected to be on the last row");
			passed = false;
		}
		if (rs.isFirst())
		{
			System.out.println("not expected to be on the first row");
			passed = false;
		}
		if (rs.isBeforeFirst())
		{
			System.out.println("not expected to be before the first row");
			passed = false;
		}

		// get/setFetchDirection()
		if (rs.getFetchDirection() != ResultSet.FETCH_FORWARD)
		{
			System.out.println(
				"getFetchDirection() expected to return FETCH_FORWARD, not " +
				rs.getFetchDirection());
			passed = false;
		}
		rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
		if (rs.getFetchDirection() != ResultSet.FETCH_UNKNOWN)
		{
			System.out.println(
				"getFetchDirection() expected to return FETCH_UNKNOWN, not " +
				rs.getFetchDirection());
			passed = false;
		}

		// get/setFetchSize()
		if (
                        (rs.getFetchSize() != 1 && !isDerbyNetClient) || 
                        (rs.getFetchSize() != 64 && isDerbyNetClient))
		{
                        if (!isDerbyNetClient) {
                            System.out.println(
                                    "getFetchSize() expected to return 1, not " + rs.getFetchSize());
                        } else {
                            System.out.println(
                                    "getFetchSize() expected to return 64, not " + rs.getFetchSize());
                        }
			passed = false;
		}
		rs.setFetchSize(5);
		if (rs.getFetchSize() != 5)
		{
			System.out.println(
				"getFetchSize() expected to return 5, not " + rs.getFetchSize());
			passed = false;
		}
		// setFetchSize() to 0 should have no effect.
                // for client server, fetchSize should have to 64
		rs.setFetchSize(0);
		if (
                        (rs.getFetchSize() != 5 && !isDerbyNetClient) || 
                        (rs.getFetchSize() != 64 && isDerbyNetClient))
		{
                        if (!isDerbyNetClient) {
                            System.out.println(
				"getFetchSize() expected to return 5, not " + rs.getFetchSize());
                        } else {
                            System.out.println(
				"getFetchSize() expected to return 64, not " + rs.getFetchSize());
                        }

			passed = false;
		}
		// done
		rs.close();


		// Scroll insensitive and updatable
		s_i_u = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_UPDATABLE);

		// We should have gotten 1 warning 
		// and a read only scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();
		s_i_u.close();

		ps_i_r = conn.prepareStatement(
									 "select * from t",
									 ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_READ_ONLY);

		// We should not have gotten any warnings 
		// and should have gotten a prepared scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("unexpected warning = " + warning);
			warning = warning.getNextWarning();
			passed = false;
		}
		conn.clearWarnings();

		rs = ps_i_r.executeQuery();
		// make sure it's scrollable
		rs.last();
		rs.close();
		ps_i_r.close();

		ps_i_u = conn.prepareStatement(
									 "select * from t",
									 ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_UPDATABLE);

		// We should have gotten 1 warning 
		// and a read only scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
		}
		conn.clearWarnings();
		ps_i_u.close();

		// Check setMaxRows()/getMaxRows()
		if (s_i_r.getMaxRows() != 0)
		{
			System.out.println("getMaxRows() expected to return 0");
			passed = false;
		}
		s_i_r.setMaxRows(5);
		if (s_i_r.getMaxRows() != 5)
		{
			System.out.println("getMaxRows() expected to return 5");
			passed = false;
		}
		rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
		if (rs == null)
		{
			System.out.println("rs expected to be non-null.");
			passed = false;
		}
		// Iterate straight thru RS, expect only 5 rows.
		for (int index = 1; index < 6; index++)
		{
			if (! rs.next())
			{
				System.out.println("rs.next() failed, index = " + index);
				passed = false;
				break;
			}
		}
		// We should not see another row (only 5, not 6)
		if (rs.next())
		{
			System.out.println("rs.next() failed, should not have seen 6th row.");
			passed = false;
		}
		rs.close();
		// Jump around and verify setMaxRows() works.
		rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
		if (rs == null)
		{
			System.out.println("rs expected to be non-null.");
			passed = false;
		}
		if (!rs.last())
		{
			System.out.println("rs.last() failed.");
			passed = false;
		} 
		// Iterate backwards thru RS, expect only 4 more (5 total) rows.
		for (int index = 1; index < 5; index++)
		{
			if (! rs.previous())
			{
				System.out.println("rs.previous() failed, index = " + index);
				passed = false;
				break;
			}
		} 
		// We should not see another row (only 5, not 6)
		if (rs.previous())
		{
			System.out.println("rs.previous() failed, should not have seen 6th row.");
			passed = false;
		}
		rs.close();
		rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
		if (rs == null)
		{
			System.out.println("rs expected to be non-null.");
			passed = false;
		}
		rs.afterLast();
		// Iterate backwards thru RS, expect only 5 rows.
		for (int index = 1; index < 6; index++)
		{
			if (! rs.previous())
			{
				System.out.println("rs.previous() failed, index = " + index);
				passed = false;
				break;
			}
		}
		// We should not see another row (only 5, not 6)
		if (rs.previous())
		{
			System.out.println("rs.previous() failed, should not have seen 6th row.");
			passed = false;
		}
		rs.close();
		// Verify setting maxRows back to 0 works.
		s_i_r.setMaxRows(0);
		rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
		if (rs == null)
		{
			System.out.println("rs expected to be non-null.");
			passed = false;
		}
		// Iterate straight thru RS, expect 6 rows.
		for (int index = 1; index < 7; index++)
		{
			if (! rs.next())
			{
				System.out.println("rs.next() failed, index = " + index);
				passed = false;
				break;
			}
		}
		// We should not see another row 
		if (rs.next())
		{
			System.out.println("rs.next() failed, should not have seen another row.");
			passed = false;
		}
		rs.close();

		return passed;
	}

	/**
 	 * Negative tests for scroll insensitive cursor.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean scrollInsensitiveNegative( Connection conn)
		throws SQLException 
	{
		boolean 	passed = true;
		ResultSet	rs;
		SQLWarning	warning;
		Statement	s_i_r = null; // insensitive, read only

		s_i_r = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
									 ResultSet.CONCUR_READ_ONLY);

		// We should not have gotten any warnings 
		// and should have gotten a scroll insensitive cursor
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("unexpected warning = " + warning);
			warning = warning.getNextWarning();
			passed = false;
		}
		conn.clearWarnings();

		// Verify that setMaxRows(-1) fails
		try
		{
			s_i_r.setMaxRows(-1);
			// Should never get here
			System.out.println("setMaxRows(-1) expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ063");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}
		// Verify maxRows still 0
		if (s_i_r.getMaxRows() != 0)
		{
			System.out.println("getMaxRows() expected to return 0");
			passed = false;
		}

		// Empty result set
		rs = s_i_r.executeQuery("select * from t where 1=0");
		// isBeforeFirst() and isAfterLast() should always return false
		// when result set is empty
		if (rs.isBeforeFirst())
		{
			System.out.println("isBeforeFirst() expected to return false on empty result set");
			passed = false;
		}
		if (rs.next())
		{
			System.out.println("rs.next() expected to show result set is empty");
			passed = false;
		}
		if (rs.previous())
		{
			System.out.println("rs.previous() expected to show result set is empty");
			passed = false;
		}
		if (rs.isAfterLast())
		{
			System.out.println("isAfterLast() expected to return false on empty result set");
			passed = false;
		}
		if (rs.isFirst())
		{
			System.out.println("isFirst() expected to return false on empty result set");
			passed = false;
		}
		if (rs.isLast())
		{
			System.out.println("isLast() expected to return false on empty result set");
			passed = false;
		}

		if (rs.relative(0))
		{
			System.out.println("relative(0) expected to return false on empty result set");
			passed = false;
		}

		if (rs.relative(1))
		{
			System.out.println("relative(1) expected to return false on empty result set");
			passed = false;
		}

		if (rs.relative(-1))
		{
			System.out.println("relative(-1) expected to return false on empty result set");
			passed = false;
		}

		if (rs.absolute(0))
		{
			System.out.println("absolute(0) expected to return false on empty result set");
			passed = false;
		}
		if (rs.absolute(1))
		{
			System.out.println("absolute(1) expected to return false on empty result set");
			passed = false;
		}

		if (rs.absolute(-1))
		{
			System.out.println("absolute(-1) expected to return false on empty result set");
			passed = false;
		}


		rs.close();
		// End of empty result set tests

		// Non-empty result set
		rs = s_i_r.executeQuery("select * from t");
		// Negative fetch size
		try
		{
			rs.setFetchSize(-5);
			System.out.println("setFetchSize(-5) expected to fail");
			passed = false;
		}
		catch (SQLException sqle)
		{
			/* Check to be sure the exception is the one we expect */
                        if (!isDerbyNetClient) {
                            passed = passed && checkException(sqle, "XJ062");
                        } else {
                            System.out.println(sqle.getMessage());
                        }

		}

		s_i_r.close();

		return passed;
	}


	/**
	 * CallableStatement tests.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	public static boolean testCallableStatements( Connection conn)
		throws SQLException 
	{
		boolean		passed = true;
		int			warningCount = 0;
		SQLWarning	warning;
		CallableStatement	cs_s_r = null; // sensitive, read only
		CallableStatement	cs_s_u = null; // sensitive, updatable
		CallableStatement	cs_i_r = null; // insensitive, read only
		CallableStatement	cs_i_u = null; // insensitive, updatable
		CallableStatement	cs_f_r = null; // forward only, read only

		cs_s_r = conn.prepareCall(
								"values cast (? as Integer)",
								ResultSet.TYPE_SCROLL_SENSITIVE,
								ResultSet.CONCUR_READ_ONLY);

		// We should have gotten 1 warnings
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
			warningCount++;
		}
		if (warningCount != 1)
		{
			System.out.println("warningCount expected to be 1, not " + warningCount);
			passed = false;
		}
		conn.clearWarnings();
		cs_s_r.close();	

		cs_s_u = conn.prepareCall(
								"values cast (? as Integer)",
								ResultSet.TYPE_SCROLL_SENSITIVE,
								ResultSet.CONCUR_UPDATABLE);

		// We should have gotten 2 warnings
		warningCount = 0;
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
			warningCount++;
		}
		if (warningCount != 2)
		{
			System.out.println("warningCount expected to be 2, not " + warningCount);
			passed = false;
		}
		conn.clearWarnings();
		cs_s_u.close();	

		cs_i_r = conn.prepareCall(
								"values cast (? as Integer)",
								ResultSet.TYPE_SCROLL_INSENSITIVE,
								ResultSet.CONCUR_READ_ONLY);

		// We should have gotten 0 warnings
		warningCount = 0;
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
			warningCount++;
		}
		if (warningCount != 0)
		{
			System.out.println("warningCount expected to be 0, not " + warningCount);
			passed = false;
		}
		conn.clearWarnings();
		cs_i_r.close();	

		cs_i_u = conn.prepareCall(
								"values cast (? as Integer)",
								ResultSet.TYPE_SCROLL_INSENSITIVE,
								ResultSet.CONCUR_UPDATABLE);

		// We should have gotten 1 warnings
		warningCount = 0;
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
			warningCount++;
		}
		if (warningCount != 1)
		{
			System.out.println("warningCount expected to be 1, not " + warningCount);
			passed = false;
		}
		conn.clearWarnings();
		cs_i_u.close();	

		cs_f_r = conn.prepareCall(
								"values cast (? as Integer)",
								ResultSet.TYPE_FORWARD_ONLY,
								ResultSet.CONCUR_READ_ONLY);

		// We should have gotten 0 warnings
		warningCount = 0;
		warning = conn.getWarnings();
		while (warning != null)
		{
			System.out.println("warning = " + warning);
			warning = warning.getNextWarning();
			warningCount++;
		}
		if (warningCount != 0)
		{
			System.out.println("warningCount expected to be 0, not " + warningCount);
			passed = false;
		}
		conn.clearWarnings();
		cs_f_r.close();	

		return passed;
	}


	/**
 	 * Tests for PreparedStatement.getMetaData().
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean getMetaDataTests( Connection conn)
		throws SQLException 
	{
		boolean 	passed = true;
		PreparedStatement	ps_f_r = null; // forward only, read only
		ResultSet	rs;
		ResultSetMetaData rsmd_ps;
		ResultSetMetaData rsmd_rs;
		SQLWarning	warning;


		ps_f_r = conn.prepareStatement(
									 "select c50, i, 43 from t",
									 ResultSet.TYPE_FORWARD_ONLY,
									 ResultSet.CONCUR_READ_ONLY);

		rsmd_ps = ps_f_r.getMetaData();
		if (rsmd_ps == null)
		{
			System.out.println("rsmd_ps expected to be non-null");
			return false;
		}

		// Now get meta data from result set
		rs = ps_f_r.executeQuery();
		rsmd_rs = rs.getMetaData();
		if (rsmd_rs == null)
		{
			System.out.println("rsmd_rs expected to be non-null");
			return false;
		}

		// check column count
		if (rsmd_ps.getColumnCount() != rsmd_rs.getColumnCount())
		{
			System.out.println("column count expected to be same, not " +
							   rsmd_ps.getColumnCount() +
							   " and " +
							   rsmd_rs.getColumnCount());
			passed = false;
		}

		// get column name for 2nd column
		if (! rsmd_ps.getColumnName(2).equals(rsmd_rs.getColumnName(2)))
		{
			System.out.println("column name expected to be same, not " +
							   rsmd_ps.getColumnName(2) +
							   " and " +
							   rsmd_rs.getColumnName(2));
			passed = false;
		}

		if (rsmd_ps.isReadOnly(2) != rsmd_rs.isReadOnly(2))
		{
			System.out.println("isReadOnly() expected to be same, not " +
							   rsmd_ps.isReadOnly(2) +
							   " and " +
							   rsmd_rs.isReadOnly(2));
			passed = false;
		}
		
		rs.close();
		ps_f_r.close();

		return passed;
	}

        
	/**
 	 * Tests for maxRow and fetchSize with scrollable cursors
	 *
	 * @param conn	The connection to use.
         * @param maxRows The maxRows value to use
         * @param fetchSize The fetchSize value to use
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
        private static boolean scrollVerifyMaxRowWithFetchSize(Connection conn, int maxRows, int fetchSize) {
            ResultSet rs;
            boolean passed = true;
            Statement	s_i_r = null;

            try {
                s_i_r = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                s_i_r.setMaxRows(maxRows);

                // Execute query
                rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15");
                rs.setFetchSize(fetchSize);

                // this should not affect the ResultSet because
                s_i_r.setMaxRows(2);
                if (maxRows == 0)
                    maxRows = 15;

                if (rs == null)
                {
                        System.out.println("rs expected to be non-null.");
                        passed = false;
                }
                // Start from before first
                // Iterate straight thru RS, expect only maxRows rows.
                for (int index = 1; index < maxRows + 1; index++)
                {
                        if (! rs.next())
                        {
                                System.out.println("rs.next() failed, index = " + index);
                                passed = false;
                                break;
                        } else {
                                if (index != rs.getInt(1)) {
                                    System.out.println("Expected: " + index + 
                                            " not: " + rs.getInt(1));
                                }
                        }
                }
                // We should not see another row (only maxRows, not total)
                if (rs.next())
                {
                        System.out.println("Error with maxRows = " + maxRows + 
                                " and fetchSize = " + fetchSize + "\n" + 
                                "rs.next() failed, should not have seen " + 
                                (maxRows + 1) + "th row.");
                        passed = false;
                }

                // Start from first and verify maxRows
                if (!rs.first())
                {
                        System.out.println("rs.first() failed.");
                        passed = false;
                } 
                // Iterate forward thru RS, expect only (maxRows - 1) more rows.
                for (int index = 1; index < maxRows; index++)
                {
                        if (! rs.next())
                        {
                                System.out.println("rs.previous() failed, index = " + 
                                        index);
                                passed = false;
                                break;
                        } else {
                                if ((index + 1) != rs.getInt(1))
                                    System.out.println("Error with maxRows = " + 
                                            maxRows + " and fetchSize = " + 
                                            fetchSize + "\n" + "Error with maxRows = " + 
                                            maxRows + " and fetchSize = " + fetchSize + 
                                            "\n" + "Expected: " + (index + 1) + 
                                            " not: " + rs.getInt(1));
                        }
                } 
                // We should not see another row (only maxRows, not total)
                if (rs.next())
                {
                        System.out.println("Error with maxRows = " + maxRows + 
                                " and fetchSize = " + fetchSize + "\n" + 
                                "rs.next() failed, should not have seen " + 
                                (maxRows + 1) + "th row.");
                        passed = false;
                }

                // Start from afterLast and verify maxRows
                rs.afterLast();
                // Iterate backwards thru RS, expect only (maxRows - 1) rows.
                for (int index = 1; index < maxRows + 1; index++)
                {
                        if (! rs.previous())
                        {
                                System.out.println("rs.previous() failed, index = " + 
                                        index);
                                passed = false;
                                break;
                        } else {
                                if (((maxRows - index) + 1) != rs.getInt(1)) {
                                    System.out.println("Error with maxRows = " + maxRows + 
                                            " and fetchSize = " + fetchSize + "\n" + 
                                            "Expected: " + ((maxRows - index) + 1) + 
                                            " not: " + rs.getInt(1));
                                }
                        }
                }
                // We should not see another row (only maxRows, not total)
                if (rs.previous())
                {
                        System.out.println("Error with maxRows = " + maxRows + 
                                " and fetchSize = " + fetchSize + "\n" + 
                                "rs.previous() failed, should not have seen " + 
                                (maxRows + 1) + "th row.");
                        passed = false;
                }

                // Start from last and verify maxRows
                if (!rs.last())
                {
                        System.out.println("rs.last() failed.");
                        passed = false;
                } 
                // Iterate backwards thru RS, expect only (maxRows - 1) more rows.
                for (int index = 1; index < maxRows; index++)
                {
                        if (! rs.previous())
                        {
                                System.out.println("rs.previous() failed, index = " + 
                                        index);
                                passed = false;
                                break;
                        } else {
                                if ((maxRows - index) != rs.getInt(1)) {
                                    System.out.println("Error with maxRows = " + maxRows + 
                                            " and fetchSize = " + fetchSize + "\n" + 
                                            "Expected: " + (maxRows - index) + " not: " + 
                                            rs.getInt(1));
                                }
                        }
                } 
                // We should not see another row (only 5, not 6)
                if (rs.previous())
                {
                        System.out.println("Error with maxRows = " + maxRows + 
                                " and fetchSize = " + fetchSize + "\n" + 
                                "rs.previous() failed, should not have seen " + 
                                (maxRows + 1) + "th row.");
                        passed = false;
                }

                rs.last();
                int rows = rs.getRow();
                
                rs.absolute(rows/2);
                if (rs.relative(-1 * (rows))) {
                    System.out.println("relative(" + -1 * (rows) + ") should return false, position outside of the resultSet");
                    
                }
                if (!rs.isBeforeFirst()) {
                    System.out.println("isBeforeFirst should be true");
                }

                rs.absolute(rows/2);
                if (rs.relative(rows)) {
                    System.out.println("relative(" + (rows) + ") should return false, position outside of the resultSet");
                }
                if (!rs.isAfterLast()) {
                    System.out.println("isAfterLast should be true");
                }
                rs.absolute(rows/2);
                if (rs.absolute(rows + 1)) {
                    System.out.println("absolute(" + (rows + 1) + ") should return false, position outside of the resultSet");
                    System.out.println("Current row: " + rs.getInt(1));
                }
                if (!rs.isAfterLast()) {
                    System.out.println("isAfterLast should be true");
                }
                rs.absolute(rows/2);
                if (rs.absolute((-1) * (rows + 1))) {
                    System.out.println("absolute(" + (((-1) * (rows + 1))) + ") should return false, position outside of the resultSet");
                    System.out.println("Current row: " + rs.getInt(1));
                }
                if (!rs.isBeforeFirst()) {
                    System.out.println("isBeforeFirst should be true");
                }

                rs.close();        
            
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            return passed;
        }


        
	/**
	 * Check to make sure that the given SQLException is an exception
	 * with the expected sqlstate.
	 *
	 * @param e		The SQLException to check
	 * @param SQLState	The sqlstate to look for
	 *
	 * @return	true means the exception is the expected one
	 */

	private static boolean checkException(SQLException e,
											String SQLState)
	{
		String				state;
		String				nextState;
		SQLException		next;
		boolean				passed = true;

		state = e.getSQLState();


		if (! SQLState.equals(state)) {
				System.out.println("FAIL -- unexpected exception " + e +
					"sqlstate: " + state + SQLState);
				passed = false;
			}

		return passed;
	}

	/**
	 * Clean up after ourselves when testing is done.
	 *
	 * @param conn	The Connection
	 * @param s		A Statement on the Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean cleanUp(Connection conn, Statement s) {
		try {
			/* Drop the table we created */
			if (s == null)
			{
				// well, then, we'll have to restart
				s = conn.createStatement();
			}
			s.execute("drop table t");
			/* Close the connection */
			conn.commit();
			conn.close();
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception caught in cleanup()");
			JDBCDisplayUtil.ShowException(System.out, e);
			return false;
		}

		return true;
	}

	/* 
	 * cleanup also before test start, just in case
	 * @param conn	The Connection
	 */
	static void cleanUp(Connection conn) throws SQLException {
		Statement cleanupStmt = conn.createStatement();
		String[] testObjects = {"table t"};
		TestUtil.cleanUpTest(cleanupStmt, testObjects);
		cleanupStmt.close();
	}


}

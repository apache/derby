/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.stmtCache3

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test the statement cache with a 3-statement size.
 *
 * @author ames
 */


public class stmtCache3 {

	private static Connection conn;
	private static boolean passed = false;

	public static void main(String[] args) {
		System.out.println("Test stmtCache3 starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(false);

			passed = setupTest(conn);

			// tests stop at first failure.

			passed = passed && testGrowsAndShrinks(conn);

			passed = passed && cleanupTest(conn);

			conn.commit();
			conn.close();

		} catch (Throwable e) {
			passed = false;
			System.out.println("FAIL: exception thrown:");
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test stmtCache3 finished");
	}

	//
	// the tests
	//

	/**
	 * Create some helper aliases for checking the cache state.
	 *
	 * @param conn the Connection
	 *
	 * @exception SQLException thrown on unexpected failure.
	 */
	static boolean setupTest(Connection conn) throws SQLException {


		boolean passed = checkCache(conn, 0);

		return passed;
	}

	/**
	 * Verify the cache state.
	 * @param conn the connection to check
	 * @param numInCache the number expected to be cached
	 * @exception SQLException thrown on failure
	 */
	static boolean checkCache(Connection conn, int numInCache) throws SQLException {
	    PreparedStatement ps = conn.prepareStatement("select count(*) from new org.apache.derby.diag.StatementCache() AS SC_CONTENTS");

		// we're adding one with this statement, so account for it.
		int actualNum = numInCache + 1;
		if (actualNum > 3)
			actualNum = 3;

		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean passed = rs.getInt(1) == actualNum;
		rs.close();
		ps.close();

		if (!passed)
		  System.out.println("FAIL -- expected "+numInCache+" statements in cache");
		return passed;
	}

	/**
	 * Test that all non-closed prepared statements
	 * hang around, while closed ones disappear immediately,
	 * When the cache is at its limit.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean testGrowsAndShrinks(Connection conn)
					throws SQLException {
		boolean passed = true;

		PreparedStatement ps1, ps2, ps3, ps4, ps5;
		ResultSet rs1, rs2, rs3, rs4, rs5;

		ps1 = conn.prepareStatement("values 1");
		ps2 = conn.prepareStatement("values 2");
		ps3 = conn.prepareStatement("values 3");
		ps4 = conn.prepareStatement("values 4");
		ps5 = conn.prepareStatement("values 5");

		passed = passed && checkCache(conn,3);

		rs1 = ps1.executeQuery();
		rs2 = ps2.executeQuery();
		rs3 = ps3.executeQuery();
		rs4 = ps4.executeQuery();
		rs5 = ps5.executeQuery();

		passed = passed && checkCache(conn,3);

		rs1.next();
		rs2.next();
		rs3.next();
		rs4.next();
		rs5.next();

		passed = passed && checkCache(conn, 3);

		// this closes all of the result sets,
		// but the prepared statements are still open
		rs1.next();
		rs2.next();
		rs3.next();
		rs4.next();
		rs5.next();

		passed = passed && checkCache(conn, 3);

		// this ensures all of the result sets are closed,
		// but the prepared statements are still open
		rs1.close();
		rs2.close();
		rs3.close();
		rs4.close();
		rs5.close();

		passed = passed && checkCache(conn,3);

		// let one get away, the cache should shrink...
		ps1.close();
		passed = passed && checkCache(conn,3);

		// let one more get away, the cache should shrink...
		ps2.close();
		passed = passed && checkCache(conn,3);

		// let one more get away, the cache won't shrink this time...
		ps3.close();
		passed = passed && checkCache(conn,3);

		// close the rest, the cache should stay three...
		ps4.close();
		ps5.close();
		passed = passed && checkCache(conn,3);

		return passed;
	}


	/**
	 * Clean up after ourselves when testing is done.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean cleanupTest(Connection conn) throws SQLException {

		return true;
	}
 
	// used by stmtcache 5 test
	public static String findStatementInCacheById(String sql) throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		PreparedStatement ps = conn.prepareStatement(sql);

		PreparedStatement ps2 = conn.prepareStatement("select SQL_TEXT  from new org.apache.derby.diag.StatementCache() as ST where ID = ?");
		ps2.setString(1, ps.toString());
		ResultSet rs = ps2.executeQuery();
		rs.next();
		String ret = rs.getString(1);
		rs.close();

		ps2.close();
		ps.close();

		return ret;

	}
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.nullSQLText

   Copyright 2001, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.JDBCTestDisplayUtil;

/**
 * Test of null strings in prepareStatement and execute 
 * result set.  Also test comments in SQL text that is
 * passed to an "execute" call.
 *
 * @author peachey
 */

public class nullSQLText { 
	public static void main(String[] args) {
		Connection con;
		PreparedStatement  ps;
		Statement s;
		String nullString = null;
	
		System.out.println("Test nullSQLText starting");
    
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			con.setAutoCommit(true); // make sure it is true
			s = con.createStatement();

			try
			{
				// test null String in prepared statement
				System.out.println("Test prepareStatement with null argument");
				ps = con.prepareStatement(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute statement
				System.out.println("Test execute with null argument");
				s.execute(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute query statement
				System.out.println("Test executeQuery with null argument");
				s.executeQuery(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute update statement
				System.out.println("Test executeUpdate with null argument");
				s.executeUpdate(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}

			// Test comments in statements.
			derby522(s);

			con.close();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace(System.out);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}
		
		System.out.println("Test nullSQLText finished");
    }
	static private void dumpSQLExceptions (SQLException se) {
		while (se != null) {
            JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);			
	         se = se.getNextException();
		}
	}


	/* ****
	 * Derby-522: When a statement with comments at the front
	 * is passed through to an "execute" call, the client throws
	 * error X0Y79 ("executeUpdate cannot be called with a statement
	 * that returns a result set").  The same thing works fine
	 * against Derby embedded.  This method executes several
	 * statements that have comments preceding them; with the
	 * fix for DERBY-522, these should all either pass or
	 * throw the correct syntax errors (i.e. the client should
	 * behave the same way as embedded).
	 */
	private static void derby522(Statement st) throws Exception
	{
		System.out.println("Starting test for DERBY-522.");

		st.execute("create table t1 (i int)");
		st.execute("insert into t1 values 1, 2, 3, 4, 5, 6, 7");
		st.execute("create procedure proc1() language java " +
			"parameter style java dynamic result sets 1 " +
			"external name 'org.apache.derbyTesting.functionTests." +
			"tests.jdbcapi.nullSQLText.sp1'");

		// These we expect to fail with syntax errors, as in embedded mode.
		testCommentStmt(st, " --", true);
		testCommentStmt(st, " -- ", true);
		testCommentStmt(st, " -- This is a comment \n --", true);
		testCommentStmt(
			st,
			" -- This is a comment\n --And another\n -- Andonemore",
			true);

		// These we expect to return valid results for embedded and
		// Derby Client (as of DERBY-522 fix); for JCC, these will
		// fail.
		testCommentStmt(st, " --\nvalues 2, 4, 8", TestUtil.isJCCFramework());
		testCommentStmt(
			st,
			" -- This is \n -- \n --3 comments\nvalues 8",
			TestUtil.isJCCFramework());
		testCommentStmt(
			st,
			" -- This is a comment\n --And another\n -- Andonemore\nvalues (2,3)",
			TestUtil.isJCCFramework());
		testCommentStmt(st,
			" -- This is a comment\n select i from t1",
			TestUtil.isJCCFramework());
		testCommentStmt(st,
			" --singleword\n insert into t1 values (8)",
			TestUtil.isJCCFramework());
		testCommentStmt(st,
			" --singleword\ncall proc1()",
			TestUtil.isJCCFramework());
		testCommentStmt(st,
			" -- leading comment\n(\nvalues 4, 8)",
			TestUtil.isJCCFramework());
		testCommentStmt(st,
			" -- leading comment\n\n(\n\n\rvalues 4, 8)",
			TestUtil.isJCCFramework());

		// While we're at it, test comments in the middle and end of the
		// statement.  Prior to the patch for DERBY-522, statements
		// ending with a comment threw syntax errors; that problem
		// was fixed with DERBY-522, as well, so all of these should now
		// succeed in all modes (embedded, Derby Client, and JCC).
		testCommentStmt(st, "select i from t1 -- This is a comment", false);
		testCommentStmt(st, "select i from t1\n -- This is a comment", false);
		testCommentStmt(st, "values 8, 4, 2\n --", false);
		testCommentStmt(st, "values 8, 4,\n -- middle comment\n2\n -- end", false);
		testCommentStmt(st, "values 8, 4,\n -- middle comment\n2\n -- end\n", false);

		// Clean-up.
		try {
			st.execute("drop table t1");
		} catch (SQLException se) {}
		try {
			st.execute("drop procedure proc1");
		} catch (SQLException se) {}

		st.close();
		System.out.println("DERBY-522 test completed.");
	}

	/* ****
	 * Helper method for derby522.
	 */
	private static void testCommentStmt(Statement st, String sql,
		boolean expectError) throws SQLException
	{

		try {

			System.out.println("[ Test Statement ]:\n" + sql);
			st.execute(sql);
			System.out.print("[ Results ]: ");
			ResultSet rs = st.getResultSet();
			if (rs != null) {
				while (rs.next())
					System.out.print(" " + rs.getInt(1));
				System.out.println();
			}
			else
				System.out.println("(NO RESULT SET)");

		} catch (SQLException se) {

			if (expectError)
				System.out.print("[ EXPECTED ERROR ]: ");
			else
				System.out.print("[ FAILED ]: ");
			dumpSQLExceptions(se);

		}

	}

	/* ****
	 * Helper method for derby522.
	 */
	public static void sp1(ResultSet [] rs) throws SQLException {

		Connection conn = DriverManager.getConnection(
			"jdbc:default:connection");

		Statement st = conn.createStatement();
		rs[0] = st.executeQuery("select i from t1");
		return;

	}

}

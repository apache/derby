/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.cursor

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

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
  This tests cursor handling

  Not done in ij since the cursor names may not be stable,
  and we want to control navigation through the cursor rows.

  This could be more complete, but since this is SQL92 Entry
  standard, we are assuming that some future purchase of the
  NIST suite or some equivalent will suffice.
 */

public class cursor {

	private static Connection conn;
	private static boolean passed = false;

	public static void main(String[] args) {
		System.out.println("Test cursor starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(false);

			setup(true);
			testCursor();
			testCursorParam();
			testgetCursorName();
			teardown();

			conn.commit();
			conn.close();

			passed = true;

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			passed = false;
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test cursor finished");
	}

	static void setup(boolean first) throws SQLException {
		Statement stmt = conn.createStatement();

		if (first) {
			verifyCount(
				stmt.executeUpdate("create table t (i int, c char(50))"),
				0);

			verifyCount(
				stmt.executeUpdate("create table s (i int, c char(50))"),
				0);
		} else {
			verifyBoolean(
				stmt.execute("delete from t"),
				false);
		}

		verifyCount(
		    stmt.executeUpdate("insert into t values (1956, 'hello world')"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (456, 'hi yourself')"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (180, 'rubber ducky')"),
			1);

		verifyCount(
		    stmt.executeUpdate("insert into t values (3, 'you are the one')"),
			1);

		stmt.close();

		System.out.println("PASS: setup complete");
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount(
		    stmt.executeUpdate("drop table t"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop table s"),
			0);

		stmt.close();

		System.out.println("PASS: teardown complete");
	}

	static void verifyCount(int count, int expect) throws SQLException {
		if (count!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+count+" rows");
			throw new SQLException("Wrong number of rows returned");
		}
		else
			System.out.println("PASS: expected and got "+count+
							   (count == 1? " row":" rows"));
	}

	static void verifyBoolean(boolean got, boolean expect) throws SQLException {
		if (got!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+got);
			throw new SQLException("Wrong boolean returned");
		}
		else
			System.out.println("PASS: expected and got "+got);
	}

	static int countRows(String query) throws SQLException {
		Statement select = conn.createStatement();
		ResultSet counter = select.executeQuery(query);
		int count = 0;

		while (counter.next()) {
			count++;
			System.out.println("Row: "+counter.getInt(1)+","+counter.getString(2));
		}
		select.close();

		return count;
	}

	static void nextRow(ResultSet r) throws SQLException {
		verifyBoolean(r.next(), true);
		System.out.println("Row: "+r.getInt(1)+","+r.getString(2));
	}

	static boolean ifRow(ResultSet r) throws SQLException {
		boolean b = r.next();

		if (b)
			System.out.println("Row: "+r.getInt(1)+","+r.getString(2));

		return b;
	}

	static void testCursor() throws SQLException {
		PreparedStatement select, delete;
		Statement select2, delete2;
		ResultSet cursor;
		boolean caught;

		// because there is no order by (nor can there be)
		// the fact that this test prints out rows may someday
		// be a problem.  When that day comes, the row printing
		// can (should) be removed from this test.

		select = conn.prepareStatement("select i, c from t for update");
		cursor = select.executeQuery(); // cursor is now open

		// TEST: fetch of a row works
		nextRow(cursor);

		// TEST: close and then fetch gets error on fetch.
		cursor.close();

		// bang away on the nexts for a little while,
		// see what the error quiets out to...
		for (int i=0;i<5;i++) {
			caught = false;
			try {
				ifRow(cursor); // no current row / closed
	   		} catch (SQLException se) {
				JDBCDisplayUtil.ShowSQLException(System.out,se);
				caught = true;
				System.out.println("PASS: Attempt to get next on closed cursor caught");
			}
			if (! caught)
				System.out.println("FAIL: No error from next on closed cursor");
		}

		// restart the query for another test.
		cursor = select.executeQuery();

		// TEST: next past the end of the table.
		while (ifRow(cursor)); // keep going to the last row
		caught = false;
		try {
			boolean b = ifRow(cursor); // no current row / closed (past the last row)
			if (!b) {
				System.out.println("No current row");
				caught = true;
			}
   		} catch (SQLException se) {
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			caught = true;
			System.out.println("PASS: Attempt to get next after end of cursor caught");
		}
		if (! caught)
			System.out.println("FAIL: No error from next past end of cursor");

		System.out.println("PASS: cursor test complete");

		cursor.close();
	}

	static void testCursorParam() throws SQLException {
		PreparedStatement select, delete;
		Statement select2, delete2;
		ResultSet cursor;
		boolean caught;

		// because there is no order by (nor can there be)
		// the fact that this test prints out rows may someday
		// be a problem.  When that day comes, the row printing
		// can (should) be removed from this test.

		select = conn.prepareStatement("select i, c from t where ?=1 for update");
		select.setInt(1,1);
		cursor = select.executeQuery(); // cursor is now open

		// TEST: fetch of a row works
		nextRow(cursor);

		// TEST: close and then fetch gets error on fetch.
		cursor.close();

		// bang away on the nexts for a little while,
		// see what the error quiets out to...
		for (int i=0;i<5;i++) {
			caught = false;
			try {
				ifRow(cursor); // no current row / closed
	   		} catch (SQLException se) {
				JDBCDisplayUtil.ShowSQLException(System.out,se);
				caught = true;
				System.out.println("PASS: Attempt to get next on closed cursor caught");
			}
			if (! caught)
				System.out.println("FAIL: No error from next on closed cursor");
		}

		// restart the query for another test.
		select.setBoolean(1,false);
		select.setCursorName("ForCoverageSake");
		cursor = select.executeQuery();

		if (cursor.getCursorName().equals("ForCoverageSake")) {
			System.out.println("PASS: cursor name set");
		}
		else
		{
			System.out.println("FAIL: cursor name not set, is still: "+cursor.getCursorName());
		}

		// TEST: next past the end of the table -- expect no rows at all.
		caught = false;
		try {
			boolean b = ifRow(cursor); // no current row / closed (past the last row)
			if (!b) {
				System.out.println("No current row");
				caught = true;
			}
   		} catch (SQLException se) {
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			caught = true;
			System.out.println("PASS: Attempt to get next after end of cursor caught");
		}
		if (! caught)
			System.out.println("FAIL: No error from next past end of cursor");

		System.out.println("PASS: cursor test complete");

		cursor.close();
	}
	static void testgetCursorName() throws SQLException {
			// test cursor name
			testCursorName("select * from t", (String)null);
			testCursorName("select * from t for update", (String)null);
			testCursorName("select * from t for update of i", (String)null);
			testCursorName("select * from t", "myselect");
			testCursorName("select * from t for update", "myselect");
			testCursorName("select * from t for update of i", "myselect");
	}
	static private void testCursorName(String statement, String cursorname)  throws SQLException{
		System.out.println("Test cursor name for " + statement + 
			" Cursor name = " + cursorname);
		Statement s = conn.createStatement();
		if (cursorname != null)
			s.setCursorName(cursorname);
		ResultSet rs = s.executeQuery(statement);
		if (rs != null)
		{
			String cursorName = rs.getCursorName();
			System.out.println("cursor name = " + cursorName);
		}
		rs.close();
		s.close();

	}

}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.currentof

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
  This tests the current of statements, i.e.
  delete where current of and update where current of.

  Not done in ij since the cursor names may not be stable.

  This could be more complete, but since this is SQL92 Entry
  standard, we are assuming that some future purchase of the
  NIST suite or some equivalent will suffice.
 */

public class currentof {

	private static Connection conn;
	private static boolean passed = false;

	public static void main(String[] args) {
		System.out.println("Test currentof starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(false);

			setup(true);
			testDelete();
			setup(false);
			testUpdate();
			teardown();

			conn.commit();
			conn.close();

			passed = true;

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			errorPrint(e);
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test currentof finished");
	}

	static void errorPrint(Throwable e) {
		if (e == null) return;

		e.printStackTrace();

		if (e instanceof SQLException)
			errorPrint(((SQLException)e).getNextException());
	}

	static void setup(boolean first) throws SQLException {
		Statement stmt = conn.createStatement();

		if (first) {
			verifyCount("create table t (i int, c char(50))",
				stmt.executeUpdate("create table t (i int, c char(50))"),
				0);

			verifyCount("create table s (i int, c char(50))",
				stmt.executeUpdate("create table s (i int, c char(50))"),
				0);
		} else {
			verifyBoolean(
				stmt.execute("delete from t"),
				false);
		}

		verifyCount("insert into t values (1956, 'hello world')",
		    stmt.executeUpdate("insert into t values (1956, 'hello world')"),
			1);

		verifyCount("insert into t values (456, 'hi yourself')",
		    stmt.executeUpdate("insert into t values (456, 'hi yourself')"),
			1);

		verifyCount("insert into t values (180, 'rubber ducky')",
		    stmt.executeUpdate("insert into t values (180, 'rubber ducky')"),
			1);

		verifyCount("insert into t values (3, 'you are the one')",
		    stmt.executeUpdate("insert into t values (3, 'you are the one')"),
			1);

		stmt.close();

		System.out.println("PASS: setup complete");
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount("drop table t",
		    stmt.executeUpdate("drop table t"),
			0);

		verifyCount("drop table s",
		    stmt.executeUpdate("drop table s"),
			0);

		stmt.close();

		System.out.println("PASS: teardown complete");
	}

	static void verifyCount(String text, int count, int expect) throws SQLException {
		if (count!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+count+" rows on stmt: "+text);
			throw new SQLException("Wrong number of rows returned");
		}
		else
			System.out.println("PASS: expected and got "+count+
							   (count == 1? " row" : " rows")+"on stmt: "+text);
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
		counter.close();
		select.close();

		return count;
	}

	static void nextRow(ResultSet r) throws SQLException {
		verifyBoolean(r.next(), true);
		System.out.println("Row: "+r.getInt(1)+","+r.getString(2));
	}

	static void testDelete() throws SQLException {
		PreparedStatement select, delete;
		Statement select2, delete2;
		ResultSet cursor;
		int startCount, endCount;
		boolean caught;

		startCount = countRows("select i, c from t for read only");
		System.out.println("Have "+startCount+" rows in table at start");

		// because there is no order by (nor can there be)
		// the fact that this test prints out rows may someday
		// be a problem.  When that day comes, the row printing
		// can (should) be removed from this test.

		select = conn.prepareStatement("select i, c from t for update");
		cursor = select.executeQuery(); // cursor is now open

		// would like to test a delete attempt before the cursor
		// is open, but finagling to get the cursor name would
		// destroy the spirit of the rest of the tests,
		// which want to operate against the generated name.

		// TEST: cursor and target table mismatch
		caught = false;
		try {
			delete = conn.prepareStatement("delete from s where current of "+
									   cursor.getCursorName());
		} catch (SQLException se) {

			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);

			if ("42X28".equals(m)) {
				caught = true;
				System.out.println("PASS: delete table and cursor table mismatch caught");
			} else {
				System.out.println("MAYBE FAIL: delete table and cursor table mismatch got unexpected exception");
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: delete table and cursor table mismatch not caught");
		}

		// TEST: find the cursor during compilation
		delete = conn.prepareStatement("delete from t where current of "+
									   cursor.getCursorName());

		// TEST: delete before the cursor is on a row
		caught = false;
		try {
			delete.executeUpdate();
   		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL08".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to delete cursor before first row caught");
			} else {
				System.out.println("...surprise error "+se);
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from delete on cursor before first row");
		}

		// TEST: find the cursor during execution and it is on a row
		nextRow(cursor);
		verifyCount("delete, ok",
		    delete.executeUpdate(),
			1);

		// TEST: delete an already deleted row; expect an error
		// expect second delete to throw a no current row exception
		// REMIND: currently it is ugly, hopefully it will get better.
		caught = false;
		/* try {
			verifyCount("<delete cursor on deleted row>", delete.executeUpdate(),
						0);
		} catch (SQLException se) {
		// will replace this with SQLState check some day,
		// at present this is a temporary message expectation.
			String m = se.getMessage();
			if (m.startsWith("\"Record ") && m.endsWith(" not found.\"")) {
				caught = true;
				System.out.println("PASS: Attempt to repeat delete did not find row");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from repeat delete");
		}*/

		// skip a row and delete another row so that two rows will
		// have been removed from the table when we are done.
		nextRow(cursor); // skip this row
		nextRow(cursor);

		verifyCount("<delete after skipping>",
		    delete.executeUpdate(),
			1);

		// TEST: delete past the last row
		nextRow(cursor); // skip this row
		verifyBoolean(cursor.next(), false); // past last row now
		caught = false;
		try {
			delete.executeUpdate(); // no current row / closed
   		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL08".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to delete cursor past last row caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from delete on cursor past last row");
		}

		// TEST: delete off a closed cursor
		// Once this is closed then the cursor no longer exists.
		cursor.close();
		caught = false;
		try {
			delete.executeUpdate();
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL07".equals(se.getSQLState())) {
					caught = true;
					System.out.println("PASS: Attempt to delete closed cursor caught");
			}
			if ("42X30".equals(se.getSQLState())) {
					caught = true;
					System.out.println("PASS: Attempt to delete closed cursor caught");
			}
			if (!caught)
				throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from delete on closed cursor");
		}

		endCount = countRows ("select i, c from t for read only");
		System.out.println("Have "+endCount+" rows in table at end");

		verifyCount("startCount-endCount", startCount-endCount,2);

		// TEST: no cursor with that name exists
		delete2 = conn.createStatement();
		caught = false;
		try {
			delete2.execute("delete from t where current of nosuchcursor");
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("42X30".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to delete nonexistent cursor caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from delete on nonexistent cursor");
		}

		delete.close();
		delete2.close();
		select.close();

		// TEST: attempt to do positioned delete before cursor execute'd
		// TBD

		System.out.println("PASS: delete test complete");
	}

	static void testUpdate() throws SQLException {
		PreparedStatement select = null;
		PreparedStatement update = null;
		Statement select2, update2;
		ResultSet cursor = null;
		int startCount, endCount;
		boolean caught;

		// these are basic tests without a where clause on the select.
		// all rows are in and stay in the cursor's set when updated.

		// because there is no order by (nor can there be)
		// the fact that this test prints out rows may someday
		// be a problem.  When that day comes, the row printing
		// can (should) be removed from this test.

		endCount = countRows ("select i, c from t for read only");
		System.out.println("Have "+endCount+" rows in table at start");

		// TEST: Updated column not found in for update of list
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from t for update of I");
			cursor = select.executeQuery(); // cursor is now open
			update = conn.prepareStatement(
							"update t set C = 'abcde' where current of " +
							cursor.getCursorName());
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("42X31".equals(m)) {
				caught = true;
				System.out.println("PASS: update of non-existant column caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: update of non-existant column not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor declared READ ONLY
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from t for read only");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
				{
				caught = true;
				System.out.println("PASS: update of read-only cursor caught");
				}
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of read-only cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor declared FETCH ONLY
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from t for fetch only");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
				{
				caught = true;
				System.out.println("PASS: update of fetch-only cursor caught");
				}
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of fetch-only cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor with a union
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from t union all select I, C from t");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
			{
				System.out.println("PASS: update of union cursor caught");
				caught = true;
			}
		} catch (SQLException se) {
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			String m = se.getSQLState();
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of union cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor with a join
		caught = false;
		try {
			select = conn.prepareStatement("select t1.I, t1.C from t t1, t t2 where t1.I = t2.I");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
			{
				System.out.println("PASS: update of join cursor caught");
				caught = true;
			}
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of join cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor with a derived table
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from (select * from t) t1");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
			{
				System.out.println("PASS: update of derived table cursor caught");
				caught = true;
			}
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of derived table cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor with a values clause
		caught = false;
		try {
			select = conn.prepareStatement("values (1, 2, 3)");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
			{
				caught = true;
				System.out.println("PASS: update of values clause cursor caught");
			}
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of values clause cursor not caught");
		}
		cursor.close();
		select.close();

		// TEST: Update of cursor with a subquery
		caught = false;
		try {
			select = conn.prepareStatement("select I, C from t where I in (select I from t)");
			cursor = select.executeQuery(); // cursor is now open
			if (cursor.getCursorName() == null)
			{
				caught = true;
				System.out.println("PASS: update of subquery cursor caught");
			}
		} catch (SQLException se) {
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			throw se;
		} finally {
			if (! caught)
				System.out.println("FAIL: update of subquery cursor not caught");
		}
		cursor.close();
		select.close();

		select = conn.prepareStatement("select I, C from t for update");
		cursor = select.executeQuery(); // cursor is now open

		// would like to test a update attempt before the cursor
		// is open, but finagling to get the cursor name would
		// destroy the spirit of the rest of the tests,
		// which want to operate against the generated name.

		// TEST: cursor and target table mismatch
		caught = false;

		try {
			update = conn.prepareStatement("update s set i=1 where current of "+
									   cursor.getCursorName());
		} catch (SQLException se) {
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			String m = se.getSQLState();
			if ("42X29".equals(m)) {
				caught = true;
				System.out.println("PASS: update table and cursor table mismatch caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: update table and cursor table mismatch not caught");
		}
		// TEST: find the cursor during compilation
		update = conn.prepareStatement(
		    "update t set i=i+10, c='Gumby was here' where current of "+
			cursor.getCursorName());

		// TEST: update before the cursor is on a row
		caught = false;
		try {
			verifyCount("update before the cursor", update.executeUpdate(), 0); // no current row / closed
   		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL08".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to update cursor before first row caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from update on cursor before first row");
		}

		// TEST: find the cursor during execution and it is on a row
		nextRow(cursor);
		verifyCount("update on row", update.executeUpdate(), 1);

		// TEST: update an already updated row; expect it to succeed.
		// will it have a cumulative effect?
		verifyCount("2nd update on row", update.executeUpdate(), 1);

		// skip a row and update another row so that two rows will
		// have been removed from the table when we are done.
		nextRow(cursor); // skip this row
		nextRow(cursor);

		verifyCount( "update after skipping", update.executeUpdate(), 1);

		// TEST: update past the last row
		nextRow(cursor); // skip this row
		verifyBoolean(cursor.next(), false); // past last row now
		caught = false;
		try {
			verifyCount("update: no current row", update.executeUpdate(), 0); // no current row / closed
   		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL08".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to update cursor past last row caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from update on cursor past last row");
		}

		// TEST: update off a closed cursor
		cursor.close();
		select.close();
		caught = false;
		try {
			verifyCount("update on closed cursor", update.executeUpdate(),
						0);
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("XCL07".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to update closed cursor caught");
			}
			if ("42X30".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to update closed cursor caught");
			}
			
			if (!caught) {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from update on closed cursor");
		}
		update.close();

		// TEST: no cursor with that name exists
		update2 = conn.createStatement();
		caught = false;
		try {
			update2.execute("update t set i=1 where current of nosuchcursor");
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("42X30".equals(m)) {
				caught = true;
				System.out.println("PASS: Attempt to update nonexistent cursor caught");
			} else {
				throw se;
			}
		} finally {
			if (! caught)
				System.out.println("FAIL: No error from update on nonexistent cursor");
		}

		endCount = countRows ("select i, c from t for read only");
		System.out.println("Have "+endCount+" rows in table at end");

		// TEST: attempt to do positioned update before cursor execute'd
		// TBD


		// TEST closing a cursor will close the related update
		bug4395(conn, "CS4395"); // Application provided cursor name
		bug4395(conn, null); // system provided cursor name


		System.out.println("PASS: update test complete");
	}

	private static void bug4395(Connection conn, String cursorName) throws SQLException {

		System.out.println("bug4395 Cursor Name " + (cursorName == null ? "System Generated" : "Application Defined"));

		PreparedStatement select = conn.prepareStatement("select I, C from t for update");
		if (cursorName != null)
			select.setCursorName(cursorName);

		ResultSet cursor = select.executeQuery(); // cursor is now open
		// TEST: find the cursor during compilation
		cursorName = cursor.getCursorName();
		PreparedStatement update = conn.prepareStatement("update t set i=i+?, c=? where current of "+
			cursorName);

		nextRow(cursor);
		update.setInt(1, 10);
		update.setString(2, "Dan was here");
		verifyCount("update: valid update", update.executeUpdate(), 1);
		cursor.close();

		// now prepare the a cursor with the same name but only column I for update
		PreparedStatement selectdd = conn.prepareStatement("select I, C from t for update of I");
		selectdd.setCursorName(cursorName);
		cursor = selectdd.executeQuery();
		nextRow(cursor);

		try {
			update.setInt(1, 7);
			update.setString(2, "no update");
			update.executeUpdate();
			System.out.println("FAIL update succeeded after cursor has been changed");
		} catch (SQLException se) {
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);
			if ("42X31".equals(m)) {
				System.out.println("PASS: Attempt to update changed invalid cursor caught");
			} else {
				throw se;
			}
		}

		cursor.close();
		cursor = selectdd.executeQuery();
		nextRow(cursor);
		cursor.close();

	}
}




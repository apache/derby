/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.updateCursor

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
  This tests updateable cursor using index, Beetle entry 3865.

  Not done in ij since we need to do many "next" and "update" to be
  able to excercise the code of creating temp conglomerate for virtual
  memory heap.  We need at minimum
  200 rows in table, if "maxMemoryPerTable" property is set to 1 (KB).
  This includes 100 rows to fill the hash table and another 100 rows
  to fill the in-memory heap.

 */

public class updateCursor {

	private static Connection conn;

	public static void main(String[] args) {
		System.out.println("Test updateable cursor using index starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(true);

			setup(true);

			conn.setAutoCommit(false);

			System.out.println("************************************TESTING VIRTUAL MEM HEAP*********");
			testVirtualMemHeap();
			System.out.println("************************************TESTING NONCOVERINGINDEX*********");
			testNonCoveringIndex();
			System.out.println("************************************TESTING DESC INDEX*********");
			testDescendingIndex();

			System.out.println("************************************TESTING UPDATE DELETE WARNING*********");
			testUpdateDeleteWarning();

			teardown();

			conn.close();

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		System.out.println("Test updateable cursor using index finished");
	}

	static void setup(boolean first) throws SQLException {
		Statement stmt = conn.createStatement();

		if (first) {
			verifyCount(
				stmt.executeUpdate("create table t1 (c1 int, c2 char(50), c3 int, c4 char(50), c5 int, c6 varchar(1000))"),
				0);

			verifyCount(
				stmt.executeUpdate("create index i11 on t1 (c3, c1, c5)"),
				0);

			verifyCount(
				stmt.executeUpdate("create table t2 (c1 int)"),
				0);

			verifyCount(
				stmt.executeUpdate("create table t3(c1 char(20) not null primary key)"),
				0);

			verifyCount(
				stmt.executeUpdate("create table t4(c1 char(20) references t3(c1) on delete cascade)"),
				0);
		} else {
			verifyBoolean(
				stmt.execute("delete from t1"),
				false);
		}

		StringBuffer sb = new StringBuffer(1000);
		for (int i = 0; i < 1000; i++)
			sb.append('a');
		String largeString = new String(sb);

		for (int i = 246; i > 0; i = i - 5)
		{
			verifyCount(
			stmt.executeUpdate("insert into t1 values ("
				+ (i+4) + ", '" + i + "', " + i + ", '" + i + "', " + i + ", '" + largeString + "'), ("
				+ (i+3) + ", '" + i + "', " + (i+1) + ", '" + i + "', " + i + ", '" + largeString + "'), ("
				+ (i+2) + ", '" + i + "', " + (i+2) + ", '" + i + "', " + i + ", '" + largeString + "'), ("
				+ (i+1) + ", '" + i + "', " + (i+3) + ", '" + i + "', " + i + ", '" + largeString + "'), ("
				+ i + ", '" + i + "', " + (i+4) + ", '" + i + "', " + i + ", '" + largeString + "')"),
			5);
		}

		stmt.executeUpdate("insert into t2 values (1)");

		stmt.close();

		System.out.println("PASS: setup complete");
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount(
		    stmt.executeUpdate("drop table t1"),
			0);
		verifyCount(
		    stmt.executeUpdate("drop table t2"),
			0);
		verifyCount(
		    stmt.executeUpdate("drop table t4"),
			0);
		verifyCount(
		    stmt.executeUpdate("drop table t3"),
			0);

		conn.commit();
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

	static void nextRow(ResultSet r, int which) throws SQLException {
		verifyBoolean(r.next(), true);
		if (which == 1)
			System.out.println("Row: "+r.getInt(1)+","+r.getInt(2));
		else if (which == 2)
			System.out.println("Row: "+r.getInt(1)+","+r.getString(2));
	}

	static boolean ifRow(ResultSet r, int which) throws SQLException {
		boolean b = r.next();

		if (b)
		{
			if (which == 1)
				System.out.println("Row: "+r.getInt(1)+","+r.getInt(2));
			else if (which == 2)
				System.out.println("Row: "+r.getInt(1)+","+r.getString(2));
		}
		return b;
	}

	static void testVirtualMemHeap() throws SQLException {
		PreparedStatement select;
		Statement update;
		ResultSet cursor;

		update = conn.createStatement();
		select = conn.prepareStatement("select c1, c3 from t1 where c3 > 1 and c1 > 0 for update");
		cursor = select.executeQuery(); // cursor is now open
		String cursorName = cursor.getCursorName();

		System.out.println(
		 "Notice the order in the rows we get: from 2 to 102 asc order on second column (c3)");
		System.out.println(
		 "then from 202 down to 103 on that column; then from 203 up to 250.  The reason is");
		System.out.println(
		  "we are using asc index on c3, all the rows updated are in the future direction of the");
		System.out.println(
		 "index scan, so they all get filled into a hash table.  The MAX_MEMORY_PER_TABLE");
		System.out.println(
		 "property determines max cap of hash table 100.  So from row 103 it goes into virtual");
		System.out.println(
		 "memory heap, whose in memory part is also 100 entries.  So row 103 to 202 goes into");
		System.out.println(
		 "the in-memory part and gets dumped out in reverse order.  Finally Row 203 to 250");
		System.out.println(
		 "goes into file system.  Here we mean row ids.");

		for (int i = 0; i < 249; i++)
		{
			nextRow(cursor, 1);
			update.execute("update t1 set c3 = c3 + 250 where current of " + cursorName);
		}
		if (! ifRow(cursor, 1))
			System.out.println("UPDATE WITH VIRTUAL MEM HEAP: got 249 rows");
		else
			System.out.println("UPDATE WITH VIRTUAL MEM HEAP FAILED! STILL GOT ROWS");
		cursor.close();
		select.close();

		System.out.println("************ See what we have in table:");
		select = conn.prepareStatement("select c1, c3 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < 250; i++)
			nextRow(cursor, 1);
		if (! ifRow(cursor, 1))
			System.out.println("AFTER UPDATE WITH VIRTUAL MEM HEAP: got 250 rows");
		else
			System.out.println("UPDATE WITH VIRTUAL MEM HEAP RESULT:FAILED!!! GOT MORE ROWS");
		conn.rollback();
	}

	static void testNonCoveringIndex() throws SQLException {
		PreparedStatement select;
		Statement update;
		ResultSet cursor;

		update = conn.createStatement();
		select = conn.prepareStatement("select c3, c2 from t1 where c3 > 125 and c1 > 0 for update");
		cursor = select.executeQuery(); // cursor is now open
		String cursorName = cursor.getCursorName();

		for (int i = 0; i < 125; i++)
		{
			nextRow(cursor, 2);
			update.execute("update t1 set c3 = c3 + 25 where current of " + cursorName);
		}
		if (! ifRow(cursor, 2))
			System.out.println("UPDATE USING NONCOVERING INDEX: got 125 rows");
		else
			System.out.println("UPDATE USING NONCOVERING INDEX FAILED! STILL GOT ROWS");
		cursor.close();
		select.close();

		System.out.println("************ See what we have in table:");
		select = conn.prepareStatement("select c1, c3 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < 250; i++)
			nextRow(cursor, 2);
		if (! ifRow(cursor, 2))
			System.out.println("AFTER UPDATE USING NONCOVERING INDEX: got 250 rows");
		else
			System.out.println("UPDATE USING NONCOVERING INDEX: FAILED!!! GOT MORE ROWS");
		conn.rollback();
	}

	static void testDescendingIndex() throws SQLException {
		PreparedStatement select;
		Statement update;
		ResultSet cursor;

		update = conn.createStatement();
		conn.setAutoCommit(true);
		verifyCount(
			update.executeUpdate("drop index i11"),
			0);
		verifyCount(
			update.executeUpdate("create index i11 on t1 (c3 desc, c1, c5 desc)"),
			0);
		conn.setAutoCommit(false);

		update = conn.createStatement();
		select = conn.prepareStatement("select c3, c1 from t1 where c3 > 125 and c1 > 0 for update");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < 125; i++)
		{
			nextRow(cursor, 2);
			/* mixed direction, half of them (whose change direction is the same as the index
			 * scan) have to go into the hash table.
			 */
			if (i % 2 == 0)
				update.execute("update t1 set c3 = c3 + 1 where current of " + cursor.getCursorName());
			else
				update.execute("update t1 set c3 = c3 - 1 where current of " + cursor.getCursorName());
		}
		if (! ifRow(cursor, 2))
			System.out.println("TEST UPDATE USING DESC INDEX: got 125 rows");
		else
			System.out.println("TEST UPDATE USING DESC INDEX FAILED! GOT MORE ROWS");
		cursor.close();
		select.close();

		System.out.println("************ See what we have in table:");

		select = conn.prepareStatement("select c3, c2 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < 250; i++)
			nextRow(cursor, 2);
		if (! ifRow(cursor, 2))
			System.out.println("TEST UPDATE USING DESC INDEX: got 250 rows");
		else
			System.out.println("TEST UPDATE USING DESC INDEX FAILED! GOT MORE ROWS");
		conn.rollback();
	}

	static void testUpdateDeleteWarning() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("update t2 set c1 = 2 where c1 = 1");
		SQLWarning sw = stmt.getWarnings();
		if (sw != null)
			System.out.println("TEST FAILED!  The update should not return a warning.");
		stmt.executeUpdate("update t2 set c1 = 2 where c1 = 1");
		sw = stmt.getWarnings();
		String state, msg;
		if (sw == null)
			System.out.println("TEST FAILED!  The update should return a warning.");
		else
		{
			state = sw.getSQLState();
			if (! state.equals("02000"))
				System.out.println("TEST FAILED!  Wrong sql state.");
			msg = sw.getMessage();
			if (! msg.startsWith("No row was found for FETCH, UPDATE or DELETE"))
				System.out.println("TEST FAILED!  Wrong message: " + msg);
		}

		stmt.executeUpdate("delete from t2 where c1 = 2");
		sw = stmt.getWarnings();
		if (sw != null)
			System.out.println("TEST FAILED!  The delete should not return a warning.");
		stmt.executeUpdate("delete from t2 where c1 = 2");
		sw = stmt.getWarnings();
		if (sw == null)
			System.out.println("TEST FAILED!  The delete should return a warning.");
		else
		{
			state = sw.getSQLState();
			if (! state.equals("02000"))
				System.out.println("TEST FAILED!  Wrong sql state.");
			msg = sw.getMessage();
			if (! msg.startsWith("No row was found for FETCH, UPDATE or DELETE"))
				System.out.println("TEST FAILED!  Wrong message: " + msg);
		}

		stmt.executeUpdate("delete from t3");
		sw = stmt.getWarnings();
		if (sw == null)
			System.out.println("TEST FAILED!  The delete cascade should return a warning.");
		else
		{
			state = sw.getSQLState();
			if (! state.equals("02000"))
				System.out.println("TEST FAILED!  Wrong sql state.");
			msg = sw.getMessage();
			if (! msg.startsWith("No row was found for FETCH, UPDATE or DELETE"))
				System.out.println("TEST FAILED!  Wrong message: " + msg);
		}
	}
}

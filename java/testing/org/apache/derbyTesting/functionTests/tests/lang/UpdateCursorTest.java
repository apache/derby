/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UpdateCursorTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

/**
 * This tests updateable cursor using index, Beetle entry 3865.
 * 
 * Not done in ij since we need to do many "next" and "update" to be able to
 * excercise the code of creating temp conglomerate for virtual memory heap. We
 * need at minimum 200 rows in table, if "maxMemoryPerTable" property is set to
 * 1 (KB). This includes 100 rows to fill the hash table and another 100 rows to
 * fill the in-memory heap.
 */
public class UpdateCursorTest extends BaseJDBCTestCase {

	private static final int SIZE_OF_T1 = 250;
	private static final int MAX_CAP_OF_HASH_TABLE = 100;
	private static final String EXPECTED_SQL_CODE = "02000";
	private static final String UNEXPECTED_MSG = "No row was found for FETCH, UPDATE or DELETE";

	/**
	 * Basic constructor.
	 */
	public UpdateCursorTest(String name) {
		super(name);
	}

	/**
	 * Sets the auto commit to false.
	 */
	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);
	}

	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite() {
		Properties props = new Properties();

		props.setProperty("derby.language.maxMemoryPerTable", "1");
		return new SystemPropertyTestSetup(new CleanDatabaseTestSetup(
				new TestSuite(UpdateCursorTest.class, "UpdateCursorTest")) {

			/**
			 * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
			 */
			protected void decorateSQL(Statement s) throws SQLException {
				StringBuffer sb = new StringBuffer(1000);
				String largeString;
				PreparedStatement pstmt;

				assertUpdateCount(s, 0, "create table T1 ("
						+ "  c1	int," + "  c2	char(50)," + "  c3	int," + "  c4   char(50),"
						+ "  c5   int," + "  c6   varchar(1000))"); 		
				assertUpdateCount(s, 0, "create index I11 on T1(c3, c1, c5)"); 	
				assertUpdateCount(s, 0, "create table T2("
						+ "  c1 	int)"); 		
				assertUpdateCount(s, 0, "create table T3("
						+ "  c1	char(20) not null primary key)"); 		
				assertUpdateCount(s, 0, "create table T4("
						+ "  c1 	char(20) references T3(c1) on delete cascade)"); 		

				/* fill the newly created tables */
				for (int i = 0; i < 1000; i++) {
					sb.append('a');
				}

				pstmt = s.getConnection().prepareStatement("insert into T1 values (?, ?, ?, ?, ?, ?), " +
						"(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?), "
						+ "(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)");

				largeString = new String(sb);
				for (int i = 246; i > 0; i = i - 5) {
					int k = 0;

					for (int j = 0; j < 5; j++) {
						pstmt.setInt(1 + k, i + (4 - j));
						pstmt.setString(2 + k, new Integer(i).toString());
						pstmt.setInt(3 + k, i + j);
						pstmt.setString(4 + k, new Integer(i).toString());
						pstmt.setInt(5 + k, i);
						pstmt.setString(6 + k, largeString);

						k += 6;
					}

					assertUpdateCount(pstmt, 5);
				}
				s.executeUpdate("insert into t2 values (1)");
				pstmt.close();
			}

		}, props);
	}

	/**
	 * Test the virtual memory heap.
	 * 
	 * @throws SQLException
	 */
	public void testVirtualMemoryHeap() throws SQLException {
		PreparedStatement select = prepareStatement("select c1, c3 from t1 where c3 > 1 and c1 > 0 for update");
		Statement update = createStatement();
		String cursorName;
		ResultSet cursor;
		int expectedValue = 1;

		cursor = select.executeQuery(); // cursor is now open
		cursorName = cursor.getCursorName();

		/* scan the entire table except the last row. */
		for (int i = 0; i < SIZE_OF_T1 - 1; i++) {	
			
			/*	Notice the order in the rows we get: from 2 to 102 asc order on second column (c3)
			 *	then from 202 down to 103 on that column; then from 203 up to 250.  The reason is
			 *	we are using asc index on c3, all the rows updated are in the future direction of the
			 *	index scan, so they all get filled into a hash table.  The MAX_MEMORY_PER_TABLE
			 *	property determines max cap of hash table 100.  So from row 103 it goes into virtual
			 *	memory heap, whose in memory part is also 100 entries.  So row 103 to 202 goes into
			 *	the in-memory part and gets dumped out in reverse order.  Finally Row 203 to 250"
			 *	goes into file system.  Here we mean row ids.
			 */
			if (i < MAX_CAP_OF_HASH_TABLE + 1) {
				expectedValue++;
			} else if (i > MAX_CAP_OF_HASH_TABLE && i <= MAX_CAP_OF_HASH_TABLE * 2) {
				if (i == MAX_CAP_OF_HASH_TABLE + 1) {
					expectedValue = 202;
				} else {
					expectedValue--;
				}
			} else if (i > MAX_CAP_OF_HASH_TABLE * 2) {
				if (i == MAX_CAP_OF_HASH_TABLE * 2 + 1) {
					expectedValue = 203;
				} else {
					expectedValue++;
				}
			}
			
			assertEquals(cursor.next(), true);
			//System.out.println("Row " + i + ": "+cursor.getInt(1)+","+cursor.getInt(2)+": "+expectedValue);
			assertEquals("Virtual memory heap test failed! Got unexpected value.", expectedValue, cursor.getInt(2));
			update.execute("update t1 set c3 = c3 + 250 where current of " + cursorName);
		}
		assertFalse(
				"Update with virtual memory heap failed! Still got rows.",
				cursor.next());

		cursor.close();
		update.close();

		/* see what we have in the table */
		select = prepareStatement("select c1, c3 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < SIZE_OF_T1; i++) {
			assertEquals(cursor.next(), true);
		}
		assertFalse(
				"Update with virtual memory heeap failed! Got more rows.",
				cursor.next());

		select.close();
		cursor.close();

		rollback();
	}

	/**
	 * Tests non covering index.
	 * 
	 * @throws SQLException
	 */
	public void testNonCoveringIndex() throws SQLException {
		PreparedStatement select;
		Statement update;
		ResultSet cursor;
		String cursorName;

		update = createStatement();
		select = prepareStatement("select c3, c2 from t1 where c3 > 125 and c1 > 0 for update");
		cursor = select.executeQuery(); // cursor is now open
		cursorName = cursor.getCursorName();

		for (int i = 0; i < (SIZE_OF_T1 / 2); i++) {
			assertEquals(cursor.next(), true);
			update.execute("update t1 set c3 = c3 + 25 where current of " + cursorName);
		}
		assertFalse(
				"Update using noncovering index failed! Still got rows.",
				cursor.next());

		cursor.close();
		select.close();

		/* see what we have in the table */
		select = prepareStatement("select c1, c3 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < SIZE_OF_T1; i++) {
			assertEquals(cursor.next(), true);
		}
		assertFalse(
				"Update using noncovering index failed! Got more rows.", cursor
						.next());

		select.close();
		cursor.close();

		rollback();
	}

	/**
	 * Tests descending index.
	 * 
	 * @throws SQLException
	 */
	public void testDescendingIndex() throws SQLException {
		PreparedStatement select;
		Statement update;
		ResultSet cursor;

		update = createStatement();

		/* drop index and recreate it */
		assertUpdateCount(update, 0, "drop index I11");
		assertUpdateCount(update, 0, "create index I11 on T1 (c3 desc, c1, c5 desc)");
		commit();

		update = createStatement();
		select = prepareStatement("select c3, c1 from t1 where c3 > 125 and c1 > 0 for update");
		cursor = select.executeQuery(); // cursor is now open

		for (int i = 0; i < (SIZE_OF_T1 / 2); i++) {
			assertEquals(cursor.next(), true);
			if ((i % 2) == 0) {
				update.execute("update t1 set c3 = c3 + 1 where current of " + cursor.getCursorName());
			} else {
				update.execute("update t1 set c3 = c3 - 1 where current of " + cursor.getCursorName());
			}
		}
		assertFalse("Update using desc index failed! Got more rows.",
				cursor.next());

		cursor.close();
		select.close();

		/* see what we have in the table */
		select = prepareStatement("select c3, c2 from t1");
		cursor = select.executeQuery(); // cursor is now open
		for (int i = 0; i < SIZE_OF_T1; i++) {
			assertEquals(cursor.next(), true);
		}
		assertFalse("Update using desc index failed! Got more rows.",
				cursor.next());

		select.close();
		cursor.close();

		rollback();
	}

	/**
	 * Test if the correct warnings are raised.
	 * 
	 * @throws SQLException
	 */
	public void testUpdateDeleteWarning() throws SQLException {
		Statement stmt = createStatement();
		SQLWarning sw;

		stmt.executeUpdate("update t2 set c1 = 2 where c1 = 1");
		sw = stmt.getWarnings();
		assertNull("The update should not return a warning.", sw);

		stmt.executeUpdate("update t2 set c1 = 2 where c1 = 1");
		sw = stmt.getWarnings();
		assertNotNull("The update should return a warning.", sw);
		assertEquals("Wrong sql state.", EXPECTED_SQL_CODE, sw
				.getSQLState());

		stmt.executeUpdate("delete from t2 where c1 = 2");
		sw = stmt.getWarnings();
		assertNull("The delete should not return a warning.", sw);

		stmt.executeUpdate("delete from t2 where c1 = 2");
		sw = stmt.getWarnings();
		assertNotNull("The delete should return a warning.", sw);
		assertEquals("Wrong sql state.", EXPECTED_SQL_CODE, sw
				.getSQLState());

		stmt.executeUpdate("delete from t3");
		sw = stmt.getWarnings();
		assertNotNull("The delete cascade should return a warning.", sw);
		assertEquals("Wrong sql state.", EXPECTED_SQL_CODE, sw
				.getSQLState());

		stmt.close();

		rollback();
	}
}

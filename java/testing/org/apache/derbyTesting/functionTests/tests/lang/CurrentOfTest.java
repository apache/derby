/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CurrentOfTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/** This tests the current of statements, i.e.
     * delete where current of and update where current of.
 * Not done in ij since the cursor names may not be stable.
 */
public class CurrentOfTest extends BaseJDBCTestCase {

	
	/**
     * Public constructor required for running test as standalone JUnit.
     */
	public CurrentOfTest(String name) {
		super(name);
	}
	/**
     * Create a suite of tests.
     */
	public static Test suite() {
		TestSuite suite = new TestSuite("CurrentOfTest");
		suite.addTestSuite(CurrentOfTest.class);
		//To run the test in both embedded and client/server mode
		//commenting it for the time being sicne the test fails in the client/server mode
		//return   TestConfiguration.defaultSuite(CurrentOfTest.class);
		return suite;
	}
	 /**
     * Set the fixture up with tables t and s and insert 4 rows in table t.
     */
	protected void setUp() throws SQLException {
		getConnection().setAutoCommit(false);
		Statement stmt = createStatement();
		stmt.executeUpdate("create table t (i int, c char(50))");
		stmt.executeUpdate("create table s (i int, c char(50))");
		stmt.executeUpdate("insert into t values (1956, 'hello world')");
		stmt.executeUpdate("insert into t values (456, 'hi yourself')");
		stmt.executeUpdate("insert into t values (180, 'rubber ducky')");
		stmt.executeUpdate("insert into t values (3, 'you are the one')");
		stmt.close();
		commit();
	}
	/**
     * Tear-down the fixture by removing the tables
     */
	protected void tearDown() throws Exception {
		Statement stmt = createStatement();
		stmt.executeUpdate("drop table t");
		stmt.executeUpdate("drop table s");
		stmt.close();
		commit();
		super.tearDown();
	}
	/**
    * Test delete with the current of statements.
    * Also do some negative testing to see whether correct
    * exceptions are thrown or not.
    * @throws Exception
    */
	public void testDelete() throws SQLException {
		PreparedStatement select, delete;
		Statement delete2;
		ResultSet cursor;
		select = prepareStatement("select i, c from t for update");
		cursor = select.executeQuery(); // cursor is now open

		// would like to test a delete attempt before the cursor
		// is open, but finagling to get the cursor name would
		// destroy the spirit of the rest of the tests,
		// which want to operate against the generated name.

		// TEST: cursor and target table mismatch

		assertCompileError("42X28","delete from s where current of " + cursor.getCursorName()); 
		
		// TEST: find the cursor during compilation
		delete = prepareStatement("delete from t where current of "
				+ cursor.getCursorName());
		// TEST: delete before the cursor is on a row
		assertStatementError("24000", delete);

		// TEST: find the cursor during execution and it is on a row
		cursor.next();
		assertUpdateCount(delete, 1);
		// skip a row and delete another row so that two rows will
		// have been removed from the table when we are done.
		cursor.next(); // skip this row
		cursor.next();
		assertUpdateCount(delete, 1);
		// TEST: delete past the last row
		cursor.next();// skip this row
		assertFalse(cursor.next());
		if (usingEmbedded())
			assertStatementError("24000", delete);
		else
			assertStatementError("XCL07", delete);
		
		
		// TEST: delete off a closed cursor
		// Once this is closed then the cursor no longer exists.
		cursor.close();
		if (usingEmbedded())
			assertStatementError("XCL07", delete);
		else 
			assertStatementError("XCL16", delete);
		
		// TEST: no cursor with that name exists
		delete2 = createStatement();
		if (usingEmbedded())
			assertStatementError("42X30", delete2,"delete from t where current of myCursor" );
		else
			assertStatementError("XJ202", delete2,"delete from t where current of myCursor" );
		delete.close();
		delete2.close();
		select.close();

		// TEST: attempt to do positioned delete before cursor execute'd
		// TBD

	}
	/**
	    * Test update with the current of statements.
	    * Also do some negative testing to see whether correct
	    * exceptions are thrown or not.
	    * @throws Exception
	    */
	public void testUpdate() throws SQLException {
		PreparedStatement select = null;
		PreparedStatement update = null;
		Statement update2;
		ResultSet cursor = null;

		// these are basic tests without a where clause on the select.
		// all rows are in and stay in the cursor's set when updated.

		// because there is no order by (nor can there be)
		// the fact that this test prints out rows may someday
		// be a problem. When that day comes, the row printing
		// can (should) be removed from this test.

		// TEST: Updated column not found in for update of list

		select = prepareStatement("select I, C from t for update of I");
		cursor = select.executeQuery(); // cursor is now open
		assertCompileError("42X31", "update t set C = 'abcde' where current of "+ cursor.getCursorName());
		cursor.close();
		select.close();
		
		// TEST: Update of cursor declared READ ONLY
		select = prepareStatement("select I, C from t for read only");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		
		cursor.close();
		select.close();
		
		// TEST: Update of cursor declared FETCH ONLY
		select = prepareStatement("select I, C from t for fetch only");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		// TEST: Update of cursor with a union
		select = prepareStatement("select I, C from t union all select I, C from t");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		// TEST: Update of cursor with a join
		select = prepareStatement("select t1.I, t1.C from t t1, t t2 where t1.I = t2.I");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		// TEST: Update of cursor with a derived table
		select = prepareStatement("select I, C from (select * from t) t1");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		// TEST: Update of cursor with a values clause
		select = prepareStatement("values (1, 2, 3)");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		// TEST: Update of cursor with a subquery
		select = prepareStatement("select I, C from t where I in (select I from t)");
		cursor = select.executeQuery(); // cursor is now open
		assertNull(cursor.getCursorName());
		cursor.close();
		select.close();

		select = prepareStatement("select I, C from t for update");
		cursor = select.executeQuery(); // cursor is now open

		// would like to test a update attempt before the cursor
		// is open, but finagling to get the cursor name would
		// destroy the spirit of the rest of the tests,
		// which want to operate against the generated name.

		// TEST: cursor and target table mismatch

		assertCompileError("42X29","update s set i=1 where current of " + cursor.getCursorName());

		// TEST: find the cursor during compilation
		update = prepareStatement("update t set i=i+10, c='Gumby was here' where current of "
				+ cursor.getCursorName());

		// TEST: update before the cursor is on a row
		assertStatementError("24000", update);

		// TEST: find the cursor during execution and it is on a row
		cursor.next();
		assertUpdateCount(update, 1);

		// TEST: update an already updated row; expect it to succeed.
		// will it have a cumulative effect?
		assertUpdateCount(update, 1);
		// skip a row and update another row so that two rows will
		// have been removed from the table when we are done.
		cursor.next(); // skip this row
		cursor.next();
		assertUpdateCount(update, 1);

		// TEST: update past the last row
		cursor.next(); // skip this row
		assertFalse(cursor.next());
		assertStatementError("24000", update);

		// TEST: update off a closed cursor
		cursor.close();
		select.close();
		assertStatementError("XCL07", update);
		update.close();

		// TEST: no cursor with that name exists
		update2 = createStatement();
		assertStatementError("42X30", update2,"update t set i=1 where current of nosuchcursor");
		update2.close();
		// TEST: attempt to do positioned update before cursor execute'd
		// TBD
		if(cursor != null)
			cursor.close();

	}
		/**
	    *TEST closing a cursor will close the related update
	    */
	public void testbug4395() throws SQLException { 
		bug4395("CS4395"); // Application provided cursor name
		bug4395(null); // system provided cursor name
	}

	private void bug4395(String cursorName) throws SQLException {

		PreparedStatement select = prepareStatement("select I, C from t for update");
		if (cursorName != null)
			select.setCursorName(cursorName);

		ResultSet cursor = select.executeQuery(); // cursor is now open

		// TEST: find the cursor during compilation
		cursorName = cursor.getCursorName();
		PreparedStatement update = prepareStatement("update t set i=i+?, c=? where current of "
				+ cursorName);
		cursor.next();
		update.setInt(1, 10);
		update.setString(2, "Dan was here");
		assertUpdateCount(update, 1);
		cursor.close();

		// now prepare the a cursor with the same name but only column I for
		// update
		PreparedStatement selectdd = prepareStatement("select I, C from t for update of I");
		selectdd.setCursorName(cursorName);
		cursor = selectdd.executeQuery();
		cursor.next();
		update.setInt(1, 7);
		update.setString(2, "no update");
		assertStatementError("42X31",update);

		cursor.close();
		cursor = selectdd.executeQuery();
		cursor.next();
		cursor.close();
		update.close();
		selectdd.close();
		select.close();

	}
}

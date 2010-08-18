/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CurrentOfTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file ecept in compliance with
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
import org.apache.derbyTesting.junit.JDBC;
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
        JDBC.dropSchema(getConnection().getMetaData(),
                getTestConfiguration().getUserName());
		super.tearDown();
	}
	
	/**
     * Test read only statements.
	 */
	public void testReadOnlyCursors() throws SQLException {
		
		String[] readOnlySQL = 
		{
            "select I, C from t for read only",
            "select I, C from t for fetch only",
            "select I, C FROM T ORDER BY 1",
            "values (1, 2, 3)",
            
            // TEST: Update of cursor with a union
            "select I, C from t union all select I, C from t",
            // TEST: Update of cursor with a join
            "select t1.I, t1.C from t t1, t t2 where t1.I = t2.I",
            // TEST: Update of cursor with a derived table
            "select I, C from (select * from t) t1",
            // TEST: Update of cursor with a subquery
            "select I, C from t where I in (select I from t)"
                   
		};
        
        // NOTE: JDK 1.4 javadoc for ResultSet.getCursorName()
        // says it will throw an execption if the statement
        // cannot support a positioned update. However that
        // line was removed in JavaSE 6 (JDBC 4) javadoc.
        
        for (int i = 0; i < readOnlySQL.length; i++)
        {
            // The system will not give a cursor name
            // to a read only statement.
            PreparedStatement select = prepareStatement(readOnlySQL[i]);
            ResultSet cursor = select.executeQuery();
            assertNull(readOnlySQL[i], cursor.getCursorName());
            cursor.close();
            
            // but will if the user supplies one.
            select.setCursorName("PLEASE_UPDATE");
            cursor = select.executeQuery();
            assertEquals(readOnlySQL[i], "PLEASE_UPDATE", cursor.getCursorName());
            
            // but the cursor is marked as read only so positioned
            // statements will fail.
            assertCompileError("42X23",
                    "DELETE FROM T WHERE CURRENT OF PLEASE_UPDATE");
            
            assertCompileError("42X23",
                "UPDATE T SET I = 3 WHERE CURRENT OF PLEASE_UPDATE");
            
            cursor.close();
            select.close();
        }
	}
	/**
    * Test delete with the current of statements.
    * Also do some negative testing to see whether correct
    * exceptions are thrown or not.
    * @throws Exception
    */
	public void testDelete() throws SQLException {
		PreparedStatement select, delete;
		Statement delete1,delete2;
		ResultSet cursor;
		String tableRows = "select i, c from t for read only";
		
		delete1 = createStatement();
		Object[][] expectedRows = new Object[][]{{new String("1956"),new String("hello world")},                                       
												 {new String("456"),new String("hi yourself")},                                       
												 {new String("180"),new String("rubber ducky")},                                      
												 {new String("3"),new String("you are the one")}}; 
		JDBC.assertFullResultSet(delete1.executeQuery(tableRows), expectedRows, true);
		
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
		cursor.next();
		assertEquals(1956, cursor.getInt(1));
		
		// TEST: find the cursor during execution and it is on a row
		assertUpdateCount(delete, 1);
		// skip a row and delete another row so that two rows will
		// have been removed from the table when we are done.
		// skip this row
		cursor.next();
		assertEquals(456, cursor.getInt(1));
		cursor.next();
		assertEquals(180, cursor.getInt(1));
		assertUpdateCount(delete, 1);
		
		// TEST: delete past the last row
		cursor.next();// skip this row
		assertEquals(3, cursor.getInt(1));
		assertFalse(cursor.next());
		if (usingEmbedded())
			assertStatementError("24000", delete);
		else
			assertStatementError("42X30", delete);
		
		
		// TEST: delete off a closed cursor
		// Once this is closed then the cursor no longer exists.
		cursor.close();
		if (usingEmbedded())
			assertStatementError("42X30", delete);
		else 
			assertStatementError("XCL16", delete);
		
		// TEST: no cursor with that name exists
		delete2 = createStatement();
		assertStatementError("42X30", delete2,"delete from t where current of myCursor" );
		expectedRows = new Object[][]{{new String("456"),new String("hi yourself")},                                       
				   					  {new String("3"),new String("you are the one")}}; 
		JDBC.assertFullResultSet(delete1.executeQuery(tableRows), expectedRows, true);
		delete.close();
		delete2.close();
		select.close();
        
        // Test a cursor where not all the columns can be updated.
        // Positioned DELETE is still allowed.
        select = prepareStatement("SELECT I, C FROM T FOR UPDATE OF I");
        cursor = select.executeQuery();
        delete = prepareStatement("delete from t where current of "
                + cursor.getCursorName());
        assertTrue(cursor.next());
        assertUpdateCount(delete, 1);
        
        delete.close();
        delete1.close();        
        cursor.close();
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
		PreparedStatement select;
		PreparedStatement update;
		Statement select1,update2;
		ResultSet cursor;
		String tableRows = "select i, c from t for read only";

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

		//Making sure we have the correct rows in the table to begin with
		select1 = createStatement();
		Object[][] expectedRows = new Object[][]{{new String("1956"),new String("hello world")},                                       
				 {new String("456"),new String("hi yourself")},                                       
				 {new String("180"),new String("rubber ducky")},                                      
				 {new String("3"),new String("you are the one")}}; 
		JDBC.assertFullResultSet(select1.executeQuery(tableRows), expectedRows, true);	
		
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
		assertEquals(1956,cursor.getInt(1));
		assertUpdateCount(update, 1);

		// TEST: update an already updated row; expect it to succeed.
		// will it have a cumulative effect?
		assertUpdateCount(update, 1);
		// skip a row and update another row so that two rows will
		// have been removed from the table when we are done.
		cursor.next(); // skip this row
		assertEquals(456,cursor.getInt(1));
		cursor.next();
		assertEquals(180,cursor.getInt(1));
		assertUpdateCount(update, 1);

		// TEST: update past the last row
		cursor.next(); // skip this row
		assertEquals(3,cursor.getInt(1));
		assertFalse(cursor.next());
		assertStatementError("24000", update);

		// TEST: update off a closed cursor
		cursor.close();
		select.close();
		assertStatementError("42X30", update);
		update.close();

		// TEST: no cursor with that name exists
		update2 = createStatement();
		assertStatementError("42X30", update2,"update t set i=1 where current of nosuchcursor");
		update2.close();
		
		//Verifyin we have the correct updated rows in the table at the end
		expectedRows = new Object[][]{{new String("1976"),new String("Gumby was here")},                                       
				 {new String("456"),new String("hi yourself")},                                       
				 {new String("190"),new String("Gumby was here")},                                      
				 {new String("3"),new String("you are the one")}}; 
		JDBC.assertFullResultSet(select1.executeQuery(tableRows), expectedRows, true);
		// TEST: attempt to do positioned update before cursor execute'd
		// TBD
		
		cursor.close();

	}
    
    /**
     * Test the positioned update correctly recompiles when an index is added.
     */
    public void testUpdateRecompileCreateIndex() throws Exception
    {
        recompile("UPDATE T SET I = I + 1 WHERE CURRENT OF ",
                "CREATE INDEX IT ON T(I)", null);
    }
    
    /**
     * Test the positioned update correctly recompiles when the
     * definition of a function is changed.
     */
    public void testUpdateRecompileChangeFunction() throws Exception
    {
        Statement s = createStatement();
        s.execute("CREATE FUNCTION F(V INTEGER) RETURNS INTEGER " +
                "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA " +
                "EXTERNAL NAME '" + getClass().getName() + ".doubleValue'");
        commit();
        String changeSQL = "CREATE FUNCTION F(V INTEGER) RETURNS INTEGER " +
            "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA " +
            "EXTERNAL NAME '" + getClass().getName() + ".tripleValue'";
        int firstI = recompile("UPDATE T SET I = F(I) WHERE CURRENT OF ",
                "DROP FUNCTION F", changeSQL);
        
        String[][] values = new String[][]
              {{"3"}, {"180"}, {"456"}, {"1956"}};
        
        if (firstI == 180)
        {
            // 180 doubled to 360
            // 456 tripled to 1368
            values[1] = new String[] {"360"};
            values[2] = new String[] {"1368"};
        }
        else
        {
            // 456 doubled to 912
            // 180 tripled to 540
            values[1] = new String[] {"540"};
            values[2] = new String[] {"912"};
             
        }

        JDBC.assertFullResultSet(s.executeQuery("SELECT I FROM T ORDER BY I"),
                values);
        
        s.close();
    }    
    /**
     * Test the positioned delete correctly recompiles when an index is added.
     */
    public void testDeleteRecompileCreateIndex()  throws Exception
    {
        recompile("DELETE FROM T WHERE CURRENT OF ",
                "CREATE INDEX IT ON T(I)", null);
    }
    
    /**
     * Execute a select and then the positioned statement against it.
     * Then execute the changeSQL that should force a recompile of the
     * positioned statement. Then execute the positioned statement
     * again and finally check all is ok with check table.
     * 
     * The positioned statements are executed against the rows that
     * have I=180 and I=456.
     * 
     * @return the value of I for the first row that had the positioned
     * statement executed against it, ie. before the change SQl was executed.
     */
    private int recompile(String positionedSQL, String changeSQL1, String changeSQL2)
         throws SQLException
    {
        Statement s = createStatement();
        PreparedStatement select = prepareStatement("select I, C from t for update");
        ResultSet cursor = select.executeQuery();
        
        
        PreparedStatement update = prepareStatement(
                positionedSQL + cursor.getCursorName());
        
        // Execute the positioned statement against one row,
        // either i=180 or 456, which ever comes first.
        int firstRowI = -1;
        while (cursor.next())
        {
            int i = cursor.getInt(1);
            if (i == 180 || i == 456) {
                update.execute();
                firstRowI = i;
                break;
            }
        }
        assertTrue(firstRowI == 180 || firstRowI == 456);
 
        s.execute(changeSQL1);
        if (changeSQL2 != null)
            s.execute(changeSQL2);
        
        // And one more execute against one more row
        // either 180 or 456.
        int secondRowI = -1;
        while (cursor.next())
        {
            int i = cursor.getInt(1);
            if (i == 180 || i == 456) {
                update.execute();
                secondRowI = i;
                break;
            }
        }
        assertTrue(firstRowI !=secondRowI);
        assertTrue(secondRowI == 180 || secondRowI == 456);
        
        update.close();
        cursor.close();
        select.close();
        commit();
        s.close();
        
        assertCheckTable("T");
        
        return firstRowI;
    }

	/**
	 * Change the current cursor from the one the positioned
     * UPDATE and DELETE was compiled against to one that only has a
     * subset of the columns being updatable.
	 */
	public void testCursorChangeUpdateList() throws SQLException {
        
        // Update will fail
        cursorChange(
                "42X31",
                "select I, C from t for update",
                "update t set i=i+19, c='OLD' || cast(i as CHAR(20)) where current of ",
                "select I, C from t for update of I"
                );
        
        // Delete will succeed.
        cursorChange(
                null,
                "select I, C from t for update",
                "DELETE FROM t WHERE CURRENT OF ",
                "select I, C from t for update of I"
                );
	}
    
    /**
     * Change the current cursor from the one the positioned
     * UPDATE/DELETE was compiled against to one that is read only
     * against the same table.
     */
    public void testCursorChangeToReadOnly() throws SQLException {
    
        cursorChange(
            "42X23", // cursor is read only
            "select I, C from t for update",
            "update t set i=i+23 where current of ",
            "select I, C from t for fetch only"
            );
        
        cursorChange(
                "42X23", // cursor is read only
                "select I, C from t for update",
                "DELETE FROM t WHERE CURRENT OF ",
                "select I, C from t for fetch only"
                );
    }

    /**
     * Change the current cursor from the one the positioned
     * UPDATE was compiled against to one that is against
     * a different table.
     * 
     * test FAILS - once fixed should be renamed to lose
     * the FAIL prefix. Fails as the positioned update wor
     */
    public void testCursorChangeToDifferentTable() throws SQLException {
        
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO S(I,C) SELECT I,C FROM T");
        s.close();
        commit();
           
        cursorChange(
            "42X29",  // this is the error testUpdate() sees.
            "select I, C from t for update",
            "update t set i=i+23 where current of ",
            "SELECT I, C FROM S FOR UPDATE"
            );
        
        cursorChange(
            "42X28", // this is the error testDelete() sees.
            "select I, C from t for update",
            "DELETE FROM t WHERE CURRENT OF ",
            "SELECT I, C FROM S FOR UPDATE"
           );
        
    }    
    
    
    /**
     * Run cursorChange() with an application provided name
     * and a system provided name.
     * 
     */
    private void cursorChange(String sqlState,
            String initialCursor,
            String positionedStatement,
            String changeToCursor) throws SQLException
    {
        // Since these tests delete rows we add a couple more to
        // ensure any cursor we open has at least one row.
        Statement s = createStatement();
        s.executeUpdate("insert into t values (425, 'apache db derby')");
        s.executeUpdate("insert into t values (280, 'derby-user users')");
        s.close();
        commit();
        
        cursorChange(sqlState, "CHANGE_ME", initialCursor, positionedStatement, changeToCursor);
        cursorChange(sqlState, null, initialCursor, positionedStatement, changeToCursor);
    }
    
    /**
     * Test what happens to a positioned update when the cursor
     * it is compiled against changes to the SQL provided, changeToSQL. 
     * This test first prepares a cursor initialCursor 
     * using with the given name (or system name if null is passed in)
     * A cursor is opened and a positioned update is opened that updates.
     * 
     * If sqlState is null then no error is expected and thus the
     * positioned statement must update a single row.
     * Otherwise sqlState is the exected exception for the update.
     * 
     * If no error is expected then three rows will be either
     * updated or deleted depending on the positioned statement.
     * 
     * If an error is expected then two rows will be updated
     * or deleted.
     */
	private void cursorChange(String sqlState,
            String cursorName,
            String initialCursor,
            String positionedStatement,
            String changeToCursor) throws SQLException {

		PreparedStatement select = prepareStatement(initialCursor);
		if (cursorName != null)
			select.setCursorName(cursorName);

		ResultSet cursor = select.executeQuery(); // cursor is now open

		// TEST: find the cursor during compilation
		cursorName = cursor.getCursorName();
		PreparedStatement update = prepareStatement(
                positionedStatement + cursorName);
		assertTrue(cursor.next());
		assertUpdateCount(update, 1);
		cursor.close();

		// now prepare the a cursor with the same name but different SQL.
		PreparedStatement selectdd = prepareStatement(changeToCursor);
		selectdd.setCursorName(cursorName);
		cursor = selectdd.executeQuery();
		assertTrue(cursor.next());
        if (sqlState != null)
		    assertStatementError(sqlState,update);
        else
            assertUpdateCount(update, 1);

		cursor.close();
		
		// now execute the original statement again and the positioned update
		// will work.
		cursor = select.executeQuery();
		cursor.next();
		assertUpdateCount(update, 1);

		cursor.close();
		update.close();
		selectdd.close();
		select.close();

	}
    
    /*
    ** Routines
    */
    
    public static int doubleValue(int i)
    {
        return i * 2;
    }
    public static int tripleValue(int i)
    {
        return i * 3;
    }
}

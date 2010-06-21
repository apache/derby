/* Derby - Class org.apache.derbyTesting.functionTests.tests.lang.HoldCursorTest
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
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test hold cursor after commit
 */
public class HoldCursorTest extends BaseJDBCTestCase {
	/**
     * Public constructor required for running test as standalone JUnit.
     */
	public HoldCursorTest(String name) {
		super(name);
	}
	/**
     * Create a suite of tests.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("HoldCursorTest");

        suite.addTest(baseSuite(true));
        suite.addTest(baseSuite(false));

        return suite;
    }

    private static Test baseSuite(boolean embeddedMode) {
        String name = "HoldCursorTest:" + (embeddedMode ? "embedded" : "client");
        TestSuite suite = new TestSuite(name);

        // Add tests that every JVM jdk1.4 or above should be able to run.
        suite.addTestSuite(HoldCursorTest.class);

        if (!JDBC.vmSupportsJSR169()) {
             suite.addTest (new HoldCursorTest("StatementsInProcedureTest"));
        }

        Test test = suite;

        if (!embeddedMode) {
            test = TestConfiguration.clientServerDecorator(suite);
        }

        return new CleanDatabaseTestSetup(test);
    }

    /**
     * Set the fixture up with tables and insert rows .
     */
	protected void setUp() throws SQLException {
		getConnection().setAutoCommit(false);
		Statement stmt = createStatement();
		final int stringLength = 400;
	    stmt.executeUpdate("CREATE TABLE T1 (c11 int, c12 int, junk varchar(" +
	                       stringLength + "))");
	    PreparedStatement insertStmt = prepareStatement("INSERT INTO T1 VALUES(?,1,?)");
	    // We need to ensure that there is more data in the table than the
	    // client can fetch in one message (about 32K). Otherwise, the
	    // cursor might be closed on the server and we are not testing the
	    // same thing in embedded mode and client/server mode.
	    final int rows = 40000 / stringLength;
	    StringBuffer buff = new StringBuffer(stringLength);
	    for (int i = 0; i < stringLength; i++) {
	        buff.append(" ");
	    }
	    for (int i = 1; i <= rows; i++) {
	        insertStmt.setInt(1, i);
	        insertStmt.setString(2, buff.toString());
	        insertStmt.executeUpdate();
	    }
	    insertStmt.close();
	    stmt.executeUpdate( "CREATE TABLE T2 (c21 int, c22 int)" );
	    stmt.executeUpdate("INSERT INTO T2 VALUES(1,1)");
	    stmt.executeUpdate("INSERT INTO T2 VALUES(1,2)");
	    stmt.executeUpdate("INSERT INTO T2 VALUES(1,3)");
	    stmt.execute("create table testtable1 (id integer, vc varchar(100))");
	    stmt.execute("insert into testtable1 values (11, 'testtable1-one'), (12, 'testtable1-two')");
	    stmt.execute("create table testtable2 (id integer, vc varchar(100))");
	    stmt.execute("insert into testtable2 values (21, 'testtable2-one'), (22, 'testtable2-two')");
	    stmt.execute("create procedure MYPROC() language java parameter style java external name " +
	    				"'org.apache.derbyTesting.functionTests.tests.lang.HoldCursorTest.testProc' result sets 2");
	    stmt.executeUpdate("Create table bug4385 (i int not null primary key, c int generated always as identity)");
	    stmt.close();
	    commit();		
	}
	/**
	 * Drop tables for clean up
	 */
	protected void tearDown() throws Exception {
		Statement stmt = createStatement();
		stmt.executeUpdate( "DROP PROCEDURE MYPROC" );
	    stmt.executeUpdate( "DROP TABLE T1" );
	    stmt.executeUpdate( "DROP TABLE T2" );
	    stmt.executeUpdate( "DROP TABLE testtable1" );
	    stmt.executeUpdate( "DROP TABLE testtable2" );
	    stmt.executeUpdate( "DROP TABLE BUG4385" );
	    stmt.close();
	    stmt.close();
		commit();
		super.tearDown();
	}
	/**
	 * test cursor holdability after commit on multi table query
	 * @throws Exception
	 */
	 public void testHoldCursorOnMultiTableQuery() throws Exception
	  {
	    ResultSet	rs;
	    Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );

	    //open a cursor with multiple rows resultset
	    rs = s.executeQuery("select t1.c11, t2.c22 from t1, t2 where t1.c11=t2.c21");
	    rs.next();
	    assertEquals("1", rs.getString(2));
	    commit();
	    rs.next(); //because holdability is true, should be able to navigate the cursor after commit
	    assertEquals("2", rs.getString(2));
	    rs.close();
	 }
	 /**
	  * Test Chaging isolation levels with and without held curosors
	  * @throws Exception
	  */
	 public void testIsolationLevelChange() throws Exception
	  {
	     ResultSet	rs;
	    //set current isolation to read committed
	    setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
	    Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,ResultSet.HOLD_CURSORS_OVER_COMMIT );
	    //open a cursor with multiple rows resultset
	    rs = s.executeQuery("select * from t1");
	    rs.next();

	    //Changing to different isolation from the current isolation for connection
	    //will give an exception because there are held cursors
		assertIsolationError("X0X03",Connection.TRANSACTION_SERIALIZABLE);

	    //Close open cursors and then try changing to different isolation.
	    //It should work.
	    rs.close();
	    setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		// set the default holdability for the Connection and try setting the isolation level
		setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
		setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		createStatement().executeUpdate("SET ISOLATION RS");

			// test for bug4385 - internal ResultSets were being re-used incorrectly
			// will occur in with JDBC 2.0,1.2 but the first statement I found that
			// failed was an insert with generated keys.
			
		PreparedStatement ps = prepareStatement("insert into bug4385(i) values(?)", Statement.RETURN_GENERATED_KEYS);
		ps.setInt(1, 199);
		ps.executeUpdate();
		assertGetIntEquals(1,1,ps.getGeneratedKeys());
		rs.close();
		
		ps.setInt(1, 299);
		ps.executeUpdate();
		assertGetIntEquals(1,2,ps.getGeneratedKeys());
		rs.close();
		ps.close();
		rollback();

	    //switch back to default isolation & holdability
		setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
	 }
	 /**
	     * Test that drop table cannot be performed when there is an open
	     * cursor on that table.
	     *
	     * @exception SQLException 
	     */
	    public void testDropTable() throws SQLException {
	        setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
	        final String dropTable = "DROP TABLE T1";
	        Statement stmt1 = createStatement();
	        Statement stmt2 = createStatement();
	        ResultSet rs = stmt1.executeQuery("SELECT * FROM T1");
	        rs.next();
	        assertStatementError("X0X95", stmt2,dropTable);
	        
	        //	dropping t1 should fail because there is an open cursor on t1
	        assertStatementError("X0X95", stmt2,dropTable);
	        commit();
	        
	        // cursors are held over commit, so dropping should still fail
	        assertStatementError("X0X95", stmt2,dropTable);
	        rs.close();

	        // cursor is closed, so this one should succeed
	        stmt2.executeUpdate(dropTable);
	        stmt1.close();
	        stmt2.close();
	        rollback();
	    }
	    /**
	     * set connection holdability and test holdability of statements inside and outside procedures
		 * test that holdability of statements always overrides holdability of connection
	     * @throws SQLException
	     */
		public void testHoldabilityOverCommit() throws SQLException{
				testStatements(ResultSet.HOLD_CURSORS_OVER_COMMIT);
				testStatements(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		}
		
		public void StatementsInProcedureTest()throws SQLException{
			StatementsInProcedure(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			StatementsInProcedure(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		}
		
		/**
		 * test holdability of statements outside procedures
		 * @param holdability
		 * @throws SQLException
		 */
		public void testStatements(int holdability) throws SQLException{
			setHoldability(holdability);
			//HOLD_CURSORS_OVER_COMMIT
			Statement st1 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
						ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			ResultSet rs1 = st1.executeQuery("select * from testtable1");
			rs1.next();
			assertEquals(11,rs1.getInt(1));
			
			commit();
			rs1.next();
			assertEquals(12,rs1.getInt(1));
			st1.close();
			
			//CLOSE_CURSORS_AT_COMMIT
			Statement st2 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
						ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ResultSet rs2 = st2.executeQuery("select * from testtable2");
			rs2.next();
			assertEquals(21,rs2.getInt(1));
			commit();
			assertNextError("XCL16",rs2);
			st2.close();
			rs1.close();
			rs2.close();
		 }
		
		/**
		 * test holdability of statements in procedures
		 * @param holdability
		 * @throws SQLException
		 */
		public void StatementsInProcedure(int holdability) throws SQLException{
			setHoldability(holdability);
			CallableStatement cs1 = prepareCall("call MYPROC()");
			cs1.execute();
			ResultSet rs2 = cs1.getResultSet();
			rs2.next();
			assertEquals(11,rs2.getInt(1));
			rs2.next();
			assertEquals(12,rs2.getInt(1));
			
			CallableStatement cs2 = prepareCall("call MYPROC()");
			cs2.execute();
			commit();
			ResultSet rs1 = cs2.getResultSet();
			rs1.next();
			assertEquals(11,rs1.getInt(1));
			if(rs1.next())
				assertEquals(12,rs1.getInt(1));
			else
				assertNull(rs1);
			cs1.close();
			cs2.close();
			rs1.close();
			rs2.close();
		}
		/**
		 * Test for drop table after closing the cursor
		 * @throws SQLException
		 */
		public void testCloseCursor()throws SQLException
	{
		// Run this test on one large table (T1) where the network
		// server won't close the cursor implicitly, and on one small
		// table (T2) where the network server will close the cursor
		// implicitly.
		final String[] tables = { "T1", "T2" };
		Statement stmt1 = createStatement();
		Statement stmt2 = createStatement();
		for (int i = 0; i < tables.length; i++) {
			String table = tables[i];
			ResultSet rs = stmt1.executeQuery("SELECT * FROM " + table);
			rs.next();
			rs.close();
			// Cursor is closed, so this should succeed. If the cursor
			// is open, it will fail because a table cannot be
			// dropped when there are open cursors depending on it.
			stmt2.executeUpdate("DROP TABLE " + table);
		}
		stmt1.close();
		stmt2.close();
		rollback();
	}
	/**
	  * Assert that the changing isolation throws
	  * an SQLException with the expected state.
	  * @param sqlState
	  * @param level
	  */
	    public void assertIsolationError(String sqlState, int level) {

	        try {
	        	getConnection().setTransactionIsolation(level);
	            fail("expected compile error: " + sqlState);
	        } catch (SQLException se) {
	            assertSQLState(sqlState, se);
	        }
	    }
	    /**
	     * Utility method to create a Statement using the connection
	     * returned by getConnection.
	     * @param resultSetType
	     * @param resultSetConcurrency
	     * @param resultSetHoldability
	     * @return Statement with desired holdability set
	     * @throws SQLException
	     */
	    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	    {
	        return getConnection().createStatement(resultSetType, resultSetConcurrency,resultSetHoldability);
	    }
	    /**
	     * Utility method to set Transaction Isolation
	     * @param level
	     * @throws SQLException
	     */
	    public void setTransactionIsolation(int level) throws SQLException
	    {
	    	getConnection().setTransactionIsolation(level);
	    }
	    /**
	     * Utility method to set Holdability
	     * @param holdability
	     * @throws SQLException
	     */
	    public void setHoldability(int holdability) throws SQLException
	    {
	    	getConnection().setHoldability(holdability);
	    }
	    /**
	     * Perform getInt(position) with expected error
	     * @param position
	     * @param expected
	     * @param rs
	     */
	    public static void assertGetIntEquals(int position,int expected,ResultSet rs)
	    {
	    	try{
	    	while(rs.next()){
	    	if(expected==rs.getInt(position))
	    		return;
	    	else 
	    		fail("Wrong value returned: "+ rs.getInt(position));
	    	}
	    }catch(SQLException se){
	    	se.printStackTrace();
	    		
	    	}
	    	
	    }
	     /**
	     * Java method for stored procedure
	     * @param rs1
	     * @param rs2
	     * @throws Exception
	     */
	    public static void testProc(ResultSet[] rs1, ResultSet[] rs2) throws Exception
		{
			Connection conn = DriverManager.getConnection("jdbc:default:connection");

			//HOLD_CURSORS_OVER_COMMIT
			Statement st1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
						ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			rs1[0] = st1.executeQuery("select * from testtable1");

			//CLOSE_CURSORS_AT_COMMIT
			Statement st2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
						ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			rs2[0] = st2.executeQuery("select * from testtable2");

		}
}

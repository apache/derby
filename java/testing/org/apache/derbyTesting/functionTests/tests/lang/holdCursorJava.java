/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.holdCursorJava

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test hold cursor after commit
 */
public class holdCursorJava {

  public static void main (String args[])
  {
    try {
		/* Load the JDBC Driver class */
		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();

		createAndPopulateTable(conn);

    //set autocommit to off after creating table and inserting data
    conn.setAutoCommit(false);
    testHoldability(conn,ResultSet.HOLD_CURSORS_OVER_COMMIT);
    testHoldability(conn,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		testHoldCursorOnMultiTableQuery(conn);
		testIsolationLevelChange(conn);

		conn.rollback();
		conn.close();
    } catch (Exception e) {
		System.out.println("FAIL -- unexpected exception "+e);
		JDBCDisplayUtil.ShowException(System.out, e);
		e.printStackTrace();
    }
  }

  //create table and insert couple of rows
  private static void createAndPopulateTable(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();

    System.out.println("Creating table...");
    stmt.executeUpdate( "CREATE TABLE T1 (c11 int, c12 int)" );
    stmt.executeUpdate("INSERT INTO T1 VALUES(1,1)");
    stmt.executeUpdate("INSERT INTO T1 VALUES(2,1)");
    stmt.executeUpdate( "CREATE TABLE T2 (c21 int, c22 int)" );
    stmt.executeUpdate("INSERT INTO T2 VALUES(1,1)");
    stmt.executeUpdate("INSERT INTO T2 VALUES(1,2)");
    stmt.executeUpdate("INSERT INTO T2 VALUES(1,3)");
    stmt.execute("create table testtable1 (id integer, vc varchar(100))");
    stmt.execute("insert into testtable1 values (11, 'testtable1-one'), (12, 'testtable1-two')");
    stmt.execute("create table testtable2 (id integer, vc varchar(100))");
    stmt.execute("insert into testtable2 values (21, 'testtable2-one'), (22, 'testtable2-two')");
    stmt.execute("create procedure MYPROC() language java parameter style java external name " +
    				"'org.apache.derbyTesting.functionTests.tests.lang.holdCursorJava.testProc' result sets 2");
    System.out.println("done creating table and inserting data.");

    stmt.close();
  }

  //test cursor holdability after commit on multi table query
  private static void testHoldCursorOnMultiTableQuery(Connection conn) throws Exception
  {
    Statement	s;
    ResultSet			rs;

    System.out.println("Start multi table query with holdability true test");
    s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
    ResultSet.HOLD_CURSORS_OVER_COMMIT );

    //open a cursor with multiple rows resultset
    rs = s.executeQuery("select t1.c11, t2.c22 from t1, t2 where t1.c11=t2.c21");
    rs.next();
    System.out.println("value of t2.c22 is " + rs.getString(2));
    conn.commit();
    rs.next(); //because holdability is true, should be able to navigate the cursor after commit
    System.out.println("value of t2.c22 is " + rs.getString(2));
    rs.close();
    System.out.println("Multi table query with holdability true test over");
  }

  //test cursor holdability after commit
  private static void testIsolationLevelChange(Connection conn) throws Exception
  {
    Statement	s;
    ResultSet			rs;

    System.out.println("Start isolation level change test");
    //set current isolation to read committed
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
    ResultSet.HOLD_CURSORS_OVER_COMMIT );

    //open a cursor with multiple rows resultset
    rs = s.executeQuery("select * from t1");
    rs.next();

    //Changing to different isolation from the current isolation for connection
    //will give an exception because there are held cursors
		try {
			System.out.println("Switch isolation while there are open cursors");
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		} catch (SQLException se) {

			System.out.println("Should see exceptions");
			String m = se.getSQLState();
			JDBCDisplayUtil.ShowSQLException(System.out,se);

			if ("X0X03".equals(m)) {
				System.out.println("PASS: Can't change isolation if they are open cursor");
			} else {
				System.out.println("FAIL: Shouldn't able to change isolation because there are open cursor");
			}
		}

    //Close open cursors and then try changing to different isolation.
    //It should work.
    rs.close();
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

	// set the default holdability for the Connection and try setting the isolation level


		conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
	conn.createStatement().executeUpdate("SET ISOLATION RS");

		// test for bug4385 - internal ResultSets were being re-used incorrectly
		// will occur in with JDBC 2.0,1.2 but the first statement I found that
		// failed was an insert with generated keys.
		conn.createStatement().executeUpdate("Create table bug4385 (i int not null primary key, c int generated always as identity)");
		conn.commit();

		PreparedStatement ps = conn.prepareStatement("insert into bug4385(i) values(?)", Statement.RETURN_GENERATED_KEYS);

		ps.setInt(1, 199);
		ps.executeUpdate();

		rs = ps.getGeneratedKeys();
		int count = 0;
		while (rs.next()) {
			rs.getInt(1);
			count++;
		}
		rs.close();
		if (count != 1)
			System.out.println("FAIL returned more than one row for generated keys");

		ps.setInt(1, 299);
		ps.executeUpdate();
		rs = ps.getGeneratedKeys();
		count = 0;
		while (rs.next()) {
			rs.getInt(1);
			count++;
		}
		if (count != 1)
			System.out.println("FAIL returned more than one row for generated keys on re-execution");
		rs.close();
		ps.close();
		conn.rollback();

    //switch back to default isolation & holdability
		conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

    System.out.println("Isolation level change test over");
	conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

	//set connection holdability and test holdability of statements inside and outside procedures
	//test that holdability of statements always overrides holdability of connection
	private static void testHoldability(Connection conn,int holdability) throws SQLException{
		
		conn.setHoldability(holdability);
		
		switch(holdability){
			case ResultSet.HOLD_CURSORS_OVER_COMMIT:
				System.out.println("\ntestHoldability with HOLD_CURSORS_OVER_COMMIT\n");
				break;
			case ResultSet.CLOSE_CURSORS_AT_COMMIT:
				System.out.println("\ntestHoldability with CLOSE_CURSORS_AT_COMMIT\n");
				break;
		}
	
		testStatements(conn);
	  	testStatementsInProcedure(conn);
	}
	
	//test holdability of statements outside procedures
	private static void testStatements(Connection conn) throws SQLException{
	    System.out.println("\ntestStatements()\n");
		
		//HOLD_CURSORS_OVER_COMMIT
		Statement st1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
					ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		ResultSet rs1 = st1.executeQuery("select * from testtable1");
		checkResultSet(rs1, "before");
		conn.commit();
		checkResultSet(rs1, "after");
		st1.close();
		
		//CLOSE_CURSORS_AT_COMMIT
		Statement st2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE ,
					ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
		ResultSet rs2 = st2.executeQuery("select * from testtable2");
		checkResultSet(rs2, "before");
		conn.commit();
		checkResultSet(rs2, "after");
		st2.close();
	 }
	
	//test holdability of statements in procedures
	private static void testStatementsInProcedure(Connection conn) throws SQLException{
		System.out.println("\ntestStatementsInProcedure()\n");
		
		CallableStatement cs1 = conn.prepareCall("call MYPROC()");
		cs1.execute();
		do{
			checkResultSet(cs1.getResultSet(), "before");
		}while(cs1.getMoreResults());
				
		CallableStatement cs2 = conn.prepareCall("call MYPROC()");
		cs2.execute();
		conn.commit();
		do{
			checkResultSet(cs2.getResultSet(),"after");
		}while(cs2.getMoreResults());
		
		cs1.close();
		cs2.close();
	}
	
	//check if resultset is accessible 
	private static void checkResultSet(ResultSet rs, String beforeOrAfter) throws SQLException{
		System.out.println("checkResultSet "+ beforeOrAfter  + " commit");
	    try{
	    	if(rs != null){
	    		rs.next();
	    		System.out.println(rs.getString(1) + ", " + rs.getString(2));
	    	}
	    	else{
	    		System.out.println("EXPECTED:ResultSet is null");
	    	}
	  	} catch(SQLException se){
	  		System.out.println("EXPECTED EXCEPTION:"+se.getMessage());
	  	}
	}
	  
	//Java method for stored procedure
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

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.holdCursorJavaReflection

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

import java.lang.reflect.*;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test hold cursor after commit using reflection. This test is specifically to test
 * this feature under jdk13.
 */
public class holdCursorJavaReflection {

  //we implemented hold cursor functionality in EmbedConnection20 package and hence
  //the functionality is available under both jdk14 and jdk13 (though, jdbc in jdk13
  //doesn't have the api to access it).
  //An internal project in TIVOLI needed access to holdability under jdk13 and we
  //recommended them to use reflection to get to holdability apis under jdk13.
  //This will also be documented on our website under faq
  //In order to have a test for that workaround in our test suite, I am using reflection
  //for createStatement and prepareStatement and prepareCall.

  //prepareStatement and prepareCall take 4 parameters
  private static Class[] PREP_STMT_PARAM = { String.class, Integer.TYPE, Integer.TYPE, Integer.TYPE };
  private static Object[] PREP_STMT_ARG = { "select * from t1  where c12 = ?", new Integer(ResultSet.TYPE_FORWARD_ONLY),
   new Integer(ResultSet.CONCUR_READ_ONLY), new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT)};

  private static Object[] PREP_STMT_ERROR_ARG = { "select * from t1NotThere  where c12 = ?",
											new
												Integer(ResultSet.TYPE_FORWARD_ONLY),   new Integer(ResultSet.CONCUR_READ_ONLY), new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT)};
	
  //createStatement takes 3 parameters
  private static Class[] STMT_PARAM = { Integer.TYPE, Integer.TYPE, Integer.TYPE };
  private static Object[] STMT_ARG = { new Integer(ResultSet.TYPE_FORWARD_ONLY),
   new Integer(ResultSet.CONCUR_READ_ONLY), new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT)};

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
		testHoldability(conn);
		testPreparedStatement(conn);
		testCallableStatement(conn);
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
    System.out.println("done creating table and inserting data.");

    stmt.close();
  }

  //test cursor holdability for callable statements
  private static void testCallableStatement(Connection conn) throws Exception
  {
    CallableStatement	cs;
    ResultSet			rs;

    System.out.println("Start hold cursor for callable statements test");

    //create a callable statement with hold cursor over commit using reflection.
    Method sh = conn.getClass().getMethod("prepareCall", PREP_STMT_PARAM);
    cs = (CallableStatement) (sh.invoke(conn, PREP_STMT_ARG));
    cs.setInt(1,1);
    rs = cs.executeQuery();

    System.out.println("do next() before commit");
    rs.next();
    System.out.println("look at first column's value: " + rs.getInt(1));
    conn.commit();
    System.out.println("After commit, look at first column's value: " + rs.getInt(1));
    System.out.println("do next() after commit. Should be at the end of resultset");
    rs.next();
    System.out.println("one more next() here will give no more rows");
    rs.next();
    System.out.println("Holdable cursor after commit for callable statements test over");
    rs.close();
  }

  //test cursor holdability after commit
  private static void testHoldability(Connection conn) throws Exception
  {
    Statement	s;
    PreparedStatement	ps;
    ResultSet			rs;

    System.out.println("Start holdable cursor after commit test");
    //create a statement with hold cursor over commit using reflection.
    Method sh = conn.getClass().getMethod("createStatement", STMT_PARAM);
    s = (Statement) (sh.invoke(conn, STMT_ARG));

    //open a cursor with multiple rows resultset
    rs = s.executeQuery("select * from t1");
    System.out.println("do next() before commit");
    rs.next();
    System.out.println("look at first column's value: " + rs.getInt(1));
    conn.commit();
    System.out.println("After commit, look at first column's value: " + rs.getInt(1));
    System.out.println("do next() after commit. Should be at the end of resultset");
    rs.next();
    System.out.println("one more next() here will give no more rows");
    rs.next();
    System.out.println("Holdable cursor after commit test over");
    rs.close();
  }

  //test cursor holdability for prepared statements
  private static void testPreparedStatement(Connection conn) throws Exception
  {
    PreparedStatement	ps;
    ResultSet			rs;

    System.out.println("Start hold cursor for prepared statements test");

    //create a prepared statement with hold cursor over commit using reflection.
    Method sh = conn.getClass().getMethod("prepareStatement", PREP_STMT_PARAM);
    ps = (PreparedStatement) (sh.invoke(conn, PREP_STMT_ARG));

    ps.setInt(1,1);
    rs = ps.executeQuery();

    System.out.println("do next() before commit");
    rs.next();
    System.out.println("look at first column's value: " + rs.getInt(1));
    conn.commit();
    System.out.println("After commit, look at first column's value: " + rs.getInt(1));
    System.out.println("do next() after commit. Should be at the end of resultset");
    rs.next();
    System.out.println("one more next() here will give no more rows");
    rs.next();
    System.out.println("Holdable cursor after commit for prepared statements test over");
    rs.close();

	// Create a prepared statement that will fail on prepare.  Make sure we
	// handle errors ok.
	sh = conn.getClass().getMethod("prepareStatement", PREP_STMT_PARAM);
	try {
		ps = (PreparedStatement) (sh.invoke(conn, PREP_STMT_ERROR_ARG));
		
		ps.setInt(1,1);
		rs = ps.executeQuery();
	}
	catch (SQLException se)
	{
		System.out.println("Expected Exception:" + se.getMessage());
	}
	catch (InvocationTargetException itex) {
		Throwable e = itex.getTargetException();
		//prepareStatement Can only throw SQLExcepton
		if (e instanceof SQLException)
		{
			SQLException se = (SQLException)e;
			System.out.println("Expected Exception:" + se.getMessage());
		}
		else
			throw itex;
	}
	// make sure our connection is still ok.
	conn.commit();
  }
	
}



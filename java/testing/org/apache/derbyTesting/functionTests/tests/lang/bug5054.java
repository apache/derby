/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.bug5054

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
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Where current of cursorname and case sensitivity
 */
public class bug5054 {

  public static void main (String args[]) 
  { 
    try {
		/* Load the JDBC Driver class */
		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();

      	createTables(conn);
      	doUpdates(conn);
      	dumpResult(conn);
		conn.close();
    } catch (Exception e) {
		System.out.println("FAIL -- unexpected exception "+e);
		JDBCDisplayUtil.ShowException(System.out, e);
      	e.printStackTrace();
    }
  } 

  private static void createTables(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
	try {
    	stmt.executeUpdate( "DROP TABLE T1" );
	}catch (Exception e) {}

    System.out.print("Creating tables...");
    stmt.executeUpdate( "CREATE TABLE T1 (a integer, b integer)" );
    stmt.executeUpdate( "INSERT INTO T1 VALUES(1, 1)" );
    stmt.executeUpdate( "INSERT INTO T1 VALUES(2, 2)" );
    System.out.println("done.");

    stmt.close();
  }

  private static void doUpdates(Connection conn) throws SQLException
  {
    int rc;
    conn.setAutoCommit(false);
    Statement stmt1 = conn.createStatement();
    stmt1.setCursorName("aBc");
    ResultSet rs = stmt1.executeQuery("select * from t1 for update");
    System.out.println("cursor name is " + rs.getCursorName());
    rs.next();

    Statement stmt2 = conn.createStatement();
    stmt2.execute("update t1 set b=11 where current of \"" + rs.getCursorName() + "\"");

    conn.commit();
    stmt1.close();
    stmt2.close();
    conn.setAutoCommit(true);
  }

  private static void dumpResult(Connection conn) throws SQLException
  {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(
      "SELECT * FROM T1"
    );
    System.out.println("T1 contents:");
    System.out.println("First row should have a b value of 11");
    while (rs.next()) {
      System.out.println(rs.getInt(1)+ " " + rs.getInt(2));
    }
  }

}

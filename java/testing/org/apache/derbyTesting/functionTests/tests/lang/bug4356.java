/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.bug4356

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Demonstrate subselect behavior with prepared statement. 
 */
public class bug4356 {
 
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
    	stmt.executeUpdate( "DROP TABLE T2" );
	}catch (Exception e) {}

    System.out.print("Creating tables...");
    stmt.executeUpdate( "CREATE TABLE T1 (a integer, b integer)" );
    stmt.executeUpdate( "CREATE TABLE T2 (a integer)" );
	stmt.executeUpdate("INSERT INTO T2 VALUES(1)");
    System.out.println("done.");

    stmt.close();
  }

  private static void doUpdates(Connection conn) throws SQLException
  {
    int rc;
	// bug only happens when autocommit is off
	conn.setAutoCommit(false);
    PreparedStatement stmt = conn.prepareStatement(
      "INSERT INTO T1 VALUES (?,(select count(*) from t2 where a = ?)) " 
    );
	stmt.setInt(1, 1);
    stmt.setInt(2, 1);

    rc = stmt.executeUpdate();

	stmt.setInt(1, 2);
    stmt.setInt(2, 2);
    rc = stmt.executeUpdate();

    conn.commit();
    stmt.close();
  }

  private static void dumpResult(Connection conn) throws SQLException
  {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(
      "SELECT * FROM T1"
    );
    System.out.println("T1 contents:");
    System.out.println("Second row should have a b value of 0");
    while (rs.next()) {
      System.out.println(rs.getInt(1)+ " " + rs.getInt(2));
    }
    rs.close();
    conn.commit();
    stmt.close();
  }

}

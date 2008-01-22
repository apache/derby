/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.derbyStress

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class derbyStress {
	
	private static int numConn = 1;
	private static int numRows = 100;
	private static int numPreparedStmts = 2000; 

	public static void main(String[] args) {
		try {
			System.out.println("Test derbyStress starting");

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = null;
			
			for (int i = 0; i < numConn; i++) {
				 conn = ij.startJBMS();
				 System.out.println("Testing with " + numPreparedStmts + " prepared statements");
				 prepStmtTest(conn, numRows, numPreparedStmts);
				 System.out.println("PASS -- Prepared statement test");
				 conn.close();
			}

			reExecuteStatementTest();
			testDerby3316();
			System.out.println("Test derbyStress finished.");
		} catch (SQLException se) {
			TestUtil.dumpSQLExceptions(se);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private static void createTables(Connection conn, int numRows) throws SQLException{
		Statement stmt = conn.createStatement();
		
		stmt.execute("create table t1 (lvc  LONG VARCHAR)");
		stmt.close();
		
		String insertTabSql = "insert into t1 values(?)";
		PreparedStatement ps = conn.prepareStatement(insertTabSql);
		 for (int i = 0; i < numRows; i++)
		 {
			 ps.setString(1,"Hello" + i);
			 ps.executeUpdate();
		 }
		 ps.close();
	}
	
	// Tests prepared statements are not leaked if not explicitly closed by
	// user (DERBY-210)
	private static void prepStmtTest(Connection conn, int numRows, int numPreparedStmts) throws Exception
	{
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn.setAutoCommit(false);
		 
		try {
		
			createTables(conn, numRows);
			
			String selTabSql = "select * from t1";
			
			for (int i = 0 ; i  < numPreparedStmts; i++)
			{
				ps = conn.prepareStatement(selTabSql);
				rs = ps.executeQuery();

				while (rs.next())
				{
					rs.getString(1);
				}

				rs.close();
			
				// Do not close the prepared statement
				//ps.close();
			}
			conn.commit();
		} 
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			conn.rollback();
		}
	}

	// Tests re-execution of a statement without closing the result
	// set (DERBY-557).
	private static void reExecuteStatementTest() throws Exception {
		System.out.print("DERBY-557: reExecuteStatementTest() ");
		Connection conn = ij.startJBMS();
		Statement stmt = conn.createStatement();
		for (int i = 0; i < 50000; i++) {
			ResultSet rs = stmt.executeQuery("values(1)");
			// How silly! I forgot to close the result set.
		}
		conn.commit();
		stmt.close();
		conn.close();
		System.out.println("PASSED");
	}
        
    /**
     * Test fix for leak if ResultSets are not closed.
     * @throws Exception
     */
    public static void testDerby3316() throws Exception {
          System.out.println("DERBY-3316: Multiple statement executions ");
          Connection conn = ij.startJBMS();
                
          Statement s = conn.createStatement();
          s.executeUpdate("CREATE TABLE TAB (col1 varchar(32672))");
          PreparedStatement ps = conn.prepareStatement("INSERT INTO TAB VALUES(?)");
          ps.setString(1,"hello");
          ps.executeUpdate();
          ps.setString(1,"hello");
          ps.executeUpdate();
          ps.close();
          for (int i = 0; i < 2000; i++)
          {
                  s = conn.createStatement();
                  ResultSet rs = s.executeQuery("SELECT * from tab");
                  // drain the resultset
                  while (rs.next());
                  // With DERBY-3316, If I don't explicitly close the resultset or 
                  // statement, we get a leak.
                  //rs.close();
                  //s.close();
          }    
          // close the connection to free up all the result sets that our sloppy 
          // user didn't close.
          conn.close();
          conn = ij.startJBMS();
          s = conn.createStatement();
          s.executeUpdate("DROP TABLE TAB");
          s.close();
          conn.close();
       }
      
}

/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.derbyStress

Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class derbyStress {
	
	private static String[] testObjects = {"table t1"};
	
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
		
		TestUtil.cleanUpTest(stmt, testObjects);
		
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
}

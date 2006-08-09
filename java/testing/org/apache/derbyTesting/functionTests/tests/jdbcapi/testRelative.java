/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.testRelative

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


import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class testRelative {
	
	static final String NO_CURRENT_ROW_SQL_STATE = 
		(TestUtil.isNetFramework() ? 
		 "XJ121" : "24000");
   
   public static void main(String[] args) {
	   System.out.println("Test testRelative starting");
	   Connection con = null;
	   try {
		   // use the ij utility to read the property file and
		   // make the initial connection.
		   ij.getPropertyArg(args);
		   con = ij.startJBMS();
		   test1(con);        
	   } catch (Exception e)
	   {
		   unexpectedException(e);
	   }
    }
    
    public static void test1(Connection con) {
		ResultSet rs = null;
		PreparedStatement pStmt = null;
		Statement stmt1 = null;
		String returnValue = null;
		
		try
		{
			con.setAutoCommit(false);
			pStmt = con.prepareStatement("create table testRelative(name varchar(10), i int)");
   			pStmt.executeUpdate();
   			con.commit();
   			
   			pStmt = con.prepareStatement("insert into testRelative values (?,?)");
   			   			
   			pStmt.setString(1,"work1");
			pStmt.setNull(2,1);
			pStmt.addBatch();
			
			pStmt.setString(1,"work2");
			pStmt.setNull(2,2);
			pStmt.addBatch();
			
			pStmt.setString(1,"work3");
			pStmt.setNull(2,3);
			pStmt.addBatch();
			
			pStmt.setString(1,"work4");
			pStmt.setNull(2,4);
			pStmt.addBatch();

		
			pStmt.executeBatch();
			con.commit();
		} catch(SQLException se) {
			unexpectedSQLException(se);
		} catch(Throwable t) {
			System.out.println("FAIL--unexpected exception: "+t.getMessage());
			t.printStackTrace(System.out);
		}
		try {
			testScrolling(ResultSet.CONCUR_READ_ONLY, con);
			testScrolling(ResultSet.CONCUR_UPDATABLE, con);
		} catch(Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e.getMessage());
			e.printStackTrace(System.out);
			
		}
	}

	private static void testScrolling(int concurrency, Connection con) 
		throws SQLException
	{
		Statement stmt1 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, concurrency);
		ResultSet rs = stmt1.executeQuery("select * from testRelative");
		
		rs.next(); // First Record
		System.out.println("Value = " + rs.getString("name"));
		
		rs.relative(2);
		System.out.println("Value = " + rs.getString("name"));
		System.out.println("isFirst = " + rs.isFirst() + 
						   " isLast = " + rs.isLast() + 
						   " isAfterLast = " + rs.isAfterLast());
		rs.relative(-2);
		System.out.println("Value = " + rs.getString("name"));
		
		try {
			rs.relative(10);
			System.out.println("Value = " + rs.getString("name"));
			System.out.println("isFirst = " + rs.isFirst() + 
							   " isLast = " + rs.isLast() + 
							   " isAfterLast = " + rs.isAfterLast());
		} catch(SQLException sqle) {
			
			expectedException(sqle, NO_CURRENT_ROW_SQL_STATE);
		} 
	}
     
      /**
	   *  Print the expected Exception's details if the SQLException SQLState
	   * matches the expected SQLState. Otherwise fail
	   *
	   * @param se  SQLException that was thrown by the test
	   * @param expectedSQLState  The SQLState that we expect. 
	   *
	   **/
	static private void expectedException (SQLException se, String expectedSQLState) {
           if( se.getSQLState() != null && (se.getSQLState().equals(expectedSQLState))) { 
                System.out.println("PASS -- expected exception");               
            } else {
	        System.out.println("FAIL--Unexpected SQLException: " +
							   "SQLSTATE(" +se.getSQLState() + ")" +
							   se.getMessage());
			while (se != null) {
				System.out.println("SQLSTATE("+se.getSQLState()+"): "+se.getMessage());
				se.printStackTrace(System.out);
				se = se.getNextException();
			}
			 
	    }
	}

     /**
       * We are here because we got an exception when did not expect one.
       * Hence printing the message and stack trace here.
       **/
     static private void unexpectedSQLException(SQLException se) {
	 System.out.println("FAIL -- Unexpected Exception: "+ 
						"SQLSTATE(" +se.getSQLState() +")" +
						se.getMessage());
	 se.printStackTrace(System.out);
     }

	static private void unexpectedException(Exception e) {
		System.out.println("FAIL -- Unexpected Exception: "+ 
						   e.getMessage());
	}
	
}


/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.largeCodeGen

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.*;

import org.apache.derby.tools.ij;


// This test tries to push byte code generation to the limit.
// It has to be run with a large amount of memory which is set with jvmflags in 
// largeCodeGen_app.properties
// There are only a few types of cases now. Other areas need to be tested such as large in clauses, etc.
// 

public class largeCodeGen
{
	private static boolean TEST_QUERY_EXECUTION = true;
	private static boolean PRINT_FAILURE_EXCEPTION = false;
	
    public static void main(String argv[]) 
       throws Exception
    {
    	ij.getPropertyArg(argv); 
        Connection con = ij.startJBMS();
        con.setAutoCommit(false);
        createTestTable(con);
        testLogicalOperators(con);
        testInClause(con);
        testUnions(con);
        con.commit();
        con.close();
    }
    
    private static void createTestTable(Connection con) throws SQLException
    {
    	 Statement stmt = null;		
    	 stmt = con.createStatement();
    	 try {
			stmt.executeUpdate("drop table t0 ");
		}catch (SQLException se)
		{
			// drop error ok.
			if (!se.getSQLState().equals("42Y55"))
				throw se;
		}	
		
		String createSQL = 	"create table t0 " +
		"(si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(3),vc varchar(20), lvc long varchar)";
		stmt.executeUpdate(createSQL);	
		stmt.executeUpdate("insert into t0 values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one')");
    }
    
    
	/**
	 * Prepares and executes query against table t0 with n parameters
	 * The assumption is that the query will always return our one row
	 * of data inserted into the t0 table.
	 * 
	 * @param con
	 * @param testName
	 * @param sqlBuffer  - StringBuffer with SQL Text
	 * @param numParams  - Number of parameters
	 * @param paramValue - Parameter value
	 * @return true if the check fails
	 */
	private static boolean checkT0Query(Connection con, String testName, 
				StringBuffer sqlBuffer, int numParams, int paramValue) {
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(sqlBuffer.toString());
			System.out.println("PASS: PREPARE: " + testName);
			if (TEST_QUERY_EXECUTION)
			{
				for (int i = 1; i <= numParams; i++)
				{	
					ps.setInt(i, paramValue);
				}
				ResultSet rs = ps.executeQuery();
				rs.next();
				checkRowData(rs);
				rs.close();
			}
			ps.close();
			System.out.println("PASS: " + testName);
			return false;
		}catch (Exception e)
		{
			reportFailure(testName, e);
			return true;
			
		}
	}

	/**
     * Test many parameters in the where clause
     * e.g. 
	 * @param con  
	 */
	private static void testLogicalOperators(Connection con)  throws SQLException {
		 
		// Fix to DERBY-921 - passed @ 800
		//   DERBY-921 - support 32bit branch offsets
		 for (int count = 200; count <= 10000 ; count += 100)
		 {
			 // keep testing until it fails with linkage error
			 if (testLogicalOperators(con, count))
				 break;
		 }
		 // 10,000 causes Stack overflow and database corruption
		 //testLogicalOperators(con, 10000);
	}

	
	/**
	 * Tests numParam parameter markers in a where clause
	 * 
	 * @param con          
	 * @param  numOperands 
	 */
	private static boolean testLogicalOperators(Connection con, 
				int numOperands) throws SQLException {
		
		// First with parameters
		String pred =  "(si = ? AND si = ? )";
		String testName = "Logical operators with " + numOperands + " parameters";
		StringBuffer sqlBuffer = new StringBuffer((numOperands * 20) + 512);
		sqlBuffer.append("SELECT * FROM T0 WHERE " + pred );
		for (int i = 2; i < numOperands; i+=2)
		{
			sqlBuffer.append(" OR " + pred);
		}
		return checkT0Query(con, testName, sqlBuffer, numOperands, 2);
		
		
		
		
	}
	
	private static void testInClause(Connection con)  throws SQLException {
	  
		// DERBY-739 raised number of parameters from 2700 to 3400
        // svn 372388 trunk - passed @ 3400
        // fixes for DERBY-766 to split methods with individual statements
        // bumps the limit to 98,000 parameters.
        testInClause(con, 3400);
		 for (int count = 97000; count <= 200000 ; count += 1000)
		 {
			 // keep testing until it fails.
			 if (testInClause(con, count))
			 	break;
		 }
	}	
	
	/**
	 * Test in clause with many parameters
	 *
	 * @param con
	 * @param numParams - Number of parameters to test
	 * @return true if the test fails
	 * @throws SQLException
	 */
	private static boolean testInClause(Connection con, int numParams) throws SQLException {
		String testName = "IN clause with " + numParams + " parameters";
		StringBuffer sqlBuffer = new StringBuffer((numParams * 20) + 512);
		sqlBuffer.append("SELECT * FROM T0 WHERE SI IN ("  );
		for (int i = 1; i < numParams; i++)
		{
			sqlBuffer.append("?, ");
		}
		sqlBuffer.append("?)");
		return checkT0Query(con, testName, sqlBuffer, numParams, 2); 	
	}
	
	private static void testUnions(Connection con) throws Exception
	{
		Statement stmt = null;
        PreparedStatement pstmt = null; 
        createTestTable(con);
		
		String viewName = "v0";		
		stmt = con.createStatement();

		
		try {
			stmt.executeUpdate("drop view " + viewName);
		}catch (SQLException se)
		{
			// drop error ok.
		}

				  
		StringBuffer createView = new StringBuffer("create view " + viewName + 
												   " as select * from t0 " );
		for (int i = 1; i < 100; i ++)
		{
			createView.append(" UNION ALL (SELECT * FROM t0 )");
		}
		//System.out.println(createViewString);
		stmt.executeUpdate(createView.toString());
		
		// svn 372388 trunk - passed @ 900
		for (int count = 800; count <= 10000; count += 100)
		{
			// keep testing until it fails
			if (largeUnionSelect(con, viewName, count))
				break;
		}
		// 10000 gives a different constant pool error
		largeUnionSelect(con, viewName, 10000);
    }
    
    private static boolean largeUnionSelect(Connection con, String viewName,
    		int numUnions) throws Exception
	{

    	// There are 100 unions in each view so round to the nearest 100
    	String testName = "SELECT with " + numUnions/100 * 100 + " unions";
		
		String unionClause = " UNION ALL (SELECT * FROM " + viewName + ")";

		StringBuffer selectSQLBuffer  =
			new StringBuffer(((numUnions/100) * unionClause.length()) + 512);
		
		selectSQLBuffer.append("select * from t0 ");
		
		for (int i = 1; i < numUnions/100;i++)
		{
			selectSQLBuffer.append(unionClause);
		}	
		
		try {
		// Ready to execute the problematic query 
		String selectSQL = selectSQLBuffer.toString();
		//System.out.println(selectSQL);
        PreparedStatement pstmt = con.prepareStatement(selectSQL);
        System.out.println("PASS: PREPARE: " + testName);
        if (largeCodeGen.TEST_QUERY_EXECUTION)
        {
	        ResultSet rs = pstmt.executeQuery();
			int numRowsExpected = (numUnions/100 * 100);
			int numRows = 0;
			while (rs.next())
			{
				numRows++;
				if ((numRows % 100) == 0)
				checkRowData(rs);
			}
			System.out.println("PASS: EXECUTE " + testName + " Row data check ok");
	        con.commit();
        }
        pstmt.close();
        return false;
     
		} catch (SQLException sqle)
		{
			reportFailure(testName, sqle);
			return true;
			
		}

      }

	// Check the data on the positioned row against what we inserted.
	private static void checkRowData(ResultSet rs) throws Exception
	{
		//" values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one')");
		String[] values = {"2", "3", "4", "5.3","5.3","5.3","31.13","123456.123",
						   "one","one","one"};
		for (int i = 1; i <= 11; i ++)
		{
			String rsValue = rs.getString(i);
			String expectedValue = values[i - 1];
			if (!rsValue.equals(values[i-1]))
				throw new Exception("Result set data value: " + rsValue +
									" does not match " + values[i-1] +
					                " for column " + i);				
		}
	}

	/**
	 * Show failure message and exception stack trace
	 * @param testName
	 * @param e
	 */
	private static void reportFailure(String testName, Exception e)
	{
		System.out.println("FAILED QUERY: " + testName +".");
		if (e instanceof SQLException)
		{
			SQLException se = (SQLException) e;
			while (se != null  && PRINT_FAILURE_EXCEPTION)
			{
				se.printStackTrace(System.out);
				se = se.getNextException();
			}
		}	
		else e.printStackTrace();
	
	}
	
}


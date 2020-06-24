/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;


// This test tries to push byte code generation to the limit.
// It has to be run with a large amount of memory which is set with jvmflags in 
// largeCodeGen_app.properties
// There are only a few types of cases now. Other areas need to be tested such as large in clauses, etc.
// 

public class largeCodeGen extends BaseJDBCTestCase
{
	private static boolean TEST_QUERY_EXECUTION = true;
    
   
    
    public largeCodeGen(String name)
    {
        super(name);
    }
    
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite();
        
        // Code generation test, just invoke on embedded
        // as the main function is to test the byte code compiler.
        if (usingEmbedded()) {
            suite.addTestSuite(largeCodeGen.class);
            return new CleanDatabaseTestSetup(suite);
        }
        return suite;
    }
       
    protected void setUp() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        getConnection().setAutoCommit(false);
    	Statement stmt = createStatement();
		
		String createSQL = 	"create table t0 " +
		"(si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(3),vc varchar(20), lvc long varchar)";
		stmt.executeUpdate(createSQL);	
		stmt.executeUpdate("insert into t0 values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one')");
        stmt.close();
        commit();
    }
    
    protected void tearDown() throws Exception
    {
        Statement stmt = createStatement();
        stmt.execute("DROP TABLE T0");
        stmt.close();
        commit();
        super.tearDown();
    }
    
    
	/**
	 * Prepares and executes query against table t0 with n parameters
	 * The assumption is that the query will always return our one row
	 * of data inserted into the t0 table.
	 * 
	 * @param testName
	 * @param sqlBuffer  - StringBuffer with SQL Text
	 * @param numParams  - Number of parameters
	 * @param paramValue - Parameter value
	 * @return true if the check fails
	 */
	private boolean checkT0Query(String testName, 
				StringBuffer sqlBuffer, int numParams, int paramValue) {
		PreparedStatement ps;
		try {
			ps = prepareStatement(sqlBuffer.toString());
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
            commit();
			return false;
		}catch (SQLException e)
		{
            // The top level exception is expected to be
            // the "user-friendly" query is too complex
            // rather than some linkage error.
            assertSQLState("42ZA0", e);
			return true;
			
		}
	}

	/**
     * Test many logical operators in the where clause.  
	 */
	public void testLogicalOperators()  throws SQLException {
		 
       int passCount = 0;
		 for (int count = 700; count <= 10000 ; count += 100)
		 {
			 // keep testing until it fails
			 if (logicalOperators(count))
				 break;
             
             passCount = count;
		 }
         
        // svn 372388 trunk - passed @ 400
        // Fix to DERBY-921 - passed @ 800
        // DERBY-921 - support 32bit branch offsets
        assertEquals("logical operators change from previous limit",
                800, passCount);
        
     
		 // 10,000 causes Stack overflow and database corruption
		 //testLogicalOperators(con, 10000);
	}

	
	/**
	 * Tests numParam parameter markers in a where clause
	 * 
	 * @param  numOperands 
	 */
	private boolean logicalOperators(int numOperands) throws SQLException {
		
		// First with parameters
		String pred =  "(si = ? AND si = ? )";
		String testName = "Logical operators with " + numOperands + " parameters";
		StringBuffer sqlBuffer = new StringBuffer((numOperands * 20) + 512);
		sqlBuffer.append("SELECT * FROM T0 WHERE " + pred );
		for (int i = 2; i < numOperands; i+=2)
		{
			sqlBuffer.append(" OR " + pred);
		}
		return checkT0Query(testName, sqlBuffer, numOperands, 2);
		
		
		
		
	}
	
	public void testInClause()  throws SQLException {
	  
		// DERBY-739 raised number of parameters from 2700 to 3400
        // svn 372388 trunk - passed @ 3400
        // So perform a quick check there.
        assertFalse("IN clause with 3400 parameters ", inClause(3400));
        
        int passCount = 0;
		 for (int count = 97000; count <= 200000 ; count += 1000)
		 {
 			 // keep testing until it fails.
			 if (inClause(count))
			 	break;
             passCount = count;
		 }
         
        // fixes for DERBY-766 to split methods with individual statements
        // bumps the limit to 98,000 parameters.
        assertEquals("IN clause change from previous limit", 98000, passCount);
	}	
	
	/**
	 * Test in clause with many parameters
	 *
	 * @param numParams Number of parameters to test
	 * @return true if the test fails
	 * @throws SQLException
	 */
	private boolean inClause(int numParams) throws SQLException {
		String testName = "IN clause with " + numParams + " parameters";
		StringBuffer sqlBuffer = new StringBuffer((numParams * 20) + 512);
		sqlBuffer.append("SELECT * FROM T0 WHERE SI IN ("  );
		for (int i = 1; i < numParams; i++)
		{
			sqlBuffer.append("?, ");
		}
		sqlBuffer.append("?)");
		return checkT0Query(testName, sqlBuffer, numParams, 2); 	
	}
	
	public void testUnions() throws SQLException
	{ 		
		String viewName = "v0";		
		Statement stmt = createStatement();
        
		StringBuffer createView = new StringBuffer("create view " + viewName + 
												   " as select * from t0 " );
		for (int i = 1; i < 100; i ++)
		{
			createView.append(" UNION ALL (SELECT * FROM t0 )");
		}
		//System.out.println(createViewString);
		stmt.executeUpdate(createView.toString());
        commit();
		
       int passCount = 0;
		for (int count = 1000; count <= 1000; count += 1000)
		{
 			// keep testing until it fails
			if (largeUnionSelect(viewName, count))
				break;
            passCount = count;
           
		}
        
        // 10000 gives a different constant pool error
        // DERBY-1315 gives out of memory error.
        //assertTrue("10000 UNION passed!",
        //        largeUnionSelect(viewName, 10000));

        createStatement().executeUpdate("DROP VIEW " + viewName);

        // svn 372388 trunk - passed @ 900
        // trunk now back to 700
        //
        assertEquals("UNION operators change from previous limit",
                1000, passCount);
        
        
    }
    
    private boolean largeUnionSelect(String viewName,
    		int numUnions) throws SQLException
	{

    	// There are 100 unions in each view so round to the nearest 100
		
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
        PreparedStatement pstmt = prepareStatement(selectSQL);
        if (largeCodeGen.TEST_QUERY_EXECUTION)
        {
	        ResultSet rs = pstmt.executeQuery();
			int numRows = 0;
			while (rs.next())
			{
				numRows++;
				if ((numRows % 100) == 0)
				checkRowData(rs);
			}
            rs.close();
	        commit();
        }
        pstmt.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-176
        return false;
     
		} catch (SQLException sqle)
		{
            // The top level exception is expected to be
            // the "user-friendly" query is too complex
            // rather than some linkage error.
            assertSQLState("42ZA0", sqle);

			return true;
			
		}

      }

	// Check the data on the positioned row against what we inserted.
	private static void checkRowData(ResultSet rs) throws SQLException
	{
		//" values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one')");
		String[] values = {"2", "3", "4", "5.3","5.3","5.3","31.13","123456.123",
						   "one","one","one"};
		for (int i = 1; i <= 11; i ++)
		{
            assertEquals("Result set data value: ",
                    values[i-1], rs.getString(i));
		}
	}
    
    /**
     * Test an INSERT statement with a large number of rows in the VALUES clause.
     * Reported as DERBY-1714.
     * @throws SQLException 
     *
     */
    public void testInsertValues() throws SQLException {
       int passCount = 0;
        for (int count = 1500; count <= 1700; count += 200) {
            // keep testing until it fails
            if (insertValues(count))
                break;
            passCount = count;

        }

        // Final fixes for DERBY-766 pushed the limit to 1700
        // Beyond that a StackOverflow occurs.
        assertEquals("INSERT VALUES change from previous limit", 1700, passCount);
    }

    /**
     * Create a large insert statement with rowCount rows all with
     * constants. Prepare and execute it and then rollback to leave
     * the table unchanged.
     * @param rowCount
     * @return false if rollback succeeds, otherwise true
     * @throws SQLException
     */
    private boolean insertValues(int rowCount) throws SQLException {
        Random r = new Random(3457245435L);

        StringBuffer insertSQL = new StringBuffer(
                "INSERT INTO T0(SI,I,BI,R,F,D,N5_2,DEC10_3,CH20,VC,LVC) VALUES\n");

        for (int i = 0; i < rowCount; i++) {
            if (i != 0)
                insertSQL.append(',');

            insertSQL.append('(');

            insertSQL.append(((short) r.nextInt()));
            insertSQL.append(',');
            insertSQL.append(i);
            insertSQL.append(',');
            insertSQL.append(r.nextLong());
            insertSQL.append(',');

            insertSQL.append(r.nextFloat());
            insertSQL.append(',');
            insertSQL.append(r.nextFloat());
            insertSQL.append(',');
            insertSQL.append(r.nextDouble());
            insertSQL.append(',');

            insertSQL.append("462.54");
            insertSQL.append(',');
            insertSQL.append("9324324.34");
            insertSQL.append(',');

            insertSQL.append('\'');
            insertSQL.append("c");
            insertSQL.append(r.nextInt() % 10);
            insertSQL.append('\'');
            insertSQL.append(',');

            insertSQL.append('\'');
            insertSQL.append("vc");
            insertSQL.append(r.nextInt() % 1000000);
            insertSQL.append('\'');
            insertSQL.append(',');

            insertSQL.append('\'');
            insertSQL.append("lvc");
            insertSQL.append(r.nextInt());
            insertSQL.append('\'');

            insertSQL.append(')');

            insertSQL.append('\n');
        }

        try {
            PreparedStatement ps = prepareStatement(insertSQL.toString());
            assertEquals("Incorrect update count", rowCount, ps.executeUpdate());
            ps.close();
            rollback();
            return false;
        } catch (SQLException e) {
            // The top level exception is expected to be
            // the "user-friendly" query is too complex
            // rather than some linkage error.
            assertSQLState("42ZA0", e);
        }

        return true;
    }
}

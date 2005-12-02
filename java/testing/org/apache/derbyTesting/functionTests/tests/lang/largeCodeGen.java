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

	
    public static void main(String argv[]) 
       throws Exception
    {
    	ij.getPropertyArg(argv); 
        Connection con = ij.startJBMS();
        con.setAutoCommit(false);
        testParamsInWhereClause(con);
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
     * Test many parameters in the where clause
     * e.g. 
	 * @param con  
	 */
	private static void testParamsInWhereClause(Connection con)  throws SQLException {
		 createTestTable(con);
		 testWhereParams(con, 200);
		 testWhereParams(con, 400);
	}

	
	/**
	 * Tests numParam parameter markers in a where clause
	 * 
	 * @param con          
	 * @param  numparams  
	 */
	private static void testWhereParams(Connection con, int numParams) throws SQLException {
		PreparedStatement ps  = null;
		String pred = "(si = ? AND i = ? )";
		String testName = "WHERE clause with " + numParams + " parameters";
		StringBuffer sqlBuffer = new StringBuffer("DELETE FROM T0 WHERE " + pred );
		for (int i = 2; i < numParams; i+=2)
		{
			sqlBuffer.append(" OR (si = ? AND i = ? ) ");
		}
		try {
			ps = con.prepareStatement(sqlBuffer.toString());
			System.out.println("PASS: " + testName);
		 
		}catch (Exception e)
		{
			reportFailure(testName, e);
			
		}
	}

	private static void testUnions(Connection con) throws Exception
	{
		Statement stmt = null;
        PreparedStatement pstmt = null; 
        createTestTable(con);
		//int numUnions = 4000;
		//int numUnions = 2000;
		/*
		  We still have problems with large queries. 
		  Passes at 4000.
		  With size 5000 it gets "java.lang.VerifyError: 
		  (class: db2j/exe/ac601a400fx0102xc673xe3e9x000000163ac04, method: 
		  execute signature: ()Lcom/ibm/db2j/protocol/Database/Language/Interface/ResultSet;) Illegal target of jump or branch". My fix affects generated method "fillResultSet". With size 10000 largeCodeGen gets Java exception: 'java.io.IOException: constant_pool(70796 > 65535)'.
		*/
		
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
		String createViewString = createView.toString();
		//System.out.println(createViewString);
		stmt.executeUpdate(createView.toString());
		
		
		// 2000 unions caused method too big error in verifier
		largeUnionSelect(con, viewName, 2000);

		// 10000 unions overflows the number of constant pool entries
		largeUnionSelect(con, viewName, 10000);

    }
    
    private static void largeUnionSelect(Connection con, String viewName,
    		int numUnions) throws Exception
	{

    	// There are 100 unions in each view so round to the nearest 100
    	String testName = "SELECT with " + numUnions/100 * 100 + " unions";
		StringBuffer selectSQLBuffer  = new StringBuffer("select * from t0 ") ;
		for (int i = 1; i < numUnions/100;i++)
		{
			selectSQLBuffer.append(" UNION ALL (SELECT * FROM " + viewName + ")");
		}	
		
		try {
		// Ready to execute the problematic query 
		String selectSQL = selectSQLBuffer.toString();
		//System.out.println(selectSQL);
        PreparedStatement pstmt = con.prepareStatement(selectSQL);
        ResultSet rs = pstmt.executeQuery();
		int numRowsExpected = (numUnions/100 * 100);
		int numRows = 0;
		while (rs.next())
		{
			numRows++;
			if ((numRows % 100) == 0)
			checkRowData(rs);
		}
		System.out.println("PASS: " + testName + " Row data check ok");
        con.commit();
        pstmt.close();
     
		} catch (SQLException sqle)
		{
			reportFailure(testName, sqle);
			
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
		System.out.print("FAILED QUERY: " + testName +". ");
		if (e instanceof SQLException)
		{
			SQLException se = (SQLException) e;
			while (se != null)
			{
				se.printStackTrace(System.out);
				se = se.getNextException();
			}
		}	
		else e.printStackTrace();
	
	}
	
}

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.*;

import java.util.Properties;
import org.apache.derby.tools.ij;


// This test tries to push byte code generation to the limit.
// the variable numUnions can be changed to push up the byte code generated
// It has to be run with a large amount of memory. if numUnions is set high e.g.
//  java -Djvmflags=-Xmx512M org.apache.derbyTesting.harness.RunTest lang/largeCodeGen
// This is but one code path other areas need to be tested such as large in clauses, etc.

public class largeCodeGen
{

    public static void main(String argv[]) 
       throws Exception
    {
        Statement stmt = null;
        PreparedStatement pstmt = null; 
		//int numUnions = 4000;
		//int numUnions = 2000;
		/*
		  We still have problems with large queries. 
		  Passes at 4000.
		  With size 5000 it gets "java.lang.VerifyError: 
		  (class: db2j/exe/ac601a400fx0102xc673xe3e9x000000163ac04, method: 
		  execute signature: ()Lcom/ibm/db2j/protocol/Database/Language/Interface/ResultSet;) Illegal target of jump or branch". My fix affects generated method "fillResultSet". With size 10000 largeCodeGen gets Java exception: 'java.io.IOException: constant_pool(70796 > 65535)'.
		*/

		String tableName = "t0";		
		String viewName = "v0";		

		ij.getPropertyArg(argv); 
	        Connection con = ij.startJBMS();        

		con.setAutoCommit(false);
		stmt = con.createStatement();
 	       System.out.println("connected");

		// Create table
		try {
			stmt.executeUpdate("drop table " + tableName);
		}catch (SQLException se)
		{
			// drop error ok.
		}
		try {
			stmt.executeUpdate("drop view " + viewName);
		}catch (SQLException se)
		{
			// drop error ok.
		}

		String createSQL = 	"create table " +
			tableName +
			"(si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(3),vc varchar(20), lvc long varchar)";
		stmt.executeUpdate(createSQL);
		stmt.executeUpdate("insert into " + tableName + " values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one')");
		
		System.out.println("Building view 100 unions");  
		StringBuffer createView = new StringBuffer("create view " + viewName + 
												   " as select * from " + 
												   tableName);
		for (int i = 1; i < 100; i ++)
		{
			createView.append(" UNION ALL (SELECT * FROM " + tableName + ")");
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

		StringBuffer selectSQLBuffer  = new StringBuffer("select * from t0 ") ;
		for (int i = 1; i < numUnions/100;i++)
		{
			selectSQLBuffer.append(" UNION ALL (SELECT * FROM " + viewName + ")");
		}	
		
		try {
		// Ready to execute the problematic query 
		String selectSQL = selectSQLBuffer.toString();
		//System.out.println(selectSQL);
		System.out.println("SELECT with " + numUnions/100 * 100 + " unions");
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
		System.out.println("PASS: Row data check ok");
        con.commit();
        pstmt.close();
        con.close();
		} catch (SQLException sqle)
		{
			System.out.println("FAILED QUERY");
			do {
				System.out.println(sqle.getSQLState() + ":" + sqle.getMessage());
				sqle = sqle.getNextException();
			} while (sqle != null);
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

}



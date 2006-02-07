/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.prepStmt

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.io.ByteArrayInputStream; 
import java.io.InputStreamReader;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
	This test tests the JDBC PreparedStatement.
*/

public class prepStmt
{
	private static Connection conn = null;

    private static String[] testObjects =  // string array for cleaning up
        {"table t1", "table tab1", "table t2", "table bigtab", "table tstab",
         "table doubletab", "table numtab", "table Numeric_Tab", "table jira614", 
	 "table jira125", 
         "table jira125125125125125125125125125125125125125125125125125125125125125125125125125125125125125125125"};

	public static void main (String args[])
	{
		try
		{
			System.out.println("prepStmt Test Starts");
			ij.getPropertyArg(args); 
			conn = ij.startJBMS();

			if (conn == null)
			{
				System.out.println("conn didn't work");
				return;
			}
	
			Statement cleanstmt = conn.createStatement();
			TestUtil.cleanUpTest(cleanstmt, testObjects);

			PreparedStatement ps;
			ResultSet rs;
			boolean hasResultSet;
			int uc;

			// executeUpdate() without parameters
			System.out.println("executeUpdate() without parameters");
			ps = conn.prepareStatement("create table t1(c1 int, c2 int, c3 int)");
			uc = ps.executeUpdate();
			System.out.println("Update count is: " + uc);

			// executeUpdate() with parameters
			System.out.println("executeUpdate() with parameters");
			ps = conn.prepareStatement("insert into t1 values (?, 5, ?)");
			ps.setInt(1, 99);
			ps.setInt(2, 9);
			uc = ps.executeUpdate();
			System.out.println("Update count is: " + uc);

			// execute() with parameters, no result set returned
			System.out.println("execute() with parameters, no result set returned");
			ps = conn.prepareStatement("insert into t1 values (2, 6, ?), (?, 5, 8)");
			ps.setInt(1, 10);
			ps.setInt(2, 7);
			hasResultSet = ps.execute();
			while (hasResultSet)
			{
				rs = ps.getResultSet();
				while (rs.next())
					System.out.println("ERROR: should not get here!");
				hasResultSet = ps.getMoreResults();
			}
			uc = ps.getUpdateCount();
			if (uc != -1)
				System.out.println("Update count is: " + uc);

			// executeQuery() without parameters
			System.out.println("executQuery() without parameters");
			ps = conn.prepareStatement("select * from t1");
			rs = ps.executeQuery();
			while (rs.next())
				System.out.println("got row: "+" "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getInt(3));
			System.out.println("end of rows");

			// executeQuery() with parameters
			System.out.println("executQuery() with parameters");
			ps = conn.prepareStatement("select * from t1 where c2 = ?");
			ps.setInt(1, 5);
			rs = ps.executeQuery();
			while (rs.next())
				System.out.println("got row: "+" "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getInt(3));
			System.out.println("end of rows");

			// execute() with parameters, with result set returned
			System.out.println("execute() with parameters with result set returned");
			ps = conn.prepareStatement("select * from t1 where c2 = ?");
			ps.setInt(1, 5);
			hasResultSet = ps.execute();
			while (hasResultSet)
			{
				rs = ps.getResultSet();
				while (rs.next())
					System.out.println("got row: "+" "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getInt(3));
				hasResultSet = ps.getMoreResults();
			}
			System.out.println("end of rows");
			uc = ps.getUpdateCount();
			if (uc != -1)
				System.out.println("Update count is: " + uc);

			// test different data types for input parameters of a PreparedStatement
			System.out.println("test different data types for input parameters of a Prepared Statement");
			ps = conn.prepareStatement("create table t2(si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(20),vc varchar(20), lvc long varchar,b20 char(23) for bit data, vb varchar(23) for bit data, lvb long varchar for bit data,  dt date, tm time, ts timestamp not null)");
			uc = ps.executeUpdate();
			System.out.println("Update count is: " + uc);

			// byte array for binary values.
			byte[] ba = new byte[] {0x00,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,0xb,0xc,
						 0xd,0xe,0xf,0x10,0x11,0x12,0x13 };

			ps = conn.prepareStatement("insert into t2 values (?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? , ?)");
			ps.setShort(1, (short) 1);
			ps.setInt(2, 2);
			ps.setLong(3, 3);
			ps.setFloat(4, (float) 4.0);
			ps.setDouble(5, 5.0);
			ps.setDouble(6, 6.0);
			ps.setBigDecimal(7, new BigDecimal("77.77"));
			ps.setBigDecimal(8, new BigDecimal("8.1"));
			ps.setString(9, "column9string");
			byte[] c10ba = new String("column10vcstring").getBytes();
			int len = c10ba.length;
			ps.setAsciiStream(10, new ByteArrayInputStream(c10ba), len);
			byte[] c11ba = new String("column11lvcstring").getBytes();
			len = c11ba.length;
			ps.setCharacterStream(11, new InputStreamReader(new ByteArrayInputStream(c11ba)),len);
			ps.setBytes(12,ba);
			// Calling setBytes on the varchar for bit data type because it 
			// Appears DB2 UDB accepts this only for the BLOB data type...
			// ps.setBinaryStream(13, new ByteArrayInputStream(ba), ba.length);
			ps.setBytes(13,ba);
			ps.setBytes(14,ba);
			ps.setDate(15, Date.valueOf("2002-04-12"));
			ps.setTime(16, Time.valueOf("11:44:30"));
			ps.setTimestamp(17, Timestamp.valueOf("2002-04-12 11:44:30.000000000"));
			uc = ps.executeUpdate();
			System.out.println("Update count is: " + uc);

			// test setObject on different datatypes of the input parameters of
			// PreparedStatement
			System.out.println("test setObject on different data types for input  parameters of a Prepared Statement");
			ps = conn.prepareStatement("insert into t2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? , ?)");
			ps.setObject(1, new Integer(1));
			ps.setObject(2, new Integer(2));
			ps.setObject(3, new Long(3));
			ps.setObject(4, new Float(4.0));
			ps.setObject(5, new Double(5.0));
			ps.setObject(6, new Double(6.0));
			ps.setObject(7, new BigDecimal("77.77"));
			ps.setObject(8, new BigDecimal("8.1"));
			ps.setObject(9, "column11string");
			ps.setObject(10, "column10vcstring");
			ps.setObject(11, "column11lvcstring");
			ps.setObject(12,ba);
			ps.setObject(13,ba);
			ps.setObject(14,ba);
			ps.setObject(15, Date.valueOf("2002-04-12"));
			ps.setObject(16, Time.valueOf("11:44:30"));
			ps.setObject(17, Timestamp.valueOf("2002-04-12 11:44:30.000000000"));
			uc = ps.executeUpdate();
			System.out.println("Update count is: " + uc);

			// test setNull on different datatypes of the input parameters of PreparedStatement
			System.out.println("test setNull on different data types for input  parameters of a Prepared Statement");
			ps = conn.prepareStatement("insert into t2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? , ?)");
			ps.setNull(1, java.sql.Types.SMALLINT);
			ps.setNull(2, java.sql.Types.INTEGER);
			ps.setNull(3, java.sql.Types.BIGINT);
			ps.setNull(4, java.sql.Types.REAL);
			ps.setNull(5, java.sql.Types.FLOAT);
			ps.setNull(6, java.sql.Types.DOUBLE);
			ps.setNull(7, java.sql.Types.NUMERIC);
			ps.setNull(8, java.sql.Types.DECIMAL);
			ps.setNull(9, java.sql.Types.CHAR);
			ps.setNull(10, java.sql.Types.VARCHAR);
			ps.setNull(11, java.sql.Types.LONGVARCHAR);
			ps.setNull(12, java.sql.Types.BINARY);
			ps.setNull(13, java.sql.Types.VARBINARY);
			ps.setNull(14, java.sql.Types.LONGVARBINARY);
			ps.setNull(15, java.sql.Types.DATE);
			ps.setNull(16, java.sql.Types.TIME);
		   
			ps.setTimestamp(17, Timestamp.valueOf("2002-04-12 11:44:31.000000000")); //slightly after
			hasResultSet = ps.execute();
			uc = ps.getUpdateCount();
			if (uc != -1)
				System.out.println("Update count is: " + uc);

			ps = conn.prepareStatement("select * from t2");
			rs = ps.executeQuery();
			while (rs.next())
			{
				System.out.println("got row: "+" "+rs.getShort(1)+
								   " "+rs.getInt(2)+" "+rs.getLong(3)+
								   " "+rs.getFloat(4)+" "+rs.getDouble(5)+
								   " "+rs.getDouble(6)+" "+rs.getBigDecimal(7)+
								   " "+rs.getBigDecimal(8)+" "+rs.getString(9)+
								   " "+rs.getString(10)+" "+rs.getString(11)+
								   " "+bytesToString(rs.getBytes(12)) +
								   " "+bytesToString(rs.getBytes(13)) +
								   " "+bytesToString(rs.getBytes(14)) +
								   " "+rs.getDate(15)+
								   " "+rs.getTime(16)+" "+rs.getTimestamp(17));
				Timestamp ts = rs.getTimestamp(17);
				Timestamp temp = Timestamp.valueOf("2002-04-12 11:44:30.000000000");
				if (ts.after(temp))
					System.out.println("After first Timestamp!");
				else if (ts.before(temp))
					System.out.println("Before first Timestamp!");
				else
					System.out.println("Timestamp match!");
			}
			System.out.println("end of rows");

			try {
				ps = conn.prepareStatement("select * from t2 where i = ?");
				rs = ps.executeQuery();
			}
			catch (SQLException e) {
				System.out.println("SQLState: " + e.getSQLState() + " message: " + e.getMessage());
			}
			try {
				ps = conn.prepareStatement("insert into t2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				ps.executeUpdate();
			}
			catch (SQLException e) {
				System.out.println("SQLState: " + e.getSQLState() + " message: " + e.getMessage());
			}
			try {
				int tabSize = 1000;
				String createBigTabSql = "create table bigtab (";
				for (int i = 1; i <= tabSize; i++)
				{
					createBigTabSql += "c"+ i + " int";
					if (i != tabSize) 
						createBigTabSql += ", ";
					else 
						createBigTabSql += " )";
				}
				//System.out.println(createBigTabSql);
				ps = conn.prepareStatement(createBigTabSql);
				uc = ps.executeUpdate();
				
				insertTab(conn, "bigtab",50);
				insertTab(conn, "bigtab",200);
				insertTab(conn, "bigtab", 300);
				insertTab(conn, "bigtab",500);
				// prepared Statement with many  params (bug 4863)
				insertTab(conn, "bigtab", 1000);
				selectFromBigTab(conn);
				// Negative Cases
				System.out.println("Insert wrong column name");
				insertTab(conn, "bigtab", 1001);
				// this one will give a sytax error
				System.out.println("Expected Syntax error ");
				insertTab(conn, "bigtab", 0);
				// table doesn't exist
				System.out.println("Expected Table does not exist ");
				insertTab(conn, "wrongtab",1000);
			}
			catch (SQLException e) {
				System.out.println("SQLState: " + e.getSQLState() + 
								   " message: " + e.getMessage());
			}
			rs.close();
			ps.close();

			testBigDecimalSetObject(conn);
			testBigDecimalSetObjectWithScale(conn);

			test4975(conn);
			test5130(conn);
			test5172(conn);
			jira614Test(conn);
			jira170Test(conn);
			jira125Test(conn);
			conn.close();
			// refresh conn before cleaning up
			conn = ij.startJBMS();
			cleanstmt = conn.createStatement();
			TestUtil.cleanUpTest(cleanstmt, testObjects);
			cleanstmt.close();
			conn.close();
			System.out.println("prepStmt Test Ends");
        }
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	// Test creation and execution of many Prepared Statements
	// Beetle 5130
	private static void test5130 (Connection conn) throws Exception
	{
		int numOfPreparedStatement = 500;
		
		PreparedStatement[] tempPreparedStatement = new
			PreparedStatement[numOfPreparedStatement];
		ResultSet rs;
		String[] tableName = new  String[numOfPreparedStatement];  
		for (int i = 0; i < numOfPreparedStatement; i++) 
		{
             tempPreparedStatement[i] = conn.prepareStatement(
			"SELECT COUNT(*) from SYS.SYSTABLES",
			ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			 rs = tempPreparedStatement[i].executeQuery();
			 rs.close();
		}
		for (int i = 0; i < numOfPreparedStatement; i++) 
			tempPreparedStatement[i].close();
		
	}
	
	private static void test5172(Connection conn) throws Exception
	{
		
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("drop table tab1");
		}
		catch (SQLException se)
		{
	}
		
		stmt.executeUpdate( "CREATE TABLE TSTAB (I int, STATUS_TS  Timestamp, PROPERTY_TS Timestamp)" );
		stmt.executeUpdate("INSERT INTO TSTAB VALUES(1 , '2003-08-15 21:20:00','2003-08-15 21:20:00')");
		stmt.executeUpdate("INSERT INTO TSTAB VALUES(2 , '1969-12-31 16:00:00.0', '2003-08-15 21:20:00')");
		
		stmt.close();
		
		String timestamp = "20";
		String query =  "select STATUS_TS  " +
			"from   TSTAB " +
			"where  (STATUS_TS >= ? or " +
			"               PROPERTY_TS<?)";

		System.out.println("Negative test setString with Invalid Timestamp:" + timestamp);

		PreparedStatement ps = conn.prepareStatement(query);
		ps.setString(1,timestamp);
		ps.setString(2, timestamp );
		try {
			ResultSet rs = ps.executeQuery();
		}
		catch (SQLException e) {
			System.out.println("SQLState: " + e.getSQLState() + " message: " + e.getMessage());
		}

	}


	private static void test4975(Connection conn) throws Exception
	{
		BigDecimal minBigDecimalVal = null;
		BigDecimal rBigDecimalVal = null;
		String sminBigDecimalVal = null;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Statement stmt = null;

		try
		{
			stmt = conn.createStatement();
			String createTableSQL = "create table Numeric_Tab (MAX_VAL NUMERIC(30,15), MIN_VAL NUMERIC(30,15), NULL_VAL NUMERIC(30,15) DEFAULT NULL)";
			// to create the Numeric Table
			stmt.executeUpdate(createTableSQL);
			
			String insertSQL = "insert into Numeric_Tab values(999999999999999,0.000000000000001, null)";
			stmt.executeUpdate(insertSQL);

			//to extract the Maximum Value of BigDecimal to be Updated 
			sminBigDecimalVal = "0.000000000000001";
			minBigDecimalVal = new BigDecimal(sminBigDecimalVal);
			logMsg("Minimum BigDecimal Value: " + minBigDecimalVal);

			// to update Null value column with Minimum value 
			String sPrepStmt = "update Numeric_Tab set NULL_VAL=?";

			// Uncomment and prepare the below statement instead to see JCC bug on setObject for decimal
			//String sPrepStmt = "update Numeric_Tab set NULL_VAL="+sminBigDecimalVal+" where 0.0 != ?";
			logMsg("Prepared Statement String: " + sPrepStmt);
			
			// get the PreparedStatement object
			pstmt = conn.prepareStatement(sPrepStmt);
			pstmt.setObject(1,minBigDecimalVal);
			pstmt.executeUpdate();

			//to query from the database to check the call of pstmt.executeUpdate
			//to get the query string
			String Null_Val_Query = "Select NULL_VAL from Numeric_Tab";
			logMsg(Null_Val_Query);
			rs = stmt.executeQuery(Null_Val_Query);
			rs.next();

			rBigDecimalVal = (BigDecimal) rs.getObject(1);
			logMsg("Returned BigDecimal Value after Updation: " + rBigDecimalVal);
			logMsg("Value returned from stmt: " + minBigDecimalVal);

			if(rBigDecimalVal.compareTo(minBigDecimalVal) == 0)
			{
				logMsg("setObject Method sets the designated parameter with the Object");
			}
			else
			{
				logErr("setObject Method does not set the designated parameter with the Object");
				throw new Exception("Call to setObject Method is Failed!");
			}
		}
		catch(SQLException sqle)
		{
			logErr("SQL Exception: " + sqle.getMessage());
			throw new Exception("Call to setObject is Failed!");
		}
		catch(Exception e)
		{
			logErr("Unexpected Exception: " + e.getMessage());
			throw new Exception("Call to setObject is Failed!");
		}

		finally
		{
			try
			{
				if(rs != null)
				{
					 rs.close();
					 rs = null;
				}
				if(pstmt != null)
				{
					 pstmt.close();
					 pstmt = null;
				}
				stmt.executeUpdate("drop table Numeric_Tab");
				if(stmt != null)
				{
					 stmt.close();
					 stmt = null;
				}
			}
			catch(Exception e){ }
		}
	}

	private static void logErr(String s)
	{
		System.err.println(s);
	}

	private static void logMsg(String s)
	{
		System.out.println(s);
	}

	private static void insertTab(Connection conn, String tabname , int numCols) throws SQLException
	{
		PreparedStatement ps = null;
		System.out.println("insertTab ( " + tabname + ","  + numCols + ")" );
		String insertSql = "insert into " + tabname + "(";
 		for (int i = 1; i <= numCols; i++)
		{
			insertSql += " c"+ i;
			if (i != numCols)
				insertSql += ", ";
			else 
				insertSql += ")";
		}
		insertSql += "  values (";
		for (int i = 1; i <= numCols; i++)
		{
			insertSql += "?";
			if (i != numCols)
				insertSql += ", ";
			else 
				insertSql += " )";
		}

		try {
			ps = conn.prepareStatement(insertSql);
			
			for (int i = 1; i <= numCols; i++)
				ps.setInt(i,i);
			ps.executeUpdate();
		} catch (SQLException e)
		{
			System.out.println("SQLState: " + e.getSQLState() + 
							   " message: " + e.getMessage());			
		}
		
	}

	private static void selectFromBigTab(Connection conn) throws SQLException
	{
		PreparedStatement ps = null;
		ResultSet rs = null;

		String selectSQL = "select * from bigtab";
		System.out.println(selectSQL);
		ps = conn.prepareStatement(selectSQL);
		rs = ps.executeQuery();
		while (rs.next())
		{
			System.out.println("Col # 500 = " + rs.getObject(500) +
					   "  Col 1000 = " + rs.getObject(1000));  
		}
		
		rs.close();
		ps.close();
   
	}
	private static void testBigDecimalSetObject(Connection conn) throws SQLException
	{
		setupDoubleTab(conn);
		testBigDecimalToDoubleConversion(conn);
	}



	private static void setupDoubleTab(Connection conn) throws SQLException
	{
		String sql;
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate("DROP TABLE doubletab");
		}
		catch (SQLException se)
		{
			//System.out.println("Table doubletab not dropped. " + se.getMessage());

		}

		sql = "CREATE TABLE doubletab (i int, doubleVal DOUBLE)";

		System.out.println(sql);
		stmt.executeUpdate(sql);
		conn.commit();
		
	}

	private static void testBigDecimalToDoubleConversion(Connection conn) throws SQLException
	{
		System.out.println("\n\ntestBigDecimalToDoubleConversion().");
		System.out.println(" Check that values are preserved when BigDecimal \n values which have more than 31 digits are converted \n to Double with setObject");		
		ResultSet rs = null;
		// Insert various double values
		double[] doubleVals = {1.0E-130,1.0E125, 0, -1.0E124};
		//BigDecimal[] bigDecimalVals = new BigDecimal[doubleVals.length];
		BigDecimal[] bigDecimalVals = { new BigDecimal(1.0E-130), 
										new BigDecimal(1.0E125),
										new BigDecimal(-1.0E124) ,
										new
											BigDecimal("12345678901234567890123456789012"),
										new BigDecimal("1.2345678901234567890123456789012")
		};

		String isql = "INSERT INTO doubletab VALUES (?, ?)";
		//System.out.println("conn.prepareStatement(" + isql +")");
		PreparedStatement insPs = conn.prepareStatement(isql);	  
		String ssql = "SELECT doubleVal FROM doubletab";
		PreparedStatement selPs = conn.prepareStatement(ssql);
		String dsql = "DELETE FROM doubletab";
		PreparedStatement delPs = conn.prepareStatement(dsql);
		for (int i = 0; i < bigDecimalVals.length; i ++)
		{
			BigDecimal bd = bigDecimalVals[i];
			// Insert value
			//System.out.println("ps.setObject(1," + bd + ",java.sql.Types.DOUBLE)");						
			insPs.setInt(1,i);
			insPs.setObject(2,bd,java.sql.Types.DOUBLE);
			insPs.executeUpdate();
			// Check Value
			rs = selPs.executeQuery();
			rs.next();
			checkDoubleMatch(bd.doubleValue() , rs.getDouble(1));
			// Clear out the table;
			delPs.executeUpdate();
		}
		insPs.close();
		selPs.close();
		delPs.close();
		rs.close();
		conn.commit();
	}

	static void testBigDecimalSetObjectWithScale(Connection conn) throws Exception
	{
		Statement stmt = conn.createStatement();
		String sql = null;

	System.out.println("\n\ntestBigDecimalSetObjectWithScale(). \nPass scale parameter of setObject");		

		try {
			stmt.executeUpdate("DROP TABLE numtab");
		}
		catch (SQLException se)
		{
			//System.out.println("Table numtab not dropped. " + se.getMessage());
		}
		sql = "CREATE TABLE numtab (num NUMERIC(10,6))";
		stmt.executeUpdate(sql);
		
		// make a big decimal from string
		BigDecimal bdFromString = new BigDecimal("2.33333333");
		
		// prepare a statement which updates the third column of the table with
		// the DOUBLE columns
		sql =  "INSERT INTO  numtab  VALUES(?)";
		PreparedStatement ps =  conn.prepareStatement(sql);
		// setObject using the big decimal value
		//System.out.println("ps.setObject(1," + bdFromString +		* ",java.sql.Types.DECIMAL,2)");
		int scale = 2;
		ps.setObject(1,bdFromString,java.sql.Types.DECIMAL,scale);
		ps.executeUpdate();
		// check the value
		sql = "SELECT num FROM numtab";
		ResultSet rs = stmt.executeQuery(sql);
		rs.next();
		// Check that the correct scale was set
		checkBigDecimalMatch(bdFromString.setScale(scale,
												   BigDecimal.ROUND_DOWN),
							 (BigDecimal)rs.getObject(1));
		rs.close();
		ps.close();
		stmt.close();

		conn.commit();
 	}

	private static void checkDoubleMatch(double expectedValue, double
										 actualValue) 	{
		if (actualValue == expectedValue)
			System.out.println("PASS: Actual value " + actualValue + " matches expected value: " + expectedValue);
		else
			new Exception("FAIL: Actual value: " + actualValue +
							" does not match expected value:" + 
						  expectedValue).printStackTrace();
		
	}

	private static void checkBigDecimalMatch(BigDecimal expectedValue,
											 BigDecimal actualValue) 	{
		if (actualValue == expectedValue || 
			(actualValue.compareTo(expectedValue) == 0))
			System.out.println("PASS: Actual value " + actualValue + " matches expected value: " + expectedValue);
		else
			new Exception("FAIL: Actual value: " + actualValue +
							" does not match expected value:" + 
						  expectedValue).printStackTrace();
		
	}

	private static String bytesToString(byte[] ba)
	{
		String s = null;
		if (ba == null)
			return s;
		s = new String();
		for (int i = 0; i < ba.length; i++)
			s += (Integer.toHexString(ba[i] & 0x00ff));
		return s;
	}

	
	// Derby bug 614 has to do with how the server responds when the
	// client closes the statement in between split QRYDTA blocks. We
	// have to cause a split QRYDTA block, which we can do by having a
	// bunch of moderately-sized rows which mostly fill a 32K block
	// followed by a single giant row which overflows the block. Then,
	// we fetch some of the rows, then close the result set.
    private static void jira614Test(Connection conn)
	    throws Exception
    {
	    Statement stmt = conn.createStatement();
            PreparedStatement ps ;
	    try {
		    stmt.execute("drop table jira614");
	    } catch (Throwable t) { }
	    ps = conn.prepareStatement(
			    "create table jira614 (c1 varchar(10000))");
	    ps.executeUpdate();
	    String workString = genString("a", 150);
	    ps = conn.prepareStatement("insert into jira614 values (?)");
	    ps.setString(1, workString);
	    for (int row = 0; row < 210; row++)
		    ps.executeUpdate();
	    workString = genString("b", 10000);
	    ps.setString(1, workString);
	    ps.executeUpdate();
	    ps = conn.prepareStatement("select * from jira614");
            ResultSet rs = ps.executeQuery();

            int rowNum = 0;
            while (rs.next())
            {
                rowNum++;
                if (rowNum == 26)
                    break;
            }
            rs.close(); // This statement actually triggers the bug.
	    System.out.println("Test jira614 completed successfully -- no Distributed Protocol Exception occurred");
    }
    private static String genString(String c, int howMany)
    {
	    StringBuffer buf = new StringBuffer();
	    for (int i = 0; i < howMany; i++)
		    buf.append(c);
	    return buf.toString();
    }
    // Jira-170 has to do with how the server handles re-synchronization of
    // the data stream when an enormous parameter value follows a failed
    // prepare statement. Note that it is deliberate here that we are preparing
    // a statement referring to a non-existing table.
    private static void jira170Test(Connection conn)
        throws Exception
    {
        Statement stmt = conn.createStatement();
        PreparedStatement ps = null ;
	    try {
		    stmt.execute("drop table jira170");
	    } catch (Throwable t) { }
        // Create a huge array of chars to be used as the input parameter
        char []cData = new char[1000000];
        for (int i = 0; i < cData.length; i++)
            cData[i] = Character.forDigit(i%10, 10);
        // The behavior of this test program depends on how the JDBC driver
        // handles statement prepares. The DB2 Universal JDBC driver implements
        // something called "deferred prepares" by default. This means that it
        // doesn't do the prepare of the statement until the statement is
        // actually executed. Other drivers, such as the standard Derby client
        // driver, do the prepare at the time of the prepare. This means that,
        // depending on which driver we're using and what the driver's
        // configuration is, we'll get the "table not found" error either on
        // the prepare or on the execute. It doesn't really matter for the
        // purposes of the test, because the whole point is that we *dont*
        // get a DRDA Protocol Exception, but rather a table-not-found
        // exception.
        try {
            ps = conn.prepareStatement("insert into jira170 values (?)");
            ps.setString(1, new String(cData));
            ps.execute();
            System.out.println("Test Jira170 failed: no exception when trying to execute a failed prepare with an enormous parameter");
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equals("42X05"))
                System.out.println("Jira170: caught expected table not found");
            else
                e.printStackTrace();
        }
    }
	/**
	 * Jira-125 has to do with proper use of continuation headers 
	 * for very large reply messages, such as the SQLDARD which is
	 * returned for a prepared statement with an enormous number of
	 * parameter markers. This test generates a multi-segment SQLDARD
	 * response message from the server, to verify that the code in
	 * DDMWriter.finalizeDSSLength is executed.
	 *
	 * Repro for DERBY-125 off-by-one error.  This repro runs in
	 * two iterations.  The first iteration, we use a table name
	 * and a column name that are extra long, so that the server-
	 * side buffer has more data in it.  The second iteration, we
	 * use simpler names for the table and column, which take up
	 * less space in the server buffer.  Then, since the server-
	 * side bytes array was previously used for a larger amount of
	 * data, then the unused bytes contain old data.  Since we
	 * intentionally put the "larger amount of data" into the buffer
	 * during the first iteration, we know what the old data bytes
	 * are going to be.  Thus, by using specific lengths for the 
	 * table and column names, we can 'shift' the old data until we
	 * reach a point where the off-by-one error manifests itself:
	 * namely, we end up incorrectly leaving a non-zero data byte
	 * in the last position of the current server buffer, which
	 * is wrong.
	 */

    private static void jira125Test(Connection conn)
        throws Exception
    {
		jira125Test_a(conn);
		jira125Test_b(conn);
    }

    private static void jira125Test_b(Connection conn)
	    throws Exception
    {
	    Statement stmt = conn.createStatement();
        PreparedStatement ps ;
	    try {
		    stmt.execute("drop table jira125");
	    } catch (Throwable t) { }
		try {
	        stmt.execute("create table jira125 (id integer)");
			stmt.execute("insert into jira125 values 1, 2, 3");
		} catch (Throwable t) { }
        StringBuffer buf = new StringBuffer();
        buf.append("SELECT id FROM jira125 WHERE id IN ( ");

		// Must have at least 551 columns here, in order to force
		// server buffer beyond 32k.  NOTE: Changing this number
		// could cause the test to "pass" even if a regression
		// occurs--so only change it if needed!
        int nCols = 556;
        for (int i = 0; i < nCols; i++) buf.append("?,");
        buf.append("?)");
        ps = conn.prepareStatement(buf.toString());
        // Note that we actually have nCols+1 parameter markers
        for (int i = 0; i <= nCols; i++) ps.setInt(i+1, 1);
        ResultSet rs = ps.executeQuery();
        while (rs.next());
        System.out.println("Test jira125 successful: " + (nCols + 1) +
			" parameter markers successfully prepared and executed.");
    }

    private static void jira125Test_a(Connection conn)
	    throws Exception
    {
	    Statement stmt = conn.createStatement();

		// Build a column name that is 99 characters long;
		// the length of the column name and the length of
		// the table name are important to the repro--so
		// do not change these unless you can confirm that
		// the new values will behave in the same way.
		StringBuffer id = new StringBuffer();
		for (int i = 0; i < 49; i++)
			id.append("id");
		id.append("i");

		// Build a table name that is 97 characters long;
		// the length of the column name and the length of
		// the table name are important to the repro--so
		// do not change these unless you can confirm that
		// the new values will behave in the same way.
		StringBuffer tabName = new StringBuffer("jira");
		for (int i = 0; i < 31; i++)
			tabName.append("125");

	    try {
		    stmt.execute("drop table " + tabName.toString());
	    } catch (Throwable t) { }
		try {
	        stmt.execute("create table " + tabName.toString() + " (" +
				id.toString() + " integer)");
			stmt.execute("insert into " + tabName.toString() + " values 1, 2, 3");
		} catch (Throwable t) { }

        PreparedStatement ps;
        StringBuffer buf = new StringBuffer();
        buf.append("SELECT " + id.toString() + " FROM " +
			tabName.toString() + " WHERE " + id.toString() + " IN ( ");

		// Must have at least 551 columns here, in order to force
		// server buffer beyond 32k.  NOTE: Changing this number
		// could cause the test to "pass" even if a regression
		// occurs--so only change it if needed!
        int nCols = 554;
        for (int i = 0; i < nCols; i++) buf.append("?,");
        buf.append("?)");
        ps = conn.prepareStatement(buf.toString());
        // Note that we actually have nCols+1 parameter markers
        for (int i = 0; i <= nCols; i++) ps.setInt(i+1, 1);
        ResultSet rs = ps.executeQuery();
        while (rs.next());
        System.out.println("Iteration 1 successful: " + (nCols + 1) +
			" parameter markers successfully prepared and executed.");
    }
}

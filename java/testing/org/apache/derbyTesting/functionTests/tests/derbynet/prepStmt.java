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

/**
	This test tests the JDBC PreparedStatement.
*/

class prepStmt
{
	private static Connection conn = null;

	public static void main (String args[])
	{
		try
		{
			System.out.println("prepStmt Test Starts");
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection conn = null;
			// test attribute passing in quoted string, beetle 4789
			String databaseURL = "jdbc:derby:net://localhost/\"wombat2;create=true;upgrade=true\"";
			java.util.Properties properties = new java.util.Properties();
			properties.put ("user", "cs");
			properties.put ("password", "cs");
			properties.put("retrieveMessagesFromServerOnGetMessage", "true");
			conn = DriverManager.getConnection(databaseURL, properties);

			if (conn == null)
			{
				System.out.println("conn didn't work");
				return;
			}
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

			test4975(conn);
			test5130(conn);
			test5172(conn);

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

		conn.rollback();
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
			logMsg("Value returned from ctssql.stmt: " + minBigDecimalVal);

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
}

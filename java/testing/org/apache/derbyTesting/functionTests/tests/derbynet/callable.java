/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.callable

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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.sql.DriverManager;
import java.sql.Connection;

/**
	This test tests the JDBC CallableStatement.
*/

public class callable
{

	public static void main (String args[])
	{
		try
		{
			System.out.println("CallableStatement Test Starts");
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection conn = null;

			// This also tests quoted pathname in database name portion of URL, beetle 4781.
			String databaseURL = "jdbc:derby:net://localhost/\"" + System.getProperty("derby.system.home") + java.io.File.separator + "wombat;create=true\"";
			java.util.Properties properties = new java.util.Properties();
			properties.put ("user", "I");
			properties.put ("password", "mine");

			conn = DriverManager.getConnection(databaseURL, properties);
			if (conn == null)
			{
				System.out.println("conn didn't work");
				return;
			}
			Statement stmt = conn.createStatement();

			// 2 input, 1 output
			stmt.execute("CREATE PROCEDURE method1(IN P1 INT, IN P2 INT, OUT P3 INT) " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method1'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			CallableStatement cs = conn.prepareCall("call method1 (?, ?, ?)");
			cs.setInt(1, 6);
			cs.setInt(2, 9);
			cs.registerOutParameter (3, java.sql.Types.INTEGER);
			cs.execute();
			int sum = cs.getInt(3);
			System.out.println("Sum of 6 and 9 is: " + sum);
			cs.close();
			stmt.execute("DROP PROCEDURE method1");

			// method returns calue, plus 1 input
			stmt.execute("CREATE FUNCTION method2(P1 INT) RETURNS INT" +
					" EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method2'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("? = call method2 (?)");
			cs.registerOutParameter (1, java.sql.Types.INTEGER);
			cs.setInt(2, 6);
			cs.execute();
			int ret = cs.getInt(1);
			System.out.println("return value: Square of 6 then plus 6 is: " + ret);
			cs.close();
			// stmt.execute("DROP FUNCTION method2");

			// no parameter
			stmt.execute("CREATE PROCEDURE method3() " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method3'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("call method3 ()");
			cs.execute();
			cs.close();
			stmt.execute("DROP PROCEDURE method3");

			// only 1 return parameter
			stmt.execute("CREATE FUNCTION method4() RETURNS INT" +
					" EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method4'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("? = call method4()");
			cs.registerOutParameter (1, java.sql.Types.INTEGER);
			cs.execute();
			System.out.println("return value is: " + cs.getInt(1));
			cs.close();
			// stmt.execute("DROP FUNCTION method4");

			// different parameter types, also method overload
			stmt.execute("CREATE PROCEDURE method4P(" +
					"IN P1 SMALLINT, IN P2 INT, IN P3 BIGINT, IN P4 REAL, " +
					"IN P5 DOUBLE, IN P6 DECIMAL(6,3), IN P7 DATE, IN P8 TIME, IN P9 TIMESTAMP, IN P10 VARCHAR(20) FOR BIT DATA, " +
					"OUT O1 SMALLINT, OUT O2 INT, OUT O3 BIGINT, OUT O4 REAL, " +
					"OUT O5 DOUBLE, OUT O6 DECIMAL(6,3), OUT O7 DATE, OUT O8 TIME, OUT O9 TIMESTAMP, OUT O10 VARCHAR(20) FOR BIT DATA" +
					") " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method4'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("call method4P(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			cs.setShort(1, (short) 3);
			cs.setInt(2, 4);
			cs.setLong(3, 5);
			cs.setFloat(4, (float) 6.0);
			cs.setDouble(5, 7.0);
			cs.setBigDecimal(6, new BigDecimal("88.88"));
			cs.setDate(7, Date.valueOf("2002-05-12"));
			cs.setTime(8, Time.valueOf("10:05:02"));
			cs.setTimestamp(9, Timestamp.valueOf("2002-05-12 10:05:02.000000000"));
			byte[] ba = new byte[2];
			ba[0] = 1;
			ba[1] = 2;
			cs.setBytes(10, ba);
			int n = 10;
			cs.registerOutParameter (n+1, java.sql.Types.SMALLINT);
			cs.registerOutParameter (n+2, java.sql.Types.INTEGER);
			cs.registerOutParameter (n+3, java.sql.Types.BIGINT);
			cs.registerOutParameter (n+4, java.sql.Types.REAL);
			cs.registerOutParameter (n+5, java.sql.Types.DOUBLE);
			cs.registerOutParameter (n+6, java.sql.Types.DECIMAL);
			cs.registerOutParameter (n+7, java.sql.Types.DATE);
			cs.registerOutParameter (n+8, java.sql.Types.TIME);
			cs.registerOutParameter (n+9, java.sql.Types.TIMESTAMP);
			cs.registerOutParameter (n+10, java.sql.Types.VARBINARY);
			cs.execute();
			System.out.println("return short: " + cs.getShort(n+1));
			System.out.println("return int: " + cs.getInt(n+2));
			System.out.println("return long: " + cs.getLong(n+3));
			System.out.println("return float: " + cs.getFloat(n+4));
			System.out.println("return double: " + cs.getDouble(n+5));
			System.out.println("return decimal: " + cs.getBigDecimal(n+6));
			System.out.println("return date: " + cs.getDate(n+7));
			System.out.println("return time: " + cs.getTime(n+8));
			System.out.println("return time stamp: " + cs.getTimestamp(n+9));
			ba = cs.getBytes(n+10);
			for (int i = 0; i < ba.length; i++)
				System.out.println("return byte["+i+"]: " + ba[i]);
			stmt.execute("DROP PROCEDURE method4P");

			// some tests on BigDecimal
			stmt.execute("CREATE PROCEDURE method5(" +
					"IN P1 DECIMAL(14,4), OUT P2 DECIMAL(14,4), IN P3 DECIMAL(14,4), OUT P4 DECIMAL(14,4), " +
					"OUT P5 DECIMAL(14,4), OUT P6 DECIMAL(14,4), OUT P7 DECIMAL(14,4), OUT P8 DECIMAL(14,4), OUT P9 DECIMAL(14,4) " +
					") " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method5'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("call method5 (?, ?, ?, ?, ?, ?, ?, ?, ?)");
			cs.setBigDecimal(1, new BigDecimal("33.333"));
			cs.registerOutParameter (2, java.sql.Types.DECIMAL);
			cs.setBigDecimal(3, new BigDecimal("-999.999999"));
			cs.registerOutParameter (4, java.sql.Types.DECIMAL);
			cs.registerOutParameter (5, java.sql.Types.DECIMAL);
			cs.registerOutParameter (6, java.sql.Types.DECIMAL);
			cs.registerOutParameter (7, java.sql.Types.DECIMAL);
			cs.registerOutParameter (8, java.sql.Types.DECIMAL);
			cs.registerOutParameter (9, java.sql.Types.DECIMAL);
			cs.execute();
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(2));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(4));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(5));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(6));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(7));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(8));
			System.out.println("method 5 return decimal: " + cs.getBigDecimal(9));
			cs.close();
			stmt.execute("DROP PROCEDURE method5");

			// INOUT param tests
			stmt.execute("CREATE PROCEDURE method6(" +
					"IN P1 INT, INOUT P2 INT, IN P3 SMALLINT, INOUT P4 SMALLINT, " +
					"IN P5 BIGINT, INOUT P6 BIGINT, IN P7 REAL, INOUT P8 REAL, IN P9 DOUBLE, INOUT P10 DOUBLE, " +
					"IN P11 TIME, INOUT P12 TIME " +
					") " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.method6'" +
					" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
			cs = conn.prepareCall("call method6 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?)");
			cs.registerOutParameter (2, java.sql.Types.INTEGER);
			cs.registerOutParameter (4, java.sql.Types.SMALLINT);
			cs.registerOutParameter (6, java.sql.Types.BIGINT);
			cs.registerOutParameter (8, java.sql.Types.REAL);
			cs.registerOutParameter (10, java.sql.Types.DOUBLE);
			cs.registerOutParameter (12, java.sql.Types.TIME);
			cs.setInt(1, 6);
			cs.setInt(2, 9);
			cs.setShort(3, (short)6);
			cs.setShort(4, (short)9);
			cs.setLong(5, (long)99999);
			cs.setLong(6, (long)88888888);
			cs.setFloat(7, (float)6.123453);
			cs.setFloat(8, (float)77777);
			cs.setDouble(9, (double)6.123453);
			cs.setDouble(10, (double)8888888888888.01234);
			cs.setTime(11, Time.valueOf("11:06:03"));
			cs.setTime(12, Time.valueOf("10:05:02"));

			cs.execute();
			System.out.println("Integer: Sum of 6 and 9 is: " + cs.getInt(2));
			System.out.println("Short: Sum of 6 and 9 is: " + cs.getShort(4));
			System.out.println("Long: Sum of 99999 and 88888888 is: " + cs.getLong(6));
			System.out.println("Float: Sum of 6.123453 and 77777 is: " + cs.getFloat(8));
			System.out.println("Double: Sum of 6.987654 and 8888888888888.01234 is: " + cs.getDouble(10));
			System.out.println("Time: Old time of 10:05:02 changed to: " + cs.getTime(12));
			cs.close();
			stmt.execute("DROP PROCEDURE method6");

			testBigDec(conn);

			testLongBinary(conn);
			// Temporarily take out testbatch until jcc bug is fixed (5827)
			// testBatch(conn);

			System.out.println("CallableStatement Test Ends");
        }
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	static void testLongBinary(Connection conn)
	{
	  try {
		String createTabSql = "create table Longvarbinary_Tab (lvbc Long varchar for bit data)";
		PreparedStatement ps = conn.prepareStatement(createTabSql);
		int updcount = ps.executeUpdate();
		String insertTabSql = "insert into Longvarbinary_Tab values( X'010305')";
		ps = conn.prepareStatement(insertTabSql);
		updcount = ps.executeUpdate();

		int bytearrsize = 50;
		byte[] bytearr=new byte[bytearrsize];
		String sbyteval=null;

		// to get the bytearray value
		for (int count=0;count<bytearrsize;count++)
		{
			sbyteval=Integer.toString(count%255);
			bytearr[count]=Byte.parseByte(sbyteval);
		}

		System.out.println("get the CallableStatement object");

		String createproc = "create procedure Longvarbinary_In(P1 VARCHAR(10000) FOR BIT DATA) MODIFIES SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.Longvarbinary_Proc_In' language java parameter style java";
		ps = conn.prepareStatement(createproc);
		updcount = ps.executeUpdate();

		CallableStatement cstmt = conn.prepareCall("{call Longvarbinary_In(?)}");
		cstmt.setObject(1,bytearr,java.sql.Types.LONGVARBINARY);
		System.out.println("execute the procedure with LONGVARBINARY");
		cstmt.executeUpdate();
		cstmt.setObject(1,bytearr,java.sql.Types.BLOB);
		System.out.println("execute the procedure with BLOB");
		cstmt.executeUpdate();

		Statement stmt = conn.createStatement();
		String Longvarbinary_Query="Select lvbc from Longvarbinary_Tab";
		System.out.println(Longvarbinary_Query);
		ResultSet rs=stmt.executeQuery(Longvarbinary_Query);

		while (rs.next())
		{
			byte[] retvalue = (byte[]) rs.getObject(1);

			for(int i=0;i<bytearrsize;i++)
			{
				if (retvalue[i]!=bytearr[i])
				{
					System.out.println("Test Failed.  setObject did not set the parameter value correctly");
				}
			}
		}
		rs.close();
		ps.close();
		stmt.close();
		cstmt.close();
	  }catch (SQLException e) {
		  System.out.println(e.getMessage());
		  e.printStackTrace();
	  }

	  System.out.println("done testing long varbinary");
	}

	static void testBatch(Connection conn)
	{
	  try {
		conn.setAutoCommit(true);
		int i=0;
		int retValue[]={0,0,0};
		int updCountLength=0;
		Statement stmt = conn.createStatement();
		PreparedStatement ps = null;
		int updcount;
		
		try {
			ps=conn.prepareStatement("drop table tab1");
			updcount=ps.executeUpdate();
			ps=conn.prepareStatement("drop table tab2");
			updcount=ps.executeUpdate();
			ps=conn.prepareStatement("drop procedure UpdTable_Proc");
			updcount=ps.executeUpdate();
		} 
		catch (SQLException e) {}

		String createtable = "create table tab1 (tab1pk int, vcc1 varchar(32), primary key(tab1pk))";
		System.out.println("doing: " + createtable);
		stmt.execute(createtable);

		String inserttable = "insert into tab1 values(2, 'STRING_2')";
		System.out.println("doing: " + inserttable);
		stmt.addBatch(inserttable);
		inserttable = "insert into tab1 values(3, 'STRING_3')";
		System.out.println("doing: " + inserttable);
		stmt.addBatch(inserttable);
		inserttable = "insert into tab1 values(5, 'STRING_5')";
		System.out.println("doing: " + inserttable);
		stmt.addBatch(inserttable);
		inserttable = "select * from tab1";
		System.out.println("adding: " + inserttable);
		stmt.addBatch(inserttable);

		int[] updateCount=null;
		try {
			updateCount = stmt.executeBatch();
		} catch(SQLException se) {
 			do {
				System.out.println("Exception chain: "+se.getMessage());
				se = se.getNextException();
			} while (se != null);
		}

		ResultSet rs = stmt.executeQuery("select * from tab1");
		while (rs.next())
		{
			System.out.println("  id: "+rs.getInt(1)+" desc: "+rs.getString(2));
		}
	
		createtable = "create table tab2 (tab2pk int, vcc2 varchar(32), fc float, tab1pk int, primary key(tab2pk), foreign key(tab1pk) references tab1)" ;
		System.out.println("doing: " + createtable);
		stmt.execute(createtable);
		
		inserttable="insert into tab2 values(1, 'STRING-1', 1.0 , 5)";
		System.out.println("doing: " + inserttable);
		stmt.execute(inserttable);
		inserttable="insert into tab2 values(2, 'STRING-2', 2.0 , 2)";
		System.out.println("doing: " + inserttable);
		stmt.execute(inserttable);
		inserttable="insert into tab2 values(3, 'STRING-3', 3.0 , 5)";
		System.out.println("doing: " + inserttable);
		stmt.execute(inserttable);
		inserttable="insert into tab2 values(9, 'STRING-9', 9.0 , 3)";
		System.out.println("doing: " + inserttable);
		stmt.execute(inserttable);

		System.out.println("setup done");

		String createproc = "create procedure UpdTable_Proc(P1 INT) MODIFIES SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.UpdTable_Proc' langauge java parameter style java";
		System.out.println("doing: " + createproc);
		stmt.execute(createproc);
		
		System.out.println("call the proc/get the callable statement");
		CallableStatement cstmt= conn.prepareCall("{call UpdTable_Proc(?)}");
		System.out.println("set first int");
		cstmt.setInt(1,2);
		System.out.println("add first to batch");
		cstmt.addBatch();

		System.out.println("set second int");
		cstmt.setInt(1,3);
		System.out.println("add second to batch");
		cstmt.addBatch();

		System.out.println("set third int");
		cstmt.setInt(1,4);
		System.out.println("add third to batch");
		cstmt.addBatch();

		try {
			System.out.println("execute the executeBatch method");
			updateCount = cstmt.executeBatch();
			updCountLength = updateCount.length;
		} catch (SQLException e) {
			System.out.println("EXPECTED Exception: ");
			System.out.println(e.getMessage());
		}

		rs = stmt.executeQuery("select * from tab2");
		while (rs.next())
		{
			System.out.println("  type id: "+rs.getInt(4)+" new float column value: "+rs.getInt(3));
		}

		System.out.println("prepare the proc");
		String prepString= "update tab2 set tab2pk=?, vcc2=? where vcc2=?";
		PreparedStatement pstmt = conn.prepareStatement(prepString);
		int batchUpdates[]={0,0,0};
		int buCountlen=0;

		try {

			System.out.println("set first values");
			pstmt.setInt(1,1);
			pstmt.setString(2, "Continue-1");
			pstmt.setString(3, "STRING-1");
			System.out.println("add first to batch");
			pstmt.addBatch();
	
			System.out.println("set second values - illegal update - forces unique constr violation");
			pstmt.setInt(1,1);
			pstmt.setString(2,"Invalid");
			pstmt.setString(3,"STRING-3");
			System.out.println("add second to batch");
			pstmt.addBatch();

			System.out.println("set third values; legal update again");
			pstmt.setInt(1,2);
			pstmt.setString(2,"Continue-2");
			pstmt.setString(3,"STRING-2");
			System.out.println("add third to batch");
			pstmt.addBatch();

			System.out.println("execute the executeBatch method");
			System.out.println("expecting batchupdateexception");
			updateCount = pstmt.executeBatch();

		}
		catch(BatchUpdateException b)
		{
			System.out.println("b: " + b.getMessage());
			System.out.println("Caught expected BatchUpdateException");
			batchUpdates = b.getUpdateCounts();
			buCountlen = batchUpdates.length;
			System.out.println("buclen: " + buCountlen);
		}
		catch(SQLException sqle)
		{
			System.out.println("Call to continueUpdate failed!" + sqle);
		}
		catch ( Exception e ) 
		{ 
			System.out.println("Call to continueUpdate failed!" + e);
		}
	
		if (buCountlen == 1) 
		{
			System.out.println("Driver does not support continued updates - OK");
			for (i = 0; i < buCountlen; i++)
				System.out.println("=== update count: "+batchUpdates[i]);
			return;
		}
		else if (buCountlen == 3) 
		{
			System.out.println("Driver supports continued updates.");
			for (i = 0; i < buCountlen; i++)
				System.out.println("=== update count: "+batchUpdates[i]);
			// Check to see if the third row from the batch was added
			try
			{
				String query ="Select count(*) from tab2 where vcc2 in ('Continue-2')";
				System.out.println("Query is: " + query);
				rs=stmt.executeQuery(query);
				System.out.println("executed, now next...");
				rs.next();
				System.out.println("next, now getInt...");
				int count = rs.getInt(1);
				rs.close();
				stmt.close();
				System.out.println("Count val is: " + count);

				// Make sure that we have the correct error code for
				// the failed update.

				if (! (batchUpdates[1] == -3 && count == 1) )
				{
					System.out.println("Driver did not insert after error.");
				}
				System.out.println("now after errorcode check");
					
			}
			catch (SQLException sqle)
			{
				System.out.println("Call to continueUpdate failed!" + sqle);
				sqle.printStackTrace();
					
			}
		}

		System.out.println("Done testing executeBatch.");

		rs.close();
		cstmt.close();
		stmt.close();

	  }catch (SQLException e) {
		  System.out.println(e.getMessage());
		  e.printStackTrace();
	  }
	}

	public static void method1 (int p1, int p2, int[] p3)
	{
		p3[0] = p1 + p2;
	}

	public static int method2 (int p1)
	{
		return (p1 * p1) + p1;
	}

	public static void method3()
	{
		System.out.println("I'm doing something here...");
	}

	public static int method4()
	{
		return 55;
	}

	public static void method4(short s, int i, long l, float f,
							double d, BigDecimal bd, Date dt, Time t, Timestamp ts, byte[] ba,
							short[] sr, int[] ir,
							long[] lr, float[] fr, double[] dr, BigDecimal[] bdr,
							Date[] dtr, Time[] tr, Timestamp[] tsr, byte[][] bar)
	{
		sr[0] = s;
		ir[0] = i;
		lr[0] = l;
		fr[0] = f;
		dr[0] = d;
		bdr[0] = bd;
		dtr[0] = dt;
		tr[0] = t;
		if (ts.equals(Timestamp.valueOf("2002-05-12 10:05:02.000000000")))
		{
			System.out.println("got the right Timestamp");
			tsr[0] = ts;
		}
		else
		{
			System.out.println("got the wrong Timestamp");
			tsr[0] = null;
		}
		bar[0] = ba;
	}

	public static void method5(BigDecimal bd1, BigDecimal bdr1[], BigDecimal bd2, BigDecimal bdr2[],
								BigDecimal bdr3[], BigDecimal bdr4[], BigDecimal bdr5[], BigDecimal bdr6[],
								BigDecimal bdr7[])
	{
		bdr1[0] = bd1;
		bdr2[0] = bd1.multiply(bd2);
		bdr3[0] = bd1.add(bd2);
		bdr4[0] = new BigDecimal(".00000");
		bdr5[0] = new BigDecimal("-.00000");
		bdr6[0] = new BigDecimal("99999999.");
		bdr7[0] = new BigDecimal("-99999999.");
	}

	// Test for INOUT params.
	public static void method6 (int p1, int p2[], short s1, short s2[], long l1, long l2[],
			float f1, float f2[], double d1, double d2[], Time t1, Time t2[])
	{
		p2[0] = p1 + p2[0];
		s2[0] = (short) (s1 + s2[0]);
		l2[0] = l1 + l2[0];
		f2[0] = f1 + f2[0];
		d2[0] = d1 + d2[0];
		t2[0] = t1;
	}

	// Test for IN parameters with Longvarbinary column
	public static void Longvarbinary_Proc_In (byte[] in_param) throws SQLException 
	{

		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("update Longvarbinary_Tab set lvbc=?");

		ps.setBytes(1,in_param);
		ps.executeUpdate();

		ps.close();ps=null;
		conn.close();conn=null;
	}


	// test update of table in batch
	public static void UpdTable_Proc (BigDecimal type_param) throws SQLException 
	{
        	Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("update t2 set fc=fc*20 where tab1pk=?");
		
		ps.setBigDecimal(1,type_param);
		ps.executeUpdate();

		ps.close(); ps=null;
		conn.close(); conn=null;
	}

	// test accessing minumum and maximum and null value for numeric columns with out params
	public static void Numeric_Proc (BigDecimal[] param1,BigDecimal[] param2,
					 BigDecimal[] param3) throws SQLException 
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select maxcol, mincol, nulcol from Num_Tab");
		
		if (rs.next()) 
		{
			param1[0]=rs.getBigDecimal(1);
			param2[0]=rs.getBigDecimal(2);
			param3[0]=rs.getBigDecimal(3);
		}
		else 
		{
	    		throw new SQLException("Data not found");
		}
		
		rs.close();rs=null;
		stmt.close();stmt=null;
		conn.close();conn=null;
	}

	// Beetle 4933.  
	static void testBigDec(Connection conn) throws Exception
	{
		PreparedStatement ps = null;
		int updcount;
		
	  
	  try {
		  try {
				  ps = conn.prepareStatement("drop table Num_Tab");
				  updcount = ps.executeUpdate();
		  } 
		  catch (SQLException e) {}
		  int tabSize = 10;
		  String createTabSql = "create table Num_Tab (maxcol NUMERIC(31,15), mincol NUMERIC(15,15), nulcol NUMERIC)";

		  ps = conn.prepareStatement(createTabSql);
		  updcount = ps.executeUpdate();
		  String insertTabSql = "insert into Num_Tab values(999999999999999,0.000000000000001, null)";
		  ps = conn.prepareStatement(insertTabSql);
		  updcount = ps.executeUpdate();
		  try {
		  String alias = "create procedure Numeric_Proc(OUT P1 DECIMAL(31,15), OUT P2 DECIMAL(31,15), OUT P3 DECIMAL(31,15)) READS SQL DATA external name 'org.apache.derbyTesting.functionTests.tests.derbynet.callable.Numeric_Proc' language java parameter style java";
		  ps = conn.prepareStatement(alias);
		  updcount = ps.executeUpdate();
		  } catch (SQLException se) {}
			  

		  CallableStatement cstmt = conn.prepareCall("{call Numeric_Proc(?,?,?)}");
		  cstmt.registerOutParameter(1,java.sql.Types.NUMERIC,15);
		  cstmt.registerOutParameter(2,java.sql.Types.NUMERIC,15);
		  cstmt.registerOutParameter(3,java.sql.Types.NUMERIC,15);
		  //		  ParameterMetaData pmd = cstmt.getParameterMetaData();
		  //System.out.println("precision: " + pmd.getPrecision(1) +
		  //				 "scale: " + pmd.getScale(1));
		  //execute the procedure
		  cstmt.execute();

		  BigDecimal retVal = cstmt.getBigDecimal(1);
		  BigDecimal retVal2 = cstmt.getBigDecimal(2);
		  BigDecimal retVal3 = cstmt.getBigDecimal(3);

		  System.out.println("cstmt.getBigDecimal(1): " + retVal);
		  System.out.println("cstmt.getBigDecimal(2): " + retVal2);
		  System.out.println("cstmt.getBigDecimal(3): " + retVal3);
		  cstmt.close();
		  ps.close();
		  conn.commit();
		  
		  }catch (SQLException e) { 
			  System.out.println(e.getMessage());
			  e.printStackTrace();
		  }
	  
	}

}

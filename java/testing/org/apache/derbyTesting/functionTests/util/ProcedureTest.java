/*

   Derby - Class org.apache.derbyTesting.functionTests.util.ProcedureTest

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;

import org.apache.derbyTesting.functionTests.util.Formatters;

import java.sql.*;
import java.math.BigDecimal;
/**
	Java procedures for the procedure.sql test.
*/
public abstract class ProcedureTest implements ResultSet {

	public static void zeroArg() {
		System.out.println("zeroArg() called");
	}

	public static void insertRow(int p1) throws SQLException {
		insertRow(p1, "int");
	}

	public static void insertRow(int p1, String p2) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("insert into t1 values (?, ?)");
		ps.setInt(1, p1);
		ps.setString(2, p2);
		ps.executeUpdate();
		ps.close();
		conn.close();
	}

	public static void maxMemPerTabTest() throws SQLException {
/*		StringBuffer sbA = new StringBuffer(20000);
		for (int i = 0; i < 20000; i++)
			sbA.append('a');
		String largeStringA20000 = new String(sbA);
    largeStringA20000.substring(0,2000);

		StringBuffer sbB = new StringBuffer(20000);
		for (int i = 0; i < 20000; i++)
			sbB.append('b');
		String largeStringB20000 = new String(sbB);
		String largeStringB2000 = largeStringB20000.substring(0,2000);

		StringBuffer sbC = new StringBuffer(20000);
		for (int i = 0; i < 20000; i++)
			sbC.append('b');
		String largeStringC20000 = new String(sbC);
		String largeStringC2000 = largeStringC20000.substring(0,2000);

		StringBuffer sbD = new StringBuffer(20000);
		for (int i = 0; i < 20000; i++)
			sbD.append('b');
		String largeStringD20000 = new String(sbD);
		String largeStringD2000 = largeStringD20000.substring(0,2000); */

		String largeStringA20000 = new String(Formatters.repeatChar("a",20000));
		String largeStringA2000 = new String(Formatters.repeatChar("a",2000));
		String largeStringB20000 = new String(Formatters.repeatChar("b",20000));
		String largeStringB2000 = new String(Formatters.repeatChar("b",2000));
		String largeStringC20000 = new String(Formatters.repeatChar("c",20000));
		String largeStringC2000 = new String(Formatters.repeatChar("c",2000));
		String largeStringD20000 = new String(Formatters.repeatChar("d",20000));
		String largeStringD2000 = new String(Formatters.repeatChar("d",2000));

		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("insert into tab1 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA20000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringB20000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringC20000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab2 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA20000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringC20000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringD20000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab3 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA2000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringB2000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringC2000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab4 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA2000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringC2000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringD2000);
		ps.executeUpdate();
		ps.close();
		conn.close();
	}

	private static void insertInBig(Connection conn, String A, String B, String C, String D) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("insert into big values (?, ?, ?, ?)");
		ps.setString(1, A);
		ps.setString(2, B);
		ps.setString(3, C);
		ps.setString(4, D);
		ps.executeUpdate();
		ps.close();
	}

	public static void bigTestData(int i) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		switch (i)
		{
			case 1:
				String largeStringA10000 = new String(Formatters.repeatChar("a",10000));
				String largeStringB10000 = new String(Formatters.repeatChar("b",10000));
				String largeStringC10000 = new String(Formatters.repeatChar("c",10000));
				String largeStringD10000 = new String(Formatters.repeatChar("d",10000));
				insertInBig(conn, largeStringA10000, largeStringB10000, largeStringC10000, largeStringD10000);
				break;
			case 2:
				largeStringA10000 = new String(Formatters.repeatChar("e",10000));
				largeStringB10000 = new String(Formatters.repeatChar("f",10000));
				largeStringC10000 = new String(Formatters.repeatChar("g",10000));
				largeStringD10000 = new String(Formatters.repeatChar("h",10000));
				insertInBig(conn, largeStringA10000, largeStringB10000, largeStringC10000, largeStringD10000);
				break;
			case 3:
				largeStringA10000 = new String(Formatters.repeatChar("i",10000));
				largeStringB10000 = new String(Formatters.repeatChar("j",10000));
				largeStringC10000 = new String(Formatters.repeatChar("k",10000));
				largeStringD10000 = new String(Formatters.repeatChar("l",10000));
				insertInBig(conn, largeStringA10000, largeStringB10000, largeStringC10000, largeStringD10000);
				break;
			case 4:
				largeStringA10000 = new String(Formatters.repeatChar("m",10000));
				largeStringB10000 = new String(Formatters.repeatChar("n",10000));
				largeStringC10000 = new String(Formatters.repeatChar("o",10000));
				largeStringD10000 = new String(Formatters.repeatChar("p",10000));
				insertInBig(conn, largeStringA10000, largeStringB10000, largeStringC10000, largeStringD10000);
				break;
			case 5:
				String largeStringA30000 = new String(Formatters.repeatChar("a",30000));
				String largeStringB2752 = new String(Formatters.repeatChar("b",2752));
				PreparedStatement ps = conn.prepareStatement("insert into big values (?, ?)");
				ps.setString(1, largeStringA30000);
				ps.setString(2, largeStringB2752);
				ps.executeUpdate();
				ps.close();
				break;
			case 6:
				largeStringA30000 = new String(Formatters.repeatChar("a",30000));
				String largeStringB2750 = new String(Formatters.repeatChar("b",2750));
				ps = conn.prepareStatement("insert into big values (?, ?)");
				ps.setString(1, largeStringA30000);
				ps.setString(2, largeStringB2750);
				ps.executeUpdate();
				ps.close();
				break;
			case 7:
				String largeStringA40000 = new String(Formatters.repeatChar("a",40000));
				ps = conn.prepareStatement("insert into big values (?)");
				ps.setString(1, largeStringA40000);
				ps.executeUpdate();
				ps.close();
				break;
			case 8:
				largeStringA40000 = new String(Formatters.repeatChar("a",40000));
				String largeStringB40000 = new String(Formatters.repeatChar("b",40000));
				String largeStringC40000 = new String(Formatters.repeatChar("c",40000));
				ps = conn.prepareStatement("insert into big values (?, ?, ?)");
				ps.setString(1, largeStringA40000);
				ps.setString(2, largeStringB40000);
				ps.setString(3, largeStringC40000);
				ps.executeUpdate();
				largeStringA40000 = new String(Formatters.repeatChar("d",40000));
				largeStringB40000 = new String(Formatters.repeatChar("e",40000));
				largeStringC40000 = new String(Formatters.repeatChar("f",40000));
				ps.setString(1, largeStringA40000);
				ps.setString(2, largeStringB40000);
				ps.setString(3, largeStringC40000);
				ps.executeUpdate();
				ps.close();
				break;
		}
		conn.close();
	}

	//public static void selectRows_coll(int p1, java.util.Collection rs) throws SQLException {

	//	ResultSet[] d1 = new ResultSet[1];
	//	selectRows(p1, d1);
	//	rs.add(d1[0]);
	//}

	public static void selectRows(int p1, ResultSet[] data) throws SQLException {

		System.out.println("selectRows - 1 arg - 1 rs");

		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("select * from t1 where i = ?");
		ps.setInt(1, p1);
		data[0] = ps.executeQuery();
		conn.close();
	}
	public static void selectRows(int p1, int p2, ResultSet[] data1, ResultSet[] data2) throws SQLException {

		System.out.println("selectRows - 2 arg - 2 rs");

		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("select * from t1 where i = ?");
		ps.setInt(1, p1);
		data1[0] = ps.executeQuery();

		ps = conn.prepareStatement("select * from t1 where i >= ?");
		ps.setInt(1, p2);
		data2[0] = ps.executeQuery();

		if (p2 == 99)
			data2[0].close();

		// return no results
		if (p2 == 199) {
			data1[0] = null;
			data2[0] = null;
		}

		// swap results
		if (p2 == 299) {
			ResultSet rs = data1[0];
			data1[0] = data2[0];
			data2[0] = rs;
		}

		conn.close();
	}

	public static void fivejp(ResultSet[] data1, ResultSet[] data2, ResultSet[] data3, ResultSet[] data4, ResultSet[] data5) throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		PreparedStatement ps1 = conn.prepareStatement("select * from MRS.FIVERS where i > ?");
		ps1.setInt(1, 1);
		data1[0] = ps1.executeQuery();

		PreparedStatement ps2 = conn.prepareStatement("select * from MRS.FIVERS  where i > ?");
		ps2.setInt(1, 2);
		data2[0] = ps2.executeQuery();

		PreparedStatement ps3 = conn.prepareStatement("select * from MRS.FIVERS  where i > ?");
		ps3.setInt(1, 3);
		data3[0] = ps3.executeQuery();

		PreparedStatement ps4 = conn.prepareStatement("select * from MRS.FIVERS  where i > ?");
		ps4.setInt(1, 4);
		data4[0] = ps4.executeQuery();

		PreparedStatement ps5 = conn.prepareStatement("select * from MRS.FIVERS  where i > ?");
		ps5.setInt(1, 5);
		data5[0] = ps5.executeQuery();

		conn.close();
	}

	public static void parameter1(int a, String b, String c, java.sql.ResultSet[] rs) throws SQLException {

		System.out.print("PT1 a=" + a);
		if (b == null)
			System.out.println(" b = null");
		else
			System.out.print(" b=<"+b+">("+b.length()+")");
		if (c == null)
			System.out.println(" c = null");
		else
			System.out.print(" c=<"+c+">("+c.length()+")");

		System.out.println("");



		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("insert into PT1 values (?, ?, ?)");
		ps.setInt(1, a);
		ps.setString(2, b);
		ps.setString(3, c);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("select a,b, length(b), c, length(c) from PT1 where a = ?");
		ps.setInt(1, a);
		rs[0] = ps.executeQuery();
		conn.close();
	}

	public static void parameter2(int a, java.math.BigDecimal b, java.math.BigDecimal c, java.sql.ResultSet[] rs) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("insert into PT1 values (?, ?, ?)");
		ps.setInt(1, a);
		ps.setString(2, b.toString());
		ps.setString(3, c.toString());
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("select a,b,c from PT1 where a = ?");
		ps.setInt(1, a);
		rs[0] = ps.executeQuery();
		conn.close();
	}


	public static void outparams1(int[] p1, int p2) {

		p1[0] = p2 * 2;
	}

	// Test for CLOB being returned.
	public static void clobselect (ResultSet[] results, 
								   ResultSet[] results1,
								   ResultSet[] results2
								   )
		throws Exception
	{

        Connection conn = DriverManager.getConnection(
			"jdbc:default:connection");
		PreparedStatement st = conn.prepareStatement("select * from lobCheckOne");
		results[0] = st.executeQuery();
		// Just some regular data
		PreparedStatement st1 = conn.prepareStatement("select count(*) from lobCheckOne");
		results1[0] = st1.executeQuery();
		// Now more Clobs
		PreparedStatement st2 = conn.prepareStatement("select * from lobCheckOne");
		results2[0] = st2.executeQuery();
		conn.close();
		return;

	}

	// Test for BLOB being returned.
	public static void blobselect (ResultSet[] results)
		throws Exception
	{

        Connection conn = DriverManager.getConnection(
			"jdbc:default:connection");
		PreparedStatement st = conn.prepareStatement("select * from lobCheckTwo");
		results[0] = st.executeQuery();
		conn.close();
		return;

	}

	public static void inoutparams2(int[] p1, int p2) {

		p1[0] = p1[0] + (p2 * 2);
	}
	public static void inoutparams3(String[] p1, int p2) {

		if (p1[0] == null)
			System.out.println("p1 is NULL");
		else
			System.out.println("p1= >" + p1[0] + "< length " + p1[0].length());
		
		if (p2 == 8)
			p1[0] = "nad";
		else if (p2 == 9)
			p1[0] = null;
		else if (p2 == 10)
			p1[0] = "abcdefghijklmnopqrstuvwzyz";
	}
	public static void inoutparams4(java.math.BigDecimal[] p1, String p2) {
		if (p2 == null)
			p1[0] = null;
		else {
			if (p1[0] == null)
				p1[0] = new BigDecimal(p2).add(new BigDecimal("17"));
			else
				p1[0] = new BigDecimal(p2).add(p1[0]);
		}
	}

	public static void ambigious1(int p1, String p2, ResultSet[] data1, ResultSet[] data2) {}
	public static void ambigious1(int p1, String p2, ResultSet[] data1) {}


	public static void ambigious2(int p1, Integer p2) {};
	public static void ambigious2(Integer p1, int p2) {};

	public static void missingDynamicParameter(int p1)  {}
	public static void missingDynamicParameter(int p1, Object p2)  {}

	public static void badDynamicParameter(int p1, ProcedureTest[] data) {}

	public static void zeroArgDynamicResult(ResultSet[] data1, ResultSet[] data2, ResultSet[] data3, ResultSet[] data4) {
		System.out.println("zeroArgDynamicResult called");
	}


	public static void sqlControl(String[] e1, String[] e2, String[] e3, String[] e4, String[] e5, String[] e6, String[] e7)
		throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		Statement s = conn.createStatement();

		executeStatement(s, "CREATE TABLE SQLCONTROL_DDL (I INT)", e1);
		executeStatement(s, "ALTER TABLE SQLC.SQLCONTROL_DML ADD COLUMN B INT DEFAULT NULL", e2);

		executeStatement(s, "INSERT INTO SQLC.SQLCONTROL_DML(I) VALUES (1)", e3);
		executeStatement(s, "UPDATE SQLC.SQLCONTROL_DML SET I = I + 11", e4);
		executeStatement(s, "SELECT * FROM SQLC.SQLCONTROL_DML", e5);
		executeStatement(s, "DELETE FROM SQLC.SQLCONTROL_DML", e6);

		executeStatement(s, "DROP TABLE SQLC.SQLCONTROL_DML", e7);

		conn.close();

	}
	public static void sqlControl2(String[] e1, String[] e2, String[] e3, String[] e4, String[] e5, String[] e6, String[] e7)
		throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		Statement s = conn.createStatement();

		executeStatement(s, "CREATE VIEW SQLCONTROL_VIEW AS SELECT * FROM SQLC.SQLCONTROL_DML", e1);
		executeStatement(s, "DROP VIEW SQLCONTROL_VIEW", e2);

		executeStatement(s, "LOCK TABLE SQLC.SQLCONTROL_DML IN EXCLUSIVE MODE", e3);
		executeStatement(s, "VALUES 1,2,3", e4);
		executeStatement(s, "SET SCHEMA SQLC", e5);
		executeStatement(s, "CREATE SCHEMA SQLC_M", e6);
		executeStatement(s, "DROP SCHEMA SQLC_M RESTRICT", e7);

		conn.close();

	}
	public static void sqlControl3(String[] e1, String[] e2, String[] e3, String[] e4, String[] e5, String[] e6, String[] e7)
		throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		Statement s = conn.createStatement();

		e1[0] = "IBM CS FEATURE";
		e2[0] = "IBM CS FEATURE";

		executeStatement(s, "SET ISOLATION CS", e3);
		executeStatement(s, "SET RUNTIMESTATISTICS OFF", e4);
		executeStatement(s, "SET STATISTICS TIMING OFF", e5);
		executeStatement(s, "VALUES 1", e6);

		executeStatement(s, "VALUES 1", e7);

		conn.close();

	}
	public static void sqlControl4(int sqlc, String[] e1, String[] e2, String[] e3, String[] e4, String[] e5, String[] e6, String[] e7, String[] e8)
		throws SQLException {

		Connection conn = DriverManager.getConnection("jdbc:default:connection");

		Statement s = conn.createStatement();

		String sql = "CALL SQLC.SQLCONTROL2_"+sqlc+" (?, ?, ?, ?, ?, ?, ?) ";

		e1[0] = sql;

		CallableStatement cs1 = conn.prepareCall(sql);
		try {
			for (int rop = 1; rop <= 7; rop++) {
				cs1.registerOutParameter(rop, Types.VARCHAR);
			}
			cs1.execute();

			e2[0] = cs1.getString(1);
			e3[0] = cs1.getString(2);
			e4[0] = cs1.getString(3);
			e5[0] = cs1.getString(4);
			e6[0] = cs1.getString(5);
			e7[0] = cs1.getString(6);
			e8[0] = cs1.getString(7);
		} catch (SQLException sqle) {
			StringBuffer sb = new StringBuffer(128);
			sb.append("STATE");
			do {
				sb.append("-");
				String ss = sqle.getSQLState();
				if (ss == null)
					ss= "?????";
				sb.append(ss);
				sqle = sqle.getNextException();
			} while (sqle != null);
			e2[0] = sb.toString();
		}

		cs1.close();

		conn.close();

	}
	private static void executeStatement(Statement s, String sql, String[] result) {

		StringBuffer sb = new StringBuffer(128);

		int len = sql.length();
		if (len > 15)
			len = 15;

		sb.append(sql.substring(0, len));
		try {
			if (s.execute(sql)) {
				ResultSet rs = s.getResultSet();
				while (rs.next())
					sb.append("- ROW(" + rs.getString(1) + ")");
				rs.close();
			} else {
				sb.append("-UPDATE " + s.getUpdateCount());
			}

			sb.append("-EXECUTE OK");

		} catch (SQLException sqle) {

			
			do {
				sb.append("-");
				String ss = sqle.getSQLState();
				if (ss == null)
					ss= "?????";
				sb.append(ss);
				sqle = sqle.getNextException();
			} while (sqle != null);

			
		}
		result[0] = sb.toString();
	}


	public static void oBOOLEAN(Boolean in, Boolean[] inout, Boolean[] out) throws SQLException {

		if (out[0] != null)
			throw new SQLException ("oBOOLEAN expected out[] to be null");

		out[0] = in;
		if (in == null)
			inout[0] = null;
		else
			inout[0] = new Boolean(inout[0].booleanValue() && in.booleanValue());

	}

	public static void pBOOLEAN(boolean in, boolean[] inout, boolean[] out) throws SQLException {

		if (out[0] != false)
			throw new SQLException ("pBOOLEAN expected out[] to be null");

		out[0] = in;
		inout[0] =inout[0] && in;

	}
	public static void oALLINT(Integer in, Integer[] inout, Integer[] out) throws SQLException {

		if (out[0] != null)
			throw new SQLException ("oALLINT expected out[] to be null");

		out[0] = in;
		if (in == null)
			;//inout[0] = null;
		else if (inout[0] == null)
			inout[0] = new Integer(3 * in.intValue());
		else
			inout[0] = new Integer(inout[0].intValue() + in.intValue());
	}
	public static void pTINYINT(byte in, byte[] inout, byte[] out) throws SQLException {

		out[0] = in;
		inout[0] += in;
	}
	public static void pSMALLINT(short in, short[] inout, short[] out) throws SQLException {

		out[0] = in;
		inout[0] += in;
	}

	/*
	** Procedures for testing literals passed to procedures as IN parameters
	*/

	public static void literalTest(int p1, String[] p2) {
		p2[0] = ">" + Integer.toString(p1) + "<";
	}
	public static void literalTest(long p1, String[] p2) {
		p2[0] = ">" + Long.toString(p1) + "<";
	}
	public static void literalTest(float p1, String[] p2) {
		p2[0] = ">" + Float.toString(p1) + "<";
	}
	public static void literalTest(double p1, String[] p2) {
		p2[0] = ">" + Double.toString(p1) + "<";
	}
	public static void literalTest(BigDecimal p1, String[] p2) {
		String s = p1 == null ? "NULL" : p1.toString();
		p2[0] = ">" + s + "<";
	}
	public static void literalTest(String p1, String[] p2) {
		String s = p1 == null ? "NULL" : p1.toString();
		p2[0] = ">" + s + "<";
	}
	public static void literalTest(java.sql.Date p1, String[] p2) {
		String s = p1 == null ? "NULL" : p1.toString();
		p2[0] = ">" + s + "<";
	}
	public static void literalTest(java.sql.Time p1, String[] p2) {
		String s = p1 == null ? "NULL" : p1.toString();
		p2[0] = ">" + s + "<";
	}
	public static void literalTest(java.sql.Timestamp p1, String[] p2) {

		String s = p1 == null ? "NULL" : p1.toString();
		p2[0] = ">" + s + "<";
	}


	/*
	** Procedures for parameter mapping testing.
	*/

	public static void pmap(short in, short[] inout, short[] out) {

		inout[0] += 6;
		out[0] = 77;
	}
	public static void pmap(int in, int[] inout, int[] out) {
		inout[0] += 9;
		out[0] = 88;

	}
	public static void pmap(long in, long[] inout, long[] out) {
		inout[0] += 8;
		out[0] = 99;
	}
	public static void pmap(float in, float[] inout, float[] out) {
		inout[0] += 9.9f;
		out[0] = 88.8f;
	}
	public static void pmap(double in, double[] inout, double[] out) {
		inout[0] += 3.9;
		out[0] = 66.8;
	}
	public static void pmap(BigDecimal in, BigDecimal[] inout, BigDecimal[] out) {
		inout[0] = inout[0].add(new BigDecimal(2.3));
		out[0] = new BigDecimal(84.1);
	}
	public static void pmap(byte[] in, byte[][] inout, byte[][] out) {

		inout[0][2] = 0x56;
		out[0] = new byte[4];
		out[0][0] = (byte) 0x09;
		out[0][1] = (byte) 0xfe;
		out[0][2] = (byte) 0xed;
		out[0][3] = (byte) 0x02;

	}
	public static void pmap(Date in, Date[] inout, Date[] out) {

		inout[0] = java.sql.Date.valueOf("2004-03-08");
		out[0] = java.sql.Date.valueOf("2005-03-08");

	}
	public static void pmap(Time in, Time[] inout, Time[] out) {
		inout[0] = java.sql.Time.valueOf("19:44:42");
		out[0] = java.sql.Time.valueOf("20:44:42");
	}
	public static void pmap(Timestamp in, Timestamp[] inout, Timestamp[] out) {

		inout[0] = java.sql.Timestamp.valueOf("2004-03-12 21:14:24.938222433");
		out[0] = java.sql.Timestamp.valueOf("2004-04-12 04:25:26.462983731");
	}
	public static void pmap(String in, String[] inout, String[] out) {
		inout[0] = inout[0].trim().concat("P2-PMAP");
		out[0] = "P3-PMAP";
	}


	public static int countRows(String schema, String table) throws SQLException
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + schema + "." + table);
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		s.close();
		conn.close();
		return count;
	}
}


/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.forbitdata

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;
import java.sql.*;

import org.apache.derby.tools.ij;
import java.io.*;
import java.math.BigInteger;
import java.math.BigDecimal;

public class forbitdata
{

	static private boolean isDB2jNet;

	public static void main (String[] argv) throws Throwable
	{
		try {
		
 		ij.getPropertyArg(argv); 
			Connection conn = ij.startJBMS();
			// waiting for meta data
			String framework = System.getProperty("framework");
			if (framework != null && framework.toUpperCase().equals("DB2JNET"))
				isDB2jNet = true;

			runTests( conn);
		} catch (Throwable t) {
			System.out.println("FAIL " + t);
			t.printStackTrace(System.out);
		}
    }

    public static void runTests( Connection conn) throws Throwable
    {
		try {
			testNegative(conn);
			testTypes(conn);
			testValues(conn);
			testCompare(conn);
			testEncodedLengths(conn);

		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
		
	}

	/**
		Negative for bit data tests.
		FBD001,FBD007 negative syntax
		FBD005 maximum char length
		FBD009 maximum varchar length
	*/
	public static void testNegative(Connection conn) throws SQLException {

		System.out.println("START testNegative");

		Statement s = conn.createStatement();

		// 
		statementExceptionExpected(s, "CREATE TABLE FBDFAIL.T001 (C001 CHAR(255) FOR BIT DATA)");
		statementExceptionExpected(s, "CREATE TABLE FBDFAIL.T002 (C002 VARCHAR(32673) FOR BIT DATA)");
		statementExceptionExpected(s, "CREATE TABLE FBDFAIL.T003 (C003 VARCHAR FOR BIT DATA)");
		statementExceptionExpected(s, "CREATE TABLE FBDFAIL.T004 (C004 LONG VARCHAR(100) FOR BIT DATA)");
		
		s.close();
		System.out.println("END testNegative");
	}

	/**
		FBD001,FBD007 - positive syntax 
		FBD004 - CHAR length defaults to one
		FBD037 - create table
		FBD006, FBD011, FBD014 - correct JDBC type

	*/
	public static void testTypes(Connection conn) throws SQLException {

		System.out.println("START testTypes");

		Statement s = conn.createStatement();

		for (int i = 1; i <= 8; i++)
			executeDrop(s, "DROP TABLE FBDOK.T00" + i);

		// FBD037

		executeOK(s, "CREATE TABLE FBDOK.T001 (C001 CHAR FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T002 (C002 CHAR(1) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T003 (C003 CHAR(10) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T004 (C004 CHAR(254) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T005 (C005 VARCHAR(1) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T006 (C006 VARCHAR(100) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T007 (C007 VARCHAR(32672) FOR BIT DATA)");
		executeOK(s, "CREATE TABLE FBDOK.T008 (C008 LONG VARCHAR FOR BIT DATA)");

		ResultSet rs = conn.getMetaData().getColumns(null, "FBDOK", null, null);
		while (rs.next()) {
			// skip 1 catalog
			System.out.print(rs.getString(2) + ",");
			System.out.print(rs.getString(3) + ",");
			System.out.print(rs.getString(4) + ",");
			System.out.print(rs.getString(5) + ",");
			System.out.print(rs.getString(6) + ",");
			System.out.print(rs.getString(7) + ",");
			// skip 8 - unused
			System.out.print(rs.getString(9) + ",");
			System.out.print(rs.getString(10) + ",");
			System.out.print(rs.getString(11) + ",");
			// skip 12 remarks
			System.out.print(rs.getString(13) + ",");
			// skip 14,15 unused
			System.out.print(rs.getString(16) + ",");
			System.out.print(rs.getString(17) + ",");
			System.out.println(rs.getString(18));
		}
		rs.close();

		for (int i = 1; i <= 8; i++) {
			try {
				PreparedStatement ps = conn.prepareStatement("SELECT * FROM FBDOK.T00" + i);
				ResultSetMetaData rsmd = ps.getMetaData();
				System.out.println("TABLE FBDOK.T00" + i);
				System.out.println("  " + rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1));
				ps.close();
			} catch (SQLException sqle) {
				showSQLE(sqle);
			}

		}

		for (int i = 1; i <= 8; i++)
			executeDrop(s, "DROP TABLE FBDOK.T00" + i);

		s.execute("DROP SCHEMA FBDOK RESTRICT");


		System.out.println("DATABASE META DATA.getTypeInfo()");
		DatabaseMetaData dmd = conn.getMetaData();

		rs = dmd.getTypeInfo();
		while (rs.next()) {
			String name = rs.getString(1);
			int jdbcType = rs.getInt(2);
			switch (jdbcType) {
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				break;
			default:
				continue;
			}

			System.out.print(name + "(" + jdbcType + ") ");
			System.out.print("precision " + rs.getInt(3));
			System.out.println("");
		}

		rs.close();

		{
		String sql = "VALUES X'2345d45a2e44'";
		PreparedStatement psv = conn.prepareStatement(sql);
		ResultSetMetaData rsmd = psv.getMetaData();
		System.out.println(sql);
		System.out.println("  " + rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1));
		}

		{
		String sql = "VALUES X''";
		PreparedStatement psv = conn.prepareStatement(sql);
		ResultSetMetaData rsmd = psv.getMetaData();
		System.out.println(sql);
		System.out.println("  " + rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1));
		}


		s.close();
		System.out.println("END testTypes");
	}

	public static void testCast(Connection conn) throws SQLException {
	}

	public static void testValues(Connection conn) throws SQLException {

		System.out.println("START testValues");

		Statement s = conn.createStatement();
		executeDrop(s, "DROP TABLE FBDVAL.T001");
		executeDrop(s, "DROP TABLE FBDVAL.X001");
		s.execute("CREATE TABLE FBDVAL.T001(ID INT NOT NULL PRIMARY KEY, C1 CHAR(10) FOR BIT DATA, C2 VARCHAR(10) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(10))");
		PreparedStatement psI = conn.prepareStatement("INSERT INTO FBDVAL.T001 VALUES(?, ?, ?, ?, ?)");
		PreparedStatement psS = conn.prepareStatement("SELECT C1, C2, C3, C4, ID FROM FBDVAL.T001 WHERE ID >= ? AND ID < ? ORDER BY ID");


		System.out.println("**** NULL");
		insertData(psI, 0, null, 10, true);
		showData(psS, 0, null);

		System.out.println("**** 7 bytes (EMPTY)");
		byte[] empty = new byte[7];
		insertData(psI, 10, empty, 10, true);
		showData(psS, 10, empty);

		// DB2
		//	CHAR			-- FAIL TOO BIG			
		//	VARCHAR			-- FAIL TOO BIG
		//	LONG VARCHAR	-- OK
		//	BLOB			-- FAIL TOO BIG
		System.out.println("**** 15 bytes (EMPTY)");
		byte[] empty2 = new byte[15];
		insertData(psI, 20, empty2, 10, true);
		showData(psS, 20, empty2);

		// DB2 - ALL OK
		System.out.println("**** 4 bytes");
		byte[] four = new byte[4];
		four[0] = (byte) 0x04;
		four[1] = (byte) 0x23;
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFD;

		insertData(psI, 30, four, 10, true);
		showData(psS, 30, four);

		// DB2 - ALL OK
		System.out.println("**** 10 bytes");
		byte[] ten = new byte[10];
		ten[0] = (byte) 0x0B;
		ten[1] = (byte) 0x27;
		ten[2] = (byte) 0xA2;
		ten[3] = (byte) 0xFD;
		ten[4] = (byte) 0x01;
		ten[5] = (byte) 0x6D;
		ten[6] = (byte) 0xE2;
		ten[7] = (byte) 0x35;
		ten[8] = (byte) 0x66;
		ten[9] = (byte) 0x90;

		insertData(psI, 40, ten, 10, true);
		showData(psS, 40, ten);

		// DB2
		//	CHAR			-- FAIL TOO BIG			
		//	VARCHAR			-- FAIL TOO BIG
		//	LONG VARCHAR	-- OK
		//	BLOB			-- FAIL TOO BIG
		System.out.println("**** 15 bytes");
		byte[] l15 = new byte[15];
		l15[0] = (byte) 0xEB;
		l15[1] = (byte) 0xCA;
		l15[2] = (byte) 0xFE;
		l15[3] = (byte) 0xBA;
		l15[4] = (byte) 0xBE;
		l15[5] = (byte) 0xFE;
		l15[6] = (byte) 0xED;
		l15[7] = (byte) 0xFA;
		l15[8] = (byte) 0xCE;
		l15[9] = (byte) 0x24;
		l15[10] = (byte) 0x78;
		l15[11] = (byte) 0x43;
		l15[12] = (byte) 0x92;
		l15[13] = (byte) 0x31;
		l15[14] = (byte) 0x6D;

		insertData(psI, 50, l15, 10, true);
		showData(psS, 50, l15);

		// DB2 UDB LUW no truncation of spaces for VARCHAR FBD, LONG VARCHAR FBD
		System.out.println("**** 4 spaces ");
		byte[] space4 = new byte[4];
		space4[0] = (byte) 0x20;
		space4[1] = (byte) 0x20;
		space4[2] = (byte) 0x20;
		space4[3] = (byte) 0x20;
		insertData(psI, 60, space4, 10, true);
		showData(psS, 60, space4);


		// DB2 UDB LUW no truncation of spaces for VARCHAR FBD, LONG VARCHAR FBD
		System.out.println("**** 6 data with trailing space ");
		byte[] space6 = new byte[6];
		space6[0] = (byte) 0xca;
		space6[1] = (byte) 0xfe;
		space6[2] = (byte) 0x20;
		space6[3] = (byte) 0x20;
		space6[4] = (byte) 0x20;
		space6[5] = (byte) 0x20;
		insertData(psI, 70, space6, 10, true);
		showData(psS, 70, space6);

		// DB2
		//	CHAR			-- FAIL TOO BIG			
		//	VARCHAR			-- FAIL TOO BIG
		//	LONG VARCHAR	-- OK
		//	BLOB			-- FAIL TOO BIG
		System.out.println("**** 12 data with trailing space ");
		byte[] space12 = new byte[12];
		space12[0] = (byte) 0xca;
		space12[1] = (byte) 0xfe;
		space12[2] = (byte) 0x20;
		space12[3] = (byte) 0x20;
		space12[4] = (byte) 0x20;
		space12[5] = (byte) 0x20;
		space12[6] = (byte) 0xca;
		space12[7] = (byte) 0xfe;
		space12[8] = (byte) 0x20;
		space12[9] = (byte) 0x20;
		space12[10] = (byte) 0x20;
		space12[11] = (byte) 0x20;
		insertData(psI, 210, space12, 10, true);
		showData(psS, 210, space12);


		String sql = "INSERT INTO FBDVAL.T001 VALUES(80, X'2020202020', X'2020202020', X'2020202020', null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 80, space4);

		// With a literal the value is truncated into CHAR FBD
		sql = "INSERT INTO FBDVAL.T001 VALUES(90, X'CAFE20202020CAFE20202020', null, null, null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 90, space12);

		sql = "INSERT INTO FBDVAL.T001 VALUES(100, null, X'CAFE20202020CAFE20202020', null, null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 100, space12);

		sql = "INSERT INTO FBDVAL.T001 VALUES(110, null, null, X'CAFE20202020CAFE20202020', null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 110, space12);
/*
		sql = "INSERT INTO FBDVAL.T001 VALUES(150, null, null, null, X'CAFE20202020CAFE20202020')";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 150, space12);
*/
		// insert with non-trailing blank from literal
		// DB2 22001 error.
		sql = "INSERT INTO FBDVAL.T001 VALUES(120, X'CAFE20202020CAFE20202020DD', null, null, null)";
		System.out.println("**** " + sql);
		try {
			s.executeUpdate(sql);
			System.out.println("FAIL - literal too long on CHAR FBD");
		} catch (SQLException sqle) {
			if ("22001".equals(sqle.getSQLState()))
				System.out.println("22001 truncation error");
			else
				showSQLE(sqle);
		}

		sql = "INSERT INTO FBDVAL.T001 VALUES(130, null, X'CAFE20202020CAFE20202020DD', null, null)";
		System.out.println("**** " + sql);
		try {
			s.executeUpdate(sql);
			System.out.println("FAIL - literal too long on VARCHAR FBD");
		} catch (SQLException sqle) {
			if ("22001".equals(sqle.getSQLState()))
				System.out.println("22001 truncation error");
			else
				showSQLE(sqle);
		}

		sql = "INSERT INTO FBDVAL.T001 VALUES(140, null, null, X'CAFE20202020CAFE20202020DD', null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 140, space12);

		s.execute("CREATE TABLE FBDVAL.X001(XID INT NOT NULL PRIMARY KEY, X1 CHAR(12) FOR BIT DATA, C2 VARCHAR(12) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(12))");

		sql = "INSERT INTO FBDVAL.X001 VALUES(200, X'CAFE20202020CAFE20202020', null, null, null)";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);


		sql = "INSERT INTO FBDVAL.T001 SELECT * FROM FBDVAL.X001";
		System.out.println("**** " + sql);
		s.executeUpdate(sql);
		showData(psS, 200, space12);

		System.out.println("END testValues");
	}

	private static void insertData(PreparedStatement psI, int id, byte[] original, int maxLen, boolean streamAsWell) throws SQLException {

		int ol = original == null ? 0: original.length;

		if (original == null || original.length <= maxLen) {
			// simple case.
			psI.setInt(1, id);
			psI.setBytes(2, original);
			psI.setBytes(3, original);
			psI.setBytes(4, original);
			psI.setBytes(5, original);
			psI.executeUpdate();

			if (streamAsWell) {
				psI.setInt(1, id+1);
				psI.setBinaryStream(2, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(3, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(4, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(5, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			}
			return;
		}

		boolean okI1;
		boolean okI2;

		// Insert potentially out of range value one at a time into the table
		System.out.println("  >> CHAR FOR BIT DATA");
		try {
		psI.setInt(1, id);
		psI.setBytes(2, original);
		psI.setBytes(3, null);
		psI.setBytes(4, null);
		psI.setBytes(5, null);
		psI.executeUpdate();
		okI1 = true;
		} catch (SQLException sqle) {
			okI1 = false;
			if ("22001".equals(sqle.getSQLState())) {
				System.out.println("22001 truncation error");
			} else
				showSQLE(sqle);
		}
		if (streamAsWell) {
			try {
			psI.setInt(1, id+1);
			psI.setBinaryStream(2, original == null ? null : new ByteArrayInputStream(original), ol);
			psI.executeUpdate();
			okI2 = true;
			} catch (SQLException sqle) {
				okI2 = false;
				if ("22001".equals(sqle.getSQLState())) {
					System.out.println("22001 truncation error");
				} else
					showSQLE(sqle);
			}

			if (okI1 != okI2)
				System.out.println("FAIL - mismatched failures");
		}

		System.out.println("  >> VARCHAR FOR BIT DATA");
		try {
		psI.setInt(1, id+2);
		psI.setBytes(2, null);
		psI.setBytes(3, original);
		psI.setBytes(4, null);
		psI.setBytes(5, null);
		psI.executeUpdate();
		okI1 = true;

		} catch (SQLException sqle) {
			okI1 = false;
			if ("22001".equals(sqle.getSQLState()))
				System.out.println("22001 truncation error");
			else
				showSQLE(sqle);
		}
		if (streamAsWell) {
			try {
			psI.setInt(1, id+3);
			psI.setBinaryStream(3, original == null ? null : new ByteArrayInputStream(original), ol);
			psI.executeUpdate();
			okI2 = true;

			} catch (SQLException sqle) {
				okI2 = false;
				if ("22001".equals(sqle.getSQLState()))
					System.out.println("22001 truncation error");
				else
					showSQLE(sqle);
			}
			if (okI1 != okI2)
				System.out.println("FAIL - mismatched failures");
		}

		System.out.println("  >> LONG VARCHAR FOR BIT DATA");
		try {
		psI.setInt(1, id+4);
		psI.setBytes(2, null);
		psI.setBytes(3, null);
		psI.setBytes(4, original);
		psI.setBytes(5, null);
		psI.executeUpdate();
		okI1 = true;
		} catch (SQLException sqle) {
			okI1 = false;
			if ("22001".equals(sqle.getSQLState()))
				System.out.println("22001 truncation error");
			else
				showSQLE(sqle);
		}

		if (streamAsWell) {
			try {
			psI.setInt(1, id+5);
			psI.setBinaryStream(4, original == null ? null : new ByteArrayInputStream(original), ol);
			psI.executeUpdate();
			okI2 = true;
			} catch (SQLException sqle) {
				okI2 = false;
				if ("22001".equals(sqle.getSQLState()))
					System.out.println("22001 truncation error");
				else
					showSQLE(sqle);
			}
			if (okI1 != okI2)
				System.out.println("FAIL - mismatched failures");
		}

			System.out.println("  >> BLOB");
		try {
		psI.setInt(1, id+6);
		psI.setBytes(2, null);
		psI.setBytes(3, null);
		psI.setBytes(4, null);
		psI.setBytes(5, original);
		okI1 = true;
		psI.executeUpdate();
		} catch (SQLException sqle) {
			okI1 = false;
			if ("22001".equals(sqle.getSQLState()))
				System.out.println("22001 truncation error");
			else
				showSQLE(sqle);
		}
		if (streamAsWell) {
			try {
			psI.setInt(1, id+7);
			psI.setBinaryStream(5, original == null ? null : new ByteArrayInputStream(original), ol);
			psI.executeUpdate();
			okI2 = true;
			} catch (SQLException sqle) {
				okI2 = false;
				if ("22001".equals(sqle.getSQLState()))
					System.out.println("22001 truncation error");
				else
					showSQLE(sqle);
			}
			if (okI1 != okI2)
				System.out.println("FAIL - mismatched failures");
		}
	}



	
	public static void testCompare(Connection conn) throws SQLException {

		System.out.println("START testCompare");

		Statement s = conn.createStatement();
		executeDrop(s, "DROP TABLE FBDVAL.T001");
		executeDrop(s, "DROP TABLE FBDVAL.T002");
		s.execute("CREATE TABLE FBDVAL.T001(ID INT NOT NULL PRIMARY KEY, C1 CHAR(10) FOR BIT DATA, C2 VARCHAR(10) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(10))");
		s.execute("CREATE TABLE FBDVAL.T002(ID INT NOT NULL PRIMARY KEY, C1 CHAR(10) FOR BIT DATA, C2 VARCHAR(10) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(10))");
		PreparedStatement psI = conn.prepareStatement("INSERT INTO FBDVAL.T001 VALUES(?, ?, ?, ?, ?)");
		PreparedStatement psI2 = conn.prepareStatement("INSERT INTO FBDVAL.T002 VALUES(?, ?, ?, ?, ?)");

		insertData(psI, 0, null, 10, false);
		insertData(psI2, 0, null, 10, false);

		byte[] four = new byte[4];
		four[0] = (byte) 0x04;
		four[1] = (byte) 0x23;
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFD;

		insertData(psI, 30, four, 10, false);
		insertData(psI2, 30, four, 10, false);
		four[2] = (byte) 0xA1;
		insertData(psI, 40, four, 10, false);
		insertData(psI2, 40, four, 10, false);
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFF;
		insertData(psI, 50, four, 10, false);
		insertData(psI2, 50, four, 10, false);

		byte[] four_plus_space = new byte[5];
		four_plus_space[0] = (byte) 0x04;
		four_plus_space[1] = (byte) 0x23;
		four_plus_space[2] = (byte) 0xA2;
		four_plus_space[3] = (byte) 0xFD;
		four_plus_space[4] = (byte) 0x20;
		insertData(psI, 60, four_plus_space, 10, false);
		insertData(psI2, 60, four_plus_space, 10, false);

		byte[] ten = new byte[10];
		ten[0] = (byte) 0x0B;
		ten[1] = (byte) 0x27;
		ten[2] = (byte) 0xA2;
		ten[3] = (byte) 0xFD;
		ten[4] = (byte) 0x01;
		ten[5] = (byte) 0x6D;
		ten[6] = (byte) 0xE2;
		ten[7] = (byte) 0x35;
		ten[8] = (byte) 0x66;
		ten[9] = (byte) 0x90;

		insertData(psI, 70, ten, 10, false);
		insertData(psI2, 70, ten, 10, false);

		String[] COLS = {"C1", "C2", "C3", "C4"};
		String[] OPS = {"=", "<>", "<", "<=", ">", ">="};

		for (int t = 0; t < COLS.length; t++) {
			for (int o = 0; o < COLS.length; o++) {
				for (int a = 0; a < OPS.length; a++) {

					String sql = "SELECT T.ID, T." + COLS[t] + ", O.ID, O." + COLS[o] +
						" FROM FBDVAL.T001 O, FBDVAL.T002 T WHERE T." + COLS[t] + " " + OPS[a] + " O." + COLS[o] + " ORDER BY 1,3";

					System.out.println(sql);
					try {
						PreparedStatement psS = conn.prepareStatement(sql);
						showCompareData(psS);
					} catch (SQLException sqle) {
						if ("42818".equals(sqle.getSQLState()))
							System.out.println("42818 types not comparable " + COLS[t] + " ... " + COLS[o]);
						else
							showSQLE(sqle);
					}
					conn.commit();
				}
			}
		}
		System.out.println("END testCompare");
	}

	/**
		The length of a binary type is encoded when stored, this
		test makes sure all the code paths are tested.
		The encoded length is hidden from the JDBC client.
	*/
	public static void testEncodedLengths(Connection conn) throws SQLException, IOException {

		System.out.println("START testEncodedLengths");

		Statement s = conn.createStatement();
		executeDrop(s, "DROP TABLE FBDVAL.TEL");
		s.execute("CREATE TABLE FBDVAL.TEL(C2 VARCHAR(32672) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(128k))");

		PreparedStatement psi = conn.prepareStatement("INSERT INTO FBDVAL.TEL VALUES(?, ?, ?)");
		PreparedStatement pss = conn.prepareStatement("SELECT * FROM FBDVAL.TEL");
		PreparedStatement psd = conn.prepareStatement("DELETE FROM FBDVAL.TEL");

		//insertEL(psi, pss, psd, 0);
		insertEL(psi, pss, psd,  10);
		insertEL(psi, pss, psd,  30);
		insertEL(psi, pss, psd,  31);
		insertEL(psi, pss, psd,  32); // switch to 2 byte length
		insertEL(psi, pss, psd,  1345);
		insertEL(psi, pss, psd,  23456);
		insertEL(psi, pss, psd,  32672);
		insertEL(psi, pss, psd,  32700);
		insertEL(psi, pss, psd,  (32*1024) - 1);
		insertEL(psi, pss, psd,  (32*1024));
		insertEL(psi, pss, psd,  (32*1024) + 1);
		insertEL(psi, pss, psd,  (64*1024) - 1);
		insertEL(psi, pss, psd,  (64*1024)); // switch to 4 byte length
		insertEL(psi, pss, psd,  (64*1024) + 1);
		insertEL(psi, pss, psd,  (110*1024) + 3242);





		psi.close();
		pss.close();
		psd.close();
		executeDrop(s, "DROP TABLE FBDVAL.TEL");
		s.close();
		System.out.println("END testEncodedLengths");

	}

	private static void insertEL(PreparedStatement psi, PreparedStatement pss, PreparedStatement psd, int length) throws SQLException, IOException {

		Connection conn = psi.getConnection();
		byte[] data = new byte[length];

		// random simple value check
		int off = (int)  (System.currentTimeMillis() % ((long) length));
		data[off] = 0x23;

		psi.setBytes(1, (length <= 32672) ? data : null);
		psi.setBytes(2, (length <= 32700) ? data : null);
		psi.setBinaryStream(3, new java.io.ByteArrayInputStream(data), length); // BLOB column
		psi.executeUpdate();
		conn.commit();

		ResultSet rs = pss.executeQuery();
		while (rs.next())
		{
			System.out.print(" EL byte[] " + length);
			byte[] v = rs.getBytes(1);
			if (v != null) {
				System.out.print(" C1 " + ((v.length == length) ? "OK" : ("FAIL <" + v.length + ">")));
				System.out.print(" DATA " + ((v[off] == 0x23) ? "OK" : ("FAIL " + off)));
			}
			else
				System.out.print(" C1 NULL");

			v = rs.getBytes(2);
			if (v != null) {
				System.out.print(" C2 " + ((v.length == length) ? "OK" : ("FAIL <" + v.length + ">")));
				System.out.print(" DATA " + ((v[off] == 0x23) ? "OK" : ("FAIL " + off)));
			}
			else
				System.out.print(" C2 NULL");
			InputStream c3 = rs.getBinaryStream(3);
			checkEncodedLengthValue("C3", c3, length, off);

			System.out.println("");
		}
		rs.close();

		rs = pss.executeQuery();
		while (rs.next())
		{
			System.out.print(" EL stream " + length);

			checkEncodedLengthValue("C1", rs.getBinaryStream(1), length, off);
			checkEncodedLengthValue("C2", rs.getBinaryStream(2), length, off);
			checkEncodedLengthValue("C3", rs.getBinaryStream(3), length, off);

			System.out.println("");
		}
		rs.close();

		conn.commit();

		psd.executeUpdate();
		conn.commit();


		psi.setBinaryStream(1, (length <= 32672) ? new java.io.ByteArrayInputStream(data) : null, length);
		psi.setBinaryStream(2, (length <= 32700) ? new java.io.ByteArrayInputStream(data) : null, length);
		psi.setBinaryStream(3, new java.io.ByteArrayInputStream(data), length); // BLOB column
		psi.executeUpdate();
		conn.commit();

		psd.executeUpdate();



		conn.commit();


	}

	private static void checkEncodedLengthValue(String col, InputStream is, int length, int off) throws IOException {

		if (is == null) {
			System.out.print(" " + col + " NULL");
			return;
		}
		byte[] buf = new byte[3213];
		boolean dataOK = false;
		int sl = 0;
		for (;;) {
			int r = is.read(buf);
			if (r < 0)
				break;

			if ((off >= sl) && (off < (sl + r))) {
				if (buf[off - sl] == 0x23)
					dataOK = true;
			}
			sl += r;
		}
		System.out.print(" " + col + " " + ((sl == length) ? "OK" : ("FAIL <" + sl + ">")));
		System.out.print(" DATA " + (dataOK ? "OK" : ("FAIL " + off)));
	}

	private static void showData(PreparedStatement psS, int id, byte[] original) throws SQLException {
		psS.setInt(1, id);
		psS.setInt(2, id + 10);
		ResultSet rs = psS.executeQuery();
		while (rs.next()) {

			System.out.print("  ORG ");
				System.out.print(showData(original));
			System.out.print("CHR ");
				System.out.print(showData(rs.getBytes(1)));
			System.out.print("VAR ");
				System.out.print(showData(rs.getBytes(2)));
			System.out.print("LVC ");
				System.out.print(showData(rs.getBytes(3)));
			System.out.print("BLOB ");
				System.out.print(showData(rs.getBytes(4)));

			System.out.println("");
		}
		rs.close();

	}

	private static void showCompareData(PreparedStatement psS) throws SQLException {
		ResultSet rs = psS.executeQuery();
		while (rs.next()) {
			System.out.print("  " + rs.getInt(1) + " ");
			System.out.print(showData(rs.getBytes(2)));
			System.out.print("  " + rs.getInt(3) + " ");
			System.out.println(showData(rs.getBytes(4)));
		}
		rs.close();
		psS.close();
	}

	private static String showData(byte[] data) {
		if (data == null)
			return "<NULL> ";

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			String s = Integer.toHexString(data[i] & 0xff);
			if (s.length() == 1)
				sb.append('0');
			sb.append(s);
		}

		sb.append(' ');
		sb.append('(');
		sb.append(data.length);
		sb.append(')');
		sb.append(' ');

		return sb.toString();
	}






	private static void showSQLE(SQLException sqle) {
		do {
			System.out.println(sqle.getSQLState() + ": " + sqle.getMessage());
			//sqle.printStackTrace(System.out);
			sqle = sqle.getNextException();
		} while (sqle != null);
	}
	private static void executeDrop(Statement s, String sql) {
		try {
			s.execute(sql);
		} catch (SQLException sqle) {
		}
	}
	private static void executeOK(Statement s, String sql) {
		System.out.println(sql);
		try {
			s.execute(sql);
		} catch (SQLException sqle) {
			System.out.println("FAIL ");
			showSQLE(sqle);
		}
	}
	private static void statementExceptionExpected(Statement s, String sql) {
		System.out.println(sql);
		try {
			s.execute(sql);
			System.out.println("FAIL - SQL expected to throw exception");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}
	}
	private static void expectedException(SQLException sqle) {
		String sqlState = sqle.getSQLState();
		if (sqlState == null) {
			sqlState = "<NULL>";
		}
		System.out.println("EXPECTED SQL Exception: (" + sqlState + ") " + sqle.getMessage());
	}

}

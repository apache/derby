/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMapping

   Copyright 2004, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;


import java.sql.*;
import java.math.*;
import java.io.*;

public class parameterMapping {

	private static int[] jdbcTypes =
	{
		Types.TINYINT,
		Types.SMALLINT,
		Types.INTEGER,
		Types.BIGINT,
		Types.REAL,
		Types.FLOAT,
		Types.DOUBLE,
		Types.DECIMAL,
		Types.NUMERIC,
		Types.BIT,
		Types.NULL, // Types.BOOLEAN
		Types.CHAR,
		Types.VARCHAR,
		Types.LONGVARCHAR,
		Types.NULL, //Types.BINARY,
		Types.VARBINARY,
		Types.NULL, //Types.LONGVARBINARY,
		Types.DATE,
		Types.TIME,
		Types.TIMESTAMP,
		Types.CLOB,
		Types.BLOB,
	};


	private static String[] SQLTypes =
	{
		null,
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"REAL",
		"FLOAT",
		"DOUBLE",
		"DECIMAL(10,5)",
		null,
		null,
		null,
		"CHAR(60)",
		"VARCHAR(60)",
		"LONG VARCHAR",
		"CHAR(60) FOR BIT DATA",
		"VARCHAR(60) FOR BIT DATA",
		"LONG VARCHAR FOR BIT DATA",
		"DATE",
		"TIME",
		"TIMESTAMP",
		"CLOB(1k)",
		"BLOB(1k)",

	};

	private static Class[] B3_GET_OBJECT =
	{
		java.lang.Integer.class, // Types.TINYINT,
		java.lang.Integer.class, // Types.SMALLINT,
		java.lang.Integer.class, // Types.INTEGER,
		java.lang.Long.class, // Types.BIGINT,
		java.lang.Float.class, // Types.REAL,
		java.lang.Double.class, // Types.FLOAT,
		java.lang.Double.class, // Types.DOUBLE,
		java.math.BigDecimal.class, // Types.DECIMAL,
		java.math.BigDecimal.class, // Types.NUMERIC,
		java.lang.Boolean.class, // Types.BIT,
		java.lang.Boolean.class, // Types.BOOLEAN
		java.lang.String.class, // Types.CHAR,
		java.lang.String.class, // Types.VARCHAR,
		java.lang.String.class, // Types.LONGVARCHAR,
		byte[].class, // Types.NULL, //Types.BINARY,
		byte[].class, // Types.VARBINARY,
		byte[].class, // Types.LONGVARBINARY,
		java.sql.Date.class, // Types.DATE,
		java.sql.Time.class, // Types.TIME,
		java.sql.Timestamp.class, // Types.TIMESTAMP,
		java.sql.Clob.class, // Types.CLOB,
		java.sql.Blob.class, // Types.BLOB,
	};





	private static final boolean _ = false;
	private static final boolean X = true;

	/**
		JDBC 3.0 spec Table B6 - Use of ResultSet getter Methods to Retrieve JDBC Data Types
	*/
	public static final boolean[][] B6 = {

    // Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
	//                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
	//                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
    //                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
    //                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
    //                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
    //                    T  N  R              L  C     N     R  R     A  R        A
	//                    T                                      C     R  B        M
    //                                                           H     B  I        P
	//                                                           A     I  N
	//                                                           R     N  

/* 0 getByte*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 1 getShort*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 getInt*/           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 getLong*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 getFloat*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 getDouble*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 getBigDecimal*/	{ X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 getBoolean*/       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 8 getString*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _},
/* 9 getBytes*/         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*10 getDate*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/*11 getTime*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, X, _, _},
/*12 getTimestamp*/     { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*13 getAsciiStream*/   { _, _, _, _, _, _, _, _, _, _, _, X, X, X, X, X, X, _, _, _, _, _},
/*14 getBinaryStream*/  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*15 getCharStream*/    { _, _, _, _, _, _, _, _, _, _, _, X, X, X, X, X, X, _, _, _, _, _},
/*16 getClob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},
/*17 getBlob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
		 
/*18 getUnicodeStream */{ _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
	};
   

	/**
		JDBC 3.0 Section 13.2.2.1 specifies that table B-2 is used to specify type mappings
		from the Java types (e.g. int as setInt) to the JDBC SQL Type (Types.INT).

		This table does not include stream methods and does not include conversions
		specified elsewhere in the text, Namely

		Section 16.3.2
			setBinaryStream may be used to set a BLOB
			setAsciiStream and setCharacterStream may be used to set a CLOB

		Thus this B2_MOD table is laid out like the B6 table and makes
		the assumptions that

		- Any Java numeric type can be used to set any SQL numeric type
		- Any Java numeric type can be used to set any SQL CHAR type
		- Numeric and date/time java types can be converted to SQL Char values.

		
	*/

    // Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
	//                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
	//                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
    //                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
    //                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
    //                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
    //                    T  N  R              L  C     N     R  R     A  R        A
	//                    T                                      C     R  B        M
    //                                                           H     B  I        P
	//                                                           A     I  N
	//                                                           R     N  

	public static boolean[][] B2_MOD = {
/* 0 setByte*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 1 setShort*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 setInt*/           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 setLong*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 setFloat*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 setDouble*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 setBigDecimal*/	{ X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 setBoolean*/       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 8 setString*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, X, X, X, _, _},
/* 9 setBytes*/         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*10 setDate*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/*11 setTime*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, X, _, _},
/*12 setTimestamp*/     { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*13 setAsciiStream*/   { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _, _, X, _},
/*14 setBinaryStream*/  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X},
/*15 setCharStream*/    { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _, _, X, _},
/*16 setClob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},
/*17 setBlob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
		 
/*18 setUnicodeStream */{ _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
	};

	/** Table B5 conversion of Objects using setObject*/

    // Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
	//                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
	//                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
    //                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
    //                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
    //                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
    //                    T  N  R              L  C     N     R  R     A  R        A
	//                    T                                      C     R  B        M
    //                                                           H     B  I        P
	//                                                           A     I  N
	//                                                           R     N  
	public static boolean[][] B5 = {
/* 0 String */          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _},
/* 1 BigDecimal */      { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 Boolean */         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 Integer */         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 Long */            { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 Float */           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 Double */			{ X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 byte[] */          { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/* 8 Date */            { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/* 9 Time */            { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, _, _, _},
/*10 Timestamp */       { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*11 Blob   */          { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
/*12 Clob */            { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},
	};


	private static boolean isDB2jNet;
 
	public static void main(String[] args) throws Exception {
		isDB2jNet = TestUtil.isNetFramework();
		System.out.println("Test parameterMapping starting");
		System.out.println("STILL TO RESOLVE -- Cloudscape getBoolean() allow conversion on strings to match JCC");
		System.out.println("STILL TO RESOLVE -- Cloudscape getXXX() disable on LOBs.");
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			 ij.getPropertyArg(args);
			 Connection conn = ij.startJBMS();

			 conn.setAutoCommit(false);

			 //create simple a table with BLOB and CLOB thta
			 // can be used to for setBlob/setClob testing.
			 Statement scb = conn.createStatement();

			 try {
				 scb.execute("DROP TABLE PM.LOB_GET");
			 }catch (SQLException seq) {
			 }
			 scb.execute("CREATE TABLE PM.LOB_GET(ID INT, B BLOB, C CLOB)");
			 PreparedStatement pscb = conn.prepareStatement("INSERT INTO PM.LOB_GET VALUES (?, ?, ?)");
			 pscb.setInt(1, 0);
			 pscb.setNull(2, Types.BLOB);
			 pscb.setNull(3, Types.CLOB);
			 pscb.executeUpdate();

			 pscb.setInt(1, 1);
			 {
				byte[] data = new byte[6];
				data[0] = (byte) 0x32;
				data[1] = (byte) 0x43;
				data[2] = (byte) 0x72;
				data[3] = (byte) 0x43;
				data[4] = (byte) 0x00;
				data[5] = (byte) 0x37;

				pscb.setBinaryStream(2, new java.io.ByteArrayInputStream(data), 6);
			 }
			 pscb.setCharacterStream(3, new java.io.StringReader("72"), 2);
			 pscb.executeUpdate();
			 scb.close();
			 pscb.close();
			 conn.commit();



			 for (int type = 0; type < SQLTypes.length; type++) {

				String sqlType = SQLTypes[type];

				System.out.println("\n\ngetXXX on : " + (sqlType == null ? Integer.toString(jdbcTypes[type]) : sqlType));
				 if (sqlType == null || jdbcTypes[type] == Types.NULL) {
					 System.out.println("  skipping");
					 continue;
				 }

				 Statement s = conn.createStatement();

				 try {
					 s.execute("DROP TABLE PM.TYPE_AS");
				 }catch (SQLException seq) {
				 }
				 s.execute("CREATE TABLE PM.TYPE_AS(VAL " + SQLTypes[type] + ")");

				 PreparedStatement psi = conn.prepareStatement("INSERT INTO PM.TYPE_AS(VAL) VALUES(?)");
				 psi.setNull(1, jdbcTypes[type]);
				 psi.executeUpdate();


				 PreparedStatement psq = conn.prepareStatement("SELECT VAL FROM PM.TYPE_AS");
				 ResultSet rs = psq.executeQuery();
				 ResultSetMetaData rsmd = rs.getMetaData();
				 if (rsmd.getColumnType(1) != jdbcTypes[type]) {
					 System.out.println("FAIL - mismatch column type " + rsmd.getColumnType(1) + " expected " + jdbcTypes[type]);
				 }
				 rs.close();

				 // For this data type
				 // Test inserting a NULL value and then performing all the getXXX() calls on it.

				 System.out.println(" NULL VALUE");
				 getXXX(psq, type, true);


				 s.execute("DELETE FROM PM.TYPE_AS");

				 // For this data type
				 // Test inserting a valid value and then performing all the getXXX() calls on it.
				 if (setValidValue(psi, 1, jdbcTypes[type])) {
					 psi.executeUpdate();
					System.out.println(" VALID VALUE");
					getXXX(psq, type, false);
				 }



				 // NOW THE SET METHODS
				 System.out.println("setNull() with all JDBC Types on " + SQLTypes[type]);
				 for (int st = 0; st <= jdbcTypes.length + 1; st++) {

					if (st >= jdbcTypes.length || jdbcTypes[st] != Types.NULL)
					{
						// explictily test Types.NULL.
						 int sqlTypeNull;
						 if (st == jdbcTypes.length + 1)
							sqlTypeNull = 235350345; // bad value
						 else if (st == jdbcTypes.length)
							 sqlTypeNull = Types.NULL;
						 else
							sqlTypeNull = jdbcTypes[st];

						s.execute("DELETE FROM PM.TYPE_AS");

						SQLException sqleResult = null;
						try {
							System.out.print("  setNull(" + TestUtil.sqlNameFromJdbc(sqlTypeNull) + ") ");
							psi.setNull(1, sqlTypeNull);
							psi.executeUpdate();

							getValidValue(psq, jdbcTypes[type]); // yes type, not st

							System.out.println("");

						} catch (SQLException sqle) {
							sqleResult = sqle;
							if ("22005".equals(sqle.getSQLState()))
								System.out.println("IC");
							else
								dumpSQLExceptions(sqle);

						}
					}					 
				 }

				 System.out.println("setXXX() with all JDBC Types on " + SQLTypes[type]);
				 System.out.println("For setXXX() methods that pass an object, a null and valid values are checked");
				 setXXX(s, psi, psq, type);

				 psi.close();
				 psq.close();
				 s.execute("DROP TABLE PM.TYPE_AS");
				 conn.commit();

				 if (isDB2jNet)
					 continue;

				 // NOW PROCEDURE PARAMETERS
				 try {
					 s.execute("DROP PROCEDURE PMP.TYPE_AS");
				 }catch (SQLException seq) {
				 }
				 String procSQL = "CREATE PROCEDURE PMP.TYPE_AS(" +
					   "IN P1 " + SQLTypes[type] + 
					 ", INOUT P2 " + SQLTypes[type] +
					 ", OUT P3 " + SQLTypes[type] +
					 ") LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL " +
					 " EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.pmap'";

				 System.out.println(procSQL);
				 try {
					s.execute(procSQL);
				 } catch (SQLException sqle) {
					 System.out.println(sqle.getSQLState() + ":" + sqle.getMessage());
					 continue;
				 }

				 // For each JDBC type try to register the out parameters with that type.
				 for (int opt = 0; opt < jdbcTypes.length; opt++) {
					 int jopt = jdbcTypes[opt];
					 if (jopt == Types.NULL)
						 continue;

					CallableStatement csp = conn.prepareCall("CALL PMP.TYPE_AS(?, ?, ?)");

					boolean bothRegistered = true;
					System.out.print("INOUT " + sqlType + " registerOutParameter(" + TestUtil.jdbcNameFromJdbc(jopt) + ") ");
					try {
						csp.registerOutParameter(2, jopt);
						System.out.println("-- OK");
					} catch (SQLException sqle) {
						System.out.println("-- " + sqle.getSQLState());
						bothRegistered = false;
					}
					System.out.print("OUT " + sqlType + " registerOutParameter(" + TestUtil.jdbcNameFromJdbc(jopt) + ") ");
					try {
						csp.registerOutParameter(3, jopt);
						System.out.println("-- OK");
					} catch (SQLException sqle) {
						System.out.println("-- " + sqle.getSQLState());
						bothRegistered = false;
					}

					if (bothRegistered) {

						try {

						// set the IN value with an accepted value according to its type
						// set the INOUT value with an accepted value according to its registered type
						if (setValidValue(csp, 1, jdbcTypes[type]) && setValidValue(csp, 2, jopt)) {

							csp.execute();

							// now get the INOUT, OUT parameters according to their registered type.
							System.out.print("P2="); getOutValue(csp, 2, jopt); System.out.println("");
							System.out.print("P3="); getOutValue(csp, 3, jopt); System.out.println("");
						}

						} catch (SQLException sqle) {
							dumpSQLExceptions(sqle);
						}
					}

					csp.close();

				 }


				s.execute("DROP PROCEDURE PMP.TYPE_AS");
				s.close();
				conn.commit();
			 }
		}
		catch (SQLException sqle) {
			unexpectedException(sqle);
		}
		catch (Throwable t) {
			t.printStackTrace(System.out);
		}
	}

	private static void getXXX(PreparedStatement ps, int type, boolean isNull) throws SQLException, java.io.IOException {

		{
		System.out.print("  getByte=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getByte(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 0, type);
		}


		{
		System.out.print("  getShort=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getShort(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 1, type);
		}

		{
		System.out.print("  getInt=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getInt(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 2, type);
		}

		{
		System.out.print("  getLong=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getLong(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 3, type);
		}

		{
		System.out.print("  getFloat=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getFloat(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 4, type);
		}

		{
		System.out.print("  getDouble=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getDouble(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 5, type);
		}

		{
		System.out.print("  getBigDecimal=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getBigDecimal(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 6, type);
		}

		{
		System.out.print("  getBoolean=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getBoolean(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 7, type);
		}

		{
		System.out.print("  getString=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			System.out.print(rs.getString(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 8, type);
		}

		{
		System.out.print("  getBytes=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			byte[] data = rs.getBytes(1) ;
			System.out.print(data == null ? null : parameterMapping.showFirstTwo(data));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 9, type);
		}

		{
		System.out.print("  getDate=");
		boolean worked;
		SQLException sqleResult = null;;
		ResultSet rs = null;
		try {
			rs = ps.executeQuery();
			rs.next();
			System.out.print(rs.getDate(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			// 22007 invalid date time conversion
			worked = "22007".equals(sqle.getSQLState());
			if (worked)
				System.out.print(sqle.getSQLState());
		} catch (Throwable t) {
			System.out.print(t.toString());
			worked = false;
		}
		if (rs != null)
			rs.close();
		judge_getXXX(worked, sqleResult, 10, type);
		}

		{
		boolean worked;
		SQLException sqleResult = null;;
		ResultSet rs = null;
		try {
			System.out.print("  getTime=");
			rs = ps.executeQuery();
			rs.next();
			System.out.print(rs.getTime(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			// 22007 invalid date time conversion
			worked = "22007".equals(sqle.getSQLState());
			if (worked)
				System.out.print(sqle.getSQLState());
		} catch (Throwable t) {
			System.out.print(t.toString());
			worked = false;
		}
		if (rs != null)
			rs.close();
		judge_getXXX(worked, sqleResult, 11, type);
		}
		
		{
		boolean worked;
		SQLException sqleResult = null;;
		ResultSet rs = null;
		try {
			System.out.print("  getTimestamp=");
			rs = ps.executeQuery();
			rs.next();
			System.out.print(rs.getTimestamp(1));
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			// 22007 invalid date time conversion
			worked = "22007".equals(sqle.getSQLState());
			if (worked)
				System.out.print(sqle.getSQLState());
		} catch (Throwable t) {
			System.out.print(t.toString());
			worked = false;
		}
		if (rs != null)
			rs.close();
		judge_getXXX(worked, sqleResult, 12, type);
		}

		{
		System.out.print("  getAsciiStream=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			InputStream is = rs.getAsciiStream(1);
			// if the value is NULL speific checks are performed below.
			if (!isNull || B6[13][type]) {
				System.out.print(is == null ? "null" : parameterMapping.showFirstTwo(is));
				System.out.print(" was null " + rs.wasNull());
			}
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}

		// getAsciiStream on a NULL value for an invalid conversion
		// is handled differently in JCC to Cloudscape. On a non-NULL
		// value an exception is correctly raised by both JCC and CS.
		// here we check this specific case to reduce canon differences
		// between CNS and CS.

		boolean judge = B6[13][type] || specificCheck(rs, worked, sqleResult, isNull);
		rs.close();
		if (judge)
			judge_getXXX(worked, sqleResult, 13, type);
		}

		{
		System.out.print("  getBinaryStream=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			InputStream is = rs.getBinaryStream(1);
			if (!isNull || B6[14][type]) {
				System.out.print(is == null ? "null" : parameterMapping.showFirstTwo(is));
				System.out.print(" was null " + rs.wasNull());
			}
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		boolean judge = B6[14][type] || specificCheck(rs, worked, sqleResult, isNull);
		rs.close();
		if (judge)
			judge_getXXX(worked, sqleResult, 14, type);
		}

		{
		System.out.print("  getCharacterStream=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			Reader r = rs.getCharacterStream(1);
			if (!isNull || B6[15][type]) {
				System.out.print(r == null ? "null" : parameterMapping.showFirstTwo(r));
				System.out.print(" was null " + rs.wasNull());
			}
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		boolean judge = B6[15][type] || specificCheck(rs, worked, sqleResult, isNull);
		rs.close();
		if (judge)
			judge_getXXX(worked, sqleResult, 15, type);
		}

		{
		System.out.print("  getClob=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			Clob clob = rs.getClob(1);
			if (!isNull || B6[16][type]) {
				System.out.print(clob == null ? "null" : clob.getSubString(1, 10));
				System.out.print(" was null " + rs.wasNull());
			}
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		boolean judge = B6[16][type] || specificCheck(rs, worked, sqleResult, isNull);
		rs.close();
		if (judge)
			judge_getXXX(worked, sqleResult, 16, type);
		}

		{
		System.out.print("  getBlob=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			Blob blob = rs.getBlob(1);
			if (!isNull || B6[17][type]) {
				System.out.print(blob == null ? "null" : parameterMapping.showFirstTwo(blob.getBinaryStream()));
				System.out.print(" was null " + rs.wasNull());
			}
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		boolean judge = B6[17][type] || specificCheck(rs, worked, sqleResult, isNull);
		rs.close();
		if (judge)
			judge_getXXX(worked, sqleResult, 17, type);
		}

		{
		System.out.print("  getUnicodeStream=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		boolean worked;
		SQLException sqleResult = null;;
		try {
			InputStream is = rs.getUnicodeStream(1);
			System.out.print(is == null ? "null" : "data");
			System.out.print(" was null " + rs.wasNull());
			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		rs.close();
		judge_getXXX(worked, sqleResult, 18, type);
		}

		// Check to see getObject returns the correct type
		{
		System.out.print("  getObject=");
		ResultSet rs = ps.executeQuery();
		rs.next();
		SQLException sqleResult = null;;
		try {
			Object o = rs.getObject(1);

			Class cgo = B3_GET_OBJECT[type];

			String cname;
			if (cgo.equals(byte[].class))
				cname = "byte[]";
			else
				cname = cgo.getName();

			String msg;
			if (o == null)
			{
				msg = "null";
			}
			else if (cgo.isInstance(o))
			{
				msg = "CORRECT :" + cgo.getName();
			}
			else
			{
				msg = "FAIL NOT :" + cgo.getName() + " is " + o.getClass().getName();
			}

			System.out.print(msg);
			System.out.println(" was null " + rs.wasNull());

		} catch (SQLException sqle) {
			sqleResult = sqle;
		}
		rs.close();
		}

  }

	private static boolean specificCheck(ResultSet rs, boolean worked, SQLException sqleResult, boolean isNull)
		throws SQLException {
		boolean judge = true;
		if (worked && isNull && rs.wasNull())
		{
			// JCC returns NULL
			if (isDB2jNet)
				judge = false;
		}
		else if (!worked && isNull)
		{
			if (!isDB2jNet && "22005".equals(sqleResult.getSQLState()))
				judge = false;
		}
		if (!judge)
			System.out.println("SPECIFIC CHECK OK");

		return judge;
	}

	private static void judge_getXXX(boolean worked, SQLException sqleResult, int whichCall, int type) {
		String msg;
		if (worked && B6[whichCall][type])
			msg = " JDBC MATCH(OK)";
		else if (worked)
			msg = " CLOUD EXT (OK)";
		else if (B6[whichCall][type]) {
			if (sqleResult != null)
				showException(sqleResult);
			msg = " JDBC FAIL " + SQLTypes[type];
		}
		else {

			String sqlState = sqleResult.getSQLState();
			if ("22005".equals(sqlState))
				System.out.print("IC"); // embedded invalid conversion error
			else if (sqlState == null) {
				// embedded invalid conversion error
				if (sqleResult.getMessage().indexOf("Wrong result column type for requested conversion") != -1)
					System.out.print("IC");
				else if (sqleResult != null)
					showException(sqleResult);
			}
			else if (sqleResult != null)
				showException(sqleResult);

			msg = " JDBC MATCH (INVALID)";
		}

		System.out.println(msg);
	}
	private static void judge_setXXX(boolean worked, SQLException sqleResult, int whichCall, int type) {
		String msg;
		if (worked && B2_MOD[whichCall][type])
			msg = " JDBC MATCH(OK)";
		else if (worked)
			msg = " CLOUD EXT (OK)";
		else if (B2_MOD[whichCall][type]) {
			if (sqleResult != null)
				showException(sqleResult);
			msg = " JDBC FAIL " + SQLTypes[type];
		}
		else {
			if (sqleResult == null)
				return;

			// XCL12 is temp
			if ("22005".equals(sqleResult.getSQLState()) || "XCL12".equals(sqleResult.getSQLState()))
				System.out.print("IC");
			else if (sqleResult.getMessage().indexOf("Illegal Conv") != -1)
				System.out.print("IC");
			else 
				showException(sqleResult);

			msg = " JDBC MATCH (INVALID)";
		}

		System.out.println(msg);
	}
	private static void judge_setObject(boolean worked, SQLException sqleResult, int b5o, int type) {
		String msg;
		if (worked && B5[b5o][type])
			msg = " JDBC MATCH(OK)";
		else if (worked)
			msg = " CLOUD EXT (OK)";
		else if (B5[b5o][type]) {
			if (sqleResult != null)
				showException(sqleResult);
			msg = " JDBC FAIL " + SQLTypes[type];
		}
		else {
			if (sqleResult == null)
				return;

			// XCL12 is temp
			if ("22005".equals(sqleResult.getSQLState()) || "XCL12".equals(sqleResult.getSQLState()))
				System.out.print("IC");
			else if (sqleResult.getMessage().indexOf("Illegal Conv") != -1)
				System.out.print("IC");
			else 
				showException(sqleResult);

			msg = " JDBC MATCH (INVALID)";
		}

		System.out.println(msg);
	}
	private static void setXXX(Statement s, PreparedStatement psi, PreparedStatement psq, int type) throws SQLException, java.io.IOException {

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setByte() ");
			psi.setByte(1, (byte) 98);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 0, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setShort() ");
			psi.setShort(1, (short) 98);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 1, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setInt() ");
			psi.setInt(1, 98);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 2, type);
		}
	
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setLong() ");
			psi.setLong(1, 98L);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 3, type);
		}
		
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setFloat() ");
			psi.setFloat(1, 98.4f);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 4, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setDouble() ");
			psi.setDouble(1, 98.5);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 5, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBigDecimal() ");
			psi.setBigDecimal(1, new BigDecimal(99.0));
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 6, type);
		}
		// null BigDecimal
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBigDecimal(null) ");
			psi.setBigDecimal(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 6, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBoolean() ");
			psi.setBoolean(1, true);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 7, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setString() ");
			psi.setString(1, "97");
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		} catch (Throwable t) {
			// JCC has some bugs
			System.out.println(t.getMessage());
			worked = false;
			sqleResult = null;

		}
		judge_setXXX(worked, sqleResult, 8, type);
		}

		// null String
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setString(null) ");
			psi.setString(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		} catch (Throwable t) {
			// JCC has some bugs
			System.out.println(t.getMessage());
			worked = false;
			sqleResult = null;

		}
		judge_setXXX(worked, sqleResult, 8, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBytes() ");
			byte[] data = {(byte) 0x04, (byte) 0x03, (byte) 0xfd, (byte) 0xc3, (byte) 0x73};
			psi.setBytes(1, data);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 9, type);
		}
		// null byte[]
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBytes(null) ");
			psi.setBytes(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 9, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setDate() ");
			psi.setDate(1, java.sql.Date.valueOf("2004-02-14"));
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 10, type);
		}
		// null Date
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setDate(null) ");
			psi.setDate(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 10, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setTime() ");
			psi.setTime(1, java.sql.Time.valueOf("13:26:42"));
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 11, type);
		}
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setTime(null) ");
			psi.setTime(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 11, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setTimestamp() ");
			psi.setTimestamp(1, java.sql.Timestamp.valueOf("2004-02-23 17:14:24.097625551"));
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 12, type);
		}
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setTimestamp(null) ");
			psi.setTimestamp(1, null);
			psi.executeUpdate();

			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 12, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setAsciiStream() ");
				byte[] data = new byte[6];
				data[0] = (byte) 0x65;
				data[1] = (byte) 0x67;
				data[2] = (byte) 0x30;
				data[3] = (byte) 0x31;
				data[4] = (byte) 0x32;
				data[5] = (byte) 0x64;

			psi.setAsciiStream(1, new java.io.ByteArrayInputStream(data), 6);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 13, type);
		}
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setAsciiStream(null) ");
			psi.setAsciiStream(1, null, 0);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 13, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBinaryStream() ");
				byte[] data = new byte[6];
				data[0] = (byte) 0x82;
				data[1] = (byte) 0x43;
				data[2] = (byte) 0xca;
				data[3] = (byte) 0xfe;
				data[4] = (byte) 0x00;
				data[5] = (byte) 0x32;

			psi.setBinaryStream(1, new java.io.ByteArrayInputStream(data), 6);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 14, type);
		}	
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBinaryStream(null) ");
			psi.setBinaryStream(1, null, 0);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 14, type);
		}	
		

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setCharacterStream() ");
			psi.setCharacterStream(1, new java.io.StringReader("89"), 2);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 15, type);
		}
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setCharacterStream(null) ");
			psi.setCharacterStream(1, null, 0);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 15, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setClob() ");

			ResultSet rsc = s.executeQuery("SELECT C FROM PM.LOB_GET WHERE ID = 1");
			rsc.next();
			Clob tester = rsc.getClob(1);
			rsc.close();


			psi.setClob(1, tester);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 16, type);
		}
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setClob(null) ");

			psi.setClob(1, null);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 16, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");
		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBlob() ");

			ResultSet rsc = s.executeQuery("SELECT B FROM PM.LOB_GET WHERE ID = 1");
			rsc.next();
			Blob tester = rsc.getBlob(1);
			rsc.close();


			psi.setBlob(1, tester);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 17, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");
		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setBlob(null) ");

			psi.setBlob(1, null);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 17, type);
		}

		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setUnicodeStream() ");
				byte[] data = new byte[6];
				data[0] = (byte) 0x82;
				data[1] = (byte) 0x43;
				data[2] = (byte) 0xca;
				data[3] = (byte) 0xfe;
				data[4] = (byte) 0x00;
				data[5] = (byte) 0x32;

			psi.setUnicodeStream(1, new java.io.ByteArrayInputStream(data), 6);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 14, type);
		}	
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setUnicodeStream(null) ");
			psi.setUnicodeStream(1, null, 0);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		judge_setXXX(worked, sqleResult, 14, type);
		}


		// setObject(null)
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			// should never work!
			System.out.print("  setObject(null) ");
			psi.setObject(1, null);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		}
		System.out.println(worked ? " FAIL " : (" OK " + sqleResult.getMessage()));
		}


		setXXX_setObject(s, psi, psq, type, "46", "java.lang.String", 0);
		setXXX_setObject(s, psi, psq, type, BigDecimal.valueOf(72L), "java.math.BigDecimal", 1);
		setXXX_setObject(s, psi, psq, type, Boolean.TRUE, "java.lang.Boolean", 2);
		setXXX_setObject(s, psi, psq, type, new Integer(74), "java.lang.Integer", 3);
		setXXX_setObject(s, psi, psq, type, new Long(79), "java.lang.Long", 4);
		setXXX_setObject(s, psi, psq, type, new Float(76.3f), "java.lang.Float", 5);
		setXXX_setObject(s, psi, psq, type, new Double(12.33d), "java.lang.Double", 6);

		{
		byte[] data = {0x32, 0x39};
		setXXX_setObject(s, psi, psq, type, data, "byte[]", 7);
		}


		setXXX_setObject(s, psi, psq, type, java.sql.Date.valueOf("2004-02-14"), "java.sql.Date", 8);
		setXXX_setObject(s, psi, psq, type, java.sql.Time.valueOf("13:26:42"), "java.sql.Time", 9);
		setXXX_setObject(s, psi, psq, type, java.sql.Timestamp.valueOf("2004-02-23 17:14:24.097625551"), "java.sql.Timestamp", 10);
		s.getConnection().commit();

		if (!isDB2jNet) {
		{
			ResultSet rsc = s.executeQuery("SELECT B FROM PM.LOB_GET WHERE ID = 1");
			rsc.next();
			Blob tester = rsc.getBlob(1);
			rsc.close();
			setXXX_setObject(s, psi, psq, type, tester, "java.sql.Blob", 11);
		}

		{
			ResultSet rsc = s.executeQuery("SELECT C FROM PM.LOB_GET WHERE ID = 1");
			rsc.next();
			Clob tester = rsc.getClob(1);
			rsc.close();
			setXXX_setObject(s, psi, psq, type, tester, "java.sql.Clob", 12);
		}
		}
	}

	private static void setXXX_setObject(Statement s, PreparedStatement psi, PreparedStatement psq, int type, Object value, String className, int b5o)
		throws SQLException, java.io.IOException
	{
		{
		s.execute("DELETE FROM PM.TYPE_AS");

		SQLException sqleResult = null;
		boolean worked;
		try {
			System.out.print("  setObject(" + className + ") ");
			psi.setObject(1, value);
			psi.executeUpdate();
			getValidValue(psq, jdbcTypes[type]);

			worked = true;

		} catch (SQLException sqle) {
			sqleResult = sqle;
			worked = false;
		} catch (Throwable t) {
			System.out.println("FAIL " + t.getMessage());
			return;
		}
		judge_setObject(worked, sqleResult, b5o, type);
		}
	}

	private static void unexpectedException(SQLException sqle) {

		System.out.print("FAIL unexpected exception - ");
		showException(sqle);
		sqle.printStackTrace(System.out);
	}

	private static void showException(SQLException sqle) {
		do {
			String state = sqle.getSQLState();
			if (state == null)
				state = "?????";

			String msg = sqle.getMessage();
			if  (msg == null)
				msg = "?? no message ??";

			System.out.print(" (" + state + "):" + msg);
			sqle = sqle.getNextException();
		} while (sqle != null);
	}

	private static boolean setValidValue(PreparedStatement ps, int param, int jdbcType)
		throws SQLException {

		switch (jdbcType) {
		case Types.BIT:
			ps.setBoolean(param, true);
			return true;
		case Types.TINYINT:
			ps.setByte(param, (byte) 32);
			return true;
		case Types.SMALLINT:
			ps.setShort(param, (short) 32);
			return true;
		case Types.INTEGER:
			ps.setInt(param, 32);
			return true;
		case Types.BIGINT:
			ps.setLong(param, 32L);
			return true;
		case Types.REAL:
			ps.setFloat(param, 32.0f);
			return true;
		case Types.FLOAT:
		case Types.DOUBLE:
			ps.setDouble(param, 32.0);
			return true;
		case Types.DECIMAL:
			ps.setBigDecimal(param, new BigDecimal(32.0));
			return true;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			ps.setString(param, "32");
			return true;
		case Types.BINARY:
		case Types.VARBINARY:
			{
				byte[] data = {(byte) 0x04, (byte) 0x03, (byte) 0xfd, (byte) 0xc3, (byte) 0x73};
				ps.setBytes(param, data);
				return true;
			}
		//Types.LONGVARBINARY:
		case Types.DATE:
			ps.setDate(param, java.sql.Date.valueOf("2004-02-14"));
			return true;
		case Types.TIME:
			ps.setTime(param, java.sql.Time.valueOf("13:26:42"));
			return true;
		case Types.TIMESTAMP:
			ps.setTimestamp(param, java.sql.Timestamp.valueOf("2004-02-23 17:14:24.097625551"));
			return true;
		case Types.CLOB:
			// JDBC 3.0 spec section 16.3.2 explictly states setCharacterStream is OK for setting a CLOB
			ps.setCharacterStream(param, new java.io.StringReader("67"), 2);
			return true;
		case Types.BLOB:
			// JDBC 3.0 spec section 16.3.2 explictly states setBinaryStream is OK for setting a BLOB
			{
				byte[] data = new byte[6];
				data[0] = (byte) 0x82;
				data[1] = (byte) 0x43;
				data[2] = (byte) 0xca;
				data[3] = (byte) 0xfe;
				data[4] = (byte) 0x00;
				data[5] = (byte) 0x32;

			ps.setBinaryStream(param, new java.io.ByteArrayInputStream(data), 6);
			return true;
			}
		default:
			return false;
		}
	}

	private static boolean getValidValue(PreparedStatement ps, int jdbcType)
		throws SQLException, IOException {

		ResultSet rs = ps.executeQuery();
		rs.next();

		switch (jdbcType) {
		case Types.SMALLINT:
			System.out.print("getShort=" + rs.getShort(1) + " was null " + rs.wasNull());
			return true;
		case Types.INTEGER:
			System.out.print("getInt=" + rs.getInt(1) + " was null " + rs.wasNull());
			return true;
		case Types.BIGINT:
			System.out.print("getLong=" + rs.getLong(1) + " was null " + rs.wasNull());
			return true;
		case Types.REAL:
			System.out.print("getFloat=" + rs.getFloat(1) + " was null " + rs.wasNull());
			return true;
		case Types.FLOAT:
		case Types.DOUBLE:
			System.out.print("getDouble=" + rs.getDouble(1) + " was null " + rs.wasNull());
			return true;
		case Types.DECIMAL:
			System.out.print("getBigDecimal=" + rs.getBigDecimal(1) + " was null " + rs.wasNull());
			return true;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			{
			String s = rs.getString(1);
			if (s != null)
			{
				// With IBM's DB2 universal driver.
				// Setting a java.sql.Clob value works with
				// a character column but sets the value to
				// be the object's toString. This is probably a bug with JCC.
				if (s.startsWith("com.ibm.db2.jcc.") || 
					s.startsWith("org.apache.derby.client"))
					s = "<OBJECT.toString()>";


				boolean hasNonAscii = false;
				// check for any characters in the control range
				for (int si = 0; si < s.length(); si++)
				{
					char c = s.charAt(si);
					if (c < (char) 0x20 || c >= (char) 0x7f)
					{
						hasNonAscii = true;
						break;
					}
				}

				if (hasNonAscii)
				{
					StringBuffer sb = new StringBuffer();

					sb.append("EncodedString: >");
					for (int si = 0; si < s.length(); si++)
					{
						sb.append(' ');
						sb.append((int) s.charAt(si));
					}
					sb.append(" <");
					s = sb.toString();

				}
			}
			System.out.print("getString=" + s + " was null " + rs.wasNull());
			return true;
			}
		case Types.BINARY:
		case Types.VARBINARY:
			{
			byte[] data = rs.getBytes(1) ;
			System.out.print("getBytes=" + (data == null ? "null" : parameterMapping.showFirstTwo(data)));
			System.out.print(" was null " + rs.wasNull());
			return true;
			}
		case Types.LONGVARBINARY:
			{
			InputStream is = rs.getBinaryStream(1);
			System.out.print("getBinaryStream=" + (is == null ? "null" : parameterMapping.showFirstTwo(is)));
			System.out.print(" was null " + rs.wasNull());
			return true;
			}

		case Types.DATE:
			System.out.print("getDate=" + rs.getDate(1) + " was null " + rs.wasNull());
			return true;
		case Types.TIME:
			System.out.print("getTime=" + rs.getTime(1) + " was null " + rs.wasNull());
			return true;
		case Types.TIMESTAMP:
			System.out.print("getTimestamp=" + rs.getTime(1) + " was null " + rs.wasNull());
			return true;
		case Types.CLOB:
			{
			Clob clob = rs.getClob(1);
			System.out.print("getClob=" + (clob == null ? "null" : parameterMapping.showFirstTwo(clob.getCharacterStream())));
			System.out.print(" was null " + rs.wasNull());
			return true;
			}
		case Types.BLOB:
			{
			Blob blob = rs.getBlob(1);
			System.out.print("getBlob=" + (blob == null ? "null" : parameterMapping.showFirstTwo(blob.getBinaryStream())));
			System.out.print(" was null " + rs.wasNull());
			return true;
			}
		default:
			System.out.println("FAIL JDBC TYPE IN getValidValue " + TestUtil.sqlNameFromJdbc(jdbcType));
			return false;
		}
	}

	private static boolean getOutValue(CallableStatement cs, int param, int jdbcType)
		throws SQLException, IOException {

		switch (jdbcType) {
		case Types.BIT:
			System.out.print("cs.getBoolean=" + cs.getBoolean(param) + " was null " + cs.wasNull());
			return true;
		case Types.TINYINT:
			System.out.print("cs.getByte=" + cs.getByte(param) + " was null " + cs.wasNull());
			return true;

		case Types.SMALLINT:
			System.out.print("cs.getShort=" + cs.getShort(param) + " was null " + cs.wasNull());
			return true;
		case Types.INTEGER:
			System.out.print("cs.getInt=" + cs.getInt(param) + " was null " + cs.wasNull());
			return true;
		case Types.BIGINT:
			System.out.print("cs.getLong=" + cs.getLong(param) + " was null " + cs.wasNull());
			return true;
		case Types.REAL:
			System.out.print("cs.getFloat=" + cs.getFloat(param) + " was null " + cs.wasNull());
			return true;
		case Types.FLOAT:
		case Types.DOUBLE:
			System.out.print("cs.getDouble=" + cs.getDouble(param) + " was null " + cs.wasNull());
			return true;
		case Types.DECIMAL:
			System.out.print("cs.getBigDecimal=" + cs.getBigDecimal(param) + " was null " + cs.wasNull());
			return true;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			System.out.print("cs.getString=" + cs.getString(param) + " was null " + cs.wasNull());
			return true;
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			{
			byte[] data = cs.getBytes(param) ;
			System.out.print("cs.getBytes=" + (data == null ? "null" : parameterMapping.showFirstTwo(data)));
			System.out.print(" was null " + cs.wasNull());
			return true;
			}

		case Types.DATE:
			System.out.print("cs.getDate=" + cs.getDate(param) + " was null " + cs.wasNull());
			return true;
		case Types.TIME:
			System.out.print("cs.getTime=" + cs.getTime(param) + " was null " + cs.wasNull());
			return true;
		case Types.TIMESTAMP:
			System.out.print("cs.getTimestamp=" + cs.getTime(param) + " was null " + cs.wasNull());
			return true;
		case Types.CLOB:
			{
			Clob clob = cs.getClob(param);
			System.out.print("cs.getClob=" + (clob == null ? "null" : parameterMapping.showFirstTwo(clob.getCharacterStream())));
			System.out.print(" was null " + cs.wasNull());
			return true;
			}
		case Types.BLOB:
			{
			Blob blob = cs.getBlob(param);
			System.out.print("cs.getBlob=" + (blob == null ? "null" : parameterMapping.showFirstTwo(blob.getBinaryStream())));
			System.out.print(" was null " + cs.wasNull());
			return true;
			}
		default:
			System.out.println("FAIL JDBC TYPE IN getOutValue " + TestUtil.sqlNameFromJdbc(jdbcType));
			return false;
		}
	}

	static void dumpSQLExceptions (SQLException se) {

		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): " + se.toString());
			se = se.getNextException();
		}
	}

	private static String showFirstTwo(java.io.Reader in) throws java.io.IOException {

		int b1 = in.read();
		int b2 = in.read();
		in.close();

		return "0x" + Integer.toHexString(b1) + "," + "0x" + Integer.toHexString(b2);
	}
	private static String showFirstTwo(java.io.InputStream in) throws java.io.IOException {

		int b1 = in.read();
		int b2 = in.read();
		in.close();

		return "0x" + Integer.toHexString(b1) + "," + "0x" + Integer.toHexString(b2);
	}
	private static String showFirstTwo(byte[] data) {

		int b1 = data[0];
		int b2 = data[1];

		return "0x" + Integer.toHexString(((int) b1) & 0xff) + "," + "0x" + Integer.toHexString(((int) b2) & 0xff);
	}
}

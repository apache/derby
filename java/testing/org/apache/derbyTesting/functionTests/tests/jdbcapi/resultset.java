/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.resultset

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;


import java.lang.reflect.*;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.JDBCTestDisplayUtil;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;

/**
 * Test of JDBC result set and result set meta-data.
 * This program simply calls each of the result set and result set meta-data
 * methods, one by one, and prints the results.  The test passes if the printed
 * results match a previously stored "master".  Thus this test cannot actually
 * discern whether it passes or not.
 *
 * Test is only touching on known result set hot-spots at present.

 * Performs SecurityCheck analysis on the JDBC ResultSet and
 * ResultSetMetaData objects returned.
 * 
 * @see org.apache.derbyTesting.functionTests.util.SecurityCheck
 * @author ames
 */

public class resultset {

  private static Class[] CONN_PARAM = { Integer.TYPE };
  private static Object[] CONN_ARG = { new Integer(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT)};

	static private boolean isDerbyNet = false;

	private static String VALID_DATE_STRING = "'2000-01-01'";
	private static String VALID_TIME_STRING = "'15:30:20'";
	private static String VALID_TIMESTAMP_STRING = "'2000-01-01 15:30:20'";
	private static String NULL_VALUE="NULL";

	private static String[] SQLTypes =
	{
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"DECIMAL(10,5)",
		"REAL",
		"DOUBLE",
		"CHAR(60)",
		"VARCHAR(60)",
		"LONG VARCHAR",
		"CHAR(60) FOR BIT DATA",
		"VARCHAR(60) FOR BIT DATA",
		"LONG VARCHAR FOR BIT DATA",
		"CLOB(1k)",
		"DATE",
		"TIME",
		"TIMESTAMP",
		"BLOB(1k)",

	};

	private static String[] ColumnNames =
	{
		"SMALLINTCOL",
		"INTEGERCOL",
		"BIGINTCOL",
		"DECIMALCOL",
		"REALCOL",
		"DOUBLECOL",
		"CHARCOL",
		"VARCHARCOL",
		"LONGVARCHARCOL",
		"CHARFORBITCOL",
		"VARCHARFORBITCOL",
		"LVARCHARFORBITCOL",
		"CLOBCOL",
		"DATECOL",
		"TIMECOL",
		"TIMESTAMPCOL",
		"BLOBCOL",

	};

 private static String[][]SQLData =
	{
		{NULL_VALUE, "0","1","2"},       // SMALLINT
		{NULL_VALUE,"0","1","21"},       // INTEGER
		{NULL_VALUE,"0","1","22"},       // BIGINT
		{NULL_VALUE,"0.0","1.0","23.0"},      // DECIMAL(10,5)
		{NULL_VALUE,"0.0","1.0","24.0"},      // REAL,
		{NULL_VALUE,"0.0","1.0","25.0"},      // DOUBLE
		{NULL_VALUE,"'0'","'aa'","'2.0'"},      // CHAR(60)
		{NULL_VALUE,"'0'","'aa'",VALID_TIME_STRING},      //VARCHAR(60)",
		{NULL_VALUE,"'0'","'aa'",VALID_TIMESTAMP_STRING},      // LONG VARCHAR
		{NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaaa'"},  // CHAR(60)  FOR BIT DATA
		{NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaba'"},  // VARCHAR(60) FOR BIT DATA
		{NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaca'"},  //LONG VARCHAR FOR BIT DATA
		{NULL_VALUE,"'13'","'14'",NULL_VALUE},     //CLOB(1k)
		{NULL_VALUE,VALID_DATE_STRING,VALID_DATE_STRING,NULL_VALUE},        // DATE
		{NULL_VALUE,VALID_TIME_STRING,VALID_TIME_STRING,VALID_TIME_STRING},        // TIME
		{NULL_VALUE,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING},   // TIMESTAMP
		{NULL_VALUE,NULL_VALUE,NULL_VALUE,NULL_VALUE}                 // BLOB
	};

 /**
	 * Hang onto the SecurityCheck class while running the
	 * tests so that it is not garbage collected during the
	 * test and lose the information it has collected.
	 */
	private final Object nogc = SecurityCheck.class;

	public static void main(String[] args) throws Throwable {

		isDerbyNet = TestUtil.isNetFramework();

		Connection con;
		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		String[]  columnNames = {"i", "s", "r", "d", "dt", "t", "ts", "c", "v",
							 "dc", "bi", "cbd", "vbd", "lvbd", "cl", "bl"};


		System.out.println("Test resultset starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			// Test setCatalog/getCatalog for beetle 5504
			con.setCatalog("mycatalog");
			String cat = con.getCatalog();
			if (cat != null )
				System.out.println("ERROR: getCatalog did not return null");
			//Use reflection to set the holdability to false so that the test can run in jdk14 and lower jdks as well
			try {
				Method sh = con.getClass().getMethod("setHoldability", CONN_PARAM);
				sh.invoke(con, CONN_ARG);
			} catch (Exception e) {System.out.println("shouldn't get that error " + e.getMessage());}//for jdks prior to jdk14

			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
									   ResultSet.CONCUR_UPDATABLE);

            try { 
                stmt.execute("drop table t"); 
            } catch (SQLException se) {
            } // ignore, assume it is because table does not exist

			stmt.execute("create table t (i int, s smallint, r real, "+
				"d double precision, dt date, t time, ts timestamp, "+
				"c char(10), v varchar(40) not null, dc dec(10,2),"+
				"bi bigint, cbd char(10) for bit data," +
				"vbd varchar(10) for bit data,lvbd long varchar for bit data,"+
				"cl clob(2G), bl blob(1G) )");
			stmt.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
						 "time('12:06:06'),timestamp('1990-07-07 07:07:07.000007'),"+
						 "'eight','nine', 10.1, 11," + 
						 TestUtil.stringToHexLiteral("twelv") +  "," +
						 TestUtil.stringToHexLiteral("3teen") +  "," +
						 TestUtil.stringToHexLiteral("4teen") +  ", null, null)" );


			rs = stmt.executeQuery("select i, s, r, d, dt, t, ts, c, v, dc, bi, cbd, vbd, lvbd, cl, bl from t");
			met = rs.getMetaData();

			int colCount;
			System.out.println("getColumnCount(): "+(colCount=met.getColumnCount()));

			// JDBC columns use 1-based counting
			for (int i=1;i<=colCount;i++) {
				System.out.println("isAutoIncrement("+i+"): "+met.isAutoIncrement(i));
				System.out.println("isCaseSensitive("+i+"): "+met.isCaseSensitive(i));
				System.out.println("isSearchable("+i+"): "+met.isSearchable(i));
				System.out.println("isCurrency("+i+"): "+met.isCurrency(i));
				System.out.println("isNullable("+i+"): "+met.isNullable(i));
				System.out.println("isSigned("+i+"): "+met.isSigned(i));
				System.out.println("getColumnDisplaySize("+i+"): "+met.getColumnDisplaySize(i));
				System.out.println("getColumnLabel("+i+"): "+met.getColumnLabel(i));
				System.out.println("getColumnName("+i+"): "+met.getColumnName(i));
				// beetle 5323
				System.out.println("getTableName("+i+"): "+met.getTableName(i));
				System.out.println("getSchemaName("+i+"): "+met.getSchemaName(i));
				System.out.println("getCatalogName("+i+"): "+met.getCatalogName(i));
				System.out.println("getColumnType("+i+"): "+met.getColumnType(i));
				System.out.println("getPrecision("+i+"): "+met.getPrecision(i));
				System.out.println("getScale("+i+"): "+met.getScale(i));
				System.out.println("getColumnTypeName("+i+"): "+met.getColumnTypeName(i));
				System.out.println("isReadOnly("+i+"): "+met.isReadOnly(i));
				boolean writable = met.isWritable(i);
				// JCC Supports updatable resultsets so isWritable is true
				if ((isDerbyNet && writable == true) ||
					(!isDerbyNet && writable == false))
					System.out.println("isWritable("+i+"): Expected isWritable value");
				System.out.println("isDefinitelyWritable("+i+"): "+met.isDefinitelyWritable(i));
			}

			/* Try the various get methods on each column */
			while (rs.next())
			{
				// JDBC columns use 1-based counting
				for (int i=1;i<=colCount;i++) {
					try {
						System.out.println("getBigDecimal("+i+",1): "+
											BigDecimalHandler.getBigDecimalString(rs,i));
					}
					catch (Throwable e) {
						System.out.println(
							"getBigDecimal("+i+",1) got exception ");
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						if (isDerbyNet)
							System.out.println("beetle 5328 - JCC returns incorrect scale for getBigDecimal(String,int)");
						System.out.println("getBigDecimal("+
										columnNames[i-1]+ ",1): "+
										BigDecimalHandler.getBigDecimalString(rs,columnNames[i-1],i));
					}
					catch (Throwable e) {
						System.out.println(
							"getBigDecimal("+
							columnNames[i-1]+",1) got exception ");
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getBoolean("+i+"): "+
													rs.getBoolean(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getBoolean("+i+") got exception ");
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getBoolean("+
										columnNames[i-1]+ "): "+
										rs.getBoolean(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getBoolean("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getByte("+i+"): "+
													rs.getByte(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getByte("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
	 				}

					try {
						System.out.println("getByte("+
										columnNames[i-1]+ "): "+
										rs.getByte(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getByte("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getBytes("+i+"): "+
													showBytes(rs.getBytes(i)));
					}
					catch (SQLException e) {
						System.out.println(
							"getBytes("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getBytes("+
										columnNames[i-1]+ "): "+
										showBytes(rs.getBytes(columnNames[i-1])));
					}
					catch (SQLException e) {
						System.out.println(
							"getBytes("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getDate("+i+"): "+
													rs.getDate(i));
					}
					catch (SQLException e) {
						System.out.println(
							"getDate("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getDate("+
										columnNames[i-1]+ "): "+
										rs.getDate(columnNames[i-1]));
					}
					catch (SQLException e) {
						System.out.println(
							"getDate("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getDouble("+i+"): "+
													rs.getDouble(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getDouble("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getDouble("+
										columnNames[i-1]+ "): "+
										rs.getDouble(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getDouble("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getFloat("+i+"): "+
													rs.getFloat(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getFloat("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getFloat("+
										columnNames[i-1]+ "): "+
										rs.getFloat(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getFloat("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getInt("+i+"): "+
													rs.getInt(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getInt("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getInt("+
										columnNames[i-1]+ "): "+
										rs.getInt(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getInt("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getLong("+i+"): "+
													rs.getLong(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getLong("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getLong("+
										columnNames[i-1]+ "): "+
										rs.getLong(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getLong("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						// with the bit datatypes the string output is not the same for every run,
						// so we need to mask that to prevent false test failures
						// this does not test the values returned, just whether it gives an exception.
						if (i>11)
						{
							BigDecimalHandler.getObjectString(rs,i);
							System.out.println("getObject("+i+") is ok");
						}
						else
							System.out.println("getObject("+i+"): "+
												BigDecimalHandler.getObjectString(rs,i));
					}
					catch (SQLException e) {
						System.out.println(
							"getObject("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						// with the bit datatypes the string output is not the same for every run,
						// so we need to mask that to prevent false test failures
						// this does not test the values returned, just whether it gives an exception.
						if (i>11)
						{
							BigDecimalHandler.getObjectString(rs,columnNames[i-1],i);
							System.out.println("getObject("+columnNames[i-1]+") is ok ");
						}
						else
							System.out.println("getObject("+
										columnNames[i-1]+ "): "+
										BigDecimalHandler.getObjectString(rs,columnNames[i-1],i));
					}
					catch (SQLException e) {
						System.out.println(
							"getObject("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getShort("+i+"): "+
													rs.getShort(i));
					}
					catch (Throwable e) {
						System.out.println(
							"getShort("+i+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getShort("+
										columnNames[i-1]+ "): "+
										rs.getShort(columnNames[i-1]));
					}
					catch (Throwable e) {
						System.out.println(
							"getShort("+
							columnNames[i-1]+") got exception " );
						if (e instanceof SQLException)
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getString("+i+"): "+
													rs.getString(i));
					}
					catch (SQLException e) {
						System.out.println(
							"getString("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}
					
					try {
						System.out.println("getString("+
										columnNames[i-1]+ "): "+
										rs.getString(columnNames[i-1]));
					}
					catch (SQLException e) {
						System.out.println(
							"getString("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getTime("+i+"): "+
													rs.getTime(i));
					}
					catch (SQLException e) {
						System.out.println(
							"getTime("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getTime("+
										columnNames[i-1]+ "): "+
										rs.getTime(columnNames[i-1]));
					}
					catch (SQLException e) {
						System.out.println(
							"getTime("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getTimestamp("+i+"): "+
													rs.getTimestamp(i));
					}
					catch (SQLException e) {
						System.out.println(
							"getTimestamp("+i+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}

					try {
						System.out.println("getTimestamp("+
										columnNames[i-1]+ "): "+
										rs.getTimestamp(columnNames[i-1]));
					}
					catch (SQLException e) {
						System.out.println(
							"getTimestamp("+
							columnNames[i-1]+") got exception " );
						JDBCTestDisplayUtil.ShowCommonSQLException(System.out, (SQLException)e);
					}
				}
			}

            rs.close();
            
			// Try getting a row from the closed result set
			try {
				rs.next();
				System.out.println(
					"FAIL - rs.next() allowed on closed result set.");
			}
			catch (SQLException e) {
				System.out.println(
					"rs.next() on closed result set got exception " );
				JDBCTestDisplayUtil.ShowCommonSQLException(System.out, e);
			}
			catch (Throwable e) {
				System.out.println("rs.next() didn't fail with SQLException as "+
					"expected on closed result set.  Got Throwable instead: "+e);
			}

			// Ensure commit or rollback in auto commit actually does something
			stmt.executeUpdate("create table bug4810(i int, b int)");
			stmt.executeUpdate("insert into bug4810 values (1,1), (1,2), (1,3), (1,4)");
			stmt.executeUpdate("insert into bug4810 values (1,1), (1,2), (1,3), (1,4)");
			con.commit();
			con.setAutoCommit(true);
			con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			System.out.println("just auto commit");
			showLocksForAutoCommitSelect(con, stmt, 0);
			System.out.println("commit with auto commit");
			showLocksForAutoCommitSelect(con, stmt, 1);
			System.out.println("rollback with auto commit");
			showLocksForAutoCommitSelect(con, stmt, 2);

            stmt.execute("drop table bug4810");
			con.commit();
			stmt.close();

			testMutableValues(con);
			testCorrelationNamesAndMetaDataCalls(con);
			testNullIfAndMetaDataCalls(con);
            //We know that JCC behavior does not match 
            //DerbyNetClient or embedded
            if (!TestUtil.isJCCFramework()) {
                runAutoCommitTests(con);
            }
		    // final clean up
    		cleanUp(con);
			con.close();

		}
		catch (SQLException e) {
			System.out.println("FAIL -- unexpected exception: " + e.toString());
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		// Print a report on System.out of the issues
		// found with the security checks. 
		SecurityCheck.report();
		System.out.println("Test resultset finished");
    }


	//test NULLIF(L,R) with and without parameter and check the return value's data type
	static private void testNullIfAndMetaDataCalls(Connection conn) throws Throwable
	{
		System.out.println("Tests to check metadata information of nullif column");
		tablesForTestingAllDatatypesCombinations(conn);
		testAllDatatypesCombinations(conn);
		testParameterForFirstOperandToNullIf(conn);
	}
          
	public static void testParameterForFirstOperandToNullIf( Connection conn) throws Throwable
	{
		System.out.println("Start testing first operand as parameter to nullif");
		PreparedStatement ps;
		for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
			System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+")");
			String nullIfString =
				new String("SELECT NULLIF(?," + ColumnNames[secondColumnType] +") from AllDataTypesTable");
			try {
					ps = conn.prepareStatement(nullIfString);
					switch(secondColumnType) {
						case 0:
						case 1:
						case 2:
						case 3:
						case 4:
						case 5:
						case 6:
						case 7:
							System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+") with setBoolean");
							ps.setBoolean(1, true);
							break;
						case 8: //'LONG VARCHAR'
						case 11: //'LONG VARCHAR FOR BIT DATA'
						case 12: //'CLOB'
						case 16: //'BLOB'
						//Take specific case of LONG VARCHAR. Prepare of nullif(?,long varchar)
						//fails early on because at bind time, Derby tries to set ? to
						//long varchar. But comparison between 2 long varchars is not
						//supported and hence bind code in BinaryComparisonOperatorNode fails
						//Similar thing happens for CLOB, BLOB and LONG VARCHAR FOR BIT DATA
						case 9:
						case 10:
							System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+") with setBinaryStream");
							ps.setBinaryStream(1, (java.io.InputStream)null, 1);
							break;
						case 13://DATE
							System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+") with setDate");
							ps.setDate(1, Date.valueOf("2000-01-01"));
							break;
						case 14://TIME
							System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+") with setTime");
							ps.setTime(1, Time.valueOf("15:30:20"));
							break;
						case 15://TIMESTAMP
							System.out.println("Testing nullif(?,"+SQLTypes[secondColumnType]+") with setTimestamp");
							ps.setTimestamp(1, Timestamp.valueOf("2000-01-01 15:30:20"));
							break;
						default: break;
					}
					dumpRS(ps.executeQuery());
			} catch (SQLException e)
			{
				dumpSQLExceptions(e);
			}
		}
	}

	public static void testAllDatatypesCombinations( Connection conn) throws Throwable
	{
		System.out.println("Start testing all datatypes combinations in NULLIF function");
		Statement s = conn.createStatement();
		for (int firstColumnType = 0; firstColumnType < SQLTypes.length; firstColumnType++) {
			StringBuffer nullIfString = new StringBuffer("SELECT NULLIF(" + ColumnNames[firstColumnType]);
			for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
				try {
					StringBuffer completeNullIfString = new StringBuffer(nullIfString.toString() + "," + ColumnNames[secondColumnType]);
					System.out.println(completeNullIfString + ") from AllDataTypesTable");
					dumpRS(s.executeQuery(completeNullIfString + ") from AllDataTypesTable"));
				} catch (SQLException e)
				{
					dumpSQLExceptions(e);
				}
			}
		}
	}

	public static void tablesForTestingAllDatatypesCombinations( Connection conn) throws Throwable
	{
		System.out.println("Set up by creating table for testing all datatypes combinations");

		Statement s = conn.createStatement();

		try {
			s.executeUpdate("DROP TABLE AllDataTypesTable");
		}
		catch(SQLException se) {}

		StringBuffer createSQL = new StringBuffer("create table AllDataTypesTable (");
		for (int type = 0; type < SQLTypes.length - 1; type++)
		{
			createSQL.append(ColumnNames[type] + " " + SQLTypes[type] + ",");
		}
		createSQL.append(ColumnNames[SQLTypes.length - 1] + " " + SQLTypes[SQLTypes.length - 1] + ")");
		System.out.println(createSQL);
		s.executeUpdate(createSQL.toString());

		for (int row = 0; row < SQLData[0].length; row++)
		{
			createSQL = new StringBuffer("insert into AllDataTypesTable values(");
			for (int type = 0; type < SQLTypes.length - 1; type++)
			{
				createSQL.append(SQLData[type][row] + ",");
			}
			createSQL.append(SQLData[SQLTypes.length - 1][row]+")");
			System.out.println(createSQL);
			s.executeUpdate(createSQL.toString());
		}

		s.close();
		conn.commit();
	}

	public static void dumpRS(ResultSet s) throws SQLException
	{
		SecurityCheck.inspect(s, "java.sql.ResultSet");
		
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();
		SecurityCheck.inspect(rsmd, "java.sql.ResultSetMetaData");

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0)
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuffer heading = new StringBuffer("\t ");
		StringBuffer underline = new StringBuffer("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++)
		{
			if (i > 1)
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append("COL"+i);
			heading.append("(datatype : " + rsmd.getColumnTypeName(i));
			heading.append(", precision : " + rsmd.getPrecision(i));
			heading.append(", scale : " + rsmd.getScale(i) + ")");
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());


		StringBuffer row = new StringBuffer();
		// Display data, fetching until end of the result set
		while (s.next())
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++)
			{
				if (i > 1) row.append(",");
				try{
				row.append(s.getString(i));
				} catch(SQLException ex){
					if (ex.getSQLState().equals("22005")) {
						if (s.getBytes(i) != null)
							row.append(s.getBytes(i).toString());
                //row.append(new String(s.getBytes(i)));
						else
                row.append(s.getBytes(i));
					} else throw ex;
				}
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}


	static private void testCorrelationNamesAndMetaDataCalls(Connection conn) throws Exception
	{
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create table s (a int, b int, c int, d int, e int, f int)");
		stmt.executeUpdate("insert into s values (0,1,2,3,4,5)");
		stmt.executeUpdate("insert into s values (10,11,12,13,14,15)");
		System.out.println("Run select * from s ss (f, e, d, c, b, a) where f = 0 and then try getTableName and getSchemaName on columns");
		ResultSet rs = stmt.executeQuery("select * from s ss (f, e, d, c, b, a) where f = 0");
    rs.next();
    ResultSetMetaData met = rs.getMetaData();
		System.out.println("getTableName(1): "+met.getTableName(1));
		System.out.println("getSchemaName(1): "+met.getSchemaName(1));

		System.out.println("Run select * from (select * from s) a and then try getTableName and getSchemaName on columns");
		rs = stmt.executeQuery("select * from (select * from s) a");
    rs.next();
    met = rs.getMetaData();
		System.out.println("getTableName(1): "+met.getTableName(1));
		System.out.println("getSchemaName(1): "+met.getSchemaName(1));

		stmt.executeUpdate("create schema s1");
		stmt.executeUpdate("create table s1.t1 (c11 int, c12 int)");
		stmt.executeUpdate("insert into s1.t1 values (11, 12), (21, 22)");
		System.out.println("Run select * from s1.t1 as abc and then try getTableName and getSchemaName on columns");
		rs = stmt.executeQuery("select * from s1.t1 as abc");
		met = rs.getMetaData();
		System.out.println("Table name of first column is " + met.getTableName(1));
		System.out.println("Schema name of first column is " + met.getSchemaName(1));
		System.out.println("Table name of second column is " + met.getTableName(2));
		System.out.println("Schema name of second column is " + met.getSchemaName(2));
		System.out.println("Run select abc.c11 from s1.t1 as abc and then try getTableName and getSchemaName on columns");
		rs = stmt.executeQuery("select abc.c11 from s1.t1 as abc");
		met = rs.getMetaData();
		System.out.println("Table name of first column is " + met.getTableName(1));
		System.out.println("Schema name of first column is " + met.getSchemaName(1));
		System.out.println("Run select bcd.a, abc.c11 from s1.t1 as abc, s as bcd and then try getTableName and getSchemaName on columns");
		rs = stmt.executeQuery("select bcd.a, abc.c11 from s1.t1 as abc, s as bcd");
		met = rs.getMetaData();
		System.out.println("Table name of first column is " + met.getTableName(1));
		System.out.println("Schema name of first column is " + met.getSchemaName(1));
		System.out.println("Table name of second column is " + met.getTableName(2));
		System.out.println("Schema name of second column is " + met.getSchemaName(2));

		stmt.executeUpdate("create schema app1");
		stmt.executeUpdate("create table app1.t1 (c11 int, c12 int)");
		stmt.executeUpdate("insert into app1.t1 values (11, 12), (21, 22)");
		stmt.executeUpdate("create schema app2");
		stmt.executeUpdate("create table app2.t1 (c11 int, c12 int)");
		stmt.executeUpdate("insert into app2.t1 values (11, 12), (21, 22)");
		System.out.println("Run select app1.t1.c11, app2.t1.c11 from app1.t1, app2.t1 and then try getTableName and getSchemaName on columns");
		rs = stmt.executeQuery("select app1.t1.c11, app2.t1.c11 from app1.t1, app2.t1");
		met = rs.getMetaData();
		System.out.println("Table name of first column is " + met.getTableName(1));
		System.out.println("Schema name of first column is " + met.getSchemaName(1));
		System.out.println("Table name of second column is " + met.getTableName(2));
		System.out.println("Schema name of second column is " + met.getSchemaName(2));
        stmt.execute("drop table s");
        stmt.execute("drop table s1.t1");
        stmt.execute("drop schema s1 restrict");
        stmt.execute("drop table app1.t1");
        stmt.execute("drop table app2.t1");
        stmt.execute("drop schema app2 restrict");
        stmt.execute("drop schema app1 restrict");
	}

	static private void doTheTests() throws Exception
	{

	}

	static private void showLocksForAutoCommitSelect(Connection conn, Statement stmt, int action) throws Exception {

		ResultSet rs = stmt.executeQuery("select i,b from bug4810");
		rs.next();
		System.out.println("  bug4810 " + rs.getInt(1) + ", " + rs.getInt(2));
		rs.next();
		System.out.println("  bug4810 " + rs.getInt(1) + ", " + rs.getInt(2));

		if (action == 1) {
			System.out.println("commit");
			conn.commit();
		} else if (action == 2) {
			System.out.println("rollback");
			conn.rollback();
		}

		showLocks();

		try {

			rs.next();
			System.out.println("  bug4810 " + rs.getInt(1) + ", " + rs.getInt(2));
		} catch (SQLException sqle) {
			JDBCTestDisplayUtil.ShowCommonSQLException(System.out, sqle);		}
		showLocks();
		rs.close();

		showLocks();

	}

	private static void showLocks() throws Exception {
		System.out.println("  LOCK TABLE");
		Connection con2 = ij.startJBMS();
		PreparedStatement ps2 = con2.prepareStatement("select XID, count(*) from SYSCS_DIAG.LOCK_TABLE as L group by XID");
		ResultSet rs2 = ps2.executeQuery();

		while (rs2.next()) {
			if (rs2.getInt(2) > 0) {
				System.out.println("Locks are held");
			} else if (rs2.getInt(2) == 0) {
				System.out.println("No locks to hold");
			}
		}
		
		rs2.close();
		ps2.close();
		con2.close();
	}

	static private void dumpSQLExceptions (SQLException se) {
		while (se != null) {
			JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);
			se = se.getNextException();
		}
	}

	static private String showBytes(byte[] bytes) {
		if (bytes == null)
			return "null";

		StringBuffer s = new StringBuffer("0x");
		s.ensureCapacity(2+2*bytes.length);
		for (int i=0;i<bytes.length;i++) {
			int hi = (bytes[i] & 0xf0) >>> 4;
			int lo = (bytes[i] & 0x0f);
			s.append(representation[hi]);
			s.append(representation[lo]);
		}
		return s.toString();
	}

	static final char[] representation =
		{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		  'A', 'B', 'C', 'D', 'E', 'F' } ;


	/**
		Test that for mutable types returned from a ResultSet we do not
		re-use the type, thus conusing any application that holds onto
		the returned value. Possible mutable types are

		byte[]
		java.sql.Date
		java.sql.Timestamp
		java.sql.Time

		The stream types are mutable but they are closed once the appliction
		moves to the next column or row.
	*/
	private static void testMutableValues(Connection conn) throws SQLException 
	{
		System.out.println("START testMutableValues");

		Statement s = conn.createStatement();

		s.execute("CREATE TABLE MUTABLE.T1(C CHAR(10) FOR BIT DATA, V VARCHAR(10) FOR BIT DATA, L LONG VARCHAR FOR BIT DATA, D DATE, T TIME, TS TIMESTAMP)");
		s.execute("INSERT INTO MUTABLE.T1 VALUES (X'34', X'4de5', X'5e3a67', '1992-01-01', '17.05.00', '2003-3-1-17.05.43.123456')");
		s.execute("INSERT INTO MUTABLE.T1 VALUES (X'93', X'4825', X'6e3a64', '1992-01-03', '17.06.00', '2007-3-1-17.05.43.123456')");
		s.execute("INSERT INTO MUTABLE.T1 VALUES (X'34', X'4de5', X'5e3a67', '1992-01-01', '17.05.00', '2003-3-1-17.05.43.123456')");

		{
		ResultSet rs = s.executeQuery("SELECT C,V,L,D,T,TS FROM MUTABLE.T1");
		java.util.ArrayList[] values = new java.util.ArrayList[6];
		for (int i = 0; i < values.length; i++) {
			values[i] = new java.util.ArrayList();
		}
		System.out.println("CHECKING on getXXX()");
		int rc = 0;
		while (rs.next()) {
			rc++;
			System.out.println("ROW " + rc);

			checkMutableValue(values[0], 1, rs.getBytes(1));
			checkMutableValue(values[1], 2, rs.getBytes(2));
			checkMutableValue(values[2], 3, rs.getBytes(3));

			checkMutableValue(values[3], 4, rs.getDate(4));
			checkMutableValue(values[4], 5, rs.getTime(5));
			checkMutableValue(values[5], 6, rs.getTimestamp(6));

		}
		rs.close();
		}
		{
		ResultSet rs = s.executeQuery("SELECT C,V,L,D,T,TS FROM MUTABLE.T1");
		java.util.ArrayList[] values = new java.util.ArrayList[6];
		for (int i = 0; i < values.length; i++) {
			values[i] = new java.util.ArrayList();
		}
		System.out.println("CHECKING on getObject()");
		int rc = 0;
		while (rs.next()) {
			rc++;
			System.out.println("ROW " + rc);

			for (int i = 0; i < 6; i++)
				checkMutableValue(values[i], i+1, rs.getObject(i+1));
		}
		rs.close();
		}

		s.execute("DROP TABLE MUTABLE.T1");

		System.out.println("COMPLETE testMutableValues");
	}

	private static void checkMutableValue(java.util.ArrayList list, int col, Object value) {

		int same = -1;
		int equals = -1;
		for (int i = 0; i < list.size(); i++) {
			Object previous = list.get(i);
			if (previous == value)
				same = i+1;
			if (previous.equals(value))
				equals = i+1;
		}

		if (same != -1)
			System.out.println("FAIL SAME OBJECT RETURNED column " + col + " existing " + same);
		if (equals != -1)
			System.out.println("OK EQUALITY OBJECT RETURNED column " + col + " existing " + equals);

		list.add(value);
	}
    
    /**
     * Helper method to set up and run the auto-commit tests.
     * 
     * @param conn The Connection
     * @throws SQLException
     */
    private static void runAutoCommitTests(Connection conn) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select tablename from sys.systables " +
                "where tablename = 'AUTOCOMMITTABLE'");
        if (rs.next()) {
            rs.close();
            s.executeUpdate("delete from AutoCommitTable");
        } else {
            rs.close();
            s.executeUpdate("create table AutoCommitTable (num int)");
        }
        s.executeUpdate("insert into AutoCommitTable values (1)");
        s.executeUpdate("insert into AutoCommitTable values (2)");
        int isolation = conn.getTransactionIsolation();
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        testSingleRSAutoCommit(conn);
        testSingleRSCloseCursorsAtCommit(conn);
        conn.setTransactionIsolation(isolation);
        s.executeUpdate("drop table AutoCommitTable");
        s.close();
    }
    
    /**
     * Tests for two things:
     * 
     * 1) The ResultSet does not close implicitly when the ResultSet completes 
     * and holdability == HOLD_CURSORS_OVER_COMMIT
     * 
     * 2) The ResultSet auto-commits when it completes and auto-commit is on. 
     * 
     * @param conn The Connection
     * @param tableName
     * @throws SQLException
     */
    private static void testSingleRSAutoCommit(Connection conn) throws SQLException {
        setHoldability(conn, JDBC30Translation.HOLD_CURSORS_OVER_COMMIT);
        System.out.print("Single RS auto-commit test: ");
        Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery("select * from AutoCommitTable");
        while (rs.next());
        if (!checkLocks()) {
            System.out.println("FAIL. Auto-commit unsuccessful.");
            rs.close();
            return;
        }
        try {
            if (!rs.next()) {
                System.out.println("PASS.");
            } else {
                System.out.println("FAIL. Final call of the ResultSet should return false");
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println("FAIL. Final call to ResultSet.next() threw an Exception: ");
            e.printStackTrace();
        }
    }
    
    /**
     * Check to see that ResultSet closes implicitly when holdability is set to
     * CLOSE_CURORS_AT_COMMIT.
     * 
     * @param conn The Connection
     * @throws SQLException
     */
    private static void testSingleRSCloseCursorsAtCommit(Connection conn) throws SQLException {
        setHoldability(conn, JDBC30Translation.CLOSE_CURSORS_AT_COMMIT);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        System.out.print("SingleRSCloseCursorsAtCommit: ");
        Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery("select * from AutoCommitTable");
        while (rs.next());
        if (!checkLocks()) {
            System.out.println("FAIL. Auto-commit unsuccessful.");
            rs.close();
            return;
        }
        try {
            rs.next();
            System.out.println("FAIL. ResultSet not closed implicitly");
            rs.close();
        } catch (SQLException e) {
            System.out.println("PASS.");
        }
    }
    
    
    /**
     * Checks to see if there is a lock on a table by attempting to modify the
     * same table. If the first connection was serializable then it will 
     * continue to hold a lock and the second Connection will time out.
     * 
     * @return false if the a lock could not be established, true if a lock
     * can be established.
     * @throws SQLException
     */
    private static boolean checkLocks() throws SQLException {
        Connection conn = null;
        try {
            conn = ij.startJBMS();
        } catch (Exception e) {
            System.out.println("FAIL. Unable to establish connection in checkLocks");
            return false;
        }
        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("update AutoCommitTable " 
                    + "set num = 3 where num = 2");
            stmt.executeUpdate("update AutoCommitTable " 
                    + "set num = 2 where num = 3");
        } catch (SQLException e) {
            if (e.getSQLState().equals(SQLState.LOCK_TIMEOUT)) {
                return false;
            } else {
                throw e;
            }
        }
        stmt.close();
        conn.close();
        return true;
    }
    
    /**
     * Sets the holdability of a Connection using reflection so it is
     * JDBC2.0 compatible.
     * 
     * @param conn The Connection
     * @param hold The new holdability.
     * @throws SQLException
     */
    public static void setHoldability(Connection conn, int hold) throws SQLException {
        try {
            Object[] holdArray = {new Integer(hold)};
            Method sh = conn.getClass().getMethod("setHoldability", CONN_PARAM);
            sh.invoke(conn, holdArray);
        } catch (Exception e) {System.out.println("shouldn't get that error " + e.getMessage());}//for jdks prior to jdk14
    }

    private static void cleanUp(Connection conn) throws SQLException {
	Statement s = conn.createStatement();
	String[] testObjects = {"TABLE APP.T"};
        try {
		TestUtil.cleanUpTest(s, testObjects);
        } catch (SQLException se){
        }
    }

}

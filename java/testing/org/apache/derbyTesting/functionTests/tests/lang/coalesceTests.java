/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.coalesceTests

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

import java.io.*;
import java.sql.*;

import org.apache.derby.tools.ij;

/**
 * Coalesce/Value tests for various datatypes
 * coalesce/value function takes arguments and returns the first argument that is not null.
 * The arguments are evaluated in the order in which they are specified, and the result of the
 * function is the first argument that is not null. The result can be null only if all the arguments
 * can be null. The selected argument is converted, if necessary, to the attributes of the result.
 */
public class coalesceTests
{

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
		{NULL_VALUE,"11","111",NULL_VALUE},       // INTEGER
		{NULL_VALUE,"22","222","3333"},       // BIGINT
		{NULL_VALUE,"3.3","3.33",NULL_VALUE},      // DECIMAL(10,5)
		{NULL_VALUE,"4.4","4.44","4.444"},      // REAL,
		{NULL_VALUE,"5.5","5.55",NULL_VALUE},      // DOUBLE
		{NULL_VALUE,"'1992-01-06'","'1992-01-16'",NULL_VALUE},      // CHAR(60)
		{NULL_VALUE,"'1992-01-07'","'1992-01-17'",VALID_TIME_STRING},      //VARCHAR(60)",
		{NULL_VALUE,"'1992-01-08'","'1992-01-18'",VALID_TIMESTAMP_STRING},      // LONG VARCHAR
		{NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaaa'"},  // CHAR(60)  FOR BIT DATA
		{NULL_VALUE,"X'10bb'",NULL_VALUE,"X'10bbbb'"},  // VARCHAR(60) FOR BIT DATA
		{NULL_VALUE,"X'10cc'",NULL_VALUE,"X'10cccc'"},  //LONG VARCHAR FOR BIT DATA
		{NULL_VALUE,"'13'","'14'",NULL_VALUE},     //CLOB(1k)
		{NULL_VALUE,VALID_DATE_STRING,VALID_DATE_STRING,NULL_VALUE},        // DATE
		{NULL_VALUE,VALID_TIME_STRING,VALID_TIME_STRING,NULL_VALUE},        // TIME
		{NULL_VALUE,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING,NULL_VALUE},   // TIMESTAMP
		{NULL_VALUE,NULL_VALUE,NULL_VALUE,NULL_VALUE}                 // BLOB
	};

	/**
	   SQL Reference Guide for DB2 has section titled "Rules for result data types" at the following url
	   http://publib.boulder.ibm.com/infocenter/db2help/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0008480.htm

	   I have constructed following table based on various tables and information under "Rules for result data types"
	   This table has FOR BIT DATA TYPES broken out into separate columns for clarity and testing
	**/


	public static final String[][]  resultDataTypeRulesTable = {

  // Types.             S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
	//                    M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
	//                    A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
  //                    L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
  //                    L  G  N  M     L     H  V  .  H  V           S
  //                    I  E  T  A     E     A  A  B  A  A           T
  //                    N  R     L           R  R  I  R  R           A
	//                    T                       C  T  .  .           M
  //                                            H     B  B           P
	//                                            A     I  I
	//                                            R     T   T
/* 0 SMALLINT */        { "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 1 INTEGER  */        { "INTEGER", "INTEGER", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 2 BIGINT   */        { "BIGINT", "BIGINT", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 3 DECIMAL  */        { "DECIMAL", "DECIMAL", "DECIMAL", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 4 REAL     */        { "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "REAL", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 5 DOUBLE   */        { "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 6 CHAR     */        { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CHAR", "VARCHAR", "LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "DATE", "TIME", "TIMESTAMP", "ERROR" },
/* 7 VARCHAR  */        { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "VARCHAR", "VARCHAR","LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "DATE", "TIME", "TIMESTAMP", "ERROR" },
/* 8 LONGVARCHAR */     { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "LONG VARCHAR", "LONG VARCHAR", "LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 9 CHAR FOR BIT */    { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CHAR () FOR BIT DATA", "VARCHAR () FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 10 VARCH. BIT   */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "VARCHAR () FOR BIT DATA", "VARCHAR () FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 11 LONGVAR. BIT */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "LONG VARCHAR FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 12 CLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CLOB", "CLOB", "CLOB", "ERROR", "ERROR", "ERROR", "CLOB", "ERROR", "ERROR", "ERROR", "ERROR" },
/* 13 DATE         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "DATE", "DATE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "DATE", "ERROR", "ERROR", "ERROR" },
/* 14 TIME         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIME", "TIME", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIME", "ERROR", "ERROR" },
/* 15 TIMESTAMP    */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIMESTAMP", "TIMESTAMP", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIMESTAMP", "ERROR" },
/* 16 BLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "BLOB" },

	};                                                                         

	public static void main (String[] argv) throws Throwable
	{
		boolean isDB2=false;
		String framework = System.getProperty("framework");
		if (framework != null && framework.toUpperCase().equals("DB2JCC"))
			isDB2 = true;

		ij.getPropertyArg(argv);
		Connection conn = ij.startJBMS();

		testCoalesceSyntax(conn);

		tablesForTestingAllDatatypesCombinations(conn);
		testCompatibleDatatypesCombinations(conn);
		testAllDatatypesCombinations(conn);

		testDateCoalesce(conn);
		testTimeCoalesce(conn);
		testTimeStampCoalesce(conn);
		testNumericCoalesce(conn);
		testMiscellaneousCoalesce(conn);
		testCharCoalesce(conn);
		testCharForBitDataCoalesce(conn);
	}

	public static void testCoalesceSyntax( Connection conn) throws Throwable
	{
    try {
			System.out.println("TestA - some syntax testing for Coalesce/Value function");

			PreparedStatement ps;
			Statement s = conn.createStatement();
			try {
			s.executeUpdate("drop table tA");
			} catch(Exception ex) {}
			s.executeUpdate("create table tA (c1 int, c2 char(254))");
			s.executeUpdate("insert into tA (c1) values(1)");

			System.out.println("TestAla - select coalesce from tA will give error because no arguments were supplied to the function");
			try {
				s.executeQuery("select coalesce from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X04"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestAlb - select value from tA will give error because no arguments were supplied to the function");
			try {
				s.executeQuery("select value from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X04"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA2a - select coalesce from tA will give error because no arguments were supplied inside the parentheses");
			try {
				s.executeQuery("select coalesce() from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X01"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA2b - select value from tA will give error because no arguments were supplied inside the parentheses");
			try {
				s.executeQuery("select value() from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X01"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA3a - select coalesce from tA with only one argument will give error");
			try {
				s.executeQuery("select coalesce(c1) from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42605"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA3b - select value from tA with only one argument will give error");
			try {
				s.executeQuery("select value(c1) from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42605"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA4a - select coalesce from tA with incorrect column name will give error");
			try {
				s.executeQuery("select coalesce(c111) from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X04"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA4b - select value from tA with incorrect column name will give error");
			try {
				s.executeQuery("select value(c111) from tA");
				System.out.println("FAIL - should have gotten error for incorrect syntax");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42X04"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA5a - create table with table name as coalesce and column name as coalesce will pass because coalesce is not a reserved-word");
			s.executeUpdate("create table coalesce (coalesce int, c12 int)");
			s.executeUpdate("insert into coalesce(coalesce) values(null)");
			s.executeUpdate("insert into coalesce values(null,1)");
			dumpRS(s.executeQuery("select coalesce(coalesce,c12) from coalesce"));
			s.executeUpdate("drop table coalesce");

			System.out.println("TestA5b - create table with table name as value and column name as value will pass because value is not a reserved-word");
			s.executeUpdate("create table value (value int, c12 int)");
			s.executeUpdate("insert into value(value) values(null)");
			s.executeUpdate("insert into value values(null,1)");
			dumpRS(s.executeQuery("select coalesce(value,c12) from value"));
			s.executeUpdate("drop table value");

			System.out.println("TestA6a - All arguments to coalesce function passed as parameters is an error");
			try {
				ps = conn.prepareStatement("select coalesce(?,?) from tA");
				System.out.println("FAIL - should have gotten error for using parameters for all the arguments");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42610"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestA6b - All arguments to value function passed as parameters is an error");
			try {
				ps = conn.prepareStatement("select value(?,?) from tA");
				System.out.println("FAIL - should have gotten error for using parameters for all the arguments");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42610"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table tA");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
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

	public static void testAllDatatypesCombinations( Connection conn) throws Throwable
	{
		System.out.println("Start testing all datatypes combinations in COALESCE/VALUE function");

		Statement s = conn.createStatement();

		// Try COALESCE with 2 datatype combinations at a time
		for (int firstColumnType = 0; firstColumnType < SQLTypes.length; firstColumnType++) {
			for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
				try {
					String coalesceString = "SELECT COALESCE(" + ColumnNames[firstColumnType] + "," + ColumnNames[secondColumnType] + ") from AllDataTypesTable";
					System.out.println(coalesceString);
					printExpectedResultDataType(firstColumnType,secondColumnType);
					dumpRS(s.executeQuery(coalesceString));
					isSupportedCoalesce(firstColumnType,secondColumnType, true);
				} catch (SQLException e)
				{
					if (e.getSQLState().equals("22007"))
						System.out.println("expected exception because char value does not match a time/timestamp format " + e.getMessage());
					else if (!isSupportedCoalesce(firstColumnType,secondColumnType, false)  && e.getSQLState().equals("42815"))
						System.out.println("expected exception " + e.getMessage());
					else
						dumpSQLExceptions(e);
				}
				try {
					String valueString = "SELECT VALUE(" + ColumnNames[firstColumnType] + "," + ColumnNames[secondColumnType] + ") from AllDataTypesTable";
					System.out.println(valueString);
					printExpectedResultDataType(firstColumnType,secondColumnType);
					dumpRS(s.executeQuery(valueString));
					isSupportedCoalesce(firstColumnType,secondColumnType, true);
				} catch (SQLException e)
				{
					if (e.getSQLState().equals("22007"))
						System.out.println("expected exception because char value does not match a time/timestamp format " + e.getMessage());
					else if (!isSupportedCoalesce(firstColumnType,secondColumnType, false)  && e.getSQLState().equals("42815"))
						System.out.println("expected exception " + e.getMessage());
					else
						dumpSQLExceptions(e);
				}
			}
		}
	}

	public static void testCompatibleDatatypesCombinations( Connection conn) throws Throwable
	{
		System.out.println("Start testing all compatible datatypes combinations in COALESCE/VALUE function");

		Statement s = conn.createStatement();

		for (int firstColumnType = 0; firstColumnType < SQLTypes.length; firstColumnType++) {
			StringBuffer coalesceString = new StringBuffer("SELECT COALESCE(" + ColumnNames[firstColumnType]);
			for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
				try {
					if (resultDataTypeRulesTable[firstColumnType][secondColumnType].equals("ERROR"))
						continue; //the datatypes are incompatible, don't try them in COALESCE/VALUE
					coalesceString.append("," + ColumnNames[secondColumnType]);
					System.out.println(coalesceString + ") from AllDataTypesTable");
					dumpRS(s.executeQuery(coalesceString + ") from AllDataTypesTable"));
				} catch (SQLException e)
				{
					if (e.getSQLState().equals("22007"))
						System.out.println("expected exception because char value does not match a time/timestamp format " + e.getMessage());
					else if (isClobWithCharAndDateTypeArguments(coalesceString.toString())  && e.getSQLState().equals("42815"))
						System.out.println("expected exception because mixing CLOB and DATA/TIME/TIMESTAMP arugments " + e.getMessage());
					else if (!isSupportedCoalesce(firstColumnType,secondColumnType, false)  && e.getSQLState().equals("42815"))
						System.out.println("expected exception " + e.getMessage());
					else
						dumpSQLExceptions(e);
				}
			}
		}
	}

	private static void printExpectedResultDataType(int oneType, int anotherType)
	{
		String coalesceDescription;
		if (resultDataTypeRulesTable[oneType][anotherType].equals("ERROR"))
		{
			coalesceDescription = " Operands " +
			SQLTypes[oneType] +
			" , " + SQLTypes[anotherType] + " are incompatible for Coalesce/Value function";
		}
		else
		{
			coalesceDescription = " Coalesc/Value with operands " +
			SQLTypes[oneType] +
			" , " + SQLTypes[anotherType] + " will have result data type of " + resultDataTypeRulesTable[oneType][anotherType];
		}
		System.out.println(coalesceDescription);
	}

	public static boolean isClobWithCharAndDateTypeArguments(String coalesceString) throws Throwable
	{
		if(coalesceString.indexOf("CLOB") != -1)
		{
			if(coalesceString.indexOf("CHAR") != -1 && (coalesceString.indexOf("DATE") != -1 || coalesceString.indexOf("TIME") != -1))
					return true;
		}
		return false;
	}

	private static boolean isSupportedCoalesce(int oneType, int anotherType, boolean throwError)
	{
		String coalesceDescription = " Coalesc/Value with operands " +
			SQLTypes[oneType] +
			" , " + SQLTypes[anotherType];

		if (throwError && resultDataTypeRulesTable[oneType][anotherType].equals("ERROR"))
				System.out.println("FAIL:" +  coalesceDescription +
							   " should not be supported");

		return (!(resultDataTypeRulesTable[oneType][anotherType].equals("ERROR")));
	}

	public static void testMiscellaneousCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tD");
    } catch(Exception ex) {}
			s.executeUpdate("create table tD (c1 int, c2 char(254))");
			s.executeUpdate("insert into tD (c1,c2) values(1,'abcdefgh')");
			s.executeUpdate("insert into tD (c1) values(2)");

			System.out.println("TestD - some miscellaneous testing for Coalesce/Value function");

			System.out.println("TestD1a - test coalesce function in values clause");
			dumpRS(s.executeQuery("values coalesce(cast('asdfghj' as char(30)),cast('asdf' as char(50)))"));

			System.out.println("TestD1b - test value function in values clause");
			dumpRS(s.executeQuery("values value(cast('asdfghj' as char(30)),cast('asdf' as char(50)))"));

			System.out.println("TestD2a - First argument to coalesce function passed as parameter with non-null value");
			ps = conn.prepareStatement("select coalesce(?,c2) from tD");
			ps.setString(1,"first argument to coalesce");
			dumpRS(ps.executeQuery());

			System.out.println("TestD2b - First argument to value function passed as parameter with non-null value");
			ps = conn.prepareStatement("select value(?,c2) from tD");
			ps.setString(1,"first argument to value");
			dumpRS(ps.executeQuery());

			System.out.println("TestD3a - First argument to coalesce function passed as parameter with null value");
			ps = conn.prepareStatement("select coalesce(?,c2) from tD");
			ps.setNull(1,Types.CHAR);
			dumpRS(ps.executeQuery());

			System.out.println("TestD3b - First argument to value function passed as parameter with null value");
			ps = conn.prepareStatement("select value(?,c2) from tD");
			ps.setNull(1,Types.BIGINT);
			dumpRS(ps.executeQuery());

			System.out.println("TestD4a - Pass incompatible value for parameter to coalesce function");
			ps = conn.prepareStatement("select coalesce(c1,?) from tD");
			try {
				ps.setString(1,"abc");
				dumpRS(ps.executeQuery());
				System.out.println("FAIL - should have gotten error because result type is int and we are trying to pass a parameter of type char");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("22018"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table tD");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testDateCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tF");
    } catch(Exception ex) {}
			s.executeUpdate("create table tF (dateCol date, charCol char(10), varcharCol varchar(50))");
			s.executeUpdate("insert into tF values(null, null, null)");
			s.executeUpdate("insert into tF values(date('1992-01-02'), '1992-01-03', '1992-01-04')");

			System.out.println("TestF - focus on date datatypes");
			System.out.println("TestF1a - coalesce(dateCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(dateCol,dateCol) from tF"));

			System.out.println("TestF1b - value(dateCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select value(dateCol,dateCol) from tF"));

			System.out.println("TestF2a - coalesce(dateCol,charCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(dateCol,charCol) from tF"));

			System.out.println("TestF2b - value(dateCol,charCol)");
			dumpRSwithScale(s.executeQuery("select value(dateCol,charCol) from tF"));

			System.out.println("TestF3a - coalesce(charCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,dateCol) from tF"));

			System.out.println("TestF3b - value(charCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,dateCol) from tF"));

			System.out.println("TestF4a - coalesce(dateCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(dateCol,charCol) from tF"));

			System.out.println("TestF4b - value(dateCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select value(dateCol,charCol) from tF"));

			System.out.println("TestF5a - coalesce(varcharCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,dateCol) from tF"));

			System.out.println("TestF5b - value(varcharCol,dateCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,dateCol) from tF"));

			System.out.println("TestF - Try invalid string representation of date into chars and varchars and then use them in coalesce function with date datatype");
			s.executeUpdate("insert into tF values(date('1992-01-01'), 'I am char', 'I am varchar')");

			try {
				System.out.println("TestF6a - coalesce(charCol,dateCol) will fail because one row has invalid string representation of date in the char column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,dateCol) from tF"));
				System.out.println("TestF6a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestF6b - value(charCol,dateCol) will fail because one row has invalid string representation of date in the char column");
				dumpRSwithScale(s.executeQuery("select value(charCol,dateCol) from tF"));
				System.out.println("TestF6b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestF7a - coalesce(varcharCol,dateCol) will fail because one row has invalid string representation of date in the varchar column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,dateCol) from tF"));
				System.out.println("TestF7a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestF7b - value(varcharCol,dateCol) will fail because one row has invalid string representation of date in the varchar column");
				dumpRSwithScale(s.executeQuery("select value(charCol,dateCol) from tF"));
				System.out.println("TestF7b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table tF");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testTimeStampCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tH");
    } catch(Exception ex) {}
			s.executeUpdate("create table tH (timestampCol timestamp, charCol char(19), varcharCol varchar(50))");
			s.executeUpdate("insert into tH values(null, null, null)");
			s.executeUpdate("insert into tH values(timestamp('1992-01-01 12:30:30'), '1992-01-01 12:30:31', '1992-01-01 12:30:32')");

			System.out.println("TestH - focus on timestamp datatypes");
			System.out.println("TestH1a - coalesce(timestampCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timestampCol,timestampCol) from tH"));

			System.out.println("TestH1b - value(timestampCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select value(timestampCol,timestampCol) from tH"));

			System.out.println("TestH2a - coalesce(timestampCol,charCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timestampCol,charCol) from tH"));

			System.out.println("TestH2b - value(timestampCol,charCol)");
			dumpRSwithScale(s.executeQuery("select value(timestampCol,charCol) from tH"));

			System.out.println("TestH3a - coalesce(charCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,timestampCol) from tH"));

			System.out.println("TestH3b - value(charCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,timestampCol) from tH"));

			System.out.println("TestH4a - coalesce(timestampCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timestampCol,charCol) from tH"));

			System.out.println("TestH4b - value(timestampCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select value(timestampCol,charCol) from tH"));

			System.out.println("TestH5a - coalesce(varcharCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,timestampCol) from tH"));

			System.out.println("TestH5b - value(varcharCol,timestampCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,timestampCol) from tH"));

			System.out.println("TestH - Try invalid string representation of timestamp into chars and varchars and then use them in coalesce function with timestamp datatype");
			s.executeUpdate("insert into tH values(timestamp('1992-01-01 12:30:33'), 'I am char', 'I am varchar')");

			try {
				System.out.println("TestH6a - coalesce(charCol,timestampCol) will fail because one row has invalid string representation of timestamp in the char column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,timestampCol) from tH"));
				System.out.println("TestH6a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestH6b - value(charCol,timestampCol) will fail because one row has invalid string representation of timestamp in the char column");
				dumpRSwithScale(s.executeQuery("select value(charCol,timestampCol) from tH"));
				System.out.println("TestH6b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestH7a - coalesce(varcharCol,timestampCol) will fail because one row has invalid string representation of timestamp in the varchar column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,timestampCol) from tH"));
				System.out.println("TestH7a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestH7b - value(varcharCol,timestampCol) will fail because one row has invalid string representation of timestamp in the varchar column");
				dumpRSwithScale(s.executeQuery("select value(charCol,timestampCol) from tH"));
				System.out.println("TestH7b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table tH");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testTimeCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tG");
    } catch(Exception ex) {}
			s.executeUpdate("create table tG (timeCol time, charCol char(10), varcharCol varchar(50))");
			s.executeUpdate("insert into tG values(null, null, null)");
			s.executeUpdate("insert into tG values(time('12:30:30'), '12:30:31', '12:30:32')");

			System.out.println("TestG - focus on time datatypes");
			System.out.println("TestG1a - coalesce(timeCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,timeCol) from tG"));

			System.out.println("TestG1b - value(timeCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select value(timeCol,timeCol) from tG"));

			System.out.println("TestG2a - coalesce(timeCol,charCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,charCol) from tG"));

			System.out.println("TestG2b - value(timeCol,charCol)");
			dumpRSwithScale(s.executeQuery("select value(timeCol,charCol) from tG"));

			System.out.println("TestG3a - coalesce(charCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,timeCol) from tG"));

			System.out.println("TestG3b - value(charCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,timeCol) from tG"));

			System.out.println("TestG4a - coalesce(timeCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,charCol) from tG"));

			System.out.println("TestG4b - value(timeCol,varcharCol)");
			dumpRSwithScale(s.executeQuery("select value(timeCol,charCol) from tG"));

			System.out.println("TestG5a - coalesce(varcharCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(charCol,timeCol) from tG"));

			System.out.println("TestG5b - value(varcharCol,timeCol)");
			dumpRSwithScale(s.executeQuery("select value(charCol,timeCol) from tG"));

			System.out.println("TestG - Try invalid string representation of time into chars and varchars and then use them in coalesce function with time datatype");
			s.executeUpdate("insert into tG values(time('12:30:33'), 'I am char', 'I am varchar')");

			try {
				System.out.println("TestG6a - coalesce(charCol,timeCol) will fail because one row has invalid string representation of time in the char column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,timeCol) from tG"));
				System.out.println("TestG6a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestG6b - value(charCol,timeCol) will fail because one row has invalid string representation of time in the char column");
				dumpRSwithScale(s.executeQuery("select value(charCol,timeCol) from tG"));
				System.out.println("TestG6b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestG7a - coalesce(varcharCol,timeCol) will fail because one row has invalid string representation of time in the varchar column");
				dumpRSwithScale(s.executeQuery("select coalesce(charCol,timeCol) from tG"));
				System.out.println("TestG7a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestG7b - value(varcharCol,timeCol) will fail because one row has invalid string representation of time in the varchar column");
				dumpRSwithScale(s.executeQuery("select value(charCol,timeCol) from tG"));
				System.out.println("TestG7b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("22007"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestG - Following will work fine with invalid string representation of time because timeCol is not null and hence we don't look at invalid time string in char/varchar columns");
			System.out.println("TestG8a - coalesce(timeCol,charCol) will pass because timeCol is non-null for all rows in table TG and hence we don't look at charCol's invalid time string");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,charCol) from tG"));

			System.out.println("TestG8b - value(timeCol,charCol) will pass because timeCol is non-null for all rows in table TG and hence we don't look at charCol's invalid time string");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,charCol) from tG"));

			System.out.println("TestG9a - coalesce(timeCol,varcharCol) will pass because timeCol is non-null for all rows in table TG and hence we don't look at varcharCol's invalid time string");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,varcharCol) from tG"));

			System.out.println("TestG9b - value(timeCol,varcharCol) will pass because timeCol is non-null for all rows in table TG and hence we don't look at varcharCol's invalid time string");
			dumpRSwithScale(s.executeQuery("select coalesce(timeCol,varcharCol) from tG"));

			s.executeUpdate("drop table tG");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testNumericCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tE");
    } catch(Exception ex) {}
			s.executeUpdate("create table tE (smallintCol smallint, intCol integer, bigintCol bigint, decimalCol1 decimal(22,2), decimalCol2 decimal(8,6), decimalCol3 decimal(31,28), realCol real, doubleCol double)");
			s.executeUpdate("insert into tE values(1, 2, 3, 4, 5.5, 6.6, 7.7, 3.4028235E38)");
			s.executeUpdate("insert into tE values(null,null,null,null,null,null,null,null)");

			System.out.println("TestE - focus on smallint datatypes");
			System.out.println("TestE1 - coalesce(smallintCol,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,smallintCol) from tE"));

			System.out.println("TestE1a - coalesce(smallintCol,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,intCol) from tE"));

			System.out.println("TestE1b - coalesce(smallintCol,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,bigintCol) from tE"));

			System.out.println("TestE1c - coalesce(SMALLINT,DECIMAL) with decimal(w,x) will give result decimal(p,x) where p=x+max(w-x,5) and if that gives p>31, then p is set to 31");
			System.out.println("TestE1c1 - coalesce(smallintCol,decimalCol1) with decimal(22,2) will give result decimal(22,2)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,decimalCol1) from tE"));

			System.out.println("TestE1c2 - coalesce(smallintCol,decimalCol2) with decimal(8,6) will give result decimal(11,6)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,decimalCol2) from tE"));

			System.out.println("TestE1c3 - coalesce(smallintCol,decimalCol3) with decimal(31,28) will give result decimal(31,28) rather than giving error for precision  > 31");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,decimalCol3) from tE"));

			System.out.println("TestE1d - coalesce(smallintCol,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,realCol) from tE"));

			System.out.println("TestE1e - coalesce(smallintCol,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(smallintCol,doubleCol) from tE"));

			System.out.println("TestE - focus on int datatypes");
			System.out.println("TestE1 - coalesce(intCol,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,intCol) from tE"));

			System.out.println("TestE1f - coalesce(intCol,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,smallintCol) from tE"));

			System.out.println("TestE1g - coalesce(intCol,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,bigintCol) from tE"));

			System.out.println("TestE1h - coalesce(INT,DECIMAL) with decimal(w,x) will give result decimal(p,x) where p=x+max(w-x,11) and if that gives p>31, then p is set to 31");
			System.out.println("TestE1h1 - coalesce(intCol,decimalCol1) with decimal(22,2) will give result decimal(22,2)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,decimalCol1) from tE"));

			System.out.println("TestE1h2 - coalesce(intCol,decimalCol2) with decimal(8,6) will give result decimal(17,6)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,decimalCol2) from tE"));

			System.out.println("TestE1h3 - coalesce(intCol,decimalCol3) with decimal(31,28) will give result decimal(31,28) rather than giving error for precision  > 31");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,decimalCol3) from tE"));

			System.out.println("TestE1i - coalesce(intCol,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,realCol) from tE"));

			System.out.println("TestE1j - coalesce(intCol,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(intCol,doubleCol) from tE"));

			System.out.println("TestE - focus on bigint datatypes");
			System.out.println("TestE1 - coalesce(bigintCol,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,bigintCol) from tE"));

			System.out.println("TestE1k - coalesce(bigintCol,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,smallintCol) from tE"));

			System.out.println("TestE1l - coalesce(bigintCol,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,intCol) from tE"));

			System.out.println("TestE1m - coalesce(BIGINT,DECIMAL) with decimal(w,x) will give result decimal(p,x) where p=x+max(w-x,19) and if that gives p>31, then p is set to 31");
			System.out.println("TestE1m1 - coalesce(bigintCol,decimalCol1) with decimal(22,2) will give result decimal(22,2)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,decimalCol1) from tE"));

			System.out.println("TestE1m2 - coalesce(bigintCol,decimalCol2) with decimal(8,6) will give result decimal(21,6)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,decimalCol2) from tE"));

			System.out.println("TestE1m3 - coalesce(bigintCol,decimalCol3) with decimal(31,28) will give result decimal(31,28) rather than giving error for precision  > 31");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,decimalCol3) from tE"));

			System.out.println("TestE1n - coalesce(bigintCol,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,realCol) from tE"));

			System.out.println("TestE1o - coalesce(bigintCol,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(bigintCol,doubleCol) from tE"));

			System.out.println("TestE - focus on decimal datatypes");

			System.out.println("TestE1 - coalesce(DECIMAL,DECIMAL) with decimal(w,x), decimal(y,z) will give result decimal(p,s)");
			System.out.println("  where p=max(x,z)+max(w-x,y-z), s=max(x,z) and if that gives p>31, then p is set to 31");
			System.out.println("TestE11 - coalesce(decimalCol1,decimalCol1) with decimal(22,2) will give result decimal(22,2)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,decimalCol1) from tE"));

			System.out.println("TestE12 - coalesce(decimalCol1,decimalCol2) with decimal(22,2) and decimal(8,6) will give result decimal(26,6)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,decimalCol2) from tE"));

			System.out.println("TestE13 - coalesce(decimalCol1,decimalCol3) with decimal(22,2) and decimal(31,28) will give result decimal(31,28) rather than giving error for precision  > 31");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,decimalCol3) from tE"));

			System.out.println("TestE1p - coalesce(decimalCol1,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,smallintCol) from tE"));

			System.out.println("TestE1q - coalesce(decimalCol1,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,intCol) from tE"));

			System.out.println("TestE1r - coalesce(decimalCol1,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,bigintCol) from tE"));

			System.out.println("TestE1s - coalesce(decimalCol1,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,realCol) from tE"));

			System.out.println("TestE1t - coalesce(decimalCol1,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(decimalCol1,doubleCol) from tE"));

			System.out.println("TestE - focus on real datatypes");
			System.out.println("TestE1 - coalesce(realCol,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,realCol) from tE"));

			System.out.println("TestE1u - coalesce(realCol,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,smallintCol) from tE"));

			System.out.println("TestE1v - coalesce(realCol,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,intCol) from tE"));

			System.out.println("TestE1w - coalesce(realCol,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,bigintCol) from tE"));

			System.out.println("TestE1x - coalesce(realCol,decimalCol1)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,decimalCol1) from tE"));

			System.out.println("TestE1y - coalesce(realCol,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(realCol,doubleCol) from tE"));

			System.out.println("TestE - focus on double datatypes");
			System.out.println("TestE1 - coalesce(doubleCol,doubleCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,doubleCol) from tE"));

			System.out.println("TestE1z - coalesce(doubleCol,smallintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,smallintCol) from tE"));

			System.out.println("TestE2a - coalesce(doubleCol,intCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,intCol) from tE"));

			System.out.println("TestE2b - coalesce(doubleCol,bigintCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,bigintCol) from tE"));

			System.out.println("TestE2c - coalesce(doubleCol,decimalCol1)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,decimalCol1) from tE"));

			System.out.println("TestE2d - coalesce(doubleCol,realCol)");
			dumpRSwithScale(s.executeQuery("select coalesce(doubleCol,realCol) from tE"));

			s.executeUpdate("drop table tE");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testCharCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tB");
    } catch(Exception ex) {}
			s.executeUpdate("create table tB (c1 char(254), c2 char(40), vc1 varchar(253), vc2 varchar(2000), lvc1 long varchar, lvc2 long varchar, clob1 CLOB(200), clob2 CLOB(33K))");
			s.executeUpdate("insert into tB values('c1 not null', 'c2 not null', 'vc1 not null', 'vc2 not null', 'lvc1 not null', 'lvc2 not null', 'clob1 not null', 'clob2 not null')");
			s.executeUpdate("insert into tB values('c1 not null but c2 is', null, 'vc1 is not null but vc2 is', null, null, null,null,null)");
			s.executeUpdate("insert into tB values(null,'c2 not null but c1 is', null, 'vc2 is not null but vc1 is', 'lvc1 not null again', 'lvc2 not null again', 'clob1 not null again', 'clob2 not null again')");
			s.executeUpdate("insert into tB values(null,null, null, null, null, null, null, null)");

			System.out.println("TestB - Focus on CHAR as atleast one of the operands");
			System.out.println("TestB1a - 2 CHAR operands coalesce(c1,c2) with c1(254) and c2(40)");
			dumpRS(s.executeQuery("select coalesce(c1,c2) from tB"));

			System.out.println("TestB1b - 2 CHAR operands value(c1,c2) with c1(254) and c2(40)");
			dumpRS(s.executeQuery("select value(c1,c2) from tB"));

			System.out.println("TestB2a - 2 CHAR operands coalesce(c2,c1) with c2(40) and c1(254)");
			dumpRS(s.executeQuery("select coalesce(c2,c1) from tB"));

			System.out.println("TestB2b - 2 CHAR operands value(c2,c1) with c2(40) and c1(254)");
			dumpRS(s.executeQuery("select value(c2,c1) from tB"));

			System.out.println("TestB3a - CHAR and VARCHAR operands coalesce(c1,vc1) with c1(254) and vc1(253)");
			dumpRS(s.executeQuery("select coalesce(c1,vc1) from tB"));

			System.out.println("TestB3b - CHAR and VARCHAR operands value(c1,vc1) with c1(254) and vc1(253)");
			dumpRS(s.executeQuery("select value(c1,vc1) from tB"));

			System.out.println("TestB4a - VARCHAR and CHAR operands coalesce(vc1,c1) with vc1(253) and c1(254)");
			dumpRS(s.executeQuery("select coalesce(vc1,c1) from tB"));

			System.out.println("TestB4b - VARCHAR AND CHAR operands value(vc1,c1) with vc1(253) and c1(254)");
			dumpRS(s.executeQuery("select value(vc1,c1) from tB"));

			System.out.println("TestB - Focus on VARCHAR as atleast one of the operands");
			System.out.println("TestB5a - 2 VARCHAR operands coalesce(vc1,vc2) with vc1(253) and vc2(2000)");
			dumpRS(s.executeQuery("select coalesce(vc1,vc2) from tB"));

			System.out.println("TestB5b - 2 VARCHAR operands value(vc1,vc2) with vc1(253) and vc2(2000)");
			dumpRS(s.executeQuery("select value(vc1,vc2) from tB"));

			System.out.println("TestB6a - 2 VARCHAR operands coalesce(vc2,vc1) with vc2(2000) and vc1(253)");
			dumpRS(s.executeQuery("select coalesce(vc2,vc1) from tB"));

			System.out.println("TestB6b - 2 VARCHAR operands value(vc2,vc1) with vc2(2000) and vc1(253)");
			dumpRS(s.executeQuery("select value(vc2,vc1) from tB"));

			System.out.println("TestB - Focus on LONG VARCHAR as atleast one of the operands");
			System.out.println("TestB7a - CHAR and LONG VARCHAR operands coalesce(c1,lvc1) with c1(254)");
			dumpRS(s.executeQuery("select coalesce(c1,lvc1) from tB"));

			System.out.println("TestB7b - CHAR and LONG VARCHAR operands value(c1,lvc1) with c1(254)");
			dumpRS(s.executeQuery("select value(c1,lvc1) from tB"));

			System.out.println("TestB8a - LONG VARCHAR and CHAR operands coalesce(lvc1,c1) with c1(254)");
			dumpRS(s.executeQuery("select coalesce(lvc1,c1) from tB"));

			System.out.println("TestB8b - LONG VARCHAR and CHAR operands value(lvc1,c1) with c1(254)");
			dumpRS(s.executeQuery("select value(lvc1,c1) from tB"));

			System.out.println("TestB9a - VARCHAR and LONG VARCHAR operands coalesce(vc1,lvc1) with vc1(253)");
			dumpRS(s.executeQuery("select coalesce(vc1,lvc1) from tB"));

			System.out.println("TestB9b - VARCHAR and LONG VARCHAR operands value(vc1,lvc1) with vc1(253)");
			dumpRS(s.executeQuery("select value(vc1,lvc1) from tB"));

			System.out.println("TestB10a - LONG VARCHAR and VARCHAR operands coalesce(lvc1,vc1) with vc1(253)");
			dumpRS(s.executeQuery("select coalesce(lvc1,vc1) from tB"));

			System.out.println("TestB10b - LONG VARCHAR and VARCHAR operands value(lvc1,vc1) with vc1(253)");
			dumpRS(s.executeQuery("select value(lvc1,vc1) from tB"));

			System.out.println("TestB11a - LONG VARCHAR and LONG VARCHAR operands coalesce(lvc1,lvc2)");
			dumpRS(s.executeQuery("select coalesce(lvc1,lvc2) from tB"));

			System.out.println("TestB11b - LONG VARCHAR and LONG VARCHAR operands value(lvc1,lvc2)");
			dumpRS(s.executeQuery("select value(lvc1,lvc2) from tB"));

			System.out.println("TestB - Focus on CLOB as atleast one of the operands");
			System.out.println("TestB12a - CLOB and CHAR operands coalesce(clob1,c1) with clob1(200) and c1(254)");
			dumpRS(s.executeQuery("select coalesce(clob1,c1) from tB"));

			System.out.println("TestB12b - CLOB and CHAR operands value(clob1,c1) with clob1(200) and c1(254)");
			dumpRS(s.executeQuery("select value(clob1,c1) from tB"));

			System.out.println("TestB13a - CHAR and CLOB operands coalesce(c1,clob2) with c1(254) and clob2(33K)");
			dumpRS(s.executeQuery("select coalesce(c1,clob2) from tB"));

			System.out.println("TestB13b - CHAR and CLOB operands value(c1,clob2) with c1(254) and clob2(33K)");
			dumpRS(s.executeQuery("select value(c1,clob2) from tB"));

			System.out.println("TestB14a - CLOB and VARCHAR operands coalesce(clob1,vc1) with clob1(200) and vc1(253)");
			dumpRS(s.executeQuery("select coalesce(clob1,vc1) from tB"));

			System.out.println("TestB14b - CLOB and VARCHAR operands value(clob1,vc1) with clob1(200) and vc1(253)");
			dumpRS(s.executeQuery("select value(clob1,vc1) from tB"));

			System.out.println("TestB15a - VARCHAR and CLOB operands coalesce(vc2,clob2) with vc2(2000) and clob2(33K)");
			dumpRS(s.executeQuery("select coalesce(vc2,clob2) from tB"));

			System.out.println("TestB15b - VARCHAR and CLOB operands value(vc2,clob2) with vc2(2000) and clob2(33K)");
			dumpRS(s.executeQuery("select value(vc2,clob2) from tB"));

			System.out.println("TestB16a - CLOB and LONG VARCHAR operands coalesce(clob1,lvc1) with clob1(200). The result length will be 32700 (long varchar max length)");
			dumpRS(s.executeQuery("select coalesce(clob1,lvc1) from tB"));

			System.out.println("TestB16b - CLOB and LONG VARCHAR operands value(clob1,lvc1) with clob1(200). The result length will be 32700 (long varchar max length)");
			dumpRS(s.executeQuery("select value(clob1,lvc1) from tB"));

			System.out.println("TestB17a - LONG VARCHAR and CLOB operands coalesce(lvc2,clob2) with clob2(33K). The result length will be 33K since clob length here is > 32700 (long varchar max length)");
			dumpRS(s.executeQuery("select coalesce(lvc2,clob2) from tB"));

			System.out.println("TestB17b - LONG VARCHAR and CLOB operands value(lvc2,clob2) with clob2(33K). The result length will be 33K since clob length here is > 32700 (long varchar max length)");
			dumpRS(s.executeQuery("select value(lvc2,clob2) from tB"));

			System.out.println("TestB18a - CLOB and CLOB operands coalesce(clob1,clob2) with clob1(200) and clob2(33K).");
			dumpRS(s.executeQuery("select coalesce(clob1,clob2) from tB"));

			System.out.println("TestB18b - CLOB and CLOB operands value(clob1,clob2) with clob1(200) and clob2(33K).");
			dumpRS(s.executeQuery("select value(clob1,clob2) from tB"));

			s.executeUpdate("drop table tB");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testCharForBitDataCoalesce( Connection conn) throws Throwable
	{
    try {
			Statement s = conn.createStatement();
			PreparedStatement ps;
    try {
			s.executeUpdate("drop table tC");
    } catch(Exception ex) {}
			s.executeUpdate("create table tC (cbd1 char(254) for bit data, cbd2 char(40) for bit data, vcbd1 varchar(253) for bit data, vcbd2 varchar(2000) for bit data, lvcbd1 long varchar for bit data, lvcbd2 long varchar for bit data, blob1 BLOB(200), blob2 BLOB(33K))");
			ps = conn.prepareStatement("insert into tC values (?,?,?,?,?,?,?,?)");
			ps.setBytes(1, "cbd1 not null".getBytes());
			ps.setBytes(2, "cbd2 not null".getBytes());
			ps.setBytes(3, "vcbd1 not null".getBytes());
			ps.setBytes(4, "vcbd2 not null".getBytes());
			ps.setBytes(5, "lvcbd1 not null".getBytes());
			ps.setBytes(6, "lvcbd2 not null".getBytes());
			ps.setBytes(7, "blob1 not null".getBytes());
			ps.setBytes(8, "blob2 not null".getBytes());
			ps.executeUpdate();
			ps.setBytes(1, "cbd1 not null but cbd2 is".getBytes());
			ps.setBytes(2, null);
			ps.setBytes(3, "vcbd1 not null but vcbd2 is".getBytes());
			ps.setBytes(4, null);
			ps.setBytes(5, null);
			ps.setBytes(6, null);
			ps.setBytes(7, null);
			ps.setBytes(8, null);
			ps.executeUpdate();
			ps.setBytes(1, null);
			ps.setBytes(2, "cbd2 not null but cbd1 is".getBytes());
			ps.setBytes(3, null);
			ps.setBytes(4, "vcbd2 not null but vcbd1 is".getBytes());
			ps.setBytes(5, "lvcbd1 not null again".getBytes());
			ps.setBytes(6, "lvcbd2 not null again".getBytes());
			ps.setBytes(7, "blob1 not null again".getBytes());
			ps.setBytes(8, "blob2 not null again".getBytes());
			ps.executeUpdate();
			ps.setBytes(1, null);
			ps.setBytes(2, null);
			ps.setBytes(3, null);
			ps.setBytes(4, null);
			ps.setBytes(5, null);
			ps.setBytes(6, null);
			ps.setBytes(7, null);
			ps.setBytes(8, null);
			ps.executeUpdate();

			System.out.println("TestC - Focus on CHAR FOR BIT DATA as atleast one of the operands");
			System.out.println("TestC1a - 2 CHAR FOR BIT DATA operands coalesce(cbd1,cbd2) with cbd1(254) and cbd2(40)");
			dumpRS(s.executeQuery("select coalesce(cbd1,cbd2) from tC"));

			System.out.println("TestC1b - 2 CHAR FOR BIT DATA operands value(cbd1,cbd2) with cbd1(254) and cbd2(40)");
			dumpRS(s.executeQuery("select value(cbd1,cbd2) from tC"));

			System.out.println("TestC2a - 2 CHAR FOR BIT DATA operands coalesce(cbd2,cbd1) with cbd2(40) and cbd1(254)");
			dumpRS(s.executeQuery("select coalesce(cbd2,cbd1) from tC"));

			System.out.println("TestC2b - 2 CHAR FOR BIT DATA operands value(cbd2,cbd1) with cbd2(40) and cbd1(254)");
			dumpRS(s.executeQuery("select value(cbd2,cbd1) from tC"));

			System.out.println("TestC3a - CHAR FOR BIT DATA and VARCHAR FOR BIT DATA operands coalesce(cbd1,vcbd1) with cbd1(254) and vcbd1(253)");
			dumpRS(s.executeQuery("select coalesce(cbd1,vcbd1) from tC"));

			System.out.println("TestC3b - CHAR FOR BIT DATA and VARCHAR FOR BIT DATA operands value(cbd1,vcbd1) with cbd1(254) and vcbd1(253)");
			dumpRS(s.executeQuery("select value(cbd1,vcbd1) from tC"));

			System.out.println("TestC4a - VARCHAR FOR BIT DATA and CHAR FOR BIT DATA operands coalesce(vcbd1,cbd1) with vcbd1(253) and cbd1(254)");
			dumpRS(s.executeQuery("select coalesce(vcbd1,cbd1) from tC"));

			System.out.println("TestC4b - VARCHAR FOR BIT DATA AND CHAR FOR BIT DATA operands value(vcbd1,cbd1) with vcbd1(253) and cbd1(254)");
			dumpRS(s.executeQuery("select value(vcbd1,cbd1) from tC"));

			System.out.println("TestC - Focus on VARCHAR FOR BIT DATA as atleast one of the operands");
			System.out.println("TestC5a - 2 VARCHAR FOR BIT DATA operands coalesce(vcbd1,vcbd2) with vcbd1(253) and vcbd2(2000)");
			dumpRS(s.executeQuery("select coalesce(vcbd1,vcbd2) from tC"));

			System.out.println("TestC5b - 2 VARCHAR FOR BIT DATA operands value(vcbd1,vcbd2) with vcbd1(253) and vcbd2(2000)");
			dumpRS(s.executeQuery("select value(vcbd1,vcbd2) from tC"));

			System.out.println("TestC6a - 2 VARCHAR FOR BIT DATA operands coalesce(vcbd2,vcbd1) with vcbd2(2000) and vcbd1(253)");
			dumpRS(s.executeQuery("select coalesce(vcbd2,vcbd1) from tC"));

			System.out.println("TestC6b - 2 VARCHAR FOR BIT DATA operands value(vcbd2,vcbd1) with vcbd2(2000) and vcbd1(253)");
			dumpRS(s.executeQuery("select value(vcbd2,vcbd1) from tC"));

			System.out.println("TestC - Focus on LONG VARCHAR FOR BIT DATA as atleast one of the operands");
			System.out.println("TestC7a - CHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands coalesce(cbd1,lvcbd1) with cbd1(254)");
			dumpRS(s.executeQuery("select coalesce(cbd1,lvcbd1) from tC"));

			System.out.println("TestC7b - CHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands value(cbd1,lvcbd1) with cbd1(254)");
			dumpRS(s.executeQuery("select value(cbd1,lvcbd1) from tC"));

			System.out.println("TestC8a - LONG VARCHAR FOR BIT DATA and CHAR FOR BIT DATA operands coalesce(lvcbd1,cbd1) with cbd1(254)");
			dumpRS(s.executeQuery("select coalesce(lvcbd1,cbd1) from tC"));

			System.out.println("TestC8b - LONG VARCHAR FOR BIT DATA and CHAR FOR BIT DATA operands value(lvcbd1,cbd1) with cbd1(254)");
			dumpRS(s.executeQuery("select value(lvcbd1,cbd1) from tC"));

			System.out.println("TestC9a - VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands coalesce(vcbd1,lvcbd1) with vcbd1(253)");
			dumpRS(s.executeQuery("select coalesce(vcbd1,lvcbd1) from tC"));

			System.out.println("TestC9b - VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands value(vcbd1,lvcbd1) with vcbd1(253)");
			dumpRS(s.executeQuery("select value(vcbd1,lvcbd1) from tC"));

			System.out.println("TestC10a - LONG VARCHAR FOR BIT DATA and VARCHAR FOR BIT DATA operands coalesce(lvcbd1,vcbd1) with vcbd1(253)");
			dumpRS(s.executeQuery("select coalesce(lvcbd1,vcbd1) from tC"));

			System.out.println("TestC10b - LONG VARCHAR FOR BIT DATA and VARCHAR FOR BIT DATA operands value(lvcbd1,vcbd1) with vcbd1(253)");
			dumpRS(s.executeQuery("select value(lvcbd1,vcbd1) from tC"));

			System.out.println("TestC11a - LONG VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands coalesce(lvcbd1,lvcbd2)");
			dumpRS(s.executeQuery("select coalesce(lvcbd1,lvcbd2) from tC"));

			System.out.println("TestC11b - LONG VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA operands value(lvcbd1,lvcbd2)");
			dumpRS(s.executeQuery("select value(lvcbd1,lvcbd2) from tC"));

			System.out.println("TestC - Focus on BLOB as atleast one of the operands");
			try {
				System.out.println("TestC12a - BLOB and CHAR FOR BIT DATA in coalesce(blob1,cbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(blob1,cbd1) from tC"));
				System.out.println("TestC12a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC12b - BLOB and CHAR FOR BIT DATA in value(blob1,cbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(blob1,cbd1) from tC"));
				System.out.println("TestC12b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC13a - CHAR FOR BIT DATA and BLOB operands coalesce(cbd1,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(cbd1,blob2) from tC"));
				System.out.println("TestC13a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC13b - CHAR FOR BIT DATA and BLOB operands value(cbd1,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(cbd1,blob2) from tC"));
				System.out.println("TestC13b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC14a - BLOB and VARCHAR FOR BIT DATA operands coalesce(blob1,vcbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(blob1,vcbd1) from tC"));
				System.out.println("TestC14a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC14b - BLOB and VARCHAR FOR BIT DATA operands value(blob1,vcbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(blob1,vcbd1) from tC"));
				System.out.println("TestC14b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC15a - VARCHAR FOR BIT DATA and BLOB operands coalesce(vcbd2,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(vcbd2,blob2) from tC"));
				System.out.println("TestC15a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC15b - VARCHAR FOR BIT DATA and BLOB operands value(vcbd2,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(vcbd2,blob2) from tC"));
				System.out.println("TestC15b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC16a - BLOB and LONG VARCHAR FOR BIT DATA operands coalesce(blob1,lvcbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(blob1,lvcbd1) from tC"));
				System.out.println("TestC16a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC16b - BLOB and LONG VARCHAR FOR BIT DATA operands coalesce(blob1,lvcbd1) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(blob1,lvcbd1) from tC"));
				System.out.println("TestC16b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC17a - LONG VARCHAR FOR BIT DATA and BLOB operands coalesce(lvcbd2,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select coalesce(lvcbd2,blob2) from tC"));
				System.out.println("TestC17a - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("TestC17b - LONG VARCHAR FOR BIT DATA and BLOB operands value(lvcbd2,blob2) will fail because BLOB is not compatible with FOR BIT DATA datatypes");
				dumpRS(s.executeQuery("select value(lvcbd2,blob2) from tC"));
				System.out.println("TestC17b - should have failed");
			} catch (SQLException e) {
				if (e.getSQLState().equals("42815"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("TestC18a - BLOB and BLOB operands coalesce(blob1,blob2) with blob1(200) and blob2(33K).");
			dumpRS(s.executeQuery("select coalesce(blob1,blob2) from tC"));

			System.out.println("TestC18b - BLOB and BLOB operands value(blob1,blob2) with blob1(200) and blob2(33K).");
			dumpRS(s.executeQuery("select value(blob1,blob2) from tC"));

			s.executeUpdate("drop table tC");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}    

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}

	// lifted from the metadata test
	public static void dumpRS(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

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
                row.append(new String(s.getBytes(i)));
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

	// lifted from the metadata test
	public static void dumpRSwithScale(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

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
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}

}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.concateTests

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
import java.util.Arrays;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.Formatters;

import java.io.ByteArrayInputStream; 

/**
  Concatenation tests for various datatypes
 */
public class concateTests
{

	private static String[] concatenatedSQLTypesNames =
	{
		/*0*/ "CHAR",
		/*1*/ "VARCHAR",
		/*2*/ "LONG VARCHAR",
		/*3*/ "CLOB",
		/*4*/ "CHAR () FOR BIT DATA",
		/*5*/ "VARCHAR () FOR BIT DATA",
		/*6*/ "LONG VARCHAR FOR BIT DATA",
		/*7*/ "BLOB",

	};

	public static void main (String[] argv) throws Throwable
	{
		ij.getPropertyArg(argv);
		Connection conn = ij.startJBMS();

		testCharConcatenation(conn);
		testCharForBitDataConcatenation(conn);
	}

	public static void testCharConcatenation( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test1 - CHAR, VARCHAR, LONGVARCHAR and CLOB concatenation tests");

			String columnC1value;
			String columnC2value = Formatters.repeatChar("a",40);
			String columnVC1value;
			String columnVC2value;
			String columnVC3value = "z";
			String columnLVC1value;
			String columnLVC2value = Formatters.repeatChar("b",32698);
			StringBuffer tempStringBuffer = new StringBuffer();

			Statement s = conn.createStatement();
			try { //this is if we ever run the test against DB2, we want to make sure table doesn't already exist in DB2
			s.executeUpdate("drop table t1");
			} catch(Exception ex) {}
			s.executeUpdate("create table t1 (c1 char(254), c2 char(40), vc1 varchar(264), vc2 varchar(4000), vc3 varchar(1), lvc1 long varchar, lvc2 long varchar)");
			PreparedStatement ps = conn.prepareStatement("insert into t1(c2, vc3) values(?, ?)");
      ps.setString(1,columnC2value);
      ps.setString(2,columnVC3value);
			ps.executeUpdate();
			ps = conn.prepareStatement("update t1 set lvc2 = ?");
      ps.setString(1,columnLVC2value);
			ps.executeUpdate();

			System.out.println("Test1a - CHAR concatenations will give result type of CHAR when concatenated string < 255");
			//operands CHAR(A) CHAR(B) and A+B<255 then result is CHAR(A+B)
			dumpSomeMetaDataInfo(s.executeQuery("values(select c2 || c2 || c2 || c2 || c2 || c2 || '12345678901234' from t1)"), concatenatedSQLTypesNames[0]);
			tempStringBuffer = new StringBuffer(columnC2value);
      tempStringBuffer.append(columnC2value).append(columnC2value).append(columnC2value).append(columnC2value).append(columnC2value);
      tempStringBuffer.append("12345678901234");
      columnC1value = tempStringBuffer.toString();
			verifyStringData(s.executeQuery("values(select c2 || c2 || c2 || c2 || c2 || c2 || '12345678901234' from t1)"), columnC1value);
			s.executeUpdate("update t1 set c1 = c2 || c2 || c2 || c2 || c2 || c2 || '12345678901234'");
			verifyStringData(s.executeQuery("select c1 from t1"), columnC1value);

			System.out.println("Test1b boundary test - CHAR concatenations will give result type of VARCHAR when concatenated string = 255");
			//operands CHAR(A) CHAR(B) and A+B>254 then result is VARCHAR(A+B)
      columnVC1value = columnC1value + "1";
			dumpSomeMetaDataInfo(s.executeQuery("values(select c1 || '1' from t1)"), concatenatedSQLTypesNames[1]);
			verifyStringData(s.executeQuery("values(select c1 || '1' from t1)"), columnVC1value);
			s.executeUpdate("update t1 set vc1 = c1 || '1'");
			verifyStringData(s.executeQuery("select vc1 from t1"), columnVC1value);

			System.out.println("Test1b - CHAR concatenations will give result type of VARCHAR when concatenated string > 254");
			//operands CHAR(A) CHAR(B) and A+B>254 then result is VARCHAR(A+B)
      columnVC1value = columnC1value + "1234567890";
			dumpSomeMetaDataInfo(s.executeQuery("values(select c1 || '1234567890' from t1)"), concatenatedSQLTypesNames[1]);
			verifyStringData(s.executeQuery("values(select c1 || '1234567890' from t1)"), columnVC1value);
			s.executeUpdate("update t1 set vc1 = c1 || '1234567890'");
			verifyStringData(s.executeQuery("select vc1 from t1"), columnVC1value);

			System.out.println("Test1c - CHAR and VARCHAR concatenations will give result type of VARCHAR when concatenated string < 4001");
			//operands CHAR(A) VARCHAR(B) and A+B<4001 then result is VARCHAR(A+B)
			//concatenated string 4000 characters long in following updates
			tempStringBuffer = new StringBuffer(columnC2value);
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
      columnVC2value = tempStringBuffer.toString();
      columnLVC1value = tempStringBuffer.toString();
			dumpSomeMetaDataInfo(s.executeQuery("values(select c2||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1 from t1)"), concatenatedSQLTypesNames[1]);
			verifyStringData(s.executeQuery("values(select c2||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1 from t1)"), columnVC2value);
			s.executeUpdate("update t1 set vc2 = c2||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1");
			verifyStringData(s.executeQuery("select vc2 from t1"), columnVC2value);
			s.executeUpdate("update t1 set lvc1 = c2||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1d - VARCHAR and CHAR concatenations will give result type of VARCHAR when concatenated string < 4001");
			//operands VARCHAR(A) CHAR(B) and A+B<4001 then result is VARCHAR(A+B)
			//concatenated string 4000 characters long in following updates
			tempStringBuffer = new StringBuffer();
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
      tempStringBuffer.append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value).append(columnVC1value);
			tempStringBuffer.append(columnC2value);
      columnVC2value = tempStringBuffer.toString();
      columnLVC1value = tempStringBuffer.toString();
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||c2 from t1)"), concatenatedSQLTypesNames[1]);
			verifyStringData(s.executeQuery("values(select vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||c2 from t1)"), columnVC2value);
			s.executeUpdate("update t1 set vc2 = vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||c2");
			verifyStringData(s.executeQuery("select vc2 from t1"), columnVC2value);
			s.executeUpdate("update t1 set lvc1 = vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||vc1||c2");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1e boundary test - CHAR and VARCHAR concatenations will give result type of LONG VARCHAR when concatenated string = 4001");
			//operands CHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
			//concatenated string is 4001 characters long in following 2 updates
      columnLVC1value = "a"+columnVC2value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select 'a'||vc2 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select 'a'||vc2 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = 'a'||vc2");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1f boundary test - VARCHAR and CHAR concatenations will give result type of LONG VARCHAR when concatenated string = 4001");
			//operands VARCHAR(A) CHAR(B) and A+B>4000 then result is LONG VARCHAR
      columnLVC1value = columnVC2value+"a";
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc2 || 'a' from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select vc2 || 'a' from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = vc2 || 'a'");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1g - CHAR and LONG VARCHAR concatenations will give result type of LONG VARCHAR");
			//operands CHAR(A) LONG VARCHAR then result is LONG VARCHAR
      columnLVC1value = "a"+columnLVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select 'a' || lvc1 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select 'a' || lvc1 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = 'a' || lvc1");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1h - VARCHAR and LONG VARCHAR concatenations will give result type of LONG VARCHAR");
			//operands VARCHAR(A) LONG VARCHAR then result is LONG VARCHAR
      columnLVC1value = columnVC1value+columnLVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || lvc1 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select vc1 || lvc1 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = vc1 || lvc1");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1i - VARCHAR and VARCHAR concatenations will give result type of VARCHAR when concatenated string < 4001");
			//operands VARCHAR(A) VARCHAR(B) and A+B<4001 then result is VARCHAR(A+B)
      columnLVC1value = columnVC1value+columnVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || vc1 from t1)"), concatenatedSQLTypesNames[1]);
			verifyStringData(s.executeQuery("values(select vc1 || vc1 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = vc1 || vc1");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1j boundary test - VARCHAR and VARCHAR concatenations will give result type of LONG VARCHAR when concatenated string = 4001");
			//operands VARCHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
      columnLVC1value = columnVC2value+columnVC3value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc2 || vc3 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select vc2 || vc3 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = vc2 || vc3");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1j - VARCHAR and VARCHAR concatenations will give result type of LONG VARCHAR when concatenated string > 4000");
			//operands VARCHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
      columnLVC1value = columnVC1value+columnVC2value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || vc2 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select vc1 || vc2 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = vc1 || vc2");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			System.out.println("Test1k - LONG VARCHAR and LONG VARCHAR concatenations will give result type of LONG VARCHAR");
			//operands LONG VARCHAR, LONG VARCHAR then result is LONG VARCHAR
      columnLVC1value = columnLVC1value+columnLVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvc1 || lvc1 from t1)"), concatenatedSQLTypesNames[2]);
			verifyStringData(s.executeQuery("values(select lvc1 || lvc1 from t1)"), columnLVC1value);
			s.executeUpdate("update t1 set lvc1 = lvc1 || lvc1");
			verifyStringData(s.executeQuery("select lvc1 from t1"), columnLVC1value);

			//operands CHAR(A)/VARCHAR(A)/LONGVARCHAR, LONGVARCHAR and "concatenated string length">32700 does not cause automatic escalation
			//to LOB for compatibility with previous releases. Any such cases would result in an error at runtime
			System.out.println("Test1l - CHAR and LONGVARCHAR concatenation resulting in concatenated string > 32700 will give error");
			try {
				dumpSomeMetaDataInfo(s.executeQuery("values(select c2 || lvc2 from t1)"), concatenatedSQLTypesNames[2]);
				System.out.println("FAIL - should have gotten overflow error for values");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("update t1 set lvc2 = c2 || lvc2");
				System.out.println("FAIL - should have gotten overflow error for insert");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Test1m - VARCHAR and LONGVARCHAR concatenation resulting in concatenated string > 32700 will give error");
			try {
				dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || lvc2 from t1)"), concatenatedSQLTypesNames[2]);
				System.out.println("FAIL - should have gotten overflow error for values");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("update t1 set lvc2 = vc1 || lvc2");
				System.out.println("FAIL - should have gotten overflow error for insert");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Test1n - LONGVARCHAR and LONGVARCHAR concatenation resulting in concatenated string > 32700 will give error");
			try {
				dumpSomeMetaDataInfo(s.executeQuery("values(select lvc1 || lvc2 from t1)"), concatenatedSQLTypesNames[2]);
				System.out.println("FAIL - should have gotten overflow error for values");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("update t1 set lvc2 = lvc1 || lvc2");
				System.out.println("FAIL - should have gotten overflow error for insert");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54006"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			try { //this is if we ever run the test against DB2, we want to make sure table doesn't already exist in DB2
			s.executeUpdate("drop table testCLOB_MAIN");
			} catch(Exception ex) {}
			s.executeUpdate("create table testCLOB_MAIN (c1 char(10), vc1 varchar(100), lvc1 long varchar, clob1 CLOB(2G), clob2 CLOB(256), clob3 CLOB(1M))");
			ps = conn.prepareStatement("insert into testCLOB_MAIN values(?,?,?,?,?,?)");
			columnC1value = "1234567890";
			ps.setString(1, columnC1value);
			columnVC1value = "this is varchar";
			ps.setString(2, columnVC1value);
			columnLVC1value = "this is long varchar";
			ps.setString(3, columnLVC1value);
			String columnCLOB1value = "this is 2G clob";
			ps.setString(4, columnCLOB1value);
			String columnCLOB2value = "this is 256 characters clob";
			ps.setString(5, columnCLOB2value);
			String columnCLOB3value = "this is 1M clob";
			ps.setString(6, columnCLOB3value);
			ps.executeUpdate();

			System.out.println("Test1o - CHAR(A) and CLOB(B) concatenations will give result type of CLOB(A+B) when A+B<2G");
      columnCLOB2value = columnC1value+"this is 256 characters clob";
			dumpSomeMetaDataInfo(s.executeQuery("values(select c1 || clob2 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select c1 || clob2 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = c1 || clob2");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1p - CLOB(A) and CHAR(B) concatenations will give result type of CLOB(A+B) when A+B<2G");
      columnCLOB2value = columnCLOB2value+columnC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob2 || c1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob2 || c1 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = clob2 || c1");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1q - CHAR(A) and CLOB(B) concatenations will give result type of CLOB(2G) when A+B>2G");
      columnCLOB1value = columnC1value+columnCLOB1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select c1 || clob1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select c1 || clob1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = c1 || clob1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test1r - CLOB(A) and CHAR(B) concatenations will give result type of CLOB(2G) when A+B>2G");
      columnCLOB1value = columnCLOB1value+columnC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob1 || c1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob1 || c1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = clob1 || c1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test1s - VARCHAR(A) and CLOB(B) concatenations will give result type of CLOB(A+B) when A+B<2G");
      columnCLOB2value = columnVC1value+columnCLOB2value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || clob2 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select vc1 || clob2 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = vc1 || clob2");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1t - CLOB(A) and VARCHAR(B) concatenations will give result type of CLOB(A+B) when A+B<2G");
      columnCLOB2value = columnCLOB2value+columnVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob2 || vc1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob2 || vc1 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = clob2 || vc1");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1u - VARCHAR(A) and CLOB(B) concatenations will give result type of CLOB(2G) when A+B>2G");
      columnCLOB1value = columnVC1value+columnCLOB1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select vc1 || clob1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select vc1 || clob1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = vc1 || clob1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test1v - CLOB(A) and VARCHAR(B) concatenations will give result type of CLOB(2G) when A+B>2G");
      columnCLOB1value = columnCLOB1value+columnVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob1 || vc1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob1 || vc1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = clob1 || vc1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test1w - LONG VARCHAR and CLOB(A) concatenations will give result type of CLOB(A+32K) when A+32K<2G");
      columnCLOB2value = columnLVC1value+columnCLOB2value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvc1 || clob2 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select lvc1 || clob2 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = lvc1 || clob2");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1x - CLOB(A) and LONG VARCHAR concatenations will give result type of CLOB(A+32K) when A+32K<2G");
      columnCLOB2value = columnCLOB2value+columnLVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob2 || lvc1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob2 || lvc1 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = clob2 || lvc1");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test1y - LONG VARCHAR and CLOB(B) concatenations will give result type of CLOB(2G) when A+32K>2G");
      columnCLOB1value = columnLVC1value+columnCLOB1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvc1 || clob1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select lvc1 || clob1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = lvc1 || clob1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test1z - CLOB(A) and LONG VARCHAR concatenations will give result type of CLOB(2G) when A+32K>2G");
      columnCLOB1value = columnCLOB1value+columnLVC1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob1 || lvc1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob1 || lvc1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = clob1 || lvc1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test11a - CLOB(A) and CLOB(B) concatenations will give result type of CLOB(A+B) when A+B<2G");
      columnCLOB2value = columnCLOB2value+columnCLOB3value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob2 || clob3 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob2 || clob3 from testCLOB_MAIN)"), columnCLOB2value);
			s.executeUpdate("update testCLOB_MAIN set clob2 = clob2 || clob3");
			verifyStringData(s.executeQuery("select clob2 from testCLOB_MAIN"), columnCLOB2value);

			System.out.println("Test11b - CLOB(A) and CLOB(B) concatenations will give result type of CLOB(2G) when A+B>2G");
      columnCLOB1value = columnCLOB2value+columnCLOB1value;
			dumpSomeMetaDataInfo(s.executeQuery("values(select clob2 || clob1 from testCLOB_MAIN)"), concatenatedSQLTypesNames[3]);
			verifyStringData(s.executeQuery("values(select clob2 || clob1 from testCLOB_MAIN)"), columnCLOB1value);
			s.executeUpdate("update testCLOB_MAIN set clob1 = clob2 || clob1");
			verifyStringData(s.executeQuery("select clob1 from testCLOB_MAIN"), columnCLOB1value);

			System.out.println("Test12 - try 2 empty string concatenation and verify that length comes back as 0 for the result");
			dumpSomeMetaDataInfo(s.executeQuery("values('' || '')"), concatenatedSQLTypesNames[0]);
			verifyStringData(s.executeQuery("values('' || '')"), "");

			s.executeUpdate("drop table testCLOB_MAIN");
			s.executeUpdate("drop table t1");
			System.out.println("Test1 finished - CHAR, VARCHAR, LONGVARCHAR and CLOB concatenation tests");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testCharForBitDataConcatenation( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test2 - CHAR FOR BIT DATA, VARCHAR FOR BIT DATA, LONGVARCHAR FOR BIT DATA and BLOB concatenation tests");
			Statement s = conn.createStatement();
			byte[] columnCBD0value = {(byte)'a'};
			byte[] columnCBD1value;
			byte[] columnCBD2value = new byte[40];
			java.util.Arrays.fill(columnCBD2value, (byte)'a');
			byte[] columnCBD3value = new byte[14];
			java.util.Arrays.fill(columnCBD3value, (byte)'a');
			byte[] columnVCBD1value;
			byte[] columnVCBD2value;
			byte[] columnVCBD3value = {(byte)'a'};
			byte[] columnLVCBD1value;
			byte[] columnLVCBD2value = new byte[32698];
			java.util.Arrays.fill(columnLVCBD2value, (byte)'a');
			byte[] tempStringBuffer;

			try { //this is if we ever run the test against DB2, we want to make sure table doesn't already exist in DB2
			s.executeUpdate("drop table t2");
			} catch(Exception ex) {}
			s.executeUpdate("create table t2 (cbd0 CHAR(1) FOR BIT DATA, cbd1 CHAR(254) FOR BIT DATA, cbd2 CHAR(40) FOR BIT DATA, cbd3 CHAR(14) FOR BIT DATA, vcbd1 VARCHAR(264) FOR BIT DATA, vcbd2 VARCHAR(4000) FOR BIT DATA, vcbd3 VARCHAR(1) FOR BIT DATA, lvcbd1 LONG VARCHAR FOR BIT DATA, lvcbd2 LONG VARCHAR FOR BIT DATA)");
			PreparedStatement ps = conn.prepareStatement("insert into t2(cbd0, cbd2, cbd3, vcbd3) values (?, ?,?,?)");
			ps.setBytes(1, columnCBD0value);
			ps.setBytes(2, columnCBD2value);
			ps.setBytes(3, columnCBD3value);
			ps.setBytes(4, columnVCBD3value);
			ps.executeUpdate();
			ps = conn.prepareStatement("update t2 set lvcbd2 = ?");
			ps.setBytes(1, columnLVCBD2value);
			ps.executeUpdate();

			System.out.println("Test2a - CHAR FOR BIT DATA concatenations will give result type of CHAR FOR BIT DATA when concatenated string < 255");
			//operands CHAR(A) FOR BIT DATA, CHAR(B) FOR BIT DATA and A+B<255 then result is CHAR(A+B) FOR BIT DATA
			columnCBD1value = new byte[254];
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 0, columnCBD2value.length);
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 40, columnCBD2value.length);
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 80, columnCBD2value.length);
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 120, columnCBD2value.length);
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 160, columnCBD2value.length);
			System.arraycopy(columnCBD2value, 0, columnCBD1value, 200, columnCBD2value.length);
			System.arraycopy(columnCBD3value, 0, columnCBD1value, 240, columnCBD3value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd3 from t2)"), concatenatedSQLTypesNames[4]);
			verifyByteData(s.executeQuery("values(select cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd3 from t2)"), columnCBD1value);
			s.executeUpdate("update t2 set cbd1 = cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd2 || cbd3");
			verifyByteData(s.executeQuery("select cbd1 from t2"), columnCBD1value);

			System.out.println("Test2b boundary test - CHAR FOR BIT DATA concatenations will give result type of VARCHAR FOR BIT DATA when concatenated string = 255");
			//operands CHAR(A) FOR BIT DATA, CHAR(B) FOR BIT DATA and A+B>254 then result is VARCHAR(A+B) FOR BIT DATA
			columnVCBD1value = new byte[255];
			System.arraycopy(columnCBD1value, 0, columnVCBD1value, 0, columnCBD1value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 254, columnCBD0value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd1 || cbd0 from t2)"), concatenatedSQLTypesNames[5]);
			verifyByteData(s.executeQuery("values(select cbd1 || cbd0 from t2)"), columnVCBD1value);
			s.executeUpdate("update t2 set vcbd1 = cbd1 || cbd0");
			verifyByteData(s.executeQuery("select vcbd1 from t2"), columnVCBD1value);

			System.out.println("Test2b - CHAR FOR BIT DATA concatenations will give result type of VARCHAR FOR BIT DATA when concatenated string > 254");
			//operands CHAR(A) FOR BIT DATA, CHAR(B) FOR BIT DATA and A+B>254 then result is VARCHAR(A+B) FOR BIT DATA
			columnVCBD1value = new byte[264];
			System.arraycopy(columnCBD1value, 0, columnVCBD1value, 0, columnCBD1value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 254, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 255, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 256, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 257, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 258, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 259, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 260, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 261, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 262, columnCBD0value.length);
			System.arraycopy(columnCBD0value, 0, columnVCBD1value, 263, columnCBD0value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd1 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 from t2)"), concatenatedSQLTypesNames[5]);
			verifyByteData(s.executeQuery("values(select cbd1 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0  from t2)"), columnVCBD1value);
			s.executeUpdate("update t2 set vcbd1 = cbd1 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0 || cbd0");
			verifyByteData(s.executeQuery("select vcbd1 from t2"), columnVCBD1value);

			System.out.println("Test2c - CHAR FOR BIT DATA and VARCHAR FOR BIT DATA concatenations will give result type of VARCHAR FOR BIT DATA when concatenated string < 4001");
			//operands CHAR(A) FOR BIT DATA, VARCHAR(B) FOR BIT DATA and A+B<4001 then result is VARCHAR(A+B) FOR BIT DATA
			//concatenated string 4000 characters long in following updates
			columnLVCBD1value = new byte[4000];
			columnVCBD2value = new byte[4000];
			System.arraycopy(columnCBD2value, 0, columnLVCBD1value, 0, columnCBD2value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 40, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 304, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 568, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 832, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1096, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1360, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1624, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1888, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2152, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2416, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2680, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2944, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3208, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3472, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3736, columnVCBD1value.length);
			System.arraycopy(columnLVCBD1value, 0, columnVCBD2value, 0, columnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd2||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1 from t2)"), concatenatedSQLTypesNames[5]);
			verifyByteData(s.executeQuery("values(select cbd2||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1 from t2)"), columnVCBD2value);
			s.executeUpdate("update t2 set vcbd2 = cbd2||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1");
			verifyByteData(s.executeQuery("select vcbd2 from t2"), columnVCBD2value);
			s.executeUpdate("update t2 set lvcbd1 = cbd2||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2d - VARCHAR FOR BIT DATA and CHAR FOR BIT DATA concatenations will give result type of VARCHAR FOR BIT DATA when concatenated string < 4001");
			//operands VARCHAR(A) FOR BIT DATA, CHAR(B) FOR BIT DATA and A+B<4001 then result is VARCHAR(A+B) FOR BIT DATA
			//concatenated string 4000 characters long in following updates
			columnLVCBD1value = new byte[4000];
			columnVCBD2value = new byte[4000];
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 0, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 264, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 528, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 792, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1056, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1320, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1584, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 1848, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2112, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2376, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2640, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 2904, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3168, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3432, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 3696, columnVCBD1value.length);
			System.arraycopy(columnCBD2value, 0, columnLVCBD1value, 3960, columnCBD2value.length);
			System.arraycopy(columnLVCBD1value, 0, columnVCBD2value, 0, columnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||cbd2 from t2)"), concatenatedSQLTypesNames[5]);
			verifyByteData(s.executeQuery("values(select vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||cbd2 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set vcbd2 = vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||cbd2");
			verifyByteData(s.executeQuery("select vcbd2 from t2"), columnVCBD2value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||vcbd1||cbd2");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2e boundary test - CHAR FOR BIT DATA and VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA when concatenated string > 4000");
			//operands CHAR(A) FOR BIT DATA, VARCHAR(B) FOR BIT DATA and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
			//concatenated string is > 4000 characters long in following 2 updates
			columnLVCBD1value = new byte[4001];
			System.arraycopy(columnCBD0value, 0, columnLVCBD1value, 0, columnCBD0value.length);
			System.arraycopy(columnVCBD2value, 0, columnLVCBD1value, 1, columnVCBD2value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd0||vcbd2 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select cbd0||vcbd2 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = cbd0||vcbd2 ");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2f boundary test - VARCHAR FOR BIT DATA and CHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA when concatenated string > 4000");
			//operands VARCHAR(A) FOR BIT DATA, CHAR(B) FOR BIT DATA and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
			columnLVCBD1value = new byte[4001];
			System.arraycopy(columnVCBD2value, 0, columnLVCBD1value, 0, columnVCBD2value.length);
			System.arraycopy(columnCBD0value, 0, columnLVCBD1value, 4000, columnCBD0value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd2 || cbd0 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select vcbd2 || cbd0 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd2 || cbd0");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2g - CHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA");
			//operands CHAR(A) FOR BIT DATA, LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
			byte[] tmpColumnLVCBD1value = new byte[4001];
			System.arraycopy(columnLVCBD1value, 0, tmpColumnLVCBD1value, 0, columnLVCBD1value.length);
			columnLVCBD1value = new byte[4002];
			System.arraycopy(columnCBD0value, 0, columnLVCBD1value, 0, columnCBD0value.length);
			System.arraycopy(tmpColumnLVCBD1value, 0, columnLVCBD1value, 1, tmpColumnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd0 || lvcbd1 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select cbd0 || lvcbd1 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = cbd0 || lvcbd1");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2h - VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA");
			//operands VARCHAR(A) FOR BIT DATA, LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
			tmpColumnLVCBD1value = new byte[4002];
			System.arraycopy(columnLVCBD1value, 0, tmpColumnLVCBD1value, 0, columnLVCBD1value.length);
			columnLVCBD1value = new byte[4266];
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 0, columnVCBD1value.length);
			System.arraycopy(tmpColumnLVCBD1value, 0, columnLVCBD1value, 264, tmpColumnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || lvcbd1 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select vcbd1 || lvcbd1 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd1 || lvcbd1");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2i - VARCHAR FOR BIT DATA and VARCHAR FOR BIT DATA concatenations will give result type of VARCHAR FOR BIT DATA when concatenated string < 4001");
			//operands VARCHAR(A) FOR BIT DATA, VARCHAR(B) FOR BIT DATA and A+B<4001 then result is VARCHAR(A+B) FOR BIT DATA
			columnLVCBD1value = new byte[528];
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 0, columnVCBD1value.length);
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 264, columnVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || vcbd1 from t2)"), concatenatedSQLTypesNames[5]);
			verifyByteData(s.executeQuery("values(select vcbd1 || vcbd1 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd1 || vcbd1");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2j boundary test - VARCHAR FOR BIT DATA and VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA when concatenated string = 4001");
			//operands VARCHAR(A) FOR BIT DATA, VARCHAR(B) FOR BIT DATA and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
			columnLVCBD1value = new byte[4001];
			System.arraycopy(columnVCBD2value, 0, columnLVCBD1value, 0, columnVCBD2value.length);
			System.arraycopy(columnVCBD3value, 0, columnLVCBD1value, 4000, columnVCBD3value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd2 || vcbd3 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select vcbd2 || vcbd3 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd2 || vcbd3");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2j - VARCHAR FOR BIT DATA and VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA when concatenated string > 4000");
			//operands VARCHAR(A) FOR BIT DATA, VARCHAR(B) FOR BIT DATA and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
			columnLVCBD1value = new byte[columnVCBD1value.length + columnVCBD2value.length];
			System.arraycopy(columnVCBD1value, 0, columnLVCBD1value, 0, columnVCBD1value.length);
			System.arraycopy(columnVCBD2value, 0, columnLVCBD1value, columnVCBD1value.length, columnVCBD2value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || vcbd2 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select vcbd1 || vcbd2 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = vcbd1 || vcbd2");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2k - LONG VARCHAR FOR BIT DATA and LONG VARCHAR FOR BIT DATA concatenations will give result type of LONG VARCHAR FOR BIT DATA");
			//operands LONG VARCHAR FOR BIT DATA, LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
			tmpColumnLVCBD1value = new byte[columnLVCBD1value.length];
			System.arraycopy(columnLVCBD1value, 0, tmpColumnLVCBD1value, 0, columnLVCBD1value.length);
			columnLVCBD1value = new byte[tmpColumnLVCBD1value.length + tmpColumnLVCBD1value.length];
			System.arraycopy(tmpColumnLVCBD1value, 0, columnLVCBD1value, 0, tmpColumnLVCBD1value.length);
			System.arraycopy(tmpColumnLVCBD1value, 0, columnLVCBD1value, tmpColumnLVCBD1value.length, tmpColumnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvcbd1 || lvcbd1 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select lvcbd1 || lvcbd1 from t2)"), columnLVCBD1value);
			s.executeUpdate("update t2 set lvcbd1 = lvcbd1 || lvcbd1");
			verifyByteData(s.executeQuery("select lvcbd1 from t2"), columnLVCBD1value);

			System.out.println("Test2l - CHAR FOR BIT DATA and LONGVARCHAR FOR BIT DATA concatenation resulting in concatenated string > 32700");
			byte[] tmpColumnLVCBD2value = new byte[columnLVCBD2value.length];
			System.arraycopy(columnLVCBD2value, 0, tmpColumnLVCBD2value, 0, columnLVCBD2value.length);
			columnLVCBD2value = new byte[columnCBD2value.length + tmpColumnLVCBD2value.length];
			System.arraycopy(columnCBD2value, 0, columnLVCBD2value, 0, columnCBD2value.length);
			System.arraycopy(tmpColumnLVCBD2value, 0, columnLVCBD2value, columnCBD2value.length, tmpColumnLVCBD2value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd2 || lvcbd2 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select cbd2 || lvcbd2 from t2)"), columnLVCBD2value);
			s.executeUpdate("update t2 set lvcbd2 = cbd2 || lvcbd2");
			verifyByteData(s.executeQuery("select lvcbd2 from t2"), columnLVCBD2value);

			System.out.println("Test2m - VARCHAR FOR BIT DATA and LONGVARCHAR FOR BIT DATA concatenation resulting in concatenated string > 32700");
			tmpColumnLVCBD2value = new byte[columnLVCBD2value.length];
			System.arraycopy(columnLVCBD2value, 0, tmpColumnLVCBD2value, 0, columnLVCBD2value.length);
			columnLVCBD2value = new byte[columnVCBD1value.length + tmpColumnLVCBD2value.length];
			System.arraycopy(columnVCBD1value, 0, columnLVCBD2value, 0, columnVCBD1value.length);
			System.arraycopy(tmpColumnLVCBD2value, 0, columnLVCBD2value, columnVCBD1value.length, tmpColumnLVCBD2value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || lvcbd2 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select vcbd1 || lvcbd2 from t2)"), columnLVCBD2value);
			s.executeUpdate("update t2 set lvcbd2 = vcbd1 || lvcbd2");
			verifyByteData(s.executeQuery("select lvcbd2 from t2"), columnLVCBD2value);

			System.out.println("Test2n - LONGVARCHAR FOR BIT DATA and LONGVARCHAR FOR BIT DATA concatenation resulting in concatenated string > 32700");
			tmpColumnLVCBD2value = new byte[columnLVCBD2value.length];
			System.arraycopy(columnLVCBD2value, 0, tmpColumnLVCBD2value, 0, columnLVCBD2value.length);
			columnLVCBD2value = new byte[columnLVCBD1value.length + tmpColumnLVCBD2value.length];
			System.arraycopy(columnLVCBD1value, 0, columnLVCBD2value, 0, columnLVCBD1value.length);
			System.arraycopy(tmpColumnLVCBD2value, 0, columnLVCBD2value, columnLVCBD1value.length, tmpColumnLVCBD2value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvcbd1 || lvcbd2 from t2)"), concatenatedSQLTypesNames[6]);
			verifyByteData(s.executeQuery("values(select lvcbd1 || lvcbd2 from t2)"), columnLVCBD2value);
			s.executeUpdate("update t2 set lvcbd2 = lvcbd1 || lvcbd2");
			verifyByteData(s.executeQuery("select lvcbd2 from t2"), columnLVCBD2value);

			try { //this is if we ever run the test against DB2, we want to make sure table doesn't already exist in DB2
			s.executeUpdate("drop table testBLOB_MAIN");
			} catch(Exception ex) {}
			s.executeUpdate("create table testBLOB_MAIN (cbd1 CHAR(10) FOR BIT DATA, vcbd1 VARCHAR(100) FOR BIT DATA, lvcbd1 LONG VARCHAR FOR BIT DATA, blob1 BLOB(2G), blob2 BLOB(256), blob3 BLOB(1M))");
			ps = conn.prepareStatement("insert into testBLOB_MAIN values(?,?,?,?,?,?)");
			columnCBD1value = new byte[10];
			java.util.Arrays.fill(columnCBD1value, (byte)'a');
			ps.setBytes(1, columnCBD1value);
			columnVCBD1value = new byte[10];
			java.util.Arrays.fill(columnVCBD1value, (byte)'b');
			ps.setBytes(2, columnVCBD1value);
			columnLVCBD1value = new byte[10];
			java.util.Arrays.fill(columnLVCBD1value, (byte)'c');
			ps.setBytes(3, columnLVCBD1value);
			byte[] columnBLOB1value = new byte[10];
			java.util.Arrays.fill(columnBLOB1value, (byte)'d');
			ps.setBytes(4, columnBLOB1value);
			byte[] columnBLOB2value = new byte[10];
			java.util.Arrays.fill(columnBLOB2value, (byte)'e');
			ps.setBytes(5, columnBLOB2value);
			byte[] columnBLOB3value = new byte[10];
			java.util.Arrays.fill(columnBLOB3value, (byte)'f');
			ps.setBytes(6, columnBLOB3value);
			ps.executeUpdate();

			System.out.println("Test2o - CHAR(A) FOR BIT DATA and BLOB(B) concatenations will give result type of BLOB(A+B) when A+B<2G");
			byte[] tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[columnCBD1value.length + tmpValue.length];
			System.arraycopy(columnCBD1value, 0, columnBLOB2value, 0, columnCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB2value, columnCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd1 || blob2 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select cbd1 || blob2 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = cast((cbd1 || blob2) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2p - BLOB(A) and CHAR(B) FOR BIT DATA concatenations will give result type of BLOB(A+B) when A+B<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[tmpValue.length + columnCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB2value, 0, tmpValue.length);
			System.arraycopy(columnCBD1value, 0, columnBLOB2value, tmpValue.length, columnCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob2 || cbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob2 || cbd1 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 =  cast((blob2 || cbd1) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2q - CHAR(A) FOR BIT DATA and BLOB(B) concatenations will give result type of BLOB(2G) when A+B>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[columnCBD1value.length + tmpValue.length];
			System.arraycopy(columnCBD1value, 0, columnBLOB1value, 0, columnCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB1value, columnCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select cbd1 || blob1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select cbd1 || blob1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((cbd1 || blob1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test2r - BLOB(A) and CHAR(B) FOR BIT DATA concatenations will give result type of BLOB(2G) when A+B>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[tmpValue.length + columnCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB1value, 0, tmpValue.length);
			System.arraycopy(columnCBD1value, 0, columnBLOB1value, tmpValue.length, columnCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob1 || cbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob1 || cbd1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((blob1 || cbd1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test2s - VARCHAR(A) FOR BIT DATA and BLOB(B) concatenations will give result type of BLOB(A+B) when A+B<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[columnVCBD1value.length + tmpValue.length];
			System.arraycopy(columnVCBD1value, 0, columnBLOB2value, 0, columnVCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB2value, columnVCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || blob2 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select vcbd1 || blob2 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = cast((vcbd1 || blob2) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2t - BLOB(A) and VARCHAR(B) FOR BIT DATA concatenations will give result type of BLOB(A+B) when A+B<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[tmpValue.length + columnVCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB2value, 0, tmpValue.length);
			System.arraycopy(columnVCBD1value, 0, columnBLOB2value, tmpValue.length, columnVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob2 || vcbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob2 || vcbd1 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = cast((blob2 || vcbd1) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2u - VARCHAR(A) FOR BIT DATA and BLOB(B) concatenations will give result type of BLOB(2G) when A+B>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[columnVCBD1value.length + tmpValue.length];
			System.arraycopy(columnVCBD1value, 0, columnBLOB1value, 0, columnVCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB1value, columnVCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select vcbd1 || blob1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select vcbd1 || blob1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((vcbd1 || blob1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test2v - BLOB(A) and VARCHAR(B) FOR BIT DATA concatenations will give result type of BLOB(2G) when A+B>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[tmpValue.length + columnVCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB1value, 0, tmpValue.length);
			System.arraycopy(columnVCBD1value, 0, columnBLOB1value, tmpValue.length, columnVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob1 || vcbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob1 || vcbd1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((blob1 || vcbd1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test2w - LONG VARCHAR FOR BIT DATA and BLOB(A) concatenations will give result type of BLOB(A+32K) when A+32K<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[columnLVCBD1value.length + tmpValue.length];
			System.arraycopy(columnLVCBD1value, 0, columnBLOB2value, 0, columnLVCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB2value, columnLVCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvcbd1 || blob2 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select lvcbd1 || blob2 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = cast((lvcbd1 || blob2) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2x - BLOB(A) and LONG VARCHAR FOR BIT DATA concatenations will give result type of BLOB(A+32K) when A+32K<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[tmpValue.length + columnLVCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB2value, 0, tmpValue.length);
			System.arraycopy(columnLVCBD1value, 0, columnBLOB2value, tmpValue.length, columnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob2 || lvcbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob2 || lvcbd1 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = cast((blob2 || lvcbd1) as blob(256))");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test2y - LONG VARCHAR FOR BIT DATA and BLOB(B) concatenations will give result type of BLOB(2G) when A+32K>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[columnLVCBD1value.length + tmpValue.length];
			System.arraycopy(columnLVCBD1value, 0, columnBLOB1value, 0, columnLVCBD1value.length);
			System.arraycopy(tmpValue, 0, columnBLOB1value, columnLVCBD1value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select lvcbd1 || blob1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select lvcbd1 || blob1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((lvcbd1 || blob1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test2z - BLOB(A) and LONG VARCHAR FOR BIT DATA concatenations will give result type of BLOB(2G) when A+32K>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[tmpValue.length + columnLVCBD1value.length];
			System.arraycopy(tmpValue, 0, columnBLOB1value, 0, tmpValue.length);
			System.arraycopy(columnLVCBD1value, 0, columnBLOB1value, tmpValue.length, columnLVCBD1value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob1 || lvcbd1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob1 || lvcbd1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = cast((blob1 || lvcbd1) as blob(2G))");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test21a - BLOB(A) and BLOB(B) concatenations will give result type of BLOB(A+B) when A+B<2G");
			tmpValue = new byte[columnBLOB2value.length];
			System.arraycopy(columnBLOB2value, 0, tmpValue, 0, columnBLOB2value.length);
			columnBLOB2value = new byte[tmpValue.length + columnBLOB3value.length];
			System.arraycopy(tmpValue, 0, columnBLOB2value, 0, tmpValue.length);
			System.arraycopy(columnBLOB3value, 0, columnBLOB2value, tmpValue.length, columnBLOB3value.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob2 || blob3 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob2 || blob3 from testBLOB_MAIN)"), columnBLOB2value);
			s.executeUpdate("update testBLOB_MAIN set blob2 = blob2 || blob3");
			verifyByteData(s.executeQuery("select blob2 from testBLOB_MAIN"), columnBLOB2value);

			System.out.println("Test21b - BLOB(A) and BLOB(B) concatenations will give result type of BLOB(2G) when A+B>2G");
			tmpValue = new byte[columnBLOB1value.length];
			System.arraycopy(columnBLOB1value, 0, tmpValue, 0, columnBLOB1value.length);
			columnBLOB1value = new byte[columnBLOB2value.length + tmpValue.length];
			System.arraycopy(columnBLOB2value, 0, columnBLOB1value, 0, columnBLOB2value.length);
			System.arraycopy(tmpValue, 0, columnBLOB1value, columnBLOB2value.length, tmpValue.length);
			dumpSomeMetaDataInfo(s.executeQuery("values(select blob2 || blob1 from testBLOB_MAIN)"), concatenatedSQLTypesNames[7]);
			verifyByteData(s.executeQuery("values(select blob2 || blob1 from testBLOB_MAIN)"), columnBLOB1value);
			s.executeUpdate("update testBLOB_MAIN set blob1 = blob2 || blob1");
			verifyByteData(s.executeQuery("select blob1 from testBLOB_MAIN"), columnBLOB1value);

			System.out.println("Test22 - try 2 empty char for bit data concatenation and verify that length comes back as 0 for the result");
			dumpSomeMetaDataInfo(s.executeQuery("values(X'' || X'')"), concatenatedSQLTypesNames[4]);

			s.executeUpdate("drop table testBLOB_MAIN");  
			s.executeUpdate("drop table t2");
			System.out.println("Test2 finished - CHAR FOR BIT DATA, VARCHAR FOR BIT DATA, LONGVARCHAR FOR BIT DATA and BLOB concatenation tests");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	private static void verifyStringData(ResultSet rs, String expectedValue) throws SQLException
	{
		if (rs == null)
		{
			System.out.println("<NULL>");
			return;
		}

		rs.next();
		if (!(rs.getString(1).equals(expectedValue))) {
			System.out.println("ERROR: expected value does not match actual value");
			System.out.println("expected value is " + expectedValue);
			System.out.println("what we got here is " + rs.getString(1));
		}
		else
		{
			System.out.println("Successful " + rs.getMetaData().getColumnTypeName(1) + " read of " + expectedValue.length() + " characters");
		}
	}

	private static void verifyByteData(ResultSet rs, byte[] expectedValue) throws SQLException
	{
		if (rs == null)
		{
			System.out.println("<NULL>");
			return;
		}

		rs.next();
		if (!(java.util.Arrays.equals(rs.getBytes(1),expectedValue))) {
			System.out.println("ERROR: expected value does not match actual value");
			System.out.println("expected value is " + expectedValue);
			System.out.println("what we got here is " + rs.getBytes(1));
		}
		else
		{
			System.out.println("Successful " + rs.getMetaData().getColumnTypeName(1) + " read of " + expectedValue.length + " bytes");
		}
	}

	private static void dumpSomeMetaDataInfo(ResultSet s, String expectedTypeName) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		StringBuffer heading = new StringBuffer("\t ");

		// Display column headings
		for (int i=1; i<=numCols; i++)
		{
			System.out.println("datatype of concatenated string is : "+rsmd.getColumnTypeName(i));
			if (!(rsmd.getColumnTypeName(i).equals(expectedTypeName)))
				System.out.println("FAIL : expected datatype of concatenated string is : "+expectedTypeName);
			System.out.println("precision of concatenated string is : "+rsmd.getPrecision(i));
		}
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}

}

/* 

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.streamingColumn

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.iapi.reference.Limits;
import java.io.*;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.util.zip.CRC32;
import java.util.Properties;

/**
 * Test of JDBC result set Stream calls.
 *
 * @author djd
 */

public class streamingColumn { 

			// set up a short (fit in one page) inputstream for insert
	static String[] fileName;
	static long[] fileLength;

	static
	{
		int numFiles = 4;
		fileName = new String[numFiles];
		fileLength = new long[numFiles];

		fileName[0] = "extin/short.data";	// set up a short (fit in one page) inputstream for insert
		fileName[1] = "extin/shortbanner"; // set up a long (longer than a page) inputstream for insert
		fileName[2] = "extin/derby.banner"; // set up a really long (over 300K) inputstream for insert
		fileName[3] = "extin/empty.data"; // set up a file with nothing in it
	}

	private static final int LONGVARCHAR = 1;
    private static final int CLOB = 2;
    private static final int VARCHAR = 3;
    
	public static void main(String[] args) {

		System.out.println("Test streamingColumn starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();

			streamTest1(conn);

			// test column size 1500 bytes
			streamTest2(conn, 1500);
			// test column size 5000 butes
			streamTest2(conn, 5000);
			streamTest2(conn, 10000);

			streamTest3(conn, 0);
			streamTest3(conn, 1500);
			streamTest3(conn, 5000);
			streamTest3(conn, 10000);

			streamTest4(conn);

			streamTest5(conn, 0);
			streamTest5(conn, 1500);
			streamTest5(conn, 5000);
        //  This test fails when running w/ derby.language.logStatementText=true
        //  see DERBY-595 
			//streamTest5(conn, 100000);

			streamTest6(conn, 5000);
			streamTest7(conn);

            // test 1st column fit, second column doesn't
            streamTest8(conn, 10, 2500);
            streamTest9(conn, 10, 2500);

            // test 1st column doesn't fit, second column does
            streamTest8(conn, 2500, 10);
            streamTest9(conn, 2500, 10);

			// test compressTable
			streamTest10(conn);

			// bug 5592 test negativte length for the setXXStream methods. Should fail.
			streamTest11(conn);

			// bug 5592 test - only non-blank character truncation should give error for varchars
			streamTest12(conn);

			// bug 5592 test - any character(including blank character) truncation should give error for long varchars
			streamTest13(conn);

            // Test clob truncation, behavior similar to varchar
            // trailingspaces are truncated but if there are trailing non-blanks then
            // exception is thrown
            // This test is similar to streamTest12.
            streamTest14(conn);
            
            
            // Derby500
            // user supplied stream parameter values are not re-used
            derby500Test(conn);

            // currently in case of char,varchar,long varchar types
            // stream paramter value is materialized the first time around
            // and used for executions. Hence verify that the fix to 
            // DERBY-500 did not change the behavior for char,varchar
            // and long varchar types when using streams.
            derby500_verifyVarcharStreams(conn);
            
			// turn autocommit on because in JCC, java.sql.Connection.close() can not be
			// requested while a transaction is in progress on the connection.
			// If autocommit is off in JCC, the transaction remains active, 
			// and the connection cannot be closed.
			// If autocommit is off in Derby, an invalid transaction state SQL exception is thrown.
			conn.setAutoCommit(true);
			conn.close();

		} catch (SQLException e) {
			dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		System.out.println("Test streamingColumn finished");
    }

	private static void streamTest1(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;

		try {
			stmt = conn.createStatement();
			stmt.execute("create table testLongVarChar (a int, b long varchar)");
			// insert a null long varchar
			stmt.execute("insert into testLongVarChar values(1, '')");
			// insert a long varchar with a short text string
			stmt.execute("insert into testLongVarChar values(2, 'test data: a string column inserted as an object')");


			for (int i = 0; i < fileName.length ; i++) {
				// prepare an InputStream from the file
				File file = new File(fileName[i]);
				fileLength[i] = file.length();
				InputStream fileIn = new FileInputStream(file);

				System.out.println("===> testing " + fileName[i] + " length = "
								   + fileLength[i]);

				// insert a streaming column
				PreparedStatement ps = conn.prepareStatement("insert into testLongVarChar values(?, ?)");
				ps.setInt(1, 100 + i);
				ps.setAsciiStream(2, fileIn, (int)fileLength[i]);
				try {//if trying to insert data > 32700, there will be an exception
					ps.executeUpdate();
					System.out.println("No truncation and hence no error");
				}
				catch (SQLException e) {
					if (fileLength[i] > Limits.DB2_LONGVARCHAR_MAXWIDTH && e.getSQLState().equals("22001")) //was getting data longer than maxValueAllowed
						System.out.println("expected exception for data > " + Limits.DB2_LONGVARCHAR_MAXWIDTH + " in length");
					else
						dumpSQLExceptions(e);
				}
				fileIn.close();
			}

			rs = stmt.executeQuery("select a, b from testLongVarChar");
			met = rs.getMetaData();
			byte[] buff = new byte[128];
			// fetch all rows back, get the long varchar columns as streams.
			while (rs.next()) {
				// get the first column as an int
				int a = rs.getInt("a");
				// get the second column as a stream
				InputStream fin = rs.getAsciiStream(2);
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
				verifyLength(a, columnSize, fileLength);
			}

			rs = stmt.executeQuery("select a, b from testLongVarChar order by a");
			met = rs.getMetaData();
			// fetch all rows back in order, get the long varchar columns as streams.
			while (rs.next()) {
				// get the first column as an int
				int a = rs.getInt("a");
				// get the second column as a stream
				InputStream fin = rs.getAsciiStream(2);
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
				verifyLength(a, columnSize, fileLength);
			}

			rs = stmt.executeQuery("select a, b from testLongVarChar");
			// fetch all rows back, get the long varchar columns as Strings.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);
				verifyLength(a, resultString.length(), fileLength);
			}

			rs = stmt.executeQuery("select a, b from testLongVarChar order by a");
			// fetch all rows back in order, get the long varchar columns as Strings.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);
				verifyLength(a, resultString.length(), fileLength);
			}

			rs = stmt.executeQuery(
				"select a, b from testLongVarChar where b like 'test data: a string column inserted as an object'");
			// should return one row.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);
				verifyLength(a, resultString.length(), fileLength);
			}

			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
			stmt.executeUpdate("create table foo (a int not null, b long varchar, primary key (a))");
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
			insertLongString(conn, 10, "ssssssssss", false);


			insertLongString(conn, 0, "", false);
			insertLongString(conn, 1, "1", false);
			insertLongString(conn, -1, null, false);
			insertLongString(conn, 20, "XXXXXXXXXXXXXXXXXXXX", false);

			rs = stmt.executeQuery("select a, b from foo");

			System.out.println("expect to get null string back");
			while(rs.next())
			{
				int a = rs.getInt("a");
				String resultString = rs.getString(2);
				if (resultString == null)
				{
					System.out.println("a = " + a + " got null string back");
				}
				else if (resultString.length() != a)
				{
					System.out.println("FAIL - failed to get string back, expect "+
									   a + " got " + resultString.length());
				}
			}

			updateLongString(conn, 1, 3000);
			updateLongString(conn, 0, 800);
			updateLongString(conn, 3000, 0);
			updateLongString(conn, 0, 51);
			updateLongString(conn, 20, 0);
			rs = stmt.executeQuery("select a, b from foo");
			while(rs.next())
			{
				int a = rs.getInt("a");
				String resultString = rs.getString(2);
				if (resultString == null)
				{
					System.out.println("a = " + a + " got null string back");
				}
				else if (resultString.length() != a)
				{
					System.out.println("FAIL - failed to get string back, expect "+
									   a + " got " + resultString.length() +
									   " " + resultString);
				}
			}

			stmt.executeUpdate("drop table foo");

			rs.close();
			stmt.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	}

	static void streamTest2(Connection conn, long length) throws Exception
	{
		Statement sourceStmt = conn.createStatement();

		sourceStmt.executeUpdate("create table foo (a int not null, b long varchar, primary key (a))");

		insertLongString(conn, 1, pad("Broadway", length), false);
		insertLongString(conn, 2, pad("Franklin", length), false);
		insertLongString(conn, 3, pad("Webster", length), false);

		sourceStmt.executeUpdate("insert into foo select a+100, b from foo");

		verifyExistence(conn, 1, "Broadway", length);
		verifyExistence(conn, 2, "Franklin", length);
		verifyExistence(conn, 3, "Webster", length);
		verifyExistence(conn, 101, "Broadway", length);
		verifyExistence(conn, 102, "Franklin", length);
		verifyExistence(conn, 103, "Webster", length);

		sourceStmt.executeUpdate("drop table foo");
	}

	static void streamTest3(Connection conn, long length) throws Exception
	{
		Statement sourceStmt = conn.createStatement();
		sourceStmt.executeUpdate("create table foo (a int not null constraint pk primary key, b long varchar)");

		insertLongString(conn, 1, pad("Broadway", length), false);
		insertLongString(conn, 2, pad("Franklin", length), false);
		insertLongString(conn, 3, pad("Webster", length), false);
		PreparedStatement ps = conn.prepareStatement(
			"update foo set a=a+1000, b=? where a<99 and a in (select a from foo)");

		File file = new File("extin/short.data");
		InputStream fileIn = new FileInputStream(file);
		ps.setAsciiStream(1, fileIn, (int)(file.length()));
		ps.executeUpdate();
		fileIn.close();

		ps = conn.prepareStatement(
			"update foo set a=a+1000, b=? where a<99 and a in (select a from foo)");
		file = new File("extin/shortbanner");
		fileIn = new FileInputStream(file);
		ps.setAsciiStream(1, fileIn, (int)(file.length()));
		ps.executeUpdate();
		fileIn.close();

		sourceStmt.executeUpdate("drop table foo");
	}

	private static void streamTest4(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;

		try {
			stmt = conn.createStatement();
			stmt.execute("create table testLongVarBinary (a int, b BLOB(1G))");
			// insert an empty string 
			stmt.execute("insert into testLongVarBinary values(1, CAST (" +
						 TestUtil.stringToHexLiteral("") + "AS BLOB(1G)))");
			// insert a short text string
			stmt.execute("insert into testLongVarBinary values(2,CAST (" +
						 TestUtil.stringToHexLiteral("test data: a string column inserted as an object") + "AS BLOB(1G)))");

			for (int i = 0; i < fileName.length; i++) {
				// prepare an InputStream from the file
				File file = new File(fileName[i]);
				fileLength[i] = file.length();
				InputStream fileIn = new FileInputStream(file);

				System.out.println("===> testing " + fileName[i] + " length = "
								   + fileLength[i]);

				// insert a streaming column
				PreparedStatement ps = conn.prepareStatement("insert into testLongVarBinary values(?, ?)");
				ps.setInt(1, 100 + i);
				ps.setBinaryStream(2, fileIn, (int)fileLength[i]);
				ps.executeUpdate();
				fileIn.close();
			}

			rs = stmt.executeQuery("select a, b from testLongVarBinary");
			met = rs.getMetaData();
			byte[] buff = new byte[128];
			// fetch all rows back, get the long varchar columns as streams.
			while (rs.next()) {
				// get the first column as an int
				int a = rs.getInt("a");
				// get the second column as a stream
				InputStream fin = rs.getBinaryStream(2);
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff, 0, 100);
					if (size == -1)
						break;
					columnSize += size;
				}
			}

			rs = stmt.executeQuery("select a, b from testLongVarBinary order by a");
			met = rs.getMetaData();
			// fetch all rows back in order, get the long varchar columns as streams.
			while (rs.next()) {
				// get the first column as an int
				int a = rs.getInt("a");
				// get the second column as a stream
				InputStream fin = rs.getBinaryStream(2);
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
			}

			rs = stmt.executeQuery("select a, b from testLongVarBinary");
			// fetch all rows back, get the long varchar columns as Strings.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);
			}

			rs = stmt.executeQuery("select a, b from testLongVarBinary order by a");
			// fetch all rows back in order, get the long varchar columns as Strings.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);
			}

			rs.close();
			stmt.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	}

	static void streamTest5(Connection conn, long length) throws Exception
	{
		Statement sourceStmt = conn.createStatement();
		String binaryType = length > 32700 ? "BLOB(1G)" : "long varchar for bit data";
		sourceStmt.executeUpdate("create table foo (a int not null constraint pk primary key, b " + binaryType + " )");

		insertLongString(conn, 1, pad("Broadway", length), true);
		insertLongString(conn, 2, pad("Franklin", length), true);
		insertLongString(conn, 3, pad("Webster", length), true);
		insertLongString(conn, 4, pad("Broadway", length), true);
		insertLongString(conn, 5, pad("Franklin", length), true);
		insertLongString(conn, 6, pad("Webster", length), true);
		PreparedStatement ps = conn.prepareStatement(
			"update foo set a=a+1000, b=? where a<99 and a in (select a from foo)");
		File file = new File("extin/short.data");
		InputStream fileIn = new FileInputStream(file);
		ps.setBinaryStream(1, fileIn, (int)(file.length()));
		ps.executeUpdate();
		fileIn.close();

		ps = conn.prepareStatement(
			"update foo set a=a+1000, b=? where a<99 and a in (select a from foo)");
		file = new File("extin/shortbanner");
		fileIn = new FileInputStream(file);
		ps.setBinaryStream(1, fileIn, (int)(file.length()));
		ps.executeUpdate();
		ps.close();
		fileIn.close();

		sourceStmt.executeUpdate("drop table foo");
	}

	static void streamTest6(Connection conn, long length) throws Exception
	{
		Statement sourceStmt = conn.createStatement();
		sourceStmt.executeUpdate("create table foo (a int not null constraint pk primary key, b long varchar)");

		insertLongString(conn, 1, pad("Broadway", length), false);
		insertLongString(conn, 2, pad("Franklin", length), false);
		insertLongString(conn, 3, pad("Webster", length), false);
		PreparedStatement ps = conn.prepareStatement(
			"update foo set a=a+1000, b=? where a<99 and a in (select a from foo)");

		streamInLongCol(ps, pad("Grand", length));
		ps.close();
		sourceStmt.close();
	}

	static void streamTest7(Connection conn) throws Exception
	{
		conn.setAutoCommit(false);

		System.out.println("streamTest7");

 		Statement s = conn.createStatement();
		s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
		s.execute("create table testlvc (a int, b char(100), lvc long varchar, d char(100))");
		s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
		s.close();
		conn.commit();

		PreparedStatement ps1 = conn.prepareStatement(
			"insert into testlvc values (?, 'filler for column b on null column', null, 'filler for column d')");

		PreparedStatement ps2 = conn.prepareStatement(
			"insert into testlvc values (?, 'filler for column b on empty string column', ?, 'filler2 for column d')");


		for (int i= 0; i < 100; i++)
		{
			ps1.setInt(1, i);
			ps1.executeUpdate();

			ByteArrayInputStream emptyString = new ByteArrayInputStream(new byte[0]);
			ps2.setInt(1, i);
			ps2.setAsciiStream(2, emptyString, 0);
			ps2.executeUpdate();
		}
		ps1.close();
		ps2.close();

		conn.commit();

		PreparedStatement ps = conn.prepareStatement("update testlvc set lvc = ? where a = ?");

		String longString =
			"this is a relatively long string, hopefully the row will be split or otherwise become long ???  I don't think it will become long but maybe if it rolls back it will become strange";
		for (int i = 0; i < 100; i++)
		{
			ByteArrayInputStream string1 = new ByteArrayInputStream(longString.getBytes());
			ps.setAsciiStream(1, string1, longString.length());
			ps.setInt(2, i);
			ps.executeUpdate();
			if ((i % 2) == 0)
				conn.rollback();
			else
				conn.commit();

			ByteArrayInputStream emptyString = new ByteArrayInputStream(new byte[0]);
			ps.setAsciiStream(1, emptyString, 0);
			ps.executeUpdate();
			if ((i%3) == 0)
				conn.rollback();
			else
				conn.commit();
		}

		ps.close();
	}

    /**
     * long row test of insert/backout case, using setAsciiStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible.  So it
     * first asks raw store to try inserting without overflowing rows or
     * columns.  If that doesn't work it then asks raw store for a mostly
     * empty page and tries to insert it there with overflow, If that doesn't
     * work then an empty page is picked.
     * <p>
     * If input parameters are conn,10,2500 - then the second row inserted
     * will have the 1st column fit, but the second not fit which caused
     * track #2240.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	static void streamTest8(Connection conn, int stream1_len, int stream2_len)
	{
        System.out.println(
            "Starting streamTest8(conn, " +
            stream1_len + ", " + stream2_len + ")");

		ResultSetMetaData   met;
		ResultSet           rs;
		Statement           stmt;

		String createsql =
            new String(
                "create table t8(a int, b long varchar, c long varchar)");

		String insertsql = new String("insert into t8 values (?, ?, ?) ");


		int numStrings = 10;

		byte[][] stream1_byte_array = new byte[numStrings][];
		byte[][] stream2_byte_array = new byte[numStrings][];

		// make string size match input sizes.
		for (int i = 0; i < numStrings; i++)
		{
			stream1_byte_array[i] = new byte[stream1_len];

			for (int j = 0; j < stream1_len; j++)
				stream1_byte_array[i][j] = (byte)('a'+i);

			stream2_byte_array[i] = new byte[stream2_len];
			for (int j = 0; j < stream2_len; j++)
				stream2_byte_array[i][j] = (byte)('A'+i);
		}

		try
		{
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			stmt.execute(createsql);
			conn.commit();

			PreparedStatement insert_ps = conn.prepareStatement(insertsql);



			for (int i = 0; i < numStrings; i++)
			{
                // create the stream and insert it
                insert_ps.setInt(1, i);

                // create the stream and insert it
                insert_ps.setAsciiStream(
                    2, new ByteArrayInputStream(stream1_byte_array[i]), stream1_len);

                // create the stream and insert it
                insert_ps.setAsciiStream(
                    3, new ByteArrayInputStream(stream2_byte_array[i]), stream2_len);

				insert_ps.executeUpdate();

                // just force a scan of the table, no insert is done.
				String checkSQL =
                    "insert into t8 select * from t8 where a = -6363";
				stmt.execute(checkSQL);
			}

			insert_ps.close();
			conn.commit();


			rs = stmt.executeQuery("select a, b, c from t8" );

			// should return one row.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				String resultString = rs.getString(2);

                // compare result with expected
                String canon = new String(stream1_byte_array[a]);

                if (canon.compareTo(resultString) != 0)
                {
                    System.out.println(
                        "FAIL -- bad result string:" +
                        "canon: " + canon +
                        "resultString: " + resultString);
                }

				// get the second column as a string
				resultString = rs.getString(3);

                // compare result with expected
                canon = new String(stream2_byte_array[a]);

                if (canon.compareTo(resultString) != 0)
                {
                    System.out.println(
                        "FAIL -- bad result string:" +
                        "canon: " + canon +
                        "resultString: " + resultString);
                }
			}

			rs.close();


			stmt.execute("insert into t8 select * from t8");

			stmt.executeUpdate("drop table t8");

			stmt.close();
			conn.commit();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

        System.out.println(
            "Finishing streamTest8(conn, " +
            stream1_len + ", " + stream2_len + ")");
	}

    /**
     * long row test of insert/backout case, using setBinaryStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible.  So it
     * first asks raw store to try inserting without overflowing rows or
     * columns.  If that doesn't work it then asks raw store for a mostly
     * empty page and tries to insert it there with overflow, If that doesn't
     * work then an empty page is picked.
     * <p>
     * If input parameters are conn,10,2500 - then the second row inserted
     * will have the 1st column fit, but the second not fit which caused
     * track #2240.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	static void streamTest9(Connection conn, int stream1_len, int stream2_len)
	{
        System.out.println(
            "Starting streamTest9(conn, " +
            stream1_len + ", " + stream2_len + ")");

		ResultSetMetaData   met;
		ResultSet           rs;
		Statement           stmt;

		String createsql =
            new String(
                "create table t9(a int, b long varchar for bit data, c long varchar for bit data)");

		String insertsql = new String("insert into t9 values (?, ?, ?) ");


		int numStrings = 10;

		byte[][] stream1_byte_array = new byte[numStrings][];
		byte[][] stream2_byte_array = new byte[numStrings][];

		// make string size match input sizes.
		for (int i = 0; i < numStrings; i++)
		{
			stream1_byte_array[i] = new byte[stream1_len];

			for (int j = 0; j < stream1_len; j++)
				stream1_byte_array[i][j] = (byte)('a'+i);

			stream2_byte_array[i] = new byte[stream2_len];
			for (int j = 0; j < stream2_len; j++)
				stream2_byte_array[i][j] = (byte)('A'+i);
		}

		try
		{
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			stmt.execute(createsql);
			conn.commit();

			PreparedStatement insert_ps = conn.prepareStatement(insertsql);



			for (int i = 0; i < numStrings; i++)
			{
                // create the stream and insert it
                insert_ps.setInt(1, i);

                // create the stream and insert it
                insert_ps.setBinaryStream(
                    2, new ByteArrayInputStream(stream1_byte_array[i]), stream1_len);

                // create the stream and insert it
                insert_ps.setBinaryStream(
                    3, new ByteArrayInputStream(stream2_byte_array[i]), stream2_len);

				insert_ps.executeUpdate();

                // just force a scan of the table, no insert is done.
				String checkSQL =
                    "insert into t9 select * from t9 where a = -6363";
				stmt.execute(checkSQL);
			}

			insert_ps.close();
			conn.commit();


			rs = stmt.executeQuery("select a, b, c from t9" );

			// should return one row.
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as an int
				int a = rs.getInt("a");

				// get the second column as a string
				byte[] resultString = rs.getBytes(2);

                // compare result with expected
                byte[] canon = stream1_byte_array[a];

                if (!byteArrayEquals(
                        canon,        0, canon.length,
                        resultString, 0, resultString.length))
                {
                    // System.out.println(
                    //   "FAIL -- bad result byte array 1:" +
                    //   "canon: " + ByteArray.hexDump(canon) +
                    //   "resultString: " + ByteArray.hexDump(resultString));
                    System.out.println(
                        "FAIL -- bad result byte array 1:" +
                        "canon: " + canon +
                        "resultString: " + resultString);
                }

				// get the second column as a string
				resultString = rs.getBytes(3);

                // compare result with expected
                canon = stream2_byte_array[a];

                if (!byteArrayEquals(
                        canon,        0, canon.length,
                        resultString, 0, resultString.length))
                {
                    // System.out.println(
                    //   "FAIL -- bad result byte array 2:" +
                    //   "canon: " + ByteArray.hexDump(canon) +
                    //   "resultString: " + ByteArray.hexDump(resultString));
                    System.out.println(
                        "FAIL -- bad result byte array 2:" +
                        "canon: " + canon +
                        "resultString: " + resultString);
                }
			}

			rs.close();

			stmt.execute("insert into t9 select * from t9");

			stmt.executeUpdate("drop table t9");

			stmt.close();
			conn.commit();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

        System.out.println(
            "Finishing streamTest9(conn, " +
            stream1_len + ", " + stream2_len + ")");
	}

    /**
     * table with multiple indexes, indexes share columns
     * table has more than 4 rows, insert stream into table
     * compress table and verify that each index is valid
	 * @exception  StandardException  Standard exception policy.
     **/
	private static void streamTest10(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println("Testing 10 starts from here");

		try {
			stmt = conn.createStatement();
            //create the table
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
			stmt.execute("create table tab10 (a int, b int, c long   varchar)");
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
            //create the indexes which shares columns
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
            stmt.execute("create index i_a on tab10 (a)");
			stmt.execute("create index i_ab on tab10 (a, b)");
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");

			// insert a null long varchar
			stmt.execute("insert into tab10 values(1, 1, '')");
			// insert a long varchar with a short text string
			stmt.execute("insert into tab10 values(2, 2, 'test data: a string column inserted as an object')");

			//insert stream into table
			for (int i = 0; i < fileName.length; i++) {
				// prepare an InputStream from the file
				File file = new File(fileName[i]);
				fileLength[i] = file.length();
				InputStream fileIn = new FileInputStream(file);

				System.out.println("===> testing " + fileName[i] + " length = "
								   + fileLength[i]);

				// insert a streaming column
				PreparedStatement ps = conn.prepareStatement("insert into tab10 values(?, ?, ?)");
				ps.setInt(1, 100 + i);
				ps.setInt(2, 100 + i);
				ps.setAsciiStream(3, fileIn, (int)fileLength[i]);
				try {//if trying to insert data > 32700, there will be an exception
					ps.executeUpdate();
					System.out.println("No truncation and hence no error");
				}
				catch (SQLException e) {
					if (fileLength[i] > Limits.DB2_LONGVARCHAR_MAXWIDTH && e.getSQLState().equals("22001")) //was getting data longer than maxValueAllowed
						System.out.println("expected exception for data > " + Limits.DB2_LONGVARCHAR_MAXWIDTH + " in length");
					else
						dumpSQLExceptions(e);
				}
				fileIn.close();
			}

			//execute the compress command
            CallableStatement cs = conn.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?)");
            cs.setString(1, "APP");
            cs.setString(2, "TESTLONGVARCHAR");
            cs.setInt(3, 0);
            cs.execute();

			//do consistency checking
			stmt.execute("CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker' LANGUAGE JAVA PARAMETER STYLE JAVA");
			stmt.execute("VALUES ConsistencyChecker()");

			stmt.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
        System.out.println("Testing 10 ends in here");
	}

	private static void streamTest11(Connection conn) {

		Statement stmt;

		System.out.println("Test 11 - Can't pass negative length as the stream length for various setXXXStream methods");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testLongVarCharInvalidStreamLength (a int, b long varchar, c long varchar for bit data)");
			// prepare an InputStream from the file
			File file = new File("extin/short.data");
			InputStream fileIn = new FileInputStream(file);

			PreparedStatement ps = conn.prepareStatement("insert into testLongVarCharInvalidStreamLength values(?, ?, ?)");
			ps.setInt(1, 100);
			try {
				System.out.println("===> testing using setAsciiStream with -2 as length");
				ps.setAsciiStream(2, fileIn, -2); //test specifically for bug 4250
				System.out.println("FAIL -- should have gotten exception for -2 param value to setAsciiStream");
			}
			catch (SQLException e) {
				if ("XJ025".equals(e.getSQLState()))
					System.out.println("PASS -- expected exception:" + e.toString());
				else
					dumpSQLExceptions(e);
			}

			Reader filer = new InputStreamReader(fileIn);
			try {
				System.out.println("===> testing using setCharacterStream with -1 as length");
				ps.setCharacterStream(2, filer, -1);
				System.out.println("FAIL -- should have gotten exception for -1 param value to setCharacterStream");
			}
			catch (SQLException e) {
				if ("XJ025".equals(e.getSQLState()))
					System.out.println("PASS -- expected exception:" + e.toString());
				else
					dumpSQLExceptions(e);
			}

			try {
				System.out.println("===> testing using setBinaryStream with -1 as length");
				ps.setBinaryStream(3, fileIn, -1);
				System.out.println("FAIL -- should have gotten exception for -1 param value to setBinaryStream");
			}
			catch (SQLException e) {
				if ("XJ025".equals(e.getSQLState()))
					System.out.println("PASS -- expected exception:" + e.toString());
				else
					dumpSQLExceptions(e);
			}

			fileIn.close();
		}
		catch (SQLException e) {
				dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	 	System.out.println("Test 11 - negative stream length tests end in here");
	}

	private static void streamTest12(Connection conn) {

		ResultSet rs;
		Statement stmt;

		//The following 2 files are for testing the truncation in varchar.
		//only non-blank character truncation will throw an exception for varchars.
		//max value allowed in varchars is 32672 characters long
		String fileName1 = "extin/char32675trailingblanks.data"; // set up a file 32675 characters long but with last 3 characters as blanks
		String fileName2 = "extin/char32675.data"; // set up a file 32675 characters long with 3 extra non-blank characters trailing in the end

		System.out.println("Test 12 - varchar truncation tests start from here");
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
			stmt.execute("create table testVarChar (a int, b varchar(32672))");
			//create a table with 4 varchars. This table will be used to try overflow through concatenation
			stmt.execute("create table testConcatenation (a varchar(16350), b varchar(16350), c varchar(16336), d varchar(16336))");
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
			String largeStringA16350 = new String(Formatters.repeatChar("a",16350));
			String largeStringA16336 = new String(Formatters.repeatChar("a",16336));
			PreparedStatement ps = conn.prepareStatement("insert into testConcatenation values (?, ?, ?, ?)");
			ps.setString(1, largeStringA16350);
			ps.setString(2, largeStringA16350);
			ps.setString(3, largeStringA16336);
			ps.setString(4, largeStringA16336);
			ps.executeUpdate();

			ps = conn.prepareStatement("insert into testVarChar values(?, ?)");

			// prepare an InputStream from the file which has 3 trailing blanks in the end, so after blank truncation, there won't be any overflow
			// try this using setAsciiStream, setCharacterStream, setString and setObject
			insertDataUsingAsciiStream(ps, 1, fileName1, Limits.DB2_VARCHAR_MAXWIDTH);
			insertDataUsingCharacterStream(ps, 2, fileName1, Limits.DB2_VARCHAR_MAXWIDTH);
			insertDataUsingStringOrObject(ps, 3, Limits.DB2_VARCHAR_MAXWIDTH, true, true);
			insertDataUsingStringOrObject(ps, 4, Limits.DB2_VARCHAR_MAXWIDTH, true, false);
			System.out.println("===> testing trailing blanks using concatenation");
			insertDataUsingConcat(stmt, 5, Limits.DB2_VARCHAR_MAXWIDTH, true, VARCHAR);

			// prepare an InputStream from the file which has 3 trailing non-blanks in the end, and hence there would be overflow exception
			// try this using setAsciiStream, setCharacterStream, setString and setObject
			insertDataUsingAsciiStream(ps, 6, fileName2, Limits.DB2_VARCHAR_MAXWIDTH);
			insertDataUsingCharacterStream(ps, 7, fileName2, Limits.DB2_VARCHAR_MAXWIDTH);
			insertDataUsingStringOrObject(ps, 8, Limits.DB2_VARCHAR_MAXWIDTH, false, true);
			insertDataUsingStringOrObject(ps, 9, Limits.DB2_VARCHAR_MAXWIDTH, false, false);
			System.out.println("===> testing trailing non-blank characters using concatenation");
			insertDataUsingConcat(stmt, 10, Limits.DB2_VARCHAR_MAXWIDTH, false, VARCHAR);

			rs = stmt.executeQuery("select a, b from testVarChar");
			streamTestDataVerification(rs, Limits.DB2_VARCHAR_MAXWIDTH);
    }
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	 	System.out.println("Test 12 - varchar truncation tests end in here");
	}

	private static void streamTest13(Connection conn) {

		ResultSet rs;
		Statement stmt;

		//The following 2 files are for testing the truncation in long varchar.
		//any character truncation (including blanks characters) will throw an exception for long varchars.
		//max value allowed in long varchars is 32700 characters long
		String fileName1 = "extin/char32703trailingblanks.data"; // set up a file 32703 characters long but with last 3 characters as blanks
		String fileName2 = "extin/char32703.data"; // set up a file 32703 characters long with 3 extra non-blank characters trailing in the end

		System.out.println("Test 13 - long varchar truncation tests start from here");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testLongVarChars (a int, b long varchar)");
			PreparedStatement ps = conn.prepareStatement("insert into testLongVarChars values(?, ?)");

			// prepare an InputStream from the file which has 3 trailing blanks in the end. For long varchar, this would throw a truncation error
			// try this using setAsciiStream, setCharacterStream, setString and setObject
			insertDataUsingAsciiStream(ps, 1, fileName1, Limits.DB2_LONGVARCHAR_MAXWIDTH);
			insertDataUsingCharacterStream(ps, 2, fileName1, Limits.DB2_LONGVARCHAR_MAXWIDTH);
			insertDataUsingStringOrObject(ps, 3, Limits.DB2_LONGVARCHAR_MAXWIDTH, true, true);
			insertDataUsingStringOrObject(ps, 4, Limits.DB2_LONGVARCHAR_MAXWIDTH, true, false);
			//bug 5600- Can't test data overflow in longvarchar using concatenation because longvarchar concatenated string can't be longer than 32700
			//System.out.println("===> testing trailing blanks using concatenation");
			//insertDataUsingConcat(stmt, 5, Limits.DB2_LONGVARCHAR_MAXWIDTH, true, true);

			// prepare an InputStream from the file which has 3 trailing non-blanks in the end, and hence there would be overflow exception
			// try this using setAsciiStream, setCharacterStream, setString and setObject
			insertDataUsingAsciiStream(ps, 6, fileName2, Limits.DB2_LONGVARCHAR_MAXWIDTH);
			insertDataUsingCharacterStream(ps, 7, fileName2, Limits.DB2_LONGVARCHAR_MAXWIDTH);
			insertDataUsingStringOrObject(ps, 7, Limits.DB2_LONGVARCHAR_MAXWIDTH, false, true);
			insertDataUsingStringOrObject(ps, 9, Limits.DB2_LONGVARCHAR_MAXWIDTH, false, false);
			//bug 5600 - Can't test data overflow in longvarchar using concatenation because longvarchar concatenated string can't be longer than 32700
			//System.out.println("===> testing trailing non-blank characters using concatenation");
			//insertDataUsingConcat(stmt, 10, Limits.DB2_LONGVARCHAR_MAXWIDTH, false, true);

			rs = stmt.executeQuery("select a, b from testLongVarChars");
			streamTestDataVerification(rs, Limits.DB2_LONGVARCHAR_MAXWIDTH);
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	 	System.out.println("Test 13 - long varchar truncation tests end in here");
	}


    /**
     * Test truncation behavior for clobs
     * Test is similar to streamTest12 except that this test tests for clob column
     * @param conn
     */
    private static void streamTest14(Connection conn) {

        ResultSet rs;
        Statement stmt;

        //The following 2 files are for testing the truncation in clob
        //only non-blank character truncation will throw an exception for clob.
        //max value allowed in clob is 2G-1
        String fileName1 = "extin/char32675trailingblanks.data"; // set up a file 32675 characters long but with last 3 characters as blanks
        String fileName2 = "extin/char32675.data"; // set up a file 32675 characters long with 3 extra non-blank characters trailing in the end

        System.out.println("Test 14 - clob truncation tests start from here");
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
            stmt.execute("drop table testConcatenation");
            stmt.execute("create table testClob (a int, b clob(32672))");
            //create a table with 4 varchars. This table will be used to try overflow through concatenation
            
            stmt.execute("create table testConcatenation (a clob(16350), b clob(16350), c clob(16336), d clob(16336))");
            stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
            String largeStringA16350 = new String(Formatters.repeatChar("a",16350));
            String largeStringA16336 = new String(Formatters.repeatChar("a",16336));
            PreparedStatement ps = conn.prepareStatement("insert into testConcatenation values (?, ?, ?, ?)");
            ps.setString(1, largeStringA16350);
            ps.setString(2, largeStringA16350);
            ps.setString(3, largeStringA16336);
            ps.setString(4, largeStringA16336);
            ps.executeUpdate();

            ps = conn.prepareStatement("insert into testClob values(?, ?)");

            // prepare an InputStream from the file which has 3 trailing blanks in the end, so after blank truncation, there won't be any overflow
            // try this using setAsciiStream, setCharacterStream, setString and setObject
            insertDataUsingAsciiStream(ps, 1, fileName1, Limits.DB2_VARCHAR_MAXWIDTH);
            insertDataUsingCharacterStream(ps, 2, fileName1, Limits.DB2_VARCHAR_MAXWIDTH);
            insertDataUsingStringOrObject(ps, 3, Limits.DB2_VARCHAR_MAXWIDTH, true, true);
            insertDataUsingStringOrObject(ps, 4, Limits.DB2_VARCHAR_MAXWIDTH, true, false);
            System.out.println("===> testing trailing blanks using concatenation");
            insertDataUsingConcat(stmt, 5, Limits.DB2_VARCHAR_MAXWIDTH, true, CLOB);

            // prepare an InputStream from the file which has 3 trailing non-blanks in the end, and hence there would be overflow exception
            // try this using setAsciiStream, setCharacterStream, setString and setObject
            insertDataUsingAsciiStream(ps, 6, fileName2, Limits.DB2_VARCHAR_MAXWIDTH);
            insertDataUsingCharacterStream(ps, 7, fileName2, Limits.DB2_VARCHAR_MAXWIDTH);
            insertDataUsingStringOrObject(ps, 8, Limits.DB2_VARCHAR_MAXWIDTH, false, true);
            insertDataUsingStringOrObject(ps, 9, Limits.DB2_VARCHAR_MAXWIDTH, false, false);
            System.out.println("===> testing trailing non-blank characters using concatenation");
            insertDataUsingConcat(stmt, 10, Limits.DB2_VARCHAR_MAXWIDTH, false, CLOB);

            rs = stmt.executeQuery("select a, b from testVarChar");
            streamTestDataVerification(rs, Limits.DB2_VARCHAR_MAXWIDTH);
    }
        catch (SQLException e) {
            dumpSQLExceptions(e);
        }
        catch (Throwable e) {
            System.out.println("FAIL -- unexpected exception:" + e.toString());
        }
        System.out.println("Test 14 - clob truncation tests end in here");
    }


    /**
     * Streams are not re-used. This test tests the fix for 
     * DERBY-500. If an update statement has multiple rows that
     * is affected, and one of the parameter values is a stream,
     * the update will fail because streams are not re-used.
     * @param conn database connection
     */
    private static void derby500Test(Connection conn) {

        Statement stmt;

        System.out.println("======================================");
        System.out.println("START  DERBY-500 TEST ");

        try {
            stmt = conn.createStatement();
            conn.setAutoCommit(false);
            stmt.execute("CREATE TABLE t1 (" + "id INTEGER NOT NULL,"
                    + "mname VARCHAR( 254 ) NOT NULL," + "mvalue INT NOT NULL,"
                    + "bytedata BLOB NOT NULL," + "chardata CLOB NOT NULL,"
                    + "PRIMARY KEY ( id ))");

            PreparedStatement ps = conn
                    .prepareStatement("insert into t1 values (?,?,?,?,?)");

            // insert 10 rows.
            int rowCount = 0;
            // use blob and clob values
            int len = 10000;
            byte buf[] = new byte[len];
            char cbuf[] = new char[len];
            char orig =  'c';
            for (int i = 0; i < len; i++) {
                buf[i] = (byte)orig;
                cbuf[i] = orig;
            }
            int randomOffset = 9998;
            buf[randomOffset] = (byte) 'e';
            cbuf[randomOffset] = 'e';
            System.out.println("Inserting rows ");
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, "mname" + i);
                ps.setInt(3, 0);
                ps.setBinaryStream(4, new ByteArrayInputStream(buf), len);
                ps.setAsciiStream(5, new ByteArrayInputStream(buf), len);
                rowCount += ps.executeUpdate();
            }
            conn.commit();
            System.out.println("Rows inserted =" + rowCount);

            
            //conn.commit();
            PreparedStatement pss = conn
                    .prepareStatement(" select chardata,bytedata from t1 where id = ?");
            verifyDerby500Test(pss, buf, cbuf,0, 10, true);
            
            // do the update, update must qualify more than 1 row and update will fail
            // as currently we dont allow stream values to be re-used
            PreparedStatement psu = conn
                    .prepareStatement("update t1 set bytedata = ? "
                            + ", chardata = ? where mvalue = ?  ");

            buf[randomOffset + 1] = (byte) 'u';
            cbuf[randomOffset +1] = 'u';
            rowCount = 0;
            System.out.println("Update qualifies many rows + streams");

            try {
                psu.setBinaryStream(1, new ByteArrayInputStream(buf), len);
                psu.setCharacterStream(2, new CharArrayReader(cbuf), len);
                psu.setInt(3, 0);
                rowCount += psu.executeUpdate();
                System.out.println("DERBY500 #1 Rows updated  ="
                        + rowCount);

            } catch (SQLException sqle) {
                System.out
                        .println("EXPECTED EXCEPTION - streams cannot be re-used");
                expectedException(sqle);
                conn.rollback();
            }
            
            //verify data
            //set back buffer value to what was inserted.
            buf[randomOffset + 1] = (byte)orig;
            cbuf[randomOffset + 1] = orig;
            
            verifyDerby500Test(pss, buf,cbuf, 0, 10,true);

            PreparedStatement psu2 = conn
                    .prepareStatement("update t1 set bytedata = ? "
                            + ", chardata = ? where id = ?  ");

            buf[randomOffset + 1] = (byte) 'u';
            cbuf[randomOffset + 1] = 'u';
            
            rowCount = 0;
            try {
                psu2.setBinaryStream(1, new ByteArrayInputStream(buf), len);
                psu2.setAsciiStream(2, new ByteArrayInputStream(buf), len);
                psu2.setInt(3, 0);
                rowCount += psu2.executeUpdate();
                System.out.println("DERBY500 #2 Rows updated  ="
                        + rowCount);

            } catch (SQLException sqle) {
                System.out
                        .println("UNEXPECTED EXCEPTION - update should have actually gone through");
                dumpSQLExceptions(sqle);
            }
            conn.commit();
            verifyDerby500Test(pss, buf,cbuf, 0, 1,true);
            
            // delete
            // as currently we dont allow stream values to be re-used
            PreparedStatement psd = conn
                    .prepareStatement("delete from t1 where mvalue = ?");

            rowCount = 0;
            try {
                psd.setInt(1, 0);
                rowCount += psd.executeUpdate();
                rowCount += psd.executeUpdate();
                System.out.println("DERBY500 #3 Rows deleted ="
                        + rowCount);

            } catch (SQLException sqle) {
                System.out
                .println("UNEXPECTED EXCEPTION - delete should have actually gone through");
                dumpSQLExceptions(sqle);
            }

            conn.commit();
            //verify data
           
            verifyDerby500Test(pss, buf,cbuf, 0, 10, true);

            PreparedStatement psd2 = conn
                    .prepareStatement("delete from t1 where id = ?");
            
            rowCount = 0;
            try {
                psd2.setInt(1, 0);
                rowCount += psd2.executeUpdate();
                System.out.println("DERBY500 #4 Rows deleted  ="
                        + rowCount);

            } catch (SQLException sqle) {
                System.out
                        .println("UNEXPECTED EXCEPTION - delete should have actually gone through");
                dumpSQLExceptions(sqle);
            }
            conn.commit();
            verifyDerby500Test(pss, buf,cbuf, 1, 2,true);

            try
            {
                ps.setInt(1,11);
                rowCount += ps.executeUpdate();
                System.out.println("Rows inserted = "+ rowCount);
            } catch (SQLException sqle) {
                System.out
                        .println("EXPECTED EXCEPTION - streams cannot be re-used");
                expectedException(sqle);
                conn.rollback();
            }

            stmt.execute("drop table t1");
            conn.commit();
            stmt.close();
            pss.close();
            psu2.close();
            psu.close();
            psd.close();
            psd2.close();
            System.out.println("END  DERBY-500 TEST ");
            System.out.println("======================================");

        } catch (SQLException sqle) {
            dumpSQLExceptions(sqle);
        } catch (Exception e) {
            System.out.println("DERBY-500 TEST FAILED!");
            e.printStackTrace();
        }

    }

    /**
     * Test that DERBY500 fix did not change the behavior for varchar,
     * char, long varchar types when stream api is used. 
     * Currently, for char,varchar and long varchar - the stream is 
     * read once and materialized, hence the materialized stream value
     * will/can be used for multiple executions of the prepared statement  
     * @param conn database connection
     */
    private static void derby500_verifyVarcharStreams(Connection conn) {

        Statement stmt;

        System.out.println("======================================");
        System.out.println("START  DERBY-500 TEST for varchar ");

        try {
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE t1 (" + "id INTEGER NOT NULL,"
                    + "mname VARCHAR( 254 ) NOT NULL," + "mvalue INT NOT NULL,"
                    + "vc varchar(32500)," + "lvc long varchar NOT NULL,"
                    + "PRIMARY KEY ( id ))");

            PreparedStatement ps = conn
                    .prepareStatement("insert into t1 values (?,?,?,?,?)");

            // insert 10 rows.
            int rowCount = 0;
            // use blob and clob values
            int len = 10000;
            byte buf[] = new byte[len];
            char cbuf[] = new char[len];
            char orig =  'c';
            for (int i = 0; i < len; i++) {
                buf[i] = (byte)orig;
                cbuf[i] = orig;
            }
            int randomOffset = 9998;
            buf[randomOffset] = (byte)'e';
            cbuf[randomOffset] = 'e';
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, "mname" + i);
                ps.setInt(3, 0);
                ps.setCharacterStream(4, new CharArrayReader(cbuf), len);
                ps.setAsciiStream(5, new ByteArrayInputStream(buf), len);
                rowCount += ps.executeUpdate();
            }
            conn.commit();
            System.out.println("Rows inserted =" + rowCount);

            try
            {
                ps.setInt(1,11);
                rowCount += ps.executeUpdate();
            } catch (SQLException sqle) {
                System.out.println("UNEXPECTED EXCEPTION - streams cannot be "+
                   "re-used but in case of varchar, stream is materialized the"+
                   " first time around. So multiple executions using streams should "+
                   " work fine. ");
                dumpSQLExceptions(sqle);
            }
            
            PreparedStatement pss = conn
                    .prepareStatement(" select lvc,vc from t1 where id = ?");
            verifyDerby500Test(pss, buf, cbuf,0, 10,false);
            
            // do the update, update must qualify more than 1 row and update will
            // pass for char,varchar,long varchar columns.
            PreparedStatement psu = conn
                    .prepareStatement("update t1 set vc = ? "
                            + ", lvc = ? where mvalue = ?  ");

            buf[randomOffset +1] = (byte)'u';
            cbuf[randomOffset +1] = 'u';
            rowCount = 0;
            try {
                psu.setAsciiStream(1, new ByteArrayInputStream(buf), len);
                psu.setCharacterStream(2, new CharArrayReader(cbuf), len);
                psu.setInt(3, 0);
                rowCount += psu.executeUpdate();
            } catch (SQLException sqle) {
                System.out
                        .println("EXPECTED EXCEPTION - streams cannot be re-used");
                expectedException(sqle);
            }
            System.out.println("DERBY500 for varchar #1 Rows updated  ="
                    + rowCount);

            //verify data
            verifyDerby500Test(pss, buf,cbuf, 0, 10, false);

            PreparedStatement psu2 = conn
                    .prepareStatement("update t1 set vc = ? "
                            + ", lvc = ? where id = ?  ");

            buf[randomOffset +1] = (byte)'h';
            cbuf[randomOffset + 1] = 'h';
            
            rowCount = 0;
            try {
                psu2.setAsciiStream(1, new ByteArrayInputStream(buf), len);
                psu2.setAsciiStream(2, new ByteArrayInputStream(buf), len);
                psu2.setInt(3, 0);
                rowCount += psu2.executeUpdate();
            } catch (SQLException sqle) {
                System.out
                        .println("UNEXPECTED EXCEPTION - update should have actually gone through");
                dumpSQLExceptions(sqle);
            }
            conn.commit();
            System.out.println("DERBY500 for varchar #2 Rows updated  ="
                    + rowCount);
            verifyDerby500Test(pss, buf,cbuf, 0, 1,false);
            
            // delete
            // as currently we dont allow stream values to be re-used
            PreparedStatement psd = conn
                    .prepareStatement("delete from t1 where mvalue = ?");

            rowCount = 0;
            try {
                psd.setInt(1, 0);
                rowCount += psd.executeUpdate();
                rowCount += psd.executeUpdate();
            } catch (SQLException sqle) {
                System.out
                .println("UNEXPECTED EXCEPTION - delete should have actually gone through");
                dumpSQLExceptions(sqle);
            }
            System.out.println("DERBY500 for varchar #3 Rows deleted ="
                    + rowCount);

            //verify data
            verifyDerby500Test(pss, buf,cbuf, 0, 10,false);

            PreparedStatement psd2 = conn
                    .prepareStatement("delete from t1 where id = ?");
            
            rowCount = 0;
            try {
                psd2.setInt(1, 0);
                rowCount += psd2.executeUpdate();
            } catch (SQLException sqle) {
                System.out
                        .println("UNEXPECTED EXCEPTION - delete should have actually gone through");
                dumpSQLExceptions(sqle);
            }
            conn.commit();
            System.out.println("DERBY500 for varchar #4 Rows deleted  ="
                    + rowCount);
            verifyDerby500Test(pss, buf,cbuf, 1, 2,false);

            stmt.execute("drop table t1");
            conn.commit();
            stmt.close();
            pss.close();
            psu2.close();
            psu.close();
            psd.close();
            psd2.close();
            System.out.println("END  DERBY-500 TEST  for varchar");
            System.out.println("======================================");

        } catch (SQLException sqle) {
            dumpSQLExceptions(sqle);
        } catch (Exception e) {
            System.out.println("DERBY-500 TEST for varchar FAILED!");
            e.printStackTrace();
        }

    }

    /**
     * verify the data in the derby500Test
     * @param ps select preparedstatement
     * @param buf byte array to compare the blob data
     * @param cbuf char array to compare the clob data
     * @param startId start id of the row to check data for 
     * @param endId end id of the row to check data for
     * @param binaryType  flag to indicate if the second column in resultset
     *                  is a binary type or not. true for binary type 
     * @throws Exception
     */
    private static void verifyDerby500Test(PreparedStatement ps, byte[] buf,char[] cbuf,
            int startId, int endId,boolean binaryType) throws Exception {
        byte[] retrieveData = null;
        int rowCount = 0;
        ResultSet rs = null;
        for (int i = startId; i < endId; i++) {
            ps.setInt(1, i);
            rs = ps.executeQuery();
            if(rs.next())
            {
            compareCharArray(rs.getCharacterStream(1), cbuf,cbuf.length);
            if(binaryType)
                byteArrayEquals(rs.getBytes(2), 0, buf.length, buf, 0, buf.length);
            else
                compareCharArray(rs.getCharacterStream(2), cbuf,cbuf.length);
                
            rowCount++;
            }
        }
        System.out.println("Rows selected =" + rowCount);
        rs.close();
    }
    /**
     * compare char data
     * @param stream data from stream to compare 
     * @param compare base data to compare against
     * @param length compare length number of chars.
     * @throws Exception
     */
    private static void compareCharArray(Reader stream, char[] compare,
            int length) throws Exception {
        int c1 = 0;
        int i = 0;
        do {
            c1 = stream.read();
            if (c1 != compare[i++]) {
                System.out
                        .println("FAIL -- MISMATCH in data stored versus data retrieved at "
                                + (i - 1));
                break;
            }
            length--;
        } while (c1 != -1 && length > 0);

    }
    
    private static void expectedException(SQLException sqle) {

        while (sqle != null) {
            String sqlState = sqle.getSQLState();
            if (sqlState == null) {
                sqlState = "<NULL>";
            }
            System.out.println("EXPECTED SQL Exception: (" + sqlState + ") "
                    + sqle.getMessage());

            sqle = sqle.getNextException();
        }
    }

	private static void streamTestDataVerification(ResultSet rs, int maxValueAllowed)
	throws Exception{
		ResultSetMetaData met;

		met = rs.getMetaData();
		byte[] buff = new byte[128];
		// fetch all rows back, get the varchar and/ long varchar columns as streams.
		while (rs.next()) {
			// get the first column as an int
			int a = rs.getInt("a");
			// get the second column as a stream
			InputStream fin = rs.getAsciiStream(2);
			int columnSize = 0;
			for (;;) {
				int size = fin.read(buff);
					if (size == -1)
					break;
					columnSize += size;
			}
			if((a>=1 && a <= 5) && columnSize == maxValueAllowed)
				System.out.println("===> verified length " + maxValueAllowed);
			else
				System.out.println("test failed, columnSize should be " + maxValueAllowed + " but it is" + columnSize);
		}
	}

	//blankPadding
	//  true means excess trailing blanks
	//  false means excess trailing non-blank characters
	//  @param tblType table type, depending on the table type, the corresponding
    //  table is used. for varchar - testVarChar , for long varchar - testVarChars,
    //  and for clob - testClob is used
    private static void insertDataUsingConcat(Statement stmt, int intValue, int maxValueAllowed, boolean blankPadding,
	 	int tblType)
	throws Exception{
		String sql;
        
        switch(tblType)
        {
            case LONGVARCHAR:
                sql = "insert into testLongVarChars select " + intValue + ", a||b||";
                break;
            case CLOB:
                sql = "insert into testClob select "+ intValue + ", c||d||";
                break;
            default:
                sql = "insert into testVarChar select "+ intValue + ", c||d||";
        }

		if (blankPadding) //try overflow with trailing blanks
			sql = sql.concat("'   ' from testConcatenation");
		else //try overflow with trailing non-blank characters
			sql = sql.concat("'123' from testConcatenation");

		//for varchars, trailing blank truncation will not throw an exception. Only non-blank characters will cause truncation error
		//for long varchars, any character truncation will throw an exception.
		try {
			stmt.execute(sql);
			System.out.println("No truncation and hence no error.");
		}
		catch (SQLException e) {
			if (e.getSQLState().equals("22001")) //truncation error
				System.out.println("expected exception for data > " + maxValueAllowed + " in length");
			else
				dumpSQLExceptions(e);
		}
	}

	//blankPadding
	//  true means excess trailing blanks
	//  false means excess trailing non-blank characters
	//testUsingString
	//  true means try setString method for overflow
	//  false means try setObject method for overflow
	private static void insertDataUsingStringOrObject(PreparedStatement ps, int intValue, int maxValueAllowed,
	 	boolean blankPadding, boolean testUsingString)
	throws Exception{
	 	StringBuffer sb = new StringBuffer(maxValueAllowed);
	 	for (int i = 0; i < maxValueAllowed; i++)
			sb.append('q');

	 	String largeString = new String(sb);
	 	if (blankPadding) {
			largeString = largeString.concat("   ");
			System.out.print("===> testing trailing blanks(using ");
	 	} else {
			largeString = largeString.concat("123");
			System.out.print("===> testing trailing non-blanks(using ");
	 	}

	 	ps.setInt(1, intValue);
	 	if (testUsingString) {
			System.out.println("setString) length = " + largeString.length());
			ps.setString(2, largeString);
	 	} else {
			System.out.println("setObject) length = " + largeString.length());
			ps.setObject(2, largeString);
	 	}

		//for varchars, trailing blank truncation will not throw an exception. Only non-blank characters cause truncation error
		//for long varchars, any character truncation will throw an exception.
	 	try {
			ps.executeUpdate();
			System.out.println("No truncation and hence no error");
	 	}
	 	catch (SQLException e) {
			if (largeString.length() > maxValueAllowed && e.getSQLState().equals("22001")) //truncation error
				System.out.println("expected exception for data > " + maxValueAllowed + " in length");
			else
				dumpSQLExceptions(e);
	 	}
	}

	private static void insertDataUsingCharacterStream(PreparedStatement ps, int intValue, String fileName, int maxValueAllowed)
	throws Exception{
	 	File file = new File(fileName);
	 	InputStream fileIn = new FileInputStream(file);
	 	Reader filer = new InputStreamReader(fileIn);
	 	System.out.println("===> testing(using setCharacterStream) " + fileName + " length = " + file.length());
	 	ps.setInt(1, intValue);
	 	// insert a streaming column
	 	ps.setCharacterStream(2, filer, (int)file.length());
		//for varchars, trailing blank truncation will not throw an exception. Only non-blank characters cause truncation error
		//for long varchars, any character truncation will throw an exception.
	 	try {
			ps.executeUpdate();
			System.out.println("No truncation and hence no error");
	 	}
	 	catch (SQLException e) {
			if (file.length() > maxValueAllowed && e.getSQLState().equals("22001")) //truncation error
				System.out.println("expected exception for data > " + maxValueAllowed + " in length");
			else
				TestUtil.dumpSQLExceptions(e,true);
	 	}
	 	filer.close();
	}

	private static void insertDataUsingAsciiStream(PreparedStatement ps, int intValue, String fileName, int maxValueAllowed)
	throws Exception{
	 	File file = new File(fileName);
	 	InputStream fileIn = new FileInputStream(file);
	 	System.out.println("===> testing(using setAsciiStream) " + fileName + " length = " + file.length());
	 	// insert a streaming column
	 	ps.setInt(1, intValue);
	 	ps.setAsciiStream(2, fileIn, (int)file.length());
		//for varchars, trailing blank truncation will not throw an exception. Only non-blank characters cause truncation error
		//for long varchars, any character truncation will throw an exception.
	 	try {
			ps.executeUpdate();
			System.out.println("No truncation and hence no error");
	 	}
	 	catch (SQLException e) {
			if (file.length() > maxValueAllowed && e.getSQLState().equals("22001")) //truncation error
				System.out.println("expected exception for data > " + maxValueAllowed + " in length");
			else
				TestUtil.dumpSQLExceptions(e,true);
	 	}
	 	fileIn.close();
	}

	static void verifyLength(int a, int columnSize, long[] fileLength)
	{
		for (int i = 0; i < fileLength.length; i++) {
			if ((a == (100 + i)) || (a == (10000 + i)))
			{
				if(columnSize != fileLength[i])
					System.out.println("test failed, columnSize should be " + fileLength[i]
					   + ", but it is " + columnSize + ", i = " + i);
				else
					System.out.println("===> verified length " + fileLength[i]);
			}
		}
	}

	static void verifyExistence(Connection conn, int key, String base, long length) throws Exception {
		if (!pad(base, length).equals(getLongString(conn, key)))
			throw new Exception("failed to find value " + base + "... at key " + key);
	}

	static String getLongString(Connection conn, int key) throws Exception {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select b from foo where a = " + key);
		if (!rs.next())
			throw new Exception("there weren't any rows for key = " + key);
		String answer = rs.getString(1);
		if (rs.next())
			throw new Exception("there were multiple rows for key = " + key);
		rs.close();
		s.close();
		return answer;
	}

	static String pad(String base, long length) {
		StringBuffer b = new StringBuffer(base);
		for (long i = 1; b.length() < length; i++)
			b.append(" " + i);
		return b.toString();
	}

	static int insertLongString(Connection conn, int key, String data, boolean binaryColumn) throws Exception {
		PreparedStatement ps = conn.prepareStatement("insert into foo values(" + key + ", ?)");
		return streamInStringCol(ps, data, binaryColumn);
	}

	static int updateLongString(Connection conn, int oldkey, int newkey)
		 throws Exception
	{
		PreparedStatement ps = conn.prepareStatement(
			"update foo set a = ?, b = ? where a = " + oldkey);

		String updateString = pad("", newkey);
		ByteArrayInputStream bais = new ByteArrayInputStream(updateString.getBytes());
		ps.setInt(1, newkey);
		ps.setAsciiStream(2, bais, updateString.length());
		int nRows = ps.executeUpdate();
		ps.close();
		return nRows;
	}

	static int streamInStringCol(PreparedStatement ps, String data, boolean binaryColumn) throws Exception {
		int nRows = 0;

		if (data == null)
		{
			ps.setAsciiStream(1, null, 0);
			nRows = ps.executeUpdate();
		}
		else
		{
			ByteArrayInputStream bais = new ByteArrayInputStream(data.getBytes("US-ASCII"));
			if (binaryColumn)
				ps.setBinaryStream(1, bais, data.length());
			else
				ps.setAsciiStream(1, bais, data.length());
			nRows = ps.executeUpdate();
			bais.close();
		}
		return nRows;
	}

	public static int streamInLongCol(PreparedStatement ps, Object data) throws Exception {
		String s = (String)data;
		ByteArrayInputStream bais = new ByteArrayInputStream(s.getBytes());
		ps.setAsciiStream(1, bais, s.length());
		int nRows = ps.executeUpdate();
		bais.close();
		return nRows;
	}

	/**
		Compare two byte arrays using value equality.
		Two byte arrays are equal if their length is
		identical and their contents are identical.
	*/
	private static boolean byteArrayEquals(
    byte[] a,
    int aOffset,
    int aLength,
    byte[] b,
    int bOffset,
    int bLength)
    {
		if (aLength != bLength)
			return false;

		for (int i = 0; i < aLength; i++) {
			if (a[i + aOffset] != b[i + bOffset])
				return false;
		}
		return true;
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		se.printStackTrace();
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"):"+se.getMessage());
			se = se.getNextException();
		}
	}
    
    
}

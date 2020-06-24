/* 

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.StreamingColumnTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.store;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of JDBC result set Stream calls.
 * 
 */
public class StreamingColumnTest extends BaseJDBCTestCase {

    public static final int DB2_LONGVARCHAR_MAXWIDTH = 32700;
    public static final int DB2_VARCHAR_MAXWIDTH = 32672;

    public StreamingColumnTest(String name) {
        super(name);
    }

    // set up a short (fit in one page) inputstream for insert
    static String[] fileName;
    static long[] fileLength;

    static {
        int numFiles = 4;
        fileName = new String[numFiles];
        fileLength = new long[numFiles];

        fileName[0] = "extin/short.data"; // set up a short (fit in one page)
        // inputstream for insert
        fileName[1] = "extin/shortbanner"; // set up a long (longer than a
        // page) inputstream for insert
        fileName[2] = "extin/derby.banner"; // set up a really long (over 300K)
        // inputstream for insert
        fileName[3] = "extin/empty.data"; // set up a file with nothing in it
    }

    private static final int LONGVARCHAR = 1;
    private static final int CLOB = 2;
    private static final int VARCHAR = 3;

    /**
     * Test inserting data into long varchar columns from FileInputStreams, and
     * retrieving them back as InputStreams. Check if the length of the
     * retrieved streams is the same as their respective files. Also, retrieve
     * as Strings and compare lengths. <p/> Check insertion and updating of long
     * strings.
     * 
     * @throws Exception
     *             If an unexpected error occurs.
     */
    public void testStream1() throws Exception {
        ResultSet rs;
        Statement stmt;
        stmt = createStatement();
        for (int i = 0; i < fileName.length; i++) {

            // prepare an InputStream from the file
            final File file = new File(fileName[i]);
            fileLength[i] = PrivilegedFileOpsForTests.length(file);
            InputStream fileIn = PrivilegedFileOpsForTests
                    .getFileInputStream(file);
            println("===>  testing " + fileName[i] + " with length = "
                    + fileLength[i]);

            // insert a streaming column
            PreparedStatement ps = prepareStatement("insert into testLongVarChar1 values(?, ?)");
            ps.setInt(1, 100 + i);
            ps.setAsciiStream(2, fileIn, (int) fileLength[i]);
            try {// if trying to insert data > 32700, there will be an
                // exception
                ps.executeUpdate();
                if (DB2_LONGVARCHAR_MAXWIDTH < fileLength[i]) {
                    fail("Attempting to insert data longer than "
                            + DB2_LONGVARCHAR_MAXWIDTH + " should have thrown"
                            + "an exception.");
                }
                println("No truncation and hence no error");
            } catch (SQLException e) {
                if (fileLength[i] > DB2_LONGVARCHAR_MAXWIDTH) {
                    if (i == 2 && usingDerbyNetClient()) {
                        assertSQLState("XJ001", e); // was getting data longer
                        // than maxValueAllowed
                    } else {
                        assertSQLState("22001", e); // was getting data longer
                        // than maxValueAllowed
                    }
                    println("expected exception for data > "
                            + DB2_LONGVARCHAR_MAXWIDTH + " in length");
                } else {
                    throw e;
                }
            } finally {
                fileIn.close();
            }
        }

        rs = stmt.executeQuery("select a, b from testLongVarChar1");
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

        rs = stmt.executeQuery("select a, b from testLongVarChar1 order by a");
        rs.getMetaData();

        // fetch all rows back in order, get the long varchar columns as
        // streams.
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

        rs = stmt.executeQuery("select a, b from testLongVarChar1");
        // fetch all rows back, get the long varchar columns as Strings.
        while (rs.next()) {
            // JDBC columns use 1-based counting

            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);
            verifyLength(a, resultString.length(), fileLength);
        }

        rs = stmt.executeQuery("select a, b from testLongVarChar1 "
                + "order by a");
        // fetch all rows back in order, get the long varchar columns as
        // Strings.
        while (rs.next()) {

            // JDBC columns use 1-based counting
            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);
            verifyLength(a, resultString.length(), fileLength);
        }

        // should return one row.
        rs = stmt.executeQuery("select a, b from testLongVarChar1 "
                + "where b like 'test data: a string column inserted "
                + "as an object'");

        while (rs.next()) {

            // JDBC columns use 1-based counting
            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);
            verifyLength(a, resultString.length(), fileLength);
        }

        // tests on table foo1
        insertLongString(10, "ssssssssss", false, "foo1");
        insertLongString(0, "", false, "foo1");
        insertLongString(1, "1", false, "foo1");
        insertLongString(-1, null, false, "foo1");
        insertLongString(20, "XXXXXXXXXXXXXXXXXXXX", false, "foo1");

        rs = stmt.executeQuery("select a, b from foo1");

        println("Expect to get null string back");
        while (rs.next()) {
            int a = rs.getInt("a");
            String resultString = rs.getString(2);
            assertEquals("FAIL - failed to get string back, expect "
                    + a
                    + " got "
                    + (resultString == null ? "null resultString" : ""
                            + resultString.length()), a,
                    resultString == null ? -1 : resultString.length());
        }

        updateLongString(1, 3000, "foo1");
        updateLongString(0, 800, "foo1");
        updateLongString(3000, 0, "foo1");
        updateLongString(0, 51, "foo1");
        updateLongString(20, 0, "foo1");
        rs = stmt.executeQuery("select a, b from foo1");
        while (rs.next()) {
            int a = rs.getInt("a");
            String resultString = rs.getString(2);
            assertEquals("FAIL - failed to get string back, expect "
                    + a
                    + " got "
                    + (resultString == null ? "null resultString" : ""
                            + resultString.length()), a,
                    resultString == null ? -1 : resultString.length());
        }
        rs.close();
        stmt.close();
    }

    /**
     * test column size 1500 bytes. Run streamTest2 with padding length 1500.
     * 
     * @throws Exception
     */
    public void testStream2_1500() throws Exception {
        long length = 1500;
        streamTest2(length, "foo2_1500");
    }

    /**
     * test column size 5000 bytes.Run streamTest2 with padding length 5000
     * 
     * @throws Exception
     */
    public void testStream2_5000() throws Exception {
        long length = 5000;
        streamTest2(length, "foo2_5000");
    }

    /**
     * test column size 10000 bytes. Run streamTest2 with padding length 10000
     * 
     * @throws Exception
     */
    public void testStream2_10000() throws Exception {
        long length = 10000;
        streamTest2(length, "foo2_10000");
    }

    /**
     * Insert strings padded with various lengths and verify their existence.
     * 
     * @param length
     *            How long the string should be padded.
     * @param tableName
     *            The table to enter the strings.
     * @throws Exception
     *             If any unexpected errors occur.
     */
    private void streamTest2(long length, String tableName) throws Exception {
        Statement sourceStmt = createStatement();

        insertLongString(1, pad("Broadway", length), false, tableName);
        insertLongString(2, pad("Franklin", length), false, tableName);
        insertLongString(3, pad("Webster", length), false, tableName);

        sourceStmt.executeUpdate("insert into " + tableName + " select a+100, "
                + "b from " + tableName);

        verifyExistence(1, "Broadway", length, tableName);
        verifyExistence(2, "Franklin", length, tableName);
        verifyExistence(3, "Webster", length, tableName);
        verifyExistence(101, "Broadway", length, tableName);
        verifyExistence(102, "Franklin", length, tableName);
        verifyExistence(103, "Webster", length, tableName);
    }

    /**
     * Run streamTest3 with padding length 0
     * 
     * @throws Exception
     */
    public void testStream3_0() throws Exception {
        final long length = 0;
        final String tableName = "foo3_0";
        streamTest3(length, tableName);
    }

    /**
     * Run streamTest3 with padding length 1500
     * 
     * @throws Exception
     */
    public void testStream3_1500() throws Exception {
        final long length = 1500;
        final String tableName = "foo3_1500";
        streamTest3(length, tableName);
    }

    /**
     * Run streamTest3 with padding length 5000
     * 
     * @throws Exception
     */
    public void testStream3_5000() throws Exception {
        final long length = 5000;
        final String tableName = "foo3_5000";
        streamTest3(length, tableName);
    }

    /**
     * Run streamTest3 with padding length 10000
     * 
     * @throws Exception
     */
    public void testStream3_10000() throws Exception {
        final long length = 10000;
        final String tableName = "foo3_10000";
        streamTest3(length, tableName);
    }

    /**
     * Similar to streamTest2 apart from the insertion of file data as ascii
     * streams.
     * 
     * @param length
     * @param tableName
     * @throws Exception
     */
    private void streamTest3(final long length, final String tableName)
            throws Exception {
        insertLongString(1, pad("Broadway", length), false, tableName);
        insertLongString(2, pad("Franklin", length), false, tableName);
        insertLongString(3, pad("Webster", length), false, tableName);
        PreparedStatement ps = prepareStatement("update " + tableName + " set "
                + "a=a+1000, b=? where a<99 and a in (select a from "
                + tableName + ")");

        File file = new File("extin/short.data");
        InputStream fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
        ps.setAsciiStream(1, fileIn, (int) (PrivilegedFileOpsForTests
                .length(file)));
        ps.executeUpdate();
        fileIn.close();

        ps = prepareStatement("update " + tableName
                + " set a=a+1000, b=? where a<99 and a " + "in (select a from "
                + tableName + ")");
        file = new File("extin/shortbanner");
        fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
        ps.setAsciiStream(1, fileIn, (int) (PrivilegedFileOpsForTests
                .length(file)));
        ps.executeUpdate();
        fileIn.close();
    }

    /**
     * Insert data from streams to a BLOB field.
     * 
     * @throws Exception
     */
    public void testStream4() throws Exception {

        ResultSet rs;
        Statement stmt;
        stmt = createStatement();

        // insert an empty string
        stmt.execute("insert into testLongVarBinary4 values(1, CAST ("
                + TestUtil.stringToHexLiteral("") + "AS BLOB(1G)))");

        // insert a short text string
        stmt.execute("insert into testLongVarBinary4 values(2,CAST ("
                + TestUtil.stringToHexLiteral("test data: a string column "
                        + "inserted as an object") + "AS BLOB(1G)))");

        for (int i = 0; i < fileName.length; i++) {

            // prepare an InputStream from the file
            File file = new File(fileName[i]);
            fileLength[i] = PrivilegedFileOpsForTests.length(file);
            InputStream fileIn = PrivilegedFileOpsForTests
                    .getFileInputStream(file);

            println("Testing with " + fileName[i] + " length = "
                    + fileLength[i]);

            // insert a streaming column
            PreparedStatement ps = prepareStatement("insert into testLongVarBinary4 values(?, ?)");
            ps.setInt(1, 100 + i);
            ps.setBinaryStream(2, fileIn, (int) fileLength[i]);
            ps.executeUpdate();
            fileIn.close();
        }

        rs = stmt.executeQuery("select a, b from testLongVarBinary4");
        rs.getMetaData();
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
            verifyLength(a, columnSize, fileLength);
        }

        rs = stmt
                .executeQuery("select a, b from testLongVarBinary4 order by a");
        rs.getMetaData();

        // fetch all rows back in order, get the long varchar columns as
        // streams.
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
            verifyLength(a, columnSize, fileLength);
        }

        rs = stmt.executeQuery("select a, b from testLongVarBinary4");

        // fetch all rows back, get the long varchar columns as Strings.

        while (rs.next()) {

            // JDBC columns use 1-based counting
            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);

            // dividing string length by 2 as the binary column's hex digits are
            // each represented by a character
            // e.g. the hex number 0xA0 would be represented by a string of
            // length 2, "AO"
            verifyLength(a, resultString.length() / 2, fileLength);
        }

        rs = stmt
                .executeQuery("select a, b from testLongVarBinary4 order by a");
        // fetch all rows back in order, get the long varchar columns as
        // Strings.

        while (rs.next()) {

            // JDBC columns use 1-based counting
            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);
            println(resultString);

            // dividing string length by 2 as the binary column's hex digits are
            // each represented by a character
            // e.g. the hex number 0xA0 would be represented by a string of
            // length 2, "AO"
            verifyLength(a, resultString.length() / 2, fileLength);
        }

        rs.close();
        stmt.close();
    }

    /**
     * Run streamTest5 with padding length 0.
     * 
     * @throws Exception
     */
    public void testStream5_0() throws Exception {
        final long length = 0;
        final String tableName = "foo5_0";
        streamTest5(length, tableName);
    }

    /**
     * Run streamTest5 with padding length 1500.
     * 
     * @throws Exception
     */
    public void testStream5_1500() throws Exception {
        final long length = 1500;
        final String tableName = "foo5_1500";
        streamTest5(length, tableName);
    }

    /**
     * Run streamTest5 with padding length 5000.
     * 
     * @throws Exception
     */
    public void testStream5_5000() throws Exception {
        final long length = 5000;
        final String tableName = "foo5_5000";
        streamTest5(length, tableName);
    }

    /**
     * Run streamTest5 with padding length 100000.
     * 
     * @throws Exception
     */
    // This test fails when running w/ derby.language.logStatementText=true
    // see DERBY-595
    // public void testStream5_100000() throws Exception {
    // final long length = 100000;
    // final String tableName = "foo5_100000";
    // streamTest5(length, tableName);
    // }
    /**
     * If length &gt; 32700 insert to a BLOB field. Else, a long varchar field.
     * 
     * @param length
     *            Padding length
     * @param tableName
     *            Name of table
     * @throws Exception
     */
    private void streamTest5(long length, String tableName) throws Exception {
        InputStream fileIn = null;
        try {
            insertLongString(1, pad("Broadway", length), true, tableName);
            insertLongString(2, pad("Franklin", length), true, tableName);
            insertLongString(3, pad("Webster", length), true, tableName);
            insertLongString(4, pad("Broadway", length), true, tableName);
            insertLongString(5, pad("Franklin", length), true, tableName);
            insertLongString(6, pad("Webster", length), true, tableName);
            PreparedStatement ps = prepareStatement("update " + tableName
                    + " set a=a+1000, "
                    + "b=? where a<99 and a in (select a from " + tableName
                    + ")");
            File file = new File("extin/short.data");
            fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
            ps.setBinaryStream(1, fileIn, (int) (PrivilegedFileOpsForTests
                    .length(file)));
            ps.executeUpdate();
            fileIn.close();

            ps = prepareStatement("update " + tableName
                    + " set a=a+1000, b=? where a<99 "
                    + "and a in (select a from " + tableName + ")");
            file = new File("extin/shortbanner");
            fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
            ps.setBinaryStream(1, fileIn, (int) (PrivilegedFileOpsForTests
                    .length(file)));
            ps.executeUpdate();
            ps.close();
        } finally {
            fileIn.close();
        }
    }

    /**
     * Test getting a ByteArrayInputStream from data and inserting.
     * 
     * @throws Exception
     */
    public void testStream6() throws Exception {
        final long length = 5000;
        final String tableName = "foo_6";
        Statement sourceStmt = createStatement();

        insertLongString(1, pad("Broadway", length), false, tableName);
        insertLongString(2, pad("Franklin", length), false, tableName);
        insertLongString(3, pad("Webster", length), false, tableName);
        PreparedStatement ps = prepareStatement("update foo_6 set a=a+1000, "
                + "b=? where a<99 and a in (select a from foo_6)");

        streamInLongCol(ps, pad("Grand", length));
        ps.close();
        sourceStmt.close();
    }

    /**
     * Test insertion of a long string to a long varchar field in a table
     * created with pagesize 1024.
     * 
     * @throws Exception
     */
    public void testStream7() throws Exception {
        setAutoCommit(false);
        println("streamTest7");

        PreparedStatement ps1 = prepareStatement("insert into testlvc7 values (?, "
                + "'filler for column b on null column', null, 'filler for column d')");
        PreparedStatement ps2 = prepareStatement("insert into testlvc7 values (?, "
                + "'filler for column b on empty string column', ?, 'filler2 for column d')");

        for (int i = 0; i < 100; i++) {
            ps1.setInt(1, i);
            ps1.executeUpdate();

            ByteArrayInputStream emptyString = new ByteArrayInputStream(
                    new byte[0]);
            ps2.setInt(1, i);
            ps2.setAsciiStream(2, emptyString, 0);
            ps2.executeUpdate();
        }
        ps1.close();
        ps2.close();

        commit();

        PreparedStatement ps = prepareStatement("update testlvc7 set lvc = ? where a = ?");

        String longString = "this is a relatively long string, hopefully "
                + "the row will be split or otherwise become long ???  "
                + "I don't think it will become long but maybe if it rolls "
                + "back it will become strange";
        for (int i = 0; i < 100; i++) {
            ByteArrayInputStream string1 = new ByteArrayInputStream(longString
                    .getBytes("US-ASCII"));
            ps.setAsciiStream(1, string1, longString.length());
            ps.setInt(2, i);
            ps.executeUpdate();
            if ((i % 2) == 0) {
                rollback();
            } else {
                commit();
            }

            ByteArrayInputStream emptyString = new ByteArrayInputStream(
                    new byte[0]);
            ps.setAsciiStream(1, emptyString, 0);
            ps.executeUpdate();
            if ((i % 3) == 0) {
                rollback();
            } else {
                commit();
            }
        }
        ps.close();
    }

    /**
     * long row test of insert/backout case, using setAsciiStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible. So it first
     * asks raw store to try inserting without overflowing rows or columns. If
     * that doesn't work it then asks raw store for a mostly empty page and
     * tries to insert it there with overflow, If that doesn't work then an
     * empty page is picked.
     * <p>
     * If parameters are 10,2500 - then the second row inserted will have the
     * 1st column fit, but the second not fit which caused track #2240.
     * 
     */
    public void testStream8_10_2500() throws Exception {
        int stream1_len = 10;
        int stream2_len = 2500;
        String tableName = "t8_10_2500";
        println("Starting testStream8_10_2500(" + stream1_len + ", "
                + stream2_len + ")");
        streamTest8(stream1_len, stream2_len, tableName);
        println("Finishing testStream8_10_2500(" + stream1_len + ", "
                + stream2_len + ")");
    }

    /**
     * long row test of insert/backout case, using setAsciiStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible. So it first
     * asks raw store to try inserting without overflowing rows or columns. If
     * that doesn't work it then asks raw store for a mostly empty page and
     * tries to insert it there with overflow, If that doesn't work then an
     * empty page is picked.
     * <p>
     * If parameters are 10,2500 - then the second row inserted will have the
     * 1st column fit, but the second not fit which caused track #2240.
     * 
     * @exception StandardException
     *                Standard exception policy.
     */
    public void testStream8_2500_10() throws Exception {
        int stream1_len = 2500;
        int stream2_len = 10;
        String tableName = "t8_2500_10";
        println("Starting streamTest8_2500_10(" + stream1_len + ", "
                + stream2_len + ")");
        streamTest8(stream1_len, stream2_len, tableName);
        println("Finishing streamTest8_2500_10(" + stream1_len + ", "
                + stream2_len + ")");
    }

    /**
     * Method called by testStream8_10_2500 and testStream8_2500_10
     * 
     * @param stream1_len
     *            Length of the 1st stream
     * @param stream2_len
     *            Length of the 2nd stream
     * @param tableName
     *            Name of table
     * @throws Exception
     */
    private void streamTest8(int stream1_len, int stream2_len, String tableName)
            throws Exception {
        println("Starting streamTest8(" + stream1_len + ", " + stream2_len
                + ")");

        ResultSet rs;
        Statement stmt;

        String insertsql = new String("insert into " + tableName
                + " values (?, ?, ?) ");

        int numStrings = 10;

        byte[][] stream1_byte_array = new byte[numStrings][];
        byte[][] stream2_byte_array = new byte[numStrings][];

        // make string size match input sizes.
        for (int i = 0; i < numStrings; i++) {
            stream1_byte_array[i] = new byte[stream1_len];

            for (int j = 0; j < stream1_len; j++)
                stream1_byte_array[i][j] = (byte) ('a' + i);

            stream2_byte_array[i] = new byte[stream2_len];
            for (int j = 0; j < stream2_len; j++)
                stream2_byte_array[i][j] = (byte) ('A' + i);
        }

        setAutoCommit(false);
        stmt = createStatement();

        PreparedStatement insert_ps = prepareStatement(insertsql);

        for (int i = 0; i < numStrings; i++) {
            // create the stream and insert it
            insert_ps.setInt(1, i);

            // create the stream and insert it
            insert_ps.setAsciiStream(2, new ByteArrayInputStream(
                    stream1_byte_array[i]), stream1_len);

            // create the stream and insert it
            insert_ps.setAsciiStream(3, new ByteArrayInputStream(
                    stream2_byte_array[i]), stream2_len);

            insert_ps.executeUpdate();

            // just force a scan of the table, no insert is done.
            String checkSQL = "insert into " + tableName + " select * from "
                    + tableName + " where a = -6363";
            stmt.execute(checkSQL);
        }

        insert_ps.close();
        commit();

        rs = stmt.executeQuery("select a, b, c from " + tableName);

        // should return one row.
        while (rs.next()) {

            // JDBC columns use 1-based counting
            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            String resultString = rs.getString(2);

            // compare result with expected, using fixed length string from
            // the streamed byte array
            String canon = new String(stream1_byte_array[a], "US-ASCII");

            assertEquals("FAIL -- bad result string:" + "canon: " + canon
                    + "resultString: " + resultString, 0, canon
                    .compareTo(resultString));

            // get the second column as a string
            resultString = rs.getString(3);

            // compare result with expected, using fixed length string from
            // the second streamed byte array.
            canon = new String(stream2_byte_array[a], "US-ASCII");

            assertEquals("FAIL -- bad result string:" + "canon: " + canon
                    + "resultString: " + resultString, 0, canon
                    .compareTo(resultString));
        }

        rs.close();

        stmt.execute("insert into " + tableName + " select * from " + tableName
                + " ");
        stmt.close();
        commit();

        println("Finishing streamTest8(" + stream1_len + ", " + stream2_len
                + ")");
    }

    /**
     * long row test of insert/backout case, using setBinaryStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible. So it first
     * asks raw store to try inserting without overflowing rows or columns. If
     * that doesn't work it then asks raw store for a mostly empty page and
     * tries to insert it there with overflow, If that doesn't work then an
     * empty page is picked.
     * <p>
     * If input parameters are 10,2500 - then the second row inserted will have
     * the 1st column fit, but the second not fit which caused track #2240.
     * 
     * @exception StandardException
     *                Standard exception policy.
     */
    public void testStream9_10_2500() throws Exception {
        int stream1_len = 10, stream2_len = 2500;
        String tableName = "t9_10_2500";
        println("Starting testStream9_10_2500(" + stream1_len + ", "
                + stream2_len + ")");
        streamTest9(stream1_len, stream2_len, tableName);
        println("Finishing testStream_10_2500(" + stream1_len + ", "
                + stream2_len + ")");
    }

    /**
     * long row test of insert/backout case, using setBinaryStream().
     * <p>
     * The heap tries to make rows all fit on one page if possible. So it first
     * asks raw store to try inserting without overflowing rows or columns. If
     * that doesn't work it then asks raw store for a mostly empty page and
     * tries to insert it there with overflow, If that doesn't work then an
     * empty page is picked.
     * <p>
     * If input parameters are 10,2500 - then the second row inserted will have
     * the 1st column fit, but the second not fit which caused track #2240.
     * 
     * @exception StandardException
     *                Standard exception policy.
     */
    public void testStream9_2500_10() throws Exception {
        int stream1_len = 2500, stream2_len = 10;
        String tableName = "t9_2500_10";
        println("Starting streamTest9_2500_10(" + stream1_len + ", "
                + stream2_len + ")");
        streamTest9(stream1_len, stream2_len, tableName);
        println("Finishing testStream9_2500_10(" + stream1_len + ", "
                + stream2_len + ")");
    }

    /**
     * Metjod called by testStream9_10_2500 and testStream9_2500_10
     * 
     * @param stream1_len
     *            Length of 1st stream
     * @param stream2_len
     *            Length of 2nd stream
     * @param tableName
     *            name of table
     * @throws SQLException
     */
    private void streamTest9(int stream1_len, int stream2_len, String tableName)
            throws SQLException {
        ResultSet rs;
        Statement stmt;

        String insertsql = new String("insert into " + tableName
                + " values (?, ?, ?) ");
        int numStrings = 10;

        byte[][] stream1_byte_array = new byte[numStrings][];
        byte[][] stream2_byte_array = new byte[numStrings][];

        // make string size match input sizes.
        for (int i = 0; i < numStrings; i++) {
            stream1_byte_array[i] = new byte[stream1_len];

            for (int j = 0; j < stream1_len; j++)
                stream1_byte_array[i][j] = (byte) ('a' + i);

            stream2_byte_array[i] = new byte[stream2_len];
            for (int j = 0; j < stream2_len; j++)
                stream2_byte_array[i][j] = (byte) ('A' + i);
        }

        setAutoCommit(false);
        stmt = createStatement();
        PreparedStatement insert_ps = prepareStatement(insertsql);

        for (int i = 0; i < numStrings; i++) {
            // create the stream and insert it
            insert_ps.setInt(1, i);

            // create the stream and insert it
            insert_ps.setBinaryStream(2, new ByteArrayInputStream(
                    stream1_byte_array[i]), stream1_len);

            // create the stream and insert it
            insert_ps.setBinaryStream(3, new ByteArrayInputStream(
                    stream2_byte_array[i]), stream2_len);

            insert_ps.executeUpdate();

            // just force a scan of the table, no insert is done.
            String checkSQL = "insert into " + tableName + " select * from "
                    + tableName + " where a = -6363";
            stmt.execute(checkSQL);
        }

        insert_ps.close();
        commit();
        rs = stmt.executeQuery("select a, b, c from " + tableName);

        // should return one row.
        while (rs.next()) {
            // JDBC columns use 1-based counting

            // get the first column as an int
            int a = rs.getInt("a");

            // get the second column as a string
            byte[] resultString = rs.getBytes(2);

            // compare result with expected
            byte[] canon = stream1_byte_array[a];
            assertTrue("FAIL -- bad result byte array 1:" + "canon: " + canon
                    + "resultString: " + resultString, Arrays.equals(canon,
                    resultString));

            // get the second column as a string
            resultString = rs.getBytes(3);

            // compare result with expected
            canon = stream2_byte_array[a];
            assertTrue("FAIL -- bad result byte array 2:" + "canon: " + canon
                    + "resultString: " + resultString, Arrays.equals(canon,
                    resultString));
        }
        rs.close();
        stmt
                .execute("insert into " + tableName + " select * from "
                        + tableName);
        stmt.close();
        commit();

        println("Finishing streamTest9(" + stream1_len + ", " + stream2_len
                + ")");
    }

    /**
     * table with multiple indexes, indexes share columns table has more than 4
     * rows, insert stream into table compress table and verify that each index
     * is valid .
     */
    public void testStream10() throws Exception {
        Statement stmt;
        println("Test 10 starts from here");
        stmt = createStatement();

        // insert stream into table
        for (int i = 0; i < fileName.length; i++) {
            println("i:" + i + " fileName:" + fileName[i]);

            // prepare an InputStream from the file
            File file = new File(fileName[i]);
            fileLength[i] = PrivilegedFileOpsForTests.length(file);
            InputStream fileIn = PrivilegedFileOpsForTests
                    .getFileInputStream(file);
            println("===> testing " + fileName[i] + " length = "
                    + fileLength[i]);

            // insert a streaming column
            PreparedStatement ps = prepareStatement("insert into tab10 values(?, ?, ?)");
            ps.setInt(1, 100 + i);
            ps.setInt(2, 100 + i);
            ps.setAsciiStream(3, fileIn, (int) fileLength[i]);
            try {// if trying to insert data > 32700, there will be an
                // exception
                println(ps.toString());
                ps.executeUpdate();
                if (i == 2) {
                    fail("Length 414000 should have thrown a truncation error!");
                }
                println("No truncation and hence no error");
            } catch (SQLException e) {
                println(i + " " + fileName[i]);
                if (fileLength[i] > DB2_LONGVARCHAR_MAXWIDTH) {
                    if (usingDerbyNetClient() && i == 2) {
                        assertSQLState("XJ001", e);
                    } else {
                        assertSQLState("22001", e);
                    }
                    // was getting data longer than maxValueAllowed
                    println("expected exception for data > "
                            + DB2_LONGVARCHAR_MAXWIDTH + " in length");
                } else {
                    throw e;
                }
            } finally {
                fileIn.close();
            }
        }

        // execute the compress command
        CallableStatement cs = prepareCall("CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?)");
        cs.setString(1, "APP");
        cs.setString(2, "TESTLONGVARCHAR1");
        cs.setInt(3, 0);
        cs.execute();

        // do consistency checking
        stmt
                .execute("CREATE FUNCTION ConsistencyChecker() "
                        + "RETURNS VARCHAR(128) EXTERNAL NAME "
                        + "'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker' "
                        + "LANGUAGE JAVA PARAMETER STYLE JAVA");
        stmt.execute("VALUES ConsistencyChecker()");
        stmt.close();

        println("Test 10 ends here");
    }

    /**
     * Test the passing of negative values for stream lengths to various
     * setXXXStream methods.
     */
    public void testStream11() throws Exception {

        println("Test 11 - Can't pass negative length as the stream length "
                + "for various setXXXStream methods");

        // prepare an InputStream from the file
        File file = new File("extin/short.data");
        InputStream fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
        PreparedStatement ps = prepareStatement("insert into "
                + "testLongVarCharInvalidStreamLength11 values(?, ?, ?)");
        ps.setInt(1, 100);
        try {
            println("===> testing using setAsciiStream with -2 as length");

            // The follwoing test depends on patch for DERBY-3705 being applied
            // to pass
            ps.setAsciiStream(2, fileIn, -2); // test specifically for
            // Cloudscape bug 4250
            fail("FAIL -- should have gotten exception for -2 "
                    + "param value to setAsciiStream");
        } catch (SQLException e) {
            assertSQLState("XJ025", e);
            println("Expected exception:" + e.toString());
        } finally {
            fileIn.close();
        }

        Reader filer = new InputStreamReader(fileIn, "US-ASCII");
        try {
            println("===> testing using setCharacterStream with -1 as length");
            ps.setCharacterStream(2, filer, -1);
            fail("FAIL -- should have gotten exception for -1 param "
                    + "value to setCharacterStream");
        } catch (SQLException e) {
            assertSQLState("XJ025", e);
            println("PASS -- expected exception:" + e.toString());
        } finally {
            fileIn.close();
        }

        try {
            println("===> testing using setBinaryStream with -1 as length");
            ps.setBinaryStream(3, fileIn, -1);
            fail("FAIL -- should have gotten exception for -1 param "
                    + "value to setBinaryStream");
        } catch (SQLException e) {
            assertSQLState("XJ025", e);
            println("Expected exception:" + e.toString());
        } finally {
            fileIn.close();
        }
        println("Test 11 - negative stream length tests end in here");
    }

    /**
     * Test truncation from files with trailing blanks and non-blanks.
     */
    public void testStream12() throws Exception {

        ResultSet rs;
        Statement stmt;

        // The following 2 files are for testing the truncation in varchar.
        // only non-blank character truncation will throw an exception for
        // varchars.
        // max value allowed in varchars is 32672 characters long

        // set up a file 32675 characters long but with last 3 characters as
        // blanks
        String fileName1 = "extin/char32675trailingblanks.data";

        // set up a file 32675 characters long with 3 extra non-blank characters
        // trailing in the end
        String fileName2 = "extin/char32675.data";
        println("Test 12 - varchar truncation tests start from here");

        stmt = createStatement();
        String largeStringA16350 = new String(Formatters.repeatChar("a", 16350));
        String largeStringA16336 = new String(Formatters.repeatChar("a", 16336));
        PreparedStatement ps = prepareStatement("insert into testConcatenation12 values (?, ?, ?, ?)");
        ps.setString(1, largeStringA16350);
        ps.setString(2, largeStringA16350);
        ps.setString(3, largeStringA16336);
        ps.setString(4, largeStringA16336);
        ps.executeUpdate();

        ps = prepareStatement("insert into testVarChar12 values(?, ?)");

        // prepare an InputStream from the file which has 3 trailing blanks
        // in the end, so after blank truncation, there won't be any
        // overflow
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 1, fileName1, DB2_VARCHAR_MAXWIDTH, 12);
        insertDataUsingCharacterStream(ps, 2, fileName1, DB2_VARCHAR_MAXWIDTH,
                12);
        insertDataUsingStringOrObject(ps, 3, DB2_VARCHAR_MAXWIDTH, true, true,
                12);
        insertDataUsingStringOrObject(ps, 4, DB2_VARCHAR_MAXWIDTH, true, false,
                12);
        println("===> testing trailing blanks using concatenation");
        insertDataUsingConcat(stmt, 5, DB2_VARCHAR_MAXWIDTH, true, VARCHAR,
                "testConcatenation12");

        // prepare an InputStream from the file which has 3 trailing
        // non-blanks in the end, and hence there would be overflow
        // exception
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 6, fileName2, DB2_VARCHAR_MAXWIDTH, 12);
        insertDataUsingCharacterStream(ps, 7, fileName2, DB2_VARCHAR_MAXWIDTH,
                12);
        insertDataUsingStringOrObject(ps, 8, DB2_VARCHAR_MAXWIDTH, false, true,
                12);
        insertDataUsingStringOrObject(ps, 9, DB2_VARCHAR_MAXWIDTH, false,
                false, 12);
        println("===> testing trailing non-blank characters using concatenation");
        insertDataUsingConcat(stmt, 10, DB2_VARCHAR_MAXWIDTH, false, VARCHAR,
                "testConcatenation12");

        rs = stmt.executeQuery("select a, b from testVarChar12");
        streamTestDataVerification(rs, DB2_VARCHAR_MAXWIDTH);

        println("Test 12 - varchar truncation tests end in here");
    }

    /**
     * Test truncation from files to long varchar.
     * 
     * @throws Exception
     */
    public void testStream13() throws Exception {

        ResultSet rs;
        Statement stmt;

        // The following 2 files are for testing the truncation in long varchar.
        // any character truncation (including blanks characters) will throw an
        // exception for long varchars.
        // max value allowed in long varchars is 32700 characters long

        // set up a file 32703 characters long but with last 3 characters
        // as blanks
        String fileName1 = "extin/char32703trailingblanks.data";

        // set up a file 32703 characters long with 3 extra non-blank
        // characters trailing in the end
        String fileName2 = "extin/char32703.data";
        println("testStream13 - long varchar truncation tests start from here");
        stmt = createStatement();
        PreparedStatement ps = prepareStatement("insert into testLongVarChars13 values(?, ?)");

        // prepare an InputStream from the file which has 3 trailing blanks
        // in the end. For long varchar, this would throw a truncation error
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 1, fileName1, DB2_LONGVARCHAR_MAXWIDTH,
                13);
        insertDataUsingCharacterStream(ps, 2, fileName1,
                DB2_LONGVARCHAR_MAXWIDTH, 13);
        insertDataUsingStringOrObject(ps, 3, DB2_LONGVARCHAR_MAXWIDTH, true,
                true, 13);
        insertDataUsingStringOrObject(ps, 4, DB2_LONGVARCHAR_MAXWIDTH, true,
                false, 13);

        // bug 5600- Can't test data overflow in longvarchar using
        // concatenation because longvarchar concatenated string can't be
        // longer than 32700
        // println("===> testing trailing blanks using
        // concatenation");
        // insertDataUsingConcat(stmt, 5, DB2_LONGVARCHAR_MAXWIDTH,
        // true, LONGVARCHAR, "testLongVarChars13");

        // prepare an InputStream from the file which has 3 trailing
        // non-blanks in the end, and hence there would be overflow
        // exception
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 6, fileName2, DB2_LONGVARCHAR_MAXWIDTH,
                13);
        insertDataUsingCharacterStream(ps, 7, fileName2,
                DB2_LONGVARCHAR_MAXWIDTH, 13);
        insertDataUsingStringOrObject(ps, 7, DB2_LONGVARCHAR_MAXWIDTH, false,
                true, 13);
        insertDataUsingStringOrObject(ps, 9, DB2_LONGVARCHAR_MAXWIDTH, false,
                false, 13);

        // bug 5600 - Can't test data overflow in longvarchar using
        // concatenation because longvarchar concatenated string can't be
        // longer than 32700
        // println("===> testing trailing non-blank characters
        // using concatenation");
        // insertDataUsingConcat(stmt, 10, DB2_LONGVARCHAR_MAXWIDTH,
        // false, LONGVARCHAR, "testLongVarChars13");
        rs = stmt.executeQuery("select a, b from testLongVarChars13");
        streamTestDataVerification(rs, DB2_LONGVARCHAR_MAXWIDTH);

        println("Test 13 - long varchar truncation tests end in here");
    }

    /**
     * Test truncation behavior for clobs Test is similar to streamTest12 except
     * that this test tests for clob column
     * 
     */
    public void testStream14() throws Exception {

        ResultSet rs;
        Statement stmt;

        // The following 2 files are for testing the truncation in clob
        // only non-blank character truncation will throw an exception for clob.
        // max value allowed in clob is 2G-1

        // set up a file 32675 characters long but with last 3 characters as
        // blanks
        String fileName1 = "extin/char32675trailingblanks.data";

        // set up a file 32675 characters long with 3 extra non-blank characters
        // trailing in the end
        String fileName2 = "extin/char32675.data";

        println("testStream 14 - clob truncation tests start from here");

        stmt = createStatement();
        String largeStringA16350 = new String(Formatters.repeatChar("a", 16350));
        String largeStringA16336 = new String(Formatters.repeatChar("a", 16336));
        PreparedStatement ps = prepareStatement("insert into testConcatenation14 values (?, ?, ?, ?)");
        ps.setString(1, largeStringA16350);
        ps.setString(2, largeStringA16350);
        ps.setString(3, largeStringA16336);
        ps.setString(4, largeStringA16336);
        ps.executeUpdate();

        ps = prepareStatement("insert into testClob14 values(?, ?)");

        // prepare an InputStream from the file which has 3 trailing blanks
        // in the end, so after blank truncation, there won't be any
        // overflow
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 1, fileName1, DB2_VARCHAR_MAXWIDTH, 14);
        insertDataUsingCharacterStream(ps, 2, fileName1, DB2_VARCHAR_MAXWIDTH,
                14);
        insertDataUsingStringOrObject(ps, 3, DB2_VARCHAR_MAXWIDTH, true, true,
                14);
        insertDataUsingStringOrObject(ps, 4, DB2_VARCHAR_MAXWIDTH, true, false,
                14);
        println("testStream14 - Testing trailing blanks using concatenation");
        insertDataUsingConcat(stmt, 5, DB2_VARCHAR_MAXWIDTH, true, CLOB,
                "testConcatenation14");

        // prepare an InputStream from the file which has 3 trailing
        // non-blanks in the end, and hence there would be overflow
        // exception
        // try this using setAsciiStream, setCharacterStream, setString and
        // setObject
        insertDataUsingAsciiStream(ps, 6, fileName2, DB2_VARCHAR_MAXWIDTH, 14);
        insertDataUsingCharacterStream(ps, 7, fileName2, DB2_VARCHAR_MAXWIDTH,
                14);
        insertDataUsingStringOrObject(ps, 8, DB2_VARCHAR_MAXWIDTH, false, true,
                14);
        insertDataUsingStringOrObject(ps, 9, DB2_VARCHAR_MAXWIDTH, false,
                false, 14);
        println("testStream14 - Testing trailing non-blank characters using concatenation");
        insertDataUsingConcat(stmt, 10, DB2_VARCHAR_MAXWIDTH, false, CLOB,
                "testConcatenation14");

        rs = stmt.executeQuery("select a, b from testVarChar12");
        streamTestDataVerification(rs, DB2_VARCHAR_MAXWIDTH);

        println("Test 14 - clob truncation tests end in here");
    }

    /**
     * Streams are not re-used. This test tests the fix for DERBY-500. If an
     * update statement has multiple rows that is affected, and one of the
     * parameter values is a stream, the update will fail because streams are
     * not re-used.
     */
    public void testDerby500() throws Exception {
        Statement stmt;
        println("======================================");
        println("START  DERBY-500 TEST ");
        stmt = createStatement();
        setAutoCommit(false);
        PreparedStatement ps = prepareStatement("insert into test500 "
                + "values (?,?,?,?,?)");

        // insert 10 rows.
        int rowCount = 0;

        // use blob and clob values
        int len = 10000;
        byte buf[] = new byte[len];
        char cbuf[] = new char[len];
        char orig = 'c';
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) orig;
            cbuf[i] = orig;
        }
        int randomOffset = 9998;
        buf[randomOffset] = (byte) 'e';
        cbuf[randomOffset] = 'e';
        println("Inserting rows ");
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setString(2, "mname" + i);
            ps.setInt(3, 0);
            ps.setBinaryStream(4, new ByteArrayInputStream(buf), len);
            ps.setAsciiStream(5, new ByteArrayInputStream(buf), len);
            rowCount += ps.executeUpdate();
        }
        commit();
        println("Rows inserted =" + rowCount);

        PreparedStatement pss = prepareStatement(" select chardata,bytedata from test500 where id = ?");
        verifyDerby500Test(pss, buf, cbuf, 0, 10, true);

        // do the update, update must qualify more than 1 row and update
        // will fail as currently we don't allow stream values to be re-used
        PreparedStatement psu = prepareStatement("update test500 set bytedata = ? "
                + ", chardata = ? where mvalue = ?  ");

        buf[randomOffset + 1] = (byte) 'u';
        cbuf[randomOffset + 1] = 'u';
        rowCount = 0;
        println("Update qualifies many rows + streams");

        try {
            psu.setBinaryStream(1, new ByteArrayInputStream(buf), len);
            psu.setCharacterStream(2, new CharArrayReader(cbuf), len);
            psu.setInt(3, 0);
            rowCount += psu.executeUpdate();
            println("DERBY500 #1 Rows updated  =" + rowCount);
            fail("Attempting to reuse stream should have thrown an exception!");
        } catch (SQLException sqle) {
            assertSQLState("XJ001", sqle);
            println("EXPECTED EXCEPTION - streams cannot be re-used");
            rollback();
        }

        // verify data
        // set back buffer value to what was inserted.
        buf[randomOffset + 1] = (byte) orig;
        cbuf[randomOffset + 1] = orig;

        verifyDerby500Test(pss, buf, cbuf, 0, 10, true);

        PreparedStatement psu2 = prepareStatement("update test500 set "
                + "bytedata = ? , chardata = ? where id = ?  ");

        buf[randomOffset + 1] = (byte) 'u';
        cbuf[randomOffset + 1] = 'u';

        rowCount = 0;
        psu2.setBinaryStream(1, new ByteArrayInputStream(buf), len);
        psu2.setAsciiStream(2, new ByteArrayInputStream(buf), len);
        psu2.setInt(3, 0);
        rowCount += psu2.executeUpdate();
        println("DERBY500 #2 Rows updated  =" + rowCount);

        commit();
        verifyDerby500Test(pss, buf, cbuf, 0, 1, true);

        // delete, as currently we dont allow stream values to be re-used
        PreparedStatement psd = prepareStatement("delete from test500 where "
                + "mvalue = ?");

        rowCount = 0;
        psd.setInt(1, 0);
        rowCount += psd.executeUpdate();
        rowCount += psd.executeUpdate();
        println("DERBY500 #3 Rows deleted =" + rowCount);

        commit();

        // verify data
        verifyDerby500Test(pss, buf, cbuf, 0, 10, true);

        PreparedStatement psd2 = prepareStatement("delete from test500 "
                + "where id = ?");
        rowCount = 0;

        psd2.setInt(1, 0);
        rowCount += psd2.executeUpdate();
        println("DERBY500 #4 Rows deleted  =" + rowCount);

        commit();
        verifyDerby500Test(pss, buf, cbuf, 1, 2, true);

        try {
            ps.setInt(1, 11);
            rowCount += ps.executeUpdate();
            fail("Attempting to reuse stream should have thrown an exception!");
        } catch (SQLException sqle) {
            if (usingDerbyNetClient()) {
            	// DERBY-4315.  This SQLState is wrong for client.
            	// It should throw XJ001 like embedded.
            	// Also client inserts bad data.
            	// Remove special case when DERBY-4315
            	// is fixed.
//IC see: https://issues.apache.org/jira/browse/DERBY-4312
//IC see: https://issues.apache.org/jira/browse/DERBY-4224
                assertSQLState("XN017", sqle);                
            } else {
                assertSQLState("XJ001", sqle);
                println("EXPECTED EXCEPTION - streams cannot be re-used");
            }
            rollback();
        }
        commit();
        stmt.close();
        pss.close();
        psu2.close();
        psu.close();
        psd.close();
        psd2.close();
        println("END  DERBY-500 TEST ");
        println("======================================");
    }

    /**
     * Test that DERBY500 fix did not change the behavior for varchar, char,
     * long varchar types when stream api is used. Currently, for char,varchar
     * and long varchar - the stream is read once and materialized, hence the
     * materialized stream value will/can be used for multiple executions of the
     * prepared statement
     */
    public void testDerby500_verifyVarcharStreams() throws Exception {
        Statement stmt;
        println("======================================");
        println("START  DERBY-500 TEST for varchar ");

        stmt = createStatement();
        PreparedStatement ps = prepareStatement("insert into test500_verify "
                + "values (?,?,?,?,?)");

        // insert 10 rows.
        int rowCount = 0;

        // use blob and clob values
        int len = 10000;
        byte buf[] = new byte[len];
        char cbuf[] = new char[len];
        char orig = 'c';
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) orig;
            cbuf[i] = orig;
        }
        int randomOffset = 9998;
        buf[randomOffset] = (byte) 'e';
        cbuf[randomOffset] = 'e';
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setString(2, "mname" + i);
            ps.setInt(3, 0);
            ps.setCharacterStream(4, new CharArrayReader(cbuf), len);
            ps.setAsciiStream(5, new ByteArrayInputStream(buf), len);
            rowCount += ps.executeUpdate();
        }
        commit();
        println("Rows inserted =" + rowCount);

        try {
            ps.setInt(1, 11);
            rowCount += ps.executeUpdate();
            // The check below is just to detect a change in behavior for the
            // client driver (this succeeds with the embedded driver due to
            // a different implementation). With the client driver the source
            // stream is read twice, whereas the embedded driver will "cache"
            // the stream content and can thus use it for a second insert.
//IC see: https://issues.apache.org/jira/browse/DERBY-4531
            if (usingDerbyNetClient()) {
                fail("Expected second executeUpdate with client driver to fail");
            }
        } catch (SQLException sqle) {
            if (usingDerbyNetClient()) {
            	// DERBY-4315.  This SQLState is wrong for client.
            	// It should have the same behavior as embedded.
            	// That may rquire some additional work in addition
            	// to DERBY-4315. 
            	// Remove special case when DERBY-4315
            	// is fixed or at least throw XJ001 and
            	// avoid bad data insert.

                // DERBY-4531: Depending on whether the finalizer has been run
                //             or not, the SQLState will differ.
                //             Don't care about this here, accept both.
//IC see: https://issues.apache.org/jira/browse/DERBY-4531
                String expectedState = "XN017";
                if (sqle.getSQLState().equals("XN014")) {
                    expectedState = "XN014";
                }
                assertSQLState(expectedState, sqle);
            } else {
                println("UNEXPECTED EXCEPTION - streams cannot be "
                        + "re-used but in case of varchar, stream is materialized the"
                        + " first time around. So multiple executions using streams should "
                        + " work fine. ");
                throw sqle;
            }
        }

        PreparedStatement pss = prepareStatement(" select lvc,vc from test500_verify where "
                + "id = ?");
        verifyDerby500Test(pss, buf, cbuf, 0, 10, false);

        // do the update, update must qualify more than 1 row and update will
        // pass for char,varchar,long varchar columns.
        PreparedStatement psu = prepareStatement("update test500_verify set vc = ? "
                + ", lvc = ? where mvalue = ?  ");

        buf[randomOffset + 1] = (byte) 'u';
        cbuf[randomOffset + 1] = 'u';
        rowCount = 0;
        psu.setAsciiStream(1, new ByteArrayInputStream(buf), len);
        psu.setCharacterStream(2, new CharArrayReader(cbuf), len);
        psu.setInt(3, 0);
        rowCount += psu.executeUpdate();

        println("DERBY500 for varchar #1 Rows updated  =" + rowCount);

        // verify data
        verifyDerby500Test(pss, buf, cbuf, 0, 10, false);

        PreparedStatement psu2 = prepareStatement("update test500_verify set vc = ? "
                + ", lvc = ? where id = ?  ");

        buf[randomOffset + 1] = (byte) 'h';
        cbuf[randomOffset + 1] = 'h';

        rowCount = 0;
        psu2.setAsciiStream(1, new ByteArrayInputStream(buf), len);
        psu2.setAsciiStream(2, new ByteArrayInputStream(buf), len);
        psu2.setInt(3, 0);
        rowCount += psu2.executeUpdate();

        commit();
        println("DERBY500 for varchar #2 Rows updated  =" + rowCount);
        verifyDerby500Test(pss, buf, cbuf, 0, 1, false);

        // delete, as currently we dont allow stream values to be re-used
        PreparedStatement psd = prepareStatement("delete from test500_verify "
                + "where mvalue = ?");

        rowCount = 0;
        psd.setInt(1, 0);
        rowCount += psd.executeUpdate();
        rowCount += psd.executeUpdate();

        println("DERBY500 for varchar #3 Rows deleted =" + rowCount);

        // verify data
        verifyDerby500Test(pss, buf, cbuf, 0, 10, false);
        PreparedStatement psd2 = prepareStatement("delete from test500_verify where id = ?");
        rowCount = 0;
        psd2.setInt(1, 0);
        rowCount += psd2.executeUpdate();

        commit();
        println("DERBY500 for varchar #4 Rows deleted  =" + rowCount);
        verifyDerby500Test(pss, buf, cbuf, 1, 2, false);
        commit();
        stmt.close();
        pss.close();
        psu2.close();
        psu.close();
        psd.close();
        psd2.close();
        println("END  DERBY-500 TEST  for varchar");
        println("======================================");

    }

    /**
     * verify the data in the derby500Test
     * 
     * @param ps
     *            select preparedstatement
     * @param buf
     *            byte array to compare the blob data
     * @param cbuf
     *            char array to compare the clob data
     * @param startId
     *            start id of the row to check data for
     * @param endId
     *            end id of the row to check data for
     * @param binaryType
     *            flag to indicate if the second column in resultset is a binary
     *            type or not. true for binary type
     * @throws Exception
     */
    private void verifyDerby500Test(PreparedStatement ps, byte[] buf,
            char[] cbuf, int startId, int endId, boolean binaryType)
            throws Exception {
        int rowCount = 0;
        ResultSet rs = null;
        for (int i = startId; i < endId; i++) {
            ps.setInt(1, i);
            rs = ps.executeQuery();
            if (rs.next()) {
                compareCharArray(rs.getCharacterStream(1), cbuf, cbuf.length);
                if (binaryType) {
                    Arrays.equals(rs.getBytes(2), buf);
                    // byteArrayEquals(rs.getBytes(2), 0, buf.length, buf, 0,
                    // buf.length);
                } else {
                    compareCharArray(rs.getCharacterStream(2), cbuf,
                            cbuf.length);
                }
                rowCount++;
            }
        }
        println("Rows selected =" + rowCount);
        rs.close();
    }

    /**
     * compare char data
     * 
     * @param stream
     *            data from stream to compare
     * @param compare
     *            base data to compare against
     * @param length
     *            compare length number of chars.
     * @throws Exception
     */
    private static void compareCharArray(Reader stream, char[] compare,
            int length) throws Exception {
        int c1 = 0;
        int i = 0;
        do {
            c1 = stream.read();
            assertEquals("MISMATCH in data stored versus data retrieved at "
                    + (i - 1), c1, compare[i++]);
            length--;
        } while (c1 != -1 && length > 0);
    }

    private static void streamTestDataVerification(ResultSet rs,
            int maxValueAllowed) throws Exception {

        rs.getMetaData();
        byte[] buff = new byte[128];
        // fetch all rows back, get the varchar and/ long varchar columns as
        // streams.
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
            if ((a >= 1 && a <= 5) && columnSize == maxValueAllowed)
                println("===> verified length " + maxValueAllowed);
            else
                println("test failed, columnSize should be " + maxValueAllowed
                        + " but it is" + columnSize);
        }
    }

    /**
     * blankPadding true means excess trailing blanks false means excess
     * trailing non-blank characters
     * 
     * @param tblType
     *            table type, depending on the table type, the corresponding
     *            table is used. for varchar - testVarChar , for long varchar -
     *            testVarChars,
     */
    private static void insertDataUsingConcat(Statement stmt, int intValue,
            int maxValueAllowed, boolean blankPadding, int tblType,
            String tableName) throws Exception {
        String sql;
        boolean throwsException = false;
        switch (tblType) {
        case LONGVARCHAR:
            sql = "insert into testLongVarChars13 select " + intValue
                    + ", a||b||";
            break;
        case CLOB:
            sql = "insert into testClob14 select " + intValue + ", c||d||";
            throwsException = true;
            break;
        default:
            sql = "insert into testVarChar12 select " + intValue + ", c||d||";
            throwsException = true;
        }

        if (blankPadding) { // try overflow with trailing blanks
            sql = sql.concat("'   ' from " + tableName);
        } else {
            // try overflow with trailing non-blank characters
            sql = sql.concat("'123' from " + tableName);
        }

        // for varchars, trailing blank truncation will not throw an exception.
        // Only non-blank characters will cause truncation error
        // for long varchars, any character truncation will throw an exception.
        try {
            stmt.execute(sql);
            if (throwsException && !blankPadding) {
                fail("Truncation sould have thrown an exception!");
            }
            println("No truncation and hence no error.");
        } catch (SQLException e) {
            assertSQLState("22001", e); // truncation error
            println("expected exception for data > " + maxValueAllowed
                    + " in length");
        }
    }

    // blankPadding: true means excess trailing blanks
    // false means excess trailing non-blank characters
    // testUsingString: true means try setString method for overflow
    // false means try setObject method for overflow
    private static void insertDataUsingStringOrObject(PreparedStatement ps,
            int intValue, int maxValueAllowed, boolean blankPadding,
            boolean testUsingString, int test) throws Exception {
        StringBuffer sb = new StringBuffer(maxValueAllowed);
        for (int i = 0; i < maxValueAllowed; i++) {
            sb.append('q');
        }

        String largeString = new String(sb);
        if (blankPadding) {
            largeString = largeString.concat("   ");
            println("===> testing trailing blanks(using ");
        } else {
            largeString = largeString.concat("123");
            println("===> testing trailing non-blanks(using ");
        }

        ps.setInt(1, intValue);
        if (testUsingString) {
            println("setString) length = " + largeString.length());
            ps.setString(2, largeString);
        } else {
            println("setObject) length = " + largeString.length());
            ps.setObject(2, largeString);
        }

        // for varchars, trailing blank truncation will not throw an exception.
        // Only non-blank characters cause truncation error
        // for long varchars, any character truncation will throw an exception.
        try {
            ps.executeUpdate();
            if (!blankPadding) {
                fail("Should have thrown a truncation error");
            }
            println("No truncation and hence no error");
        } catch (SQLException e) {
            if (largeString.length() > maxValueAllowed) {
                if (!blankPadding && usingDerbyNetClient()) {
                    assertSQLState("XJ001", e); // truncation error
                } else if (test == 13 && usingDerbyNetClient()) {
                    assertSQLState("XJ001", e); // truncation error
                } else {
                    assertSQLState("22001", e); // truncation error
                }
                println("expected exception for data > " + maxValueAllowed
                        + " in length");
            } else {
                throw e;
            }
        }
    }

    /**
     * Method used by testStream12, testStream13, testStream14 to insert data
     * from a file using a character stream
     */
    private static void insertDataUsingCharacterStream(PreparedStatement ps,
            int intValue, String fileName, int maxValueAllowed, int test)
            throws Exception {
        File file = new File(fileName);
        InputStream fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
        Reader filer = new InputStreamReader(fileIn, "US-ASCII");
        println("===> testing(using setCharacterStream) " + fileName
                + " length = " + PrivilegedFileOpsForTests.length(file));
        ps.setInt(1, intValue);
        // insert a streaming column
        ps.setCharacterStream(2, filer, (int) PrivilegedFileOpsForTests
                .length(file));
        // for varchars, trailing blank truncation will not throw an exception.
        // Only non-blank characters cause truncation error
        // for long varchars, any character truncation will throw an exception.
        try {
            ps.executeUpdate();
            if ("extin/char32675.data".equals(fileName)) {
                fail("Should have thrown a truncation error");
            }
            println("No truncation and hence no error");
        } catch (SQLException e) {
            if (PrivilegedFileOpsForTests.length(file) > maxValueAllowed) {
                if (test == 12) {
                    if (usingDerbyNetClient()
                            && "extin/char32675.data".equals(fileName)) {
                        assertSQLState("XJ001", e); // truncation error
                    } else {
                        assertSQLState("22001", e); // truncation error
                    }
                } else if (test == 13) {
                    if (usingDerbyNetClient()) {
                        assertSQLState("XJ001", e); // truncation error
                    } else {
                        assertSQLState("22001", e); // truncation error
                    }
                } else {
                    assertSQLState("XJ001", e); // truncation error
                }
                println("expected exception for data > " + maxValueAllowed
                        + " in length");
            } else {
                throw e;
            }
        } finally {
            filer.close();
        }
    }

    /**
     * Method used by testStream12, testStream13, testStream14 to insert data
     * from a file using an ASCII stream
     */
    private static void insertDataUsingAsciiStream(PreparedStatement ps,
            int intValue, String fileName, int maxValueAllowed, int test)
            throws Exception {
        File file = new File(fileName);
        InputStream fileIn = PrivilegedFileOpsForTests.getFileInputStream(file);
        println("===> testing(using setAsciiStream) " + fileName + " length = "
                + PrivilegedFileOpsForTests.length(file));
        // insert a streaming column
        ps.setInt(1, intValue);
        ps.setAsciiStream(2, fileIn, (int) PrivilegedFileOpsForTests
                .length(file));

        // for varchars, trailing blank truncation will not throw an exception.
        // Only non-blank characters cause truncation error
        // for long varchars, any character truncation will throw an exception.
        try {
            ps.executeUpdate();
            if ("extin/char32675.data".equals(fileName)) {
                fail("Should have thrown a truncation error");
            }
            println("No truncation and hence no error");
        } catch (SQLException e) {
            if (PrivilegedFileOpsForTests.length(file) > maxValueAllowed) {
                if (test == 12) {
                    if (usingDerbyNetClient()
                            && "extin/char32675.data".equals(fileName)) {
                        assertSQLState("XJ001", e); // truncation error
                    } else {
                        assertSQLState("22001", e); // truncation error
                    }
                } else if (test == 13) {
                    if (usingDerbyNetClient()) { // "extin/char32675.data".equals(fileName))
                        // {
                        assertSQLState("XJ001", e); // truncation error
                    } else {
                        assertSQLState("22001", e); // truncation error
                    }
                } else if (test == 14) {
                    if ("extin/char32675.data".equals(fileName)) {
                        assertSQLState("XJ001", e); // truncation error
                    } else {
                        assertSQLState("22001", e); // truncation error
                    }
                }
                println("expected exception for data > " + maxValueAllowed
                        + " in length");
            } else {
                throw e;
            }
        } finally {
            fileIn.close();
        }
    }

    private void verifyLength(int a, int columnSize, long[] fileLength) {

        for (int i = 0; i < fileLength.length; i++) {
            if ((a == (100 + i)) || (a == (10000 + i))) {
                assertEquals("ColumnSize should be " + fileLength[i]
                        + ", but it is " + columnSize + ", i = " + i,
                        fileLength[i], columnSize);
            }
        }
    }

    private void verifyExistence(int key, String base, long length,
            String tableName) throws Exception {
        assertEquals("failed to find value " + base + "... at key " + key, pad(
                base, length), getLongString(key, tableName));
    }

    private String getLongString(int key, String tableName) throws Exception {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select b from " + tableName
                + " where a = " + key);
        assertTrue("There weren't any rows for key = " + key, rs.next());
        String answer = rs.getString(1);
        assertFalse("There were multiple rows for key = " + key, rs.next());
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

    private int insertLongString(int key, String data, boolean binaryColumn,
            String tableName) throws Exception {
        PreparedStatement ps = prepareStatement("insert into " + tableName
                + " values(" + key + ", ?)");
        return streamInStringCol(ps, data, binaryColumn);
    }

    private int updateLongString(int oldkey, int newkey, String tableName)
            throws Exception {
        PreparedStatement ps = prepareStatement("update " + tableName
                + " set a = ?, b = ? where a = " + oldkey);

        String updateString = pad("", newkey);
        ByteArrayInputStream bais = new ByteArrayInputStream(updateString
                .getBytes("US-ASCII"));
        ps.setInt(1, newkey);
        ps.setAsciiStream(2, bais, updateString.length());
        int nRows = ps.executeUpdate();
        ps.close();
        return nRows;
    }

    private int streamInStringCol(PreparedStatement ps, String data,
            boolean binaryColumn) throws Exception {
        int nRows = 0;
        if (data == null) {
            ps.setAsciiStream(1, null, 0);
            nRows = ps.executeUpdate();
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream(data
                    .getBytes("US-ASCII"));
            if (binaryColumn) {
                ps.setBinaryStream(1, bais, data.length());
            } else {
                ps.setAsciiStream(1, bais, data.length());
            }
            nRows = ps.executeUpdate();
            bais.close();
        }
        return nRows;
    }

    /**
     * 
     * @param ps
     *            PreparedStatement
     * @param data
     *            Data to be padded and inserted
     * @return Number of rows
     * @throws Exception
     */
    private static int streamInLongCol(PreparedStatement ps, Object data)
            throws Exception {
        String s = (String) data;
        ByteArrayInputStream bais = new ByteArrayInputStream(s
                .getBytes("US-ASCII"));
        ps.setAsciiStream(1, bais, s.length());
        int nRows = ps.executeUpdate();
        bais.close();
        return nRows;
    }

    /**
     * Runs the test fixtures in embedded and client.
     * 
     * @return test suite
     */
    public static Test suite() {
        Properties strColProperties = new Properties();
        strColProperties.setProperty("derby.storage.sortBufferMax", "5");
        strColProperties.setProperty("derby.debug.true", "testSort");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("StreamingColumnTest");
        suite.addTest(baseSuite("StreamingColumnTest:embedded"));
        suite
                .addTest(TestConfiguration
                        .clientServerDecorator(baseSuite("StreamingColumnTest:client")));
        return new SystemPropertyTestSetup(suite, strColProperties);
    }

    protected static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(StreamingColumnTest.class);
        Test test = new SupportFilesSetup(suite, new String[] {
                "functionTests/tests/store/short.data",
                "functionTests/tests/store/shortbanner",
                "functionTests/tests/store/derby.banner",
                "functionTests/tests/store/empty.data",
                "functionTests/tests/store/char32703trailingblanks.data",
                "functionTests/tests/store/char32703.data",
                "functionTests/tests/store/char32675trailingblanks.data",
                "functionTests/tests/store/char32675.data" });
        return new CleanDatabaseTestSetup(DatabasePropertyTestSetup
                .setLockTimeouts(test, 2, 4)) {
            /**
             * Creates the tables used in the test cases.
             * 
             * @exception SQLException
             *                if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {

                // testStream1
                stmt
                        .execute("create table testLongVarChar1 (a int, b long varchar)");
                // insert a null long varchar
                stmt.execute("insert into testLongVarChar1 values(1, '')");
                // insert a long varchar with a short text string
                stmt
                        .execute("insert into testLongVarChar1 values(2, "
                                + "'test data: a string column inserted as an object')");
                // todo use setProperty method
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
                stmt
                        .execute("create table foo1 (a int not null, b long varchar, primary key (a))");
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");

                // testStream2_1500
                stmt.execute("create table foo2_1500 (a int not null, "
                        + "b long varchar, primary key (a))");

                // testStream2_5000
                stmt.execute("create table foo2_5000 (a int not null, "
                        + "b long varchar, primary key (a))");

                // testStream2_10000
                stmt.execute("create table foo2_10000 (a int not null, "
                        + "b long varchar, primary key (a))");

                // testStream3_0
                stmt.execute("create table foo3_0 (a int not null "
                        + "constraint pk3_0 primary key, b long varchar)");

                // testStream3_1500
                stmt.execute("create table foo3_1500 (a int not null "
                        + "constraint pk3_1500 primary key, b long varchar)");

                // testStream3_5000
                stmt.execute("create table foo3_5000 (a int not null "
                        + "constraint pk3_5000 primary key, b long varchar)");

                // testStream3_10000
                stmt.execute("create table foo3_10000 (a int not null "
                        + "constraint pk3_10000 primary key, b long varchar)");

                // testStream4
                stmt
                        .execute("create table testLongVarBinary4 (a int, b BLOB(1G))");

                // testStream5_0
                long length = 0;
                String binaryType = length > 32700 ? "BLOB(1G)"
                        : "long varchar for bit data";
                stmt.execute("create table foo5_0 (a int not null "
                        + "constraint pk5_0 primary key, b " + binaryType
                        + " )");

                // testStream5_1500
                length = 1500;
                binaryType = length > 32700 ? "BLOB(1G)"
                        : "long varchar for bit data";
                stmt.execute("create table foo5_1500 (a int not null "
                        + "constraint pk5_1500 primary key, b " + binaryType
                        + " )");

                // testStream5_5000
                length = 5000;
                binaryType = length > 32700 ? "BLOB(1G)"
                        : "long varchar for bit data";
                stmt.executeUpdate("create table foo5_5000 (a int not null "
                        + "constraint pk5_5000 primary key, b " + binaryType
                        + " )");

                // testStream5_100000
                length = 100000;
                binaryType = length > 32700 ? "BLOB(1G)"
                        : "long varchar for bit data";
                stmt.executeUpdate("create table foo5_100000 (a int not null "
                        + "constraint pk5_100000 primary key, b " + binaryType
                        + " )");

                // testStream6
                stmt
                        .executeUpdate("create table foo_6 (a int not null constraint"
                                + " pk6 primary key, b long varchar)");

                // testStream7
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
                stmt
                        .execute("create table testlvc7 (a int, b char(100), lvc long varchar, d char(100))");
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");

                // testStream8_10_2500
                stmt
                        .execute("create table t8_10_2500(a int, b long varchar, c long varchar)");

                // testStream8_2500_10
                stmt
                        .execute("create table t8_2500_10(a int, b long varchar, c long varchar)");

                // testStream9_10_2500
                stmt
                        .execute("create table t9_10_2500(a int, b long varchar for bit data, "
                                + "c long varchar for bit data)");

                // testStream9_2500_10
                stmt
                        .execute("create table t9_2500_10(a int, b long varchar for bit data, "
                                + "c long varchar for bit data)");

                // testStream10
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '1024')");
                stmt
                        .execute("create table tab10 (a int, b int, c long   varchar)");
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
                // create the indexes which shares columns
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
                stmt.execute("create index i_a on tab10 (a)");
                stmt.execute("create index i_ab on tab10 (a, b)");
                stmt
                        .execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
                // insert a null long varchar
                stmt.execute("insert into tab10 values(1, 1, '')");
                // insert a long varchar with a short text string
                stmt
                        .execute("insert into tab10 values(2, 2, 'test data: a string column inserted as an object')");

                // testStream11
                stmt
                        .execute("create table testLongVarCharInvalidStreamLength11 "
                                + "(a int, b long varchar, c long varchar for bit data)");

                // testStream12
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
                stmt
                        .execute("create table testVarChar12 (a int, b varchar(32672))");
                // create a table with 4 varchars. This table will be used to
                // try
                // overflow through concatenation
                stmt
                        .execute("create table testConcatenation12 (a varchar(16350), b varchar(16350), c varchar(16336), d varchar(16336))");
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");

                // testStream13
                stmt
                        .execute("create table testLongVarChars13 (a int, b long varchar)");

                // testStream14
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
                stmt.execute("create table testClob14 (a int, b clob(32672))");
                // create a table with 4 varchars. This table will be used to
                // try
                // overflow through concatenation

                stmt
                        .execute("create table testConcatenation14 (a clob(16350), b clob(16350), c clob(16336), d clob(16336))");
                stmt
                        .executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");

                // testDerby500
                stmt.execute("CREATE TABLE test500 (" + "id INTEGER NOT NULL,"
                        + "mname VARCHAR( 254 ) NOT NULL,"
                        + "mvalue INT NOT NULL," + "bytedata BLOB NOT NULL,"
                        + "chardata CLOB NOT NULL," + "PRIMARY KEY ( id ))");

                // testDerby500_verifyVarcharStreams
                stmt.execute("CREATE TABLE test500_verify ("
                        + "id INTEGER NOT NULL,"
                        + "mname VARCHAR( 254 ) NOT NULL,"
                        + "mvalue INT NOT NULL," + "vc varchar(32500),"
                        + "lvc long varchar NOT NULL," + "PRIMARY KEY ( id ))");
            }
        };
    }

}

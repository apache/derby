/*
 *
 * Derby - Class BlobClob4BlobTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;
import org.apache.derbyTesting.functionTests.util.Formatters;

import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.Utilities;

import junit.framework.*;
import java.sql.*;

import org.apache.derbyTesting.functionTests.util.streams.ByteAlphabet;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of JDBC blob and clob
 */
public class BlobClob4BlobTest extends BaseJDBCTestCase {

    /** Creates a new instance of BlobClob4BlobTest */
    public BlobClob4BlobTest(String name) {
        super(name);
    }

    /**
     * Set up the conection to the database.
     */
    public void setUp() throws  Exception {
        getConnection().setAutoCommit(false);

        // creating small tables then add large column - that way forcing table
        // to have default small page size, but have large rows.

        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE testClob (b INT, c INT)");
        stmt.executeUpdate("ALTER TABLE testClob ADD COLUMN a CLOB(300K)");

        stmt.executeUpdate("CREATE TABLE testBlob (b INT)");
        stmt.executeUpdate("ALTER TABLE testBlob ADD COLUMN a blob(300k)");
        stmt.executeUpdate("ALTER TABLE testBlob ADD COLUMN crc32 BIGINT");

        stmt.close();
        commit();
    }

    protected void tearDown() throws Exception {
        rollback();
        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE testClob");
        stmt.executeUpdate("DROP TABLE testBlob");
        stmt.close();
        commit();
        super.tearDown();
    }

    /***                TESTS               ***/

    /**
     * DERBY-3085.  Test update where streamed parameter is not 
     * consumed by the server. Network Server needs to clean-up 
     * after execution.
     * 
     */
    public void testUnconsumedParameter() throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        // Test table with no rows.
        s.executeUpdate("create table testing(num int, addr varchar(40), contents blob(16M))");
        // no rows inserted so there is no match.
        byte[] data = new byte[ 38000];
        for (int i = 0; i < data.length; i++)
            data[i] = 'a';
        ByteArrayInputStream is = new ByteArrayInputStream( data);           
        String sql = "UPDATE testing SET Contents=? WHERE num=1";
        
        PreparedStatement ps = prepareStatement( sql);
        ps.setBinaryStream( 1, is,data.length);
        ps.executeUpdate();          
        // Make sure things still work ok when we have a parameter that does get consumed.
        // insert a matching row.
        s.executeUpdate("insert into testing values (1,null,null)");
        is = new ByteArrayInputStream(data);
        ps.setBinaryStream( 1, is,data.length);
        ps.executeUpdate();
        // Check update occurred
        ResultSet rs = s.executeQuery("select length(contents) from testing where num = 1");
        JDBC.assertSingleValueResultSet(rs, "38000");
        ps.close();
        conn.commit();
        // Check the case where there are rows inserted but there is no match.
        is = new ByteArrayInputStream( data);           
        sql = "UPDATE testing SET Contents=? WHERE num=2";
        ps = prepareStatement( sql);
        ps.setBinaryStream( 1, is,data.length);
        ps.executeUpdate();
        ps.close();
        s.executeUpdate("drop table testing");
        conn.commit();
        
        // Test with multiple parameters
        s.executeUpdate("create table testing(num int, addr varchar(40), contents blob(16M),contents2 blob(16M))");
        
        is = new ByteArrayInputStream( data);
        ByteArrayInputStream is2 = new ByteArrayInputStream(data);
        sql = "UPDATE testing SET Contents=?, contents2=?  WHERE num=1";

        ps = prepareStatement( sql);
        ps.setBinaryStream( 1, is,data.length);
        ps.setBinaryStream(2, is2,data.length);
        ps.executeUpdate();
        
        
        // multiple parameters and matching row
        s.executeUpdate("insert into testing values (1,'addr',NULL,NULL)");
        is = new ByteArrayInputStream( data);
        is2 = new ByteArrayInputStream(data);
        ps.setBinaryStream( 1, is,data.length);
        ps.setBinaryStream(2, is2,data.length);
        ps.executeUpdate();
        rs = s.executeQuery("select length(contents), length(contents2) from testing where num = 1");
        JDBC.assertFullResultSet(rs, new String[][] {{"38000","38000"}});
        rs.close();
        s.executeUpdate("drop table testing");
        
        // With Clob
        s.executeUpdate("create table testing(num int, addr varchar(40), contents Clob(16M))");
        char[] charData = new char[ 38000];
        for (int i = 0; i < data.length; i++)
       data[i] = 'a';
        CharArrayReader reader = new CharArrayReader( charData);            
        sql = "UPDATE testing SET Contents=? WHERE num=1";

       ps = prepareStatement( sql);
       ps.setCharacterStream( 1, reader,charData.length);
       ps.executeUpdate();
       // with a matching row
       s.executeUpdate("insert into testing values (1,null,null)");
       reader = new CharArrayReader(charData);
       ps.setCharacterStream( 1, reader,data.length);
       ps.executeUpdate();
       // Check update occurred
       rs = s.executeQuery("select length(contents) from testing where num = 1");
       JDBC.assertSingleValueResultSet(rs, "38000");
       s.executeUpdate("drop table testing");
       ps.close();
       
       conn.commit();
        
    }

    /**
     * Test that it is possible to change the isolation level after reading a
     * BLOB (DERBY-3427).
     */
    public void testIsolationLevelChangeAfterRead() throws SQLException {
        ResultSet rs =
            createStatement().executeQuery("VALUES CAST(X'FFFF' AS BLOB)");
        JDBC.assertDrainResults(rs);
        getConnection().setTransactionIsolation(
            Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * Tests PreparedStatement.setCharacterStream
     */
    public void testSetCharacterStream() throws Exception {
        int clobLength = 5009;

        // insert a streaming column
        PreparedStatement ps = prepareStatement(
                "insert into testClob (a) values(?)");
        Reader streamReader = new LoopingAlphabetReader(
                clobLength, CharAlphabet.tamil());
        ps.setCharacterStream(1, streamReader, clobLength);
        ps.executeUpdate();
        streamReader.close();
        ps.close();
        commit();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT a FROM testClob");
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            assertEquals("FAIL - wrong clob length", clobLength, clob.length());
            Reader clobValue = clob.getCharacterStream();
            Reader origValue = new LoopingAlphabetReader(
                    clobLength, CharAlphabet.tamil());

            assertTrue("New clob value did not match",
                    compareReaders(origValue, clobValue));
            origValue.close();
            clobValue.close();
        }
        rs.close();
        stmt.close();

        commit();

    }

    /**
     *  basic test of getAsciiStream also tests length
     */
    public void testGetAsciiStream() throws Exception {
        byte[] buff = new byte[1024];

        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT a, b FROM testClob");

        // fetch row back, get the column as a clob.
        Clob clob;
        int clobLength;
        while (rs.next()) {
            // get the first column in select as a clob
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);

            if (clob != null) {
                assertEquals("FAIL - wrong clob.length()",
                        clobLength, clob.length());

                InputStream fin = clob.getAsciiStream();
                int columnSize = 0;
                int size = -1;
                do {
                    size = fin.read(buff);
                    columnSize += (size > 0) ? size : 0;
                } while (size >= 0);

                assertEquals("FAIL - wrong column size",
                        clobLength, columnSize);
            } else {
                assertTrue("Clob was null but length was not 0",
                        (clobLength == 0));
            }
        }
        rs.close();
        stmt.close();

        commit();
    }

    /**
     * basic test of getCharacterStream also tests length
     */
    public void testGetCharacterStream() throws Exception {

        char[] buff = new char[128];

        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a , b from testClob");
        ResultSetMetaData met = rs.getMetaData();

        // fetch row back, get the column as a clob.
        int clobLength = 0;
        while (rs.next()) {
            // get the first column as a clob
            Clob clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            if (clob != null) {
                assertEquals("FAIL - wrong clob.length()",
                        clobLength, clob.length());

                Reader reader = clob.getCharacterStream();
                int columnSize = 0;
                int size = -1;
                do {
                    size = reader.read(buff);
                    columnSize += (size >= 0) ? size : 0;
                } while (size >= 0);

                assertEquals("FAIL - wrong column size",
                        clobLength, columnSize);
            } else {
                assertTrue("Clob was null but length was not 0",
                        (clobLength == 0));
            }
        }
        rs.close();
        stmt.close();

        commit();
    }

    /**
     * test of getCharacterStream on a table containing unicode characters
     */
    public void testGetCharacterStreamWithUnicode() throws Exception {
        String[] unicodeStrings = {
            "\u0061\u0062\u0063",
            "\u0370\u0371\u0372",
            "\u05d0\u05d1\u05d2"};
        insertUnicodeData(unicodeStrings);

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT a, b, c FROM testClob");

        int clobLength = 0, arrayIndex = 0;
        while (rs.next()) {
            clobLength = rs.getInt(2);
            arrayIndex = rs.getInt(3);
            Clob clob = rs.getClob(1);
            if (clob != null) {
                assertEquals("FAIL - wrong clob.length()",
                        clobLength, clob.length());

                Reader clobValue = clob.getCharacterStream();
                if (arrayIndex > 0) {
                    char[] buff = new char[3];
                    clobValue.read(buff);
                    assertEquals("Clob value does not match unicodeString",
                            unicodeStrings[arrayIndex],
                            new String(buff));
                    assertEquals("Expected end of stream",
                            -1, clobValue.read());
                } else {
                    Reader origValue = new LoopingAlphabetReader(
                            clobLength, CharAlphabet.tamil());
                    compareReaders(origValue, clobValue);
                }
            } else {
                assertTrue("Clob was null but length was not 0",
                        (clobLength == 0));
            }
        }
        rs.close();
        stmt.close();

        commit();
    }

    /**
     * Test triggers on CLOB columns.
     */
    public void testTriggersWithClobColumn() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        stmt.executeUpdate(
                "CREATE TABLE testClobTriggerA (a CLOB(400k), b int)");
        stmt.executeUpdate(
                "CREATE TABLE testClobTriggerB (a CLOB(400k), b int)");
        stmt.executeUpdate(
                "create trigger T13A after update on testClob " +
                "referencing new as n old as o " +
                "for each row "+
                "insert into testClobTriggerA(a, b) values (n.a, n.b)");
        stmt.executeUpdate(
                "create trigger T13B after INSERT on testClobTriggerA " +
                "referencing new table as n " +
                "for each statement "+
                "insert into testClobTriggerB(a, b) select n.a, n.b from n");

        commit();

        // Fire the triggers
        stmt.executeUpdate("UPDATE testClob SET b = b + 0");
        commit();

        // Verify the results
        Statement origSt = createStatement();
        Statement trigASt = createStatement();
        Statement trigBSt = createStatement();

        ResultSet origRS = origSt.executeQuery(
                "select a, length(a), b  from testClob order by b");
        ResultSet trigARS = trigASt.executeQuery(
                "select a, length(a), b from testClobTriggerA order by b");
        ResultSet trigBRS = trigBSt.executeQuery(
                "select a, length(a), b from testClobTriggerA order by b");

        int count = 0;
        while (origRS.next()) {
            count ++;
            assertTrue("row trigger produced less rows " +
                    count, trigARS.next());
            assertTrue("statement trigger produced less rows " +
                    count, trigBRS.next());

            if (origRS.getClob(1) != null) {
                assertEquals("FAIL - Invalid checksum for row trigger",
                        getStreamCheckSum(origRS.getClob(1).getAsciiStream()),
                        getStreamCheckSum(trigARS.getClob(1).getAsciiStream()));
                assertEquals("FAIL - Invalid checksum for statement trigger",
                        getStreamCheckSum(origRS.getClob(1).getAsciiStream()),
                        getStreamCheckSum(trigBRS.getClob(1).getAsciiStream()));
            }

            assertEquals("FAIL - Invalid length in row trigger",
                    origRS.getInt(2), trigARS.getInt(2));
            assertEquals("FAIL - Invalid length in statement trigger",
                    origRS.getInt(2), trigBRS.getInt(2));

            assertEquals("FAIL - Length not updated on row trigger",
                    origRS.getInt(3), trigARS.getInt(3));
            assertEquals("FAIL - Length not updated on statement trigger",
                    origRS.getInt(3), trigBRS.getInt(3));
        }

        origRS.close();
        trigARS.close();
        trigBRS.close();
        origSt.close();
        trigASt.close();
        trigBSt.close();

        stmt.executeUpdate("DROP TRIGGER T13A");
        stmt.executeUpdate("DROP TRIGGER T13B");
        stmt.executeUpdate("DROP TABLE testClobTriggerB");
        stmt.executeUpdate("DROP TABLE testClobTriggerA");

        stmt.close();
        commit();
    }

    /**
     * test Clob.getSubString() method
     */
    public void testGetSubString() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        int clobLength = 0;
        Clob clob;
        while (rs.next()) {
            clob = rs.getClob(1);
            if (clob == null)
                continue;
            clobLength = rs.getInt(2);
            verifyInterval(clob, 9905, 50, 0, clobLength);
            verifyInterval(clob, 5910, 150, 1, clobLength);
            verifyInterval(clob, 5910, 50, 2, clobLength);
            verifyInterval(clob, 204, 50, 3, clobLength);
            verifyInterval(clob, 68, 50, 4, clobLength);
            verifyInterval(clob, 1, 50, 5, clobLength);
            verifyInterval(clob, 1, 1, 6, clobLength);
            verifyInterval(clob, 1, 0, 7, clobLength); // length 0 at start
            verifyInterval(clob, clobLength + 1, 0, 8, clobLength); // and end
            if (clobLength > 100) {
                String res = clob.getSubString(clobLength-99,200);
                assertEquals("FAIL - wrong length of substring",
                        100, res.length());
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * test getSubString with unicode
     */
    public void testGetSubStringWithUnicode() throws Exception {
        String[] unicodeStrings = {
            "\u0061\u0062\u0063",
            "\u0370\u0371\u0372",
            "\u05d0\u05d1\u05d2"};
        insertUnicodeData(unicodeStrings);

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b, c from testClob");
        int clobLength = 0, arrayIndex = 0;
        Clob clob;
        while (rs.next()) {
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            arrayIndex = rs.getInt(3);
            if (clob != null) {
                if (arrayIndex >= 0) {
                    assertEquals("FAIL - wrong substring returned",
                            unicodeStrings[arrayIndex],
                            clob.getSubString(1, 3));
                } else {
                    if (clob.length() > 0) {
                        long charsToRead = Math.min((clob.length() / 3), 2048);
                        char[] charValue = new char[(int)charsToRead];
                        Reader clobReader = clob.getCharacterStream();
                        clobReader.read(charValue);
                        clobReader.read(charValue);
                        String subString = clob.getSubString(charsToRead + 1,
                                (int)charsToRead);
                        assertEquals("FAIL - wrong substring length",
                                charValue.length, subString.length());
                        for (int i=0; i< charValue.length; i++) {
                            assertEquals("FAIL - wrong substring returned at " +
                                    i, charValue[i], subString.charAt(i));
                        }
                    }
                }
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * test position with a String argument
     */
    public void testPositionString() throws Exception {
        insertDefaultData();

        runPositionStringTest();
    }

    /**
     * test position with a String argument and unicode data.
     */
    public void testPositionStringWithUnicode() throws Exception {
        String[] unicodeStrings = {
            "\u0061\u0062\u0063",
            "\u0370\u0371\u0372",
            "\u05d0\u05d1\u05d2"};
        insertUnicodeData(unicodeStrings);

        runPositionStringTest();
    }

    private void runPositionStringTest() throws Exception {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        int clobLength = 0;
        Clob clob;
        Random random = new Random();
        String searchString;
        int start, length, startSearchPos;
        int distance, maxStartPointDistance;
        long foundAt;
        // clobs are generated with looping alphabet streams
        maxStartPointDistance = CharAlphabet.MODERNLATINLOWER.length;
        while (rs.next()) {
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            if (clob != null && clobLength > 0) {
                println("\n\nclobLength: " + clobLength);
                for (int i=0; i<10; i++) {
                    // find a random string to search for
                    start = Math.max(random.nextInt(clobLength - 1), 1);
                    length = random.nextInt(clobLength - start) + 1;
                    println("start:" + start + " length:" + length);
                    searchString = clob.getSubString(start, length);
                    // get random position to start the search from
                    distance = random.nextInt(maxStartPointDistance);
                    startSearchPos = Math.max((start - distance), 1);
                    // make sure that the searched string does not happen
                    // before the expected position
                    String tmp = clob.getSubString(startSearchPos, start);
                    if (tmp.indexOf(searchString) != -1) {
                        startSearchPos = start;
                    }
                    println("startSearchPos: " + startSearchPos +
                            "searchString: " + searchString);
                    foundAt = clob.position(searchString, startSearchPos);
                    assertEquals("FAIL - wrong match found for " +
                            searchString + " start at " + startSearchPos +
                            " with length " + searchString.length(),
                            start, foundAt);
                }
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * test position with a Clob argument
     */
    public void testPositionClob() throws Exception {
        insertDefaultData();

        runPositionClobTest();
    }

    /**
     * test position with a Clob argument containing unicode characters
     */
    public void testPositionClobWithUnicode() throws Exception {
        String[] unicodeStrings = {
            "\u0061\u0062\u0063",
            "\u0370\u0371\u0372",
            "\u05d0\u05d1\u05d2"};
        insertUnicodeData(unicodeStrings);

        runPositionClobTest();
    }

    private void runPositionClobTest() throws Exception {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        int clobLength = 0;
        Clob clob;
        Statement stmt2 = createStatement();
        Random random = new Random();
        String searchString;
        int start, length, startSearchPos;
        int distance, maxStartPointDistance;
        long foundAt;
        // clobs are generated with looping alphabet streams
        maxStartPointDistance = CharAlphabet.MODERNLATINLOWER.length;
        while (rs.next()) {
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            if (clob != null && clobLength > 0) {
                println("\n\nclobLength: " + clobLength);
                // Create a table with clobs to search
                stmt2.executeUpdate("CREATE TABLE searchClob " +
                        "(a clob(300K), start int, position int)");
                // insert clobs into the table
                PreparedStatement ps = prepareStatement(
                        "INSERT INTO searchClob values (?, ?, ?) ");
                for (int i=0; i<10; i++) {
                    // find a random string to search for
                    start = Math.max(random.nextInt(clobLength - 1), 1);
                    length = random.nextInt(clobLength - start) + 1;
                    println("start:" + start + " length:" + length);
                    searchString = clob.getSubString(start, length);
                    // get random position to start the search from
                    distance = random.nextInt(maxStartPointDistance);
                    startSearchPos = Math.max((start - distance), 1);
                    // make sure that the searched string does not happen
                    // before the expected position
                    String tmp = clob.getSubString(startSearchPos, start);
                    if (tmp.indexOf(searchString) != -1) {
                        startSearchPos = start;
                    }

                    ps.setString(1, searchString);
                    ps.setInt(2, startSearchPos);
                    ps.setInt(3, start);
                    ps.executeUpdate();
                }

                ps.close();

                ResultSet rs2 = stmt2.executeQuery(
                        "SELECT a, start, position FROM searchClob");
                while (rs2.next()) {
                    Clob searchClob = rs2.getClob(1);
                    startSearchPos = rs2.getInt(2);
                    start = rs2.getInt(3);

                    searchString = searchClob.getSubString(1L,
                            (int)searchClob.length());
                    println("startSearchPos: " + startSearchPos +
                            "searchString: " + searchString);
                    foundAt = clob.position(searchClob, startSearchPos);
                    assertEquals("FAIL - wrong match found for " +
                            searchString + " starting at " + startSearchPos +
                            " with length " + searchString.length(),
                            start, foundAt);
                }
                rs2.close();
                stmt2.executeUpdate("DROP TABLE searchClob");
            }
        }
        rs.close();
        stmt.close();
        stmt2.close();
    }

    /**
     * make sure clobs work for small CLOB fields also test length method
     */
    public void testSmallClobFields() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate(
                "ALTER TABLE testClob ADD COLUMN smallClob CLOB(10)");

        PreparedStatement ps = prepareStatement(
                "insert into testClob (smallClob) values(?)");
        String val = "";
        for (int i = 0; i < 10; i++) {
            // insert a string
            ps.setString(1, val);
            ps.executeUpdate();
            val += "x";
        }

        ResultSet rs = stmt.executeQuery("select a from testClob");
        byte[] buff = new byte[128];
        int j = 0;
        // fetch all rows back, get the columns as clobs.
        while (rs.next()) {
            // get the first column as a clob
            Clob clob = rs.getClob(1);
            if (clob != null) {
                InputStream fin = clob.getAsciiStream();
                int columnSize = 0, size = 0;
                do
                {
                    size = fin.read(buff);
                    columnSize += (size > 0) ? size : 0;
                } while (size != -1);
                assertEquals("FAIL - wrong clob size", j, columnSize);
                assertEquals("FAIL - wrong clob length", j, clob.length());
                j++;
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * make sure cannot get a clob from an int column
     */
    public void testGetClobFromIntColumn() throws Exception{

        insertDefaultData();

        Statement stmt = createStatement();

        ResultSet rs = stmt.executeQuery("select b from testClob");
        while (rs.next()) {
            try {
                Clob clob = rs.getClob(1);

                rs.close(); // Cleanup on fail
                stmt.close();

                fail("FAIL - getClob on column type int should throw " +
                        "an exception");
            } catch (SQLException se) {
                checkException(LANG_DATA_TYPE_GET_MISMATCH, se);
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * make sure setClob doesn't work on an int column
     */
    public void testSetClobToIntColumn() throws Exception {
        insertDefaultData();

        PreparedStatement ps = prepareStatement(
                "insert into testClob (b, c) values (?, ?)");
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        Clob clob;
        int clobLength;
        while (rs.next()) {
            // get the first ncolumn as a clob
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            if (clob != null) {
                try {
                    ps.setClob(1,clob);
                    ps.setInt(2, clobLength);
                    ps.executeUpdate();

                    rs.close(); // Cleanup on fail
                    stmt.close();

                    fail("FAIL - can not use setClob on int column");
                } catch (SQLException se) {
                    checkException(LANG_DATA_TYPE_GET_MISMATCH, se);
                }
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * test raising of exceptions
     */
    public void testRaisingOfExceptionsClob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select a, b from testClob WHERE a is not NULL");
        int i = 0, clobLength = 0;
        Clob clob;
        rs.next();
        clob = rs.getClob(1);
        clobLength = rs.getInt(2);
        rs.close();
        assertFalse("FAIL - clob can not be null", clob == null);

        // 0 or negative position value
        try {
            clob.getSubString(0,5);
            fail("FAIL - getSubString with 0 as position should have " +
                    "caused an exception");
        } catch (SQLException e) {
            checkException(BLOB_BAD_POSITION, e);
        }

        // negative length value
        try {
            clob.getSubString(1,-76);
            fail("FAIL - getSubString with negative length should have " +
                    "caused an exception");
        } catch (SQLException e) {
            checkException(BLOB_NONPOSITIVE_LENGTH, e);
        }
        // boundary negative 1 length
        try {
            clob.getSubString(1,-1);
            fail("FAIL - getSubString with negative length should have " +
                    "caused an exception");
        } catch (SQLException e) {
            checkException(BLOB_NONPOSITIVE_LENGTH, e);
        }
        // before start with length zero
        try {
            clob.getSubString(0,0);
            fail("FAIL - getSubString with 0 as position should have " +
                    "caused an exception");
        } catch (SQLException e) {
            checkException(BLOB_BAD_POSITION, e);
        }
        // 2 past end with length 0
        try {
            clob.getSubString(clobLength + 2,0);
            fail("FAIL - getSubString with position bigger than clob " +
                    "should have caused an exception");
        }  catch (SQLException e) {
            checkException(BLOB_POSITION_TOO_LARGE, e);
        } catch (StringIndexOutOfBoundsException se) {
            assertTrue("FAIL - This exception should only happen with " +
                    "DB2 client", usingDB2Client());
        }
        // 0 or negative position value
        try {
            clob.position("xx",-4000);
            fail("FAIL - position with negative as position should " +
                    "have caused an exception");
        } catch (SQLException e) {
            checkException(BLOB_BAD_POSITION, e);
        }
        // null pattern
        try {
            clob.position((String) null,5);
            fail("FAIL = position((String) null,5)");
        } catch (SQLException e) {
            checkException(BLOB_NULL_PATTERN_OR_SEARCH_STR, e);
        }
        // 0 or negative position value
        try {
            clob.position(clob,-42);
            fail("FAIL = position(clob,-42)");
        } catch (SQLException e) {
            checkException(BLOB_BAD_POSITION, e);
        }
        // null pattern
        try {
            clob.position((Clob) null,5);
            fail("FAIL = pposition((Clob) null,5)");
        } catch (SQLException e) {
            checkException(BLOB_NULL_PATTERN_OR_SEARCH_STR, e);
        }
    }

    /**
     * test setClob
     */
    public void testSetClob() throws Exception {
        insertDefaultData();
        ResultSet rs, rs2;
        Statement stmt = createStatement();
        stmt.execute("create table testSetClob (a CLOB(300k), b integer)");
        PreparedStatement ps = prepareStatement(
                "insert into testSetClob values(?,?)");
        rs = stmt.executeQuery("select a, b from testClob");
        Clob clob;
        int clobLength;
        while (rs.next()) {
            // get the first column as a clob
            clob = rs.getClob(1);
            clobLength = rs.getInt(2);
            if (clob != null && clobLength != 0) {
                ps.setClob(1,clob);
                ps.setInt(2,clobLength);
                ps.executeUpdate();
            }
        }
        rs.close();

        rs = stmt.executeQuery("select a, b from testSetClob");
        Clob clob2;
        int clobLength2, nullClobs = 0;
        while (rs.next()) {
            clob2 = rs.getClob(1);
            clobLength2 = rs.getInt(2);
            assertFalse("FAIL - Clob is NULL", clob2 == null);
            assertEquals("FAIL - clob.length() != clobLength",
                    clobLength2, clob2.length());
        }
        rs.close();
        stmt.executeUpdate("DROP TABLE testSetClob");
        stmt.close();
    }

    /**
     * Test Clob.position()
     */
    public void testPositionAgressive() throws Exception {
        Statement s = createStatement();

        s.execute("CREATE TABLE C8.T8POS" +
                "(id INT NOT NULL PRIMARY KEY, DD CLOB(1m), pos INT, L INT)");
        s.execute("CREATE TABLE C8.T8PATT(PATT CLOB(300k))");

        // characters used to fill the String
        char[] fill = new char[4];
        fill[0] = 'd';          // 1 byte UTF8 character (ASCII)
        fill[1] = '\u03a9';     // 2 byte UTF8 character (Greek)
        fill[2] = '\u0e14';     // 3 byte UTF8 character (Thai)
        fill[3] = 'j';          // 1 byte UTF8 character (ASCII)

        char[] base = new char[256 * 1024];

        for (int i = 0; i < base.length; i += 4) {

            base[i] = fill[0];
            base[i+1] = fill[1];
            base[i+2] = fill[2];
            base[i+3] = fill[3];

        }

        char[]  patternBase = new char[2 * 1024];
        for (int i = 0; i < patternBase.length; i += 8) {

            patternBase[i] = 'p';
            patternBase[i+1] = 'a';
            patternBase[i+2] = 't';
            patternBase[i+3] = '\u03aa';
            patternBase[i+4] = (char) i;// changed value to keep pattern varying
            patternBase[i+5] = 'b';
            patternBase[i+6] = 'm';
            patternBase[i+7] = '\u0e15';

        }

        PreparedStatement ps = prepareStatement(
                "INSERT INTO C8.T8POS VALUES (?, ?, ?, ?)");
        PreparedStatement psp = prepareStatement(
                "INSERT INTO C8.T8PATT VALUES (?)");

        T8insert(ps, 1, base, 256, patternBase, 8, 100, true);
        T8insert(ps, 2, base, 3988, patternBase, 8, 2045, true);
        T8insert(ps, 3, base, 16321, patternBase, 8, 4566, true);
        T8insert(ps, 4, base, 45662, patternBase, 8, 34555, true);
        T8insert(ps, 5, base, 134752, patternBase, 8, 67889, true);
        T8insert(ps, 6, base, 303, patternBase, 8, 80, false);
        T8insert(ps, 7, base, 4566, patternBase, 8, 2086, false);
        T8insert(ps, 8, base, 17882, patternBase, 8, 4426, false);
        T8insert(ps, 9, base, 41567, patternBase, 8, 31455, false);
        String pstr =
                T8insert(ps, 10, base, 114732, patternBase, 8, 87809, false);

        commit();

        psp.setString(1, pstr);
        psp.executeUpdate();

        checkClob8(s, pstr);
        commit();

        ResultSet rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
        rsc.next();
        checkClob8(s, rsc.getClob(1));

        rsc.close();


        commit();

        s.execute("DELETE FROM C8.T8POS");
        s.execute("DELETE FROM C8.T8PATT");


        T8insert(ps, 1, base, 256, patternBase, 134, 100, true);
        T8insert(ps, 2, base, 3988, patternBase, 134, 2045, true);
        T8insert(ps, 3, base, 16321, patternBase, 134, 4566, true);
        T8insert(ps, 4, base, 45662, patternBase, 134, 34555, true);
        T8insert(ps, 5, base, 134752, patternBase, 134, 67889, true);
        T8insert(ps, 6, base, 303, patternBase, 134, 80, false);
        T8insert(ps, 7, base, 4566, patternBase, 134, 2086, false);
        T8insert(ps, 8, base, 17882, patternBase, 134, 4426, false);
        T8insert(ps, 9, base, 41567, patternBase, 134, 31455, false);
        pstr = T8insert(ps, 10, base, 114732, patternBase, 134, 87809, false);

        commit();
        psp.setString(1, pstr);
        psp.executeUpdate();
        commit();


        checkClob8(s, pstr);
        commit();

        rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
        rsc.next();
        checkClob8(s, rsc.getClob(1));

        s.execute("DELETE FROM C8.T8POS");
        s.execute("DELETE FROM C8.T8PATT");

        T8insert(ps, 1, base, 256, patternBase, 679, 100, true);
        T8insert(ps, 2, base, 3988, patternBase, 679, 2045, true);
        T8insert(ps, 3, base, 16321, patternBase, 679, 4566, true);
        T8insert(ps, 4, base, 45662, patternBase, 679, 34555, true);
        T8insert(ps, 5, base, 134752, patternBase, 679, 67889, true);
        T8insert(ps, 6, base, 303, patternBase, 679, 80, false);
        T8insert(ps, 7, base, 4566, patternBase, 679, 2086, false);
        T8insert(ps, 8, base, 17882, patternBase, 679, 4426, false);
        T8insert(ps, 9, base, 41567, patternBase, 679, 31455, false);
        pstr = T8insert(ps, 10, base, 114732, patternBase, 679, 87809, false);

        commit();
        psp.setString(1, pstr);
        psp.executeUpdate();
        commit();


        checkClob8(s, pstr);
        commit();

        rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
        rsc.next();
        checkClob8(s, rsc.getClob(1));

        s.execute("DELETE FROM C8.T8POS");
        s.execute("DELETE FROM C8.T8PATT");
        ps.close();
        psp.close();

        s.execute("DROP TABLE C8.T8POS");
        s.execute("DROP TABLE C8.T8PATT");

        s.close();

        commit();
    }

    private static String T8insert(PreparedStatement ps, int id, char[] base,
            int bl, char[] pattern, int pl, int pos, boolean addPattern)
            throws SQLException
    {

        StringBuffer sb = new StringBuffer();
        sb.append(base, 0, bl);

        // Assume the pattern looks like Abcdefgh
        // put together a block of misleading matches such as
        // AAbAbcAbcdAbcde

        int last = addPatternPrefix(sb, pattern, pl, 5, 10);

        if (last >= (pos / 2))
            pos = (last + 10) * 2;

        // now a set of misleading matches up to half the pattern width
        last = addPatternPrefix(sb, pattern, pl, pl/2, pos/2);

        if (last >= pos)
            pos = last + 13;

        // now a complete set of misleading matches
        pos = addPatternPrefix(sb, pattern, pl, pl - 1, pos);

        if (addPattern) {
            // and then the pattern
            sb.insert(pos, pattern, 0, pl);
        } else {
            pos = -1;
        }


        String dd = sb.toString();
        String pstr = new String(pattern, 0, pl);

        assertEquals("FAIL - test confused pattern not at expected location",
                pos, dd.indexOf(pstr));

        // JDBC uses 1 offset for first character
        if (pos != -1)
            pos = pos + 1;

        ps.setInt(1, id);
        ps.setString(2, dd);
        ps.setInt(3, pos);
        ps.setInt(4, dd.length());
        ps.executeUpdate();

        return pstr;

    }

    private static int addPatternPrefix(
            StringBuffer sb, char[] pattern, int pl, int fakeCount, int pos) {

        for (int i = 0; i < fakeCount && i < (pl - 1); i++) {

            sb.insert(pos, pattern, 0, i + 1);
            pos += i + 1;
        }

        return pos;
    }

    private static void checkClob8(Statement s, String pstr) throws SQLException
    {

        ResultSet rs = s.executeQuery(
                "SELECT ID, DD, POS, L FROM C8.T8POS ORDER BY 1");

        while (rs.next()) {

            int id = rs.getInt(1);

            java.sql.Clob cl = rs.getClob(2);

            int pos = rs.getInt(3);
            int len = rs.getInt(4);

            long clobPosition = cl.position(pstr, 1);
            assertEquals("FAIL - position did not match",
                    (long) pos, clobPosition);
        }
        rs.close();
    }

    private static void checkClob8(Statement s, Clob pstr) throws SQLException {
        ResultSet rs = s.executeQuery(
                "SELECT ID, DD, POS, L FROM C8.T8POS ORDER BY 1");

        while (rs.next()) {

            int id = rs.getInt(1);

            java.sql.Clob cl = rs.getClob(2);

            int pos = rs.getInt(3);
            int len = rs.getInt(4);

            long clobPosition = cl.position(pstr, 1);
            assertEquals("FAIL - position did not match",
                    (long) pos, clobPosition);

        }
        rs.close();
    }

    /**
     * make sure clob is still around after we go to the next row,
     * after we close the result set, and after we close the statement
     */
    public void testClobAfterClose() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        byte[] buff = new byte[128];
        Clob[] clobArray = new Clob[10];
        int[] clobLengthArray = new int[10];
        int j = 0;
        while (rs.next()) {
            clobArray[j] = rs.getClob(1);
            clobLengthArray[j++] = rs.getInt(2);
        }
        rs.close();
        stmt.close();

        for (int i = 0; i < 10; i++) {
            if (clobArray[i] != null) {
                InputStream fin = clobArray[i].getAsciiStream();
                int columnSize = 0;
                for (;;) {
                    int size = fin.read(buff);
                    if (size == -1)
                        break;
                    columnSize += size;
                }
                assertEquals("FAIL - wrong column size",
                        columnSize, clobLengthArray[i]);
                assertEquals("FAIL - wrong column size",
                        columnSize, clobArray[i].length());
            }
        }
    }

    /**
     * test locking
     */
    public void testLockingClob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");
        // fetch row back, get the column as a clob.
        Clob clob = null, shortClob = null;
        int clobLength;
        while (rs.next()) {
            clobLength = rs.getInt(2);
            if (clobLength == 10000)
                clob = rs.getClob(1);
            if (clobLength == 26)
                shortClob = rs.getClob(1);
        }
        rs.close();
        stmt.close();

        assertTrue("shortClob is null", shortClob != null);
        assertTrue("clob is null", clob != null);

        Connection conn2 = openDefaultConnection();
        // turn off autocommit, otherwise blobs/clobs cannot hang around
        // until end of transaction
        conn2.setAutoCommit(false);
        // update should go through since we don't get any locks on clobs
        // that are not long columns
        Statement stmt2 = conn2.createStatement();
        stmt2.executeUpdate("update testClob set a = 'foo' where b = 26");
        assertEquals("FAIL: clob length changed", 26, shortClob.length());
        // should timeout waiting for the lock to do this
        try {
            stmt2.executeUpdate(
                    "update testClob set b = b + 1 where b = 10000");
            stmt2.close(); // Cleanup on fail
            conn2.rollback();
            conn2.close();
            fail("FAIL: row should be locked");
        } catch (SQLException se) {
            checkException(LOCK_TIMEOUT, se);
        }
        assertEquals("FAIL: clob length changed", 10000, clob.length());
        
        // Test that update goes through after the transaction is committed
        commit();
        stmt2.executeUpdate("update testClob set b = b + 1 where b = 10000");
        
        stmt2.close();
        conn2.rollback();
        conn2.close();
    }

    /**
     * test locking with a long row + long column
     */
    public void testLockingWithLongRowClob() throws Exception
    {
        ResultSet rs;
        Statement stmt, stmt2;
        stmt = createStatement();
        stmt.execute("alter table testClob add column al varchar(2000)");
        stmt.execute("alter table testClob add column bl varchar(3000)");
        stmt.execute("alter table testClob add column cl varchar(2000)");
        stmt.execute("alter table testClob add column dl varchar(3000)");
        stmt.execute("alter table testClob add column el CLOB(400k)");
        PreparedStatement ps = prepareStatement(
            "insert into testClob (al, bl, cl, dl, el, b) values(?,?,?,?,?,?)");
        ps.setString(1,Formatters.padString("blaaa",2000));
        ps.setString(2,Formatters.padString("tralaaaa",3000));
        ps.setString(3,Formatters.padString("foodar",2000));
        ps.setString(4,Formatters.padString("moped",3000));
        InputStream streamIn = new LoopingAlphabetStream(10000);
        ps.setAsciiStream(5, streamIn, 10000);
        ps.setInt(6, 1);
        ps.executeUpdate();
        streamIn.close();
        ps.close();
        commit();

        stmt = createStatement();
        rs = stmt.executeQuery("select el from testClob");
        // fetch row back, get the column as a clob.
        Clob clob = null;
        assertTrue("FAIL - row not found", rs.next());
        clob = rs.getClob(1);
        assertTrue("FAIL - clob is null", clob != null);
        rs.close();
        stmt.close();

        Connection conn2 = openDefaultConnection();
        // turn off autocommit, otherwise blobs/clobs cannot hang around
        // until end of transaction
        conn2.setAutoCommit(false);
        // the following should timeout
        stmt2 = conn2.createStatement();
        try {
            stmt2.executeUpdate(
                    "update testClob set el = 'smurfball' where b = 1");
            stmt2.close(); // Cleanup on fail
            conn2.rollback();
            conn2.close();
            fail("FAIL - statement should timeout");
        } catch (SQLException se) {
            checkException(LOCK_TIMEOUT, se);
        }
                
        // Test that update goes through after the transaction is committed
        commit();
        stmt2.executeUpdate("update testClob set el = 'smurfball' where b = 1");
        
        stmt2.close();
        conn2.commit();
        conn2.close();
    }

    /**
     * test accessing clob after commit
     */
    public void testClobAfterCommit() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testClob");
        // fetch row back, get the column as a clob.
        Clob clob = null, shortClob = null;
        int clobLength;
        int i = 0;
        while (rs.next()) {
            clobLength = rs.getInt(2);
            if (clobLength == 10000)
                clob = rs.getClob(1);
            if (clobLength == 26)
                shortClob = rs.getClob(1);
        }
        
        /*
        * We call it before the commit(); to cache the result
        * DERBY-3574
        */
        clob.length();
        shortClob.length();
        
        rs.close();
        stmt.close();
        commit();
        assertTrue("FAIL - shortClob is NULL", shortClob != null);
        // this should give blob/clob unavailable exceptions on client
        try {
            shortClob.length();
            //Should have thrown an SQLException in the
            fail("FAIL - should not be able to access Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }

        // these should all give blob/clob data unavailable exceptions
        try {
            clob.length();
            //Large Clobs not accessible after commit. 
            //Should have thrown an SQLException here.
            fail("FAIL - should not be able to access large Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }
        try {
            clob.getSubString(2,3);
            //Large Clobs are not accessible after commit. 
            //Should have thrown an SQLException here.
            fail("FAIL - should not be able to access large Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }
        try {
            clob.getAsciiStream();
            //Large Clobs are not accessible after commit. 
            //Should have thrown an SQLException here.
            fail("FAIL - should not be able to access large Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }
        try {
            clob.position("foo",2);
            //Large Clobs are not accessible after commit. 
            //Should have thrown an SQLException here.
            fail("FAIL - should not be able to access large Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }
        try {
            clob.position(clob,2);
            //Large Clobs are not accessible after commit. 
            //Should have thrown an SQLException here.
            fail("FAIL - should not be able to access large Clob after commit");
        } catch (SQLException e) {
            //The same SQLState String INVALID_LOB
            //is used for LOB's(Both Clob and Blob). Ensure that
            //we get the expected exception by comparing the SQLState.
            checkException(INVALID_LOB, e);
        }
    }

    /**
     * test accessing clob after closing the connection
     */
    public void testClobAfterClosingConnection() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testClob");
        // fetch row back, get the column as a clob.
        Clob clob = null, shortClob = null;
        int clobLength;
        while (rs.next()) {
            clobLength = rs.getInt(2);
            if (clobLength == 10000)
                clob = rs.getClob(1);
            if (clobLength == 26)
                shortClob = rs.getClob(1);
        }
		
        /*
         * We call it before the commit(); to cache the result
         * DERBY-3574
         */
        clob.length();
        shortClob.length();
		
        rs.close();
        stmt.close();
        commit();
        getConnection().close();

        try {
            long len = shortClob.length();
            //Clobs on the Embedded side and the NetworkClient
            //side are not accessible after closing the
            //connection. Should have thrown an SQLException here.
            fail("FAIL - should not be able to access Clob after " +
                    "Connection Close");
        }
        catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }

        // these should all give blob/clob data unavailable exceptions
        try {
            clob.length();
            //Large Clobs on the Embedded side and the NetworkClient
            //side are not accessible after Connection Close. Should
            //have thrown an SQLException here.
            fail("FAIL - should not be able to access large " +
                    "Clob after Connection Close");
        } catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            clob.getSubString(2,3);
            //Large Clobs on the Embedded side and the NetworkClient
            //side are not accessible after Connection Close. Should
            //have thrown an SQLException here.
            fail("FAIL - should not be able to access large " +
                    "Clob after Connection Close");
        } catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            clob.getAsciiStream();
            //Large Clobs on the Embedded side and the NetworkClient
            //side are not accessible after Connection Close. Should
            //have thrown an SQLException here.
            fail("FAIL - should not be able to access large " +
                    "Clob after Connection Close");
        } catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            clob.position("foo",2);
            //Large Clobs on the Embedded side and the NetworkClient
            //side are not accessible after Connection Close. Should
            //have thrown an SQLException here.
            fail("FAIL - should not be able to access large " +
                    "Clob after Connection Close");
        } catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            clob.position(clob,2);
            //Large Clobs on the Embedded side and the NetworkClient
            //side are not accessible after Connection Close. Should
            //have thrown an SQLException here.
            fail("FAIL - should not be able to access large " +
                    "Clob after Connection Close");
        } catch (SQLException e) {
            //Ensure that we get the expected exception by comparing
            //the SQLState.
            checkException(NO_CURRENT_CONNECTION, e);
        }
    }

    /**
     * Make sure we get an error attempting to access the 
     * lob after commit.
     */
    public void testClobAfterCommitWithSecondClob() throws SQLException
    {
        getConnection().setAutoCommit(false);
        Statement s1 = createStatement();
        ResultSet rs1 = s1.executeQuery("values cast('first' as clob)");
        rs1.next();
        Clob first = rs1.getClob(1);
        rs1.close(); 
        commit();
        Statement s2 = createStatement();
        ResultSet rs2 = s2.executeQuery("values cast('second' as clob)");
        rs2.next();
        Clob second = rs2.getClob(1);
        try {
            first.getSubString(1,100);
            fail("first.getSubString should have failed because after the commit");
        } catch (SQLException se){
            assertSQLState(INVALID_LOB,se);
        }
        assertEquals("second",second.getSubString(1, 100));        
        rs2.close(); 
    }
    /**
     * Test fix for derby-1382.
     *
     * Test that the getClob() returns the correct value for the clob before and
     * after updating the clob when using result sets of type
     * TYPE_SCROLL_INSENSITIVE.
     *
     * @throws SQLException
     */
    public void testGetClobBeforeAndAfterUpdate() throws SQLException {
        String clobData = "initial clob ";
        PreparedStatement ps = prepareStatement("insert into " +
                "testClob (b, a) values (?, ?)");
        for (int i=0; i<10; i++) {
            ps.setInt(1, i);
            ps.setString(2, clobData + i);
            ps.execute();
        }
        ps.close();

        Statement scrollStmt = createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = scrollStmt.executeQuery("SELECT b, a FROM testClob");

        String value;
        Clob c;

        rs.first();
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.next();
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.relative(3);
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.absolute(7);
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.previous();
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.last();
        checkContentsBeforeAndAfterUpdatingClob(rs);
        rs.previous();
        checkContentsBeforeAndAfterUpdatingClob(rs);

        rs.close();
        scrollStmt.close();
    }

    private void checkContentsBeforeAndAfterUpdatingClob(ResultSet rs)
    throws SQLException {
        Clob c;
        String value, expectedValue;
        String clobData = "initial clob ";
        String updatedClobData = "updated clob ";

        c = rs.getClob(2);
        // check contents
        value = c.getSubString(1, (int)c.length());
        expectedValue = clobData + rs.getInt(1);
        assertEquals("FAIL - wrong clob value", expectedValue, value);
        // update contents
        value = updatedClobData + rs.getInt(1);
        c.setString(1, value);
        rs.updateClob(2, c);
        rs.updateRow();
        // check update values
        rs.next(); // leave the row
        rs.previous(); // go back to updated row
        c = rs.getClob(2);
        // check contents
        value = c.getSubString(1, (int)c.length());
        expectedValue = updatedClobData + rs.getInt(1);
        assertEquals("FAIL - wrong clob value", expectedValue, value);
    }

    /**
     * Test fix for derby-1421.
     *
     * Test that the getClob() returns the correct value for the blob before and
     * after updating the Clob using the method updateCharacterStream().
     *
     * @throws SQLException
     */
    public void testGetClobBeforeAndAfterUpdateStream() throws SQLException {
        String clobData = "initial clob ";
        PreparedStatement ps = prepareStatement("insert into " +
                "testClob (b, a) values (?, ?)");
        for (int i=0; i<10; i++) {
            ps.setInt(1, i);
            ps.setString(2, clobData + i);
            ps.execute();
        }
        ps.close();

        Statement scrollStmt = createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = scrollStmt.executeQuery("SELECT b, a FROM testClob");

        rs.first();
        updateClobWithUpdateCharacterStream(rs);
        rs.next();
        updateClobWithUpdateCharacterStream(rs);
        rs.relative(3);
        updateClobWithUpdateCharacterStream(rs);
        rs.absolute(7);
        updateClobWithUpdateCharacterStream(rs);
        rs.previous();
        updateClobWithUpdateCharacterStream(rs);
        rs.last();
        updateClobWithUpdateCharacterStream(rs);
        rs.previous();
        updateClobWithUpdateCharacterStream(rs);

        rs.close();
        scrollStmt.close();
    }

    private void updateClobWithUpdateCharacterStream(ResultSet rs)
    throws SQLException {
        Clob c;
        String value, expectedValue;
        String clobData = "initial clob ";
        String updatedClobData = "updated clob ";

        c = rs.getClob(2);
        // check contents
        value = c.getSubString(1, (int)c.length());
        expectedValue = clobData + rs.getInt(1);
        assertEquals("FAIL - wrong clob value", expectedValue, value);

        // update contents
        value = (updatedClobData + rs.getInt(1));
        Reader updateValue = new StringReader(value);
        rs.updateCharacterStream(2, updateValue, value.length());
        rs.updateRow();
        // check update values
        rs.next(); // leave the row
        rs.previous(); // go back to updated row
        c = rs.getClob(2);
        // check contents
        value = c.getSubString(1, (int)c.length());
        expectedValue = updatedClobData + rs.getInt(1);
        assertEquals("FAIL - wrong clob value", expectedValue, value);
    }

    /**
     * test clob finalizer closes the container (should only release table and
     * row locks that are read_committed)
     * NOTE: this test does not produce output since it needs to call the
     * garbage collector whose behaviour is unreliable. It is in the test run to
     * exercise the code (most of the time).
     */
    public void testClobFinalizer() throws Exception {
        insertDefaultData();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testClob");

        Clob[] clobArray = new Clob[10];
        int[] clobLengthArray = new int[10];
        int j = 0;
        while (rs.next()) {
            clobArray[j] = rs.getClob(1);
            clobLengthArray[j++] = rs.getInt(2);
        }
        rs.close();
        stmt.close();

        for (int i = 0; i < 10; i++) {
            clobArray[i] = null;
        }

        System.gc();
        System.gc();
    }


    /**
     * basic test of getBinaryStream also tests length
     */
    public void testGetBinaryStream() throws Exception {
        insertDefaultData();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b, crc32 from testBlob");
        testBlobContents(rs);
        stmt.close();
        commit();
    }

    /**
     * test getBytes
     */
    public void testGetBytes() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");
        int blobLength = 0;
        Blob blob;
        while (rs.next()) {
            blob = rs.getBlob(1);
            blobLength = rs.getInt(2);
            if (blob != null) {
                verifyInterval(blob, 9905, 50, 0, blobLength);
                verifyInterval(blob, 5910, 150, 1, blobLength);
                verifyInterval(blob, 5910, 50, 2, blobLength);
                verifyInterval(blob, 204, 50, 3, blobLength);
                verifyInterval(blob, 68, 50, 4, blobLength);
                verifyInterval(blob, 1, 50, 5, blobLength);
                verifyInterval(blob, 1, 1, 6, blobLength);
                verifyInterval(blob, 1, 0, 7, blobLength);
                verifyInterval(blob, blobLength + 1, 0, 8, blobLength);
                if (blobLength > 100) {
                    byte[] res = blob.getBytes(blobLength-99,200);
                    assertEquals("FAIL - wrong length in bytes",
                            100, res.length);
                    // Get expected value
                    InputStream inStream = blob.getBinaryStream();
                    long left = blobLength - 100;
                    long read = 0;
                    while (left > 0 && read != -1) {
                        read = inStream.skip(Math.min(1024, left));
                        left -= read > 0? read : 0;
                    }
                    byte[] expected = new byte[100];
                    read = inStream.read(expected);
                    inStream.close();
                    assertEquals("FAIL - wrong value",
                            new String(expected),new String(res));
                }
            } else {
                assertTrue("FAIL - blob was NULL but length != 0",
                        blobLength == 0);
            }
        }
        stmt.close();
        commit();
    }

    /**
     * test position with a byte[] argument
     */
    public void testPositionBytes() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testBlob");
        int blobLength = 0;
        Blob blob;
        Random random = new Random();
        byte[] searchBytes;
        int start, length, startSearchPos;
        int distance, maxStartPointDistance;
        long foundAt;
        // clobs are generated with looping alphabet streams
        maxStartPointDistance = CharAlphabet.MODERNLATINLOWER.length;
        while (rs.next()) {
            blob = rs.getBlob(1);
            blobLength = rs.getInt(2);
            if (blob != null && blobLength > 0) {
                println("\n\nblobLength: " + blobLength);
                for (int i=0; i<10; i++) {
                    // find a random string to search for
                    start = Math.max(random.nextInt(blobLength - 1), 1);
                    length = random.nextInt(blobLength - start) + 1;
                    println("start:" + start + " length:" + length);
                    searchBytes = blob.getBytes(start, length);
                    String searchString = new String(searchBytes);
                    // get random position to start the search from
                    distance = random.nextInt(maxStartPointDistance);
                    startSearchPos = Math.max((start - distance), 1);
                    // make sure that the searched string does not happen
                    // before the expected position
                    byte[] tmp = blob.getBytes(startSearchPos, start);
                    if (new String(tmp).indexOf(searchString) != -1)
                    {
                        startSearchPos = start;
                    }
                    println("startSearchPos: " + startSearchPos +
                            "searchString: " + new String(searchBytes));
                    foundAt = blob.position(searchBytes, startSearchPos);
                    assertEquals("FAIL - wrong match found for " +
                            searchString + " starting at " + startSearchPos +
                            " and length of " + searchBytes.length,
                            start, foundAt);

                }
            }
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests the {@code Blob.position} using a deterministic sequence of
     * actions and arguments.
     */
    public void testPositionBlobDeterministic()
            throws IOException, SQLException {
        getConnection().setAutoCommit(false);
        final int size = 100000;
        PreparedStatement ps = prepareStatement(
                "INSERT INTO testBlob (a, b) VALUES (?, ?)");
        ps.setBinaryStream(1, new LoopingAlphabetStream(size), size);
        ps.setInt(2, size);
        ps.executeUpdate();
        ps.close();
        ps = prepareStatement("select a from testBlob where b = ?");
        ps.setInt(1, size);
        ResultSet rs = ps.executeQuery();
        assertTrue("No data found", rs.next());
        Blob blob = rs.getBlob(1);
        // Try with a one-byte pattern.
        byte[] pattern = new byte[] {(byte)'k'}; // k number 11 in the alphabet
        assertEquals(11, blob.position(pattern, 1));
        // Try with a non-existing pattern.
        pattern = new byte[] {(byte)'p', (byte)'o'};
        assertEquals(-1, blob.position(pattern, size / 3));

        // Loop through all matches 
        pattern = new byte[] {(byte)'d', (byte)'e'};
        long foundAtPos = 1;
        int index = 0;
        int stepSize = ByteAlphabet.modernLatinLowercase().byteCount();
        while ((foundAtPos = blob.position(pattern, foundAtPos +1)) != -1) {
            assertEquals((stepSize * index++) + 4, foundAtPos);
            byte[] fetchedPattern = blob.getBytes(foundAtPos, pattern.length);
            assertTrue(Arrays.equals(pattern, fetchedPattern));
        }

        // Try a longer pattern.
        int pSize = 65*1024; // 65 KB
        pattern = new byte[pSize];
        assertEquals(pSize, new LoopingAlphabetStream(pSize).read(pattern));
        assertEquals(1, blob.position(pattern, 1));
        assertEquals(stepSize * 100 +1,
                blob.position(pattern, stepSize * 99 + 7));
        // Try again after getting the length.
        assertEquals(size, blob.length());
        assertEquals(stepSize * 100 +1,
                blob.position(pattern, stepSize * 99 + 7));

        // Try specifing a starting position that's too big.
        try {
            blob.position(pattern, size*2);
            fail("Accepted position after end of Blob");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }

        // Fetch the last 5 bytes, try with a partial match at the end.
        byte[] blobEnd = blob.getBytes(size - 4, 5);
        pattern = new byte[6];
        System.arraycopy(blobEnd, 0, pattern, 0, blobEnd.length);
        pattern[5] = 'X'; // Only lowercase in the looping alphabet stream.
        assertEquals(-1, blob.position(pattern, size - 10));

        // Get the very last byte, try with a partial match at the end.
        blobEnd = blob.getBytes(size, 1);
        pattern = new byte[] {blobEnd[0], 'X'};
        assertEquals(-1, blob.position(pattern, size - 5));
    }

    /**
     * Test Blob.position() with blob argument
     */
    public void testPositionBlob() throws Exception {
        insertDefaultData();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testBlob");
        int blobLength = 0;
        Blob blob;
        Statement stmt2 = createStatement();
        Random random = new Random();
        String searchString;
        int start, length, startSearchPos;
        int distance, maxStartPointDistance;
        long foundAt;
        // clobs are generated with looping alphabet streams
        maxStartPointDistance = CharAlphabet.MODERNLATINLOWER.length;
        while (rs.next()) {
            blob = rs.getBlob(1);
            blobLength = rs.getInt(2);
            if (blob != null && blobLength > 0) {
                println("\n\nblobLength: " + blobLength);
                // Create a table with clobs to search
                stmt2.executeUpdate("CREATE TABLE searchBlob " +
                        "(a Blob(300K), start int, position int)");
                // insert clobs into the table
                PreparedStatement ps = prepareStatement(
                        "INSERT INTO searchBlob values (?, ?, ?) ");
                for (int i=0; i<10; i++) {
                    // find a random string to search for
                    start = Math.max(random.nextInt(blobLength - 1), 1);
                    length = random.nextInt(blobLength - start) + 1;
                    println("start:" + start + " length:" + length);
                    searchString = new String(blob.getBytes(start, length));
                    // get random position to start the search from
                    distance = random.nextInt(maxStartPointDistance);
                    startSearchPos = Math.max((start - distance), 1);
                    // make sure that the searched string does not happen
                    // before the expected position
                    String tmp = new String(
                            blob.getBytes(startSearchPos, start));
                    if (tmp.indexOf(searchString) != -1) {
                        startSearchPos = start;
                    }

                    ps.setBytes(1, searchString.getBytes());
                    ps.setInt(2, startSearchPos);
                    ps.setInt(3, start);
                    ps.executeUpdate();
                }

                ps.close();

                ResultSet rs2 = stmt2.executeQuery(
                        "SELECT a, start, position FROM searchBlob");
                while (rs2.next()) {
                    Blob searchBlob = rs2.getBlob(1);
                    startSearchPos = rs2.getInt(2);
                    start = rs2.getInt(3);

                    searchString = new String(
                            searchBlob.getBytes(1L, (int)searchBlob.length()));
                    println("startSearchPos: " + startSearchPos +
                            "searchString: " + searchString);
                    foundAt = blob.position(searchBlob, startSearchPos);
                    assertEquals("FAIL - wrong match found for " +
                            searchString + " starting at " + startSearchPos +
                            " and length of " + searchString.length(),
                            start, foundAt);
                }
                rs2.close();
                stmt2.executeUpdate("DROP TABLE searchBlob");
            }
        }
        rs.close();
        stmt.close();
        stmt2.close();
    }

    /**
     * Test triggers on BLOB columns.
     */
    public void testTriggerWithBlobColumn() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE blobTest8TriggerA " +
                "(a BLOB(400k), b int, crc32 BIGINT)");
        stmt.executeUpdate("CREATE TABLE blobTest8TriggerB " +
                "(a BLOB(400k), b int, crc32 BIGINT)");
        stmt.executeUpdate(
                "create trigger T8A after update on testBlob " +
                "referencing new as n old as o " +
                "for each row "+
                "insert into blobTest8TriggerA(a, b, crc32) " +
                "values (n.a, n.b, n.crc32)");
        stmt.executeUpdate(
                "create trigger T8B after INSERT on blobTest8TriggerA " +
                "referencing new table as n " +
                "for each statement "+
                "insert into blobTest8TriggerB(a, b, crc32) " +
                "select n.a, n.b, n.crc32 from n");

        commit();
        ResultSet rs = stmt.executeQuery(
                "select a,b,crc32 from blobTest8TriggerA");
        assertFalse("FAIL - Table blobTest8TriggerA should contain no rows",
                rs.next());
        rs.close();
        commit();
        stmt.executeUpdate("UPDATE testBlob set b = b + 0");
        commit();
        rs = stmt.executeQuery(
                "select a,b,crc32 from blobTest8TriggerA");
        testBlobContents(rs);
        rs.close();
        commit();

        rs = stmt.executeQuery(
                "select a,b,crc32 from blobTest8TriggerB");
        testBlobContents(rs);
        rs.close();
        commit();
        stmt.executeUpdate("DROP TRIGGER T8A");
        stmt.executeUpdate("DROP TRIGGER T8B");
        stmt.executeUpdate("DROP TABLE blobTest8TriggerB");
        stmt.executeUpdate("DROP TABLE blobTest8TriggerA");

        stmt.close();
        commit();

    }

    /**
     * tests small blob abd length method
     */
    public void testVarbinary() throws Exception{

        Statement stmt = createStatement();
        stmt.execute("ALTER TABLE testBlob ADD COLUMN smallBlob blob(13)");

        PreparedStatement ps = prepareStatement(
                "insert into testBlob (smallBlob) values (?)");
        String val = "";

        for (int i = 0; i < 10; i++) {
            // insert a string
            ps.setBytes(1, val.getBytes("US-ASCII"));
            ps.executeUpdate();
            val = val.trim() + "x";
        }

        ResultSet rs = stmt.executeQuery("select smallBlob from testBlob");
        byte[] buff = new byte[128];
        int j = 0;
        // fetch all rows back, get the columns as clobs.
        while (rs.next()) {
            // get the first column as a clob
            Blob blob = rs.getBlob(1);
            assertTrue("FAIL - blob is null", blob != null);
            InputStream fin = blob.getBinaryStream();
            int columnSize = 0;
            for (;;) {
                int size = fin.read(buff);
                if (size == -1)
                    break;
                columnSize += size;
            }
            assertEquals("FAIL - unexpected blob size", j, columnSize);
            assertEquals("FAIL - unexpected blob length", j, blob.length());
            j++;
        }
        ps.close();
        stmt.close();
        commit();
    }

    /**
     * make sure cannot get a blob from an int column
     */
    public void testGetBlobFromIntColumn() throws Exception {

        insertDefaultData();
        Statement stmt = createStatement();

        ResultSet rs = stmt.executeQuery("select b from testClob");
        while (rs.next()) {
            // get the first column as a clob
            try {
                Blob blob = rs.getBlob(1);
                rs.close();
                stmt.close();
                fail("FAIL - getBlob on int column should throw an " +
                        "exception");
            } catch (SQLException se) {
                checkException(LANG_DATA_TYPE_GET_MISMATCH, se);
            }
        }
        rs.close();
        stmt.close();
        commit();

    }

    /**
     * make sure setBlob doesn't work for an int column
     */
    public void testSetBlobOnIntColumn() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        PreparedStatement ps = prepareStatement(
                "insert into testBlob (b) values(?)");
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");
        Blob blob;
        int blobLength;
        while (rs.next()) {
            // get the first column as a blob
            blob = rs.getBlob(1);
            if (blob != null) {
                try {
                    ps.setBlob(1,blob);
                    ps.executeUpdate();
                    rs.close();
                    stmt.close();
                    ps.close();
                    fail("FAIL - setBlob worked on INT column");
                } catch (SQLException se) {
                    checkException(LANG_DATA_TYPE_GET_MISMATCH, se);
                }
            }
        }
        rs.close();
        stmt.close();
        ps.close();
        commit();
    }

    /**
     * test raising of exceptions
     */
    public void testRaisingOfExceptionsBlob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");

        int blobLength = 0;
        Blob blob;
        while (rs.next()) {
            blob = rs.getBlob(1);
            blobLength = rs.getInt(2);
            if (blob != null) {

                // 0 or negative position value
                try {
                    blob.getBytes(0, 5);
                    fail("FAIL - getBytes with 0 as position should " +
                            "have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_BAD_POSITION, e);
                }
                // negative length value
                try {
                    blob.getBytes(1, -76);
                    fail("FAIL - getBytes with negative length should " +
                            "have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_NONPOSITIVE_LENGTH, e);
                }
                // zero length value
                try {
                    blob.getBytes(1, -1);
                    fail("FAIL - getBytes with negative length should " +
                            "have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_NONPOSITIVE_LENGTH, e);
                }
                // before begin length 0
                try {
                    blob.getBytes(0, 0);
                    fail("FAIL - getBytes with 0 position and length " +
                            "should have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_BAD_POSITION, e);
                }
                // after end length 0
                try {
                    blob.getBytes(blobLength + 2, 0);
                    fail("FAIL - getBytes with position larger than " +
                            "the length of the blob should have caused an " +
                            "exception");
                } catch (SQLException e) {
                    checkException(BLOB_POSITION_TOO_LARGE, e);
                } catch (NegativeArraySizeException nase) {
                    assertTrue("FAIL - this exception should only happen " +
                            "with DB2 client", usingDB2Client());
                }
                // 0 or negative position value
                try {
                    blob.position(new byte[0], -4000);
                    if (!usingDB2Client()) {
                        fail("FAIL - position with negative start " +
                                "position should have caused an exception");
                    }
                } catch (SQLException e) {
                    checkException(BLOB_BAD_POSITION, e);
                    assertTrue("FAIL - JCC should not get an exception",
                            !usingDB2Client());
                }
                // null pattern
                try {
                    blob.position((byte[]) null, 5);
                    fail("FAIL - position with null pattern should " +
                            "have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_NULL_PATTERN_OR_SEARCH_STR, e);
                }
                // 0 or negative position value
                try {
                    blob.position(blob, -42);
                    if (!usingDB2Client()) {
                        fail("FAIL - position with negative start " +
                                "position should have caused an exception");
                    }
                } catch (SQLException e) {
                    checkException(BLOB_BAD_POSITION, e);
                } catch (ArrayIndexOutOfBoundsException aob) {
                    assertTrue("FAIL - this excpetion should only happen " +
                            "with DB2 client", usingDB2Client());
                }
                // null pattern
                try {
                    blob.position((Blob) null, 5);
                    fail("FAIL - position with null pattern should " +
                            "have caused an exception");
                } catch (SQLException e) {
                    checkException(BLOB_NULL_PATTERN_OR_SEARCH_STR, e);
                }
            }
        }
        rs.close();
        stmt.close();
        commit();
    }

    /**
     * test setBlob
     */
    public void testSetBlob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        stmt.execute("create table testBlobX (a blob(300K), b integer)");
        PreparedStatement ps = prepareStatement(
                "insert into testBlobX values(?,?)");
        ResultSet rs = stmt.executeQuery("select a, b from testBlob");
        Blob blob;
        int blobLength;
        while (rs.next()) {
            // get the first column as a blob
            blob = rs.getBlob(1);
            blobLength = rs.getInt(2);
            if (blob != null) {
                ps.setBlob(1,blob);
                ps.setInt(2,blobLength);
                ps.executeUpdate();
            }
        }
        rs.close();
        commit();

        rs = stmt.executeQuery("select a,b from testBlobX");
        Blob blob2;
        int blobLength2;
        while (rs.next()) {
            // get the first column as a blob
            blob2 = rs.getBlob(1);
            blobLength2 = rs.getInt(2);
            assertTrue("FAIL - blob is NULL", blob2 != null);
            assertEquals("FAIL - wrong blob length",
                    blob2.length(), blobLength2);
        }
        rs.close();

        stmt.executeUpdate("DROP TABLE testBlobX");
        stmt.close();
        commit();
    }

    /**
     * make sure blob is still around after we go to the next row,
     * after we close the result set, and after we close the statement
     */
    public void testBlobAfterClose() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");
        byte[] buff = new byte[128];
        Blob[] blobArray = new Blob[10];
        int[] blobLengthArray = new int[10];
        int j = 0;
        while (rs.next()) {
            blobArray[j] = rs.getBlob(1);
            blobLengthArray[j++] = rs.getInt(2);
        }
        rs.close();
        stmt.close();

        for (int i = 0; i < 10; i++) {
            if (blobArray[i] != null) {
                InputStream fin = blobArray[i].getBinaryStream();
                int columnSize = 0;
                for (;;) {
                    int size = fin.read(buff);
                    if (size == -1)
                        break;
                    columnSize += size;
                }
                assertEquals("FAIL - invalid length",
                        blobLengthArray[i], columnSize);
                assertEquals("FAIL - invalid length",
                        columnSize, blobArray[i].length());
            }
        }
    }

    /**
     * test locking
     */
    public void testLockingBlob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");
        // fetch row back, get the column as a blob.
        Blob blob = null, shortBlob = null;
        int blobLength;
        while (rs.next()) {
            blobLength = rs.getInt(2);
            if (blobLength == 10000)
                blob = rs.getBlob(1);
            if (blobLength == 26)
                shortBlob = rs.getBlob(1);
        }
        rs.close();

        Connection conn2 = openDefaultConnection();
        // turn off autocommit, otherwise blobs/clobs cannot hang around
        // until end of transaction
        conn2.setAutoCommit(false);
        // Update should go through since we don't get any locks on blobs
        // that are not long columns
        Statement stmt2 = conn2.createStatement();
        stmt2.executeUpdate("update testBlob set a = null where b = 26");
        assertEquals("FAIL - blob was updated", 26, shortBlob.length());

        // should timeout waiting for the lock to do this
        try {
            
            stmt2.executeUpdate(
                    "update testBlob set b = b + 1 where b = 10000");
            stmt.close();
            stmt2.close();
            conn2.rollback();
            conn2.close();
            fail("FAIL - should have gotten lock timeout");
        } catch (SQLException se) {
            checkException(LOCK_TIMEOUT, se);
        }
        
        // Test that update goes through after the transaction is committed
        commit();
        stmt2.executeUpdate("update testBlob set b = b + 1 where b = 10000");
        
        stmt.close();
        stmt2.close();
        conn2.commit();
        conn2.close();
    }

    /**
     * test locking with a long row + long column
     */
    public void testLockingWithLongRowBlob() throws Exception
    {
        ResultSet rs;
        Statement stmt, stmt2;
        stmt = createStatement();
        stmt.execute("alter table testBlob add column al varchar(2000)");
        stmt.execute("alter table testBlob add column bl varchar(3000)");
        stmt.execute("alter table testBlob add column cl varchar(2000)");
        stmt.execute("alter table testBlob add column dl varchar(3000)");
        stmt.execute("alter table testBlob add column el BLOB(400k)");
        PreparedStatement ps = prepareStatement(
            "insert into testBlob (al, bl, cl, dl, el, b) values(?,?,?,?,?,?)");
        ps.setString(1,Formatters.padString("blaaa",2000));
        ps.setString(2,Formatters.padString("tralaaaa",3000));
        ps.setString(3,Formatters.padString("foodar",2000));
        ps.setString(4,Formatters.padString("moped",3000));
        InputStream streamIn = new LoopingAlphabetStream(10000);
        ps.setBinaryStream(5, streamIn, 10000);
        ps.setInt(6, 1);
        ps.executeUpdate();
        streamIn.close();
        ps.close();
        commit();

        stmt = createStatement();
        rs = stmt.executeQuery("select el from testBlob");
        // fetch row back, get the column as a clob.
        Blob blob = null;
        assertTrue("FAIL - row not found", rs.next());
        blob = rs.getBlob(1);
        assertTrue("FAIL - blob is null", blob != null);
        rs.close();
        stmt.close();

        Connection conn2 = openDefaultConnection();
        // turn off autocommit, otherwise blobs/clobs cannot hang around
        // until end of transaction
        conn2.setAutoCommit(false);
        // the following should timeout
        stmt2 = conn2.createStatement();
        try {
            stmt2.executeUpdate("update testBlob set el = null where b = 1");
            stmt2.close();
            stmt.close();
            conn2.rollback();
            conn2.close();
            fail("FAIL - statement should timeout");
        } catch (SQLException se) {
            checkException(LOCK_TIMEOUT, se);
        }
        // Test that update goes through after the transaction is committed
        commit();
        stmt2.executeUpdate("update testBlob set el = null where b = 1");
        
        stmt2.close();
        conn2.commit();
        stmt.close();
        conn2.close();
    }

    /**
     * test accessing blob after commit
     */
    public void testBlobAfterCommit() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testBlob");
        // fetch row back, get the column as a blob.
        Blob blob = null, shortBlob = null;
        int blobLength;
        while (rs.next()) {
            blobLength = rs.getInt(2);
            if (blobLength == 10000)
                blob = rs.getBlob(1);
            if (blobLength == 26)
                shortBlob = rs.getBlob(1);
        }
		
        /*
         * We call it before the commit(); to cache the result
         * DERBY-3574
         */
        blob.length();
        shortBlob.length();
		
        rs.close();
        stmt.close();
        commit();


        assertTrue("FAIL - shortBlob is NULL", shortBlob != null);
        // This should give blob/clob unavailable exceptions with both
        // client and embedded driver.
        try {
            shortBlob.length();
            fail("FAIL - should not be able to access Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }

        assertTrue("FAIL - blob is NULL", blob != null);
        // these should all give blob/clob data unavailable exceptions
        try {
            blob.length();
            fail("FAIL - should not be able to access large Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }
        try {
            blob.getBytes(2,3);
            fail("FAIL - should not be able to access large Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }
        try {
            blob.getBinaryStream();
            fail("FAIL - should not be able to access large Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }
        try {
            blob.position("foo".getBytes("US-ASCII"),2);
            fail("FAIL - should not be able to access large Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }
        try {
            blob.position(blob,2);
            fail("FAIL - should not be able to access large Blob after commit");
        } catch (SQLException e) {
            checkException(INVALID_LOB, e);
        }
    }

    /**
     * test accessing blob after closing the connection
     */
    public void testBlobAfterClosingConnection() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a, b from testBlob");
        // fetch row back, get the column as a blob.
        Blob blob = null, shortBlob = null;
        int blobLength;
        while (rs.next()) {
            blobLength = rs.getInt(2);
            if (blobLength == 10000)
                blob = rs.getBlob(1);
            if (blobLength == 26)
                shortBlob = rs.getBlob(1);
        }
		
        /*
         * We call it before the commit(); to cache the result
         * DERBY-3574
         */
        blob.length();
        shortBlob.length();
		
        rs.close();
        rollback();
        getConnection().close();

        try {
            long length = shortBlob.length();                        
            fail("FAIL - should get an exception, connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);            
        }

        // these should all give blob/clob data unavailable exceptions
        try {
            blob.length();
            fail("FAIL - should not be able to access large lob " +
                    "after the connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            blob.getBytes(2,3);
            fail("FAIL - should not be able to access large lob " +
                    "after the connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            blob.getBinaryStream();
            fail("FAIL - should not be able to access large lob " +
                    "after the connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            blob.position("foo".getBytes("US-ASCII"),2);
            fail("FAIL - should not be able to access large lob " +
                    "after the connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);
        }
        try {
            blob.position(blob,2);
            fail("FAIL - should not be able to access large lob " +
                    "after the connection is closed");
        } catch (SQLException e) {
            checkException(NO_CURRENT_CONNECTION, e);
        }

        // restart the connection
        getConnection().setAutoCommit(false);
    }

    /**
     * test blob finalizer closes the container
     * (should only release table and row locks that are read_committed)
     * NOTE: this test does not produce output since it needs to call the
     * garbage collector whose behaviour is unreliable. It is in the test run to
     * exercise the code (most of the time).
     */
    public void testBlobFinalizer() throws Exception {
        insertDefaultData();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select a,b from testBlob");

        Blob[] blobArray = new Blob[10];
        int[] blobLengthArray = new int[10];
        int j = 0;
        while (rs.next()) {
            blobArray[j] = rs.getBlob(1);
            blobLengthArray[j++] = rs.getInt(2);
        }
        rs.close();
        stmt.close();

        for (int i = 0; i < 10; i++) {
            blobArray[i] = null;
        }

        System.gc();
        System.gc();

    }

    /**
     * Test fix for derby-1382.
     *
     * Test that the getBlob() returns the correct value for the blob before and
     * after updating the blob when using result sets of type
     * TYPE_SCROLL_INSENSITIVE.
     *
     * @throws Exception
     */
    public void testGetBlobBeforeAndAfterUpdate() throws Exception {
        String blobData = "initial blob ";
        PreparedStatement ps =
                prepareStatement(
                "insert into testBlob (b, a) values (?, ?)");
        for (int i=0; i<10; i++) {
            ps.setInt(1, i);
            ps.setBytes(2, (blobData + i).getBytes());
            ps.execute();
        }
        ps.close();

        Statement scrollStmt = createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = scrollStmt.executeQuery("SELECT b, a FROM testBlob");

        rs.first();
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.next();
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.relative(3);
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.absolute(7);
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.previous();
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.last();
        checkContentsBeforeAndAfterUpdatingBlob(rs);
        rs.previous();
        checkContentsBeforeAndAfterUpdatingBlob(rs);

        rs.close();
        scrollStmt.close();

    }

    private void checkContentsBeforeAndAfterUpdatingBlob(ResultSet rs)
    throws SQLException {
        Blob b;
        byte[] value, expectedValue;
        String blobData = "initial blob ";
        String updatedBlobData = "updated blob ";

        b = rs.getBlob(2);
        // check contents
        value = b.getBytes(1, blobData.length() + 1);
        expectedValue = (blobData + rs.getInt(1)).getBytes();
        assertTrue("FAIL - wrong blob value",
                Arrays.equals(value, expectedValue));

        // update contents
        value = (updatedBlobData + rs.getInt(1)).getBytes();
        b.setBytes(1, value);
        rs.updateBlob(2, b);
        rs.updateRow();
        // check update values
        rs.next(); // leave the row
        rs.previous(); // go back to updated row
        b = rs.getBlob(2);
        // check contents
        value = b.getBytes(1, updatedBlobData.length() + 1);
        expectedValue = (updatedBlobData + rs.getInt(1)).getBytes();
        assertTrue("FAIL - wrong blob value",
                Arrays.equals(value, expectedValue));
    }

    /**
     * Test fix for derby-1421.
     *
     * Test that the getBlob() returns the correct value for the blob before and
     * after updating the blob using the method updateBinaryStream().
     *
     * @throws Exception
     */
    public void testGetBlobBeforeAndAfterUpdateStream() throws Exception {

        String blobData = "initial blob ";
        PreparedStatement ps =
                prepareStatement(
                "insert into testBlob (b, a) values (?, ?)");
        for (int i=0; i<10; i++) {
            ps.setInt(1, i);
            ps.setBytes(2, (blobData + i).getBytes());
            ps.execute();
        }
        ps.close();

        Statement scrollStmt = createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = scrollStmt.executeQuery("SELECT b, a FROM testBlob");

        rs.first();
        updateBlobWithUpdateBinaryStream(rs);
        rs.next();
        updateBlobWithUpdateBinaryStream(rs);
        rs.relative(3);
        updateBlobWithUpdateBinaryStream(rs);
        rs.absolute(7);
        updateBlobWithUpdateBinaryStream(rs);
        rs.previous();
        updateBlobWithUpdateBinaryStream(rs);
        rs.last();
        updateBlobWithUpdateBinaryStream(rs);
        rs.previous();
        updateBlobWithUpdateBinaryStream(rs);

        rs.close();
        scrollStmt.close();
    }

    private void updateBlobWithUpdateBinaryStream(ResultSet rs)
    throws SQLException {
        Blob b;
        byte[] value, expectedValue;
        String blobData = "initial blob ";
        String updatedBlobData = "updated blob ";

        b = rs.getBlob(2);
        // check contents
        value = b.getBytes(1, blobData.length() + 1);
        expectedValue = (blobData + rs.getInt(1)).getBytes();
        assertTrue("FAIL - wrong blob value",
                Arrays.equals(value, expectedValue));

        // update contents
        value = (updatedBlobData + rs.getInt(1)).getBytes();
        InputStream updateValue = new ByteArrayInputStream(value);
        rs.updateBinaryStream(2, updateValue, value.length);
        rs.updateRow();
        // check update values
        rs.next(); // leave the row
        rs.previous(); // go back to updated row
        b = rs.getBlob(2);
        // check contents
        value = b.getBytes(1, updatedBlobData.length() + 1);
        expectedValue = (updatedBlobData + rs.getInt(1)).getBytes();
        assertTrue("FAIL - wrong blob value",
                Arrays.equals(value, expectedValue));
    }

    /**
     * test behaviour of system with self destructive user
     * update a long column underneath a clob
     */
    public void testSelfDestructiveClob() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select a, b from testClob where b = 10000");
        byte[] buff = new byte[128];
        // fetch row back, get the column as a clob.
        Clob clob = null;
        InputStream fin;
        int clobLength = 0, i = 0;
        assertTrue("FAIL - row not found", rs.next());
        i++;
        clobLength = rs.getInt(2);
        // get the first column as a clob
        clob = rs.getClob(1);
        assertEquals("FAIL - wrong clob length", 10000, clobLength);
        fin = clob.getAsciiStream();
        int columnSize = 0;

        PreparedStatement ps = prepareStatement(
                "update testClob set a = ? where b = 10000");
        StringBuffer foo = new StringBuffer();
        for (int k = 0; k < 1000; k++)
            foo.append('j');
        ps.setString(1,foo.toString());
        ps.executeUpdate();

        rs = stmt.executeQuery("select a from testClob where b = 10000");
        while (rs.next()) {
            int j = 1;
            String val = rs.getString(1);
            assertEquals("FAIL - invalid blob value", foo.substring(0, 50),
                    val.substring(0,50));
            j++;
        }

        while (columnSize < 11000) {
            int size = fin.read(buff);
            if (size == -1)
                break;
            columnSize += size;
        }
        assertEquals("FAIL - invalid column size", 10000, columnSize);

        assertEquals("FAIL - invalid column size", clobLength, columnSize);
        assertEquals("FAIL - invalid column size", columnSize, clob.length());

        rs.close();
        stmt.close();
    }

    /**
     * test behaviour of system with self destructive user
     * drop table and see what happens to the clob
     * expect an IOException when moving to a new page of the long column
     */
    public void testSelfDestructiveClob2() throws Exception {
        insertDefaultData();

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select a,b from testClob where b = 10000");
        byte[] buff = new byte[128];
        // fetch row back, get the column as a clob.
        Clob clob = null;
        InputStream fin;
        int clobLength = 0, i = 0;
        assertTrue("FAIL - row not found", rs.next());
        i++;
        clobLength = rs.getInt(2);
        // get the first column as a clob
        clob = rs.getClob(1);
        assertEquals("FAIL - wrong clob length", 10000, clobLength);
        fin = clob.getAsciiStream();
        int columnSize = 0;

        stmt.executeUpdate("drop table testClob");

        try {
            while (columnSize < 11000) {
                int size = fin.read(buff);
                if (size == -1)
                    break;
                columnSize += size;
            }
            fail("FAIL - should have got an IOException");
        } catch (java.io.IOException ioe) {
            if(usingEmbedded()) {
                assertEquals("FAIL - wrong exception",
                        "ERROR 40XD0: Container has been closed.",
                        ioe.getMessage());
            }
        }

        rollback();
    }

    /**
     * Test fix for derby-265.
     * Test that if getBlob is called after the transaction
     * in which it was created is committed, a proper user error
     * is thrown instead of an NPE.
     * Basically per the spec, getBlob is valid only for the duration of
     * the transaction it was created in
     * @throws Exception
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void testNegativeTestDerby265Blob() throws Exception {
        getConnection().setAutoCommit(false);

        PreparedStatement ps = prepareStatement(
                "insert into testBlob(b, a) values(?,?)");
        for (int i = 0; i < 3; i++) {
            InputStream fis = new LoopingAlphabetStream(300000);
            ps.setInt(1, i);
            ps.setBinaryStream(2, fis, 300000);
            ps.executeUpdate();
            fis.close();
        }
        commit();

        getConnection().setAutoCommit(true);

        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s.execute("SELECT b, a FROM testBlob");
        ResultSet rs1 = s.getResultSet();
        Statement s2 = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s2.executeQuery("SELECT b, a FROM testBlob");
        ResultSet rs2 = s2.getResultSet();
        rs2.next();

        Blob b2 = rs2.getBlob(2);
        rs1.next();
        Blob b1 = rs1.getBlob(2);
        rs1.close();
        try {
            rs2.next();
            rs2.getBlob(2);
            fail("FAIL - can not access blob after implicit commit");
        } catch (SQLException sqle) {
            checkException(BLOB_ACCESSED_AFTER_COMMIT, sqle);
        } finally {
            rs2.close();
            s2.close();
            s.close();
            ps.close();
        }
    }

    /**
     * Test fix for derby-265.
     * Test that if getClob is called after the transaction
     * in which it was created is committed, a proper user error
     * is thrown instead of an NPE.
     * Basically per the spec, getClob is valid only for the duration of
     * the transaction in it was created in
     * @throws Exception
     */
    public void testNegativeTestDerby265Clob() throws Exception {

        getConnection().setAutoCommit(false);

        PreparedStatement ps = prepareStatement(
                "insert into testClob(b, a) values(?,?)");
        for (int i = 0; i < 3; i++) {
            Reader fis = new LoopingAlphabetReader(300000);
            ps.setInt(1, i);
            ps.setCharacterStream(2, fis, 300000);
            ps.executeUpdate();
            fis.close();
        }
        commit();

        getConnection().setAutoCommit(true);

        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s.execute("SELECT b, a FROM testClob");
        ResultSet rs1 = s.getResultSet();
        Statement s2 = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s2.executeQuery("SELECT b, a FROM testClob");
        ResultSet rs2 = s2.getResultSet();
        rs2.next();

        Clob b2 = rs2.getClob(2);
        rs1.next();
        Clob b1 = rs1.getClob(2);
        rs1.close();
        try {
            rs2.next();
            rs2.getClob(2);
            fail("FAIL - can not access blob after implicit commit");
        } catch (SQLException sqle) {
            checkException(BLOB_ACCESSED_AFTER_COMMIT, sqle);
        } finally {
            rs2.close();
            s2.close();
            s.close();
            ps.close();
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("BlobClob4BlobTest");
        suite.addTest(
                TestConfiguration.embeddedSuite(BlobClob4BlobTest.class));
        suite.addTest(
                TestConfiguration.clientServerSuite(BlobClob4BlobTest.class));
        // JSR169 does not have encryption support
        if (JDBC.vmSupportsJDBC3())
        {
            TestSuite encSuite = new TestSuite ("BlobClob4BlobTest:encrypted");
            encSuite.addTestSuite (BlobClob4BlobTest.class);
            suite.addTest(Decorator.encryptedDatabase (encSuite));
        }
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4));
    }


    private void insertDefaultData() throws Exception {
        PreparedStatement ps = prepareStatement(
                "INSERT INTO testClob (a, b, c) VALUES (?, ?, ?)");

        String clobValue = "";
        ps.setString(1, clobValue);
        ps.setInt(2, clobValue.length());
        ps.setLong(3, 0);
        ps.addBatch();
        clobValue = "you can lead a horse to water but you can't form it " +
                "into beverage";
        ps.setString(1, clobValue);
        ps.setInt(2, clobValue.length());
        ps.setLong(3, 0);
        ps.addBatch();
        clobValue = "a stitch in time says ouch";
        ps.setString(1, clobValue);
        ps.setInt(2, clobValue.length());
        ps.setLong(3, 0);
        ps.addBatch();
        clobValue = "here is a string with a return \n character";
        ps.setString(1, clobValue);
        ps.setInt(2, clobValue.length());
        ps.setLong(3, 0);
        ps.addBatch();

        ps.executeBatch();
        ps.clearBatch();

        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.modernLatinLowercase(), 0);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.modernLatinLowercase(), 56);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.modernLatinLowercase(), 5000);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.modernLatinLowercase(), 10000);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.modernLatinLowercase(), 300000);

        ps.setNull(1, Types.CLOB);
        ps.setInt(2, 0);
        ps.setLong(3, 0);
        ps.executeUpdate();

        ps.close();

        ps = prepareStatement(
                "INSERT INTO testBlob (a, b, crc32) VALUES (?, ?, ?)");

        byte[] blobValue = "".getBytes("US-ASCII");
        ps.setBytes(1, blobValue);
        ps.setInt(2, blobValue.length);
        ps.setLong(3, getStreamCheckSum(new ByteArrayInputStream(blobValue)));
        ps.addBatch();
        blobValue = ("you can lead a horse to water but you can't form it " +
                "into beverage").getBytes("US-ASCII");
        ps.setBytes(1, blobValue);
        ps.setInt(2, blobValue.length);
        ps.setLong(3, getStreamCheckSum(new ByteArrayInputStream(blobValue)));
        ps.addBatch();
        blobValue = "a stitch in time says ouch".getBytes("US-ASCII");
        ps.setBytes(1, blobValue);
        ps.setInt(2, blobValue.length);
        ps.setLong(3, getStreamCheckSum(new ByteArrayInputStream(blobValue)));
        ps.addBatch();
        blobValue = "here is a string with a return \n character".
                getBytes("US-ASCII");
        ps.setBytes(1, blobValue);
        ps.setInt(2, blobValue.length);
        ps.setLong(3, getStreamCheckSum(new ByteArrayInputStream(blobValue)));
        ps.addBatch();

        ps.executeBatch();
        ps.clearBatch();

        insertLoopingAlphabetStreamData(
                ps, 0);
        insertLoopingAlphabetStreamData(
                ps, 56);
        insertLoopingAlphabetStreamData(
                ps, 5000);
        insertLoopingAlphabetStreamData(
                ps, 10000);
        insertLoopingAlphabetStreamData(
                ps, 300000);

        ps.setNull(1, Types.BLOB);
        ps.setInt(2, 0);
        ps.setNull(3, Types.BIGINT);
        ps.executeUpdate();

        ps.close();

        commit();
    }


    private void insertUnicodeData(String[] unicodeString) throws Exception {
        PreparedStatement ps = prepareStatement(
                "INSERT INTO testClob (a, b, c) VALUES (?, ?, ?)");

        for (int i=0; i<unicodeString.length; i++) {
            ps.setString(1, unicodeString[i]);
            ps.setInt(2, unicodeString[i].length());
            ps.setInt(3, i);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.clearBatch();

        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.tamil(), 0);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.tamil(), 56);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.tamil(), 5000);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.tamil(), 10000);
        insertLoopingAlphabetStreamData(
                ps, CharAlphabet.tamil(), 300000);

        ps.setNull(1, Types.CLOB);
        ps.setInt(2, 0);
        ps.setInt(3, -1);
        ps.executeUpdate();

        ps.close();
        commit();
    }

    private void insertLoopingAlphabetStreamData(
            PreparedStatement ps,
            int lobLength)
        throws Exception
    {
        ps.setBinaryStream(1, new LoopingAlphabetStream(lobLength), lobLength);
        ps.setInt(2, lobLength);
        ps.setLong(3, getStreamCheckSum(new LoopingAlphabetStream(lobLength)));
        ps.executeUpdate();
    }

    private void insertLoopingAlphabetStreamData(
            PreparedStatement ps,
            CharAlphabet alphabet,
            int lobLength)
        throws Exception
    {
        ps.setCharacterStream(1, new LoopingAlphabetReader(lobLength, alphabet),
                lobLength);
        ps.setInt(2, lobLength);
        ps.setLong(3, -1);
        ps.executeUpdate();
    }

    private boolean compareReaders(Reader origValue, Reader newValue)
    throws Exception {
        char[] origBuff = new char[1024];
        char[] newBuff = new char[1024];
        int countOrig = -1;
        int countNew = -1;
        do {
            countOrig = origValue.read(origBuff);
            countNew = newValue.read(newBuff);
            if (countOrig != countNew) {
                return false;
            }
            if (!java.util.Arrays.equals(origBuff, newBuff)) {
                return false;
            }
        } while (countOrig != -1);
        return true;
    }

    /**
     * Get the CRC32 checksum of a stream, reading
     * its contents entirely and closing it.
     */
    private long getStreamCheckSum(InputStream in)
    throws Exception {
        CRC32 sum = new CRC32();

        byte[] buf = new byte[32*1024];

        for (;;) {
            int read = in.read(buf);
            if (read == -1)
                break;
            sum.update(buf, 0, read);
        }
        in.close();
        return sum.getValue();
    }

    /**
     * Verifies the value returned by the method Clob.getSubstring()
     */
    private void verifyInterval(Clob clob, long pos, int length,
            int testNum, int clobLength) throws Exception {
        try {
            String subStr = clob.getSubString(pos, length);
            assertEquals("FAIL - getSubString returned wrong length",
                    Math.min((clob.length() - pos) + 1, length),
                    subStr.length());
            assertEquals("FAIL - clob has mismatched lengths",
                    clobLength, clob.length());
            assertFalse("FAIL - NO ERROR ON getSubString POS TOO LARGE",
                    (pos > clobLength + 1));

            // Get expected value usign Clob.getAsciiStream()
            char[] value = new char[length];
            String valueString;
            Reader reader = clob.getCharacterStream();
            long left = pos - 1;
            long skipped = 0;
            if (clobLength > 0) {
                println("clobLength: " + clobLength);
                while (left > 0 && skipped >= 0) {
                    skipped = reader.skip(Math.min(1024, left));
                    left -= skipped > 0 ? skipped : 0;
                }
            }
            int numBytes = reader.read(value);

            // chech the the two values match
            if (numBytes >= 0) {
                char[] readBytes = new char[numBytes];
                System.arraycopy(value, 0, readBytes, 0, numBytes);
                valueString = new String(readBytes);
                assertEquals("FAIL - wrong substring value",
                        valueString, subStr);
            } else {
                assertTrue("FAIL - wrong length", subStr.length() == 0);
            }
        } catch (SQLException e) {
            if (pos <= 0) {
                checkException(BLOB_BAD_POSITION, e);
            } else {
                if (pos > clobLength + 1) {
                    checkException(BLOB_POSITION_TOO_LARGE, e);
                } else {
                    throw e;
                }
            }
        } catch (StringIndexOutOfBoundsException obe) {
            // Known bug.  JCC 5914.
            if (!((pos > clobLength) && usingDB2Client())) {
                throw obe;
            }
        }
    }

    /**
     * Verifies the value returned by the method Blob.getBytes()
     */
    private void verifyInterval(Blob blob, long pos, int length,
            int testNum, int blobLength) throws Exception {
        try {
            String subStr = new String(blob.getBytes(pos,length), "US-ASCII");
            assertEquals("FAIL - getSubString returned wrong length ",
                    Math.min((blob.length() - pos) + 1, length),
                    subStr.length());
            assertEquals("FAIL - clob has mismatched lengths",
                    blobLength, blob.length());
            assertFalse("FAIL - NO ERROR ON getSubString POS TOO LARGE",
                    (pos > blobLength + 1));

            // Get expected value usign Blob.getBinaryStream()
            byte[] value = new byte[length];
            String valueString;
            InputStream inStream = blob.getBinaryStream();
            inStream.skip(pos - 1);
            int numBytes = inStream.read(value);
            // check that the two values match
            if (numBytes >= 0) {
                byte[] readBytes = new byte[numBytes];
                System.arraycopy(value, 0, readBytes, 0, numBytes);
                valueString = new String(readBytes, "US-ASCII");
                assertEquals("FAIL - wrong substring value",
                        valueString, subStr);
            } else {
                assertTrue("FAIL - wrong length", subStr.length() == 0);
            }
        } catch (SQLException e) {
            if (pos <= 0) {
                checkException(BLOB_BAD_POSITION, e);
            } else {
                if (pos > blobLength + 1) {
                    checkException(BLOB_POSITION_TOO_LARGE, e);
                } else {
                    throw e;
                }
            }
        } catch (NegativeArraySizeException nase) {
            if (!((pos > blobLength) && usingDB2Client())) {
                throw nase;
            }
        }
    }


    /**
     * Test the contents of the testBlob table or ResultSet
     * with identical shape.
     * @param rs
     * @throws Exception
     */
    public void testBlobContents(ResultSet rs) throws Exception {
        int nullCount = 0;
        int rowCount = 0;
        byte[] buff = new byte[128];
        // fetch row back, get the long varbinary column as a blob.
        Blob blob;
        int blobLength = 0, i = 0;
        while (rs.next()) {
            i++;
            // get the first column as a clob
            blob = rs.getBlob(1);
            long crc32 = rs.getLong(3);
            boolean crc2Null = rs.wasNull();
            if (blob == null) {
                assertTrue("FAIL - NULL BLOB but non-NULL checksum", crc2Null);
                nullCount++;
            } else {
                rowCount++;

                long blobcrc32 = getStreamCheckSum(blob.getBinaryStream());
                assertEquals("FAIL - mismatched checksums for blob with " +
                        "length " + blob.length(), blobcrc32, crc32);

                InputStream fin = blob.getBinaryStream();
                int columnSize = 0;
                for (;;) {
                    int size = fin.read(buff);
                    if (size == -1)
                        break;
                    columnSize += size;
                }
                blobLength = rs.getInt(2);
                assertEquals("FAIL - wrong column size",
                        blobLength, columnSize);
                assertEquals("FAIL - wrong column length",
                        blobLength, blob.length());
            }
        }
        assertEquals("FAIL - wrong not null row count null:" + nullCount,
                9, rowCount);
        assertEquals("FAIL - wrong null blob count", 1, nullCount);
    }

    /**
     * From LobTest.java, test various inserts on a BLOB column
     * 
     * @throws SQLException
     */
    public void testBlobInsert() throws SQLException {
        String[] typeNames = { "int", "char(10)", "varchar(80)", "long varchar",
                "char(10) for bit data", "long varchar for bit data", "blob(80)" };

        Connection conn = getConnection();
        Statement s = conn.createStatement();

        // create table for testing

        s.execute("create table blobCheck (bl blob(80)) ");

        int columns = typeNames.length;
        // test insertion of literals.
        for (int i = 0; i < columns; i++) {

            if (typeNames[i].indexOf("blob") == -1)
                continue;

            // Check char literals.
            // (fail)
            String insert = "insert into blobCheck (bl"
                    + " ) values ('string' )";
            assertStatementError("42821",s,insert);
            // (succeed)
            insert = "insert into blobCheck (bl" + " ) values (cast ("
                    + Utilities.stringToHexLiteral("string") + " as blob(80)) )";
            s.execute(insert);
            // Check bit literals.
            // (fail)
            insert = "insert into blobCheck (bl" + " ) values (X'48' )";
           assertStatementError("42821",s,insert);
            // old CS compatible value: ( b'01001' )
            // (succeed)
            insert = "insert into blobCheck (bl"
                    + " ) values (cast (X'C8' as blob(80)) )";
            s.execute(insert);
            // Check hex literals.
            // (fail)
            insert = "insert into blobCheck (bl" + " ) values ( X'a78a' )";
            assertStatementError("42821",s,insert);
            // (succeed)
            insert = "insert into blobCheck (bl"
                    + " ) values (cast (X'a78a' as blob(80)) )";
            s.execute(insert);
        }
        s.execute("drop table blobCheck");
    }

     
    private void checkException(String SQLState, SQLException se)
            throws Exception
    {
        if (!usingDB2Client()) {
            assertSQLState(SQLState, se);
        }
    }


    /**
     * DERBY-3243 Fix ArrayIndexOutOfBounds Exception
     * if we retrieve more than 32K lobs
     * 
     */
    public void testRetrieveMoreThan32KLobs() throws SQLException
    {
        int numRows = 34000;
        // Load the database
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = createStatement();
        
        PreparedStatement ps = prepareStatement("INSERT INTO TESTCLOB VALUES(?,?,?)");
        for (int i = 0 ; i < numRows;i++)
        {
            ps.setInt(1,i);
            ps.setInt(2,i);
            ps.setString(3,"" + i);
            ps.executeUpdate();
            if (i % 1000 == 0) {
                commit();
            }
        }
        commit();
        
        // retrieve the data
        
        ResultSet rs = s.executeQuery("SELECT * from TESTCLOB");
        while (rs.next()) {
            rs.getInt(1);
            Clob c = rs.getClob(3);
            c.getSubString(1,100);
        }
        rs.close();
        
        conn.commit();
        
        
    }




        
    
    private static final String BLOB_BAD_POSITION = "XJ070";
    private static final String BLOB_NONPOSITIVE_LENGTH = "XJ071";
    private static final String BLOB_POSITION_TOO_LARGE = "XJ076";
    private static final String LANG_DATA_TYPE_GET_MISMATCH = "22005";
    private static final String BLOB_NULL_PATTERN_OR_SEARCH_STR = "XJ072";
    private static final String LOCK_TIMEOUT = "40XL1";
    private static final String BLOB_ACCESSED_AFTER_COMMIT = "XJ073";
    private static final String NO_CURRENT_CONNECTION = "08003";
    private static final String INVALID_LOB = "XJ215";
    private static final String INVALID_LOCATOR = "XJ217";

}

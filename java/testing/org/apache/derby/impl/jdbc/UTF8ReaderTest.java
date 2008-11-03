/*

   Derby - Class org.apache.derby.impl.jdbc.UTF8ReaderTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */
package org.apache.derby.impl.jdbc;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * Tests {@code UTF8Reader} using package-private classes/methods.
 */
public class UTF8ReaderTest
    extends BaseJDBCTestCase {

    public UTF8ReaderTest(String name) {
        super(name);
    }

    /**
     * Tests simple repositioning.
     */
    public void testRepositioningSimple()
            throws IOException, SQLException, StandardException {
        setAutoCommit(false);
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select * from Utf8ReaderTest where id = 101");
        rs.next();
        final int size = rs.getInt(2);
        DataValueDescriptor dvd = ((EmbedResultSet)rs).getColumn(3);
        StoreStreamClob ssClob = new StoreStreamClob(
                dvd.getStream(), (EmbedResultSet)rs);
        Reader reader = ssClob.getInternalReader(1);
        assertEquals('a', reader.read());
        // Get internal readers and do stuff.
        checkInternalStream(1, ssClob); // Get first character.
        checkInternalStream(26, ssClob); // Skip forwards inside buffer.
        checkInternalStream(17003, ssClob); // Skip forwards, refill buffer.
        checkInternalStream(size, ssClob); // Skip until end.
        assertEquals(-1, reader.read());
        checkInternalStream(10, ssClob); // Rewind and refill buffer.
        try {
            checkInternalStream(size*2, ssClob); // Should fail, invalid pos.
            fail("Should have failed due to invalid position");
        } catch (EOFException eofe) {
            // As expected, do nothing.
        }
    }

    /**
     * Tests repositioning withing the buffer.
     */
    public void testRepositioningWithinBuffer()
            throws IOException, SQLException, StandardException {
        setAutoCommit(false);
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select * from Utf8ReaderTest where id = 100");
        rs.next();
        DataValueDescriptor dvd = ((EmbedResultSet)rs).getColumn(3);
        StoreStreamClob ssClob = new StoreStreamClob(
                dvd.getStream(), (EmbedResultSet)rs);
        Reader reader = ssClob.getInternalReader(1);
        assertEquals('a', reader.read());
        int bufSize = 26000;
        char[] buf = new char[bufSize];
        int count = 0;
        while (count < bufSize) {
            count += reader.read(buf, count, bufSize - count);
        }
        // We have now read 26001 chars. Next char should be 'b'.
        // Internal buffer size after the singel read below should be:
        // 26002 % 8192 = 1426
        assertEquals('b', reader.read());
        reader.close();
        // Get internal readers and do stuff.
        checkInternalStream(26002, ssClob);
        checkInternalStream(26001, ssClob);
        checkInternalStream(26002-1426+1, ssClob); // First char in buffer
        checkInternalStream(26001+(8192-1426+1), ssClob); // Last char in buffer
        checkInternalStream(26002-1426, ssClob); // Requires reset
        checkInternalStream(26002-1426+1, ssClob); // Requires refilling buffer
        checkInternalStream(26002, ssClob);
        checkInternalStream(1, ssClob);
    }

    /**
     * Tests repositioning withing buffer with a "real text" to make sure the
     * correct values are returned.
     */
    public void testRepositioningWithinBufferRealText()
            throws IOException, SQLException, StandardException {
        setAutoCommit(false);
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                // See insertTestData
                "select * from Utf8ReaderTest where id = 1");
        rs.next();
        DataValueDescriptor dvd = ((EmbedResultSet)rs).getColumn(3);
        StoreStreamClob ssClob = new StoreStreamClob(
                dvd.getStream(), (EmbedResultSet)rs);
        Reader reader = ssClob.getInternalReader(1);
        assertEquals('B', reader.read());
        reader = ssClob.getInternalReader(24);
        assertEquals('\'', reader.read());
        reader = ssClob.getInternalReader(42);
        assertEquals('H', reader.read());
        reader = ssClob.getInternalReader(70);
        assertEquals('M', reader.read());
        reader = ssClob.getInternalReader(102);
        assertEquals('M', reader.read());
        reader = ssClob.getInternalReader(128);
        assertEquals('B', reader.read());
        reader = ssClob.getInternalReader(155);
        assertEquals('A', reader.read());
        reader = ssClob.getInternalReader(184);
        assertEquals('S', reader.read());
        reader = ssClob.getInternalReader(207);
        assertEquals('H', reader.read());
        reader = ssClob.getInternalReader(224);
        assertEquals('O', reader.read());
        reader = ssClob.getInternalReader(128);
        char[] buf = new char[4];
        assertEquals(4, reader.read(buf));
        assertEquals("But ", new String(buf));
        reader = ssClob.getInternalReader(70);
        buf = new char[32];
        assertEquals(32, reader.read(buf));
        assertEquals("Men the grocer and butcher sent\n", new String(buf));
    }

    /**
     * Makes sure the data returned from the internal Clob matches the data
     * returned by a fresh looping alphabet stream.
     *
     * @param pos 1-based Clob position
     * @param clob internal store stream Clob representation
     */
    private static void checkInternalStream(long pos, StoreStreamClob clob)
            throws IOException, SQLException {
        Reader canonStream = new LoopingAlphabetReader(pos + 100);
        long toSkip = pos -1; // Convert to 0-based index.
        while (toSkip > 0) {
            long skipped = canonStream.skip(toSkip);
            if (skipped > 0) {
                toSkip -= skipped;
            }
        }
        Reader clobStream = clob.getInternalReader(pos);
        assertEquals("Data mismatch", canonStream.read(), clobStream.read());
        clobStream.close();
    }

    /**
     * Returns a simple test suite, using the embedded driver only.
     *
     * @return A test suite.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(UTF8ReaderTest.class);
        return new CleanDatabaseTestSetup(suite) {
            public void decorateSQL(Statement stmt)
                    throws SQLException {
                insertTestData(stmt);
            }
        };
    }

    /**
     * Inserts data used by the tests.
     * <p>
     * Use the id to select a Clob with specific contents.
     */
    private static void insertTestData(Statement stmt)
            throws SQLException {
        int[][] sizes = new int[][] {
                            {100, 1*1024*1024},        // 1M chars
                            {101, 32*1024},            // 32K chars
            };
        stmt.executeUpdate(
                "create table Utf8ReaderTest" +
                "(id int primary key, size int, dClob clob)");
        PreparedStatement ps = stmt.getConnection().prepareStatement(
                "insert into Utf8ReaderTest values (?,?,?)");
        for (int i=0; i < sizes.length; i++) {
            ps.setInt(1, sizes[i][0]);
            int size = sizes[i][1];
            ps.setInt(2, size);
            ps.setCharacterStream(3, new LoopingAlphabetReader(size), size);
            ps.executeUpdate();
        }
        
        // Insert some special pieces of text, repeat to get it represented as
        // a stream.
        ps.setInt(1, 1);
        int size = aintWeGotFun.length();
        ps.setInt(2, size);
        StringBuffer str = new StringBuffer(32*1024 + aintWeGotFun.length());
        while (str.length() < 32*1024) {
            str.append(aintWeGotFun);
        }
        ps.setString(3, str.toString());
        ps.executeUpdate();
    }

    /**
     * Test data, first part of "Ain't We Got Fun?" (public domain).
     * See http://en.wikipedia.org/wiki/Ain%27t_We_Got_Fun%3F
     */
    public static final String aintWeGotFun =
            // 1-based positions for the first and the last character on line.
            "Bill collectors gather\n" + // 1
            "'Round and rather\n" + // 24
            "Haunt the cottage next door\n" + // 42
            "Men the grocer and butcher sent\n" + // 70
            "Men who call for the rent\n" + // 102
            "But with in a happy chappy\n" + // 128
            "And his bride of only a year\n" + // 155
            "Seem to be so cheerful\n" + // 184
            "Here's an earful\n" + // 207
            "Of the chatter you hear\n"; // 224

    /*
        // Code that can be used to check the positions in the text.
        String[] firstWords = new String[] {"Bill", "'Round", "Haunt", "Men th",
            "Men wh", "But", "And", "Seem", "Here's", "Of"};
        for (int i=0; i < firstWords.length; i++) {
            System.out.println("> " + firstWords[i]);
            int clobPos = (int)clob.position(firstWords[i], 1);
            int strPos = aintWeGotFun.indexOf(firstWords[i]);
            System.out.println("\tClob: " + clobPos);
            System.out.println("\tString: " + strPos);
            assertTrue(clobPos == strPos +1);
        }
    */
}

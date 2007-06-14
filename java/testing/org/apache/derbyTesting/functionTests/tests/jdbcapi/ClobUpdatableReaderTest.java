/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ClobUpdatableReaderTest
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

import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Test class to test <code>UpdateableReader</code> for <code>Clob</code> in
 * embedded driver.
 */
public class ClobUpdatableReaderTest extends BaseJDBCTestCase {
    
    private final String dummy = "This is a new String";
        
    public ClobUpdatableReaderTest (String name) {
        super (name);
    }
    
    /**
     * Test updating a large clob
     */
    public void testUpdateableStoreReader () throws Exception {
        Connection con = getConnection();
        try {
            con.setAutoCommit (false);
            PreparedStatement ps = con.prepareStatement ("insert into updateClob " +
                    "(id , data) values (? ,?)");
            ps.setInt (1, 2);
            StringBuffer sb = new StringBuffer ();
            String base = "SampleSampleSample";
            for (int i = 0; i < 100000; i++) {
                sb.append (base);
            }
            //insert a large enough data to ensure stream is created in dvd
            ps.setCharacterStream (2, new StringReader (sb.toString()), 
                                                sb.length());
            ps.execute();
            ps.close();
            Statement stmt = con.createStatement ();
            ResultSet rs = stmt.executeQuery("select data from " +
                    "updateClob where id = 2");
            rs.next();
            Clob clob = rs.getClob (1);            
            rs.close();
            stmt.close();
            assertEquals (sb.length(), clob.length());
            Reader r = clob.getCharacterStream();
            String newString = "this is a new string";
            //access reader before modifying the clob
            long l = r.skip (100);
            clob.setString (1001, newString);
            //l chars are already skipped
            long toSkip = 1000 - l;
            while (toSkip > 0) {
                long skipped = r.skip (toSkip);
                toSkip -= skipped;
            }
            char [] newdata = new char [newString.length()];
            int len = r.read(newdata);
            assertEquals ("updated not reflected", newString, 
                                    new String (newdata, 0, len));
            r.close();
        }
        finally {
            if (con != null) {
                con.commit ();
                con.close ();
            }
        }

    }

    /**
     * Tests updates on reader.
     */
    public void testUpdateableReader () throws Exception {
        Connection con = getConnection();
        try {
            con.setAutoCommit (false);
            PreparedStatement ps = con.prepareStatement ("insert into updateClob " +
                    "(id , data) values (? ,?)");
            ps.setInt (1, 1);
            StringBuffer sb = new StringBuffer ();
            String base = "SampleSampleSample";
            for (int i = 0; i < 100; i++) {
                sb.append (base);
            }
            ps.setCharacterStream (2, new StringReader (sb.toString()), 
                                                sb.length());
            ps.execute();
            ps.close();
            Statement stmt = con.createStatement ();
            ResultSet rs = stmt.executeQuery("select data from " +
                    "updateClob where id = 1");
            rs.next();
            Clob clob = rs.getClob (1);
            rs.close();
            stmt.close();
            assertEquals (sb.length(), clob.length());
            Reader r = clob.getCharacterStream();
            char [] clobData = new char [sb.length()];
            r.read (clobData);
            assertEquals ("mismatch from inserted string", 
                                String.valueOf (clobData), sb.toString());
            r.close();
            //update before gettting the reader
            clob.setString (50, dummy);        
            r = clob.getCharacterStream();
            r.skip (49);
            char [] newChars = new char [dummy.length()];
            r.read (newChars);
            assertEquals ("update not reflected", dummy,
                                        String.valueOf (newChars));
            //update again and see if stream is refreshed
            clob.setString (75, dummy);
            r.skip (75 - 50 - dummy.length());
            char [] testChars = new char [dummy.length()];
            r.read (testChars);
            assertEquals ("update not reflected", dummy,
                                        String.valueOf (newChars));
            r.close();
            //try inserting some unicode string
            String unicodeStr = getUnicodeString();
            clob.setString (50, unicodeStr);
            char [] utf16Chars = new char [unicodeStr.length()];
            r = clob.getCharacterStream();
            r.skip(49);
            r.read(utf16Chars);
            assertEquals ("update not reflected",  unicodeStr,
                                        String.valueOf (utf16Chars));
            r.close();
            Writer w = clob.setCharacterStream (1);
            //write enough data to switch the data to file
            r = clob.getCharacterStream ();
            for (int i = 0; i < 10000; i++) {
                w.write (dummy);
            }
            w.close();            
            clob.setString (500, unicodeStr);
            r.skip (499);
            char [] unicodeChars = new char [unicodeStr.length()];
            r.read (unicodeChars);
            assertEquals ("update not reflected",  unicodeStr,
                                        String.valueOf (unicodeChars));            
        }
        finally {
            if (con != null) {
                con.commit ();
                con.close();
            }
        }
    }   
    
    /**
     * Tests that the Clob can handle multiple streams and the length call
     * multiplexed.
     * <p>
     * This test was written after bug DERBY-2806 was reported, where getting
     * the length of the Clob after fetching a stream from it would exhaust
     * the stream and cause the next read to return -1.
     * <p>
     * The test is written to work on a Clob that operates on streams from
     * the store, which currently means that it must be over a certain size
     * and that no modifying methods can be called on it.
     */
    public void testMultiplexedOperationProblem()
            throws IOException, SQLException {
        int length = 266000;
        PreparedStatement ps = prepareStatement(
                "insert into updateClob (id, data) values (?,?)");
        ps.setInt(1, length);
        ps.setCharacterStream(2, new LoopingAlphabetReader(length), length);
        assertEquals(1, ps.executeUpdate());
        ps.close();
        PreparedStatement psFetchClob = prepareStatement(
                "select data from updateClob where id = ?");
        psFetchClob.setInt(1, length);
        ResultSet rs = psFetchClob.executeQuery();
        assertTrue("No Clob of length " + length + " in database", rs.next());
        Clob clob = rs.getClob(1);
        assertEquals(length, clob.length());
        Reader r = clob.getCharacterStream();
        int lastReadChar = r.read();
        lastReadChar = assertCorrectChar(lastReadChar, r.read());
        lastReadChar = assertCorrectChar(lastReadChar, r.read());
        assertEquals(length, clob.length());
        // Must be bigger than internal buffers might be.
        int nextChar;
        for (int i = 2; i < 160000; i++) {
            nextChar = r.read();
            // Check manually to report position where it fails.
            if (nextChar == -1) {
                fail("Failed at position " + i + ", stream should not be" +
                        " exhausted now");
            }
            lastReadChar = assertCorrectChar(lastReadChar, nextChar);
        }
        lastReadChar = assertCorrectChar(lastReadChar, r.read());
        lastReadChar = assertCorrectChar(lastReadChar, r.read());
        InputStream ra = clob.getAsciiStream();
        assertEquals(length, clob.length());
        int lastReadAscii = ra.read();
        lastReadAscii = assertCorrectChar(lastReadAscii, ra.read());
        lastReadAscii = assertCorrectChar(lastReadAscii, ra.read());
        assertEquals(length, clob.length());
        lastReadAscii = assertCorrectChar(lastReadAscii, ra.read());
        lastReadChar = assertCorrectChar(lastReadChar, r.read());
    }


    /**
     * Asserts that the two specified characters follow each other in the
     * modern latin lowercase alphabet.
     */
    private int assertCorrectChar(int prevChar, int nextChar)
            throws IOException {
        assertTrue("Reached EOF unexpectedly", nextChar != -1);
        if (nextChar < 97 && nextChar > 122) {
            fail("Char out of range: " + nextChar);
        }
        if (prevChar < 97 && prevChar > 122) {
            fail("Char out of range: " + prevChar);
        }
        if (prevChar > -1) {
            // Work with modern latin lowercase: 97 - 122
            if (prevChar == 122) {
                assertTrue(prevChar + " -> " + nextChar,
                        nextChar == 97);
            } else {
                assertTrue(prevChar + " -> " + nextChar,
                        nextChar == prevChar +1);
            }
        }
        return nextChar;
    }
    /**
     * Generates a (static) string containing various Unicode characters.
     *
     * @return a string with ASCII and non-ASCII characters
     */
    private String getUnicodeString () {
        char[] fill = new char[4];
        fill[0] = 'd';          // 1 byte UTF8 character (ASCII)
        fill[1] = '\u03a9';     // 2 byte UTF8 character (Greek)
        fill[2] = '\u0e14';     // 3 byte UTF8 character (Thai)
        fill[3] = 'j';          // 1 byte UTF8 character (ASCII)
        StringBuffer sb = new StringBuffer ();
        for (int i = 0; i < 4; i++) {
            sb.append (fill);
        }
        return sb.toString();        
    }
    
    /**
     * Setup the test.
     *
     * @throws SQLException if database access fails
     */
    public void setUp() throws Exception {
        Connection con = getConnection ();
        Statement stmt = con.createStatement ();
        stmt.execute ("create table updateClob " +
                "(id integer primary key, data clob)");
        stmt.close();
        con.commit();
        con.close();
    }
    
    public static Test suite() {
        TestSuite ts = new TestSuite ("ClobUpdatableReaderTest");
        ts.addTest(TestConfiguration.defaultSuite(
                    ClobUpdatableReaderTest.class));
        TestSuite encSuite = new TestSuite ("ClobUpdatableReaderTest:encrypted");
        encSuite.addTestSuite (ClobUpdatableReaderTest.class);
        ts.addTest(Decorator.encryptedDatabase (encSuite));
        return ts;
    }        

    /**
     * Cleans up the database.
     */
    protected void tearDown() throws java.lang.Exception {
        Connection con = getConnection ();
        Statement stmt = con.createStatement ();
        stmt.execute ("drop table updateClob");
        stmt.close();
        con.close();
    }
}

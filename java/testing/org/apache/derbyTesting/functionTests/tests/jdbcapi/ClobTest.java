/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ClobTest

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.Connection;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;



/**
 * Test the methods defined by the {@link java.sql.Clob} interface.
 * <p>
 * Only methods defined by JDBC 3 or earlier are tested here, and the focus of
 * the test is the interface methods. Less attention is given to inserting
 * Clobs and fetching Clobs from the database.
 */
public class ClobTest
    extends BaseJDBCTestCase {

    /** Buffer size to use when transferring data between streams. */
    private static final int TRANSFER_BUFFER_SIZE = 4*1024; // 4 KB

    /** Constant for Clob.setString method. */
    private static final int SET_STRING = 1;
    /** Constant for Clob.setAsciiStream method. */
    private static final int SET_ASCII_STREAM = 2;
    /** Constant for Clob.setCharacterStream method. */
    private static final int SET_CHARACTER_STREAM = 4;

    /** Test data, 18 characters long, containing only Norwegian letters. */
    private static final String NORWEGIAN_LETTERS =
            "\u00e6\u00f8\u00e5\u00e6\u00f8\u00e5\u00e6\u00f8\u00e5" +
            "\u00e6\u00f8\u00e5\u00e6\u00f8\u00e5\u00e6\u00f8\u00e5";

    /** Result set used to obtain Clob. */
    private ResultSet rs = null;
    /**
     * The Clob used for testing.
     * It is reinitialized to a Clob containing the empty string for each test.
     */
    private Clob clob = null;

    public ClobTest(String testName) {
        super(testName);
    }

    public void testGetSubString_PosOneTooBig()
            throws SQLException {
        long length = this.clob.length();
        assertEquals("", this.clob.getSubString(length +1, 10));
    }

    public void testGetSubString_PosTooBig() {
        try {
            this.clob.getSubString(999, 10);
            fail("getSubString with pos larger than clob length must fail");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
    }

    public void testGetSubString_PosNegative()
            throws SQLException {
        try {
            this.clob.getSubString(-123, 10);
            fail("getSubString with negative position should fail");
        } catch (SQLException sqle) {
            assertSQLState("XJ070", sqle);
        }
    }

    public void testGetSubString_RequestZeroLength_PosValid()
            throws SQLException {
        // Tests if an exception is thrown or not.
        // According to the JDBC spec, 0 is a valid length.
        assertEquals("", this.clob.getSubString(1L, 0));
    }

    public void testGetSubString_RequestZeroLength_PosTooBig()
            throws SQLException {
        try {
            this.clob.getSubString(999L, 0);
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
    }

    /**
     * Tests if big strings can be handled.
     * <p>
     * The motivation for the test is to make sure big buffers are filled with
     * the call to read inside a loop. Big in this sense means bigger than some
     * internal buffer. This is typically around 8 KB or so, but we try
     * something considerably bigger. If a char/byte array is attempted filled
     * with a single call to read, the resulting string wil typically contain
     * \u0000 at positions after the size of the internal buffer.
     */
    public void testGetSubString_BiggerThanInternalBuffer()
            throws IOException, SQLException {
        int stringLength = 1*1024*1024; // 1 M characters
        transferData(new LoopingAlphabetReader(stringLength),
                     this.clob.setCharacterStream(1L),
                     TRANSFER_BUFFER_SIZE);
        String obtained = this.clob.getSubString(1, stringLength);
        assertEquals("Incorrect string length",
            stringLength, obtained.length());
        // Obtain the string we inserted for comparison.
        CharArrayWriter charWriter = new CharArrayWriter();
        transferData(new LoopingAlphabetReader(stringLength), charWriter,
                                               TRANSFER_BUFFER_SIZE);
        assertEquals("String do not match",
            charWriter.toString(), obtained);
    }

    public void testLengthOnEmptyClob()
            throws SQLException {
        assertEquals(0, this.clob.length());
    }

    public void testInsertStringOnEmptyClob_Singlebyte()
            throws SQLException {
        String content = "This is the new Clob content.";
        this.clob.setString(1, content);
        assertEquals("Incorrect length reported",
            content.length(), this.clob.length());
        assertEquals("Clob content is incorrect",
            content, this.clob.getSubString(1, content.length()));
    }

    public void testInsertStringOnEmptyClob_Multibyte()
            throws SQLException {
        String content = "A few Norwegian letters: \u00e6, \u00e5, \u00f8.";
        this.clob.setString(1, content);
        assertEquals("Incorrect length reported",
            content.length(), this.clob.length());
        assertEquals("Clob content is incorrect",
            content, this.clob.getSubString(1, content.length()));
    }

    public void testInsertStringInMiddle_Multibyte()
            throws SQLException {
        // Add some content to work on first.
        this.clob.setString(1, NORWEGIAN_LETTERS);
        assertEquals(NORWEGIAN_LETTERS,
            this.clob.getSubString(1, NORWEGIAN_LETTERS.length()));

        // Replace a portion with single byte characters.
        char[] modifiedContent = NORWEGIAN_LETTERS.toCharArray();
        // Replace chars at 0-based indexes 4,5 and 8
        modifiedContent[4] = 'a';
        modifiedContent[5] = 'b';
        modifiedContent[8] = 'c';
        String newContent = String.copyValueOf(modifiedContent);
        // Do this in a "funny" order, or else it currently fails when running
        // with the client driver.
        assertEquals(1, this.clob.setString(9, "c"));
        assertEquals(1, this.clob.setString(5, "a"));
        assertEquals(1, this.clob.setString(6, "b"));
	    assertEquals("Clob content is incorrect",
            newContent, this.clob.getSubString(1, newContent.length()));
    }

    /**
     * Tests that Derby specific end-of-stream markers aren't passed over to
     * the temporary Clob, which doesn't use such markers.
     * <p>
     * Passing the marker over will normally result in a UTF encoding exception.
     * <p>
     * ID USAGE: reads id 2, writes id 10002
     */
    public void testInsertCharacter_ReadOnlyToTemporary()
            throws IOException, SQLException {
        getConnection().setAutoCommit(false);
        // Insert data, a medium sized Clob to store it as a stream.
        Statement stmt = createStatement();
        PreparedStatement ps = prepareStatement(
                "insert into ClobTestData values (?,?)");
        int initalSize = 128*1024;
        ps.setInt(1, 2);
        ps.setCharacterStream(
                2, new LoopingAlphabetReader(initalSize), initalSize);
        ps.executeUpdate();

        // Select the Clob, and change one character.
        PreparedStatement psSelect = prepareStatement(
                "select dClob from ClobTestData where id = ?");
        psSelect.setInt(1, 2);
        ResultSet lRs = psSelect.executeQuery();
        lRs.next();
        Clob lClob = lRs.getClob(1);
        lClob.setString(1, "K");
        Reader r = lClob.getCharacterStream();
        assertEquals('K', r.read());
        long length = 1;
        while (true) {
            // Since we're skipping characters, the bytes have to be decoded
            // and we will detect any encoding errors.
            long skipped = r.skip(4096);
            if (skipped > 0) {
                length += skipped;
            } else {
                break;
            }
        }
        lRs.close();
        assertEquals("Wrong length!", initalSize, length);
        // Reports the correct length, now try to insert it.
        ps.setInt(1, 10003);
        ps.setClob(2, lClob);
        ps.executeUpdate();
        // Fetch it back.
        psSelect.setInt(1, 10003);
        lRs = psSelect.executeQuery();
        lRs.next();
        Clob lClob2 = lRs.getClob(1);
        assertEquals(lClob.getCharacterStream(), lClob2.getCharacterStream());
        assertEquals(initalSize, lClob2.length());
    }

    public void testPositionWithString_ASCII_SimplePartialRecurringPattern()
            throws IOException, SQLException {
        String token = "xxSPOTxx";
        String inserted ="abcdexxSPxabcdexabxxSPxxxSPOTxabcxxSPOTxxabc";
        this.clob.setString(1L, inserted);
        assertEquals("Invalid match position",
            inserted.indexOf(token, 0) +1, this.clob.position(token, 1L));
    }

    public void testPositionWithString_USASCII()
            throws IOException, SQLException {
        String token = "xxSPOTxx";
        final long prefix = 91*1024 +7;
        final long postfix = 12*1024;
        insertDataWithToken(token, prefix, postfix, SET_ASCII_STREAM);
        executeTestPositionWithStringToken(token, prefix);
    }

    public void testPositionWithString_IOS88591()
            throws IOException, SQLException {
        String token = "xx\u00c6\u00c6\u00c6xx";
        final long prefix = 67*1024;
        final long postfix = 1*1024-2;
        insertDataWithToken(token, prefix, postfix, SET_ASCII_STREAM);
        executeTestPositionWithStringToken(token, prefix);
    }

    public void testPositionWithString_CJK()
            throws IOException, SQLException {
        final long prefix = 11L;
        final long postfix = 90L;
        char[] tmpChar = new char[1];
        LoopingAlphabetReader tokenSrc =
            new LoopingAlphabetReader(1L, CharAlphabet.cjkSubset());
        tokenSrc.read(tmpChar);
        String token = String.copyValueOf(tmpChar);
        insertDataWithToken(token, prefix, postfix, SET_CHARACTER_STREAM);
        //insertDataWithToken(token, prefix, 2*1024-7, SET_CHARACTER_STREAM);
        executeTestPositionWithStringToken(token, prefix);
    }

    /* Test ideas for more tests
     *
     * truncate:
     *      truncate both on in store and from createClob
     *      truncate multiple times, check length and compare content
     *      truncate with negative size
     *      truncate with too big size
     *      truncate to 0
     *      truncate to current length
     *
     * setString:
     *      test with null string
     *      test with offset out of range
     *      test with length of string to insert out of range
     */

    /**
     * Insert text into a Clob using {@link java.sql.Clob#setAsciiStream} and
     * then search for the specified token.
     * <p>
     * Some data is inserted before and after the token, and the specified token
     * is converted to bytes by using the ISO-8859-1 encoding.
     * Note that ascii in JDBC is equivalent to ISO-8859-1, not US-ASCII.
     */
    private void executeTestPositionWithStringToken(String token, long prefixLength)
            throws IOException, SQLException {

        final long TOKEN_POS = prefixLength +1;
        // Start searching behind the token.
        assertEquals(-1, this.clob.position(token, TOKEN_POS+1));
        // Start searching exactly at the right position.
        assertEquals(TOKEN_POS, this.clob.position(token, TOKEN_POS));
        // Start searching at the start of the Clob.
        assertEquals(TOKEN_POS, this.clob.position(token, 1L));
    }

    /**
     * Obtain a Clob containing the empty string.
     */
    protected void setUp()
            throws Exception {
        // Obtain a Clob containing the empty string ("").
        Statement stmt = createStatement();
        // Keep reference to the result set to be able to close it.
        this.rs = stmt.executeQuery(
                "select dClob from ClobTestData where id = 1");
        assertTrue(this.rs.next());
        this.clob = this.rs.getClob(1);
    }

    /**
     * Nullify reference to Clob, close the parent result set.
     */
    protected void tearDown()
            throws Exception {
        this.clob = null;
        this.rs.close();
        super.tearDown();
    }

    public static Test suite() {
        return new ClobTestSetup(
            TestConfiguration.defaultSuite(ClobTest.class, false));
    }

    /**
     * Transfer data from an input stream to an output stream.
     *
     * @param source source data
     * @param dest destination to write to
     * @param tz buffer size in number of bytes. Must be 1 or greater.
     * @return Number of bytes read from the source data. This should equal the
     *      number of bytes written to the destination.
     */
    private int transferData(InputStream source, OutputStream dest, int tz)
            throws IOException {
        if (tz < 1) {
            throw new IllegalArgumentException(
                "Buffer size must be 1 or greater: " + tz);
        }
        BufferedInputStream in = new BufferedInputStream(source);
        BufferedOutputStream out = new BufferedOutputStream(dest, tz);
        byte[] bridge = new byte[tz];
        int total = 0;
        int read;
        while ((read = in.read(bridge, 0, tz)) != -1) {
            out.write(bridge, 0, read);
            total += read;
        }
        in.close();
        // Don't close the stream, in case it will be written to again.
        out.flush();
        return total;
    }

        /**
     * Transfer data from a source Reader to a destination Writer.
     *
     * @param source source data
     * @param dest destination to write to
     * @param tz buffer size in number of characters. Must be 1 or greater.
     * @return Number of characters read from the source data. This should equal the
     *      number of characters written to the destination.
     */
    private int transferData(Reader source, Writer dest, int tz)
            throws IOException {
        if (tz < 1) {
            throw new IllegalArgumentException(
                "Buffer size must be 1 or greater: " + tz);
        }
        BufferedReader in = new BufferedReader(source);
        BufferedWriter out = new BufferedWriter(dest, tz);
        char[] bridge = new char[tz];
        int total = 0;
        int read;
        while ((read = in.read(bridge, 0, tz)) != -1) {
            out.write(bridge, 0, read);
            total += read;
        }
        in.close();
        // Don't close the stream, in case it will be written to again.
        out.flush();
        return total;
    }

    private int transferData(Reader source, int tz)
            throws IOException, SQLException {
        if (tz < 1) {
            throw new IllegalArgumentException(
                "Buffer size must be 1 or greater: " + tz);
        }
        BufferedReader in = new BufferedReader(source);
        char[] bridge = new char[tz];
        int total = 0;
        int read;
        while ((read = in.read(bridge, 0, tz)) != -1) {
            this.clob.setString(total +1L, String.copyValueOf(bridge, 0, read));
            total += read;
        }
        in.close();
        return total;
    }

    private void insertDataWithToken(String token, 
                                     long pre, long post, int mode)
            throws IOException, SQLException {
        int TRANSFER_BUFFER_SIZE = 4*1024; // Byte and char array size.
        long total = 0;
        switch (mode) {
            case SET_STRING: {
                Reader charIn = new LoopingAlphabetReader(pre);
                total += transferData(charIn, TRANSFER_BUFFER_SIZE);
                this.clob.setString(pre +1, token);
                total += token.length();
                charIn = new LoopingAlphabetReader(post);
                total += transferData(charIn, TRANSFER_BUFFER_SIZE);
                break;
            } case SET_ASCII_STREAM: {
                OutputStream asciiOut = this.clob.setAsciiStream(1L);
                InputStream asciiIn = new LoopingAlphabetStream(pre);
                total += transferData(asciiIn, asciiOut, TRANSFER_BUFFER_SIZE);
                byte[] tokenBytes = token.getBytes("ISO-8859-1");
                asciiOut.write(tokenBytes, 0, tokenBytes.length);
                total += tokenBytes.length;
                asciiIn = new LoopingAlphabetStream(post);
                total += transferData(asciiIn, asciiOut, TRANSFER_BUFFER_SIZE);
                break;
            } case SET_CHARACTER_STREAM: {
                Writer charOut = this.clob.setCharacterStream(1L);
                Reader charIn = new LoopingAlphabetReader(pre);
                total += transferData(charIn, charOut, TRANSFER_BUFFER_SIZE);
                charOut.write(token);
                total += token.length();
                charIn = new LoopingAlphabetReader(post);
                total += transferData(charIn, charOut, TRANSFER_BUFFER_SIZE);
                break;
            } default:
                throw new IllegalArgumentException(
                    "Unknown insertion mode: " + mode);
        }
        assertEquals("Invalid length after insertion",
            pre + post + token.length(), this.clob.length());
    }

    /**
     * Decorator creating the neccessary test data.
     */
    private static class ClobTestSetup extends BaseJDBCTestSetup {

        ClobTestSetup(Test test) {
            super(test);
        }

        protected void setUp() throws SQLException {
            Connection con = getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("create table ClobTestData (" +
                    "id int unique, dClob CLOB)");
            stmt.executeUpdate("insert into ClobTestData values (1, '')");
            stmt.close();
       }

        protected void tearDown()
                throws Exception {
            Connection con = getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("drop table ClobTestData");
            stmt.close();
            super.tearDown();
        }
    } // End inner class ClobTestSetup
}

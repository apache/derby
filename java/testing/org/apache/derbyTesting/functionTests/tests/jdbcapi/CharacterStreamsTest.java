/*
 *
 * Derby - Class CharacterStreamsTest
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
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the following PreparedStatement methods:
 *   -> setCharacterStream(int parameterIndex, InputStream x, int length)
 *   -> setAsciiStream(int parameterIndex, Reader reader, int length)
 */
public class CharacterStreamsTest extends BaseJDBCTestCase {

    public static Test suite() {
        // Run only in embedded mode until DERBY-2017 is fixed.
        return TestConfiguration.embeddedSuite(CharacterStreamsTest.class);
    }

    /** Creates a new instance of CharacterStreamsTest */
    public CharacterStreamsTest(String name) {
        super(name);
    }

    /**
     * Test PreparedStatement.setAsciiStream() with column of type CHAR
     */
    public void testSetAsciiStreamIntoChar() throws Exception {
        runTestSetAsciiStream(1);
    }

    /**
     * Test PreparedStatement.setAsciiStream() with column of type VARCHAR
     */
    public void testSetAsciiStreamIntoVarchar() throws Exception {
        runTestSetAsciiStream(2);
    }

    /**
     * Test PreparedStatement.setAsciiStream() with column of type LONG VARCHAR
     */
    public void testSetAsciiStreamIntoLongVarchar() throws Exception {
        runTestSetAsciiStream(3);
    }

    /**
     * Test PreparedStatement.setAsciiStream() with column of type CLOB
     */
    public void testSetAsciiStreamIntoClob() throws Exception {
        runTestSetAsciiStream(4);
    }

    /**
     * Test PreparedStatement.setCharacterStream() with column of type CHAR
     */
    public void testSetCharacterStreamIntoChar() throws Exception {
        runTestSetCharacterStream(1);
    }

    /**
     * Test PreparedStatement.setCharacterStream() with column of type VARCHAR
     */
    public void testSetCharacterStreamIntoVarchar() throws Exception {
        runTestSetCharacterStream(2);
    }

    /**
     * Test PreparedStatement.setCharacterStream() with column of type
     * LONG VARCHAR
     */
    public void testSetCharacterStreamIntoLongVarchar() throws Exception {
        runTestSetCharacterStream(3);
    }

    /**
     * Test PreparedStatement.setCharacterStream() with column of type CLOB
     */
    public void testSetCharacterStreamIntoClob() throws Exception {
        runTestSetCharacterStream(4);
    }

    /**
     * Test PreparedStatement.setAsciiStream() with streams with sizes from
     * 60characters to 32K characters
     */
    public void testSetAsciiStreamLongValues() throws Exception {
        runTestSetAsciiStreamLongValues();
    }

    /**
     * Test PreparedStatement.setCharacterStream() with streams with sizes from
     * 60 characters to 32K characters
     */
    public void testSetCharacterStreamLongValues() throws Exception {
        runTestSetCharacterStreamLongValues();
    }

    private void runTestSetAsciiStream(int col) throws Exception {
        PreparedStatement psi = prepareStatement(
                "insert into charstream(c, vc, lvc, lob) " +
                "values(?,?,?,?)");
        PreparedStatement psq = prepareStatement(
                "select id, c, {fn length(c)} AS CLEN, " +
                "cast (vc as varchar(25)) AS VC, " +
                "{fn length(vc)} AS VCLEN, " +
                "cast (lvc as varchar(25)) AS LVC, " +
                "{fn length(lvc)} AS LVCLEN, " +
                "cast (lob as varchar(25)) AS LOB, " +
                "{fn length(lob)} AS LOBLEN " +
                "from charstream " +
                "where id > ? order by 1");

        // test setAsciiStream into CHAR
        println("\nTest setAsciiStream into CHAR");
        psi.setString(1, null);
        psi.setString(2, null);
        psi.setString(3, null);
        psi.setString(4, null);
        int maxid = getMaxId();
        setAscii(psi, col);
        psq.setInt(1, maxid);
        verifyAsciiStreamResults(psq.executeQuery(), col);

        // Show results as various streams
        PreparedStatement psStreams = prepareStatement(
                "SELECT id, c, vc, lvc, lob " +
                "FROM charstream where id > ? order by 1");
        psStreams.setInt(1, maxid);
        verifyResultsUsingAsciiStream(psStreams.executeQuery(), col);
        verifyResultsUsingCharacterStream(psStreams.executeQuery(), col);
        verifyResultsUsingCharacterStreamBlock(psStreams.executeQuery(), col);
        psStreams.close();

        psi.close();
        psq.close();

    }
    private void runTestSetCharacterStream(int col) throws Exception {
        PreparedStatement psi = prepareStatement(
                "insert into charstream(c, vc, lvc, lob) " +
                "values(?,?,?,?)");
        PreparedStatement psq = prepareStatement(
                "select id, c, {fn length(c)} AS CLEN, " +
                "cast (vc as varchar(25)) AS VC, " +
                "{fn length(vc)} AS VCLEN, " +
                "cast (lvc as varchar(25)) AS LVC, " +
                "{fn length(lvc)} AS LVCLEN, " +
                "cast (lob as varchar(25)) AS LOB, " +
                "{fn length(lob)} AS LOBLEN " +
                "from charstream " +
                "where id > ? order by 1");

        // test setCharacterStream into CHAR
        println("\nTest setCharacterStream into CHAR");
        psi.setString(1, null);
        psi.setString(2, null);
        psi.setString(3, null);
        psi.setString(4, null);
        int maxid = getMaxId();
        setCharacter(psi, col);
        psq.setInt(1, maxid);
        verifyCharStreamResults(psq.executeQuery(), col);

        psi.close();
        psq.close();
    }

    private void runTestSetAsciiStreamLongValues() throws Exception {
        // now insert long values using streams and check them programatically.
        PreparedStatement psi = prepareStatement(
                "insert into charstream(c, vc, lvc, lob) " +
                "values(?,?,?,?)");
        PreparedStatement psDel = prepareStatement("DELETE FROM charstream");
        PreparedStatement psq2 =
                prepareStatement("select c, vc, lvc, lob from charstream");

        // now insert long values using streams and check them programatically.
        println("setAsciiStream(LONG ASCII STREAMS)");
        checkAsciiStreams(psDel, psi, psq2, 18, 104, 67, 67);
        checkAsciiStreams(psDel, psi, psq2, 25, 16732, 14563, 14563);
        checkAsciiStreams(psDel, psi, psq2, 1, 32433, 32673, 32673);
        checkAsciiStreams(psDel, psi, psq2, 0, 32532, 32700, 32700);

        psi.close();
        psDel.close();
        psq2.close();
    }

    private void runTestSetCharacterStreamLongValues() throws Exception {
        // now insert long values using streams and check them programatically.
        PreparedStatement psi = prepareStatement(
                "insert into charstream(c, vc, lvc, lob) " +
                "values(?,?,?,?)");
        PreparedStatement psDel = prepareStatement("DELETE FROM charstream");
        PreparedStatement psq2 =
                prepareStatement("select c, vc, lvc, lob from charstream");

        println("setCharacterStream(LONG CHARACTER STREAMS WITH UNICODE)");
        checkCharacterStreams(psDel, psi, psq2, 14, 93, 55, 55);
        checkCharacterStreams(psDel, psi, psq2, 25, 19332, 18733, 18733);
        checkCharacterStreams(psDel, psi, psq2, 1, 32433, 32673, 32673);
        checkCharacterStreams(psDel, psi, psq2, 0, 32532, 32700, 32700);

        psi.close();
        psDel.close();
        psq2.close();
    }

    private int getMaxId() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select max(id) from charstream");
        rs.next();
        int maxid = rs.getInt(1);
        rs.close();
        stmt.close();
        return maxid;
    }

    private void setAscii(PreparedStatement ps, int targetCol)
    throws Exception {
        byte[] asciiBytes = null;
        // correct byte count
        println("CORRECT NUMBER OF BYTES IN STREAM");
        asciiBytes = ASCII_VALUE.getBytes("US-ASCII");
        ps.setAsciiStream(targetCol,
                new ByteArrayInputStream(asciiBytes), LEN_ASCII_VALUE);
        ps.executeUpdate();

        // less bytes than stream contains. JDBC 3.0 indicates it should throw
        // an exception (in Tutorial & reference book)
        println("MORE BYTES IN STREAM THAN PASSED IN VALUE");
        try {
            asciiBytes = "against Republicans George W. Bush ".
                    getBytes("US-ASCII");
            ps.setAsciiStream(targetCol,
                    new ByteArrayInputStream(asciiBytes), 19);
            ps.executeUpdate();
            fail("FAIL - MORE BYTES IN ASCII STREAM THAN SPECIFIED LENGTH");
        } catch (SQLException sqle) {
            assertSQLState("XJ001", sqle);
        }

        // more bytes than the stream contains JDBC 3.0 changed to indicate an
        // exception should be thrown. (in Tutorial & reference book)
        println("LESS BYTES IN STREAM THAN PASSED IN VALUE");
        try {
            asciiBytes = "and Dick Cheney.".getBytes("US-ASCII");
            ps.setAsciiStream(targetCol,
                    new ByteArrayInputStream(asciiBytes), 17);
            ps.executeUpdate();
            fail("FAIL - LESS BYTES IN ASCII STREAM THAN SPECIFIED LENGTH");
        } catch (SQLException sqle) {
            assertSQLState("XJ001", sqle);
        }

        // null
        println("NULL ASCII STREAM");
        ps.setAsciiStream(targetCol, null, 1);
        ps.executeUpdate();

    }

    private void setCharacter(PreparedStatement ps, int targetCol)
    throws Exception {
        Reader reader = null;

        // correct character count
        reader = new StringReader(CHAR_VALUE1);
        ps.setCharacterStream(targetCol, reader, LEN_CHAR_VALUE1);
        ps.executeUpdate();

        reader = new StringReader(CHAR_VALUE2);
        ps.setCharacterStream(targetCol, reader, LEN_CHAR_VALUE2);
        ps.executeUpdate();

        // less bytes than stream contains.
        try {
            reader = new StringReader("for comments he made at");
            ps.setCharacterStream(targetCol, reader, 20);
            ps.executeUpdate();
            fail("FAIL - MORE CHARACTERS IN READER THAN SPECIFIED LENGTH");
        } catch (SQLException sqle) {
            assertSQLState("XJ001", sqle);
        }

        // more bytes than the stream contains,
        // JDBC 3.0 changed to indicate an exception should be thrown.
        try {
            reader = new StringReader("a birthday party");
            ps.setCharacterStream(targetCol, reader, 17);
            ps.executeUpdate();
            fail("FAIL - LESS CHARACTERS IN READER THAN SPECIFIED LENGTH");
        } catch (SQLException sqle) {
            assertSQLState("XJ001", sqle);
        }

        // null
        ps.setCharacterStream(targetCol, null, 1);
        ps.executeUpdate();
    }

    private void verifyAsciiStreamResults(ResultSet rs, int col)
            throws Exception
    {
        String value;
        int length;

        // First row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col * 2);
        assertFalse("FAIL - value should not be null", rs.wasNull());
        length = rs.getInt((col * 2) + 1);
        assertFalse("FAIL - length should not be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col,
                ASCII_VALUE, value.trim());
        assertEquals("FAIL - wrong length " + col, LEN_ASCII_VALUE, length);

        // null row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col * 2);
        assertTrue("FAIL - value should be null", rs.wasNull());
        length = rs.getInt((col * 2) + 1);
        assertTrue("FAIL - length should be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col, null, value);
        assertEquals("FAIL - wrong length " + col, 0, length);

        assertFalse("FAIL - more rows than expected", rs.next());
    }

    private void verifyCharStreamResults(ResultSet rs, int col)
    throws Exception {
        String value;
        int length;

        // First row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col * 2);
        assertFalse("FAIL - value should not be null", rs.wasNull());
        length = rs.getInt((col * 2) + 1);
        assertFalse("FAIL - length should not be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col,
                CHAR_VALUE1, value.trim());
        assertEquals("FAIL - wrong length " + col, LEN_CHAR_VALUE1, length);

        // Second row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col * 2);
        assertFalse("FAIL - value should not be null", rs.wasNull());
        length = rs.getInt((col * 2) + 1);
        assertFalse("FAIL - length should not be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col,
                CHAR_VALUE2, value.trim());
        assertEquals("FAIL - wrong length " + col, LEN_CHAR_VALUE2, length);

        // null row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col * 2);
        assertTrue("FAIL - value should be null", rs.wasNull());
        length = rs.getInt((col * 2) + 1);
        assertTrue("FAIL - length should be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col, null, value);
        assertEquals("FAIL - wrong length " + col, 0, length);

        assertFalse("FAIL - more rows than expected", rs.next());
        rs.close();
    }

    private void verifyResultsUsingAsciiStream(ResultSet rs, int col)
            throws Exception
    {
        InputStream valueStream;
        String value;

        // First row
        assertTrue("FAIL - row not found", rs.next());
        valueStream = rs.getAsciiStream(col + 1);
        assertFalse("FAIL - value should not be null", rs.wasNull());

        byte[] valueBytes = new byte[LEN_ASCII_VALUE];
        assertEquals("FAIL - wrong length read from stream", LEN_ASCII_VALUE,
                valueStream.read(valueBytes));
        assertEquals("FAIL - wrong value on column " + col,
                ASCII_VALUE, new String(valueBytes, "US-ASCII"));

        // null row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col + 1);
        assertTrue("FAIL - value should be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col, null, value);

        assertFalse("FAIL - more rows than expected", rs.next());
        rs.close();
    }

    private void verifyResultsUsingCharacterStream(ResultSet rs, int col)
            throws Exception
    {
        Reader valueReader;
        String value;

        // First row
        assertTrue("FAIL - row not found", rs.next());
        // Read characters one by one
        valueReader = rs.getCharacterStream(col + 1);
        StringBuffer sb = new StringBuffer();
        int c = 0;
        while ((c = valueReader.read()) != -1) {
            sb.append((char)c);
        }
        value = sb.toString().trim();
        assertEquals("FAIL - wrong length read from stream", LEN_ASCII_VALUE,
                value.length());
        assertEquals("FAIL - wrong value on column " + col,
                ASCII_VALUE, value);

        // null row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col + 1);
        assertTrue("FAIL - value should be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col, null, value);

        assertFalse("FAIL - more rows than expected", rs.next());
        rs.close();
    }

    private void verifyResultsUsingCharacterStreamBlock(ResultSet rs, int col)
            throws Exception
    {
        Reader valueReader;
        String value;

        // First row
        assertTrue("FAIL - row not found", rs.next());
        valueReader = rs.getCharacterStream(col + 1);
        assertFalse("FAIL - value should not be null", rs.wasNull());
        // Read all characters in a block
        char[] valueChars = new char[LEN_ASCII_VALUE];
        assertEquals("FAIL - wrong length read from stream", LEN_ASCII_VALUE,
                valueReader.read(valueChars));
        assertEquals("FAIL - wrong value on column " + col,
                ASCII_VALUE, new String(valueChars));

        // null row
        assertTrue("FAIL - row not found", rs.next());
        value = rs.getString(col + 1);
        assertTrue("FAIL - value should be null", rs.wasNull());

        assertEquals("FAIL - wrong value on column " + col, null, value);

        assertFalse("FAIL - more rows than expected", rs.next());
        rs.close();
    }

    private void checkAsciiStreams(
            PreparedStatement psDel,
            PreparedStatement psi,
            PreparedStatement psq2,
            int cl, int vcl, int lvcl, int lob)
            throws SQLException, IOException {

        psDel.executeUpdate();

        // now insert long values using streams and check them programatically.
        psi.setAsciiStream(1, new c3AsciiStream(cl), cl);
        psi.setAsciiStream(2, new c3AsciiStream(vcl), vcl);
        psi.setAsciiStream(3, new c3AsciiStream(lvcl), lvcl);
        psi.setAsciiStream(4, new c3AsciiStream(lob), lob);
        psi.executeUpdate();

        ResultSet rs = psq2.executeQuery();
        rs.next();

        InputStream is = rs.getAsciiStream(1);
        checkAsciiStream(is, cl, 25);

        is = rs.getAsciiStream(2);
        checkAsciiStream(is, vcl, -1);

        is = rs.getAsciiStream(3);
        checkAsciiStream(is, lvcl, -1);

        is = rs.getAsciiStream(4);
        checkAsciiStream(is, lob, -1);

        rs.close();

        rs = psq2.executeQuery();
        rs.next();

        Reader r = rs.getCharacterStream(1);
        checkAsciiStream(r, cl, 25);

        r = rs.getCharacterStream(2);
        checkAsciiStream(r, vcl, -1);

        r = rs.getCharacterStream(3);
        checkAsciiStream(r, lvcl, -1);

        r = rs.getCharacterStream(4);
        checkAsciiStream(r, lob, -1);

        rs.close();

        // and check as Strings
        rs = psq2.executeQuery();
        rs.next();

        r = new StringReader(rs.getString(1));
        checkAsciiStream(r, cl, 25);

        r = new StringReader(rs.getString(2));
        checkAsciiStream(r, vcl, -1);

        r = new StringReader(rs.getString(3));
        checkAsciiStream(r, lvcl, -1);

        r = new StringReader(rs.getString(4));
        checkAsciiStream(r, lob, -1);

        rs.close();
    }

    private void checkCharacterStreams(
            PreparedStatement psDel,
            PreparedStatement psi,
            PreparedStatement psq2,
            int cl, int vcl, int lvcl, int lob)
            throws SQLException, IOException {
        psDel.executeUpdate();

        psi.setCharacterStream(1, new c3Reader(cl), cl);
        psi.setCharacterStream(2, new c3Reader(vcl), vcl);
        psi.setCharacterStream(3, new c3Reader(lvcl), lvcl);
        psi.setCharacterStream(4, new c3Reader(lob), lob);
        psi.executeUpdate();

        ResultSet rs = psq2.executeQuery();
        rs.next();

        InputStream is = rs.getAsciiStream(1);
        checkCharStream(is, cl, 25);

        is = rs.getAsciiStream(2);
        checkCharStream(is, vcl, -1);

        is = rs.getAsciiStream(3);
        checkCharStream(is, lvcl, -1);

        is = rs.getAsciiStream(4);
        checkCharStream(is, lob, -1);

        rs.close();

        rs = psq2.executeQuery();
        rs.next();

        Reader r = rs.getCharacterStream(1);
        checkCharStream(r, cl, 25);

        r = rs.getCharacterStream(2);
        checkCharStream(r, vcl, -1);

        r = rs.getCharacterStream(3);
        checkCharStream(r, lvcl, -1);

        r = rs.getCharacterStream(4);
        checkCharStream(r, lob, -1);

        rs.close();

        // check converting them into Strings work
        rs = psq2.executeQuery();
        rs.next();

        String suv = rs.getString(1);
        r = new StringReader(suv);
        checkCharStream(r, cl, 25);

        suv = rs.getString(2);
        r = new StringReader(suv);
        checkCharStream(r, vcl, -1);

        suv = rs.getString(3);
        r = new StringReader(suv);
        checkCharStream(r, lvcl, -1);

        suv = rs.getString(4);
        r = new StringReader(suv);
        checkCharStream(r, lob, -1);

        rs.close();

    }

    private void checkAsciiStream(InputStream is, int length, int fixedLen)
            throws IOException
    {

        InputStream orig = new c3AsciiStream(length);

        int count = 0;
        for (;;) {

            int o = orig == null ?
                (count == fixedLen ? -2 : 0x20) : orig.read();
            int c = is.read();
            if (o == -1) {
                orig = null;
                if (fixedLen != -1 && fixedLen != length)
                    o = ' ';
            }
            if (o == -2)
                o = -1;

            assertEquals("FAIL - wrong value at position " + count, o, c);
            if (orig == null) {
                if (fixedLen == -1)
                    break;
            }

            if (c == -1 && fixedLen != -1)
                break;

            count++;
        }
        if (fixedLen != -1)
            length = fixedLen;

        assertEquals("FAIL - wrong length", length, count);
        is.close();
    }

    private void checkAsciiStream(Reader r, int length, int fixedLen)
            throws IOException
    {

        InputStream orig = new c3AsciiStream(length);

        int count = 0;
        for (;;) {

            int o = orig == null ?
                (count == fixedLen ? -2 : 0x20) : orig.read();
            int c = r.read();
            if (o == -1) {
                orig = null;
                if (fixedLen != -1 && fixedLen != length)
                    o = ' ';
            }
            if (o == -2)
                o = -1;

            assertEquals("FAIL - wrong value", o, c);
            if (orig == null) {
                if (fixedLen == -1)
                    break;
            }

            if (c == -1 && fixedLen != -1)
                break;

            count++;
        }
        if (fixedLen != -1)
            length = fixedLen;

        assertEquals("FAIL - wrong length", length, count);
        r.close();
    }

    private void checkCharStream(InputStream is, int length, int fixedLen)
            throws IOException
    {

        Reader orig = new c3Reader(length);

        int count = 0;
        for (;;) {

            int o = orig == null ?
                (count == fixedLen ? -2 : 0x20) : orig.read();
            int c = is.read();
            if (o == -1) {
                orig = null;
                if (fixedLen != -1 && fixedLen != length)
                    o = ' ';
            }
            if (o == -2)
                o = -1;

            if (o != -1) {
                if (o <= 255)
                    o = o & 0xFF; // convert to single byte extended ASCII
                else
                    o = '?'; // out of range character.
            }

            assertEquals("FAIL - wrong value", o, c);
            if (orig == null) {
                if (fixedLen == -1)
                    break;
            }

            if (c == -1 && fixedLen != -1)
                break;

            count++;
        }
        if (fixedLen != -1)
            length = fixedLen;

        assertEquals("FAIL - wrong length", length, count);
        is.close();
    }

    private void checkCharStream(Reader r, int length, int fixedLen)
            throws IOException
    {

        Reader orig = new c3Reader(length);

        int count = 0;
        for (;;) {

            int o = (orig == null) ?
                (count == fixedLen ? -2 : 0x20) : orig.read();
            int c = r.read();
            if (o == -1) {
                orig = null;
                if (fixedLen != -1 && fixedLen != length)
                    o = ' ';
            }
            if (o == -2)
                o = -1;

            assertEquals("FAIL - wrong value", o, c);
            if (orig == null) {
                if (fixedLen == -1)
                    break;
            }

            if (c == -1 && fixedLen != -1)
                break;

            count++;
        }
        if (fixedLen != -1)
            length = fixedLen;

        assertEquals("FAIL - wrong length", length, count);
        r.close();
    }


    protected void setUp() throws Exception {
        createStatement().executeUpdate(
                "create table charstream(" +
                "id int GENERATED ALWAYS AS IDENTITY primary key, " +
                "c char(25), " +
                "vc varchar(32532), " +
                "lvc long varchar, " +
                "lob clob(300K))");
    }

    protected void tearDown() throws Exception {
        rollback();
        createStatement().executeUpdate("DROP TABLE charstream");
        commit();
        super.tearDown();
    }

    private final static String ASCII_VALUE = "Lieberman ran with Gore";
    private final static int LEN_ASCII_VALUE = 23;

    private final static String CHAR_VALUE1 = "A Mississippi Republican";
    private final static int LEN_CHAR_VALUE1 = 24;

    private final static String CHAR_VALUE2 = "Lott has apologized";
    private final static int LEN_CHAR_VALUE2 = 19;

}


class c3AsciiStream extends InputStream {

    private final int size;
    private int count;
    c3AsciiStream(int size) {
        this.size = size;
    }
    public int read(byte[] buf, int off, int length) {
        if (count >= size)
            return -1;

        if (length > (size - count))
            length = (size - count);

        // ensure the readers don't always get a full buffer,
        // makes sure they are not assuming the buffer will be filled.

        if (length > 20)
            length -= 17;

        for (int i = 0; i < length ; i++) {
            buf[off + i] = (byte) count++;
        }

        return length;
    }

    private byte[] rd = new byte[1];
    public int read() {

        int read = read(rd, 0, 1);
        if (read == -1)
            return -1;
        return rd[0] & 0xFF;
    }

    public void close() {
    }
}

class c3Reader extends Reader {

    private final int size;
    private int count;
    c3Reader(int size) {
        this.size = size;
    }
    public int read(char[] buf, int off, int length) {
        if (count >= size)
            return -1;

        if (length > (size - count))
            length = (size - count);

        // ensure the readers don't always get a full buffer,
        // makes sure they are not assuming the buffer will be filled.

        if (length > 20)
            length -= 17;

        for (int i = 0; i < length ; i++) {
            char c;
            switch (count % 3) {
                case 0:
                    c = (char) (count & 0x7F); // one byte UTF8
                    break;
                case 1:
                    c = (char) ((count + 0x7F) & 0x07FF); // two byte UTF8
                    break;
                default:
                case 2:
                    c = (char) (count + 0x07FF); // three byte UTF8
                    break;

            }
            buf[off + i] = c;
            count++;
        }
        return length;
    }

    public void close() {
    }
}

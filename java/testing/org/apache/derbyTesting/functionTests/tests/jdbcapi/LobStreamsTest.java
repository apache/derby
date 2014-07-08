/*
 *
 * Derby - Class LobStreamsTest
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class LobStreamsTest extends BaseJDBCTestCase {

    int[] streamSize = new int[2];
    private final String unicodeTestString =
            "This is a test string containing a few non-ascii characters:\n " +
            "\u00E6\u00F8\u00E5 and \u00C6\u00D8\u00C5 are used in " +
            "norwegian: 'Bl\u00E5b\u00E6rsyltet\u00F8y' means 'blueberry jam'" +
            ", and tastes great on pancakes. =)";

    {
        streamSize[0] =  300000;
        streamSize[1] =  10000;
    }

    /** Creates a new instance of LobStreamsTest */
    public LobStreamsTest(String name) {
        super(name);
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws Exception {
        getConnection().setAutoCommit(false);

        Statement stmt1 = createStatement();
        stmt1.execute("create table testBlobX1 (" +
                "a integer, " +
                "b blob(300K), " +
                "c clob(300K))");
        stmt1.close();

        byte[] b2 = new byte[1];
        b2[0] = (byte)64;
        String c2 = "c";
        PreparedStatement stmt2 = prepareStatement(
                "INSERT INTO testBlobX1(a, b, c) " +
                "VALUES (?, ?, ?)");
        stmt2.setInt(1, 1);
        stmt2.setBytes(2,  b2);
        stmt2.setString(3,  c2);
        stmt2.execute();
        stmt2.close();
    }

    /**
     * Originally tested that the usage pattern {@code rs.getBlob().method()}
     * didn't cause the underlying source stream to be closed too early. This
     * behavior was forbidden, the test now checks that an exception is thrown.
     * <p>
     * Test description: Select from a BLOB column, access the BLOB using the
     * pattern rs.getBlob(1).blobMethod() (note that we do not keep a reference
     * to the Blob-object), provoke/invoke GC and finalization, and finally try
     * to access the same BLOB again (through a different/another call to
     * rs.getBlob(1)).
     * <p>
     * Note that the BLOB must be of a certain size (i.e. multiple pages), such
     * that it is stored/accessed as a stream in store.
     * <p>
     * See DERBY-3844 and DERBY-4440.
     *
     * @throws Exception if something goes wrong
     */
    public void testGettingBlobTwice()
            throws Exception {
        setAutoCommit(false);
        // We need a Blob represented as a stream in store.
        int length = 71*1024+7;
        PreparedStatement ps =
                prepareStatement("insert into testBlobX1(a,b) values (?,?)");
        ps.setInt(1, 2);
        ps.setBinaryStream(2, new LoopingAlphabetStream(length), length);
        ps.executeUpdate();
        ps.close();

        // Get a result set with the Blob.
        ps = prepareStatement("select b from testBlobX1 where a = ?");
        ps.setInt(1, 2);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        Blob b = rs.getBlob(1);
        try {
            // Get the length, but don't keep a reference to the Blob.
            assertEquals(length, rs.getBlob(1).length());
            fail("Getting the Blob the second time should have failed");
        } catch (SQLException sqle) {
            assertSQLState("XCL18", sqle);
        }

        // Increase the likelyhood of getting the finalizer run.
        // Create some junk to fill up the heap, hopefully not JIT'ed away...
        int size = 10*1024; // 10 K
        byte[] bytes = null;
        for (int i=0; i < 50; i++) {
            bytes = new byte[size *(i +1)];
        }
        // For good measure...
        System.gc();
        System.runFinalization();
        try {
            Thread.sleep(100L);
        } catch (InterruptedException ie) {
            // No need to reset the interrupted flag here in the test.
        }

        // This will fail if the finalizer caused the source stream to be
        // closed and the source page to be unlatched.
        InputStream is = b.getBinaryStream();
        while (is.read() != -1) {
            // Keep on reading...
        }
        assertNotNull(bytes);
    }

    /**
     * Tests that accessing the same Clob multiple times on a row results in
     * an exception being thrown.
     *
     * @throws Exception if something goes wrong
     */
    public void testGettingClobTwice()
            throws SQLException {
        // We need a few Clobs.
        int length = 71*1024+7;
        PreparedStatement ps =
                prepareStatement("insert into testBlobX1(a,c) values (?,?)");
        ps.setInt(1, 3);
        ps.setCharacterStream(2, new LoopingAlphabetReader(length), length);
        ps.executeUpdate();
        ps.setInt(1, 4);
        ps.setString(2, "short clob");
        ps.executeUpdate();
        ps.close();

        // Get a result set with the Clobs.
        final int clobCount = 2;
        int count = 0;
        ps = prepareStatement(
                "select c from testBlobX1 where a >= ? and a <= ?");
        ps.setInt(1, 3);
        ps.setInt(2, 4);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        do {
            count++;
            // First get a Clob.
            Clob c = rs.getClob(1);
            // Get a second Clob, which should fail.
            try {
                rs.getClob(1);
                fail("Getting the Clob the second time should have failed");
            } catch (SQLException sqle) {
                assertSQLState("XCL18", sqle);
            }
            // Finally try to access the column as a stream.
            try {
                rs.getCharacterStream(1);
                fail("Getting the Clob the third time should have failed");
            } catch (SQLException sqle) {
                assertSQLState("XCL18", sqle);
            }
        } while (rs.next());
        rs.close();
        assertEquals(clobCount, count);
    }

    /**
     * Tests the BlobOutputStream.write(byte  b[], int off, int len) method
     **/
    public void testBlobWrite3Param() throws Exception {
        InputStream streamIn = new LoopingAlphabetStream(streamSize[0]);
        assertTrue("FAIL -- file not found", streamIn != null);

        PreparedStatement stmt3 = prepareStatement(
            "SELECT b FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Blob blob = rs3.getBlob(1);

        assertTrue ("FAIL -- blob is NULL", (blob != null));

        int count = 0;
        byte[] buffer = new byte[1024];
        OutputStream outstream = blob.setBinaryStream(1L);
        while ((count = streamIn.read(buffer)) != -1) {
            outstream.write(buffer, 0, count);
        }
        outstream.close();
        streamIn.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET b = ? WHERE a = 1");
        stmt4.setBlob(1,  blob);
        stmt4.executeUpdate();
        stmt4.close();
        rs3.close();

        rs3 = stmt3.executeQuery();
        assertTrue("FAIL -- blob not found", rs3.next());

        blob = rs3.getBlob(1);
        long new_length = blob.length();
        assertEquals("FAIL -- wrong blob length;",
                streamSize[0], new_length);

        // Check contents ...
        InputStream fStream = new LoopingAlphabetStream(streamSize[0]);
        InputStream lStream = blob.getBinaryStream();
        assertTrue("FAIL - Blob and file contents do not match",
                compareLob2File(fStream, lStream));

        fStream.close();
        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the BlobOutputStream.write(int b) method
     **/
    public void testBlobWrite1Param() throws Exception {
        InputStream streamIn = new LoopingAlphabetStream(streamSize[1]);

        PreparedStatement stmt3 = prepareStatement(
            "SELECT b FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Blob blob = rs3.getBlob(1);

        assertTrue("FAIL -- blob is NULL", blob != null);

        int buffer;
        OutputStream outstream = blob.setBinaryStream(1L);
        while ((buffer = streamIn.read()) != -1) {
            outstream.write(buffer);
        }
        outstream.close();
        streamIn.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET b = ? WHERE a = 1");
        stmt4.setBlob(1,  blob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();

        assertTrue("FAIL -- blob not found", rs3.next());

        blob = rs3.getBlob(1);
        long new_length = blob.length();
        assertEquals("FAIL -- wrong blob length", streamSize[1], new_length);

        // Check contents ...
        InputStream fStream = new LoopingAlphabetStream(streamSize[1]);
        InputStream lStream = blob.getBinaryStream();
        assertTrue("FAIL - Blob and file contents do not match",
                compareLob2File(fStream, lStream));

        fStream.close();
        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobOutputStream.write(int b) method
     **/
    public void testClobAsciiWrite1Param() throws Exception
    {
        InputStream streamIn = new LoopingAlphabetStream(streamSize[1]);

        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);

        assertTrue("FAIL -- clob is NULL", clob != null);
        int buffer;
        OutputStream outstream = clob.setAsciiStream(1L);
        while ((buffer = streamIn.read()) != -1) {
            outstream.write(buffer);
        }
        outstream.close();
        streamIn.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();
        assertTrue("FAIL -- clob not found", rs3.next());

        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length", streamSize[1], new_length);

        // Check contents ...
        InputStream fStream = new LoopingAlphabetStream(streamSize[1]);
        InputStream lStream = clob.getAsciiStream();
        assertTrue("FAIL - Clob and file contents do not match", compareLob2File(fStream, lStream));
        fStream.close();
        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobOutputStream.write(byte  b[], int off, int len) method
     **/
    public void testClobAsciiWrite3Param() throws Exception {
        InputStream streamIn = new LoopingAlphabetStream(streamSize[0]);
        assertTrue("FAIL -- file not found", streamIn != null);

        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);

        assertTrue("FAIL -- clob is NULL", clob != null);

        int count = 0;
        byte[] buffer = new byte[1024];
        OutputStream outstream = clob.setAsciiStream(1L);
        while ((count = streamIn.read(buffer)) != -1) {
            outstream.write(buffer, 0, count);
        }
        outstream.close();
        streamIn.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();

        assertTrue("FAIL -- clob not found", rs3.next());

        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length",
                streamSize[0], new_length);
        // Check contents ...
        InputStream fStream = new LoopingAlphabetStream(streamSize[0]);
        InputStream lStream = clob.getAsciiStream();
        assertTrue("FAIL - Clob and file contents do not match",
                compareLob2File(fStream, lStream));

        fStream.close();
        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobWriter.write(char cbuf[], int off, int len) method
     **/
    public void testClobCharacterWrite3ParamChar() throws Exception
    {
        char[] testdata = unicodeTestString.toCharArray();

        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);
        assertTrue("FAIL -- clob is NULL", clob != null);
        Writer clobWriter = clob.setCharacterStream(1L);
        clobWriter.write(testdata, 0, testdata.length);
        clobWriter.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();

        assertTrue("FAIL -- clob not found", rs3.next());
        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length",
                testdata.length, new_length);

        // Check contents ...
        Reader lStream = clob.getCharacterStream();
        assertTrue("FAIL - Clob and buffer contents do not match",
                compareClobReader2CharArray(testdata, lStream));

        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobWriter.write(String str, int off, int len) method
     **/
    public void testClobCharacterWrite3ParamString() throws Exception
    {
        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);
        assertTrue("FAIL -- clob is NULL", clob != null);
        Writer clobWriter = clob.setCharacterStream(1L);
        clobWriter.write(unicodeTestString, 0, unicodeTestString.length());
        clobWriter.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();
        assertTrue("FAIL -- clob not found", rs3.next());

        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length", unicodeTestString.length(), new_length);

        // Check contents ...
        Reader lStream = clob.getCharacterStream();
        assertTrue("FAIL - Clob and buffer contents do not match",
                compareClobReader2CharArray(
                    unicodeTestString.toCharArray(),
                    lStream));

        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobWriter.write(String str) method
     **/
    public void testClobCharacterWrite1ParamString() throws Exception
    {
        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);
        assertTrue("FAIL -- clob is NULL", clob != null);
        Writer clobWriter = clob.setCharacterStream(1L);
        clobWriter.write(unicodeTestString);
        clobWriter.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();
        assertTrue("FAIL -- clob not found", rs3.next());

        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length", unicodeTestString.length(), new_length);

        // Check contents ...
        Reader lStream = clob.getCharacterStream();
        assertTrue("FAIL - Clob and buffer contents do not match",
                compareClobReader2CharArray(
                    unicodeTestString.toCharArray(),
                    lStream));

        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Tests the ClobWriter.write(int c) method
     **/
    public void testClobCharacterWrite1Char() throws Exception
    {
        char testchar = 'a';

        PreparedStatement stmt3 = prepareStatement(
            "SELECT c FROM testBlobX1 WHERE a = 1");
        ResultSet rs3 = stmt3.executeQuery();
        rs3.next();
        Clob clob = rs3.getClob(1);

        assertTrue ("FAIL -- clob is NULL", clob != null);
        Writer clobWriter = clob.setCharacterStream(1L);
        clobWriter.write(testchar);
        clobWriter.close();

        PreparedStatement stmt4 = prepareStatement(
            "UPDATE testBlobX1 SET c = ? WHERE a = 1");
        stmt4.setClob(1,  clob);
        stmt4.executeUpdate();
        stmt4.close();

        rs3.close();
        rs3 = stmt3.executeQuery();
        assertTrue("FAIL -- clob not found", rs3.next());

        clob = rs3.getClob(1);
        long new_length = clob.length();
        assertEquals("FAIL -- wrong clob length", 1, new_length);
        // Check contents ...
        Reader lStream = clob.getCharacterStream();
        char clobchar = (char) lStream.read();
        assertEquals("FAIL - fetched Clob and original contents do " +
                "not match", testchar, clobchar);

        lStream.close();
        rs3.close();
        stmt3.close();
    }

    /**
     * Run with DerbyNetClient only.
     * Embedded Clob/Blob.setXXXStream() methods are not implemented.
     */
    public static Test suite() {
                
        BaseTestSuite ts  = new BaseTestSuite ("LobStreamsTest");
        ts.addTest(TestConfiguration.defaultSuite (LobStreamsTest.class));
        // JSR169 does not have support for encryption
        if (JDBC.vmSupportsJDBC3()) {
            BaseTestSuite encSuite =
                new BaseTestSuite ("LobStreamsTest:encrypted");
            encSuite.addTestSuite (LobStreamsTest.class);
            ts.addTest(Decorator.encryptedDatabase (encSuite));
        }
        return ts;
    }
    //method to ensure that buffer is filled if there is any data in stream
    private int readBytesFromStream (byte [] b, InputStream is) 
                                                          throws IOException {
        int read = 0;
        while (read < b.length) {
            int ret = is.read (b, read, b.length - read);
            if (ret < 0) {
                if (read == 0) {
                    return ret;
                }
                else {
                    break;
                }
            }
            read += ret;
        }
        return read;
    }

    private boolean compareLob2File(
            InputStream fStream,
            InputStream lStream) throws Exception
    {
        byte[] fByte = new byte[1024];
        byte[] lByte = new byte[1024];
        int lLength = 0, fLength = 0;
        String fString, lString;

        do {
            fLength = readBytesFromStream (fByte, fStream);
            lLength = readBytesFromStream (lByte, lStream);
            if (!java.util.Arrays.equals(fByte, lByte))
                return false;
        } while (fLength > 0 && lLength > 0);

        fStream.close();
        lStream.close();

        return true;
    }

    private boolean compareClobReader2CharArray(
            char[] cArray,
            Reader charReader) throws Exception
    {
        char[] clobChars = new char[cArray.length];

        int readChars = 0;
        int totalCharsRead = 0;

        do {
            readChars = charReader.read(clobChars, totalCharsRead, cArray.length - totalCharsRead);
            if (readChars != -1)
                totalCharsRead += readChars;
        } while (readChars != -1 && totalCharsRead < cArray.length);
        charReader.close();
        if (!java.util.Arrays.equals(cArray, clobChars))
            return false;

        return true;
    }

}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.LobStreamTest

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
package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.IOException;
import junit.framework.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.sql.Blob;
import java.sql.Connection;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;



public class LobStreamTest extends BaseJDBCTestCase {

    private InputStream in = null;
    private OutputStream out = null;
    private Blob blob;

    public LobStreamTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        Connection conn = getConnection();
        blob = conn.createBlob();
        in = blob.getBinaryStream();
        out = blob.setBinaryStream (1);
    }

    protected void tearDown() throws Exception {
        blob.free();
        blob = null;
        super.tearDown();
    }

    /**
     * Test cases
     */

    /**
     * Test read and write methods with no parameter.
     */
    public void testReadWriteNoParameters() throws IOException {

        for (int i=0; i<8000; i++) {
            out.write((i%255));
        }

        for (int i=0; i<8000; i++) {
            int value = in.read();
            assertEquals("Output does not match input", i%255, value);
        }

        in.close();
        in = null;
        out.close();
        out = null;
    }

    /**
     * test read method with no parameters and write method with one parameter
     */
    public void testReadNoParameterWriteOneParameter() throws IOException {
        byte[] b = new byte[100];
        for (int i=0; i<8000; i++) {
            b[i%100] = (byte) (((byte)(i%255)) & 0xFF);
            if (i%100 == 99) {
                out.write(b);
            }
        }

        for (int i=0; i<8000; i++) {
            int value = in.read();
            assertEquals("Output does not match input", i%255, value);
        }

        in.close();
        in = null;
        out.close();
        out = null;
    }

    /**
     * test read and write method with one parameter
     */
    public void testReadWriteOneParameter() throws IOException {

        byte[] b = new byte[100];
        for (int i=0; i<8000; i++) {
            b[i%100] = (byte) (((byte)(i%255)) & 0xFF);
            if (i%100 == 99) {
                out.write(b);
            }
        }

        for (int i=0; i<80; i++) {
            int count = in.read(b);
            for (int j=0; j<count; j++) {
                int value = b[j] & 0xFF;
                assertEquals("Output does not match input",
                                        (((i * 100) + j) % 255), value);
            }
        }

        in.close();
        in = null;
        out.close();
        out = null;
    }

    /**
     * test read and write method with three parameter
     */
    public void testReadWriteThreeParameter() throws IOException {

        byte[] b = new byte[200];
        int offset = 0;
        for (int i=0; i<8000; i++) {
            b[(i%100) + offset] = (byte) (((byte)(i%255)) & 0xFF);
            if (i%100 == 99) {
                out.write(b, offset, 100);
                offset += 1;
            }
        }

        offset = 0;
        for (int i=0; i<80; i++) {
            int count = in.read(b, offset, 100);
            for (int j=0; j<count; j++) {
                int value = b[j + offset] & 0xFF;
                assertEquals("Output does not match input",
                                        (((i * 100) + j) % 255), value);
            }
            offset += 1;
        }

        in.close();
        in = null;
        out.close();
        out = null;

    }

    /**
     * Test that stream returns -1 on end of stream when reading a byte at
     * a time.
     */
    public void testEndOfStreamValue() throws IOException {

        for (int i=0; i<8000; i++) {
            out.write((i%255));
        }

        int count = 0;
        while (in.read() != -1) {
            count++;
        }
        assertEquals("All values have been read", 8000, count);

        in.close();
        in = null;
        out.close();
        out = null;

    }

    /**
     * Test that read with one parameter returns the correct count
     * at end of stream.
     */
    public void testEndOfStreamOnReadOneParameter() throws IOException {

        for (int i=0; i<8050; i++) {
            out.write((i%255));
        }

        int count = 0, totalCount = 0;
        byte[] b = new byte[100];
        assertTrue("b.length should not be = 0", (b.length != 0));
        while ((count = in.read(b)) != -1) {
            assertTrue("Number of bytes read can not be = 0", (count != 0));
            totalCount += count;
        }
        assertEquals("All values have been read", 8050, totalCount);

        in.close();
        in = null;
        out.close();
        out = null;

    }

    /**
     * Test that read with three parameter returns the correct count
     * at end of stream.
     */
    public void testEndOfStreamOnReadThreeParameters() throws IOException {

        for (int i=0; i<8050; i++) {
            out.write((i%255));
        }

        int count = 0, totalCount = 0, offset = 0;
        byte[] b = new byte[200];
        assertTrue("b.length should not be = 0", (b.length != 0));
        while ((count = in.read(b, offset, 100)) != -1) {
            assertTrue("Number of bytes read can not be = 0", (count != 0));
            totalCount += count;
            offset++;
        }
        assertEquals("All values have been read", 8050, totalCount);

        in.close();
        in = null;
        out.close();
        out = null;
    }

    /**
     * Test the skip method of the input stream
     */
    public void testSkip() throws IOException {

        for (int i=0; i<8000; i++) {
            out.write((i%255));
        }

        int i = 0;
        while (i < 8000) {
            if ((i%255) < 100) {
                int value = in.read();
                assertEquals("Output does not match input", i%255, value);
                i++;
            } else {
                long count = in.skip(155);
                i += count;
            }
        }
        in.close();
        out.close();
        in = null;
        out = null;
    }

    /**
     * Test write method with three parameters with invalid parameter
     * values.
     */
     public void testWriteWithInvalidParameterValues() throws IOException {

        for (int i=0; i<8000; i++) {
            out.write((i%255));
        }

        // b is null
        byte[] b = null;
        try {
            out.write(b, 100, 20);
            fail("byte[] = null should cause exception");
        } catch (Exception e) {
            assertTrue("Expected NullPointerException",
                    e instanceof NullPointerException);
        }
        // length > b.length
        b = new byte[100];
        try {
            out.write(b, 0, 200);
            fail("length > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                    e instanceof IndexOutOfBoundsException);
        }

        // offset > b.length
        try {
            out.write(b, 150, 0);
            fail("offset > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                    e instanceof IndexOutOfBoundsException);
        }

        // offset + length > b.length
        try {
            out.write(b, 50, 100);
            fail("length + offset > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                    e instanceof IndexOutOfBoundsException);
        }

        // offset is negative
        try {
            out.write(b, -50, 100);
            fail("negative offset should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                        e instanceof IndexOutOfBoundsException);
        }

        //length is negative
        try {
            out.write(b, 0, -100);
            fail("negative length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                e instanceof IndexOutOfBoundsException);
        }

        // stream is closed
        out.close();
        try {
            out.write(b, 0, 100);
            fail("Stream should be closed");
        } catch (Exception e) {
            assertTrue("Expected IOException", e instanceof IOException);
        }

        out = null;
        in.close();
        in = null;
     }

    /**
     * Test write method with three parameters with invalid parameter
     * values.
     */
     public void testReadWithInvalidParameterValues() throws IOException {

        for (int i=0; i<8000; i++) {
            out.write((i%255));
        }
        out.close();
        out = null;

        // b is null
        byte[] b = null;
        try {
            in.read(b, 100, 20);
            fail("byte[] = null should cause exception");
        } catch (Exception e) {
            assertTrue("Expected NullPointerException",
                                        e instanceof NullPointerException);
        }

        // length > b.length
        b = new byte[100];
        try {
            int count = in.read(b, 0, 200);
            fail("length > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                    e instanceof IndexOutOfBoundsException);
        }

        // offset > b.length
        try {
            in.read(b, 150, 0);
            fail("offset > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                        e instanceof IndexOutOfBoundsException);
        }

        // offset + length > b.length
        try {
            int count = in.read(b, 50, 100);
            fail("offset + length > b.length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                    e instanceof IndexOutOfBoundsException);
        }

        // offset is negative
        try {
            in.read(b, -50, 100);
            fail("negative offset should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                        e instanceof IndexOutOfBoundsException);
        }

        //length is negative
        try {
            in.read(b, 0, -100);
            fail("negative length should cause exception");
        } catch (Exception e) {
            assertTrue("Expected IndexOutOfBoundException",
                                    e instanceof IndexOutOfBoundsException);
        }

        // stream is closed
        in.close();
        try {
            in.read(b, 0, 100);
            fail("Stream should be closed");
        } catch (Exception e) {
            assertTrue("Expected IOException", e instanceof IOException);
        }

        in = null;
     }

     /**
     * Suite method automatically generated by JUnit module.
     */
    public static Test suite() {
        //testing only embedded driver generic test suite testing both
        //client and ebedded is present in jdbcapi/LobStreamsTest
        TestSuite ts  = new TestSuite ("LobStreamTest");
        ts.addTest(TestConfiguration.embeddedSuite(LobStreamTest.class));
        TestSuite encSuite = new TestSuite ("LobStreamsTest:encrypted");
        encSuite.addTestSuite (LobStreamTest.class);
        ts.addTest(Decorator.encryptedDatabase (encSuite));
        return ts;
    }

}

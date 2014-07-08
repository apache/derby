/*

   Derby - Class org.apache.derby.impl.jdbc.InternalClobTest

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import junit.framework.TestCase;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derbyTesting.functionTests.util.streams.ByteAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

/**
 * A set of tests for the {@link org.apache.derby.impl.jdbc.InternalClob}
 * interface.
 * <p>
 * The tests are split into two categories; read-only and modifying tests. The
 * latter should only be run if @{link InternalClob#isWritable} is true.
 * <p>
 * <em>Implementation notes</em>: To implement a test by subclassing, a few
 * things must be done. First of all, many of the tests require that the number
 * of bytes per character is fixed. The following variables must be initialized
 * by the subclass:
 * <ul> <li>initialByteLength : The number of bytes in the Clob.
 *      <li>initialCharLength : The number of chars in the Clob.
 *      <li>bytesPerChar : The number of bytes used to represent each char.
 * </ul>
 */
public abstract class InternalClobTest
    extends BaseJDBCTestCase {

    /** Buffer used for reading/skipping from streams. */
    final static int BUFFER_SIZE = 4096;

    /**
     * The InternalClob used by the tests, the concrete implementation is
     * chosen by the subclasses.
     */
    protected InternalClob iClob = null;
    protected long initialByteLength = Long.MIN_VALUE;
    protected long initialCharLength = Long.MIN_VALUE;
    protected long bytesPerChar = Long.MIN_VALUE;
    protected long headerLength = Long.MIN_VALUE;

    InternalClobTest(String name) {
        super(name);
    }

    public void tearDown()
            throws Exception {
        super.tearDown();
    }

    protected static Test addModifyingTests(Class<? extends TestCase> theClass)
            throws Exception {
        BaseTestSuite suite =
            new BaseTestSuite("Modifying InternalClob test suite");
        Method[] methods = theClass.getMethods();
        List<String> testMethods = new ArrayList<String>();
        for (int i=0; i < methods.length; i++) {
            Method m = methods[i];
            if (m.getReturnType().equals(Void.TYPE) &&
                m.getName().startsWith("modTest") &&
                m.getParameterTypes().length == 0) {
                testMethods.add(m.getName());
            }
        }
        Constructor<? extends Test> ctor = theClass.getConstructor(new Class[] {String.class});
        for (int i=0; i < testMethods.size(); i++) {
            suite.addTest(ctor.newInstance(
                new Object[] {testMethods.get(i)}));
        }
        return suite;
    }

    /**
     * This test just ensures the initial variables are set in a sane way.
     * As can be seen, these tests require the number of bytes per character to
     * be fixed, thus only certain parts of Unicode can be used with these tests
     * when using UTF-8 as encoding.
     */
    public void testSanity() {
        assertEquals(initialByteLength,
                initialCharLength * bytesPerChar + headerLength);
        assertTrue(initialCharLength > 25);
    }

    /* All the XXXAfterRelease tests just checks that an exception is thrown if
     * the method is invoked after the Clob has been released. Note that an
     * UnsupportedOperationException is allowed for read-only Clobs.
     */

    public void testGetCharLengthAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.getCharLength();
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        }
    }

    public void testGetRawByteStreamAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.getRawByteStream();
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        }
    }

    public void testGetReaderAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.getReader(1L);
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        }
    }

    public void testReleaseAfterRelase()
            throws IOException, SQLException {
        iClob.release();
        // This one should be a no-op and not fail.
        iClob.release();
    }

    public void testGetWriterAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.getWriter(1L);
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        } catch (UnsupportedOperationException uoe) {
            assertFalse("Must support getWriter if the Clob is writable",
                iClob.isWritable());
        }
    }

    public void testInsertStringAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.insertString("What a nice sunny day :)", 1L);
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        } catch (UnsupportedOperationException uoe) {
            assertFalse("Must support insertString if the Clob is writable",
                iClob.isWritable());
        }
    }

    public void testTruncateAfterRelease()
            throws IOException, SQLException {
        iClob.release();
        try {
            iClob.truncate(1L);
            fail("Exception should have been raised, but was not");
        } catch (IllegalStateException ise) {
            // This is as expected.
        } catch (UnsupportedOperationException uoe) {
            assertFalse("Must support trucate if the Clob is writable",
                iClob.isWritable());
        }
    }

    /* End of XXXAfterRelease tests. */

    public void testGetCharLength()
            throws IOException, SQLException {
        assertEquals(this.initialCharLength, iClob.getCharLength());
    }

    public void testGetReaderAtStartPos()
            throws IOException, SQLException {
        Reader reader = iClob.getReader(1L);
        assertEquals(initialCharLength,
                     readFromStream(reader, initialCharLength));
        assertEquals(-1, reader.read());
        reader.close();
    }

    public void testGetReaderAtSecondPos()
            throws IOException, SQLException {
        Reader reader = iClob.getReader(2L);
        assertEquals(initialCharLength -1,
                     readFromStream(reader, initialCharLength -1));
        assertEquals(-1, reader.read());
        reader.close();
    }

    public void testGetReaderAtEndPos()
            throws IOException, SQLException {
        Reader reader = iClob.getReader(initialCharLength);
        assertTrue(reader.read() != -1);
        assertEquals(-1, reader.read());
        reader.close();
    }

    public void testGetReaderAfterEndPos()
            throws IOException, SQLException {
        Reader reader = iClob.getReader(initialCharLength +1);
        assertEquals(-1, reader.read());
        reader.close();
        try {
            reader = iClob.getReader(initialCharLength +767);
            fail("Got a reader at a position greater than the Clob");
        } catch (EOFException eofe) {
            // As expected
        }
    }

    public void modTestInsertString_append_small()
            throws IOException, SQLException {
        long cLength = iClob.getCharLength();
        iClob.insertString("END", cLength +1);
        assertEquals(cLength + 3, iClob.getCharLength());
        assertEquals("END", subString(iClob, cLength +1, 3));
    }

    /**
     * Replaces a piece of the Clob.
     */
    public void modTestInsertString_replace_small()
            throws IOException, SQLException {
        String replacement = "MIDDLE";
        iClob.insertString(replacement, 15L);
        assertEquals(initialCharLength, iClob.getCharLength());
        assertEquals(replacement,
                     this.subString(iClob, 15L, replacement.length()));
    }

    /**
     * Replaces the last part of the Clob and then adds a little more, all in
     * one operation.
     */
    public void modTestInsertString_replace_and_append_small()
            throws IOException, SQLException {
        String replacement = "REPLACING_AND_APPENDING!";
        assertTrue("Length of replacement text must be even",
            replacement.length() % 2 == 0);
        int halfLength = replacement.length() / 2;
        iClob.insertString(replacement,
            initialCharLength - halfLength +1);
        assertEquals("Wrong length after replace and append",
            initialCharLength + halfLength, iClob.getCharLength());
        assertEquals("Corresponding substring does not match replacement",
            replacement,
            this.subString(iClob, initialCharLength - halfLength +1,
                           replacement.length()));
    }

    /**
     * Extracts a substring from the Clob.
     *
     * @param clob the clob to extract from
     * @param pos the starting position in the Clob
     * @param count the number of characters to extract. Note that the actual
     *      number of characters extracted might be smaller if there are not
     *      enough characters in the Clob.
     * @return A substring up to <code>count</code> characters long.
     */
    protected static String subString(InternalClob clob, long pos, int count)
            throws IOException, SQLException {
        Reader reader = clob.getReader(pos);
        char[] sub = new char[count];
        int offset = 0;
        while (offset < count) {
            long read = reader.read(sub, offset, count - offset);
            if (read == -1) {
                break;
            }
            offset += read;
        }
        return String.copyValueOf(sub);
    }

    /**
     * Transfers data from the source to the destination.
     */
    public static long transferData(Reader src, Writer dest, long charsToCopy)
            throws IOException {
        BufferedReader in = new BufferedReader(src);
        BufferedWriter out = new BufferedWriter(dest, BUFFER_SIZE);
        char[] bridge = new char[BUFFER_SIZE];
        long charsLeft = charsToCopy;
        int read;
        while ((read = in.read(bridge, 0, (int)Math.min(charsLeft, BUFFER_SIZE))) > 0) {
            out.write(bridge, 0, read);
            charsLeft -= read;
        }
        in.close();
        // Don't close the stream, in case it will be written to again.
        out.flush();
        return charsToCopy - charsLeft;
    }

    /**
     * Attemps to read the specified number of characters from the stream.
     */
    public static final long readFromStream(Reader in, long characterCount)
            throws IOException {
        char[] buf = new char[BUFFER_SIZE];
        long leftToRead = characterCount;
        while (leftToRead > 0) {
            long read =
                in.read(buf, 0,(int)Math.min(leftToRead, (long)BUFFER_SIZE));
            if (read == 0) {
                break;
            }
            leftToRead -= read;
        }
        return characterCount - leftToRead;
    }

    /**
     * A fake store stream passed in to StoreStreamClob.
     * <p>
     * Note that it is made such that init must be called before using the
     * stream, or after close, or else a NPE will be thrown.
     */
    static class FakeStoreStream
        extends InputStream
        implements Resetable {

        private static final ByteAlphabet ALPHABET =
            ByteAlphabet.modernLatinLowercase();
        private LoopingAlphabetStream stream = null;
        private final long length;
        private int encodedLengthRemaining = 2;
        private int eofMarkerRemaining = 3;

        public FakeStoreStream(long length) {
            super();
            this.length = length;
        }

        public int read(byte[] b, int off, int len)
                throws IOException {
            int count = 0;
            while (count < len) {
                int ret = read();
                if (ret == -1) {
                    if (count == 0) {
                        // Inform about EOF.
                        return -1;
                    } else {
                        // Return what we got.
                        break;
                    }
                }
                b[off+count++] = (byte)ret;
            }
            return count;
        }

        public int read() throws IOException {
            if (this.encodedLengthRemaining > 0) {
                this.encodedLengthRemaining--;
                return 0;
            }
            int b = this.stream.read();
            if (b == -1 && this.eofMarkerRemaining > 0) {
                if (this.eofMarkerRemaining == 3) {
                    b = 0xe0;
                } else {
                    b = 0x00;
                }
                this.eofMarkerRemaining--;
            }
            return b;
        }

        public void resetStream() throws IOException, StandardException {
            this.stream = new LoopingAlphabetStream(length, ALPHABET);
            this.encodedLengthRemaining = 2;
            this.eofMarkerRemaining = 3;
        }

        public void initStream() throws StandardException {
            this.stream = new LoopingAlphabetStream(length, ALPHABET);
            this.encodedLengthRemaining = 2;
            this.eofMarkerRemaining = 3;
        }

        public void closeStream() {
            this.stream = null;
        }

    } // End private static class FakeStoreStream
} // End abstract class InternalClobTest

/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.ReaderToUTF8StreamTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.junit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Random;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.services.io.InputStreamUtil;
import org.apache.derby.iapi.types.ClobStreamHeaderGenerator;
import org.apache.derby.iapi.types.ReaderToUTF8Stream;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Unit tests for ReaderToUTF8Stream.
 * <p>
 * Explicit tests for the mark/reset feature start with "testMark".
 */
public class ReaderToUTF8StreamTest
        extends BaseTestCase {

    /**
     * The default size of the internal buffer in ReaderToUTF8Stream. Used to
     * trigger specific events in the reader.
     */
    private static int DEFAULT_INTERNAL_BUFFER_SIZE = 32*1024;

    public ReaderToUTF8StreamTest(String name) {
        super(name);
    }

    public static Test suite() {
        return new TestSuite(ReaderToUTF8StreamTest.class);
    }

    /**
     * Tests a very basic use of the mark/reset mechanism.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetSimplePosZero()
            throws IOException {
        InputStream is = getStream(100);
        is.mark(10);
        assertEquals(10, is.read(new byte[10]));
        is.reset();
        checkBeginningOfStream(is);
    }

    /**
     * Tests a very basic use of the mark/reset mechanism.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetSimplePosNonZero()
            throws IOException {
        InputStream is = getStream(200);
        assertEquals(127, is.read(new byte[127]));
        is.mark(10);
        byte[] readBeforeReset = new byte[10];
        byte[] readAfterReset = new byte[10];
        assertEquals(10, is.read(readBeforeReset));
        is.reset();
        assertEquals(10, is.read(readAfterReset));
        assertTrue(Arrays.equals(readBeforeReset, readAfterReset));
    }

    /**
     * Tests that shifting of existing bytes works.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetShiftBytesFew_Internal()
            throws IOException {
        InputStream is = getStream(128*1024);
        byte[] buf = new byte[DEFAULT_INTERNAL_BUFFER_SIZE - 2*1024];
        fillArray(is, buf);
        // The following mark fits within the existing default buffer, but the
        // bytes after the mark have to be shifted to the left.
        is.mark(4*1024);
        byte[] readBeforeReset = new byte[3*1024];
        byte[] readAfterReset = new byte[3*1024];
        fillArray(is, readBeforeReset);
        // Obtain something to compare with.
        InputStream src = getStream(128*1024);
        InputStreamUtil.skipFully(src, DEFAULT_INTERNAL_BUFFER_SIZE - 2*1024);
        byte[] comparisonRead = new byte[3*1024];
        fillArray(src, comparisonRead);
        // Compare
        assertEquals(new ByteArrayInputStream(comparisonRead),
                     new ByteArrayInputStream(readBeforeReset));
        // Reset the stream.
        is.reset();
        fillArray(is, readAfterReset);
        assertEquals(new ByteArrayInputStream(readBeforeReset),
                     new ByteArrayInputStream(readAfterReset));
    }

    /**
     * Tests that shifting of existing bytes works.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetShiftBytesMany_Internal()
            throws IOException {
        InputStream is = getStream(128*1024);
        is.read();
        is.read();
        // The following mark fits within the existing default buffer, but the
        // bytes after the mark have to be shifted to the left.
        is.mark(DEFAULT_INTERNAL_BUFFER_SIZE -6);
        byte[] readBeforeReset = new byte[DEFAULT_INTERNAL_BUFFER_SIZE -6];
        byte[] readAfterReset = new byte[DEFAULT_INTERNAL_BUFFER_SIZE -6];
        fillArray(is, readBeforeReset);
        // Obtain something to compare with.
        InputStream src = getStream(128*1024);
        src.read();
        src.read();
        byte[] comparisonRead = new byte[DEFAULT_INTERNAL_BUFFER_SIZE -6];
        fillArray(src, comparisonRead);
        // Compare
        assertEquals(new ByteArrayInputStream(comparisonRead),
                     new ByteArrayInputStream(readBeforeReset));
        // Reset the stream.
        is.reset();
        fillArray(is, readAfterReset);
        assertEquals(new ByteArrayInputStream(readBeforeReset),
                     new ByteArrayInputStream(readAfterReset));
    }

    /**
     * Tests an implementation specific feature of ReaderToUTF8Stream, which is
     * that the mark isn't invalidated even though we read past the read ahead
     * limit, given that the internal buffer doesn't have to be refilled.
     * <p>
     * <em>WARNING</em>:This implementation specific feature should not be
     * relied on by the production code! It may change at any time.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetExceedReadAheadLimitOK_Internal()
            throws IOException {
        InputStream is = getStream(4*1024+17);
        is.mark(10);
        assertEquals(20, is.read(new byte[20]));
        // Note the following is implementation dependent.
        // Since the bytes are already stored in the internal buffer, we won't
        // fail the reset even though we have exceeded the read ahead limit.
        // With a different stream implementation, this may fail!
        is.reset();
    }

    /**
     * Tests that the reset-call will fail we exceed the mark ahead limit and
     * the internal buffer has to be refilled.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetExceedReadAheadLimitFail_Internal()
            throws IOException {
        InputStream is = getStream(64*1024+17);
        is.mark(10);
        // The internal buffer is 32 KB (implementation detail).
        int toRead = 38*1024+7;
        int read = 0;
        byte[] buf = new byte[toRead];
        while (read < toRead) {
            read += is.read(buf, read, toRead - read);
        }
        // Note the following is implementation dependent.
        try {
            is.reset();
            fail("reset-call was expected to throw IOException");
        } catch (IOException ioe) {
            // As expected, do nothing
        }
    }

    /**
     * Reads almost enough bytes to read past the read ahead limit, then tests
     * that the reset works. After that, reads past the read ahead limit and
     * tests that the reset fails.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkResetOverflowInternalBufferKeepBytes()
            throws IOException {
        InputStream is = getStream(128*1024);
        is.mark(120*1024);
        byte[] buf = new byte[120*1024-1];
        fillArray(is, buf);
        is.reset();
        checkBeginningOfStream(is);

        // Again, but this time read past the read ahead limit.
        is = getStream(36*1024);
        is.mark(4*1024);
        buf = new byte[36*1024-1];
        fillArray(is, buf);
        try {
            is.reset();
            fail("reset-call was expected to throw IOException");
        }  catch (IOException ioe) {
            // As expected, do nothing
        }
    }

    /**
     * Marks the stream with a read ahead limit larger than the stream itself,
     * then reads until the end of the stream.
     * <p>
     * The current implementation does not allow the stream to be reset after
     * the last byte in the stream has been read once.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkReadUntilEOF()
            throws IOException {
        // Try with a single buffer fill first.
        InputStream is = getStream(4*1024);
        is.mark(8*1024);
        byte[] buf = new byte[8*1024];
        int read = 0;
        while (true) {
            int readNow = is.read(buf, read, buf.length - read);
            if (readNow == -1) {
                break;
            }
            read += readNow;
        }
        try {
            is.reset();
            fail("reset-call was expected to throw IOException");
        } catch (IOException ioe) {
            // The current implementation does not allow resetting the stream
            // when the source stream itself has been drained and all the data
            // has been read once.
        }

        // Now try with multiple buffer fills.
        is = getStream(640*1024);
        is.mark(128*1024);
        buf = new byte[8*1024];
        while (true) {
            // Just drain the stream.
            if (is.read(buf, 0, buf.length) == -1) {
                break;
            }
        }
        try {
            is.reset();
            fail("reset-call was expected to throw IOException");
        } catch (IOException ioe) {
            // The current implementation does not allow resetting the stream
            // when the source stream itself has been drained and all the data
            // has been read once.
        }
    }

    /**
     * Marks the stream with a read ahead limit larger than the stream itself,
     * then reads until just before the end of the stream.
     *
     * @throws IOException if something goes wrong
     */
    public void testMarkReadAlmostUntilEOF()
            throws IOException {
        // Try with a single buffer fill first.
        int limit = 4*1024;
        InputStream is = getStream(limit);
        is.mark(8*1024);
        byte[] buf = new byte[limit*2];
        int read = 0;
        while (read < limit -1) {
            int readNow = is.read(buf, read, (limit -1) - read);
            if (readNow == -1) {
                break;
            }
            read += readNow;
        }
        // EOF has been reached when filling the internal buffer, but we still
        // havent't read it. Therefore, the reset should succeed.
        is.reset();
        checkBeginningOfStream(is);
    }

    /**
     * Makes sure that the header bytes are copied when creating a new buffer
     * to hold all the required bytes when the stream has been marked.
     * This will only happen the first time the buffer is filled, i.e. when the
     * stream is marked before the first read (mark at position zero).
     *
     * @throws IOException if something goes wrong
     */
    public void testHeaderPresentInStream_Internal()
            throws IOException {
        final int valueLen = DEFAULT_INTERNAL_BUFFER_SIZE + 5*1024;
        InputStream is = getStream(valueLen);
        is.mark(valueLen - 1024);
        // Obtain a header generator to compare with.
        ClobStreamHeaderGenerator hdrGen = new ClobStreamHeaderGenerator(false);
        byte[] hdrTmp = new byte[100];
        int headerLen = hdrGen.generateInto(hdrTmp, 0, valueLen);
        byte[] hdr1 = new byte[headerLen];
        System.arraycopy(hdrTmp, 0, hdr1, 0, headerLen);
        byte[] hdr2 = new byte[headerLen];
        // Get the first bytes from the stream being tested.
        assertEquals(headerLen, is.read(hdr2));
        assertEquals(new ByteArrayInputStream(hdr1),
                     new ByteArrayInputStream(hdr2));
    }

    /**
     * Returns a stream to test, loaded with the repeating modern latin
     * lowercase alphabet.
     *
     * @param length the length of the stream in characters
     * @return A stream serving bytes.
     */
    private InputStream getStream(int length) {
        Reader src = new LoopingAlphabetReader(length,
                                        CharAlphabet.modernLatinLowercase());
        InputStream is = new ReaderToUTF8Stream(
                src, length, 0, "CLOB", new ClobStreamHeaderGenerator(false));
        assertTrue("The stream doesn't support mark/reset", is.markSupported());
        return is;
    }

    /**
     * Checks the beginning of the stream, which is expected to consist of five
     * header bytes (skipped) followed by the bytes for the characters 'a' and
     * 'b'.
     *
     * @param is the stream to check
     * @throws IOException if reading from the stream fails
     * @throws AssertionFailedError if the stream content isn't as expected
     */
    private void checkBeginningOfStream(InputStream is)
            throws IOException {
        assertEquals(5, is.skip(5));
        // We should now get the character a, followed by b.
        assertEquals((byte)'a', is.read());
        assertEquals((byte)'b', is.read());
    }

    /**
     * Fills the array by reading from the stream.
     *
     * @param is input stream to read from
     * @param b array to fill with bytes from the stream
     * @throws IOException if reading from the array fails, or the end of the
     *      stream is reached
     */
    private void fillArray(InputStream is, byte[] b)
            throws IOException {
        final int toRead = b.length;
        int read = 0;
        while (read < toRead) {
            int readNow = is.read(b, read, toRead - read);
            assertTrue("reached EOF", readNow != -1);
            read += readNow;
        }
    }

    /**
     * Performs a series of random operations on a {@code ReaderToUTF8Stream},
     * consisting of read, skip, mark, reset and a noop.
     * <p>
     * <em>Note</em>: Turn on debugging (derby.tests.debug=true) to see some
     * information, turn on tracing (derby.tests.trace=true) in addition to see
     * a lot more information.
     * <p>
     * If the test fails, the seed will be reported in the error message, and
     * the load that failed can be rerun.
     *
     * @throws IOException if the test fails
     */
    public void testRandomSequence()
            throws IOException {
        final long seed = System.currentTimeMillis();
        try {
            testRandomSequence(seed);
        } catch (IOException ioe) {
            // Report the seed for repeatability.
            IOException wrapper = new IOException("seed=" + seed);
            wrapper.initCause(ioe);
            throw wrapper;
        }
    }

    /**
     * Performs a series of random operations on a {@code ReaderToUTF8Stream},
     * consisting of read, skip, mark, reset and a noop.
     * <p>
     * Note that this test verifies that executing the operations don't fail,
     * but it doesn't verify that the bytes obtained from the stream are the
     * correct ones.
     *
     * @param seed seed controlling the test load
     * @throws IOException if the test fails
     */
    private void testRandomSequence(long seed)
            throws IOException {
        println("testRandomSequence seed: " + seed);
        final int iterations = 100;
        final Random rng = new Random(seed);
        for (int i=0; i < iterations; i++) {
            // Operation counters.
            int reads = 0, skips = 0, resets = 0, marks = 0, invalidations = 0;
            // Stream length (up to ~1 MB).
            int length = 1024*rng.nextInt(1024) + rng.nextInt(1024);
            boolean rs = rng.nextBoolean();
            println(">>> iteration " + i + ", length=" + length);
            int currentPos = 0;
            int limit = 0;
            int mark = -1;
            InputStream is = getStream(length);
            int ops = 0;
            while (ops < 200 && currentPos < length - 10) {
                if (rng.nextBoolean()) { // Whether to read/skip or mark/reset.
                    int toRead = getRandomLength(currentPos, length, rng, rs);
                    if (rng.nextBoolean()) {
                        // Read
                        mytrace("\treading " + toRead + " bytes");
                        reads++;
                        is.read(new byte[toRead]);
                    } else {
                        // Skip
                        mytrace("\tskipping " + toRead + " bytes");
                        skips++;
                        is.skip(toRead);
                    }
                    currentPos += toRead;
                    if (mark != -1 && (currentPos - mark) > limit) {
                        mytrace("\t\tmark invalidated");
                        invalidations++;
                        mark = -1;
                        limit = 0;
                    }
                }
                if (rng.nextBoolean()) { // Whether to read/skip or mark/reset.
                    // Mark/reset, or do nothing.
                    if (rng.nextBoolean()) {
                        if (rng.nextInt(100) < 40 && mark != -1) {
                            // Reset
                            mytrace("\tresetting to position " + mark);
                            resets++;
                            is.reset();
                            currentPos = mark;
                            mark = -1;
                        } else {
                            // Mark
                            limit = getRandomLength(currentPos, length, rng);
                            mytrace("\tmarking position " + currentPos +
                                    " with limit " + limit);
                            marks++;
                            mark = currentPos;
                            is.mark(limit);
                        }
                    }
                }
                ops++;
            }
            println("ops=" + ops + ", reads=" + reads + ", skips=" + skips +
                    ", marks=" + marks + ", resets=" + resets +
                    ", invalidations=" + invalidations);
        }
    }


    /**
     * Returns a random length within the limits.
     * <p>
     * This call will operate in the full range of the remaining bytes.
     *
     * @param currentPos the current position of the stream
     * @param length the length of the stream
     * @param rng random generator
     * @return A random length within the limits of the stream.
     */
    private int getRandomLength(int currentPos, int length, Random rng) {
        return getRandomLength(currentPos, length, rng, false);
    }

    /**
     * Returns a random length within the limits.
     *
     * @param currentPos the current position of the stream
     * @param length the length of the stream
     * @param rng random generator
     * @param reducedSize whether to return smaller number or not
     *      (setting to true may increase the number of operations that will be
     *      performed on a stream before it is exhausted)
     * @return A random length within the limits of the stream.
     */
    private int getRandomLength(int currentPos, int length, Random rng, boolean reducedSize) {
        int max = length - currentPos;
        if (reducedSize) {
            max = max / 5;
        }
        return (1 + (int)(max * rng.nextFloat()));
    }

    /**
     * Trace only if both trace and verbose is true in the test configuration.
     *
     * @param str the string to print
     */
    private void mytrace(String str) {
        if (getTestConfiguration().isVerbose()) {
            traceit(str);
        }
    }

    /**
     * Tests mark/reset functionality by comparing with
     * {@code ByteArrayInputStream}.
     *
     * @throws IOException if the test fails
     */
    public void testMarkReset1()
            throws IOException {
        InputStream is = getStream(64*1024);
        byte[] srcBuf = new byte[64*1024+5];
        fillArray(is, srcBuf);
        InputStream src = new ByteArrayInputStream(srcBuf);
        // Reinitialize the stream.
        is = getStream(64*1024);

        StreamUtil su = new StreamUtil(is, src);
        su.mark(1024);
        su.skip(17);
        su.reset();
        su.read(1);
        su.read(2133);
        su.mark(1024);
        su.reset();
        su.mark(1024);
        su.skip(18);
        su.read(1024);
    }

    /**
     * Tests mark/reset functionality by comparing with
     * {@code ByteArrayInputStream}. This test relies on knowing the size of
     * the internal buffer to force a shifting of existing bytes to take place.
     *
     * @throws IOException if the test fails
     */
    public void testMarkReset2_Internal()
            throws IOException {
        InputStream is = getStream(128*1024);
        byte[] srcBuf = new byte[128*1024+5];
        fillArray(is, srcBuf);
        InputStream src = new ByteArrayInputStream(srcBuf);
        // Reinitialize the stream.
        is = getStream(128*1024);

        StreamUtil su = new StreamUtil(is, src);
        su.skip(DEFAULT_INTERNAL_BUFFER_SIZE);
        su.mark(DEFAULT_INTERNAL_BUFFER_SIZE + 2*1024);
        su.read(1024);
        su.reset();
        su.read(3*1024);
    }

    /**
     * Utility class executing a few selected method calls on two streams,
     * expecting both of them to behave in the same way.
     */
    private class StreamUtil {
        private final InputStream is1;
        private final InputStream is2;

        StreamUtil(InputStream is1, InputStream is2) {
            assertNotNull(is1);
            assertNotNull(is2);
            this.is1 = is1;
            this.is2 = is2;
        }

        public void mark(int readAheadLimit) {
            is1.mark(readAheadLimit);
            is2.mark(readAheadLimit);
        }

        public void reset()
                throws IOException {
            is1.reset();
            is2.reset();
        }

        public long skip(long skip)
                throws IOException {
            long skip1 = 0;
            long skip2 = 0;
            // Skip data in the first stream.
            while (skip1 < skip) {
                long skippedNow = is1.skip(skip - skip1);
                if (skippedNow == -1) {
                    fail("stream one reached EOF: " + is1.getClass());
                }
                skip1 += skippedNow;
            }
            // Skip data in the second stream.
            while (skip2 < skip) {
                long skippedNow = is2.skip(skip - skip2);
                if (skippedNow == -1) {
                    fail("stream two reached EOF: " + is2.getClass());
                }
                skip2 += skippedNow;
            }
            assertEquals(skip1, skip2);
            return skip1;
        }

        public int read(int toRead)
                throws IOException {
            byte[] b1 = new byte[toRead];
            byte[] b2 = new byte[toRead];
            int read = read(b1, b2, false);
            assertEquals(new ByteArrayInputStream(b1),
                         new ByteArrayInputStream(b2));
            return read;
        }

        public int read(byte[] b1, byte[] b2, boolean expectEOF)
                throws IOException {
            assertEquals("unequal sized arrays", b1.length, b2.length);
            int read1 = 0;
            int read2 = 0;
            final int toRead = b1.length;
            // Read from the first stream.
            while (read1 < toRead) {
                int readNow = is1.read(b1, read1, toRead - read1);
                if (readNow == -1) {
                    if (expectEOF) {
                        break;
                    } else {
                        fail("stream one reached EOF: " + is1.getClass());
                    }
                }
                read1 += readNow;
            }
            // Read from the second stream.
            while (read2 < toRead) {
                int readNow = is2.read(b2, read2, toRead - read2);
                if (readNow == -1) {
                    if (expectEOF) {
                        break;
                    } else {
                        fail("stream two reached EOF: " + is2.getClass());
                    }
                }
                read2 += readNow;
            }
            assertEquals(read1, read2);
            return read1;
        }
    }
}

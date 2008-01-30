/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.UTF8UtilTest

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
package org.apache.derbyTesting.unitTests.junit;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

import org.apache.derby.iapi.types.ReaderToUTF8Stream;
import org.apache.derby.iapi.util.UTF8Util;

import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Tests that <code>skipFully</code> and <code>skipUntilEOF</code> behaves
 * correctly on Derby's modfied UTF-8 encoded streams.
 * <p>
 * These tests are dependent on the behavior of <code>ReaderToUTF8Stream</code>.
 * Note that this class inserts two bytes at the start of the user/application
 * stream to encode the length of the stream. These two bytes may be zero, even
 * if the stream is short enough for its length to be encoded.
 * <p>
 * Also note that the lengths chosen for large streams are just suitably large
 * integers. The point is to choose them large enough to make sure buffer
 * boundaries are crossed.
 * 
 * @see ReaderToUTF8Stream
 * @see UTF8Util
 */
public class UTF8UtilTest
    extends BaseTestCase {

    /**
     * Creates a test of the specified name.
     */
    public UTF8UtilTest(String name) {
        super(name);
    }

    /**
     * Ensure the assumption that the default looping alphabet stream and the
     * modified UTF-8 encoding is equal.
     * <p>
     * If this assumption is broken, several of the other tests will fail.
     */
    public void testEqualityOfModifedUTF8AndASCII()
            throws IOException {
        final int length = 12706;
        InputStream ascii = new LoopingAlphabetStream(length);
        InputStream modUTF8 = new ReaderToUTF8Stream(
                                    new LoopingAlphabetReader(length),
                                    length, 0, "ignored-test-type");
        modUTF8.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        assertEquals(ascii, modUTF8);
    }

    public void testSkipUntilEOFOnZeroLengthStream()
            throws IOException {
        assertEquals(0, UTF8Util.skipUntilEOF(new LoopingAlphabetStream(0)));
    }
    
    public void testSkipUntilEOFOnShortStreamASCII()
            throws IOException {
        assertEquals(5, UTF8Util.skipUntilEOF(new LoopingAlphabetStream(5)));
    }

    public void testSkipUntilEOFOnShortStreamCJK()
            throws IOException {
        final int charLength = 5;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        assertEquals(charLength, UTF8Util.skipUntilEOF(in));
    }

    public void testSkipUntilEOFOnLongStreamASCII()
            throws IOException {
        assertEquals(127019, UTF8Util.skipUntilEOF(
                new LoopingAlphabetStream(127019)));
    }
    
    public void testSkipUntilEOFOnLongStreamCJK()
            throws IOException {
        final int charLength = 127019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        assertEquals(charLength, UTF8Util.skipUntilEOF(in));
    }

    /**
     * Tests that <code>skipFully</code> successfully skips the requested
     * characters and returns the correct number of bytes skipped.
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    public void testSkipFullyOnValidLongStreamCJK()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        // Returns count in bytes, we are using CJK chars so multiply length
        // with 3 to get expected number of bytes.
        assertEquals(charLength *3, UTF8Util.skipFully(in, charLength));
    }

    /**
     * Tests that <code>skipFully</code> throws exception if the stream contains
     * less characters than the requested number of characters to skip.
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    public void testSkipFullyOnTooShortStreamCJK()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        try {
            UTF8Util.skipFully(in, charLength + 100);
            fail("Should have failed because of too short stream.");
        } catch (EOFException eofe) {
            // As expected, do nothing.
        }
    }
    
    /**
     * Tests that <code>skipFully</code> throws exception if there is a UTF-8
     * encoding error in the stream
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    public void testSkipFullyOnInvalidStreamCJK()
            throws IOException {
        final int charLength = 10;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        in.skip(1L); // Skip one more byte to trigger a UTF error.
        try {
            UTF8Util.skipFully(in, charLength);
            fail("Should have failed because of UTF error.");
        } catch (UTFDataFormatException udfe) {
            // As expected, do nothing.
        }
    }

    /**
     * Tests a sequence of skip calls.
     */
    public void testMixedSkipOnStreamTamil()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.tamil()),
                charLength, 0, "ignored-test-type");
        in.skip(2L); // Skip encoded length added by ReaderToUTF8Stream.
        int firstSkip = 10078;
        assertEquals(firstSkip*3, UTF8Util.skipFully(in, firstSkip));
        assertEquals(charLength - firstSkip, UTF8Util.skipUntilEOF(in));
        assertEquals(0, UTF8Util.skipUntilEOF(in)); // Nothing left here.
        try {
            UTF8Util.skipFully(in, 1L);
            fail("Should have failed because the stream has been drained.");
        } catch (EOFException eofe) {
            // As expected, do nothing
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * two bytes present. However, only one byte is provided.
     */
    public void testMissingSecondByteOfTwo()
            throws IOException {
        // 0xdf = 11011111
        byte[] data = {'a', (byte)0xdf};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * three bytes present. However, only one byte is provided.
     */
    public void testMissingSecondByteOfThree()
            throws IOException {
        // 0xef = 11101111
        byte[] data = {'a', (byte)0xef};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * three bytes present. However, only two bytes are provided.
     */
    public void testMissingThirdByteOfThree()
            throws IOException {
        // 0xef = 11101111, 0xb8 = 10111000
        byte[] data = {'a', (byte)0xef, (byte)0xb8};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to read a stream of data where there is an invalid UTF-8 encoded
     * byte.
     */
    public void testInvalidUTF8Encoding()
            throws IOException {
        // 0xf8 = 11111000 <-- invalid UTF-8 encoding
        byte[] data = {'a', 'b', 'c', (byte)0xf8, 'e', 'f'};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 6);
            fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected when reading invalid data
        }
    }

    /**
     * Demonstrates that skipping incorrectly encoded character sequences
     * works because the stream is not checked for well-formedness.
     */
    public void testSkippingInvalidEncodingWorks()
            throws IOException {
        // The array contains three valid characters and one invalid three-byte
        // representation that only has two bytes present.
        // When skipping, this sequence is (incorrectly) taken as a sequence of
        // three characters ('a' - some three byte character - 'a').
        // 0xef = 11101111, 0xb8 = 10111000
        byte[] data = {'a', (byte)0xef, (byte)0xb8, 'a', 'a'};
        byte[] dataWithLength =
            {0x0, 0x5, 'a', (byte)0xef, (byte)0xb8, 'a', 'a'};
        InputStream is = new ByteArrayInputStream(data);
        // This is actually incorrect, but does work currently.
        UTF8Util.skipFully(is, 3);
        // Verify that decoding this actually fails.
        DataInputStream dis = new DataInputStream(
                                    new ByteArrayInputStream(dataWithLength));
        try {
            dis.readUTF();
            fail("UTF-8 expected to be invalid, read should fail");
        } catch (UTFDataFormatException udfe) {
            // This is expected, since the UTF-8 encoding is invalid
        }
    }

    /**
     * Returns a suite of tests.
     */
    public static Test suite() {
        return new TestSuite(UTF8UtilTest.class, "UTF8Util tests");
    }
}

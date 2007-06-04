/*

   Derby - Class org.apache.derby.iapi.util.UTF8Util

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
package org.apache.derby.iapi.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

/**
 * Utility methods for handling UTF-8 encoded byte streams.
 * <p>
 * Note that when the <code>skip<code> methods mention detection of invalid
 * UTF-8 encodings, it only checks the first byte of a character. For multibyte
 * encodings, the second and third byte are not checked for correctness, just
 * skipped and ignored.
 *
 * @see java.io.DataInput
 */
//@ThreadSafe
public final class UTF8Util {

    /** Constant used to look up character count in an array. */
    private static final int CHAR_COUNT = 0;
    /** Constant used to look up byte count in an array. */
    private static final int BYTE_COUNT = 1;

    /** This class cannot be instantiated. */
    private UTF8Util() {}

    /**
     * Skip until the end-of-stream is reached.
     *
     * @param in byte stream with UTF-8 encoded characters
     * @return The number of characters skipped.
     * @throws IOException if reading from the stream fails
     * @throws UTFDataFormatException if an invalid UTF-8 encoding is detected
     */
    public static final long skipUntilEOF(InputStream in)
            throws IOException {
        // No need to do the skip in a loop, as Reader.read() returning -1
        // means EOF has been reached.
        // Note that a loop should be used if skip is used instead of read.
        return internalSkip(in, Long.MAX_VALUE)[CHAR_COUNT];
    }

    /**
     * Skip the requested number of characters from the stream.
     * <p>
     * @param in byte stream with UTF-8 encoded characters
     * @param charsToSkip number of characters to skip
     * @return The number of bytes skipped.
     * @throws EOFException if end-of-stream is reached before the requested
     *      number of characters are skipped
     * @throws IOException if reading from the stream fails
     * @throws UTFDataFormatException if an invalid UTF-8 encoding is detected
     */
    public static final long skipFully(InputStream in, long charsToSkip)
            throws EOFException, IOException {
        long[] counts = internalSkip(in, charsToSkip);
        if (counts[CHAR_COUNT] != charsToSkip) {
            throw new EOFException("Reached end-of-stream prematurely at " +
                "character/byte position " + counts[CHAR_COUNT] + "/" +
                counts[BYTE_COUNT] + ", trying to skip " + charsToSkip);
        }
        return counts[BYTE_COUNT];
    }

    /**
     * Skip characters in the stream.
     * <p>
     * Note that a smaller number than requested might be skipped if the
     * end-of-stream is reached before the specified number of characters has
     * been decoded. It is up to the caller to decide if this is an error
     * or not. For instance, when determining the character length of a stream,
     * <code>Long.MAX_VALUE</code> could be passed as the requested number of
     * characters to skip.
     *
     * @param in byte stream with UTF-8 encoded characters
     * @param charsToSkip the number of characters to skip
     * @return A long array with counts; the characters skipped at position
     *      <code>CHAR_COUNT<code>, the bytes skipped at position
     *      <code>BYTE_COUNT</code>. Note that the number of characters skipped
     *      may be smaller than the requested number.
     * @throws IOException if reading from the stream fails
     * @throws UTFDataFormatException if an invalid UTF-8 encoding is detected
     */
    private static final long[] internalSkip(final InputStream in,
                                             final long charsToSkip)
            throws IOException {
        long charsSkipped = 0;
        long bytesSkipped = 0;
        // Decoding routine for modified UTF-8.
        // See java.io.DataInput
        while (charsSkipped < charsToSkip) {
            int c = in.read();
            if (c == -1) {
                break;
            }
            charsSkipped++;
            if ((c & 0x80) == 0x00) { // 8th bit set (top bit)
                // Found char of one byte width.
                bytesSkipped++;
            } else if ((c & 0x60) == 0x40) { // 7th bit set, 6th bit unset
                // Found char of two byte width.
                if (skipPersistent(in, 1L) != 1L) {
                    // No second byte present.
                    throw new UTFDataFormatException(
                        "Second byte in two byte character missing; byte pos " +
                        bytesSkipped + " ; char pos " + charsSkipped);
                }
                bytesSkipped += 2;
            } else if ((c & 0x70) == 0x60) { // 7th and 6th bit set, 5th unset
                // Found char of three byte width.
                int skipped = 0;
                if (c == 0xe0) {
                    // Check for Derby EOF marker.
                    int c1 = in.read();
                    int c2 = in.read();
                    if (c1 == 0x00 && c2 == 0x00) {
                        // Found Derby EOF marker, exit loop.
                        charsSkipped--; // Compensate by subtracting one.
                        break;
                    }
                    // Do some rudimentary error checking.
                    // Allow everything except EOF, which is the same as done in
                    // normal processing (skipPersistent below).
                    if (c1 != -1 && c2 != -1) {
                        skipped = 2;
                    }
                } else {
                    skipped = (int)skipPersistent(in, 2L);
                }
                if (skipped != 2) {
                    // No second or third byte present
                    throw new UTFDataFormatException(
                        "Second or third byte in three byte character " +
                        "missing; byte pos " + bytesSkipped + " ; char pos " +
                        charsSkipped);
                }
                bytesSkipped += 3;
            } else {
                throw new UTFDataFormatException(
                    "Invalid UTF-8 encoding encountered: (decimal) " + c);
            }
        }
        // We don't close the stream, since it might be reused. One example of
        // this is use of Resetable streams.
        return new long[] {charsSkipped, bytesSkipped};
    }

    /**
     * Tries harder to skip the requested number of bytes.
     * <p>
     * Note that even if the method fails to skip the requested number of bytes,
     * it will not throw an exception. If this happens, the caller can be sure
     * that end-of-stream has been reached.
     *
     * @param in byte stream
     * @param bytesToSkip the number of bytes to skip
     * @return The number of bytes skipped.
     * @throws IOException if reading from the stream fails
     */
    private static final long skipPersistent(InputStream in, long bytesToSkip)
            throws IOException {
        long skipped = 0;
        while (skipped < bytesToSkip) {
            long skippedNow = in.skip(bytesToSkip - skipped);
            if (skippedNow <= 0) {
                if (in.read() == -1) {
                    // EOF, return what we have and leave it up to caller to
                    // decide what to do about it.
                    break;
                } else {
                    skippedNow = 1; // Added to count below.
                }
            }
            skipped += skippedNow;
        }
        return skipped;
    }

    private static final boolean isDerbyEOFMarker(InputStream in)
            throws IOException {
        // Expected to have read 224 (0xe0), check if the two next bytes are 0.
        return (in.read() == 0x00 && in.read() == 0x00);
    }
} // End class UTF8Util

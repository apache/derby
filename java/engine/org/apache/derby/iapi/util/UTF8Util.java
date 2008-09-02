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

import org.apache.derby.iapi.services.io.InputStreamUtil;

/**
 * Utility methods for handling UTF-8 encoded byte streams.
 * <p>
 * Note that when the {@code skip} methods mention detection of invalid
 * UTF-8 encodings, it only checks the first byte of a character. For multibyte
 * encodings, the second and third byte are not checked for correctness, just
 * skipped and ignored.
 *
 * @see java.io.DataInput
 */
//@ThreadSafe
public final class UTF8Util {

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
        return internalSkip(in, Long.MAX_VALUE).charsSkipped();
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
        SkipCount skipped = internalSkip(in, charsToSkip);
        if (skipped.charsSkipped() != charsToSkip) {
            throw new EOFException("Reached end-of-stream prematurely at " +
                "character/byte position " + skipped.charsSkipped() + "/" +
                skipped.bytesSkipped() + ", trying to skip " + charsToSkip);
        }
        return skipped.bytesSkipped();
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
     *      <code>CHAR_COUNT</code>, the bytes skipped at position
     *      <code>BYTE_COUNT</code>. Note that the number of characters skipped
     *      may be smaller than the requested number.
     * @throws IOException if reading from the stream fails
     * @throws UTFDataFormatException if an invalid UTF-8 encoding is detected
     */
    private static final SkipCount internalSkip(final InputStream in,
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
                if (InputStreamUtil.skipPersistent(in, 1L) != 1L) {
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
                    skipped = (int)InputStreamUtil.skipPersistent(in, 2L);
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
        return new SkipCount(charsSkipped, bytesSkipped);
    }

    /**
     * Helper class to hold skip counts; one for chars and one for bytes.
     */
    // @Immutable
    private static final class SkipCount {
        /** Number of bytes skipped. */
        private final long byteCount;
        /** Number of characters skipped. */
        private final long charCount; 

        /**
         * Creates a holder for the specified skip counts.
         * 
         * @param byteCount number of bytes
         * @param charCount number of characters
         */
        SkipCount(long charCount, long byteCount) {
            if (byteCount < 0 || charCount < 0) {
                // Don't allow negative counts.
                throw new IllegalArgumentException("charCount/byteCount " +
                        "cannot be negative: " + charCount + "/" + byteCount);
            }
            if (byteCount < charCount) {
                // A char must always be represented by at least one byte.
                throw new IllegalArgumentException("Number of bytes cannot be" +
                        "less than number of chars: " + byteCount + " < " +
                        charCount);
            }
            this.byteCount = byteCount;
            this.charCount = charCount;
        }

        long charsSkipped() {
            return this.charCount;
        }

        long bytesSkipped() {
            return this.byteCount;
        }
    }
} // End class UTF8Util

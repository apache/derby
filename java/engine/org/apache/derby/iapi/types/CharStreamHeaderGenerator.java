/*

   Derby - Class org.apache.derby.iapi.types.CharStreamHeaderGenerator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package org.apache.derby.iapi.types;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Generates stream headers for non-Clob string data types.
 * <p>
 * The stream header encodes the byte length of the stream. Since two bytes
 * are used for the header, the maximum encodable length is 65535 bytes. There
 * are three special cases, all handled by encoding zero into the header and
 * possibly appending an EOF-marker to the stream:
 * <ul> <li>Unknown length - with EOF marker</li>
 *      <li>Length longer than maximum encodable length - with EOF marker</li>
 *      <li>Length of zero - no EOF marker</li>
 * </ul>
 * The length is encoded like this:
 * <pre>
            out.writeByte((byte)(byteLength >>> 8));
            out.writeByte((byte)(byteLength >>> 0));
 * </pre>
 */
//@Immutable
public final class CharStreamHeaderGenerator
    implements StreamHeaderGenerator {

    /** The maximum length that can be encoded by the header. */
    private static final int MAX_ENCODABLE_LENGTH = 65535;

    /**
     * A byte count is expected.
     *
     * @return {@code false}.
     */
    public boolean expectsCharCount() {
        return false;
    }

    /**
     * Generates the header for the specified length and writes it into the
     * provided buffer, starting at the specified offset.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param byteLength the length to encode in the header
     * @return The number of bytes written into the buffer.
     */
    public int generateInto(byte[] buffer, int offset, long byteLength) {
        if (byteLength > 0 && byteLength <= MAX_ENCODABLE_LENGTH) {
            buffer[offset] = (byte)(byteLength >>> 8);
            buffer[offset +1] = (byte)(byteLength >>> 0);
        } else {
            // Byte length is zero, unknown or too large to encode.
            buffer[offset] = 0x00;
            buffer[offset +1] = 0x00;
        }
        return 2;
    }

    /**
     * Generates the header for the specified length.
     *
     * @param out the destination stream
     * @param byteLength the byte length to encode in the header
     * @return The number of bytes written to the destination stream.
     * @throws IOException if writing to the destination stream fails
     */
    public int generateInto(ObjectOutput out, long byteLength)
            throws IOException {
        if (byteLength > 0 && byteLength <= MAX_ENCODABLE_LENGTH) {
            out.writeByte((byte)(byteLength >>> 8));
            out.writeByte((byte)(byteLength >>> 0));
        } else {
            // Byte length is zero, unknown or too large to encode.
            out.writeByte(0x00);
            out.writeByte(0x00);
        }
        return 2;
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the buffer for a stream
     * of the specified byte length, if required.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param byteLength the byte length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(byte[] buffer, int offset, long byteLength) {
        if (byteLength < 0 || byteLength > MAX_ENCODABLE_LENGTH) {
            System.arraycopy(DERBY_EOF_MARKER, 0,
                             buffer, offset, DERBY_EOF_MARKER.length);
            return DERBY_EOF_MARKER.length;
        } else {
            return 0;
        }
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the destination stream
     * for the specified byte length, if required.
     *
     * @param out the destination stream
     * @param byteLength the length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(ObjectOutput out, long byteLength)
            throws IOException {
        if (byteLength < 0 || byteLength > MAX_ENCODABLE_LENGTH) {
            out.write(DERBY_EOF_MARKER);
            return DERBY_EOF_MARKER.length;
        } else {
            return 0;
        }
    }

    /**
     * Returns the maximum header length.
     *
     * @return Maximum header length in bytes.
     */
    public int getMaxHeaderLength() {
        return 2;
    }
}

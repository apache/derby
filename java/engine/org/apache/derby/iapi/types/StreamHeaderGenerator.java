/*

   Derby - Class org.apache.derby.iapi.types.StreamHeaderGenerator

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
 * Generates stream headers encoding the length of the stream.
 */
public interface StreamHeaderGenerator {

    /** The Derby-specific end-of-stream marker. */
    byte[] DERBY_EOF_MARKER = new byte[] {(byte)0xE0, 0x00, 0x00};

    /**
     * Tells if the header encodes a character or byte count.
     *
     * @return {@code true} if the character count is encoded into the header,
     *      {@code false} if the byte count is encoded into the header.
     */
    boolean expectsCharCount();

    /**
     * Generates the header for the specified length and writes it into the
     * provided buffer, starting at the specified offset.
     *
     * @param buf the buffer to write into
     * @param offset starting offset in the buffer
     * @param valueLength the length of the stream, can be in either bytes or
     *      characters depending on the header format
     * @return The number of bytes written into the buffer.
     */
    int generateInto(byte[] buf, int offset, long valueLength);

    /**
     * Generates the header for the specified length and writes it into the
     * destination stream.
     *
     * @param out the destination stream
     * @param valueLength the length of the stream, can be in either bytes or
     *      characters depending on the header format
     * @return The number of bytes written to the destination stream.
     * @throws IOException if writing to the destination stream fails
     */
    int generateInto(ObjectOutput out, long valueLength) throws IOException;

    /**
     * Writes a Derby-specific end-of-stream marker to the buffer for a stream
     * of the specified length, if required.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param valueLength the length of the stream, can be in either bytes or
     *      characters depending on the header format
     * @return Number of bytes written (zero or more).
     */
    int writeEOF(byte[] buffer, int offset, long valueLength);

    /**
     * Writes a Derby-specific end-of-stream marker to the destination stream
     * for the specified length, if required.
     *
     * @param out the destination stream
     * @param valueLength the length of the stream, can be in either bytes or
     *      characters depending on the header format
     * @return Number of bytes written (zero or more).
     * @throws IOException if writing to the destination stream fails
     */
    int writeEOF(ObjectOutput out, long valueLength) throws IOException;

    /**
     * Returns the maximum length of the header.
     *
     * @return Max header length in bytes.
     */
    int getMaxHeaderLength();
}

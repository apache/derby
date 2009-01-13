/*

   Derby - Class org.apache.derby.iapi.types.StreamHeaderHolder

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
package org.apache.derby.iapi.types;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A holder class for a stream header.
 * <p>
 * A stream header is used to store meta information about stream, typically
 * length information.
 */
//@Immutable
public final class StreamHeaderHolder {

    /** The header bytes. */
    private final byte[] hdr;
    /**
     * Describes if and how the header can be updated with a new length.
     * <p>
     * If {@code null}, updating the length is not allowed, and an exception
     * will be thrown if the update method is called. If allowed, the update
     * is described by the numbers of bits to right-shift at each position of
     * the header. Positions with a "negative shift" are skipped. Example:
     * <pre>
     * current hdr  shift       updated hdr
     * 0x00         24          (byte)(length >>> 24)
     * 0x00         16          (byte)(length >>> 16)
     * 0xF0         -1          0xF0
     * 0x00          8          (byte)(length >>> 8)
     * 0x00          0          (byte)(length >>> 0)
     * </pre>
     * <p>
     * Needless to say, this mechanism is rather simple, but sufficient for the
     * current header formats.
     */
    private final byte[] shifts;
    /**
     * Tells if the header encodes the character or byte length of the stream.
     */
    private final boolean lengthIsCharCount;
    /**
     * Whether a Derby-specific end-of-stream marker is required or not.
     * It is expected that the same EOF marker is used for all headers:
     * {@code 0xE0 0x00 0x00}.
     */
    private final boolean writeEOF;

    /**
     * Creates a new stream header holder object.
     *
     * @param hdr the stream header bytes
     * @param shifts describes how to update the header with a new length, or
     *      {@code null} if updating the header is forbidden
     * @param lengthIsCharCount whether the length is in characters
     *      ({@code true}) or bytes ({@code false})
     * @param writeEOF whether a Derby-specific EOF marker is required
     */
    public StreamHeaderHolder(byte[] hdr, byte[] shifts,
                       boolean lengthIsCharCount, boolean writeEOF) {
        this.hdr = hdr;
        this.shifts = shifts;
        this.lengthIsCharCount = lengthIsCharCount;
        this.writeEOF = writeEOF;
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(shifts == null || hdr.length == shifts.length);
        }
    }

    /**
     * Copies the header bytes into the specified buffer at the given offset.
     *
     * @param buf target byte array
     * @param offset offset in the target byte array
     * @return The number of bytes written (the header length).
     */
    public int copyInto(byte[] buf, int offset) {
        System.arraycopy(hdr, 0, buf, offset, hdr.length);
        return hdr.length;
    }

    /**
     * Returns the header length.
     *
     * @return The header length in bytes.
     */
    public int headerLength() {
        return hdr.length;
    }

    /**
     * Tells if the header encodes the character or the byte length of the
     * stream.
     *
     * @return {@code true} if the character length is expected, {@code false}
     *      if the byte length is expected.
     */
    public boolean expectsCharLength() {
        return lengthIsCharCount;
    }

    /**
     * Tells if a Derby-specific end-of-stream marker should be appended to the
     * stream associated with this header.
     *
     * @return {@code true} if EOF marker required, {@code false} if not.
     */
    public boolean writeEOF() {
        return writeEOF;
    }

    /**
     * Creates a new holder object with a header updated for the new length.
     * <p>
     * <em>NOTE</em>: This method does not update the header in the stream
     * itself. It must be updated explicitly using {@linkplain #copyInto}.
     *<p>
     * <em>Implementation note</em>: This update mechanism is very simple and
     * may not be sufficient for later header formats. It is based purely on
     * shifting of the bits in the new length.
     *
     * @param length the new length to encode into the header
     * @param writeEOF whether the new header requires an EOF marker or not
     * @return A new stream header holder for the new length.
     * @throws IllegalStateException if updating the header is disallowed
     */
    public StreamHeaderHolder updateLength(int length, boolean writeEOF) {
        if (shifts == null) {
            throw new IllegalStateException(
                    "Updating the header has been disallowed");
        }
        byte[] newHdr = new byte[hdr.length];
        for (int i=0; i < hdr.length; i++) {
            if (shifts[i] >= 0) {
                newHdr[i] = (byte)(length >>> shifts[i]);
            } else {
                newHdr[i] = hdr[i];
            }
        }
        return new StreamHeaderHolder(
                newHdr, shifts, lengthIsCharCount, writeEOF);
    }
}

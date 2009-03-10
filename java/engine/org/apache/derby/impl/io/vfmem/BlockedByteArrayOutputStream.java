/*

   Derby - Class org.apache.derby.impl.io.vfmem.BlockedByteArrayOutputStream

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

package org.apache.derby.impl.io.vfmem;

import java.io.OutputStream;

/**
 * Output stream writing bytes into an underlying blocked byte array.
 */
public class BlockedByteArrayOutputStream
        extends OutputStream {

    /** The underlying destination. Set to {@code null} when closed. */
    private BlockedByteArray src;
    /** The current position of the stream. */
    private long pos;

    /**
     * Creates a new stream writing data into the specified blocked byte array.
     *
     * @param src the underlying blocked byte array
     * @param pos the initial position of stream
     */
    public BlockedByteArrayOutputStream(BlockedByteArray src, long pos) {
        if (src == null) {
            throw new IllegalArgumentException(
                    "BlockedByteArray cannot be null");
        }
        this.src = src;
        this.pos = pos;
    }

    /**
     * Sets the position.
     *
     * @param newPos the new byte position
     */
    void setPosition(long newPos) {
        this.pos = newPos;
    }

    /**
     * Returns the current position.
     *
     * @return The current byte position.
     */
    long getPosition() {
        return this.pos;
    }

    /**
     * Writes the single byte into the underlying blocked byte array.
     *
     * @param b the byte to write
     */
    public void write(int b) {
        pos += src.writeByte(pos, (byte)b);
    }

    /**
     * Writes the specified bytes into the underlying blocked byte array.
     *
     * @param buf source byte array
     * @param offset index of the first byte to write
     * @param len the number of bytes to write
     */
    public void write(byte[] buf, int offset, int len) {
        pos += src.writeBytes(pos, buf, offset, len);
    }

    /**
     * Closes the stream.
     */
    public void close() {
        this.src = null;
    }
}

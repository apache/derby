/*

   Derby - Class org.apache.derby.impl.io.vfmem.BlockedByteArrayInputStream

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

import java.io.InputStream;

/**
 * An input stream reading from a blocked byte array.
 */
class BlockedByteArrayInputStream
        extends InputStream {

    /** The underlying source. Set to {@code null} when closed. */
    private BlockedByteArray src;
    /** The current position of the stream. */
    private long pos;

    /**
     * Creates a new input stream reading from a blocked byte array.
     *
     * @param src the source blocked byte array
     * @param pos the initial position to start reading from
     */
    public BlockedByteArrayInputStream(BlockedByteArray src, long pos) {
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
     * Reads a single byte.
     *
     * @return A byte.
     */
    public int read() {
        int ret = src.read(pos);
        if (ret != -1) {
            pos++;
        }
        return ret;
    }

    /**
     * Reads up to {@code len} bytes.
     *
     * @param buf destination buffer
     * @param offset offset into the destination buffer
     * @param len number of bytes to read
     * @return The number of bytes read.
     */
    public int read(byte[] buf, int offset, int len) {
        int ret = src.read(pos, buf, offset, len);
        if (ret != -1) {
            pos += ret;
        }
        return ret;
    }

    /**
     * Closes the stream.
     */
    public void close() {
        this.src = null;
    }
}

/*

   Derby - Class org.apache.derby.impl.jdbc.ClobUpdatableReader

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

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * {@code ClobUpdatableReader} is used to create a {@code Reader} capable of
 * detecting changes to the underlying source.
 * <p>
 * This class is aware that the underlying stream can be modified and
 * reinitializes itself if it detects any change in the stream. This
 * invalidates the cache so the changes are reflected immediately.
 * <p>
 * The task of this class is to detect changes in the underlying Clob.
 * Repositioning is handled by other classes.
 */
final class ClobUpdatableReader extends Reader {
 
    /** Reader accessing the Clob data and doing the work. */
    private Reader streamReader;
    /** Character position of this reader (1-based). */
    private long pos;
    /** The last update count seen on the underlying Clob. */
    private long lastUpdateCount = -1;
    /**
     * The Clob object we are reading from.
     * <p>
     * Note that even though the Clob itself is final, the internal
     * representation of the content may change. The reference to the Clob is
     * needed to get a hold of the new internal representation if it is changed.
     *
     * @see #iClob
     */
    private final EmbedClob clob;
    /**
     * The current internal representation of the Clob content.
     * <p>
     * If the user starts out with a read-only Clob and then modifies it, the
     * internal representation will change.
     */
    private InternalClob iClob;
    /**
     * Position in Clob where to stop reading unless EOF is reached first.
     */
    private final long maxPos;
    /** Tells if this reader has been closed. */
    private volatile boolean closed = false;

    /**
     * Creates an updatable reader configured with initial position set to the
     * first character in the Clob and with no imposed length limit.
     *
     * @param clob source data
     * @throws IOException if obtaining the underlying reader fails
     * @throws SQLException if obtaining the underlying reader fails
     */
    public ClobUpdatableReader(EmbedClob clob)
            throws IOException, SQLException {
        this(clob, 1L, Long.MAX_VALUE);
    }

    /**
     * Creates an updatable reader configured with the specified initial
     * position and with an imposed length limit.
     *
     * @param clob source data
     * @param initialPos the first character that will be read
     * @param length the maximum number of characters that will read
     * @throws IOException if obtaining the underlying reader fails
     * @throws SQLException if obtaining the underlying reader fails
     */
    public ClobUpdatableReader(EmbedClob clob, long initialPos, long length)
            throws IOException, SQLException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(initialPos > 0);
            SanityManager.ASSERT(length > 0);
        }
        this.clob = clob;
        this.iClob = clob.getInternalClob();
        this.pos = initialPos;
        // Temporary computation due to possible overflow.
        long tmpMaxPos = initialPos + length; // May overflow
        if (tmpMaxPos < length || tmpMaxPos < initialPos) {
            tmpMaxPos = Long.MAX_VALUE;
        }
        this.maxPos = tmpMaxPos;
    }

    public int read() throws IOException {
        if (closed) {
            throw new IOException("Reader closed");
        }
        if (pos >= maxPos) {
            return -1;
        }
        updateReaderIfRequired();
        // Adjust length if required, read data and update position.
        int retVal = this.streamReader.read();
        if (retVal > 0) {
            this.pos++;
        }
        return retVal;
    }

    public int read(char[] cbuf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Reader closed");
        }
        if (pos >= maxPos) {
            return -1;
        }
        updateReaderIfRequired();
        // Adjust length if required, read data and update position.
        int adjustedLen = (int)Math.min(len, maxPos - pos);
        int readCount = this.streamReader.read(cbuf, off, adjustedLen);
        if (readCount > 0) {
            this.pos += readCount;
        }
        return readCount;
    }

    public long skip(long len) throws IOException {
        if (closed) {
            throw new IOException("Reader closed");
        }
        if (pos >= maxPos) {
            return 0;
        }
        updateReaderIfRequired();
        // Adjust length if required, skip data and update position.
        long adjustedLen = Math.min(len, maxPos - pos);
        long skipped = this.streamReader.skip(adjustedLen);
        if (skipped > 0) {
            this.pos += skipped;
        }
        return skipped;
    }

    /**
     * Closes this reader.
     * <p>
     * An {@code IOException} will be thrown if any of the read or skip methods
     * are called after the reader has been closed.
     *
     * @throws IOException if an error occurs while closing
     */
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            // Can be null if the stream is created and closed immediately.
            if (this.streamReader != null) {
                this.streamReader.close();
            }
        }
    }

    /**
     * Updates the reader if the underlying data has been modified.
     * <p>
     * There are two cases to deal with:
     * <ol> <li>The underlying data of the internal Clob representation has been
     *          modified.</li>
     *      <li>The internal Clob representation has changed.</li>
     * </ol>
     * The latter case happens when a read-only Clob, represented as a stream
     * into store, is modified by the user and a new temporary internal
     * representation is created.
     *
     * @throws IOException if verifying or updating the reader fails
     */
    private void updateReaderIfRequired() throws IOException {
        // Case two as described above; changed representation.
        if (iClob.isReleased()) {
            iClob = this.clob.getInternalClob();
            lastUpdateCount = -1;
            // Check again. If both are closed, the Clob itself is closed.
            if (iClob.isReleased()) {
                close();
                return;
            }
        }
        // Case one as described above; content has been modified.
        if (lastUpdateCount != iClob.getUpdateCount()) {
            lastUpdateCount = iClob.getUpdateCount();
            try {
                this.streamReader = iClob.getReader(pos);
            } catch (SQLException sqle) {
                throw new IOException(sqle.getMessage());
            }
        }
    }
}

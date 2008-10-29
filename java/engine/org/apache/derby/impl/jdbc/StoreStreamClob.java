/*

   Derby - Class org.apache.derby.impl.jdbc.StoreStreamClob

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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.SQLException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.UTF8Util;

/**
 * A read-only Clob representation operating on streams out of the Derby store
 * module.
 * <p>
 * Note that the streams from the store are expected to have the following
 * properties:
 * <ol> <li>The first two bytes are used for length encoding. Note that due to
 *          the inadequate max number of this format, it is always ignored. This
 *          is also true if there actually is a length encoded there. The two
 *          bytes are excluded from the length of the stream.
 *      <li>A Derby-specific end-of-stream marker at the end of the stream can
 *          be present. The marker is expected to be <code>0xe0 0x00 0x00</code>
 * </ol>
 */
final class StoreStreamClob
    implements InternalClob {

    /** Tells whether this Clob has been released or not. */
    private volatile boolean released = false;

    /**
     * The stream from store, used to read bytes from the database.
     * <p>
     * To be able to support the requirements, the stream must implement
     * {@link Resetable}.
     */
    //@GuardedBy("synchronizationObject")
    private final PositionedStoreStream positionedStoreStream;
    /**
     * The cached length of the store stream in number of characters.
     * A value of {@code 0} means the length is unknown, and zero is an invalid
     * length for a store stream Clob. It is set to zero because that is the
     * value encoded as length in the store stream (on disk format) when the
     * length is unknown or cannot be represented.
     */
    private long cachedCharLength = 0;
    /** The connection (child) this Clob belongs to. */
    private final ConnectionChild conChild;
    /** Object used for synchronizing access to the store stream. */
    private final Object synchronizationObject;
    /**
     * Shared internal reader, closed when the Clob is released.
     * This is a performance optimization, and the stream is shared between
     * "one time" operations, for instance {@code getSubString} calls. Often a
     * subset, or the whole, of the Clob is read subsequently and then this
     * optimization avoids repositioning costs (the store does not support
     * random access for LOBs).
     * <b>NOTE</b>: Do not publish this reader to the end-user.
     */
    private UTF8Reader internalReader;
    /** The internal reader wrapped so that it cannot be closed. */
    private FilterReader unclosableInternalReader;

    /**
     * Creates a new Clob based on a stream from store.
     * <p>
     * Note that the stream passed in have to fulfill certain requirements,
     * which are not currently totally enforced by Java (the language).
     *
     * @param stream the stream containing the Clob value. This stream is
     *      expected to implement {@link Resetable} and to be a
     *      {@link org.apache.derby.iapi.services.io.FormatIdInputStream} with
     *      an ${link org.apache.derby.impl.store.raw.data.OverflowInputStream}
     *      inside. However, the available interfaces does not guarantee this.
     *      See the class JavaDoc for more information about this stream.
     * @param conChild the connection (child) this Clob belongs to
     * @throws StandardException if initializing the store stream fails
     * @throws NullPointerException if <code>stream</code> or
     *      <code>conChild</code> is null
     * @throws ClassCastException if <code>stream</code> is not an instance
     *      of <code>Resetable</code>
     * @see org.apache.derby.iapi.services.io.FormatIdInputStream
     * @see org.apache.derby.impl.store.raw.data.OverflowInputStream
     */
    public StoreStreamClob(InputStream stream, ConnectionChild conChild)
            throws StandardException {
        this.positionedStoreStream = new PositionedStoreStream(stream);
        this.conChild = conChild;
        this.synchronizationObject = conChild.getConnectionSynchronization();
        this.positionedStoreStream.initStream();
    }

    /**
     * Releases resources associated with this Clob.
     */
    public void release() {
        if (!released) {
            if (this.internalReader != null) {
                this.internalReader.close();
            }
            this.positionedStoreStream.closeStream();
            this.released = true;
        }
    }

    /**
     * Returns the number of characters in the Clob.
     *
     * @return Number of characters in the Clob.
     * @throws SQLException if any kind of error is encountered, be it related
     *      to I/O or something else
     */
    public long getCharLength()
            throws SQLException {
        checkIfValid();
        if (this.cachedCharLength == 0) {
            // Decode the stream to find the length.
            synchronized (this.synchronizationObject) {
                this.conChild.setupContextStack();
                try {
                    this.cachedCharLength = UTF8Util.skipUntilEOF(
                            new BufferedInputStream(getRawByteStream()));
                } catch (Throwable t) {
                    throw noStateChangeLOB(t);
                } finally {
                    this.conChild.restoreContextStack();
                }
            }
        }
        return this.cachedCharLength;
    }

    /**
     * Returns a stream serving the raw bytes of this Clob.
     * <p>
     * Note that the stream returned is an internal stream, and it should not be
     * pulished to end users.
     *
     * @return A stream serving the bytes of this Clob, initialized at byte 0 of
     *      the data. The buffer must be assumed to be unbuffered, but no such
     *      guarantee is made.
     * @throws IOException if accessing the I/O resources fail
     * @throws SQLException if accessing the store resources fail
     */
    public InputStream getRawByteStream()
            throws IOException, SQLException {
        checkIfValid();
        try {
            // Skip the encoded length.
            this.positionedStoreStream.reposition(2L);
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
        return this.positionedStoreStream;
    }

    /**
     * Returns a reader for the Clob, initialized at the specified character
     * position.
     *
     * @param pos character position. The first character is at position 1.
     * @return A reader initialized at the specified position.
     * @throws EOFException if the positions is larger than the Clob
     * @throws IOException if accessing the I/O resources fail
     * @throws SQLException if accessing the store resources fail
     */
    public Reader getReader(long pos)
            throws IOException, SQLException  {
        checkIfValid();
        try {
            this.positionedStoreStream.reposition(0L);
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
        Reader reader = new UTF8Reader(this.positionedStoreStream,
                                TypeId.CLOB_MAXWIDTH, this.conChild,
                                this.synchronizationObject);
        long leftToSkip = pos -1;
        long skipped;
        while (leftToSkip > 0) {
            skipped = reader.skip(leftToSkip);
            // Since Reader.skip block until some characters are available,
            // a return value of 0 must mean EOF.
            if (skipped <= 0) {
                throw new EOFException("Reached end-of-stream prematurely");
            }
            leftToSkip -= skipped;
        }
        return reader;
    }

    /**
     * Returns an internal reader for the Clob, initialized at the specified
     * character position.
     *
     * @param characterPosition 1-based character position.
     * @return A reader initialized at the specified position.
     * @throws EOFException if the positions is larger than the Clob
     * @throws IOException if accessing the I/O resources fail
     * @throws SQLException if accessing the store resources fail
     */
    public Reader getInternalReader(long characterPosition)
            throws IOException, SQLException {
        if (this.internalReader == null) {
            this.internalReader = new UTF8Reader(positionedStoreStream,
                    TypeId.CLOB_MAXWIDTH, conChild, synchronizationObject);
            this.unclosableInternalReader =
                    new FilterReader(this.internalReader) {
                        public void close() {
                            // Do nothing.
                            // Stream will be closed when the Clob is released.
                        }
                    };
        }
        try {
            this.internalReader.reposition(characterPosition);
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
        return this.unclosableInternalReader;
    }

    /**
     * Not supported.
     *
     * @see InternalClob#getWriter
     */
    public Writer getWriter(long pos) {
        throw new UnsupportedOperationException(
            "A StoreStreamClob object is not updatable");
    }

    /**
     * Not supported.
     *
     * @see InternalClob#insertString
     */
    public long insertString(String str, long pos) {
        throw new UnsupportedOperationException(
            "A StoreStreamClob object is not updatable");
    }

    /**
     * Tells if this Clob can be modified.
     *
     * @return <code>false</code>, this Clob is read-only.
     */
    public boolean isWritable() {
        return false;
    }

    /**
     * Not supported.
     *
     * @see InternalClob#truncate
     */
    public void truncate(long newLength) {
        throw new UnsupportedOperationException(
            "A StoreStreamClob object is not updatable");
    }

    /**
     * Wrap real exception in a {@link SQLException} to avoid changing the state
     * of the connection child by cleaning it up.
     *
     * @param t real cause of error that we want to "ignore" with respect to
     *      transaction context cleanup
     * @return A {@link SQLException} wrapped around the real cause of the error
     */
    private static SQLException noStateChangeLOB(Throwable t) {
        if (t instanceof StandardException)
        {
            // container closed means the blob or clob was accessed after commit
            if (((StandardException) t).getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED))
            {
                t = StandardException.newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
            }
        }
        return org.apache.derby.impl.jdbc.EmbedResultSet.noStateChangeException(t);
    }

    /**
     * Makes sure the Clob has not been released.
     * <p>
     * All operations are invalid on a released Clob.
     *
     * @throws IllegalStateException if the Clob has been released
     */
    private void checkIfValid() {
        if (this.released) {
            throw new IllegalStateException(
                "The Clob has been released and is not valid");
        }
    }
} // End class StoreStreamClob

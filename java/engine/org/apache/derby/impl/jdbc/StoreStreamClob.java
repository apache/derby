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

    /** Maximum value used when requesting bytes/chars to be skipped. */
    private static final long SKIP_BUFFER_SIZE = 8*1024; // 8 KB

    /** Tells whether this Clob has been released or not. */
    private volatile boolean released = false;

    /**
     * The stream from store, used to read bytes from the database.
     * <p>
     * To be able to support the requirements, the stream must implement
     * {@link Resetable}.
     */
    //@GuardedBy("synchronizationObject")
    private final InputStream storeStream;
    /** The connection (child) this Clob belongs to. */
    private final ConnectionChild conChild;
    /** Object used for synchronizing access to the store stream. */
    private final Object synchronizationObject;


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
        this.storeStream = stream;
        this.conChild = conChild;
        this.synchronizationObject = conChild.getConnectionSynchronization();
        ((Resetable)this.storeStream).initStream();
    }

    /**
     * Releases resources associated with this Clob.
     */
    public void release() {
        if (!released) {
            ((Resetable)this.storeStream).closeStream();
            this.released = true;
        }
    }

    /**
     * Returns the number of bytes in the Clob.
     *
     * @return The number of bytes in the Clob.
     * @throws IOException if accessing the I/O resources fail
     * @throws SQLException if accessing the store resources fail
     */
    public long getByteLength()
            throws IOException, SQLException {
        checkIfValid();
        // Read through the whole stream to get the length.
        long byteLength = 0;
        try {
            this.conChild.setupContextStack();
            // See if length is encoded in the stream.
            byteLength = resetStoreStream(true);
            if (byteLength == 0) {
                while (true) {
                    long skipped = this.storeStream.skip(SKIP_BUFFER_SIZE);
                    if (skipped <= 0) {
                        break;
                    }
                    byteLength += skipped;
                }
                // Subtract 3 bytes for the end-of-stream marker.
                byteLength -= 3;
            }
            return byteLength;
        } finally {
            this.conChild.restoreContextStack();
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
        synchronized (this.synchronizationObject) {
            this.conChild.setupContextStack();
            try {
                return UTF8Util.skipUntilEOF(
                                new BufferedInputStream(getRawByteStream()));
            } catch (Throwable t) {
                throw noStateChangeLOB(t);
            } finally {
                this.conChild.restoreContextStack();
            }
        }
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
        resetStoreStream(true);
        return this.storeStream;
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
        resetStoreStream(false);
        Reader reader = new UTF8Reader(this.storeStream, TypeId.CLOB_MAXWIDTH,
            this.conChild, this.synchronizationObject);
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
     * Returns the byte position for the specified character position.
     *
     * @param charPos character position. First character is at position 1.
     * @return Corresponding byte position. First byte is at position 0.
     * @throws EOFException if the position is bigger then the Clob
     * @throws IOException if accessing the underlying I/O resources fail
     * @throws SQLException if accessing the underlying store resources fail
     */
    public long getBytePosition(long charPos)
            throws IOException, SQLException {
        return UTF8Util.skipFully(getRawByteStream(), charPos -1);
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

    /**
     * Reset the store stream, skipping two bytes of length encoding if
     * requested.
     *
     * @param skipEncodedLength <code>true</code> will cause length encoding to
     *      be skipped. Note that the length is not always recorded when data is
     *      written to store, and therefore it is ignored.
     * @return The length encoded in the stream, or <code>-1</code> if the
     *      length information is not decoded. A return value of <code>0</code>
     *      means the stream is ended with a Derby end-of-stream marker.
     * @throws IOException if skipping the two bytes fails
     * @throws SQLException if resetting the stream fails in store
     */
    private long resetStoreStream(boolean skipEncodedLength)
            throws IOException, SQLException {
        try {
            ((Resetable)this.storeStream).resetStream();
        } catch (StandardException se) {
            throw noStateChangeLOB(se);
        }
        long encodedLength = -1L;
        if (skipEncodedLength) {
            int b1 = this.storeStream.read();
            int b2 = this.storeStream.read();
            if (b1 == -1 || b2 == -1) {
                throw Util.setStreamFailure(
                    new IOException("Reached end-of-stream prematurely"));
            }
            // Length is currently written as an unsigned short.
            encodedLength = (b1 << 8) + (b2 << 0);
        }
        return encodedLength;
    }
} // End class StoreStreamClob

/*

   Derby - Class org.apache.derby.impl.jdbc.TemporaryClob

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
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.UTF8Util;

/**
 * A Clob representation where the Clob is stored either in memory or on disk.
 * <p>
 * Character positions given as input to methods in this class are always
 * 1-based. Byte positions are always 0-based.
 */
final class TemporaryClob implements InternalClob {

    /**
     * Connection child assoicated with this Clob.
     * <p>
     * Currently only used for synchronization in *some* streams associated
     * with the Clob. This suggests something is off wrt. synchronization.
     */
    private ConnectionChild conChild;
    /** Underlying structure holding this Clobs raw bytes. */
    //@GuardedBy("this")
    private final LOBStreamControl bytes;
    /** Tells whether this Clob has been released or not. */
    // GuardedBy("this")
    private boolean released = false;
    /**
     * Cached character length of the Clob.
     * <p>
     * A value of {@code 0} is interpreted as unknown length, even though it is
     * a valid value. If the length is requested and the value is zero, an
     * attempt to obtain the length is made by draining the source.
     */
    private long cachedCharLength;
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

    /** Simple one-entry cache for character-byte position. */
    // @GuardedBy("this")
    private final CharToBytePositionCache posCache =
        new CharToBytePositionCache();

    /**
     * Clones the content of another internal Clob.
     *
     * @param dbName name of the assoicated database
     * @param conChild assoiated connection child
     * @param clob the Clob whose content to clone
     * @return A read-write Clob.
     * @throws IOException if accessing the I/O resources fail (read or write)
     * @throws SQLException if accessing underlying resources fail
     */
    static InternalClob cloneClobContent(String dbName,
                                         ConnectionChild conChild,
                                         InternalClob clob)
            throws IOException, SQLException {
        TemporaryClob newClob = new TemporaryClob(conChild);
        newClob.copyClobContent(clob);
        return newClob;
    }

    /**
     * Clones the content of another internal Clob.
     *
     * @param dbName name of the assoicated database
     * @param conChild assoiated connection child
     * @param clob the Clob whose content to clone
     * @param length number of chars in new InternalClob
     * @return A read-write Clob.
     * @throws IOException if accessing the I/O resources fail (read or write)
     * @throws SQLException if accessing underlying resources fail
     */
    static InternalClob cloneClobContent(String dbName,
                                         ConnectionChild conChild,
                                         InternalClob clob,
                                         long length)
            throws IOException, SQLException {
        TemporaryClob newClob = new TemporaryClob(conChild);
        newClob.copyClobContent(clob, length);
        return newClob;
    }

    /**
     * Constructs a <code>TemporaryClob</code> object used to perform
     * operations on a CLOB value.
     * @param conChild connection object used to obtain synchronization object
     * 
     * @throws NullPointerException if <code>conChild</code> is
     *      <code>null</code>
     */
    TemporaryClob (ConnectionChild conChild) {
        if (conChild == null) {
            throw new NullPointerException("conChild cannot be <null>");
        }
        this.conChild = conChild;
        this.bytes = new LOBStreamControl(conChild.getEmbedConnection());
    }

    /**
     * Releases this Clob by freeing assoicated resources.
     *
     * @throws IOException if accessing underlying I/O resources fail
     */
    public synchronized void release()
            throws IOException {
        if (!this.released) {
            this.released = true;
            this.bytes.free();
            if (internalReader != null) {
                internalReader.close();
                internalReader = null;
                unclosableInternalReader = null;
            }
        }
    }

    /**
     * Returns a stream serving the raw bytes of this Clob.
     * <p>
     * The stream is managed by the underlying byte store, and can serve bytes
     * both from memory and from a file on disk.
     *
     * @return A stream serving the raw bytes of the stream, initialized at
     *      byte position <code>0</code>.
     * @throws IOException if obtaining the stream fails
     */
    public synchronized InputStream getRawByteStream()
            throws IOException {
        checkIfValid();
        return this.bytes.getInputStream(0L);
    }

    /**
     * Constructs a <code>TemporaryClob</code> object and
     * initializes with a initial String.
     * @param data initial value in String
     * @param conChild connection object used to obtain synchronization object
     */
    TemporaryClob (String data, ConnectionChild conChild)
                          throws IOException, StandardException {
        if (conChild == null) {
            throw new NullPointerException("conChild cannot be <null>");
        }
        this.conChild = conChild;
        bytes = new LOBStreamControl(conChild.getEmbedConnection(), getByteFromString (data));
        this.cachedCharLength = data.length();
    }
    /**
     * Finds the corresponding byte position for the given UTF-8 character
     * position, starting from the byte position <code>startPos</code>.
     * See comments in SQLChar.readExternal for more notes on
     * processing the UTF8 format.
     *
     * @param charPos character position
     * @return Stream position in bytes for the given character position.
     * @throws EOFException if the character position specified is greater than
     *      the Clob length +1
     * @throws IOException if accessing underlying I/O resources fail
     */
    //@GuardedBy(this)
    private long getBytePosition (final long charPos)
            throws IOException {
        long bytePos;
        if (charPos == this.posCache.getCharPos()) {
            // We already know the position.
            bytePos = this.posCache.getBytePos();
        } else {
            long startingBytePosition = 0L; // Default to start at position 0.
            long charsToSkip = charPos -1; // Subtract one to get number to skip
            if (charPos > this.posCache.getCharPos()) {
                // Exploit the last known character position.
                startingBytePosition = this.posCache.getBytePos();
                charsToSkip -= (this.posCache.getCharPos() -1);
            }
            InputStream utf8Bytes =
                this.bytes.getInputStream(startingBytePosition);
            bytePos = startingBytePosition +
                UTF8Util.skipFully(new BufferedInputStream(utf8Bytes),
                                   charsToSkip);
            this.posCache.updateCachedPos(charPos, bytePos);
        }
        return bytePos;
    }

    /**
     * Returns the update count of this Clob.
     *
     * @return Update count.
     */
    public long getUpdateCount() {
        return bytes.getUpdateCount();
    }

    /**
     * Constructs and returns a <code>Writer</code> for the CLOB value.
     *
     * @param pos the initial position in bytes for the <code>Writer</code>
     * @return A <code>Writer</code> to write to the CLOB value.
     * @throws IOException
     * @throws SQLException if the specified position is invalid
     */
    public synchronized Writer getWriter (long pos)
            throws IOException, SQLException {
        checkIfValid();
        // If pos is too large, an error will first be thrown when the writer
        // is written to. Is this okay behavior, is does it break the spec?
        if (pos < this.posCache.getCharPos()) {
            this.posCache.reset();
        }
        return new ClobUtf8Writer (this, pos);
    }

    /**
     * Constructs and returns a <code>Reader</code>.
     * @param pos initial position of the returned <code>Reader</code> in
     *      number of characters. Expected to be non-negative. The first
     *      character is at position <code>0</code>.
     * @return A <code>Reader</code> with the underlying <code>CLOB</code>
     *      value as source.
     * @throws IOException
     * @throws SQLException if the specified position is too big
     */
    public synchronized Reader getReader (long pos)
            throws IOException, SQLException {
        checkIfValid();
        if (pos < 1) {
            throw new IllegalArgumentException(
                "Position must be positive: " + pos);
        }
        // getCSD obtains a descriptor for the stream to allow the reader
        // to configure itself.
        Reader isr = new UTF8Reader(getCSD(), conChild,
                conChild.getConnectionSynchronization());

        long leftToSkip = pos -1;
        long skipped;
        while (leftToSkip > 0) {
            skipped = isr.skip(leftToSkip);
            // Since Reader.skip block until some characters are available,
            // a return value of 0 must mean EOF.
            if (skipped <= 0) {
                throw new EOFException("Reached end-of-stream prematurely");
            }
            leftToSkip -= skipped;
        }
        return isr;
    }

    /**
     * @see #getReader
     */
    public Reader getInternalReader(long characterPosition)
            throws IOException, SQLException {
        if (this.internalReader == null) {
            // getCSD obtains a descriptor for the stream to allow the reader
            // to configure itself.
            this.internalReader = new UTF8Reader(getCSD(), conChild,
                    conChild.getConnectionSynchronization());
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
     * Returns number of characters in the Clob.
     *
     * @return The length of the Clob in number of characters.
     * @throws IOException if accessing the underlying I/O resources fail
     */
    public synchronized long getCharLength() throws IOException {
        checkIfValid();
        if (cachedCharLength == 0) {
            cachedCharLength = UTF8Util.skipUntilEOF(
                    new BufferedInputStream(getRawByteStream()));
        }
        return cachedCharLength;
    }

    /**
     * Returns the cached character count for the Clob, if any.
     *
     * @return The number of characters in the Clob, or {@code -1} if unknown.
     */
    public synchronized long getCharLengthIfKnown() {
        checkIfValid();
        // Treat a cached value of zero as a special case.
        return (cachedCharLength == 0 ? -1 : cachedCharLength);
    }

    /**
     * Returns the size of the Clob in bytes.
     *
     * @return Number of bytes in the <code>CLOB</code> value.
     * @throws IOException if accessing the underlying I/O resources fail
     */
    public synchronized long getByteLength () throws IOException {
        checkIfValid();
        return this.bytes.getLength();
    }

    /**
     * Inserts a string at the given position.
     *
     * @param str the string to insert
     * @param insertionPoint the character position to insert the string at
     * @return Number of characters inserted.
     * @throws EOFException if the position is larger than the Clob length +1
     * @throws IOException if accessing the underlying I/O resources fail
     * @throws SQLException if accessing the underlying resources fail
     */
    public synchronized long insertString (String str, long insertionPoint)
                 throws IOException, SQLException {
        checkIfValid();
        if (insertionPoint < 1) {
            throw new IllegalArgumentException(
                "Position must be positive: " + insertionPoint);
        }
        long prevLength = cachedCharLength;
        updateInternalState(insertionPoint);
        long byteInsertionPoint = getBytePosition(insertionPoint);
        long curByteLength = this.bytes.getLength();
        byte[] newBytes = getByteFromString(str);
        // See if we are appending or replacing bytes.
        if (byteInsertionPoint == curByteLength) {
            try {
                this.bytes.write(newBytes, 0, newBytes.length,
                    byteInsertionPoint);
            } catch (StandardException se) {
                throw Util.generateCsSQLException(se);
            }
        } else {
            // Calculate end position of the byte block to replace.
            // Either we only replace bytes, or we replace and append bytes.
            long endPos;
            try {
                endPos = getBytePosition(insertionPoint + str.length());
                // Must reset the position cache here, as the last obtained
                // one may be invalid after we replace the bytes (because of
                // the variable number of bytes per char).
                this.posCache.updateCachedPos(
                    insertionPoint, byteInsertionPoint);
            } catch (EOFException eofe) {
                endPos = curByteLength; // We replace and append.
            }
            try {
                this.bytes.replaceBytes(newBytes, byteInsertionPoint, endPos);
            } catch (StandardException se) {
                throw Util.generateCsSQLException(se);
            }
            // Update the length if we know the previous length.
            if (prevLength != 0) {
                long newLength = (insertionPoint -1) + str.length();
                if (newLength > prevLength) {
                    cachedCharLength = newLength; // The Clob grew.
                } else {
                    // We only wrote over existing characters, length unchanged.
                    cachedCharLength = prevLength;
                }
            }
        }
        return str.length();
    }

    /**
     * Tells if this Clob has been released.
     *
     * @return {@code true} if released, {@code false} if not.
     */
    public synchronized boolean isReleased() {
        return released;
    }

    /**
     * Tells if this Clob is intended to be writable.
     *
     * @return <code>true</code>
     */
    public boolean isWritable() {
        return true;
    }

    /**
     * Truncate the Clob to the specifiec size.
     *
     * @param newCharLength the new length, in characters, of the Clob
     * @throws IOException if accessing the underlying I/O resources fails
     */
    public synchronized void truncate(long newCharLength)
            throws IOException, SQLException {
        checkIfValid();
        try {
            // Get the length in bytes.
            long byteLength = UTF8Util.skipFully (
                    new BufferedInputStream(getRawByteStream()), newCharLength);
            this.bytes.truncate(byteLength);
            // Reset the internal state, and then update the length.
            updateInternalState(newCharLength);
            cachedCharLength = newCharLength;
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
    }

    /**
     * Converts a string into the modified UTF-8 byte encoding.
     *
     * @param str string to represent with modified UTF-8 encoding
     * @return Byte array representing the string in modified UTF-8 encoding.
     */
    private byte[] getByteFromString (String str) {
        //create a buffer with max size possible
        byte [] buffer = new byte [3 * str.length()];
        int len = 0;
        //start decoding
        for (int i = 0; i < str.length(); i++) {
            int c = str.charAt (i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer[len++] = (byte) c;
            }
            else if (c > 0x07FF) {
                buffer[len++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[len++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                buffer[len++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
            else {
                buffer[len++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                buffer[len++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        byte [] buff = new byte [len];
        System.arraycopy (buffer, 0, buff, 0, len);
        return buff;
    }

    /**
     * Copies the content of another Clob into this one.
     *
     * @param clob the Clob to copy from
     * @throws IOException if accessing I/O resources fail (both read and write)
     * @throws SQLException if accessing underlying resources fail
     */
    private void copyClobContent(InternalClob clob)
            throws IOException, SQLException {
        try {
            long knownLength = clob.getCharLengthIfKnown();
            if (knownLength == -1) {
                // Decode UTF-8 data and copy until EOF, obtain char length.
                this.cachedCharLength = this.bytes.copyUtf8Data(
                        clob.getRawByteStream(), Long.MAX_VALUE);
            } else {
                // We already know the character length, and can copy raw bytes
                // without decoding the UTF-8 data.
                // Specify LONG.MAX_VALUE to copy data until EOF.
                this.cachedCharLength = knownLength;
                this.bytes.copyData(clob.getRawByteStream(), Long.MAX_VALUE);
            }
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
    }

    /**
     * Copies the content of another Clob into this one.
     *
     * @param clob the Clob to copy from
     * @param charLength number of chars to copy
     * @throws EOFException if the length of the stream is shorter than the
     *      specified length
     * @throws IOException if accessing I/O resources fail (both read and write)
     * @throws SQLException if accessing underlying resources fail
     */
    private void copyClobContent(InternalClob clob, long charLength)
            throws IOException, SQLException {
        try {
            long knownLength = clob.getCharLengthIfKnown();
            if (knownLength > charLength || knownLength == -1) {
                // Decode and copy the requested number of chars.
                this.cachedCharLength = this.bytes.copyUtf8Data(
                    clob.getRawByteStream(), charLength);
            } else if (knownLength == charLength) {
                this.cachedCharLength = knownLength;
                // Copy raw bytes until EOF.
                // Special case optimization, avoids UTF-8 decoding.
                this.bytes.copyData(clob.getRawByteStream(), Long.MAX_VALUE);
            } else {
                // The known length must be smaller than the requested length.
                throw new EOFException();
            }
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
    }

    /**
     * Makes sure the Clob has not been released.
     * <p>
     * All operations are invalid on a released Clob.
     *
     * @throws IllegalStateException if the Clob has been released
     */
    private final void checkIfValid() {
        if (this.released) {
            throw new IllegalStateException(
                "The Clob has been released and is not valid");
        }
    }

    /**
     * Updates the internal state after a modification has been performed on
     * the Clob content.
     * <p>
     * Currently the state update consists of dicarding the internal reader to
     * stop it from delivering stale data, to reset the byte/char position
     * cache if necessary, and to reset the cached length.
     *
     * @param charChangePosition the position where the Clob change started
     */
    private final void updateInternalState(long charChangePosition) {
        // Discard the internal reader, don't want to deliver stale data.
        if (internalReader != null) {
            internalReader.close();
            internalReader = null;
            unclosableInternalReader = null;
        }
        // Reset the cache if last cached position has been cut away.
        if (charChangePosition < posCache.getCharPos()) {
            posCache.reset();
        }
        // Reset the cached length.
        cachedCharLength = 0;
    }

    /**
     * Returns a character stream descriptor for the stream.
     * <p>
     * All streams from the underlying source ({@code LOBStreamControl}) are
     * position aware and can be moved to a specific byte position cheaply.
     * The maximum length is not really needed, nor known, at the moment, so
     * the maximum allowed Clob length in Derby is used.
     *
     * @return A character stream descriptor.
     * @throws IOException if obtaining the length of the stream fails
     */
    private final CharacterStreamDescriptor getCSD()
            throws IOException {
        return new CharacterStreamDescriptor.Builder().
                    // Static values.
                    positionAware(true).
                    maxCharLength(TypeId.CLOB_MAXWIDTH).
                    // Values depending on content and/or state.
                    stream(bytes.getInputStream(0)).
                    bufferable(bytes.getLength() > 4096).
                    byteLength(bytes.getLength()).
                    charLength(cachedCharLength). // 0 means unknown
                    build();
    }

    /**
     * A simple class to hold the byte position for a character position.
     * <p>
     * The implementation is very simple and is basically intended to speed up
     * writing a sequence of consequtive characters one character at a time.
     * Even though this should be avoided if possible, the penalty of updating a
     * large Clob this way and finding the correct byte position by navigating
     * from the start of the byte stream each time is so severe that a simple
     * caching mechanism should be in place. Note that for other encodings than
     * UTF-8, this might not be a problem if the mapping between character
     * position and byte position is one-to-one.
     * <p>
     * Note that to ensure consistency between character and byte positions,
     * access to this class must be synchronized externally to avoid caller 1
     * getting the character position, then caller 2 updates the cached values
     * and then caller 1 gets the updated byte position.
     */
    //@NotThreadSafe
    private static class CharToBytePositionCache {
        private long charPos = 1L;
        private long bytePos = 0L;

        CharToBytePositionCache() {}

        /**
         * Returns the last cached byte position.
         *
         * @return The byte position for the last cached character position.
         */
        long getBytePos() {
            return this.bytePos;
        }

        /**
         * Returns the last cached character position.
         *
         * @return The last cached character position.
         */
        long getCharPos() {
            return this.charPos;
        }

        /**
         * Updates the position cache.
         *
         * @param charPos the character position to cache the byte position for
         * @param bytePos byte position for the specified character position
         */
        void updateCachedPos(long charPos, long bytePos) {
            if (charPos -1 > bytePos) {
                throw new IllegalArgumentException("(charPos -1) cannot be " +
                    "greater than bytePos; " + (charPos -1) + " > " + bytePos);
            }
            this.charPos = charPos;
            this.bytePos = bytePos;
        }

        /**
         * Resets the position cache.
         */
        void reset() {
            this.charPos = 1L;
            this.bytePos = 0L;
        }
    } // End internal class CharToBytePositionCache
}

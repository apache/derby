/*

   Derby - Class org.apache.derby.impl.jdbc.UTF8Reader

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

package org.apache.derby.impl.jdbc;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.io.EOFException;
import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.PositionedStream;

/**
 * Class for reading characters from streams encoded in the modified UTF-8
 * format.
 * <p>
 * Note that we often operate on a special Derby stream.
 * A Derby stream is possibly different from a "normal" stream in two ways;
 * an encoded length is inserted at the head of the stream, and if the encoded
 * length is <code>0</code> a Derby-specific end of stream marker is appended
 * to the data.
 * <p>
 * If the underlying stream is capable of repositioning itself on request,
 * this class supports multiple readers on the same source stream in such a way
 * that the various readers do not interfere with each other (except for
 * serializing access). Each reader instance will have its own pointer into the
 * stream, and request that the stream repositions itself before calling
 * read/skip on the stream.
 *
 * @see PositionedStoreStream
 */
public final class UTF8Reader extends Reader
{
    private static final String READER_CLOSED = "Reader closed";
    /** Maximum size in number of chars for the internal character buffer. */
    private static final int MAXIMUM_BUFFER_SIZE = 8*1024; // 8 KB

    /** The underlying data stream. */
    private InputStream in;
    /** Stream that can reposition itself on request (may be {@code null}). */
    private final PositionedStream positionedIn;
    /**
     * Store the last visited position in the store stream, if it is capable of
     * repositioning itself ({@code positionedIn != null}).
     */
    private long rawStreamPos = 0L;
    /** Number of bytes read from the stream, including any header bytes. */
    private long       utfCount;        // bytes
    /** Number of characters read from the stream. */
    private long       readerCharCount; // characters

    /** Internal character buffer storing characters read from the stream. */
    private final char[]   buffer;
    /** The number of characters in the internal buffer. */
    private int            charactersInBuffer; // within buffer
    /** The position of the next character to read in the internal buffer. */
    private int            readPositionInBuffer;

    /** Tells if this reader has been closed. */
    private boolean noMoreReads;

    /** 
     * A reference to the parent object of the stream.
     * <p>
     * The reference is kept so that the parent object can't get
     * garbage collected until we are done with the stream.
     */
    private ConnectionChild parent;

    /**
     * Descriptor containing information about the stream.
     * Except for the current positions, the information in this object is
     * considered permanent and valid for the life-time of the stream.
     */
    private final CharacterStreamDescriptor csd;

    /**
     * Constructs a reader on top of the source UTF-8 encoded stream.
     *
     * @param csd a description of and reference to the source stream
     * @param conChild the parent object / connection child
     * @param sync synchronization object used when accessing the underlying
     *      data stream
     * @throws IOException if reading from the underlying stream fails
     */
    public UTF8Reader(CharacterStreamDescriptor csd, ConnectionChild conChild,
            Object sync)
            throws IOException {
        super(sync);
        this.csd = csd;
        this.positionedIn =
                (csd.isPositionAware() ? csd.getPositionedStream() : null);
        this.parent = conChild;

        int buffersize = calculateBufferSize(csd);
        this.buffer = new char[buffersize];

        if (csd.isPositionAware()) {
            // Check and save the stream state.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                        csd.getCurBytePos() == positionedIn.getPosition());
            }
            this.rawStreamPos = positionedIn.getPosition();
            // Make sure we start at the first data byte, not in the header.
            // The position will be changed on the next buffer fill.
            if (rawStreamPos < csd.getDataOffset()) {
                rawStreamPos = csd.getDataOffset();
            }
        } else {
            // Skip the header if required.
            if (csd.getCurBytePos() < csd.getDataOffset()) {
                csd.getStream().skip(csd.getDataOffset() - csd.getCurBytePos());
            }
        }

        // Buffer stream for improved performance, if appropriate.
        if (csd.isBufferable()) {
            this.in = new BufferedInputStream(csd.getStream(), buffersize);
        } else {
            this.in = csd.getStream();
        }
        // Add the header portion to the utfCount.
        utfCount = csd.getDataOffset();
    }

    /*
     * Reader implemention.
     */

    /**
     * Reads a single character from the stream.
     * 
     * @return A character or <code>-1</code> if end of stream has been reached.
     * @throws IOException if the stream has been closed, or an exception is
     *      raised while reading from the underlying stream
     */
    public int read() throws IOException
    {
        synchronized (lock) {

            // check if closed..
            if (noMoreReads)
                throw new IOException(READER_CLOSED);

            if (readPositionInBuffer >= charactersInBuffer) {
                if (fillBuffer()) {
                    return -1;
                }
            }

            return buffer[readPositionInBuffer++];
        }
    }

    /**
     * Reads characters into an array.
     * 
     * @return The number of characters read, or <code>-1</code> if the end of
     *      the stream has been reached.
     */ 
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        synchronized (lock) {
            // check if closed..
            if (noMoreReads)
                throw new IOException(READER_CLOSED);

            if (readPositionInBuffer >= charactersInBuffer) {
                if (fillBuffer()) {
                    return -1;
                }
            }

            int remainingInBuffer = charactersInBuffer - readPositionInBuffer;

            if (len > remainingInBuffer)
                len = remainingInBuffer;

            System.arraycopy(buffer, readPositionInBuffer, cbuf, off, len);
            readPositionInBuffer += len;

            return len;
        }
    }

    /**
     * Skips characters.
     * 
     * @param len the numbers of characters to skip
     * @return The number of characters actually skipped.
     * @throws IllegalArgumentException if the number of characters to skip is
     *      negative
     * @throws IOException if accessing the underlying stream fails
     */
    public long skip(long len) throws IOException {
        if (len < 0) {
            throw new IllegalArgumentException(
                "Number of characters to skip must be positive: " + len);
        }
        synchronized (lock) {
            // check if closed..
            if (noMoreReads)
                throw new IOException(READER_CLOSED);

            if (readPositionInBuffer >= charactersInBuffer) {
                // do somthing
                if (fillBuffer()) {
                    return 0L;
                }
            }

            int remainingInBuffer = charactersInBuffer - readPositionInBuffer;

            if (len > remainingInBuffer)
                len = remainingInBuffer;

            readPositionInBuffer += len;

            return len;
        }

    }

    /**
     * Close the reader, disallowing further reads.
     */
    public void close()
    {
        synchronized (lock) {
            closeIn();
            parent = null;
            noMoreReads = true;
        }
    }

    /*
     * Methods just for Derby's JDBC driver
     */

    /**
     * Reads characters from the stream.
     * <p>
     * Due to internal buffering a smaller number of characters than what is
     * requested might be returned. To ensure that the request is fulfilled,
     * call this method in a loop until the requested number of characters is
     * read or <code>-1</code> is returned.
     * 
     * @param sb the destination buffer
     * @param len maximum number of characters to read
     * @return The number of characters read, or <code>-1</code> if the end of
     *      the stream is reached.
     */
    public int readInto(StringBuffer sb, int len) throws IOException {

        synchronized (lock) {
            if (readPositionInBuffer >= charactersInBuffer) {
                if (fillBuffer()) {
                    return -1;
                }
            }

            int remainingInBuffer = charactersInBuffer - readPositionInBuffer;

            if (len > remainingInBuffer)
                len = remainingInBuffer;
            sb.append(buffer, readPositionInBuffer, len);

            readPositionInBuffer += len;

            return len;
        }
    }

    /**
     * Reads characters into an array as ASCII characters.
     * <p>
     * Due to internal buffering a smaller number of characters than what is
     * requested might be returned. To ensure that the request is fulfilled,
     * call this method in a loop until the requested number of characters is
     * read or <code>-1</code> is returned.
     * <p>
     * Characters outside the ASCII range are replaced with an out of range
     * marker.
     * 
     * @param abuf the buffer to read into
     * @param off the offset into the destination buffer
     * @param len maximum number of characters to read
     * @return The number of characters read, or <code>-1</code> if the end of
     *      the stream is reached.
     */
    int readAsciiInto(byte[] abuf, int off, int len) throws IOException {

        synchronized (lock) {
            if (readPositionInBuffer >= charactersInBuffer) {
                if (fillBuffer()) {
                    return -1;
                }
            }

            int remainingInBuffer = charactersInBuffer - readPositionInBuffer;

            if (len > remainingInBuffer)
                len = remainingInBuffer;

            char[] lbuffer = buffer;
            for (int i = 0; i < len; i++) {
                char c = lbuffer[readPositionInBuffer + i];
                byte cb;
                if (c <= 255)
                    cb = (byte) c;
                else
                    cb = (byte) '?'; // Question mark - out of range character.

                abuf[off + i] = cb;
            }

            readPositionInBuffer += len;

            return len;
        }
    }

    /*
     * internal implementation
     */

    /**
     * Close the underlying stream if it is open.
     */
    private void closeIn() {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ioe) {
                // Ignore exceptions thrown on close.
                // [TODO] Maybe we should log these?
            } finally {
                in = null;
            }
        }
    }

    /**
     * Convenience method generating an {@link UTFDataFormatException} and
     * cleaning up the reader state.
     */
    private IOException utfFormatException(String s) {
        noMoreReads = true;
        closeIn();
        return new UTFDataFormatException(s);
    }

    /**
     * Fills the internal character buffer by decoding bytes from the stream.
     * 
     * @return <code>true</code> if the end of the stream is reached,
     *      <code>false</code> if there is apparently more data to be read.
     */
    //@GuardedBy("lock")
    private boolean fillBuffer() throws IOException
    {
        if (in == null)
            return true;

        charactersInBuffer = 0;
        readPositionInBuffer = 0;

        try {
        try {
            parent.setupContextStack();
            // If we are operating on a positioned stream, reposition it to
            // continue reading at the position we stopped last time.
            if (this.positionedIn != null) {
                try {
                    this.positionedIn.reposition(this.rawStreamPos);
                } catch (StandardException se) {
                    throw Util.generateCsSQLException(se);
                }
            }
            // Keep track of how much we are allowed to read.
            final long utfLen = csd.getByteLength();
            final long maxFieldSize = csd.getMaxCharLength();
readChars:
        while (
                (charactersInBuffer < buffer.length) &&
                ((utfCount < utfLen) || (utfLen == 0)) &&
                ((maxFieldSize == 0) || (readerCharCount < maxFieldSize))
              )
        {
            int c = in.read();
            if (c == -1) {
                if (utfLen == 0) {
                    // Close the stream if it cannot be reset.
                    if (!csd.isPositionAware()) {
                        closeIn();
                    }
                    break readChars;
                }
                throw utfFormatException("Reached EOF prematurely, " +
                    "read " + utfCount + " out of " + utfLen + " bytes");
            }

            int finalChar;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    // 0xxxxxxx
                    utfCount++;
                    finalChar = c;
                    break;

                case 12: case 13:
                    {
                    // 110x xxxx   10xx xxxx
                    utfCount += 2;
                    int char2 = in.read();
                    if (char2 == -1)
                        throw utfFormatException("Reached EOF when reading " +
                            "second byte in a two byte character encoding; " +
                            "byte/char position " + utfCount + "/" +
                            readerCharCount);

                    if ((char2 & 0xC0) != 0x80)
                        throw utfFormatException("Second byte in a two byte" +
                            "character encoding invalid: (int)" + char2 +
                            ", byte/char pos " + utfCount + "/" +
                            readerCharCount);
                    finalChar = (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                    }

                case 14:
                    {
                    // 1110 xxxx  10xx xxxx  10xx xxxx
                    utfCount += 3;
                    int char2 = in.read();
                    int char3 = in.read();
                    if (char2 == -1 || char3 == -1)
                        throw utfFormatException("Reached EOF when reading " +
                            "second/third byte in a three byte character " +
                            "encoding; byte/char position " + utfCount + "/" +
                            readerCharCount);

                    if ((c == 0xE0) && (char2 == 0) && (char3 == 0))
                    {
                        if (utfLen == 0) {
                            // we reached the end of a long string,
                            // that was terminated with
                            // (11100000, 00000000, 00000000)
                            // Close the stream if it cannot be reset.
                            if (!csd.isPositionAware()) {
                                closeIn();
                            }
                            break readChars;
                        }
                        throw utfFormatException("Internal error: Derby-" +
                            "specific EOF marker read");
                    }

                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw utfFormatException("Second/third byte in a " +
                            "three byte character encoding invalid: (int)" +
                            char2 + "/" + char3 + ", byte/char pos " +
                            utfCount + "/" + readerCharCount);

                    finalChar = (((c & 0x0F) << 12) |
                               ((char2 & 0x3F) << 6) |
                               ((char3 & 0x3F) << 0));
                    }
                    break;

                default:
                    // 10xx xxxx,  1111 xxxx
                    throw utfFormatException("Invalid UTF encoding at " +
                        "byte/char position " + utfCount + "/" +
                        readerCharCount + ": (int)" + c);
            }

            buffer[charactersInBuffer++] = (char) finalChar;
            readerCharCount++;
        }
        if (utfLen != 0 && utfCount > utfLen)
            throw utfFormatException("Incorrect encoded length in stream, " +
                "expected " + utfLen + ", have " + utfCount + " bytes");

        if (charactersInBuffer != 0) {
            if (this.positionedIn != null) {
                // Save the last visisted position so we can start reading where
                // we let go the next time we fill the buffer.
                this.rawStreamPos = this.positionedIn.getPosition();
            }
            return false;
        }

        // Close the stream if it cannot be reset.
        if (!csd.isPositionAware()) {
            closeIn();
        }
        return true;
        } finally {
            parent.restoreContextStack();
        }
        } catch (SQLException sqle) {
            throw Util.newIOException(sqle);
        }
    }

    /**
     * Resets the reader.
     * <p>
     * This method is used internally to achieve better performance.
     * @see #reposition(long)
     *
     * @throws IOException if resetting or reading from the stream fails
     * @throws StandardException if resetting the stream fails
     */
    private void resetUTF8Reader()
            throws IOException, StandardException {
        // Skip the length encoding bytes.
        this.positionedIn.reposition(csd.getDataOffset());
        this.utfCount = this.rawStreamPos = this.positionedIn.getPosition();
        // If bufferable, discard buffered stream and create a new one.
        if (csd.isBufferable()) {
            this.in = new BufferedInputStream(csd.getStream(), buffer.length);
        }
        this.readerCharCount = 0L;
        this.charactersInBuffer = this.readPositionInBuffer = 0;
    }

    /**
     * Repositions the stream so that the next character read will be the
     * character at the requested position.
     * <p>
     * There are three types of repositioning, ordered after increasing cost:
     * <ol> <li>Reposition within current character buffer (small hops forwards
     *          and potentially backwards - in range 1 char to
     *          {@code MAXIMUM_BUFFER_SIZE} chars)</li>
     *      <li>Forward stream from current position (hops forwards)</li>
     *      <li>Reset stream and skip data (hops backwards)</li>
     * </ol>
     *
     * @param requestedCharPos 1-based requested character position
     * @throws IOException if resetting or reading from the stream fails
     * @throws StandardException if resetting the stream fails
     */
    void reposition(long requestedCharPos)
            throws IOException, StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(this.positionedIn != null);
            SanityManager.ASSERT(requestedCharPos > 0);
        }
        if (requestedCharPos <= readerCharCount - charactersInBuffer) {
            // The stream must be reset, because the requested position is
            // before the current lower buffer boundary.
            resetUTF8Reader();
        }

        long currentCharPos =
            readerCharCount - charactersInBuffer + readPositionInBuffer;
        long difference = (requestedCharPos - 1) - currentCharPos;

        if (difference <= 0) {
            // Move back in the buffer.
            readPositionInBuffer += difference;
        } else {
            // Skip forward.
            persistentSkip(difference);
        }
    }

    /**
     * Calculates an optimized buffer size.
     * <p>
     * The maximum size allowed is returned if the specified values don't give
     * enough information to say a smaller buffer size is preferable.
     *
     * @param csd stream descriptor
     * @return An (sub)optimal buffer size.
     */
    private final int calculateBufferSize(CharacterStreamDescriptor csd) {
        // Using the maximum buffer size will be optimal,
        // unless the data is smaller than the maximum buffer.
        int bufferSize = MAXIMUM_BUFFER_SIZE;
        long knownLength = csd.getCharLength();
        long maxCharLength = csd.getMaxCharLength();
        if (knownLength < 1) {
            // Unknown char length, use byte count instead (might be zero too).
            knownLength = csd.getByteLength();
        }
        if (knownLength > 0 && knownLength < bufferSize) {
            bufferSize = (int)knownLength;
        }
        if (maxCharLength > 0 && maxCharLength < bufferSize) {
            bufferSize = (int)maxCharLength;
        }
        return bufferSize;
    }

    /**
     * Skips the requested number of characters.
     *
     * @param toSkip number of characters to skip
     * @throws EOFException if there are too few characters in the stream
     * @throws IOException if reading from the stream fails
     */
    private final void persistentSkip(long toSkip)
            throws IOException {
        long remaining = toSkip;
        while (remaining > 0) {
            long skipped = skip(remaining);
            if (skipped == 0) {
                if (SanityManager.DEBUG) {
                    // Add details to the exception in sane builds.
                    throw new EOFException("Reached end-of-stream after " +
                        readerCharCount + " characters, " + remaining +
                        " remaining to skip");
                }
                throw new EOFException();
            }
            remaining -= skipped;
        }
    }
}

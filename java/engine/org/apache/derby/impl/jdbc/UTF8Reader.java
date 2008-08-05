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
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.Resetable;

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

    /** The underlying data stream. */
    private InputStream in;
    /** Store stream that can reposition itself on request. */
    private final PositionedStoreStream positionedIn;
    /** Store last visited position in the store stream. */
    private long rawStreamPos = 0L;
    /**
     * The expected number of bytes in the stream, if known.
     * <p>
     * A value of <code>0<code> means the length is unknown, and that the end
     * of the stream is marked with a Derby-specific end of stream marker.
     */
    private final long utfLen;          // bytes
    /** Number of bytes read from the stream. */
    private long       utfCount;        // bytes
    /** Number of characters read from the stream. */
    private long       readerCharCount; // characters
    /** 
     * The maximum number of characters allowed for the column
     * represented by the passed stream.
     * <p>
     * A value of <code>0</code> means there is no associated maximum length.
     */
    private final long maxFieldSize;    // characters

    /** Internal character buffer storing characters read from the stream. */
    private final char[]   buffer = new char[8 * 1024];
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
     * Constructs a reader and consumes the encoded length bytes from the
     * stream.
     * <p>
     * The encoded length bytes either state the number of bytes in the stream,
     * or it is <code>0</code> which informs us the length is unknown or could
     * not be represented and that we have to look for the Derby-specific
     * end of stream marker.
     * 
     * @param in the underlying stream
     * @param maxFieldSize the maximum allowed column length in characters
     * @param parent the parent object / connection child
     * @param synchronization synchronization object used when accessing the
     *      underlying data stream
     * 
     * @throws IOException if reading from the underlying stream fails
     * @throws SQLException if setting up or restoring the context stack fails
     */
    public UTF8Reader(
        InputStream in,
        long maxFieldSize,
        ConnectionChild parent,
        Object synchronization)
            throws IOException, SQLException
    {
        super(synchronization);
        this.maxFieldSize = maxFieldSize;
        this.parent = parent;

        parent.setupContextStack();
        try {
            synchronized (lock) { // Synchronize access to store.
                if (in instanceof PositionedStoreStream) {
                    this.positionedIn = (PositionedStoreStream)in;
                    // This stream is already buffered, and buffering it again
                    // this high up complicates the handling a lot. Must
                    // implement a special buffered reader to buffer again.
                    // Note that buffering this UTF8Reader again, does not
                    // cause any trouble...
                    this.in = in;
                    try {
                        this.positionedIn.resetStream();
                    } catch (StandardException se) {
                        throw Util.newIOException(se);
                    }
                } else {
                    this.positionedIn = null;
                    // Buffer this for improved performance.
                    this.in = new BufferedInputStream (in);
                }
                this.utfLen = readUnsignedShort();
                // Even if we are reading the encoded length, the stream may
                // not be a positioned stream. This is currently true when a
                // stream is passed in after a ResultSet.getXXXStream method.
                if (this.positionedIn != null) {
                    this.rawStreamPos = this.positionedIn.getPosition();
                }
            } // End synchronized block
        } finally {
            parent.restoreContextStack();
        }
    }

    /**
     * Constructs a <code>UTF8Reader</code> using a stream.
     * <p>
     * This consturctor accepts the stream size as parameter and doesn't
     * attempt to read the length from the stream.
     *
     * @param in the underlying stream
     * @param maxFieldSize the maximum allowed length for the associated column
     * @param streamSize size of the underlying stream in bytes
     * @param parent the connection child this stream is associated with
     * @param synchronization object to synchronize on
     */
    public UTF8Reader(
            InputStream in,
            long maxFieldSize,
            long streamSize,
            ConnectionChild parent,
            Object synchronization) {
        super(synchronization);
        this.maxFieldSize = maxFieldSize;
        this.parent = parent;
        this.utfLen = streamSize;
        this.positionedIn = null;

        if (SanityManager.DEBUG) {
            // Do not allow the inputstream here to be a Resetable, as this
            // means (currently, not by design...) that the length is encoded in
            // the stream and we can't pass that out as data to the user.
            SanityManager.ASSERT(!(in instanceof Resetable));
        }
        // Buffer this for improved performance.
        this.in = new BufferedInputStream(in);
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
                readPositionInBuffer = 0;
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
                readPositionInBuffer = 0;
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
                readPositionInBuffer = 0;
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
                readPositionInBuffer = 0;
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
                readPositionInBuffer = 0;
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
                    closeIn();
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
                            closeIn();
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

        closeIn();
        return true;
        } finally {
            parent.restoreContextStack();
        }
        } catch (SQLException sqle) {
            throw Util.newIOException(sqle);
        }
    }

    /**
     * Decode the length encoded in the stream.
     * 
     * This method came from {@link java.io.DataInputStream}
     * 
     * @return The number of bytes in the stream, or <code>0</code> if the
     *      length is unknown and the end of stream must be marked by the
     *      Derby-specific end of stream marker.
     */
    private final int readUnsignedShort() throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException("Reached EOF when reading" +
                                   "encoded length bytes");

        return (ch1 << 8) + (ch2 << 0);
    }
}

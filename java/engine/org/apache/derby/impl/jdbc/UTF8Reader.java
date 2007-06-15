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
*/
public final class UTF8Reader extends Reader
{

    private InputStream in;
    /** Stream store that can reposition itself on request. */
    private final PositionedStoreStream positionedIn;
    /** Store last visited position in the store stream. */
    private long rawStreamPos = 0L;
    private final long         utfLen;    // bytes
    private long        utfCount;        // bytes
    private long        readerCharCount; // characters
    private long        maxFieldSize;    // characeters

    private char[]        buffer = new char[8 * 1024];
    private int            charactersInBuffer;    // within buffer
    private int            readPositionInBuffer;

    private boolean        noMoreReads;

    // maintain a reference to the parent object so that it can't get 
    // garbage collected until we are done with the stream.
    private ConnectionChild      parent;

    public UTF8Reader(
    InputStream in,
    long maxFieldSize,
    ConnectionChild      parent,
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
                        IOException ioe = new IOException(se.getMessage());
                        ioe.initCause(se);
                        throw ioe;
                    }
                } else {
                    this.positionedIn = null;
                    // Buffer this for improved performance.
                    this.in = new BufferedInputStream (in);
                }
                this.utfLen = readUnsignedShort();
                // Even if we are reading the encoded length, the stream may
                // not be a positioned stream. This is currently true when a
                // stream is passed in after a ResetSet.getXXXStream method.
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
                Object synchronization)
                throws IOException {
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
    ** Reader implemention.
    */
    public int read() throws IOException
    {
        synchronized (lock) {

            // check if closed..
            if (noMoreReads)
                throw new IOException();

            if (readPositionInBuffer >= charactersInBuffer) {
                if (fillBuffer()) {
                    return -1;
                }
                readPositionInBuffer = 0;
            }

            return buffer[readPositionInBuffer++];
        }
    }

    public int read(char[] cbuf, int off, int len) throws IOException
    {
        synchronized (lock) {
            // check if closed..
            if (noMoreReads)
                throw new IOException();

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

    public long skip(long len) throws IOException {
        if (len < 0) {
            throw new IllegalArgumentException(
                "Number of characters to skip must be positive:" + len);
        }
        synchronized (lock) {
            // check if closed..
            if (noMoreReads)
                throw new IOException();

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

    public void close()
    {
        synchronized (lock) {
            closeIn();
            parent  = null;
            noMoreReads = true;
        }
    }

    /*
    ** Methods just for Derby's JDBC driver
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
    ** internal implementation
    */


    private void closeIn() {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ioe) {
            } finally {
                in = null;
            }
        }
    }
    private IOException utfFormatException(String s) {
        noMoreReads = true;
        closeIn();
        return new UTFDataFormatException(s);
    }

    private IOException utfFormatException() {
        noMoreReads = true;
        closeIn();
        return new UTFDataFormatException();
    }

    /**
        Fill the buffer, return true if eof has been reached.
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
                throw utfFormatException();
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
                        throw utfFormatException();

                    if ((char2 & 0xC0) != 0x80)
                        throw utfFormatException();          
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
                        throw utfFormatException();

                    if ((c == 0xE0) && (char2 == 0) && (char3 == 0))
                    {
                        if (utfLen == 0) {
                            // we reached the end of a long string,
                            // that was terminated with
                            // (11100000, 00000000, 00000000)
                            closeIn();
                            break readChars;
                        }
                        throw utfFormatException();
                    }

                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw utfFormatException();    

                    finalChar = (((c & 0x0F) << 12) |
                               ((char2 & 0x3F) << 6) |
                               ((char3 & 0x3F) << 0));
                    }
                    break;

                default:
                    // 10xx xxxx,  1111 xxxx
                    throw utfFormatException();          
            }

            buffer[charactersInBuffer++] = (char) finalChar;
            readerCharCount++;
        }
        if (utfLen != 0 && utfCount > utfLen) 
            throw utfFormatException("utfCount " + utfCount + " utfLen " + utfLen);          

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
            IOException ioe =
                new IOException(sqle.getSQLState() + ": " + sqle.getMessage());
            ioe.initCause(sqle);
            throw ioe;
        }
    }


    // this method came from java.io.DataInputStream
    private final int readUnsignedShort() throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();

        return (ch1 << 8) + (ch2 << 0);
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream

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

package org.apache.derbyTesting.functionTests.util.streams;

import java.io.InputStream;

import org.apache.derby.iapi.types.Resetable;

/**
 * A stream returning a cycle of the 26 lowercase letters of the modern Latin
 * alphabet.
 */
public class LoopingAlphabetStream
    extends InputStream
    implements Resetable {

    /**
     * Maximum size of buffer.
     * Balance between size and memory usage.
     */
    private static final int MAX_BUF_SIZE = 32*1024;
    private static final byte SPACE = (byte)' ';

    /** Length of the stream. */
    private final long length;
    private final int trailingBlanks;
    /** Remaining bytes in the stream. */
    private long remainingBlanks;
    private long remainingNonBlanks;
    private byte[] buffer = new byte[0];
    private final ByteAlphabet alphabet;

    /**
     * Create a looping modern latin alphabet stream of the specified length.
     *
     * @param length the number of characters (and also the number of bytes)
     */
    public LoopingAlphabetStream(long length) {
        this(length, 0);
    }

    /**
     * Creates a looping alphabet stream with the specified length, in which the
     * last characters are blanks.
     *
     * @param length total length of the stream
     * @param trailingBlanks number of trailing blanks
     */
    public LoopingAlphabetStream(long length, int trailingBlanks) {
        if (trailingBlanks > length) {
            throw new IllegalArgumentException("Number of trailing blanks " +
                    "cannot be greater than the total length.");
        }
        this.length = length;
        this.trailingBlanks = trailingBlanks;
        this.remainingNonBlanks = length - trailingBlanks;
        this.remainingBlanks = trailingBlanks;
        this.alphabet = ByteAlphabet.modernLatinLowercase();
        fillBuffer(alphabet.byteCount());
    }

    /**
     * Create a looping alphabet of the specified type and length.
     *
     * @param length the number of bytes in the stream
     * @param alphabet the alphabet to loop over
     */
    public LoopingAlphabetStream(long length, ByteAlphabet alphabet) {
        this(length, alphabet, 0);
    }

    public LoopingAlphabetStream(long length,
                                 ByteAlphabet alphabet,
                                 int trailingBlanks) {
        this.length = length;
        this.trailingBlanks = trailingBlanks;
        this.remainingNonBlanks = length - trailingBlanks;
        this.remainingBlanks = trailingBlanks;
        this.alphabet = alphabet;
        fillBuffer(alphabet.byteCount());
    }

    public int read() {
        if (remainingBlanks <= 0 && remainingNonBlanks <= 0) {
            return -1;
        }
        if (remainingNonBlanks <= 0) {
            remainingBlanks--;
            return SPACE;
        }
        remainingNonBlanks--;
        return (alphabet.nextByte() & 0xff);
    }

    public int read(byte[] buf, int off, int length) {
        if (remainingBlanks <= 0 && remainingNonBlanks <= 0) {
            return -1;
        }
        // We can only read as many bytes as there are in the stream.
        int nonBlankLength = (int)Math.min(remainingNonBlanks, (long)length);
        fillBuffer(nonBlankLength);
        int read = 0;
        // Find position of next letter in the buffer.
        int bOff = alphabet.nextByteToRead(0);
        if (nonBlankLength <= (buffer.length - bOff)) {
            System.arraycopy(buffer, bOff, buf, off, nonBlankLength);
            remainingNonBlanks -= nonBlankLength;
            read = nonBlankLength;
            alphabet.nextByteToRead(nonBlankLength);
        } else {
            // Must read several times from the buffer.
            int toRead = 0;
            while (remainingNonBlanks > 0 && read < nonBlankLength) {
                bOff = alphabet.nextByteToRead(toRead);
                toRead = Math.min(buffer.length - bOff, nonBlankLength - read);
                System.arraycopy(buffer, bOff, buf, off + read, toRead);
                remainingNonBlanks -= toRead;
                read += toRead;
            }
            bOff = alphabet.nextByteToRead(toRead);
        }
        if (read < length && remainingBlanks > 0) {
            read += fillBlanks(buf, off + read, length - read);
        }
        return read;
    }

    /**
     * Reset the stream.
     */
    public void reset() {
        remainingNonBlanks = length - trailingBlanks;
        remainingBlanks = trailingBlanks;
        alphabet.reset();
    }

    /**
     * Return remaining bytes in the stream.
     */
    public int available() {
        return (int)(remainingNonBlanks + remainingBlanks);
    }

    /**
     * Fill internal buffer of bytes (from character sequence).
     *
     * @param bufSize the wanted size, might be ignored if too big
     */
    private void fillBuffer(int bufSize) {
        if (bufSize > MAX_BUF_SIZE) {
            bufSize = MAX_BUF_SIZE;
        }
        if (bufSize <= buffer.length) {
            return;
        }
        int curOff = alphabet.nextByteToRead(0);
        // First letter in buffer is always the first letter in the alphabet.
        alphabet.reset();
        buffer = new byte[bufSize];
        for (int i=0; i < bufSize; i++) {
            buffer[i] = alphabet.nextByte();
        }
        // Must reset internal state of the alphabet, as we have not yet
        // delivered any bytes.
        alphabet.reset();
        alphabet.nextByteToRead(curOff);
    }

    private int fillBlanks(byte[] buf, int off, int length) {
        int i=0;
        for (; i < length; i++) {
            if (remainingBlanks > 0) {
                buf[off+i] = SPACE;
                remainingBlanks--;
            } else {
                break;
            }
        }
        return i;
    }

    // Resetable interface

    public void resetStream() {
        reset();
    }

    public void initStream() {
        reset();
    }

    public void closeStream() {
        // Does nothing for this stream.
    }
} // End class LoopingAlphabetStream

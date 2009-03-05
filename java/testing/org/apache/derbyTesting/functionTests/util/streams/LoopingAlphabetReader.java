/*

   Derby - Class org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader

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

import java.io.IOException;
import java.io.Reader;

/**
 * A stream returning characters by looping over an alphabet.
 */
public class LoopingAlphabetReader
    extends Reader {

    /**
     * Maximum size of buffer.
     * Balance between size and memory usage.
     */
    private static final int MAX_BUF_SIZE = 32*1024;
    /** The character used for blanks (SPACE). */
    private static final int SPACE = ' ';

    /** Number of characters in the reader. */
    private final long length;
    /** Number of blanks at the end of stream. */
    private final int trailingBlanks;
    /** Remaining non-blank characters. */
    private long remainingNonBlanks;
    /** Remaining blanks. */
    private long remainingBlanks;
    /** 
     * Internal buffer of characters. 
     * Used by the read-methods with a char[] argument. 
     */
    private char[] buffer = new char[0];
    /** The alphabet to draw letters from. */
    private final CharAlphabet alphabet;
    /** Tell if the reader is closed or not. */
    private boolean closed = false;

    /**
     * Create a looping modern latin alphabet reader of the specified length.
     *
     * @param length the number of characters
     */
    public LoopingAlphabetReader(long length) {
        this(length, 0);
    }

    /**
     * Create a looping modern latin alphabet of the specified length, with
     * the specified number of trailing blanks.
     *
     * The number of non-blank characters is
     * <code>length - trailingBlanks</code>.
     *
     * @param length total number of characters
     * @param trailingBlanks number of blank characters at the end
     */
    public LoopingAlphabetReader(long length, int trailingBlanks) {
        this.length = length;
        this.trailingBlanks = trailingBlanks;
        this.remainingNonBlanks = length - trailingBlanks;
        this.remainingBlanks = trailingBlanks;
        this.alphabet = CharAlphabet.modernLatinLowercase();
        fillBuffer(alphabet.charCount());
    }

    /**
     * Create a looping alphabet of the specified type and length.
     *
     * @param length the number of chars in the reader
     * @param alphabet the alphabet to loop over
     */
    public LoopingAlphabetReader(long length, CharAlphabet alphabet) {
        this(length, alphabet, 0);
    }

    /**
     * Create a looping alphabet of the specified type and length, with
     * the specified number of trailing blanks.
     *
     * The number of non-blank characters is
     * <code>length - trailingBlanks</code>.
     *
     * @param length total number of characters
     * @param alphabet the alphabet to draw characters from
     * @param trailingBlanks number of blank characters at the end
     */
    public LoopingAlphabetReader(long length,
                                 CharAlphabet alphabet,
                                 int trailingBlanks) {
        this.length = length;
        this.trailingBlanks = trailingBlanks;
        this.remainingNonBlanks = length - trailingBlanks;
        this.remainingBlanks = trailingBlanks;
        this.alphabet = alphabet.getClone();
        fillBuffer(alphabet.charCount());
    }

    public int read()
            throws IOException {
        ensureOpen();
        if (remainingBlanks <= 0 && remainingNonBlanks <= 0) {
            return -1;
        }
        if (remainingNonBlanks <= 0) {
            remainingBlanks--;
            return SPACE;
        }
        remainingNonBlanks--;
        return alphabet.nextCharAsInt();
    }

    public int read(char[] buf, int off, int length)
            throws IOException {
        ensureOpen();
        if (remainingBlanks <= 0 && remainingNonBlanks <= 0) {
            return -1;
        }
        // We can only read as many chars as there are in the stream.
        int nonBlankLength = Math.min((int)remainingNonBlanks, length);
        fillBuffer(nonBlankLength);
        int read = 0;
        // Find position of next char in the buffer.
        int cOff = alphabet.nextCharToRead(0);
        if (nonBlankLength <= (buffer.length - cOff)) {
            System.arraycopy(buffer, cOff, buf, off, nonBlankLength);
            remainingNonBlanks -= nonBlankLength;
            read = nonBlankLength;
            alphabet.nextCharToRead(nonBlankLength);
        } else {
            // Must read several times from the buffer.
            int toRead = 0;
            while (remainingNonBlanks > 0 && read < nonBlankLength) {
                cOff = alphabet.nextCharToRead(toRead);
                toRead = Math.min(buffer.length - cOff, nonBlankLength - read);
                System.arraycopy(buffer, cOff, buf, off + read, toRead);
                remainingNonBlanks -= toRead;
                read += toRead;
            }
            cOff = alphabet.nextCharToRead(toRead);
        }
        if (read < length && remainingBlanks > 0) {
            read += fillBlanks(buf, off + read, length - read);
        }
        return read;
    }

    /**
     * Reset the stream.
     */
    public void reset()
            throws IOException {
        ensureOpen();
        remainingNonBlanks = length - trailingBlanks;
        remainingBlanks = trailingBlanks;
        alphabet.reset();
    }

    /**
     * Return remaining characters in the stream.
     */
    public int available() {
        return (int)(remainingNonBlanks + remainingBlanks);
    }

    /**
     * Close the reader.
     */
    public void close() {
        this.closed = true;
    }

    /**
     * Fill internal buffer of character sequence.
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
        int curOff = alphabet.nextCharToRead(0);
        // First letter in buffer is always the first letter in the alphabet.
        alphabet.reset();
        buffer = new char[bufSize];
        for (int i=0; i < bufSize; i++) {
            buffer[i] = alphabet.nextChar();
        }
        // Must reset internal state of the alphabet, as we have not yet
        // delivered any bytes.
        alphabet.reset();
        alphabet.nextCharToRead(curOff);
    }

    /**
     * Fill array with blanks (SPACE).
     *
     * @param buf array to fill
     * @param off starting offset
     * @param length maximum number of blanks to fill in
     */
    private int fillBlanks(char[] buf, int off, int length) {
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

    /**
     * Ensure reader is open.
     *
     * @throws IOException if reader is closed
     */
    private final void ensureOpen()
            throws IOException {
        if (closed) {
            throw new IOException("Reader closed");
        }
    }
} // End class LoopingAlphabetReader

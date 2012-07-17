/*
    Derby - Class org.apache.derby.client.am.ByteArrayCombinerStream

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
package org.apache.derby.client.am;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A stream whose source is a list of byte arrays.
 *
 * This class was created when first implementing the JDBC 4 length less
 * overloads in the client driver. The reason was missing support for
 * streaming data with unknown length from the client to the server.
 *
 * The purpose of the stream is to avoid having to repeatedly copy data to grow
 * the byte buffer, or doing a single big copy to combine the byte arrays in
 * the end. This is important for the temporary solution, since we must
 * materialize the stream to find the length anyway.
 *
 * If there is less data available than the specified length, an exception is
 * thrown. Available data is determined by the length of the byte arrays, not
 * the contents of them. A byte array with all 0's is considered valid data.
 *
 * Besides from truncation, this stream does not change the underlying data in
 * any way.
 */
public class ByteArrayCombinerStream
    extends InputStream {

    /** A list of the arrays to combine. */
    private final ArrayList<byte[]> arrays;
    /** Length of the stream. */
    private final long specifiedLength;
    /** Global offset into the whole stream. */
    private long gOffset = 0;
    /** Index of the array we are currently reading from. */
    private int arrayIndex = 0;
    /** The array we are currently reading from. */
    private byte[] curArray;
    /** The local offset into the current array. */
    private int off = 0;

    /**
     * Create a stream whose source is a list of byte arrays.
     *
     * @param arraysIn an <code>ArrayList</code> with references to the source
     *      byte arrays. The references are copied to a new
     *      <code>ArrayList</code> instance.
     * @param length the length of the stream. Never published outside
     *      this object. Note that the length specified can be shorter
     *      than the actual number of bytes in the byte arrays.
     * @throws IllegalArgumentException if there is less data available than
     *      specified by <code>length</code>, or <code>length</code> is
     *      negative.
     */
    public ByteArrayCombinerStream(ArrayList<byte[]> arraysIn, long length) {
        // Don't allow negative length.
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " +
                    length);
        }
        this.specifiedLength = length;
        long tmpRemaining = length;
        if (arraysIn != null && arraysIn.size() > 0) {
            // Copy references to the byte arrays to a new ArrayList.
            int arrayCount = arraysIn.size();
            byte[] tmpArray;
            arrays = new ArrayList<byte[]>(arrayCount);
            // Truncate data if there are more bytes then specified.
            // Done to simplify boundary checking in the read-methods.
            for (int i=0; i < arrayCount && tmpRemaining > 0; i++) {
                tmpArray = arraysIn.get(i);
                if (tmpRemaining < tmpArray.length) {
                    // Create a new shrunk array.
                    byte[] shrunkArray =
                        new byte[(int)(tmpRemaining)];
                    System.arraycopy(tmpArray, 0,
                                     shrunkArray, 0, shrunkArray.length);
                    arrays.add(shrunkArray);
                    tmpRemaining -= shrunkArray.length;
                    break;
                } else {
                    // Add the whole array.
                    tmpRemaining -= tmpArray.length;
                    arrays.add(tmpArray);
                }
            }
            // Set the first array as the current one.
            curArray = nextArray();
        } else {
            // Specify gOffset so available returns 0;
            gOffset = length;
            arrays = null;
        }
        // If we don't have enough data, throw exception.
        if (tmpRemaining > 0) {
            throw new IllegalArgumentException("Not enough data, " + 
                    tmpRemaining + " bytes short of specified length " +
                    length);
        }
    }

    /**
     * Read a single byte.
     *
     * @return a byte, or <code>-1</code> if the end-of-stream is reached
     */
    public int read()
            throws IOException {
        if (curArray == null) {
            return -1;
        }
        if (off >= curArray.length) {
            curArray = nextArray();
            if (curArray == null) {
                return -1;
            }
        }
        gOffset++;
        return curArray[off++];
    }

    /**
     * Reads up to len bytes of data from the input stream into an array of
     * bytes.
     * An attempt is made to read as many as <code>len</code> bytes, but
     * a smaller number may be read. The number of bytes actually read
     * is returned as an integer.
     *
     * @param buf the array to copy bytes into
     * @param offset offset into the array
     * @param length the maximum number of bytes to read
     * @return the number of bytes read, or <code>-1</code> if end-of-stream
     *      is reached
     */
    public int read(byte[] buf, int offset, int length)
            throws IOException {
        int read = 0;
        if (curArray == null) {
            return -1;
        }
        if (length <= (curArray.length - off)) {
            System.arraycopy(curArray, off, buf, offset, length);
            off += length;
            gOffset += length;
            read = length;
        } else {
            int toRead = 0;
            while (curArray != null && read < length) {
                toRead = Math.min(curArray.length - off, length - read);
                System.arraycopy(curArray, off, buf, offset + read, toRead);
                read += toRead;
                gOffset += toRead;
                off += toRead;
                if ( off < curArray.length) {
                    break;
                }
                curArray = nextArray();
            }
        }
        return read;
    }

    /**
     * Return the number of available bytes.
     * The method assumes the specified length of the stream is correct.
     *
     * @return number of available bytes
     */
    public int available() {
        return (int)(specifiedLength - gOffset);
    }

    /**
     * Fetch the next array to read data from.
     * The reference in the <code>ArrayList</code> is cleared when the array
     * is "taken out".
     *
     * @return a <code>byte[]</code>-object, or <code>null</code> if there are
     *      no more arrays
     */
    private byte[] nextArray() {
        if (arrayIndex >= arrays.size()) {
            return null;
        }
        byte[] tmp = (byte[])arrays.get(arrayIndex);
        arrays.set(arrayIndex++, null);
        off = 0;
        return tmp;
    }
} // End of class ByteArrayCombinerStream

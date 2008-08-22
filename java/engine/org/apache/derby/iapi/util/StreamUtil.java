/*

   Derby - Class org.apache.derby.iapi.util.StreamUtil

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
package org.apache.derby.iapi.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility methods for handling streams.
 * It clarifies the using of skipping a stream. See DERBY-3770.
 */
public class StreamUtil {
    private static final int SKIP_BUFFER_SIZE = 1024 * 1024;

    /**
     * Skips until EOF, returns number of bytes skipped.
     * @param is
     *      InputStream to be skipped.
     * @return
     *      number of bytes skipped in fact.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'is' equals null.
     */
    public static long skipFully(InputStream is) throws IOException {
        if(is == null)
            throw new NullPointerException();

        long bytes = 0;
        long r = 0;
        while((r = skipPersistent(is, SKIP_BUFFER_SIZE)) > 0){
            bytes += r;
        }

        return bytes;
    }

    /**
     * Skips requested number of bytes,
     * throws EOFException if there is too few bytes in the stream.
     * @param is
     *      InputStream to be skipped.
     * @param skippedBytes
     *      number of bytes to skip. if skippedBytes <= zero, do nothing.
     * @throws EOFException
     *      if EOF meets before requested number of bytes are skipped.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'is' equals null.
     */
    public static void skipFully(InputStream is, long skippedBytes)
    throws IOException {
        if(is == null)
            throw new NullPointerException();

        if(skippedBytes <= 0)
            return;

        long bytes = skipPersistent(is, skippedBytes);

        if(bytes < skippedBytes)
            throw new EOFException();
    }

    /**
     * Tries harder to skip the requested number of bytes.
     * <p>
     * Note that even if the method fails to skip the requested number of bytes,
     * it will not throw an exception. If this happens, the caller can be sure
     * that end-of-stream has been reached.
     *
     * @param in byte stream
     * @param bytesToSkip the number of bytes to skip
     * @return The number of bytes skipped.
     * @throws IOException if reading from the stream fails
     */
    static final long skipPersistent(InputStream in, long bytesToSkip)
            throws IOException {
        long skipped = 0;
        while (skipped < bytesToSkip) {
            long skippedNow = in.skip(bytesToSkip - skipped);
            if (skippedNow <= 0) {
                if (in.read() == -1) {
                    // EOF, return what we have and leave it up to caller to
                    // decide what to do about it.
                    break;
                } else {
                    skippedNow = 1; // Added to count below.
                }
            }
            skipped += skippedNow;
        }
        return skipped;
    }
}

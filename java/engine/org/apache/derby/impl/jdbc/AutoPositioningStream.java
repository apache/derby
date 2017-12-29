/*

   Derby - Class org.apache.derby.impl.jdbc.AutoPositioningStream

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

import org.apache.derby.shared.common.error.StandardException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This Stream is a wrapper for PositionedStoreStream to set the position
 * correctly before performing any operation on it. 
 * All the read and skip methods ensure that the PositionedStoreStream
 * is set to right position before actually performing these operations.
 * PositionedStoreStream is accessed within synchronized block to ensure
 * exclusive access to it.
 *
 * This class must be constructed while synchronizing on 
 * ConnectionChild.getConnectionSynchronization
 */
final class AutoPositioningStream extends BinaryToRawStream {

    /** ConnectionChild to get synchronizion object */
    private final ConnectionChild conChild;
    private long pos;
    private final PositionedStoreStream positionedStream;

    /**
     * Constructs AutoPositioningStream object. This constructor must
     * be called from block synchronized on 
     * conChild.getConnectionSynchronization.
     * @param conChild  ConnectionChild to get synchronization object
     *                  before accessing PositionedStoreStream
     * @param in        InputStream
     * @param parent    Parent of the stream to prevent it from being
     *                  gc.
     * @throws IOException if an I/O error occurs
     */
    AutoPositioningStream(ConnectionChild conChild, 
                InputStream in, Object parent) throws IOException {
        //set the stream to actual data 
        //BinaryToRawStream will skip the initial length info
        super (in, parent);
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT (in instanceof PositionedStoreStream,
                    "Unexpected stream");
        }
        positionedStream = (PositionedStoreStream) in;
        pos = positionedStream.getPosition();
        this.conChild = conChild;
    }

    /**
     * Reads a single byte from the underlying stream.
     *
     * @return The next byte of data, or -1 if the end of the stream is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read() throws IOException {
        synchronized (conChild.getConnectionSynchronization()) {
            try {
                setPosition ();
            }
            catch (EOFException e) {
                return -1;
            }
            int ret = positionedStream.read();
            if (ret >= 0)
                pos++;
            return ret;
        }
    }

    /**
     * Reads a number of bytes from the underlying stream and stores them in the
     * specified byte array at the specified offset.
     *
     * @return The actual number of bytes read, or -1 if the end of the stream
     *      is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] b, int off, int len) throws IOException {
        synchronized (conChild.getConnectionSynchronization()) {
            try {
                setPosition ();
            }
            catch (EOFException e) {
                return -1;
            }
            int ret = positionedStream.read(b, off, len);
            if (ret > 0)
                pos +=ret;
            return ret;
        }
    }

    /**
     * Skips up to the specified number of bytes from the underlying stream.
     *
     * @return The actual number of bytes skipped.
     * @throws IOException if an I/O error occurs
     */
    public long skip(long n) throws IOException {
        synchronized (conChild.getConnectionSynchronization()) {
            setPosition ();
            long ret = positionedStream.skip(n);
            pos += ret;
            return ret;
        }
    }

    /**
     * Reads a number of bytes from the underlying stream and stores them in the
     * specified byte array.
     *
     * @return The actual number of bytes read, or -1 if the end of the stream
     *      is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] b) throws IOException {
        return read (b, 0, b.length);
    }
    
    /**
     * Checks if positionedStream's position was changed since 
     * last used, sets the position to right position if its 
     * changed.
     */
    private void setPosition () throws IOException {
        try {
            if (pos != positionedStream.getPosition()) {
                positionedStream.reposition (pos);
            }
        }
        catch (StandardException se) {
            throw Util.newIOException(se);
        }
    }
}

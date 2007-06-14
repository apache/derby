/*
 
   Derby - Class org.apache.derby.client.am.BlobLocatorOutputStream
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derby.client.am;
import java.io.IOException;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * An <code>OutputStream</code> that will use an locator to write
 * bytes to the Blob value on the server.
 * <p>
 * Closing a <code>BlobLocatorOutputStream</code> has no effect. The
 * methods in this class can be called after the stream has been
 * closed without generating an <code>IOException</code>.
 * <p>
 * This <code>OutputStream</code> implementation is pretty basic.  No
 * buffering of data is done.  Hence, for efficieny #write(byte[])
 * should be used instead of #write(int).  
 */
public class BlobLocatorOutputStream extends java.io.OutputStream {
    
    /**
     * Create an <code>OutputStream</code> for writing to the
     * <code>Blob</code> value represented by the given locator based
     * <code>Blob</code> object.
     * @param connection connection to be used to write to the
     *        <code>Blob</code> value on the server
     * @param blob <code>Blob</code> object that contains locator for
     *        the <code>Blob</code> value on the server.
     * @param pos the position in the <code>BLOB</code> value at which
     *        to start writing; the first position is 1
     * @throws org.apache.derby.client.am.SqlException 
     */
    public BlobLocatorOutputStream(Connection connection, Blob blob, long pos)
        throws SqlException
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(blob.isLocator());
        }

        if (pos-1 > blob.sqlLength()) {
            throw new IndexOutOfBoundsException();
        }
        
        this.connection = connection;
        this.blob = blob;
        this.currentPos = pos;
    }

    /**
     * @see java.io.OutputStream#write(int)
     *
     * This method writes one byte at a time to the server. For more 
     * efficient writing, use #write(byte[]).
     */
    public void write(int b) throws IOException            
    {
        byte[] ba = {(byte )b};
        writeBytes(ba);
    }

    /**
     * @see java.io.OutputStream#write(byte[])
     */
    public void write(byte[] b) throws IOException 
    {
        writeBytes(b);
    }
    
    
    
    /**
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    public void write(byte[] b, int off, int len) throws IOException 
    {
        if (len == 0) return;
        if ((off < 0) || (off > b.length) || (len < 0) || 
            (off+len > b.length) || (off+len < 0)) {
            throw new IndexOutOfBoundsException();
        } 
        
        byte[] ba = b;
        if ((off > 0) || (len < b.length)) { // Copy the part we will use
            ba = new byte[len];
            System.arraycopy(b, off, ba, 0, len);
        }
        writeBytes(ba);
    }

    /**
     * Write the <code>byte[]</code> to the <code>Blob</code> value on
     * the server; starting from the current position of this stream.
     * 
     * @param b The byte array containing the bytes to be written
     * @throws java.io.IOException Wrapped SqlException if writing
     *         to server fails.
     */
    private void writeBytes(byte[] b) throws IOException
    {
        try {         
            blob.setBytesX(currentPos, b, 0, b.length);
            currentPos += b.length;
        } catch (SqlException ex) {
            IOException ioEx= new IOException();
            ioEx.initCause(ex);
            throw ioEx;
        }
    }
    
    /**
     * Connection used to read Blob from server.
     */
    private final Connection connection;

    /**
     * The Blob to be accessed.
     */
    private final Blob blob;

    /**
     * Current position in the underlying Blob.
     * Blobs are indexed from 1
     */
    private long currentPos;

}

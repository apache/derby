/*
 
   Derby - Class org.apache.derby.client.am.BlobLocatorInputStream
 
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

import java.sql.CallableStatement;
import java.sql.SQLException;

import java.io.IOException;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * An <code>InputStream</code> that will use an locator to fetch the
 * Blob value from the server.  
 * <p>
 * Closing a <code>ByteArrayInputStream</code> has no effect. The methods in
 * this class can be called after the stream has been closed without
 * generating an <code>IOException</code>.
 * <p>
 * This <code>InputStream</code> implementation is pretty basic.  No
 * buffering of data is done.  Hence, for efficieny #read(byte[])
 * should be used instead of #read().  Marks are not supported, but it
 * should be pretty simple to extend the implementation to support
 * this.  A more efficient skip implementation should also be
 * straight-forward.
 */
public class BlobLocatorInputStream extends java.io.InputStream 
{

    /**
     * Create an <code>InputStream</code> for reading the
     * <code>Blob</code> value represented by the given locator based
     * <code>Blob</code> object.
     * @param connection connection to be used to read the
     *        <code>Blob</code> value from the server
     * @param blob <code>Blob</code> object that contains locator for
     *        the <code>Blob</code> value on the server.
     * @throws SqlException if an error occurs when obtaining the
     *         length of the <code>Blob</code>.
     */
    public BlobLocatorInputStream(Connection connection, Blob blob) 
        throws SqlException
    {        
        if (SanityManager.DEBUG) {
        	SanityManager.ASSERT(blob.isLocator());
        }
        this.connection = connection;
        this.blob = blob;
        this.currentPos = 1;
        this.maxPos = blob.sqlLength();
    }
    
    /**
     * Create an <code>InputStream</code> for reading the
     * <code>Blob</code> value represented by the given locator based
     * <code>Blob</code> object.
     * @param connection connection to be used to read the
     *        <code>Blob</code> value from the server
     * @param blob <code>Blob</code> object that contains locator for
     *        the <code>Blob</code> value on the server.
     * @param offset the offset in the <code>Blob</code> of the first
     *        byte to read.  
     * @param length the maximum number of bytes to read from
     *        the <code>Blob</code>.
     * @throws SqlException if an error occurs when obtaining the
     *         length of the <code>Blob</code>.
     */
    public BlobLocatorInputStream(Connection connection, Blob blob,
                                  long position, long length) 
        throws SqlException
    {        
        SanityManager.ASSERT(blob.isLocator());
        this.connection = connection;
        this.blob = blob;
        this.currentPos = position;
        this.maxPos = Math.min(blob.sqlLength(), position + length - 1);
    }
    
    /**
     * @see java.io.InputStream#read()
     *
     * This method fetches one byte at a time from the server. For more 
     * efficient retrieval, use #read(byte[]).
     */    
    public int read() throws IOException
    {
        byte[] bytes = readBytes(1);
        if (bytes.length == 0) { // EOF
            return -1;
        } else {
            return bytes[0];
        }
    }
    
    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException 
    {
        if (len == 0) return 0;
        if ((off < 0) || (len < 0) || (off+len > b.length)) {
            throw new IndexOutOfBoundsException();
        }
        
        byte[] bytes = readBytes(len);
        if (bytes.length == 0) { // EOF
            return -1;
        } else {
            System.arraycopy(bytes, 0, b, off, bytes.length);
            return bytes.length;
        }
    }

    /**
     * Read the next <code>len</code> bytes of the <code>Blob</code>
     * value from the server.
     * 
     * @param len number of bytes to read
     * @throws java.io.IOException Wrapped SqlException if reading
     *         from server fails.
     * @return <code>byte[]</code> containing the read bytes
     */
    private byte[] readBytes(int len) throws IOException
    {
        try {
            int actualLength 
                = (int )Math.min(len, maxPos - currentPos + 1);
            byte[] result = connection.locatorProcedureCall()
                .blobGetBytes(blob.getLocator(), currentPos, actualLength);
            currentPos += result.length;
            return result;       
        } catch (SqlException ex) {
            IOException ioEx = new IOException();
            ioEx.initCause(ex);
            throw ioEx;
        }
    }
    
    
    /**
     * Connection used to read Blob from server.
     */
    private Connection connection;
    
    /**
     * The Blob to be accessed.
     */
    private Blob blob;

    /**
     * Current position in the underlying Blob.
     * Blobs are indexed from 1
     */
    private long currentPos;

    /**
     * Position in Blob where to stop reading.
     */
    private long maxPos;
}

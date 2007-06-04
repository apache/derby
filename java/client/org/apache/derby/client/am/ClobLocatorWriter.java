/*
 
   Derby - Class org.apache.derby.client.am.ClobLocatorWriter
 
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

/**
 * An <code>Writer</code> that will use an locator to write the
 * Clob value into the server.
 * <p>
 * This <code>Writer</code> implementation is pretty basic.  No
 * buffering of data is done.  Hence, for efficieny #write(char[])
 * should be used instead of #write(int).
 */
public class ClobLocatorWriter extends java.io.Writer {
    /**
     * Connection used to read Clob from server.
     */
    private final Connection connection;
    
    /**
     * The Clob to be accessed.
     */
    private final Clob clob;
    
    /**
     * Current position in the underlying Clob.
     * Clobs are indexed from 1
     */
    private long currentPos;
    
    /**
     * Stores the information to whether this Writer has been
     * closed or not. Is set to true if close() has been
     * called. Is false otherwise.
     */
    private boolean isClosed = false;
    
    /**
     * Create a <code>Writer</code> for writing to the
     * <code>Clob</code> value represented by the given locator based
     * <code>Clob</code> object.
     * @param connection connection to be used to write to the
     *        <code>Clob</code> value on the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     * @param pos the position in the <code>CLOB</code> value at which
     *        to start writing; the first position is 1
     * @throws org.apache.derby.client.am.SqlException
     */
    public ClobLocatorWriter(Connection connection, Clob clob, long pos)
    throws SqlException {
        if (pos-1 > clob.sqlLength()) {
            throw new IndexOutOfBoundsException();
        }
        
        this.connection = connection;
        this.clob = clob;
        this.currentPos = pos;
    }
    
    /**
     * @see java.io.Writer#close()
     */
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
    }
    
    /**
     * Check to see if this <code>Writer</code> is closed. If it
     * is closed throw an <code>IOException</code> that states that
     * the stream is closed.
     *
     * @throws IOException if isClosed = true.
     */
    private void checkClosed() throws IOException {
        //if isClosed=true this means that close() has
        //been called on this Writer already.
        if(isClosed) {
            //since this method would be used from the write method
            //implementations throw an IOException that states that
            //these operations cannot be done once close has been
            //called.
            throw new IOException("This operation is not " +
                    "permitted because the" +
                    "Writer has been closed");
        }
    }
    
    /**
     * @see java.io.Writer#write(int)
     *
     * This method writes one Character at a time to the server. For more
     * efficient writing, use #write(char[]).
     */
    public void write(int c) throws IOException {
        char[] ca = {(char )c};
        writeCharacters(ca, 0, ca.length);
    }
    
    /**
     * @see java.io.Writer#write(char[])
     */
    public void write(char[] c) throws IOException {
        checkClosed();
        writeCharacters(c, 0, c.length);
    }
    
    /**
     * @see java.io.Writer#flush()
     */
    public void flush() {
        //There is no necessity to flush since each write
        //automatically calls the stored procedure to write
        //the characters to the locator on the server.
    }
    
    
    /**
     * @see java.io.Writer#write(char[], int, int)
     */
    public void write(char[] c, int off, int len) throws IOException {
        checkClosed();
        if (len == 0) return;
        if ((off < 0) || (off > c.length) || (len < 0) ||
                (len > c.length - off)) {
            throw new IndexOutOfBoundsException();
        }
        writeCharacters(c, off, len);
    }
    
    /**
     * Write the <code>char[]</code> to the <code>Clob</code> value on
     * the server; starting from the current position of this stream.
     *
     * @param c The character array containing the chars to be written
     * @throws java.io.IOException Wrapped SqlException if writing
     *         to server fails.
     */
    private void writeCharacters(char[] c, int off, int len)
    throws IOException {
        try {
            connection.locatorProcedureCall().clobSetString
                    (clob.locator_, currentPos, len,
                    new String(c, off, len));
            currentPos += len;
            if (currentPos-1 > clob.sqlLength()) {
                // Wrote past the old end of the Clob value, update length
                clob.setSqlLength(currentPos - 1);
            }
        } catch (SqlException ex) {
            IOException ioEx= new IOException();
            ioEx.initCause(ex);
            throw ioEx;
        }
    }
}

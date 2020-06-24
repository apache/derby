/*
 
   Derby - Class org.apache.derby.client.am.ClobLocatorReader
 
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
import java.io.Reader;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * An <code>Reader</code> that will use an locator to fetch the
 * Clob value from the server.
 * <p>
 * This <code>Reader</code> implementation is pretty basic.  No
 * buffering of data is done.  Hence, for efficiency #read(char[])
 * should be used instead of #read().  Marks are not supported, but it
 * should be pretty simple to extend the implementation to support
 * this.  A more efficient skip implementation should also be
 * straight-forward.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ClobLocatorReader extends Reader {
    /**
     * Connection used to read Clob from server.
     */
    private final ClientConnection connection;
    
    /**
     * The Clob to be accessed.
     */
    private final ClientClob clob;
    
    /**
     * Current position in the underlying Clob.
     * Clobs are indexed from 1
     */
    private long currentPos;
    
    /**
     * Position in Clob where to stop reading.
     * maxPos starts counting from 1.
     */
    private final long maxPos;
    
    /**
     * Stores the information to whether this Reader has been
     * closed or not. Is set to true if close() has been
     * called. Is false otherwise.
     */
    private boolean isClosed = false;
    
    /**
     * Create an <code>Reader</code> for reading the
     * <code>Clob</code> value represented by the given locator based
     * <code>Clob</code> object.
     * @param connection connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobLocatorReader(ClientConnection connection, ClientClob clob)
    throws SqlException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(clob.isLocator());
        }
        
        this.connection = connection;
        this.clob = clob;
        this.currentPos = 1;
       
        //In this case a subset of the Clob has
        //not been requested for. We set maxPos 
        //to -1 and instead will call 
        //clob.sqlLength() each time we want 
        //the maximum length.
        this.maxPos = -1;
    }
    
    /**
     * Create an <code>Reader</code> for reading the
     * <code>Clob</code> value represented by the given locator based
     * <code>Clob</code> object.
     * @param connection connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     * @param pos The offset to the first character of the partial value to be
     *            retrieved.
     * @param len The length in characters of the partial value to be retrieved.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobLocatorReader(ClientConnection connection, ClientClob clob,
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
            long pos, long len) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2622
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(clob.isLocator());
        }

        this.connection = connection;
        this.clob = clob;
        this.currentPos = pos;
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
        if(len != -1) {
            this.maxPos = Math.min(clob.sqlLength(), pos + len - 1);
        }
        else {
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
            this.maxPos = -1;
        }
    }
    
    /**
     * @see java.io.Reader#read()
     *
     * This method fetches one character at a time from the server. For more
     * efficient retrieval, use #read(char[]).
     */
    public int read() throws IOException {
        checkClosed();
        char[] chars = readCharacters(1);
        if (chars.length == 0) { // EOF
            return -1;
        } else {
            return chars[0];
        }
    }
    
    /**
     * @see java.io.Reader#read(char[], int, int)
     */
    public int read(char[] c, int off, int len) throws IOException {
        checkClosed();
        if (len == 0) return 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        if ((off < 0) || (len < 0) || (len > c.length - off)) {
            throw new IndexOutOfBoundsException();
        }
        
        char[] chars = readCharacters(len);
        if (chars.length == 0) { // EOF
            return -1;
        } else {
            System.arraycopy(chars, 0, c, off, chars.length);
            return chars.length;
        }
    }
    
    /**
     * @see java.io.Reader#close()
     */
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
    }

    /**
     * Check to see if this <code>Reader</code> is closed. If it
     * is closed throw an <code>IOException</code> that states that
     * the stream is closed.
     *
     * @throws IOException if isClosed = true.
     */
    private void checkClosed() throws IOException {
        //if isClosed=true this means that close() has
        //been called on this Reader already.
        if(isClosed) {
            //since this method would be used from the read method
            //implementations throw an IOException that states that
            //these operations cannot be done once close has been
            //called.
            throw new IOException("This operation is not " +
                    "permitted because the" +
                    "Reader has been closed");
        }
    }
    
    /**
     * Read the next <code>len</code> characters of the <code>Clob</code>
     * value from the server.
     *
     * @param len number of characters to read.
     * @throws java.io.IOException Wrapped SqlException if reading
     *         from server fails.
     * @return <code>char[]</code> containing the read characters.
     */
    private char[] readCharacters(int len) throws IOException {
        try {
            //stores the actual length that can be read
            //based on the value of the current position
            //in the stream(currentPos) and the length of
            //the stream.
            int actualLength = -1;
            //check if maxPos has been set and calculate actualLength
            //based on that.
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
            if(maxPos != -1) {
                //maxPos has been set. use maxPos to calculate the
                //actual length based on the value set for maxPos.
                actualLength 
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                    = (int )Math.min(len, maxPos - currentPos + 1);
            }
            else {
                //The subset of the Blob was not requested for
                //hence maxPos is -1. Here we use clob.sqllength
                //instead.
                actualLength 
                    = (int )Math.min(len, clob.sqlLength() - currentPos + 1);
            }
            String resultStr = connection.locatorProcedureCall().
                    clobGetSubString(clob.getLocator(),
                    currentPos, actualLength);
            char[] result = resultStr.toCharArray();
            currentPos += result.length;
            return result;
        } catch (SqlException ex) {
            // Passing cause as ctor argument ensures that the IOException 
            // inherits the cause's message, (unlike invoking initCause() on a 
            // default-constructed IOException).
            throw new IOException(ex);
        }
    }
}

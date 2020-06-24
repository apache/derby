/*
 
   Derby - Class org.apache.derby.client.am.ClobLocatorInputStream
 
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
import java.io.InputStream;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * An <code>InputStream</code> that will use an locator to fetch the
 * Clob value from the server.
 * <p>
 * Closing a <code>ByteArrayInputStream</code> has no effect. The methods in
 * this class can be called after the stream has been closed without
 * generating an <code>IOException</code>.
 * <p>
 * This <code>InputStream</code> implementation is pretty basic.  No
 * buffering of data is done.  Hence, for efficiency #read(byte[])
 * should be used instead of #read().  Marks are not supported, but it
 * should be pretty simple to extend the implementation to support
 * this.  A more efficient skip implementation should also be
 * straight-forward.
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ClobLocatorInputStream extends InputStream {
    
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
     * Create an <code>InputStream</code> for reading the
     * <code>Clob</code> value represented by the given locator based
     * <code>Clob</code> object.
     * @param connection connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobLocatorInputStream(ClientConnection connection, ClientClob clob)
            throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2622
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(clob.isLocator());
        }
        
        this.connection = connection;
        this.clob = clob;
        this.currentPos = 1;
    }
    
    /**
     * Create an <code>InputStream</code> for reading the
     * <code>Clob</code> value represented by the given locator based
     * <code>Clob</code> object.
     * @param connection connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     * @param pos the position inside the <code>Clob<code> from which
     *            the reading must begin.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobLocatorInputStream(
            ClientConnection connection,
            ClientClob clob,
            long pos) throws SqlException{
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
        this(connection, clob);
        this.currentPos = pos;
    }
    
    /**
     * @see java.io.InputStream#read()
     *
     * This method fetches one byte at a time from the server. For more
     * efficient retrieval, use #read(byte[]).
     */
    public int read() throws IOException {
        byte[] bytes = readBytes(1);
        if (bytes.length == 0) { // EOF
            return -1;
        } else {
            // convert byte in range [-128,127] to int in range [0,255]
//IC see: https://issues.apache.org/jira/browse/DERBY-3226
            return bytes[0] & 0xff;
        }
    }
    
    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) return 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        if ((off < 0) || (len < 0) || (len > b.length - off)) {
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
     * Read the next <code>len</code> bytes of the <code>Clob</code>
     * value from the server.
     *
     * @param len number of bytes to read
     * @throws java.io.IOException Wrapped SqlException if reading
     *         from server fails.
     * @return <code>byte[]</code> containing the read bytes
     */
    private byte[] readBytes(int len) throws IOException {
        try {
            int actualLength
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
                    = (int )Math.min(len, clob.sqlLength() - currentPos + 1);
            String resultStr = connection.locatorProcedureCall().
                    clobGetSubString(clob.getLocator(),
                    currentPos, actualLength);
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
            byte[] result = getBytesFromString(resultStr);
            currentPos += result.length;
            return result;
        } catch (SqlException ex) {
            // Passing cause as ctor argument ensures that the IOException 
            // inherits the cause's message, (unlike invoking initCause() on a 
            // default-constructed IOException).
            throw new IOException(ex);
        }
    }

    /**
     * Returns a <code>Byte</code> array from the
     * <code>String</code> passed as Input.
     *
     * @param str the input <code>String</code>.
     * @return The <code>Byte</code> corresponding
     *         to the <code>String</code> that was
     *         input.
     */
    private byte[] getBytesFromString(String str) {
        //The Byte array that will hold the final
        //converted Byte array that will be returned
        //to the user
        byte[] result = new byte[str.length()];
//IC see: https://issues.apache.org/jira/browse/DERBY-2604

        //Iterate through the String to
        //Convert each character in the
        //String
        for (int i = 1; i <= str.length(); i++) {
            //charAt function accpets a index that
            //starts from 0 and ranges to length()-1
            char oneChar = str.charAt(i-1);

            if (oneChar <= 0xff) {
                //Check if the value is lesser
                //than maximum value that can
                //be stored in a byte. If it is
                //lesser store it directly in the
                //byte array
                result[i-1] = (byte)oneChar;
            }
            else {
                //The value is greater than the
                //maximum value that can be
                //stored. Use the value 0x003f
                //which corresponds to '?'
                //signifying an unknown character
                result[i-1] = 0x3f;
            }
        }
        return result;
    }
}

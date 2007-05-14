/* 

   Derby - Class org.apache.derby.impl.jdbc.ClobStreamControl

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UTFDataFormatException;
import java.io.Writer;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.util.ByteArray;

final class ClobStreamControl extends LOBStreamControl {
    
    private ConnectionChild conChild;

    /**
     * Constructs a <code>ClobStreamControl</code> object used to perform
     * operations on a CLOB value.
     *
     * @param dbName name of the database the CLOB value belongs to
     * @param conChild connection object used to obtain synchronization object
     */
    ClobStreamControl (String dbName, ConnectionChild conChild) {
        super (dbName);
        this.conChild = conChild;
    }        
    
    /**
     * Finds the corresponding byte position for the given UTF-8 character
     * position, starting from the byte position <code>startPos</code>.
     *
     * @param startPos start position in number of bytes
     * @param charPos character position
     * @return Stream position in bytes for the given character position.
     * @throws IOException
     */
    synchronized long getStreamPosition (long startPos, long charPos) throws IOException {
        InputStream in = new BufferedInputStream (getInputStream (startPos));
        long charLength = 0;
        long streamLength = 0;
        //utf decoding routine
        while (charLength < charPos) {
            int c = in.read();
            if (c < 0)
                return -1;
            charLength ++;
             if ((c >= 0x0001) && (c <= 0x007F)) {
                //found char of one byte width
                streamLength++;
            }
            else if (c > 0x07FF) {
                //found char of three byte width
                if (in.skip (2) < 2) {
                    //no second and third byte present
                    throw new UTFDataFormatException();
                }
                streamLength += 3;
                break;
            }
            else {
                //found char of three byte width
                if (in.skip (1) != 1) {
                    //no second and third byte present
                    throw new UTFDataFormatException();
                }
                streamLength += 2;
            }
        }
        
        in.close();
        return streamLength;        
    }
    
    /**
     * Constructs and returns a <code>Writer</code> for the CLOB value.
     *
     * @param pos the initial position in bytes for the <code>Writer</code>
     * @return A <code>Writer</code> to write to the CLOB value.
     * @throws IOException
     * @throws SQLException if the specified position is invalid
     */
    synchronized Writer getWriter (long pos) throws IOException, SQLException {
        long charPos = getStreamPosition (0, pos);
        if (charPos == -1)
            throw Util.generateCsSQLException (SQLState.BLOB_POSITION_TOO_LARGE,
                                "" + (pos + 1));
        return new ClobUtf8Writer (this, getStreamPosition (0, charPos));
    }
    
    /**
     * Constructs and returns a <code>Reader</code>.
     * @param pos initial position of the returned <code>Reader</code> in
     *      number of characters
     * @return A <code>Reader</code> with the underlying <code>CLOB</code>
     *      value as source.
     * @throws IOException
     * @throws SQLException if the specified position is invalid
     */
    Reader getReader (long pos) throws IOException, SQLException {
        Reader isr = new ClobUpdateableReader (
                (LOBInputStream) getInputStream (0), conChild);

        long leftToSkip = pos;
        while (leftToSkip > 0) {
            leftToSkip -= isr.skip (leftToSkip);
        }
        return isr;
    }    
    
    /**
     * Returns a substring.
     * @param bIndex 
     * @param eIndex 
     * @return A substring of the <code>CLOB</code> value.
     * @throws IOException
     * @throws SQLException
     */
    synchronized String getSubstring (long bIndex, long eIndex) 
                                            throws IOException, SQLException {
        Reader r = getReader(bIndex);        
        char [] buff = new char [(int) (eIndex - bIndex)];
        int length = 0;
        do {
            int ret = r.read (buff, length, (int) (eIndex - bIndex) - length);
            if (ret == -1 )
                break;
            length += ret;
        } while (length < eIndex - bIndex);
        return new String (buff, 0, length);
    }
    
    /**
     * returns number of charecter in the clob.
     * @return char length
     * @throws IOException, SQLException
     */
    synchronized long getCharLength () throws IOException, SQLException {
        Reader reader = getReader(0);
        char [] dummy = new char [4 * 1024];
        int length = 0;
        do {
            long ret = reader.read (dummy);
            if (ret == -1) break;
            length += ret;
        }while (true);
        return length;
    }
    
    /**
     * Returns the size of the Clob in bytes.
     * @return Number of bytes in the <code>CLOB</code> value.
     * @throws IOException
     */
    long getByteLength () throws IOException {
        return super.getLength();
    }
    
    /**
     * inserts a string at a given postion.
     * @param str 
     * @param pos byte postion
     * @return current byte postion
     * @throws IOException
     */
    synchronized long insertString (String str, long pos) 
                                            throws IOException, SQLException {
        int len = str.length();
        if (pos == super.getLength()) {
            byte b [] = getByteFromString (str);
            long l = write (b, 0, b.length, pos);
            return str.length();
        }
        long endPos = getStreamPosition (pos, len);
        endPos = (endPos < 0) ? getLength() : pos + endPos;
        replaceBytes (getByteFromString (str), pos, endPos);
        return str.length();
    }
    
    /**
     * Converts a string into utf8 byte array.
     * @param str 
     * @return utf8 bytes array
     */
    private byte[] getByteFromString (String str) {
        //create a buffer with max size possible
        byte [] buffer = new byte [3 * str.length()];
        int len = 0;
        //start decoding
        for (int i = 0; i < str.length(); i++) {
            int c = str.charAt (i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer[len++] = (byte) c;
            }
            else if (c > 0x07FF) {
                buffer[len++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[len++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                buffer[len++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
            else {
                buffer[len++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                buffer[len++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        byte [] buff = new byte [len];
        System.arraycopy (buffer, 0, buff, 0, len);
        return buff;
    } 
}

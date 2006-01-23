/*

   Derby - Class org.apache.derby.client.am.Blob

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.sql.SQLException;
import org.apache.derby.shared.common.reference.SQLState;
public class Blob extends Lob implements java.sql.Blob {
    //-----------------------------state------------------------------------------

    byte[] binaryString_ = null;

    // Only used for input purposes.  For output, each getBinaryStream call
    // must generate an independent stream.
    java.io.InputStream binaryStream_ = null;
    int dataOffset_;

    //---------------------constructors/finalizer---------------------------------

    public Blob(byte[] binaryString,
                Agent agent,
                int dataOffset) {
        super(agent);
        binaryString_ = binaryString;
        dataType_ |= BINARY_STRING;
        sqlLength_ = binaryString.length - dataOffset;
        lengthObtained_ = true;
        dataOffset_ = dataOffset;
    }

    // CTOR for input:
    public Blob(Agent agent,
                java.io.InputStream binaryStream,
                int length) {
        super(agent);
        binaryStream_ = binaryStream;
        dataType_ |= BINARY_STREAM;
        sqlLength_ = length;
        lengthObtained_ = true;
    }

    // ---------------------------jdbc 2------------------------------------------

    public long length() throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "length");
                }
                long retVal = super.sqlLength();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "length", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // can return an array that may be have a length shorter than the supplied
    // length (no padding occurs)
    public byte[] getBytes(long pos, int length) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getBytes", (int) pos, length);
                }
                if (pos <= 0) {
                    throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.BLOB_BAD_POSITION), 
                        new Long(pos));
                }
                if (length < 0) {
                    throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
                        new Integer(length));
                }
                byte[] retVal = getBytesX(pos, length);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getBytes", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private byte[] getBytesX(long pos, int length) throws SqlException {
        checkForClosedConnection();

        // we may need to check for overflow on this cast
        long actualLength;
        try {
            actualLength = Math.min(this.length() - pos + 1, (long) length);
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }

        byte[] retVal = new byte[(int) actualLength];
        System.arraycopy(binaryString_, (int) pos + dataOffset_ - 1, retVal, 0, (int) actualLength);
        return retVal;
    }


    public java.io.InputStream getBinaryStream() throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getBinaryStream");
                }
                java.io.InputStream retVal = getBinaryStreamX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getBinaryStream", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private java.io.InputStream getBinaryStreamX() throws SqlException {
        checkForClosedConnection();

        if (isBinaryStream())    // this Lob is used for input
        {
            return binaryStream_;
        }

        return new java.io.ByteArrayInputStream(binaryString_, dataOffset_, binaryString_.length - dataOffset_);
    }

    public long position(byte[] pattern, long start) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "position(byte[], long)", pattern, start);
                }
                if (pattern == null) {
                    throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.BLOB_NULL_PATTERN));
                }
                long pos = positionX(pattern, start);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "position(byte[], long)", pos);
                }
                return pos;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private long positionX(byte[] pattern, long start) throws SqlException {
        checkForClosedConnection();

        return binaryStringPosition(pattern, start);
    }

    public long position(java.sql.Blob pattern, long start) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "position(Blob, long)", pattern, start);
                }
                if (pattern == null) {
                    throw new SqlException(agent_.logWriter_, 
                        new MessageId(SQLState.BLOB_NULL_PATTERN));
                }
                long pos = positionX(pattern, start);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "position(Blob, long)", pos);
                }
                return pos;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private long positionX(java.sql.Blob pattern, long start) throws SqlException {
        checkForClosedConnection();

        try {
            return binaryStringPosition(pattern.getBytes(1L, (int) pattern.length()), start);
        } catch (java.sql.SQLException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }
    }

    // -------------------------- JDBC 3.0 -----------------------------------


    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBytes", (int) pos, bytes);
                }
                int length = setBytesX(pos, bytes, 0, bytes.length);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setBytes", length);
                }
                return length;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBytes", (int) pos, bytes, offset, len);
                }
                int length = setBytesX(pos, bytes, offset, len);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setBytes", length);
                }
                return length;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int setBytesX(long pos, byte[] bytes, int offset, int len) throws SqlException {
        int length = 0;
        if ((int) pos <= 0) {
            throw new SqlException(agent_.logWriter_,
                new MessageId(SQLState.BLOB_BAD_POSITION), new Long(pos));
        }
        
        if ( pos > binaryString_.length - dataOffset_) {
            throw new SqlException(agent_.logWriter_, 
                new MessageId(SQLState.BLOB_POSITION_TOO_LARGE), new Long(pos));
        }
        if ((offset < 0) || offset > bytes.length )
        {
            throw new SqlException(agent_.logWriter_,
                new MessageId(SQLState.INVALID_BLOB_OFFSET), 
                new Integer(offset));
        }
        if ( len < 0 ) {
            throw new SqlException(agent_.logWriter_,
                new MessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
                new Integer(length));
        }
        if (len == 0) {
            return 0;
        }
        length = Math.min((bytes.length - offset), len);
        if ((binaryString_.length - dataOffset_ - (int) pos + 1) < length) {
            byte newbuf[] = new byte[(int) pos + length + dataOffset_ - 1];
            System.arraycopy(binaryString_, 0, newbuf, 0, binaryString_.length);
            binaryString_ = newbuf;
        }

        System.arraycopy(bytes, offset, binaryString_, (int) pos + dataOffset_ - 1, length);
        binaryStream_ = new java.io.ByteArrayInputStream(binaryString_);
        sqlLength_ = binaryString_.length - dataOffset_;
        return length;
    }

    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setBinaryStream", (int) pos);
            }
            BlobOutputStream outStream = new BlobOutputStream(this, pos);

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "setBinaryStream", outStream);
            }
            return outStream;
        }
    }

    public void truncate(long len) throws SQLException {
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, " truncate", (int) len);
                }
                if (len < 0 || len > this.length()) {
                    throw new SqlException(agent_.logWriter_,
                        new MessageId(SQLState.INVALID_API_PARAMETER),
                        new Long(len), "len", "Blob.truncate()");
                }
                if (len == this.length()) {
                    return;
                }
                long newLength = (int) len + dataOffset_;
                byte newbuf[] = new byte[(int) len + dataOffset_];
                System.arraycopy(binaryString_, 0, newbuf, 0, (int) newLength);
                binaryString_ = newbuf;
                binaryStream_ = new java.io.ByteArrayInputStream(binaryString_);
                sqlLength_ = binaryString_.length - dataOffset_;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //------------------ Material layer event callback methods -------------------

    //---------------------------- helper methods --------------------------------
    public boolean isBinaryString() {
        return ((dataType_ & BINARY_STRING) == BINARY_STRING);
    }

    public boolean isBinaryStream() {
        return ((dataType_ & BINARY_STREAM) == BINARY_STREAM);
    }

    public byte[] getBinaryString() {
        return binaryString_;
    }

    protected long binaryStringPosition(byte[] pattern, long start) {
        // perform a local byte string search, starting at start
        // check that the range of comparison is valid
        int index = (int) start + dataOffset_ - 1; // api start begins at 1

        while (index + pattern.length <= binaryString_.length) {
            if (isSubString(pattern, index)) {
                return (long) (index - dataOffset_ + 1); // readjust for api indexing
            }
            index++;
        }
        return -1L; // not found
    }

    // precondition: binaryString_ is long enough for the comparison
    protected boolean isSubString(byte[] pattern, int index) {
        for (int i = 0; i < pattern.length; i++, index++) {
            if (pattern[i] != binaryString_[index]) {
                return false;
            }
        }

        return true;
    }
}

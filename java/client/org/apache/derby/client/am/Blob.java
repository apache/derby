/*

   Derby - Class org.apache.derby.client.am.Blob

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

import org.apache.derby.shared.common.reference.SQLState;

public class Blob extends Lob implements java.sql.Blob {
    
    //This boolean variable indicates whether the Blob object has
    //been invalidated by calling free() on it
    private boolean isValid = true;
    
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
        
        super(agent, 
              false);
        
        binaryString_ = binaryString;
        dataType_ |= BINARY_STRING;
        setSqlLength(binaryString.length - dataOffset);
        dataOffset_ = dataOffset;
    }

    // CTOR for input:
    public Blob(Agent agent,
                java.io.InputStream binaryStream,
                int length) {
        
        super(agent,
              false);
        
        binaryStream_ = binaryStream;
        dataType_ |= BINARY_STREAM;
        setSqlLength(length);
    }

    /**
     * Create a new <code>Blob</code> from a stream with unknown length.
     * <em>Important:</em> This constructor is a temporary solution for
     * implementing lengthless overloads in the JDBC4 API. Before a proper
     * solution can be implemented, we need to enable streaming without having
     * to know the stream length in the DRDA protocol. See Jira DERBY-1471 and
     * DERBY-1417 for more details.
     *
     * <em>Shortcomings:</em> This constructor will cause the <em>whole stream
     * to be materialized</em> to determine its length. If the stream is big
     * enough, the client will fail with an OutOfMemoryError. Since this is a
     * temporary solution, state checking is not added to all methods as it
     * would clutter up the class severely. After using the constructor, the
     * <code>length</code>-method must be called, which will materialize the
     * stream and determine the length. <em>Do not pass a Blob object created
     * with this constructor to the user!</em>
     *
     * @param agent
     * @param binaryStream the stream to get data from
     */
    public Blob(Agent agent, java.io.InputStream binaryStream) {
        
        super(agent,
              isLayerBStreamingPossible(agent));
        
        binaryStream_ = binaryStream;
        dataType_ |= BINARY_STREAM;
    }
    
    /**
     * Create a <code>Blob</code> object for a Blob value stored 
     * on the server and indentified by <code>locator</code>.
     * @param agent context for this Blob object (incl. connection)
     * @param locator reference id to Blob value on server
     */
    public Blob(Agent agent, int locator)  
    {
        super(agent, false);
        locator_ = locator;
        dataType_ |= LOCATOR;
    }

    // ---------------------------jdbc 2------------------------------------------

    public long length() throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "length");
                }

                checkForClosedConnection();
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
    
    /**
     * Get the length in bytes of the <code>Blob</code> value represented by 
     * this locator based <Blob> object.  
     * 
     * A stored procedure call will be made to get it from the server.
     * @throws org.apache.derby.client.am.SqlException 
     * @return length of Blob in bytes
     */
    long getLocatorLength() throws SqlException
    {
        return agent_.connection_.locatorProcedureCall()
            .blobGetLength(locator_);
    }

  /**
   * Returns as an array of bytes part or all of the <code>BLOB</code>
   * value that this <code>Blob</code> object designates.  The byte
   * array contains up to <code>length</code> consecutive bytes
   * starting at position <code>pos</code>.
   * The starting position must be between 1 and the length
   * of the BLOB plus 1. This allows for zero-length BLOB values, from
   * which only zero-length byte arrays can be returned. 
   * If a larger length is requested than there are bytes available,
   * characters from the start position to the end of the BLOB are returned.
   * @param pos the ordinal position of the first byte in the
   * <code>BLOB</code> value to be extracted; the first byte is at
   * position 1
   * @param length is the number of consecutive bytes to be copied
   * @return a byte array containing up to <code>length</code>
   * consecutive bytes from the <code>BLOB</code> value designated
   * by this <code>Blob</code> object, starting with the
   * byte at position <code>startPos</code>.
   * @exception SQLException if there is an error accessing the
   * <code>BLOB</code>
   * NOTE: If the starting position is the length of the BLOB plus 1,
   * zero bytess are returned regardless of the length requested.
   */
    public byte[] getBytes(long pos, int length) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getBytes", (int) pos, length);
                }
                if (pos <= 0) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION), 
                        new Long(pos));
                }
                if (pos > sqlLength() + 1) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_POSITION_TOO_LARGE), 
                        new Long(pos));
                }
                if (length < 0) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
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

        long actualLength;
        // actual length is the lesser of the number of bytes requested
        // and the number of bytes available from pos to the end
        actualLength = Math.min(sqlLength() - pos + 1, (long) length);
        byte[] retVal; 
        if (isLocator()) {
            retVal = agent_.connection_.locatorProcedureCall()
                .blobGetBytes(locator_, pos, (int )actualLength);
        } else {
            retVal = new byte[(int) actualLength];
            System.arraycopy(binaryString_, (int) pos + dataOffset_ - 1, 
                             retVal, 0, (int) actualLength);
        }
        return retVal;
    }


    public java.io.InputStream getBinaryStream() throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
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
        } else if (isLocator()) {
            return new BufferedInputStream(
                    new BlobLocatorInputStream(agent_.connection_, this));
        } else {  // binary string
            return new java.io.ByteArrayInputStream(binaryString_, dataOffset_,
                                           binaryString_.length - dataOffset_);
        }
    }

    public long position(byte[] pattern, long start) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "position(byte[], long)", pattern, start);
                }
                if (pattern == null) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR));
                }
                if (start < 1) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION), 
                            new Long(start));
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

        if (isLocator()) {
            return agent_.connection_.locatorProcedureCall()
                .blobGetPositionFromBytes(locator_, pattern, start);
        } else {
            return binaryStringPosition(pattern, start);
        }
    }

    public long position(java.sql.Blob pattern, long start) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "position(Blob, long)", pattern, start);
                }
                if (pattern == null) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR));
                }
                if (start < 1) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION), 
                            new Long(start));
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
            if (isLocator()) {
                if ((pattern instanceof Blob) 
                    && ((Blob )pattern).isLocator()) {
                    // Send locator for pattern to server
                    return agent_.connection_.locatorProcedureCall()
                        .blobGetPositionFromLocator(locator_, 
                                                 ((Blob )pattern).getLocator(),
                                                 start);
                } else {
                    // Convert pattern to byte array before sending to server
                    return agent_.connection_.locatorProcedureCall()
                        .blobGetPositionFromBytes(locator_, 
                                  pattern.getBytes(1L, (int )pattern.length()),
                                  start);
                }
            } else { 
                return binaryStringPosition(
                                  pattern.getBytes(1L, (int )pattern.length()),
                                  start);
            }
        } catch (java.sql.SQLException e) {
            throw new SqlException(e);
        }
    }

    // -------------------------- JDBC 3.0 -----------------------------------


    public int setBytes(long pos, byte[] bytes) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
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
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
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
        
        /*
            Check if position is less than 0 and if true
            raise an exception
         */
        
        if (pos <= 0L) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.BLOB_BAD_POSITION), new Long(pos));
        }
        
        /*
           Currently only 2G-1 bytes can be inserted in a
           single Blob column hence check corresponding position
           value
         */
        
        if (pos  >= Integer.MAX_VALUE) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.BLOB_POSITION_TOO_LARGE), new Long(pos));
        }
        
        if (pos - 1 > sqlLength()) {
            throw new SqlException(agent_.logWriter_,
                         new ClientMessageId(SQLState.BLOB_POSITION_TOO_LARGE),
                         new Long(pos));
        }
        
        if ((offset < 0) || offset > bytes.length )
        {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_INVALID_OFFSET), 
                new Integer(offset));
        }
        if ( len < 0 ) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
                new Integer(length));
        }
        if (len == 0) {
            return 0;
        }
        
        length = Math.min((bytes.length - offset), len);
        if (isLocator()) {  
            byte[] ba = bytes;
            if ((offset > 0) || (length < bytes.length)) { 
                // Copy the part we will use into a new array
                ba = new byte[length];
                System.arraycopy(bytes, offset, ba, 0, length);
            }
            agent_.connection_.locatorProcedureCall()
                .blobSetBytes(locator_, pos, length, ba);
            if (pos+length-1 > sqlLength()) { // Wrote beyond the old end
                // Update length
                setSqlLength(pos + length - 1);
            } 
        } else {
            if ((binaryString_.length - dataOffset_ - (int)pos + 1) < length) {
                byte newbuf[] = new byte[(int) pos + length + dataOffset_ - 1];
                System.arraycopy(binaryString_, 0, 
                                 newbuf, 0, binaryString_.length);
                binaryString_ = newbuf;
            }

            System.arraycopy(bytes, offset, 
                             binaryString_, (int) pos + dataOffset_ - 1, 
                             length);
            binaryStream_ = new java.io.ByteArrayInputStream(binaryString_);
            setSqlLength(binaryString_.length - dataOffset_);
        }
        return length;
    }

    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBinaryStream", (int) pos);
                }
                if (pos < 1) {
                    throw new SqlException(agent_.logWriter_,
                            new ClientMessageId(SQLState.BLOB_BAD_POSITION),
                            new Long(pos));
                }
                
                OutputStream outStream;
                if (isLocator()) {
                    outStream = new BlobLocatorOutputStream(agent_.connection_,
                                                            this,
                                                            pos);
                } else {
                    outStream = new BlobOutputStream(this, pos);
                }

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, 
                                                "setBinaryStream", 
                                                outStream);
                }
                return outStream;
            }
        } catch ( SqlException se ) {
            throw se.getSQLException();
        }
    }

    public void truncate(long len) throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, " truncate", (int) len);
                }
                if (len < 0 || len > sqlLength()) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                        new Long(len), "len", "Blob.truncate()");
                }
                if (len == this.sqlLength()) {
                    return;
                }
                if (isLocator()) {
                    agent_.connection_.locatorProcedureCall()
                        .blobTruncate(locator_, len);
                    setSqlLength(len);
                } else {
                    long newLength = (int) len + dataOffset_;
                    byte newbuf[] = new byte[(int) len + dataOffset_];
                    System.arraycopy(binaryString_, 0, 
                                     newbuf, 0, (int) newLength);
                    binaryString_ = newbuf;
                    binaryStream_ 
                        = new java.io.ByteArrayInputStream(binaryString_);
                    setSqlLength(binaryString_.length - dataOffset_);
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // -------------------------- JDBC 4.0 -----------------------------------
    
    /**
     * This method frees the <code>Blob</code> object and releases the resources that 
     * it holds. The object is invalid once the <code>free</code>
     * method is called. If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * 
     * @throws SQLException if an error occurs releasing
     * the Blob's resources
     */
    public void free()
        throws SQLException {
        
        //calling free() on a already freed object is treated as a no-op
        if (!isValid) return;
        
        //now that free has been called the Blob object is no longer
        //valid
        isValid = false;
        try {            
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "free");
                }
                if (isBinaryStream()) {
                    try {
                        binaryStream_.close();
                    } catch(IOException ioe) {
                        throw new SqlException(null, new ClientMessageId(
                                             SQLState.IO_ERROR_UPON_LOB_FREE));
                    }
                } else if (isLocator()) {
                    agent_.connection_.locatorProcedureCall()
                        .blobReleaseLocator(locator_);
                } else {
                    binaryString_ = null;
                }
            }
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>
     * Blob</code> value, starting  with the byte specified by pos,
     * which is length bytes in length.
     *
     * @param pos the offset to the first byte of the partial value to
     * be retrieved. The first byte in the <code>Blob</code> is at position 1.
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial
     * <code>Blob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than
     * the number of bytes in the <code>Blob</code> or if pos + length is
     * greater than the number of bytes in the <code>Blob</code>
     *
     * @throws SQLException.
     */
    public InputStream getBinaryStream(long pos, long length)
        throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getBinaryStream",
                                                 (int) pos, length);
                }
                checkPosAndLength(pos, length);
                
                InputStream retVal;
                if (isLocator()) {
                    retVal = new BufferedInputStream(
                            new BlobLocatorInputStream(agent_.connection_,
                                                       this,
                                                       pos,
                                                       length));
                } else {  // binary string
                    retVal = new java.io.ByteArrayInputStream
                        (binaryString_, 
                         (int)(dataOffset_ + pos - 1), 
                         (int)length);
                }
                
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, 
                                                "getBinaryStream", 
                                                retVal);
                }
                return retVal;
            }
        } catch ( SqlException se ) {
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
    
    /*
     * Checks is isValid is true. If it is not true throws 
     * a SQLException stating that a method has been called on
     * an invalid LOB object
     *
     * throws SQLException if isvalid is not true.
     */
    private void checkValidity() throws SQLException{
        if(!isValid)
            throw new SqlException(null,new ClientMessageId(SQLState.LOB_OBJECT_INVALID))
                                                  .getSQLException();
    }
    

    /**
     * Materialize the stream used for input to the database.
     *
     * @throws SqlException
     */
    protected void materializeStream() throws SqlException 
    {
        binaryStream_ = super.materializeStream(binaryStream_, "java.sql.Blob");
    }

}

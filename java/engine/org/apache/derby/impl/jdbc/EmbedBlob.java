/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedBlob

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */


package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.EngineLOB;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RawToBinaryFormatStream;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.iapi.services.io.InputStreamUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.InterruptStatus;

import java.sql.SQLException;
import java.sql.Blob;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

/**
    Implements java.sql.Blob (see the JDBC 2.0 spec).
    A blob sits on top of a BINARY, VARBINARY or LONG VARBINARY column.
    If its data is small (less than 1 page) it is a byte array taken from
    the SQLBit class. If it is large (more than 1 page) it is a long column
    in the database. The long column is accessed as a stream, and is implemented
    in store as an OverflowInputStream.  The Resetable interface allows sending
    messages to that stream to initialize itself (reopen its container and
    lock the corresponding row) and to reset itself to the beginning. 

    NOTE: In the case that the data is large, it is represented as a stream.
    This stream is returned to the user in the getBinaryStream() method.
    This means that we have limited control over the state of the stream,
    since the user can read bytes from it at any time.  Thus all methods
    here reset the stream to the beginning before doing any work.
    CAVEAT: The methods may not behave correctly if a user sets up
    multiple threads and sucks data from the stream (returned from
    getBinaryStream()) at the same time as calling the Blob methods.

  <P><B>Supports</B>
   <UL>
   <LI> JSR169 - no subsetting for java.sql.Blob
   <LI> JDBC 2.0
   <LI> JDBC 3.0 - no new dependencies on new JDBC 3.0 or JDK 1.4 classes,
        new update methods can safely be added into implementation.
   </UL>

 */

final class EmbedBlob extends ConnectionChild implements Blob, EngineLOB
{
    /**
     * Tells whether the Blob has been materialized or not.
     * <p>
     * Materialization happens when the Blob is updated by the user. A
     * materialized Blob is represented either in memory or in a temporary file
     * on disk, depending on size.
     * <p>
     * A Blob that has not been materialized is represented by a stream into the
     * Derby store, and is read-only.
     */
    private boolean         materialized;
    /**
     * The underlying positionable store stream, if any.
     * <p>
     * If {@link #materialized} is {@code true}, the stream is {@code null}.
     */
    private PositionedStoreStream myStream;
    
    /**
     * Locator value for this Blob, used as a handle by the client driver to
     * map operations to the correct Blob on the server side.
     *
     * @see #getLocator()
     */
    private int locator = 0;
    
    /**
     * Length of the stream representing the Blob.
     * <p>
     * Set to -1 when the stream has been {@link #materialized} or
     * the length of the stream is not currently known.
     */
    private long streamLength = -1;
    
    /**
     * Position offset for the stream representing the Blob, if any.
     * <p>
     * This offset accounts for the bytes encoding the stream length at the
     * head of the stream. Data byte {@code pos} is at
     * {@code pos + streamPositionOffset} in the underlying stream.
     * Set to {@code Integer.MIN_VALUE} if the Blob isn't represented by a
     * store stream.
     */
    private final int streamPositionOffset;

    //This boolean variable indicates whether the Blob object has
    //been invalidated by calling free() on it
    private boolean isValid = true;

    private LOBStreamControl control;

     /**
     * This constructor is used to create a empty Blob object. It is used by the
     * Connection interface method createBlob().
     * 
     * @param blobBytes A byte array containing the data to be stores in the 
     *        Blob.
     *
     * @param con The EmbedConnection object associated with this Blob object.
     *
     */
    
     EmbedBlob(byte [] blobBytes,EmbedConnection con) throws SQLException {
        super(con);
         try {
             control = new LOBStreamControl (con, blobBytes);
             materialized = true;
             streamPositionOffset = Integer.MIN_VALUE;
             //add entry in connection so it can be cleared 
             //when transaction is not valid
             con.addLOBReference (this);
         }
         catch (IOException e) {
             throw Util.setStreamFailure (e);
         }
         catch (StandardException se) {
            throw Util.generateCsSQLException (se);
         }
     }
     
    /*
      This constructor should only be called by EmbedResultSet.getBlob
    */
    protected EmbedBlob(DataValueDescriptor dvd, EmbedConnection con)
        throws StandardException
    {
        super(con);
        // if the underlying column is null, ResultSet.getBlob will return null,
        // never should get this far
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(!dvd.isNull(), "blob is created on top of a null column");

        /*
           We support three scenarios at this point:
            a) The Blob value is already represented as bytes in memory.
               This is the case for small Blobs (less than 32 KB).
            b) The Blob value is represented as a resetable stream.
               This is the case for Blobs coming from the store
               (note the comment about SQLBit below).
            c) The Blob value is represented as a wrapped user stream.
               This stream cannot be reset, which means we have to drain the
               stream and store it temporarily until it is either discarded or
               inserted into the database.
         */
        if (dvd.hasStream()) { // Cases b) and c)
            streamPositionOffset = handleStreamValue(dvd.getStream(), con);
        } else { // a) Blob already materialized in memory
            materialized = true;
            streamPositionOffset = Integer.MIN_VALUE;
            // copy bytes into memory so that blob can live after result set
            // is closed
            byte[] dvdBytes = dvd.getBytes();

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(dvdBytes != null,"blob has a null value underneath");
            try {
                control = new LOBStreamControl (
                            getEmbedConnection(), dvdBytes);
            } catch (IOException e) {
                throw StandardException.newException (
                                        SQLState.SET_STREAM_FAILURE, e);
            }
        }
        //add entry in connection so it can be cleared 
        //when transaction is not valid
        con.addLOBReference (this);
    }


    /**
     * Constructs a Blob object on top of a stream.
     *
     * @param dvdStream the source stream
     * @param con the connection owning the Blob
     * @return The offset into the stream where the user data begins (used if
     *      resetting the stream).
     * @throws StandardException if accessing the stream fails, or if writing
     *      data to temporary storage fails
     */
    private int handleStreamValue(InputStream dvdStream, EmbedConnection con)
            throws StandardException {
        int offset = 0;
        // b) Resetable stream
        //    In this case the stream is coming from the Derby store.
        if (dvdStream instanceof Resetable) {
            materialized = false;

            /*
             We are expecting this stream to be a FormatIdInputStream with an
             OverflowInputStream inside. FormatIdInputStream implements
             Resetable. This should be the case when retrieving
             data from a long column. However, SQLBit, which is the class
             implementing the getStream() method for dvd.getStream(), does not
             guarantee this for us
             */
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(dvdStream instanceof Resetable);
            }
            // Create a position aware stream on top of dvdStream so we can
            // more easily move back and forth in the Blob.
            try {
                myStream = new PositionedStoreStream(dvdStream);
                // The BinaryToRawStream will read the encoded length bytes.
                BinaryToRawStream tmpStream =
                        new BinaryToRawStream(myStream, con);
                offset = (int)myStream.getPosition();
                // Check up front if the stream length is specified.
                streamLength = tmpStream.getLength();
                tmpStream.close();
            } catch (StandardException se) {
                if (se.getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED)) {
                    throw StandardException
                            .newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
                } else {
                    throw se;
                }
            } catch (IOException ioe) {
                throw StandardException.newException(
                     SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION, ioe, "BLOB");
            }
        // c) Non-resetable stream
        //    This is most likely a stream coming in from the user, and we
        //    don't have any guarantees on how it behaves.
        } else {
            // The code below will only work for RawToBinaryFormatStream.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                        dvdStream instanceof RawToBinaryFormatStream,
                        "Invalid stream type: " + dvdStream.getClass());
            }
            // The source stream isn't resetable, so we have to write it to a
            // temporary location to be able to support the Blob operations.
            materialized = true;
            offset = Integer.MIN_VALUE;
            try {
                control = new LOBStreamControl(getEmbedConnection());
                BinaryToRawStream tmpStream =
                        new BinaryToRawStream(dvdStream, con);
                // Transfer the data.
                byte[] bytes = new byte[4096]; // 4 KB buffer
                long pos = 0;
                while (true) {
                    int read = tmpStream.read(bytes, 0, bytes.length);
                    if (read < 1) {
                        // Reached EOF, or stream is behaving badly.
                        break;
                    }
                    // If the stream is larger than the maximum allowed by
                    // Derby, the call below will thrown an exception.
                    pos = control.write(bytes, 0, read, pos);
                }
                tmpStream.close();
            } catch (IOException ioe) {
                throw StandardException.newException (
                                        SQLState.SET_STREAM_FAILURE, ioe);
            }
        }
        return offset;
    }

    /**
     * Sets the position of the Blob to {@code logicalPos}, where position 0 is
     * the beginning of the Blob content.
     * <p>
     * The position is only guaranteed to be valid from the time this method is
     * invoked until the synchronization monitor is released, or until the next
     * invokation of this method.
     * <p>
     * The position is logical in the sense that it specifies the requested
     * position in the Blob content. This position might be at a different
     * position in the underlying representation, for instance the Derby store
     * stream prepends the Blob content with a length field.
     *
     * @param logicalPos requested Blob position, 0-based
     * @return The new position, which will be equal to the requested position.
     * @throws IOException if reading/accessing the Blob fails
     * @throws StandardException throws BLOB_POSITION_TOO_LARGE if the requested
     *      position is larger than the Blob length, throws other SQL states if
     *      resetting the stream fails
     */
    //@GuardedBy(getConnectionSynchronization())
    private long setBlobPosition(long logicalPos)
        throws StandardException, IOException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(logicalPos >= 0);
        if (materialized) {
            // Nothing to do here, except checking if the position is valid.
            if (logicalPos >= control.getLength()) {
                throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(logicalPos));
            }
        } else {
            // Reposition the store stream, account for the length field offset.
            try {
                this.myStream.reposition(
                        logicalPos + this.streamPositionOffset);
            } catch (EOFException eofe) {
                throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, eofe,
                        new Long(logicalPos));
            }
        }
        return logicalPos;
    }


    /**
     * Reads one byte from the Blob at the specified position.
     * <p>
     * Depending on the representation, this might result in a read from a byte
     * array, a temporary file on disk or from a Derby store stream.
     *
     * @return the byte at the current position, or -1 if end of file has been
     * reached
     * @throws IOException if reading from the underlying data representation
     *      fails
     */
    private int read(long pos)
            throws IOException, StandardException {
        int c;
        if (materialized) {
            if (pos >= control.getLength())
                return -1;
            else
                c = control.read (pos);
        } else {
            // Make sure we're at the right position.
            this.myStream.reposition(pos + this.streamPositionOffset);
            // Read one byte from the stream.
            c = this.myStream.read();
        }
        return c;
    }

  /**
   * Returns the number of bytes in the <code>BLOB</code> value
   * designated by this <code>Blob</code> object.
   * @return length of the <code>BLOB</code> in bytes
   * @exception SQLException if there is an error accessing the
   * length of the <code>BLOB</code>
   */
    // PT stream part may get pushed to store
    public long length()
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        try {
            if (materialized)
                return control.getLength ();
        }
        catch (IOException e) {
            throw Util.setStreamFailure (e);
        }
        if (streamLength != -1)
            return streamLength;
        
        boolean pushStack = false;
        try
        {
           // we have a stream
            synchronized (getConnectionSynchronization())
            {
                EmbedConnection ec = getEmbedConnection();
                pushStack = !ec.isClosed();
                if (pushStack)
                    setupContextStack();

                // We have to read the entire stream!
                myStream.resetStream();
                BinaryToRawStream tmpStream =
                        new BinaryToRawStream(myStream, this);
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(tmpStream.getLength() == -1);
                }

                streamLength = InputStreamUtil.skipUntilEOF(tmpStream);

                tmpStream.close();
                // Save for future uses.

                restoreIntrFlagIfSeen(pushStack, ec);

                return streamLength;
            }
        }
        catch (Throwable t)
        {
			throw handleMyExceptions(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }
    }


  /**
   * Returns as an array of bytes part or all of the <code>BLOB</code>
   * value that this <code>Blob</code> object designates.  The byte
   * array contains up to <code>length</code> consecutive bytes
   * starting at position <code>startPos</code>.
   * The starting position must be between 1 and the length
   * of the BLOB plus 1. This allows for zero-length BLOB values, from
   * which only zero-length byte arrays can be returned. 
   * If a larger length is requested than there are bytes available,
   * characters from the start position to the end of the BLOB are returned.
   * @param startPos the ordinal position of the first byte in the
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
    public byte[] getBytes(long startPos, int length)
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        
        boolean pushStack = false;
        try
        {
            if (startPos < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(startPos));
            if (length < 0)
                throw StandardException.newException(
                    SQLState.BLOB_NONPOSITIVE_LENGTH, new Integer(length));

            byte[] result;
            // if the blob is materialized
            if (materialized) {
                 result = new byte [length];
                 int sz = control.read (result, 0, result.length, startPos - 1);
                 if (sz == -1) {
                     InterruptStatus.restoreIntrFlagIfSeen();
                     return new byte [0];
                 }
                 if (sz < length) {
                     byte [] tmparray = new byte [sz];
                     System.arraycopy (result, 0, tmparray, 0, sz);
                     result = tmparray;
                 }
                 InterruptStatus.restoreIntrFlagIfSeen();
            }
            else // we have a stream
            {
                synchronized (getConnectionSynchronization())
                {
                    EmbedConnection ec = getEmbedConnection();
                    pushStack = !ec.isClosed();
                    if (pushStack)
                        setupContextStack();

                    setBlobPosition(startPos-1);
                    // read length bytes into a string
                    result = new byte[length];
                    int n = InputStreamUtil.readLoop(myStream,result,0,length);
                    /*
                     According to the spec, if there are only n < length bytes
                     to return, we should just return these bytes. Rather than
                     return them in an array of size length, where the trailing
                     bytes are not initialized, and the user cannot tell how
                     many bytes were actually returned, we should return an
                     array of n bytes.
                     */
                    if (n < length)
                    {
                        byte[] result2 = new byte[n];
                        System.arraycopy(result,0,result2,0,n);

                        restoreIntrFlagIfSeen(pushStack, ec);

                        return result2;
                    }

                    restoreIntrFlagIfSeen(pushStack,ec);
                }
            }


            return result;
        }
        catch (StandardException e)
        {  // if this is a setPosition exception then we ran out of Blob
            if (e.getMessageId().equals(SQLState.BLOB_LENGTH_TOO_LONG))
                e = StandardException.newException(
                    SQLState.BLOB_POSITION_TOO_LARGE, new Long(startPos));
            throw handleMyExceptions(e);
        }
        catch (Throwable t)
        {
			throw handleMyExceptions(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }

    }


  /**
   * Retrieves the <code>BLOB</code> designated by this
   * <code>Blob</code> instance as a stream.
   * @return a stream containing the <code>BLOB</code> data
   * @exception SQLException if there is an error accessing the
   * <code>BLOB</code>
   */
    public java.io.InputStream getBinaryStream()
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        
        boolean pushStack = false;
        try
        {
            // if we have byte array, not a stream
            if (materialized)
            {
                java.io.InputStream result = control.getInputStream(0);
                return result;
            }
            else
            { 
                // have a stream

                synchronized (getConnectionSynchronization())
                {
                    EmbedConnection ec = getEmbedConnection();
                    pushStack = !ec.isClosed();
                    if (pushStack)
                        setupContextStack();

                    // Reset stream, because AutoPositionigStream wants to read
                    // the encoded length bytes.
                    myStream.resetStream();
                    UpdatableBlobStream result = new UpdatableBlobStream(
                        this,
                        new AutoPositioningStream (this, myStream, this));

                    restoreIntrFlagIfSeen(pushStack, ec);

                    return result;
                }
            }
        }
        catch (Throwable t)
        {
			throw handleMyExceptions(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }
    }


  /**
   * Determines the byte position at which the specified byte
   * <code>pattern</code> begins within the <code>BLOB</code>
   * value that this <code>Blob</code> object represents.  The
   * search for <code>pattern</code. begins at position
   * <code>start</code>
   * @param pattern the byte array for which to search
   * @param start the position at which to begin searching; the
   *        first position is 1
   * @return the position at which the pattern appears, else -1.
   * @exception SQLException if there is an error accessing the
   * <code>BLOB</code>
   */
    public long position(byte[] pattern, long start)
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        
        boolean pushStack = false;

        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (pattern == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);
            if (pattern.length == 0)
                return start; // match DB2's SQL LOCATE function

            synchronized (getConnectionSynchronization())
            {
                EmbedConnection ec = getEmbedConnection();

                pushStack = !ec.isClosed();
                if (pushStack)
                    setupContextStack();

                long pos = setBlobPosition(start -1);
                // look for first character
                int lookFor = pattern[0];
                long curPos;
                int c;
                while (true)
                {
                    c = read(pos++); // Note the position increment.
                    if (c == -1) { // run out of stream
                        restoreIntrFlagIfSeen(pushStack, ec);
                        return -1;
                    }
                    if (c == lookFor)
                    {
                        curPos = pos;
                        if (checkMatch(pattern, pos)) {
                            restoreIntrFlagIfSeen(pushStack, ec);
                            return curPos;
                        } else
                            pos = setBlobPosition(curPos);
                    }
                }
            }
        }
        catch (StandardException e) {
            throw handleMyExceptions(e);
        }
        catch (Throwable t)
        {
			throw handleMyExceptions(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }

    }

    /**
     * Checks if the pattern (starting from the second byte) appears inside
     * the Blob content.
     * <p>
     * At this point, the first byte of the pattern must already have been
     * matched, and {@code pos} must be pointing at the second byte to compare.
     *
     * @param pattern the byte array to search for, passed in by the user
     * @param pos the position in the Blob content to start searching from
     * @return {@code true} if a match is found, {@code false} if not.
     */
    private boolean checkMatch(byte[] pattern, long pos)
            throws IOException, StandardException {
       // check whether rest matches
       // might improve performance by reading more
        for (int i = 1; i < pattern.length; i++)
        {
            int b = read(pos++);
            if ((b < 0) || (b != pattern[i]))  // mismatch or stream runs out
                return false;
        }
        return true;
    }

  /**
   * Determines the byte position in the <code>BLOB</code> value
   * designated by this <code>Blob</code> object at which
   * <code>pattern</code> begins.  The search begins at position
   * <code>start</code>.
   * @param pattern the <code>Blob</code> object designating
   * the <code>BLOB</code> value for which to search
   * @param start the position in the <code>BLOB</code> value
   *        at which to begin searching; the first position is 1
   * @return the position at which the pattern begins, else -1
   * @exception SQLException if there is an error accessing the
   * <code>BLOB</code>
   */
    public long position(Blob pattern, long start)
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        
        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (pattern == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);

            synchronized (getConnectionSynchronization())
            {
                EmbedConnection ec = getEmbedConnection();

                pushStack = !ec.isClosed();
                if (pushStack)
                    setupContextStack();

                long pos = setBlobPosition(start-1);
                // look for first character
                byte[] b;
                try
                { // pattern is not necessarily a Derby Blob
                    b = pattern.getBytes(1,1);
                }
                catch (SQLException e)
                {
                    throw StandardException.newException(SQLState.BLOB_UNABLE_TO_READ_PATTERN);
                }
                if (b == null || b.length < 1) { // the 'empty' blob
                    restoreIntrFlagIfSeen(pushStack, ec);
                    return start; // match DB2's SQL LOCATE function
                }
                int lookFor = b[0];
                int c;
                long curPos;
                while (true)
                {
                    c = read(pos++); // Note the position increment.
                    if (c == -1) {  // run out of stream
                        restoreIntrFlagIfSeen(pushStack, ec);
                        return -1;
                    }
                    if (c == lookFor)
                    {
                        curPos = pos;
                        if (checkMatch(pattern, pos)) {
                            restoreIntrFlagIfSeen(pushStack, ec);
                            return curPos;
                        } else
                            pos = setBlobPosition(curPos);
                    }
                }
            }
        }
        catch (StandardException e) {
            throw handleMyExceptions(e);
        }
        catch (Throwable t)
        {
			throw handleMyExceptions(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }

    }


    /**
     * Checks if the pattern (starting from the second byte) appears inside
     * the Blob content.
     *
     * @param pattern the Blob to search for, passed in by the user
     * @param pos the position in the Blob (this) content to start searching
     * @return {@code true} if a match is found, {@code false} if not.
     */
    private boolean checkMatch(Blob pattern, long pos)
            throws IOException, StandardException {
        // check whether rest matches
        // might improve performance by reading buffer at a time
        InputStream pStream;
        try
        {
            pStream = pattern.getBinaryStream();
        }
        catch (SQLException e)
        {
            return false;
        }
        if (pStream == null)
            return false;
        // throw away first character since we already read it in the calling
        // method
        int b1 = pStream.read();
        if (b1 < 0)
            return false;
        while (true)
        {
            b1 = pStream.read();
            if (b1 < 0)  // search blob runs out
                return true;
            int b2 = read(pos++);
            if ((b1 != b2) || (b2 < 0))  // mismatch or stream runs out
                return false;
        }
    }

    /*
      Convert exceptions where needed before calling handleException to convert
      them to SQLExceptions.
    */
	private SQLException handleMyExceptions(Throwable t)
        throws SQLException
    {
        if (t instanceof StandardException)
        {
            // container closed means the blob or clob was accessed after commit
            if (((StandardException) t).getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED))
            {
                t = StandardException.newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
            }
        }
        return handleException(t);
	}


   /*
    If we have a stream, release the resources associated with it.
    */
    protected void finalize()
    {
        if (!materialized)
            myStream.closeStream();
    }

	/**
    Following methods are for the new JDBC 3.0 methods in java.sql.Blob
    (see the JDBC 3.0 spec). We have the JDBC 3.0 methods in Local20
    package, so we don't have to have a new class in Local30.
    The new JDBC 3.0 methods don't make use of any new JDBC3.0 classes and
    so this will work fine in jdbc2.0 configuration.
	*/

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 3.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

	/**
     * Writes the given array of bytes to the BLOB value that this Blob object
     * represents, starting at position pos, and returns the number of bytes
     * written.
     *
     * @param pos the position in the BLOB object at which to start writing
     * @param bytes the array of bytes to be written to the BLOB value that this
     *       Blob object represents
     * @return The number of bytes written to the BLOB.
     * @throws SQLException if writing the bytes to the BLOB fails
     * @since 1.4
	 */
	public int setBytes(long pos, byte[] bytes) throws SQLException {
            return setBytes(pos, bytes, 0, bytes.length);
	}

    /**
     * Writes all or part of the given array of byte array to the BLOB value
     * that this Blob object represents and returns the number of bytes written.
     * Writing starts at position pos in the BLOB value; len bytes from the
     * given byte array are written.
     *
     * @param pos the position in the BLOB object at which to start writing
     * @param bytes the array of bytes to be written to the BLOB value that this
     *       Blob object represents
     * @param offset the offset into the byte array at which to start reading
     *       the bytes to be written
     * @param len the number of bytes to be written to the BLOB value from the
     *       array of bytes bytes
     * @return The number of bytes written to the BLOB.
     * @throws SQLException if writing the bytes to the BLOB fails
     * @throws IndexOutOfBoundsException if {@code len} is larger than
     *       {@code bytes.length - offset}
     * @since 1.4
	 */
    public int setBytes(long pos,
            byte[] bytes,
            int offset,
            int len) throws SQLException {
        checkValidity();

        if (pos - 1 > length())
            throw Util.generateCsSQLException(SQLState.BLOB_POSITION_TOO_LARGE,
                    new Long(pos));
        if (pos < 1)
            throw Util.generateCsSQLException(SQLState.BLOB_BAD_POSITION,
                    new Long(pos));
        
        if ((offset < 0) || offset > bytes.length) {
            throw Util.generateCsSQLException(SQLState.BLOB_INVALID_OFFSET,
                    new Long(offset));
        }
        if (len < 0) {
            throw Util.generateCsSQLException(SQLState.BLOB_NONPOSITIVE_LENGTH,
                    new Long(len));
        }
        if (len == 0) {
            return 0;
        }
        if (len > bytes.length - offset) {
            throw Util.generateCsSQLException(SQLState.BLOB_LENGTH_TOO_LONG,
                    new Long(len));
        }

        try {
            if (materialized) {
                control.write(bytes, offset, len, pos - 1);
            } else {
                control = new LOBStreamControl(getEmbedConnection());
                control.copyData(myStream, length());
                control.write(bytes, offset, len, pos - 1);
                myStream.close();
                streamLength = -1;
                materialized = true;
            }
            return len;
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
    }

   /**
    * JDBC 3.0
    *
    * Retrieves a stream that can be used to write to the BLOB value that this
    * Blob object represents. The stream begins at position pos. 
    *
    * @param pos - the position in the BLOB object at which to start writing
    * @return a java.io.OutputStream object to which data can be written 
    * @exception SQLException Feature not implemented for now.
	*/
	public java.io.OutputStream setBinaryStream (long pos)
                                    throws SQLException {
            checkValidity ();
            if (pos - 1 > length())
                throw Util.generateCsSQLException(
                    SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
            if (pos < 1)
                throw Util.generateCsSQLException(
                    SQLState.BLOB_BAD_POSITION, new Long(pos));
            try {
                if (materialized) {
                    return control.getOutputStream (pos - 1);
                }
                else {
                    control = new LOBStreamControl (
                                            getEmbedConnection());
                    control.copyData (myStream, pos - 1);
                    myStream.close ();
                    streamLength = -1;
                    materialized = true;
                    return control.getOutputStream(pos - 1);

                }
            }
            catch (IOException e) {
                throw Util.setStreamFailure (e);
            }
            catch (StandardException se) {
                throw Util.generateCsSQLException (se);
            }
	}

	/**
    * JDBC 3.0
    *
    * Truncates the BLOB value that this Blob object represents to be len bytes
    * in length.
    *
    * @param len - the length, in bytes, to which the BLOB value that this Blob
    * object represents should be truncated
    * @exception SQLException Feature not implemented for now.
	*/
	public void truncate(long len)
                                        throws SQLException
	{
            if (len > length())
                throw Util.generateCsSQLException(
                    SQLState.BLOB_LENGTH_TOO_LONG, new Long(len));
            try {
                if (materialized) {
                    control.truncate (len);
                }
                else {
                    control = new LOBStreamControl (getEmbedConnection());
                    control.copyData (myStream, len);
                    myStream.close();
                    streamLength = -1;
                    materialized = true;
                }
            }
            catch (IOException e) {
                throw Util.setStreamFailure (e);
            }
            catch (StandardException se) {
                throw Util.generateCsSQLException (se);
            }
	}

    /////////////////////////////////////////////////////////////////////////
    //
    //	JDBC 4.0	-	New public methods
    //
    /////////////////////////////////////////////////////////////////////////
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
        
        // Remove entry from connection if a locator has been created.
        if (this.locator != 0) {
            localConn.removeLOBMapping(locator);
        }
        //initialialize length to default value -1
        streamLength = -1;
        
        //if it is a stream then close it.
        //if a array of bytes then initialize it to null
        //to free up space
        if (!materialized) {
            myStream.closeStream();
            myStream = null;
        } else {
            try {
                control.free ();
                control = null;
            }
            catch (IOException e) {
                throw Util.setStreamFailure (e);
            }
        }
    }
    
    /**
     * Returns an <code>InputStream</code> object that contains a partial 
     * <code>Blob</code> value, starting with the byte specified by pos, 
     * which is length bytes in length.
     *
     * @param pos the offset to the first byte of the partial value to be 
     *      retrieved. The first byte in the <code>Blob</code> is at 
     *      position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return through which the partial <code>Blob</code> value can be read. 
     * @throws SQLException if pos is less than 1 or if pos is greater than 
     *      the number of bytes in the {@code Blob} or if {@code pos + length}
     *      is greater than {@code Blob.length() +1}
     */
    public InputStream getBinaryStream(long pos, long length)
        throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Blob object has been freed by calling free() on it
        checkValidity();
        
        if (pos <= 0) {
            throw Util.generateCsSQLException(
                    SQLState.BLOB_BAD_POSITION,
                    new Long(pos));
        }
        if (length < 0) {
            throw Util.generateCsSQLException(
                    SQLState.BLOB_NONPOSITIVE_LENGTH,
                    new Long(length));
        }
        if (length > (this.length() - (pos -1))) {
            throw Util.generateCsSQLException(
                    SQLState.POS_AND_LENGTH_GREATER_THAN_LOB,
                    new Long(pos), new Long(length));
        }
        
        try {
            return new UpdatableBlobStream(this,
                                            getBinaryStream(),
                                            pos-1,
                                            length);
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        }
    }
    
    /*
     * Checks is isValid is true. If it is not true throws 
     * a SQLException stating that a method has been called on
     * an invalid LOB object
     *
     * throws SQLException if isvalid is not true.
     */
    private void checkValidity() throws SQLException{
        //check for connection to maintain sqlcode for closed
        //connection
        getEmbedConnection().checkIfClosed();
        if(!isValid)
            throw newSQLException(SQLState.LOB_OBJECT_INVALID);
    }
    
    /**
     * Returns if blob data is stored locally (using LOBStreamControl).
     * @return true if materialized else false
     */
    boolean isMaterialized () {
        return materialized;
    }

    /**
     * Return locator for this lob.
     * 
     * @return The locator identifying this lob.
     */
    public int getLocator() {
        if (locator == 0) {
            locator = localConn.addLOBMapping(this);
        }
        return locator;
    }
}

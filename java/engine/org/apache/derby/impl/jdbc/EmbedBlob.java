/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedBlob

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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


package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.impl.jdbc.ConnectionChild;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.services.io.NewByteArrayInputStream;
import org.apache.derby.iapi.services.io.InputStreamUtil;
import org.apache.derby.iapi.services.io.ArrayInputStream;

import java.sql.SQLException;
import java.sql.Blob;
import java.io.InputStream;
import java.io.EOFException;
import java.io.IOException;

/*
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

 */

class EmbedBlob extends ConnectionChild implements Blob
{
    // clob is either bytes or stream
    private boolean         isBytes;
    private InputStream     myStream;
    private byte[]          myBytes;
    // note: cannot control position of the stream since user can do a getBinaryStream
    private long            pos;
    // this stream sits on top of myStream
    private BinaryToRawStream biStream;

    // buffer for reading in blobs from a stream (long column)
    // and trashing them (to set the position of the stream etc.)
    private static int BLOB_BUF_SIZE = 4096;
    private byte buf[];

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

        myStream = dvd.getStream();
        if (myStream == null)
        {
            isBytes = true;
            // copy bytes into memory so that blob can live after result set
            // is closed
            byte[] dvdBytes = dvd.getBytes();

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(dvdBytes != null,"blob has a null value underneath");

            myBytes = new byte[dvdBytes.length];
            System.arraycopy(dvdBytes, 0, myBytes, 0, dvdBytes.length);
        }
        else
        {
            isBytes = false;

            /*
             We are expecting this stream to be a FormatIdInputStream with an
             OverflowInputStream inside. FormatIdInputStream implements
             Resetable. This should be the case when retrieving
             data from a long column. However, SQLBit, which is the class
             implementing the getStream() method for dvd.getStream(), does not
             guarantee this for us
             */
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(myStream instanceof Resetable);

            ((Resetable)myStream).initStream();
            // set up the buffer for trashing the bytes to set the position of the
            // stream, only need a buffer when we have a long column
            buf = new byte[BLOB_BUF_SIZE];
        }
        pos = 0;
    }


    /*
        Sets the position of the stream to position newPos, where position 0 is
        the beginning of the stream.

        @param newPos the position to set to
        @exception StandardException (BLOB_SETPOSITION_FAILED) throws this if
        the stream runs out before we get to newPos
    */
    private void setPosition(long newPos)
        throws StandardException, IOException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(newPos >= 0);
        if (isBytes)
            pos = newPos;
        else
        {
            // Always resets the stream to the beginning first, because user can
            // influence the state of the stream without letting us know.
            ((Resetable)myStream).resetStream();
            // PT could try to save creating a new object each time
            biStream = new BinaryToRawStream(myStream, this);
            pos = 0;
            while (pos < newPos)
            {
                int size = biStream.read(
                    buf,0,(int) Math.min((newPos-pos), (long) BLOB_BUF_SIZE));
                if (size <= 0)   // ran out of stream
                    throw StandardException.newException(SQLState.BLOB_SETPOSITION_FAILED);
                pos += size;
            }
        }
    }


    /*
        Reads one byte, either from the byte array or else from the stream.
    */
    private int read()
        throws IOException
    {
        int c;
        if (isBytes)
        {
            if (pos >= myBytes.length)
                return -1;
            else
                c = myBytes[(int) pos];
        }
        else
            c = biStream.read();
        pos++;
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
        boolean pushStack = false;
        try
        {
            if (isBytes)
                return myBytes.length;
            // we have a stream
            synchronized (getConnectionSynchronization())
            {
                pushStack = !getEmbedConnection().isClosed();
                if (pushStack)
                    setupContextStack();

                setPosition(0);
                for (;;)
                {
                    int size = biStream.read(buf);
                    if (size == -1)
                        break;
                    pos += size;
                }
                return pos;
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
   * starting at position <code>pos</code>.
   * @param pos the ordinal position of the first byte in the
   * <code>BLOB</code> value to be extracted; the first byte is at
   * position 1
   * @param length is the number of consecutive bytes to be copied
   * @return a byte array containing up to <code>length</code>
   * consecutive bytes from the <code>BLOB</code> value designated
   * by this <code>Blob</code> object, starting with the
   * byte at position <code>pos</code>.
   * @exception SQLException if there is an error accessing the
   * <code>BLOB</code>
   NOTE: return new byte[0] if startPos is too large
   */
   // PT stream part may get pushed to store

    public byte[] getBytes(long startPos, int length)
        throws SQLException
    {
        boolean pushStack = false;
        try
        {
            if (startPos < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(startPos));
            if (length <= 0)
                throw StandardException.newException(
                    SQLState.BLOB_NONPOSITIVE_LENGTH, new Integer(length));

            byte[] result;
            // if we have a byte array, not a stream
            if (isBytes)
            {
                // if blob length is less than pos bytes, raise an exception
                if (myBytes.length < startPos)
                    throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(startPos));
                // cannot go over length of array
                int lengthFromPos = myBytes.length - (int) startPos + 1;
                int actualLength = length > lengthFromPos ? lengthFromPos : length;
                result = new byte[actualLength];
                System.arraycopy(myBytes, ((int) startPos) - 1, result, 0, actualLength);
            }
            else // we have a stream
            {
                synchronized (getConnectionSynchronization())
                {
                    pushStack = !getEmbedConnection().isClosed();
                    if (pushStack)
                        setupContextStack();

                    setPosition(startPos-1);
                    // read length bytes into a string
                    result = new byte[length];
                    int n = InputStreamUtil.readLoop(biStream,result,0,length);
                    pos += n;
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
                        return result2;
                    }
                }
            }
            return result;
        }
        catch (StandardException e)
        {  // if this is a setPosition exception then we ran out of Blob
            if (e.getMessageId().equals(SQLState.BLOB_SETPOSITION_FAILED))
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
        boolean pushStack = false;
        try
        {
            // if we have byte array, not a stream
            if (isBytes)
            {
                return new NewByteArrayInputStream(myBytes);
            }
            else
            { 
                // have a stream

                synchronized (getConnectionSynchronization())
                {
                    pushStack = !getEmbedConnection().isClosed();
                    if (pushStack)
                        setupContextStack();

                    setPosition(0);
                    return biStream;
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
        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (pattern == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN);
            if (pattern.length == 0)
                return start; // match DB2's SQL LOCATE function

            synchronized (getConnectionSynchronization())
            {
                pushStack = !getEmbedConnection().isClosed();
                if (pushStack)
                    setupContextStack();

                setPosition(start-1);
                // look for first character
                int lookFor = pattern[0];
                long curPos;
                int c;
                while (true)
                {
                    c = read();
                    if (c == -1)  // run out of stream
                        return -1;
                    if (c == lookFor)
                    {
                        curPos = pos;
                        if (checkMatch(pattern))
                            return curPos;
                        else
                            setPosition(curPos);
                    }
                }
            }
        }
        catch (StandardException e)
        {  // if this is a setPosition exception then not found
            if (e.getMessageId().equals(SQLState.BLOB_SETPOSITION_FAILED))
                return -1;
            else
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


    /*
     check whether pattern (starting from the second byte) appears inside
     posStream (at the current position)
     @param posStream the stream to search inside
     @param pattern the byte array passed in by the user to search with
     @return true if match, false otherwise
     */
    private boolean checkMatch(byte[] pattern)
        throws IOException
    {
       // check whether rest matches
       // might improve performance by reading more
        for (int i = 1; i < pattern.length; i++)
        {
            int b = read();
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
        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (pattern == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN);
            synchronized (getConnectionSynchronization())
            {
                pushStack = !getEmbedConnection().isClosed();
                if (pushStack)
                    setupContextStack();

                setPosition(start-1);
                // look for first character
                byte[] b;
                try
                { // pattern is not necessarily a cloudscape Blob
                    b = pattern.getBytes(1,1);
                }
                catch (SQLException e)
                {
                    throw StandardException.newException(SQLState.BLOB_UNABLE_TO_READ_PATTERN);
                }
                if (b == null || b.length < 1)  // the 'empty' blob
                    return start; // match DB2's SQL LOCATE function
                int lookFor = b[0];
                int c;
                long curPos;
                while (true)
                {
                    c = read();
                    if (c == -1)  // run out of stream
                        return -1;
                    if (c == lookFor)
                    {
                        curPos = pos;
                        if (checkMatch(pattern))
                            return curPos;
                        else
                            setPosition(curPos);
                    }
                }
            }
        }
        catch (StandardException e)
        {  // if this is a setPosition exception then not found
            if (e.getMessageId().equals(SQLState.BLOB_SETPOSITION_FAILED))
                return -1;
            else
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


    /*
     check whether pattern (starting from the second byte) appears inside
     posStream (at the current position)
     @param posStream the stream to search inside
     @param pattern the blob passed in by the user to search with
     @return true if match, false otherwise
     */
    private boolean checkMatch(Blob pattern)
        throws IOException
    {
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
            int b2 = read();
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
        if (!isBytes)
            ((Resetable)myStream).closeStream();
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
    * JDBC 3.0
    *
    * Writes the given array of bytes to the BLOB value that this Blob object
    * represents, starting at position pos, and returns the number of bytes written.
    *
    * @param pos - the position in the BLOB object at which to start writing
    * @param bytes - the array of bytes to be written to the BLOB value that this
    * Blob object represents
    * @return the number of bytes written
    * @exception SQLException Feature not implemented for now.
	*/
	public int setBytes(long pos,
					byte[] bytes)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Writes all or part of the given array of byte array to the BLOB value that
    * this Blob object represents and returns the number of bytes written.
    * Writing starts at position pos in the BLOB value; len bytes from the given
    * byte array are written.
    *
    * @param pos - the position in the BLOB object at which to start writing
    * @param bytes - the array of bytes to be written to the BLOB value that this
    * Blob object represents
    * @param offset - the offset into the array bytes at which to start reading
    * the bytes to be set
    * @param len - the number of bytes to be written to the BLOB value from the
    * array of bytes bytes
    * @return the number of bytes written
    * @exception SQLException Feature not implemented for now.
	*/
	public int setBytes(long pos,
					byte[] bytes, int offset,
					int len)
    throws SQLException
	{
		throw Util.notImplemented();
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
	public java.io.OutputStream setBinaryStream(long pos)
    throws SQLException
	{
		throw Util.notImplemented();
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
		throw Util.notImplemented();
	}

}




/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedClob

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
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.impl.jdbc.ConnectionChild;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.UTF8Reader;
import org.apache.derby.impl.jdbc.ReaderToAscii;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.Reader;
import java.io.IOException;
import java.io.EOFException;
import java.sql.SQLException;
import java.sql.Clob;

/**
    Implements java.sql.Clob (see the JDBC 2.0 spec).
    A clob sits on top of a CHAR, VARCHAR or LONG VARCHAR column.
    If its data is small (less than 1 page) it is a byte array taken from
    the SQLChar class. If it is large (more than 1 page) it is a long column
    in the database. The long column is accessed as a stream, and is implemented
    in store as an OverflowInputStream. The Resetable interface allows sending
    messages to that stream to initialize itself (reopen its container and
    lock the corresponding row) and to reset itself to the beginning.
    <p>
    NOTE: In the case that the data is large, it is represented as a stream.
    This stream can be returned to the user in the getAsciiStream() method.
    This means that we have limited control over the state of the stream,
    since the user can read bytes from it at any time.  Thus all methods
    here reset the stream to the beginning before doing any work.
    CAVEAT: The methods may not behave correctly if a user sets up
    multiple threads and sucks data from the stream (returned from
    getAsciiStream()) at the same time as calling the Clob methods.

  <P><B>Supports</B>
   <UL>
   <LI> JSR169 - no subsetting for java.sql.Clob
   <LI> JDBC 2.0
   <LI> JDBC 3.0 - no new dependencies on new JDBC 3.0 or JDK 1.4 classes,
        new update methods can safely be added into implementation.
   </UL>
 */
final class EmbedClob extends ConnectionChild implements Clob
{
    // clob is either a string or stream
    private boolean         materialized;
    private InputStream     myStream;
    private ClobStreamControl control;

    //This boolean variable indicates whether the Clob object has
    //been invalidated by calling free() on it
    private boolean isValid = true;

    /**
     * This constructor is used to create a empty Clob object. It is used by the
     * Connection interface method createClob().
     *
     * @param clobString A String object containing the data to be stores in the
     *        Clob.
     *
     * @param con The Connection object associated with this EmbedClob object.
     * @throws SQLException
     *
     */

    EmbedClob(String clobString,EmbedConnection con) throws SQLException {
        super(con);
        materialized = true;
        control = new ClobStreamControl (con.getDBName(), this);
        try {
            control.insertString (clobString, 0);
        }
        catch (IOException e) {
            throw Util.setStreamFailure (e);
        }
        catch (StandardException se) {
            throw Util.generateCsSQLException (se);
        }
    }

    /**
     * This constructor should only be called by {@link EmbedResultSet#getClob}.
     *
     * @param dvd 
     * @param con 
     * @throws StandardException
     */
    protected EmbedClob(DataValueDescriptor dvd, EmbedConnection con)
        throws StandardException
    {
        super(con);
        // if the underlying column is null, ResultSet.getClob will return null,
        // never should get this far
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(!dvd.isNull(), "clob is created on top of a null column");

        myStream = dvd.getStream();
        if (myStream == null)
        {
            control = new ClobStreamControl (con.getDBName(), this);
            materialized = true;
            try {
                String str = dvd.getString();
                control.insertString (dvd.getString(), 0);
            }
            catch (SQLException sqle) {
                throw StandardException.newException (sqle.getSQLState(), sqle);
            }
            catch (IOException e) {
                throw StandardException.newException (
                                        SQLState.SET_STREAM_FAILURE, e);
            }
        }
        else
        {
            /*
             We are expecting this stream to be a FormatIdInputStream with an
             OverflowInputStream inside. FormatIdInputStream implements
             Resetable, as does OverflowInputStream. This should be the case
             when retrieving data from a long column. However, SQLChar, which is
             the class implementing the getStream() method for dvd.getStream(),
             does not guarantee this for us. In particular, the logging system
             (see StoredPage.logColumn) calls setStream with an argument that
             is sometimes a RememberBytesInputStream on a SQLChar object
             (e.g. see test repStreaming.sql). However, such a SQLChar
             object is going to the log buffer, NOT back to the user, so it
             should not break the ASSERT below.
             */
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(myStream instanceof Resetable);

            try {
                ((Resetable) myStream).initStream();
            } catch (StandardException se) {
                if (se.getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED)) {
                    throw StandardException
                            .newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
                }
            }
        }
    }


  /**
   * Returns the number of characters
   * in the <code>CLOB</code> value
   * designated by this <code>Clob</code> object.
   * @return length of the <code>CLOB</code> in characters
   * @exception SQLException if there is an error accessing the
   * length of the <code>CLOB</code>
   */

    public long length() throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();
        // if we have a string, not a stream
        try {
            if (materialized)
                return control.getCharLength ();
        }
        catch (IOException e) {
            throw Util.setStreamFailure (e);
        }


		Object synchronization = getConnectionSynchronization();
        synchronized (synchronization)
        {
			Reader clobReader = null;
            setupContextStack();
			try {

				clobReader = getCharacterStream();
                long clobLength = 0;
                for (;;)
                {
                    long size = clobReader.skip(32 * 1024);
                    if (size == -1)
                        break;
                    clobLength += size;
                }
				clobReader.close();
				clobReader = null;

				return clobLength;
			}
			catch (Throwable t)
			{
				throw noStateChangeLOB(t);
			}
			finally
			{
				if (clobReader != null) {
					try {
						clobReader.close();
					} catch (IOException ioe) {
					}
				}
				restoreContextStack();
			}
		}
	}

  /**
   * Returns a copy of the specified substring
   * in the <code>CLOB</code> value
   * designated by this <code>Clob</code> object.
   * The substring begins at position
   * <code>pos</code> and has up to <code>length</code> consecutive
   * characters. The starting position must be between 1 and the length
   * of the CLOB plus 1. This allows for zero-length CLOB values, from
   * which only zero-length substrings can be returned.
   * If a larger length is requested than there are characters available,
   * characters from the start position to the end of the CLOB are returned.
   * @param pos the first character of the substring to be extracted.
   *            The first character is at position 1.
   * @param length the number of consecutive characters to be copied
   * @return a <code>String</code> that is the specified substring in
   *         the <code>CLOB</code> value designated by this <code>Clob</code> object
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code>

   * NOTE: If the starting position is the length of the CLOB plus 1,
   * zero characters are returned regardless of the length requested.
   */
    public String getSubString(long pos, int length) throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        if (pos < 1)
            throw Util.generateCsSQLException(
                SQLState.BLOB_BAD_POSITION, new Long(pos));
        if (length < 0)
            throw Util.generateCsSQLException(
                SQLState.BLOB_NONPOSITIVE_LENGTH, new Integer(length));

        // if we have a string, not a stream
        if (materialized)
        {
            try {
                long sLength = control.getCharLength ();
                if (sLength + 1 < pos)
                    throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
                int endIndex = ((int) pos) + length - 1;
                // cannot go over length of string
                    return control.getSubstring (pos - 1,
                            (sLength > endIndex ? endIndex : sLength));
            }
            catch (IOException e) {
                throw Util.setStreamFailure (e);
            }
        }

		Object synchronization = getConnectionSynchronization();
        synchronized (synchronization)
        {
            setupContextStack();

			UTF8Reader clobReader = null;
			try {

				clobReader = getCharacterStreamAtPos(pos, synchronization);
				if (clobReader == null)
					throw StandardException.newException(SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));

				StringBuffer sb = new StringBuffer(length);
				int remainToRead = length;
				while (remainToRead > 0) {

					int read = clobReader.readInto(sb, remainToRead);
					if (read == -1)
						break;

					remainToRead -= read;
				}
				clobReader.close();
				clobReader = null;

				return sb.toString();
			}
			catch (Throwable t)
			{
				throw noStateChangeLOB(t);
			}
			finally
			{
				if (clobReader != null)
					clobReader.close();
				restoreContextStack();
			}
		}
    }


  /**
   * Gets the <code>Clob</code> contents as a Unicode stream.
   * @return a Unicode stream containing the <code>CLOB</code> data
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code>
   */

    public java.io.Reader getCharacterStream() throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();


        Object synchronization = getConnectionSynchronization();
        synchronized (synchronization)
        {
            setupContextStack();

			try {
                            if (materialized) {
                                return control.getReader (0);
                            }

                            return getCharacterStreamAtPos(1, synchronization);
			}
			catch (Throwable t)
			{
				throw noStateChangeLOB(t);
			}
			finally
			{
				restoreContextStack();
			}
		}
    }


  /**
   * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
   * object as a stream of Ascii bytes.
   * @return an ascii stream containing the <code>CLOB</code> data
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code> value
   */

    public java.io.InputStream getAsciiStream() throws SQLException
    {
                //call checkValidity to exit by throwing a SQLException if
                //the Clob object has been freed by calling free() on it
                checkValidity();
		return new ReaderToAscii(getCharacterStream());
    }

    private UTF8Reader getCharacterStreamAtPos(long position, Object synchronization)
    throws IOException, StandardException {
        UTF8Reader clobReader = null;
        if (materialized)
            clobReader = new UTF8Reader (control.getInputStream (0), 0,
                                            control.getByteLength(),
                                    this, control);
        else {
            ((Resetable)myStream).resetStream();
            clobReader = new UTF8Reader(myStream, 0, this, synchronization);
        }

        // skip to the correct position (pos is one based)
        long remainToSkip = position - 1;
        while (remainToSkip > 0) {
            long skipBy = clobReader.skip(remainToSkip);
            if (skipBy == -1)
                return null;

            remainToSkip -= skipBy;
        }

        return clobReader;
    }


  /**
   * Determines the character position at which the specified substring
   * <code>searchStr</code> appears in the <code>CLOB</code> value.  The search
   * begins at position <code>start</code>. The method uses the following
   * algorithm for the search:
   * <p>
   * If the <code>CLOB</code> value is materialized as a string, use
   * <code>String.indexOf</code>.
   * <p>
   * If the <code>CLOB</code> value is represented as a stream, read a block of
   * chars from the start position and compare the chars with
   * <code>searchStr</code>. Then:
   * <ul> <li>If a matching char is found, increment <code>matchCount</code>.
   *      <li>If <code>matchCount</code> is equal to the length of
   *          <code>searchStr</code>, return with the current start position.
   *      <li>If no match is found, and there is more data, restart search
   *          (see below).
   *      <li>If all data is processed without a match, return <code>-1</code>.
   * </ul>
   * <p>
   * The position where the stream has a char equal to the first char of
   * <code>searchStr</code> will be remembered and used as the starting
   * position for the next search-iteration if the current match fails.
   * If a non-matching char is found, start a fresh search from the position
   * remembered. If there is no such position, next search will start at the
   * current position <code>+1</code>.
   *
   * @param searchStr the substring for which to search
   * @param start the position at which to begin searching; the first position
   *    is <code>1</code>
   * @return The position at which the substring appears, <code>-1</code> if
   *    it does not appear in the <code>CLOB</code> value. The first position
   *    is <code>1</code>.
   * @exception SQLException if there is an error accessing the
   *    <code>CLOB</code> value
   */
    public long position(String searchStr, long start)
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (searchStr == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);
            if (searchStr == "")
                return start; // match DB2's SQL LOCATE function

            Object synchronization = getConnectionSynchronization();
            synchronized (synchronization)
            {
                pushStack = !getEmbedConnection().isClosed();
                if (pushStack)
                    setupContextStack();
                int matchCount = 0;
                long pos = start - 1;
                long newStart = -1;
                Reader reader = getCharacterStreamAtPos (start, this);
                char [] tmpClob = new char [4096];
                boolean reset;
                for (;;) {
                    reset = false;
                    int readCount = reader.read (tmpClob);
                    if (readCount == -1)
                        return -1;
                    if (readCount == 0)
                        continue;
                    for (int clobOffset = 0;
                                clobOffset < readCount; clobOffset++) {
                        if (tmpClob [clobOffset]
                                        == searchStr.charAt (matchCount)) {
                            //find the new starting position in
                            // case this match is unsuccessful
                            if (matchCount != 0 && newStart == -1
                                    && tmpClob [clobOffset]
                                    == searchStr.charAt (0)) {
                                newStart = pos + clobOffset + 1;
                            }
                            matchCount ++;
                            if (matchCount == searchStr.length()) {
                                //return after converting the position
                                //to 1 based index
                                return pos + clobOffset
                                        - searchStr.length() + 1 + 1;
                            }
                        }
                        else {
                            if (matchCount > 0) {
                                if (newStart == -1) {
                                    if (matchCount > 1) {
                                        //compensate for increment in the "for"
                                        clobOffset--;
                                    }
                                    matchCount = 0;
                                    continue;
                                }
                                matchCount = 0;
                                if (newStart < pos) {
                                    pos = newStart;
                                    reader.close();
                                    reader = getCharacterStreamAtPos
                                                (newStart + 1, this);
                                    newStart = -1;
                                    reset = true;
                                    break;
                                }
                                clobOffset = (int) (newStart - pos) - 1;
                                newStart = -1;
                                continue;
                            }
                        }
                    }
                    if (!reset) {
                        pos += readCount;
                    }
                }

            }
        }
        catch (Throwable t)
        {
			throw noStateChangeLOB(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }

    }


  /**
   * Determines the character position at which the specified
   * <code>Clob</code> object <code>searchstr</code> appears in this
   * <code>Clob</code> object.  The search begins at position
   * <code>start</code>.
   * @param searchClob the <code>Clob</code> object for which to search
   * @param start the position at which to begin searching; the first
   *              position is 1
   * @return the position at which the <code>Clob</code> object appears,
   * else -1; the first position is 1
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code> value
   */

    public long position(Clob searchClob, long start)
        throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (searchClob == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);

            synchronized (getConnectionSynchronization())
            {
				char[] subPatternChar = new char[1024];

				boolean seenOneCharacter = false;

				//System.out.println("BEGIN CLOB SEARCH @ " + start);

restartScan:
				for (;;) {

					long firstPosition = -1;

					Reader patternReader = searchClob.getCharacterStream();

					//System.out.println("RESTART CLOB SEARCH @ " + start);

					try {

						for (;;) {

							int read = patternReader.read(subPatternChar, 0, subPatternChar.length);
							if (read == -1) {
								//empty pattern
								if (!seenOneCharacter)
									return start; // matches DB2 SQL LOCATE function

								return firstPosition;
							}
							if (read == 0) {
								//System.out.println("STUCK IN READ 0 HELL");
								continue;
							}

							seenOneCharacter = true;

							String subPattern = new String(subPatternChar, 0, read);
					//System.out.println("START CLOB SEARCH @ " + start + " -- " + subPattern);
							long position = position(subPattern, start);
					//System.out.println("DONE SUB CLOB SEARCH @ " + start + " -- " + position);
							if (position == -1) {
								// never seen any match
								if (firstPosition == -1)
									return -1;

								start = firstPosition + 1;
								continue restartScan;
							}

							if (firstPosition == -1)
								firstPosition = position;
							else if (position != start) {
								// must match at the first character of the segment
								start = firstPosition + 1;
								continue restartScan;
							}

							// read is the length of the subPattern string
							start = position + read;
					}
					} finally {
						patternReader.close();
					}
				}
            }
        }
        catch (Throwable t)
        {
			throw noStateChangeLOB(t);
        }
        finally
        {
            if (pushStack)
                restoreContextStack();
        }

    }


    /*
     If we have a stream, release the resources associated with it.
     */
    protected void finalize()
    {
        // System.out.println("finalizer called");
        if (!materialized)
            ((Resetable)myStream).closeStream();
    }


	/**
    Following methods are for the new JDBC 3.0 methods in java.sql.Clob
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
    * Writes the given Java String to the CLOB value that this Clob object designates
    * at the position pos.
    *
    * @param pos - the position at which to start writing to the CLOB value that
    * this Clob object represents
    * @return the number of characters written
    * @exception SQLException Feature not implemented for now.
	*/
	public int setString(long pos, String str) throws SQLException {
            return setString (pos, str, 0, str.length());
	}

   /**
    * JDBC 3.0
    *
    * Writes len characters of str, starting at character offset, to the CLOB value
    * that this Clob represents.
    *
    * @param pos - the position at which to start writing to this Clob object
    * @param str - the string to be written to the CLOB value that this Clob designates
    * @param offset - the offset into str to start reading the characters to be written
    * @param len - the number of characters to be written
    * @return the number of characters written
    * @exception SQLException Feature not implemented for now.
	*/
        public int setString(long pos, String str, int offset, int len)
        throws SQLException {
            try {
                if (!materialized) {
                    control.copyData(myStream, length());
                    materialized = true;
                }
                long charPos = control.getStreamPosition(0, pos - 1);
                if (charPos == -1)
                    throw Util.generateCsSQLException(
                            SQLState.BLOB_POSITION_TOO_LARGE, "" + pos);
                return (int) control.insertString(str.substring (offset,
                        (offset + len)), charPos);
            } catch (IOException e) {
                throw Util.setStreamFailure(e);
            }
            catch (StandardException se) {
                throw Util.generateCsSQLException (se);
            }
        }

	/**
    * JDBC 3.0
    *
    * Retrieves a stream to be used to write Ascii characters to the CLOB
    * value that this Clob object represents, starting at position pos.
    *
    * @param pos - the position at which to start writing to this Clob object
    * @return the stream to which ASCII encoded characters can be written
    * @exception SQLException Feature not implemented for now.
	*/
    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        try {
            return new ClobAsciiStream (control.getWriter(pos - 1));
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        }
    }

	/**
    * JDBC 3.0
    *
    * Retrieves a stream to be used to write a stream of Unicode characters to the
    * CLOB value that this Clob object represents, starting at position pos.
    *
    * @param pos - the position at which to start writing to this Clob object
    * @return the stream to which Unicode encoded characters can be written
    * @exception SQLException Feature not implemented for now.
	*/
    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        try {
            if (!materialized) {
                control.copyData(myStream, length());
                materialized = true;
            }
            return control.getWriter(pos - 1);
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        } catch (StandardException se) {
            throw Util.generateCsSQLException (se);
        }
    }

  	/**
    * JDBC 3.0
    *
    * Truncates the CLOB value that this Clob designates to have a length of len characters
    *
    * @param len - the length, in bytes, to which the CLOB value that this Blob
    * value should be truncated
    * @exception SQLException Feature not implemented for now.
	*/
	public void truncate(long len) throws SQLException
	{
		throw Util.notImplemented();
	}

    /////////////////////////////////////////////////////////////////////////
    //
    //	JDBC 4.0	-	New public methods
    //
    /////////////////////////////////////////////////////////////////////////
    /**
     * This method frees the <code>Clob</code> object and releases the resources the resources
     * that it holds.  The object is invalid once the <code>free</code> method
     * is called. If <code>free</code> is called multiple times, the
     * subsequent calls to <code>free</code> are treated as a no-op.
     *
     * @throws SQLException if an error occurs releasing
     * the Clob's resources
     */
    public void free()
        throws SQLException {
        //calling free() on a already freed object is treated as a no-op
        if (!isValid) return;

        //now that free has been called the Clob object is no longer
        //valid
        isValid = false;

        if (!materialized) {
            ((Resetable)myStream).closeStream();
        }
        else {
            try {
                control.free();
            }
            catch (IOException e) {
                throw Util.setStreamFailure(e);
            }
        }
    }

    public java.io.Reader getCharacterStream(long pos, long length)
        throws SQLException {
        throw Util.notImplemented();
    }

	/*
	**
	*/

	static SQLException noStateChangeLOB(Throwable t) {
        if (t instanceof StandardException)
        {
            // container closed means the blob or clob was accessed after commit
            if (((StandardException) t).getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED))
            {
                t = StandardException.newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
            }
        }
		return org.apache.derby.impl.jdbc.EmbedResultSet.noStateChangeException(t);
	}

        /*
         * Checks is isValid is true. If it is not true throws
         * a SQLException stating that a method has been called on
         * an invalid LOB object
         *
         * throws SQLException if isValid is not true.
         */
        private void checkValidity() throws SQLException{
            if(!isValid)
                throw newSQLException(SQLState.LOB_OBJECT_INVALID);
            localConn.checkIfClosed();
        }
}

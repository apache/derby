/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedClob

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

/*
    Implements java.sql.Clob (see the JDBC 2.0 spec).
    A clob sits on top of a CHAR, VARCHAR or LONG VARCHAR column.
    If its data is small (less than 1 page) it is a byte array taken from
    the SQLChar class. If it is large (more than 1 page) it is a long column
    in the database. The long column is accessed as a stream, and is implemented
    in store as an OverflowInputStream.  The Resetable interface allows sending
    messages to that stream to initialize itself (reopen its container and
    lock the corresponding row) and to reset itself to the beginning.

    NOTE: In the case that the data is large, it is represented as a stream.
    This stream can be returned to the user in the getAsciiStream() method.
    This means that we have limited control over the state of the stream,
    since the user can read bytes from it at any time.  Thus all methods
    here reset the stream to the beginning before doing any work.
    CAVEAT: The methods may not behave correctly if a user sets up
    multiple threads and sucks data from the stream (returned from
    getAsciiStream()) at the same time as calling the Clob methods.

 */
final class EmbedClob extends ConnectionChild implements Clob
{
    // clob is either a string or stream
    private boolean         isString;
    private InputStream     myStream;
    private String          myString;

    /*
    This constructor should only be called by EmbedResultSet.getClob
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
            isString = true;
           myString = dvd.getString();
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(myString != null,"clob has a null value underneath");
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

            ((Resetable)myStream).initStream();

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
        // if we have a string, not a stream
        if (isString)
            return myString.length();


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
   * characters.
   * @param pos the first character of the substring to be extracted.
   *            The first character is at position 1.
   * @param length the number of consecutive characters to be copied
   * @return a <code>String</code> that is the specified substring in
   *         the <code>CLOB</code> value designated by this <code>Clob</code> object
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code>

   NOTE: return the empty string if pos is too large
   */

    public String getSubString(long pos, int length) throws SQLException
    {
        if (pos < 1)
            throw Util.generateCsSQLException(
                SQLState.BLOB_BAD_POSITION, new Long(pos));
        if (length <= 0)
            throw Util.generateCsSQLException(
                SQLState.BLOB_NONPOSITIVE_LENGTH, new Integer(length));

        // if we have a string, not a stream
        if (isString)
        {
            int sLength = myString.length();
            if (sLength < pos)
                throw Util.generateCsSQLException(
                    SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
            int endIndex = ((int) pos) + length - 1;
            // cannot go over length of string, or we get an exception
            return myString.substring(((int) pos) - 1, (sLength > endIndex ? endIndex : sLength));
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

        // if we have a string, not a stream
        if (isString)
        {
            return new StringReader(myString);
        }


		Object synchronization = getConnectionSynchronization();
        synchronized (synchronization)
        {
            setupContextStack();

			try {
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
		return new ReaderToAscii(getCharacterStream());
    }

	private UTF8Reader getCharacterStreamAtPos(long position, Object synchronization)
		throws IOException, StandardException
	{
        ((Resetable)myStream).resetStream();
		UTF8Reader clobReader = new UTF8Reader(myStream, 0, this, synchronization);

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
   * <code>searchstr</code> appears in the <code>CLOB</code>.  The search
   * begins at position <code>start</code>.
   * @param searchstr the substring for which to search
   * @param start the position at which to begin searching; the first position
   *              is 1
   * @return the position at which the substring appears, else -1; the first
   *         position is 1
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code> value
   */
    public long position(String searchStr, long start)
        throws SQLException
    {
        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (searchStr == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN);
            if (searchStr == "")
                return start; // match DB2's SQL LOCATE function

            // if we have a string, not a stream
            if (isString)
            {
				// avoid truncation errors in the cast of start to an int.
				if (start > myString.length())
					return -1;

                int result = myString.indexOf(searchStr, (int) start-1);
                return result < 0 ? -1 : result + 1;
            }
            else // we have a stream
            {
				Object synchronization = getConnectionSynchronization();
                synchronized (synchronization)
                {
                    pushStack = !getEmbedConnection().isClosed();
                    if (pushStack)
                        setupContextStack();

					char[] tmpClob = new char[256];
					int patternLength = searchStr.length();

restartPattern:
					for (;;) {

					//System.out.println("RESET " + start);
						UTF8Reader clobReader = getCharacterStreamAtPos(start, synchronization);
						if (clobReader == null)
							return -1;



						// start of any match of the complete pattern.

						int patternIndex = 0;
						char[] tmpPattern = null;
						boolean needPattern = true;

						// how many characters of the patter segment we have matched
						int matchCount = 0;

						long currentPosition = start;
						int clobOffset = -1;
						int read = -1;

						// absolute position of a possible match
						long matchPosition = -1;


						// absolute position of the next possible match
						long nextBestMatchPosition = -1;
						//System.out.println("restartPattern: " + start);


search:
						for (;;)
						{
							//System.out.println("search: " + needPattern + " -- " + clobOffset);
							if (needPattern) {

								String tmpPatternS;
								if ((patternLength - patternIndex) > 256)
									tmpPatternS = searchStr.substring(patternIndex, 256);
								else
									tmpPatternS = searchStr;

								tmpPattern = tmpPatternS.toCharArray();
								needPattern = false;
								matchCount = 0;

							}

							if (clobOffset == -1) {
								
								read = clobReader.read(tmpClob, 0, tmpClob.length);
							//System.out.println("MORE DATA " + read);
								if (read == -1)
									return -1;

								if (read == 0)
									continue search;

								clobOffset = 0;
							}


							// find matches within our two temp arrays.
compareArrays:
							for (; clobOffset < read; clobOffset++) {

								//System.out.println("compareArrays " + clobOffset);

								char clobC = tmpClob[clobOffset];


								if (clobC == tmpPattern[matchCount])
								{
									if (matchPosition == -1) {
										matchPosition = currentPosition + clobOffset;
									}

									matchCount++;

									// have we matched the entire pattern segment
									if (matchCount == tmpPattern.length)
									{
										// move onto the next segment.
										patternIndex += tmpPattern.length;
										if (patternIndex == patternLength) {
											// complete match !!
											clobReader.close();
											//System.out.println("COMPLETE@" + matchPosition);
											return matchPosition;
										}

										needPattern = true;
										continue search;

									}

									if (clobC == tmpPattern[0]) {

										// save the next best start position.

										// must be the first character of the actual pattern
										if (patternIndex == 0) {

											// must not be just a repeat of the match of the first character
											if (matchCount != 1) {

												// must not have a previous next best.

												if (nextBestMatchPosition == -1) {
													nextBestMatchPosition = currentPosition + clobOffset;
												}

											}

										}
									}

									continue compareArrays;
								}
								else
								{
									// not a match
									//
									// 
									if (matchPosition != -1) {
										// failed after we matched some amount of the pattern
										matchPosition = -1;

										// See if we found a next best match
										if (nextBestMatchPosition == -1)
										{
											// NO - just continue on, re-starting at this character

											if (patternIndex != 0) {
												needPattern = true;
												continue search;
											}
										}
										else if (nextBestMatchPosition >= currentPosition)
										{
											// restart in the current array
											clobOffset = (int) (nextBestMatchPosition - currentPosition);
											nextBestMatchPosition = -1;
									
											if (patternIndex != 0) {
												needPattern = true;
												continue search;
											}
										}
										else
										{
											clobReader.close();
											start = nextBestMatchPosition;
											continue restartPattern;
										}

										clobOffset--; // since the continue will increment it
										matchCount = 0;
										continue compareArrays;
									}
									
									// no current match, just continue
								}
							}

							currentPosition += read;

							// indicates we need to read more data
							clobOffset = -1;
						}
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
   * @param searchstr the <code>Clob</code> object for which to search
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
        boolean pushStack = false;
        try
        {
            if (start < 1)
                throw StandardException.newException(
                    SQLState.BLOB_BAD_POSITION, new Long(start));
            if (searchClob == null)
                throw StandardException.newException(SQLState.BLOB_NULL_PATTERN);

            synchronized (getConnectionSynchronization())
            {
				char[] subPatternChar = new char[256];

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
        if (!isString)
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
    * @param str - the string to be written to the CLOB value that this Clob designates
    * @return the number of characters written 
    * @exception SQLException Feature not implemented for now.
	*/
	public int setString(long pos, String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
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
    throws SQLException
	{
		throw Util.notImplemented();
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
	public java.io.OutputStream setAsciiStream(long pos)
    throws SQLException
	{
		throw Util.notImplemented();
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
	public java.io.Writer setCharacterStream(long pos)
    throws SQLException
	{
		throw Util.notImplemented();
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
	public void truncate(long len)
    throws SQLException
	{
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

}

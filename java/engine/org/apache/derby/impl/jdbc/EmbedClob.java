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
import org.apache.derby.iapi.jdbc.EngineClob;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.impl.jdbc.ConnectionChild;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.ReaderToAscii;

import java.io.InputStream;
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
final class EmbedClob extends ConnectionChild implements Clob, EngineClob
{

    /**
     * The underlying Clob object, which may change depending on what the user
     * does with the Clob.
     */
    private InternalClob clob;

    /** Tells whether the Clob has been freed or not. */
    private boolean isValid = true;

    private final int locator;
    
    /**
     * Creates an empty Clob object.
     *
     * @param con The Connection object associated with this EmbedClob object.
     * @throws SQLException
     *
     */
    EmbedClob(EmbedConnection con) throws SQLException {
        super(con);
        this.clob = new TemporaryClob (con.getDBName(), this);
        this.locator = con.addLOBMapping (this);
    }

    /**
     * Creates a Clob on top of a data value descriptor.
     * <p>
     * This constructor should only be called by {@link EmbedResultSet#getClob}.
     * The data value descriptor may provide a <code>String</code> or a stream
     * as the source of the Clob.
     *
     * @param dvd data value descriptor providing the Clob source
     * @param con associated connection for the Clob
     * @throws StandardException
     */
    protected EmbedClob(EmbedConnection con, DataValueDescriptor dvd)
        throws StandardException
    {
        super(con);
        // if the underlying column is null, ResultSet.getClob will return null,
        // never should get this far
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(!dvd.isNull(),
                                 "clob is created on top of a null column");

        InputStream storeStream = dvd.getStream();
        // See if a String or a stream will be the source of the Clob.
        if (storeStream == null) {
            try {
                clob = new TemporaryClob(con.getDBName(),
                        dvd.getString(), this);
            }
            catch (SQLException sqle) {
                throw StandardException.newException (sqle.getSQLState(), sqle);
            }
            catch (IOException e) {
                throw StandardException.newException (
                                        SQLState.SET_STREAM_FAILURE, e);
            }
        } else {
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
                SanityManager.ASSERT(storeStream instanceof Resetable);

            try {
                this.clob = new StoreStreamClob(storeStream, this);
            } catch (StandardException se) {
                if (se.getMessageId().equals(SQLState.DATA_CONTAINER_CLOSED)) {
                    throw StandardException
                            .newException(SQLState.BLOB_ACCESSED_AFTER_COMMIT);
                }
                throw se;
            }
        }
        this.locator = con.addLOBMapping (this);
    }

    /**
     * Returns the number of characters in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     *
     * @return The length of the <code>CLOB</code> in number of characters.
     * @exception SQLException if obtaining the length fails
     */
    public long length() throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();
        try {
            return this.clob.getCharLength();
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        }
    }

    /**
     * Returns a copy of the specified substring in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * <p>
     * The substring begins at position <code>pos</code> and has up to
     * * <code>length</code> consecutive characters. The starting position must
     * be between 1 and the length of the CLOB plus 1. This allows for 
     * zero-length CLOB values, from which only zero-length substrings can be
     * returned.
     * <p>
     * If a larger length is requested than there are characters available,
     * characters from the start position to the end of the CLOB are returned.
     * <p>
     * <em>NOTE</em>: If the starting position is the length of the CLOB plus 1,
     * zero characters are returned regardless of the length requested.
     *
     * @param pos the first character of the substring to be extracted.
     *    The first character is at position 1.
     * @param length the number of consecutive characters to be copied
     * @return A <code>String</code> that is the specified substring in the
     *    <code>CLOB</code> value designated by this <code>Clob</code> object
     * @exception SQLException if there is an error accessing the
     *    <code>CLOB</code>
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

        String result;
        // An exception will be thrown if the position is larger than the Clob.
        try {
            Reader reader = this.clob.getReader(pos);
            char[] chars = new char[length];
            int charsRead = 0;
            // Read all the characters requested, or until EOF is reached.
            while (charsRead < length) {
                int read = reader.read(chars, charsRead, length - charsRead);
                if (read == -1) {
                    break;
                }
                charsRead += read;
            }
            reader.close();
            // If we have an empty Clob or requested length is zero, return "".
            if (charsRead == 0) {
                result = "";
            } else {
                result = String.copyValueOf(chars, 0, charsRead);
            }
        } catch (EOFException eofe) {
            throw Util.generateCsSQLException(
                                        SQLState.BLOB_POSITION_TOO_LARGE, eofe);
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        }
        return result;
    }

    /**
     * Gets the <code>Clob</code> contents as a stream of characters.
     * @return A character stream containing the <code>CLOB</code> data.
     * @exception SQLException if there is an error accessing the
     *    <code>CLOB</code>
     */
    public java.io.Reader getCharacterStream() throws SQLException
    {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();
        try {
            return new ClobUpdatableReader (this);
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        }
    }

    /**
     * Gets the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a stream of Ascii bytes.
     * @return An Ascii stream containing the <code>CLOB</code> data. Valid
     *      values in the stream are 0 - 255.
     * @exception SQLException if there is an error accessing the
     *    <code>CLOB</code> value
     */
    public java.io.InputStream getAsciiStream() throws SQLException
    {
        // Validity is checked in getCharacterStream().
        return new ReaderToAscii(getCharacterStream());
    }

    /**
     * Determines the character position at which the specified substring
     * <code>searchStr</code> appears in the <code>CLOB</code> value.
     * <p>
     * The search begins at position <code>start</code>. The method uses the
     * following algorithm for the search:
     * <p>
     * If the <code>CLOB</code> value is materialized as a string, use
     * <code>String.indexOf</code>.
     * <p>
     * If the <code>CLOB</code> value is represented as a stream, read a block
     * of chars from the start position and compare the chars with
     * <code>searchStr</code>. Then:
     * <ul> <li>If a matching char is found, increment <code>matchCount</code>.
     *      <li>If <code>matchCount</code> is equal to the length of
     *          <code>searchStr</code>, return with the current start position.
     *      <li>If no match is found, and there is more data, restart search
     *          (see below).
     *      <li>If no match is found, return <code>-1</code>.
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
        if (start < 1)
            throw Util.generateCsSQLException(
                            SQLState.BLOB_BAD_POSITION, new Long(start));
        if (searchStr == null)
            throw Util.generateCsSQLException(
                            SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);
        if (searchStr == "")
            return start; // match DB2's SQL LOCATE function

        boolean pushStack = false;
        try
        {

            Object synchronization = getConnectionSynchronization();
            synchronized (synchronization)
            {
                pushStack = !getEmbedConnection().isClosed();
                if (pushStack)
                    setupContextStack();
                int matchCount = 0;
                long pos = start;
                long newStart = -1;
                Reader reader = this.clob.getReader(start);
                char [] tmpClob = new char [4096];
                boolean reset;
                for (;;) {
                    reset = false;
                    int readCount = reader.read (tmpClob);
                    if (readCount == -1)
                        return -1;
                    for (int clobOffset = 0;
                                clobOffset < readCount; clobOffset++) {
                        if (tmpClob[clobOffset]
                                        == searchStr.charAt(matchCount)) {
                            //find the new starting position in
                            // case this match is unsuccessful
                            if (matchCount != 0 && newStart == -1
                                    && tmpClob[clobOffset]
                                    == searchStr.charAt(0)) {
                                newStart = pos + clobOffset + 1;
                            }
                            matchCount ++;
                            if (matchCount == searchStr.length()) {
                                return pos + clobOffset
                                        - searchStr.length() + 1;
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
                                    reader = this.clob.getReader(newStart);
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
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        } finally {
            if (pushStack) {
                restoreContextStack();
            }
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
        if (start < 1)
            throw Util.generateCsSQLException(
                                SQLState.BLOB_BAD_POSITION, new Long(start));
        if (searchClob == null)
            throw Util.generateCsSQLException(
                                SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR);

        boolean pushStack = false;
        try
        {
            synchronized (getConnectionSynchronization())
            {
                char[] subPatternChar = new char[1024];

                boolean seenOneCharacter = false;

restartScan:
                for (;;) {
                    long firstPosition = -1;
                    Reader patternReader = searchClob.getCharacterStream();
                        for (;;) {

                            int read = patternReader.read(subPatternChar, 0,
                                                        subPatternChar.length);
                            if (read == -1) {
                                //empty pattern
                                if (!seenOneCharacter)
                                    // matches DB2 SQL LOCATE function
                                    return start;
                                return firstPosition;
                            }
                            if (read == 0) {
                                continue;
                            }
                            seenOneCharacter = true;

                            String subPattern =
                                new String(subPatternChar, 0, read);
                            long position = position(subPattern, start);
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
                                // must match at the first character of segment
                                start = firstPosition + 1;
                                continue restartScan;
                            }

                            // read is the length of the subPattern string
                            start = position + read;
                    } // End inner for loop
            } // End outer for loop
        } // End synchronized block
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        } finally {
            if (pushStack) {
                restoreContextStack();
            }
        }
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
    //    JDBC 3.0    -    New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * JDBC 3.0
     *
     * Writes the given Java String to the CLOB value that this Clob object
     * designates at the position pos.
     *
     * @param pos the position at which to start writing to the CLOB value that
     *      this Clob object represents
     * @return the number of characters written
     * @throws SQLException if writing the string fails
     */
    public int setString(long pos, String str) throws SQLException {
            return setString (pos, str, 0, str.length());
    }

    /**
     * JDBC 3.0
     *
     * Writes len characters of str, starting at character offset, to the CLOB
     * value that this Clob represents.
     *
     * @param pos the position at which to start writing to this Clob object
     * @param str the string to be written to the CLOB value that this Clob
     *      designates
     * @param offset the offset into str to start reading the characters to be
     *      written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @exception SQLException if writing the string fails
     */
    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        checkValidity();
        if (pos < 1)
            throw Util.generateCsSQLException(
                SQLState.BLOB_BAD_POSITION, new Long(pos));
        try {
            if (!this.clob.isWritable()) {
                makeWritableClobClone();
            }
            // Note that Clob.length() +1 is a valid position for setString.
            // If the position is larger than this, an EOFException will be
            // thrown. This is cheaper then getting the length up front.
            this.clob.insertString(str.substring(offset, (offset + len)),
                                   pos);
        } catch (EOFException eofe) {
            throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE,
                        new Long(pos));
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        }
        return str.length();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves a stream to be used to write Ascii characters to the CLOB
     * value that this Clob object represents, starting at position pos.
     *
     * @param pos the position at which to start writing to this Clob object
     * @return the stream to which ASCII encoded characters can be written
     * @exception SQLException if obtaining the stream fails
     */
    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        checkValidity();
        try {
            return new ClobAsciiStream (this.clob.getWriter(pos));
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        }
    }

    /**
     * JDBC 3.0
     *
     * Retrieves a stream to be used to write a stream of characters to the CLOB
     * value that this Clob object represents, starting at position pos.
     *
     * @param pos the position at which to start writing to this Clob object
     * @return the stream to which Unicode encoded characters can be written
     * @exception SQLException if obtaining the stream fails
     */
    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        checkValidity();
        try {
            if (!this.clob.isWritable()) {
                makeWritableClobClone();
            }
            return this.clob.getWriter(pos);
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        }
    }

    /**
     * JDBC 3.0
     *
     * Truncates the CLOB value that this Clob designates to have a length of
     * len characters
     *
     * @param len the length, in characters, to which the CLOB value should be
     *      truncated
     * @exception SQLException if truncating the CLOB value fails
     */
    public void truncate(long len) throws SQLException
    {
        checkValidity();
        if (len < 1)
            throw Util.generateCsSQLException(
                SQLState.BLOB_BAD_POSITION, new Long(len));
        try {
            if (!clob.isWritable()) {
                makeWritableClobClone(len);
            }
            else {
                clob.truncate (len);
            }
        }
        catch (EOFException eofe) {
            throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE,
                        new Long(len));
        } catch (IOException e) {
            throw Util.setStreamFailure(e);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //    JDBC 4.0    -    New public methods
    //
    /////////////////////////////////////////////////////////////////////////
    /**
     * Frees the <code>Clob</code> and releases the resources that it holds.
     * <p>
     * The object is invalid once the <code>free</code> method
     * is called. If <code>free</code> is called multiple times, the
     * subsequent calls to <code>free</code> are treated as a no-op.
     *
     * @throws SQLException if an error occurs releasing the Clobs resources
     */
    public void free()
        throws SQLException {
        if (this.isValid) {
            this.isValid = false;
            // Release and nullify the internal Clob.
            try {
                this.clob.release();
            } catch (IOException e) {
                throw Util.setStreamFailure(e);
            } finally {
                this.clob = null;
            }
        }
    }

    /**
     * Returns a <code>Reader</code> object that contains a partial
     * <code>Clob</code> value, starting with the character specified by pos,
     * which is length characters in length.
     *
     * @param pos the offset to the first character of the partial value to
     * be retrieved.  The first character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be
     * retrieved.
     * @return <code>Reader</code> through which the partial <code>Clob</code>
     * value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the
     * number of
     * characters in the <code>Clob</code> or if pos + length is greater than
     * the number of
     * characters in the <code>Clob</code>
     *
     * @throws SQLException.
     */
    public java.io.Reader getCharacterStream(long pos, long length)
        throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
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
        if (length > (this.length() - pos)) {
            throw Util.generateCsSQLException(
                    SQLState.POS_AND_LENGTH_GREATER_THAN_LOB,
                    new Long(pos), new Long(length));
        }
        
        try {
            return new ClobUpdatableReader(this,
                                            pos-1,
                                            length);
        } catch (IOException ioe) {
            throw Util.setStreamFailure(ioe);
        } 
    }

    /*
     * Checks if the Clob is valid.
     * <p>
     * A Clob is invalidated when {@link #free} is called or if the parent
     * connection is closed.
     *
     * @throws SQLException if the Clob is not valid
     */
    private void checkValidity() throws SQLException{
        localConn.checkIfClosed();        
        if(!isValid)
            throw newSQLException(SQLState.LOB_OBJECT_INVALID);
    }

    /**
     * Makes a writable clone of the current Clob.
     * <p>
     * This is called when we have a {@link StoreStreamClob} and the user calls
     * a method updating the content of the Clob. A temporary Clob will then be
     * created to hold the updated content.
     *
     * @throws IOException if accessing underlying I/O resources fail
     * @throws SQLException if accessing underlying resources fail
     */
    private void makeWritableClobClone()
            throws IOException, SQLException {
        InternalClob toBeAbandoned = this.clob;
        this.clob = TemporaryClob.cloneClobContent(
                        getEmbedConnection().getDBName(),
                        this, toBeAbandoned);
        toBeAbandoned.release();
    }

    /**
     * Makes a writable clone of the current Clob.
     * <p>
     * This is called when we have a {@link StoreStreamClob} and the user calls
     * a method updating the content of the Clob. A temporary Clob will then be
     * created to hold the updated content.
     *
     * @param len number of characters to be cloned (should be smaller
     *      than clob length)
     * @throws IOException if accessing underlying I/O resources fail
     * @throws SQLException if accessing underlying resources fail
     */
    private void makeWritableClobClone(long len)
            throws IOException, SQLException {
        InternalClob toBeAbandoned = this.clob;
        this.clob = TemporaryClob.cloneClobContent(
                        getEmbedConnection().getDBName(),
                        this, toBeAbandoned, len);
        toBeAbandoned.release();
    }

    /**
     * Returns the current internal Clob representation.
     * <p>
     * Care should be taken, as the representation can change when the user
     * performs operations on the Clob. An example is if the Clob content is
     * served from a store stream and the user updates the content. The
     * internal representation will then be changed to a temporary Clob copy
     * that allows updates.
     *
     * @return The current internal Clob representation.
     */
    InternalClob getInternalClob() {
        return this.clob;
    }

    /**     
     * @return locator value for this Clob.
     */
    public int getLocator() {
        return locator;
    }
}

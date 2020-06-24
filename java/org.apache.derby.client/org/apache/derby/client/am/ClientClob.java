/*

   Derby - Class org.apache.derby.client.am.ClientClob

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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

import org.apache.derby.client.net.EncodedInputStream;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This class implements the JDBC {@code java.sql.Clob} interface.
 */
public class ClientClob extends Lob implements Clob {
    //---------------------navigational members-----------------------------------

    //-----------------------------state------------------------------------------
    protected String string_ = null;

    // Only used for input purposes.  For output, each getXXXStream call
    // must generate an independent stream.
    private InputStream asciiStream_ = null;
    private InputStream unicodeStream_ = null;
    private Reader characterStream_ = null;

    // used for input
    // Therefore, we always convert a String to UTF-8 before we flow it for input
    private byte[] utf8String_;

    //---------------------constructors/finalizer---------------------------------
    public ClientClob(Agent agent, String string) {

        this(agent,
             false);

        string_ = string;
        setSqlLength(string_.length());
        dataType_ |= STRING;
    }

    // CTOR for output, when a btc isn't available; the encoding is
    public ClientClob(Agent agent,
                byte[] unconvertedBytes,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                Charset charset,
                int dataOffset) throws SqlException {

        this(agent,
             false);

        // check for null encoding is needed because the net layer
        // will no longer throw an exception if the server didn't specify
        // a mixed or double byte ccsid (ccsid = 0).  this check for null in the
        // cursor is only required for types which can have mixed or double
        // byte ccsids.
        if (charset == null) {
            throw new SqlException(agent.logWriter_,
                new ClientMessageId(SQLState.CHARACTER_CONVERTER_NOT_AVAILABLE));
        }

        string_ = new String(unconvertedBytes,
                dataOffset,
                unconvertedBytes.length - dataOffset,
                charset);
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        setSqlLength(string_.length());
        dataType_ |= STRING;
    }

    // CTOR for ascii/unicode stream input
    //"ISO-8859-1", "UTF-8", or "UnicodeBigUnmarked"
    public ClientClob(Agent agent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                InputStream inputStream,
                Charset encoding,
                int length) {

        this(agent,
             false);

        setSqlLength(length);
//IC see: https://issues.apache.org/jira/browse/DERBY-2540

        if (encoding.equals(Cursor.ISO_8859_1)) {
            asciiStream_ = inputStream;
            dataType_ |= ASCII_STREAM;
        } else if (encoding.equals(Cursor.UTF_8)) {
            unicodeStream_ = inputStream;
            dataType_ |= UNICODE_STREAM;
        } else if (encoding.equals(Cursor.UTF_16BE)) {
            characterStream_ =
                    new InputStreamReader(inputStream, Cursor.UTF_16BE);
            dataType_ |= CHARACTER_STREAM;
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
            setSqlLength(length / 2);
        }
    }

    /**
     * Create a <code>ClientClob</code> of unknown length with the specified
     * encoding.
     *
     * This constructor was added to support the JDBC 4 length less overloads.
     * Note that a <code>ClientClob</code> created with this constructor is
     * made for input to the database only. Do not pass it out to the user!
     *
     * @param agent
     * @param inputStream the data to insert
     * @param encoding encoding to use for characters. Only "ISO-8859-1" is
     *      allowed.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
    ClientClob(Agent agent, InputStream inputStream, Charset encoding)
            throws SqlException {

        this(agent,
             isLayerBStreamingPossible( agent ));

        if (encoding.equals(Cursor.ISO_8859_1)) {
            asciiStream_ = inputStream;
            dataType_ |= ASCII_STREAM;
        } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.UNSUPPORTED_ENCODING),
                encoding + " InputStream", "String/Clob");
        }
    }

    // CTOR for character stream input
    // THE ENCODING IS ASSUMED TO BE "UTF-16BE"
    ClientClob(Agent agent, Reader reader, int length) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

        this(agent,
             false);

//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        setSqlLength(length);
        characterStream_ = reader;
        dataType_ |= CHARACTER_STREAM;
    }

    /**
     * Create a <code>ClientClob</code> object for a Clob value stored
     * on the server and indentified by <code>locator</code>.
     * @param agent context for this <code>Clob</code>
     *              object (incl. connection).
     * @param locator reference id to <code>Clob</code> value on server.
     */
    public ClientClob(Agent agent, int locator)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        super(agent, false);
        locator_ = locator;
        dataType_ |= LOCATOR;
    }

    /**
     * Create a <code>ClientClob</code> of unknown length.
     *
     * This constructor was added to support the JDBC 4 length less overloads.
     * Note that a <code>ClientClob</code> created with this constructor is
     * made for input to the database only. Do not pass it out to the user!
     *
     * @param agent
     * @param reader the data to insert
     */
    ClientClob(Agent agent, Reader reader) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

        this(agent,
             isLayerBStreamingPossible( agent ) );

        // Wrap reader in stream to share code.
        unicodeStream_ = EncodedInputStream.createUTF8Stream(reader);
        // Override type to share logic with the other stream types.
        dataType_ |= UNICODE_STREAM;
    }

    private ClientClob(Agent agent,
                 boolean willBeLayerBStreamed) {
        super(agent,
              willBeLayerBStreamed);
    }

    // ---------------------------jdbc 2------------------------------------------
    // Create another method lengthX for internal calls
    public long length() throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "length");
                }

                long length = super.sqlLength();
//IC see: https://issues.apache.org/jira/browse/DERBY-2540

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "length", length);
                }
                return length;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

  /**
   * Returns a copy of the specified substring
   * in the <code>CLOB</code> value
   * designated by this <code>ClientClob</code> object.
   * The substring begins at position
   * <code>pos</code> and has up to <code>length</code> consecutive
   * characters. The starting position must be between 1 and the length
   * of the CLOB plus 1. This allows for zero-length CLOB values, from
   * which only zero-length substrings can be returned.
   * If a larger length is requested than there are characters available,
   * characters to the end of the CLOB are returned.
   * @param pos the first character of the substring to be extracted.
   *            The first character is at position 1.
   * @param length the number of consecutive characters to be copied
   * @return a <code>String</code> that is the specified substring in the
   *         <code>CLOB</code> value designated by this <code>ClientClob</code>
   *         object
   * @exception SQLException if there is an error accessing the
   * <code>CLOB</code>

   * NOTE: If the starting position is the length of the CLOB plus 1,
   * zero characters are returned regardless of the length requested.
   */
    public String getSubString(long pos, int length) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                String retVal = null;

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getSubString", (int) pos, length);
                }

                if ( pos <= 0 ) {
                    throw new SqlException(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION), pos);
                }

                if ( length < 0 ) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
                        length);
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-2540
                if (pos > sqlLength() + 1) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_POSITION_TOO_LARGE),
                        pos);
                }
                retVal = getSubStringX(pos, length);

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getSubString", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private String getSubStringX(long pos, int length) throws SqlException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        checkForClosedConnection();
        // actual length is the lesser of the length requested
        // and the number of characters available from pos to the end
        long actualLength = Math.min(this.sqlLength() - pos + 1, (long) length);
        //Check to see if the Clob object is locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        if (isLocator()) {
            //The Clob object is locator enabled. Hence call the stored
            //procedure CLOBGETLENGTH to determine the length of the Clob.
            return agent_.connection_.locatorProcedureCall()
                .clobGetSubString(locator_, pos, (int)actualLength);
        }
        else {
            //The Clob object is not locator enabled.
            return string_.substring
                    ((int) pos - 1, (int) (pos - 1 + actualLength));
        }
    }

    public Reader getCharacterStream() throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getCharacterStream");
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                Reader retVal = getCharacterStreamX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getCharacterStream", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    Reader getCharacterStreamX() throws SqlException {
        checkForClosedConnection();

        //check is this Lob is locator enabled
        if (isLocator()) {
            //The Lob is locator enabled. Return an instance of the
            //update sensitive Reader that wraps inside it a
            //Buffered Locator Reader. The wrapper class
            //watches out for updates.
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
            return new UpdateSensitiveClobLocatorReader
                    (agent_.connection_, this);
        }
        else if (isCharacterStream())  // this Lob is used for input
        {
            return characterStream_;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        return new StringReader(string_);
    }

    public InputStream getAsciiStream() throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getAsciiStream");
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                InputStream retVal = getAsciiStreamX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getAsciiStream", retVal);
                }
                return retVal;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    InputStream getAsciiStreamX() throws SqlException {
        checkForClosedConnection();

        if (isAsciiStream())  // this Lob is used for input
        {
            return asciiStream_;
        }
        else if(isLocator()) { // Check to see if this Lob is locator enabled
            //The Lob is locator enabled. Return an instance
            //of the update sensitive wrappers that wrap inside
            //it a Buffered Locator enabled InputStream. The
            //wrapper watches out for updates to the underlying
            //Clob.
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
            return new UpdateSensitiveClobLocatorInputStream
                    (agent_.connection_,this);
        }
        else {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            return new AsciiStream(string_, new StringReader(string_));
        }
    }

    public long position(String searchstr, long start) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this,
                            "position(String, long)",
                            searchstr,
                            start);
                }
                if (searchstr == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR));
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-1516
//IC see: https://issues.apache.org/jira/browse/DERBY-1516
                if (start < 1) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION),
                            start);
                }

                long pos = positionX(searchstr, start);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "position(String, long)", pos);
                }
                return pos;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private long positionX(String searchstr, long start) throws SqlException {
        checkForClosedConnection();

//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        long index = -1;
        if (start <= 0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.INVALID_API_PARAMETER),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                start, "start", "Clob.position()");
        }

        //Check is locator support is available for this LOB.
        if (isLocator()) {
            //Locator support is available. Hence call
            //CLOBGETPOSITIONFROMSTRING to determine the position
            //of the given substring inside the LOB.
            index = agent_.connection_.locatorProcedureCall()
                .clobGetPositionFromString(locator_, searchstr, start);
        } else {
            //Locator support is not available.
            index = string_.indexOf(searchstr, (int) start - 1);
            if (index != -1) {
                index++; // api index starts at 1
            }
        }
        return index;
    }

    public long position(Clob searchstr, long start) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this,
                            "position(Clob, long)",
                            searchstr,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                            start);
                }
                if (start < 1) {
                    throw new SqlException(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        new ClientMessageId(SQLState.BLOB_BAD_POSITION), start);
                }

                if (searchstr == null) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR));
                }
                long pos = positionX(searchstr, start);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "position(Clob, long)", pos);
                }
                return pos;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private long positionX(Clob searchstr, long start) throws SqlException {
        checkForClosedConnection();

        if (start <= 0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.INVALID_API_PARAMETER),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                start, "start", "Clob.position()");
        }

        // if the searchstr is longer than the source, no match
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        long index;
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
            if (searchstr.length() > sqlLength()) {
                return -1;
            }

            //Check if locator support is available for this LOB.
            if (isLocator()) {
                //Locator support is available. Hence call
                //CLOBGETPOSITIONFROMLOCATOR to determine the position
                //of the given Clob inside the LOB.
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                index = agent_.connection_.locatorProcedureCall()
                    .clobGetPositionFromLocator(locator_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        ((ClientClob)searchstr).getLocator(),
                        start);
            } else {
                //Locator support is not available.
                index = string_.indexOf(searchstr.getSubString(1L,
                                                    (int) searchstr.length()),
                                        (int) start - 1);
                //increase the index by one since String positions are
                //0-based and Clob positions are 1-based
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                if (index != -1) {
                    index++;
                }
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        } catch (SQLException e) {
            throw new SqlException(e);
        }
        return index;
    }

    //---------------------------- jdbc 3.0 -----------------------------------

    public int setString(long pos, String str) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setString", (int) pos, str);
                }
                int length = setStringX(pos, str, 0, str.length());
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setString", length);
                }
                return length;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setString", (int) pos, str, offset, len);
                }
                int length = setStringX(pos, str, offset, len);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setString", length);
                }
                return length;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    int setStringX(long pos, String str, int offset, int len)
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SqlException {
        if ((int) pos <= 0 ) {
            throw new SqlException(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                new ClientMessageId(SQLState.BLOB_BAD_POSITION), pos);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        if ( pos - 1 > sqlLength()) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_POSITION_TOO_LARGE), pos);
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2769
        if (str == null) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(
                            SQLState.BLOB_NULL_PATTERN_OR_SEARCH_STR));
        }
        
        if (str.length() == 0) {
            return 0;
        }
        
        if ((offset < 0) || offset >= str.length() ) {
            throw new SqlException(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                new ClientMessageId(SQLState.BLOB_INVALID_OFFSET), offset);
        }

        if ( len < 0 ) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH), len);
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2769
        if (offset + len > str.length()) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(
                            SQLState.LANG_SUBSTR_START_ADDING_LEN_OUT_OF_RANGE),
                    offset, len, str);
        }

        if (len == 0) {
            return 0;
        }

        int length = 0;
        length = Math.min((str.length() - offset), len);
        //check if the Clob object is locator enabled
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
        if (isLocator()) {
            //The Clob is locator enabled. Call the CLOBSETSTRING
            //stored procedure to set the given string in the Clob.
            agent_.connection_.locatorProcedureCall().clobSetString
                (locator_, pos, length, str.substring(offset, offset + length));
            if (pos+length-1 > sqlLength()) { // Wrote beyond the old end
                // Update length
                setSqlLength(pos + length - 1);
            }
            //The Clob value has been
            //updated. Increment the
            //update count.
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
            incrementUpdateCount();
        }
        else {
            //The Clob is not locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            reInitForNonLocator(
                string_.substring(0, (int) pos - 1)
                    .concat(str.substring(offset, offset + length)));
        }
        return length;
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setAsciiStream", (int) pos);
                }
                OutputStream outStream = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                if(isLocator()) { // Check to see if the Lob is locator enabled
                    //The Lob is locator enabled. Return an instance of the
                    //Locator enabled Clob specific OutputStream implementation.
                    outStream = new ClobLocatorOutputStream
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            (this, pos);
                }
                else {
                    //The Lob is not locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
                    outStream = new
                            ClobOutputStream(this, pos);
                }
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setAsciiStream", outStream);
                }
                return outStream;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public Writer setCharacterStream(long pos) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setCharacterStream", (int) pos);
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                Writer writer = null;
                //Check to see if this Clob is locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                if (isLocator()) {
                    //return an instance of the locator enabled implementation
                    //of the writer interface
                    writer = new ClobLocatorWriter(agent_.connection_, this, pos);
                }
                else {//The Lob is not locator enabled.
                    writer = new ClobWriter(this, pos);
                }

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "setCharacterStream", writer);
                }
                return writer;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void truncate(long len) throws SQLException {

        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
//IC see: https://issues.apache.org/jira/browse/DERBY-1328

        try
        {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, " truncate", (int) len);
                }
                if (len < 0 ) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        len);
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-2540
                if ( len > sqlLength()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.BLOB_LENGTH_TOO_LONG),
                        len);
                }

                if (len == sqlLength()) {
                    return;
                }

                //check whether the Lob is locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
                if (isLocator()) {
                    //The Lob is locator enabled then call the stored
                    //procedure CLOBTRUNCATE to truncate this Lob.
                    agent_.connection_.locatorProcedureCall().
                            clobTruncate(locator_, len);
                    // The Clob value has been modified.
                    // Increment the update count and update the length.
                    incrementUpdateCount();
//IC see: https://issues.apache.org/jira/browse/DERBY-3978
                    setSqlLength(len);
                }
                else {
                    //The Lob is not locator enabled.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                    reInitForNonLocator(string_.substring(0, (int) len));
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //---------------------------- jdbc 4.0 -------------------------------------
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
        if (!isValid_) return;

        //now that free has been called the Blob object is no longer
        //valid
        isValid_ = false;

//IC see: https://issues.apache.org/jira/browse/DERBY-3107
        try {
            synchronized (agent_.connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "free");
                }
                if (isLocator()) {
                    agent_.connection_.locatorProcedureCall()
                        .clobReleaseLocator(locator_);
                }
            }
        } catch (SqlException se) {
            throw se.getSQLException();
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-1328
        if(isString()) {
            string_ = null;
            utf8String_ = null;
        }
        if(isAsciiStream()) {
            try {
                asciiStream_.close();
            }
            catch(IOException ioe) {
                throw new SqlException(null, new ClientMessageId(SQLState.IO_ERROR_UPON_LOB_FREE)).getSQLException();
            }
        }
        if(isUnicodeStream()) {
            try {
                unicodeStream_.close();
            }
            catch(IOException ioe) {
                throw new SqlException(null, new ClientMessageId(SQLState.IO_ERROR_UPON_LOB_FREE)).getSQLException();
            }
        }
        if(isCharacterStream()) {
            try {
                characterStream_.close();
            }
            catch(IOException ioe) {
                throw new SqlException(null, new ClientMessageId(SQLState.IO_ERROR_UPON_LOB_FREE)).getSQLException();
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
     * characters in the {@code Clob} or if {@code pos + length} is greater than
     * {@code Clob.length() +1}
     */
    public Reader getCharacterStream(long pos, long length)
        throws SQLException {
        //call checkValidity to exit by throwing a SQLException if
        //the Clob object has been freed by calling free() on it
        checkValidity();
//IC see: https://issues.apache.org/jira/browse/DERBY-2444

        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream",
                    (int) pos, length);
            }
            checkPosAndLength(pos, length);
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
            Reader retVal = null;
            //check if the Lob is locator enabled.
            if(isLocator()) {
                //1) The Lob is locator enabled. Return the update
                //   sensitive wrapper that wraps inside it a
                //   locator enabled Clob Reader. The wrapper
                //   watches out for updates to the underlying
                //   Clob.
                //2) len is the number of characters in the
                //   stream starting from pos.
                //3) checkPosAndLength will ensure that pos and
                //   length fall within the boundaries of the
                //   Clob object.
                try {
//IC see: https://issues.apache.org/jira/browse/DERBY-2763
                    retVal = new UpdateSensitiveClobLocatorReader
                            (agent_.connection_, this,
                            pos, length);
                }
                catch(SqlException sqle) {
                    throw sqle.getSQLException();
                }
            }
            else {
                //The Lob is not locator enabled.
                String retVal_str = null;
                try {
                    retVal_str = getSubStringX(pos, (int)length);
                }
                catch(SqlException sqle) {
                    throw sqle.getSQLException();
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                retVal = new StringReader(retVal_str);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getCharacterStream", retVal);
                }
            }
            return retVal;
        }
    }

    //----------------------------helper methods----------------------------------

    public boolean isString() {
        return ((dataType_ & STRING) == STRING);
    }

    public boolean isAsciiStream() {
        return ((dataType_ & ASCII_STREAM) == ASCII_STREAM);
    }

    public boolean isCharacterStream() {
        return ((dataType_ & CHARACTER_STREAM) == CHARACTER_STREAM);
    }

    public boolean isUnicodeStream() {
        return ((dataType_ & UNICODE_STREAM) == UNICODE_STREAM);
    }

    public InputStream getUnicodeStream() {
        return unicodeStream_;
    }

    public String getString() {
        return string_;
    }

    public byte[] getUtf8String() {
        return utf8String_;
    }

    // Return the length of the equivalent UTF-8 string
    // precondition: string_ is not null and dataType_ includes STRING
    public int getUTF8Length() {
        if (utf8String_ != null) {
            return utf8String_.length;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6231
        utf8String_ = string_.getBytes(Cursor.UTF_8);
        return utf8String_.length;
    }

    /**
     * Reinitialize the value of this CLOB.
     *
     * This is legacy code, only used when talking to servers that don't
     * support locators.
     *
     * @param newString the new value
     */
    // The StringBufferInputStream class is deprecated, but we don't care too
    // much since this code is only for talking to very old servers. Suppress
    // the deprecation warnings for now.
    @SuppressWarnings("deprecation")
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    void reInitForNonLocator(String newString) {
        string_ = newString;
        asciiStream_ = new java.io.StringBufferInputStream(string_);
        unicodeStream_ = new java.io.StringBufferInputStream(string_);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        characterStream_ = new StringReader(string_);
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        setSqlLength(string_.length());
    }


    /**
     * Materialize the stream used for input to the database.
     *
     * @throws SqlException on error
     */
    protected void materializeStream()
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
        throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3107
        unicodeStream_ = super.materializeStream(isAsciiStream() ?
                                                        asciiStream_ :
                                                        unicodeStream_,
                                                 "java.sql.Clob");
        dataType_ = UNICODE_STREAM;
    }

    /*---------------------------------------------------------------------
//IC see: https://issues.apache.org/jira/browse/DERBY-2604
      Methods used in the locator implementation.
     ----------------------------------------------------------------------*/

    /**
     * Get the length in bytes of the <code>Clob</code> value represented by
     * this locator based <code>Clob</code> object.
     *
     * A stored procedure call will be made to get it from the server.
     * @throws org.apache.derby.client.am.SqlException
     * @return length of <code>Clob</code> in bytes
     */
    long getLocatorLength() throws SqlException
    {
        return agent_.connection_.locatorProcedureCall()
            .clobGetLength(locator_);
    }
}

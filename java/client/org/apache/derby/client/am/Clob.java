/*

   Derby - Class org.apache.derby.client.am.Clob

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

public class Clob extends Lob implements java.sql.Clob {
    //---------------------navigational members-----------------------------------

    //-----------------------------state------------------------------------------
    protected String string_ = null;

    // Only used for input purposes.  For output, each getXXXStream call
    // must generate an independent stream.
    protected java.io.InputStream asciiStream_ = null;
    protected java.io.InputStream unicodeStream_ = null;
    protected java.io.Reader characterStream_ = null;

    // used for input
    // Therefore, we always convert a String to UTF-8 before we flow it for input
    protected byte[] utf8String_;

    // the length of the clob returned by the LENGTH function.
    protected long lengthInBytes_ = 0;

    private PreparedStatement internalLengthStmt_ = null;

    protected String encoding_ = "UNICODE";

    //---------------------constructors/finalizer---------------------------------
    public Clob(Agent agent, String string) {
        this(agent);
        string_ = string;
        sqlLength_ = string_.length();
        lengthObtained_ = true;
        dataType_ |= STRING;
    }

    // CTOR for output, when a btc isn't available; the encoding is
    public Clob(Agent agent,
                byte[] unconvertedBytes,
                String charsetName,
                int dataOffset) throws SqlException {
        this(agent);
        try {
            // check for null encoding is needed because the net layer
            // will no longer throw an exception if the server didn't specify
            // a mixed or double byte ccsid (ccsid = 0).  this check for null in the
            // cursor is only required for types which can have mixed or double
            // byte ccsids.
            if (charsetName == null) {
                throw new SqlException(agent.logWriter_,
                        "Required character converter not available for data type.");
            }

            string_ = new String(unconvertedBytes,
                    dataOffset,
                    unconvertedBytes.length - dataOffset,
                    charsetName);
            sqlLength_ = string_.length();
            lengthObtained_ = true;
            dataType_ |= STRING;
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }
    }

    // CTOR for ascii/unicode stream input
    //"US-ASCII", "UTF-8", or "UnicodeBigUnmarked"
    public Clob(Agent agent,
                java.io.InputStream inputStream,
                String encoding,
                int length) throws SqlException {
        this(agent);

        sqlLength_ = length;
        lengthObtained_ = true;

        if (encoding.equals("US-ASCII")) {
            asciiStream_ = inputStream;
            dataType_ |= ASCII_STREAM;
        } else if (encoding.equals("UTF-8")) { // "UTF-8"
            unicodeStream_ = inputStream;
            dataType_ |= UNICODE_STREAM;
        } else if (encoding.equals("UnicodeBigUnmarked")) { // "UnicodeBigUnmarked"
            try {
                characterStream_ =
                        new java.io.InputStreamReader(inputStream, "UnicodeBigUnmarked");
            } catch (java.io.UnsupportedEncodingException e) {
                throw new SqlException(agent_.logWriter_, e.getMessage());
            }
            dataType_ |= CHARACTER_STREAM;
            sqlLength_ = length / 2;
        }
    }

    // CTOR for character stream input
    // THE ENCODING IS ASSUMED TO BE "UTF-16BE"
    public Clob(Agent agent, java.io.Reader reader, int length) {
        this(agent);
        sqlLength_ = length;
        lengthObtained_ = true;
        characterStream_ = reader;
        dataType_ |= CHARACTER_STREAM;
    }

    private Clob(Agent agent) {
        super(agent);
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
        if (internalLengthStmt_ != null) {
            internalLengthStmt_.closeX();
        }
    }

    // ---------------------------jdbc 2------------------------------------------
    // Create another method lengthX for internal calls
    public long length() throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "length");
            }

            if (lengthObtained_) {
                return sqlLength_;
            }

            lengthInBytes_ = super.sqlLength();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "length", sqlLength_);
            }
            return sqlLength_;
        }
    }

    public String getSubString(long pos, int length) throws SqlException {
        synchronized (agent_.connection_) {
            String retVal = null;

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getSubString", (int) pos, length);
            }

            // We can also do a check for pos > length()
            // Defer it till FP7 so that proper testing can be performed on this
            if ((pos <= 0) || (length < 0)) {
                throw new SqlException(agent_.logWriter_, "Invalid position " + pos + " or length " + length);
            }

            retVal = getSubStringX(pos, length);

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getSubString", retVal);
            }
            return retVal;
        }
    }

    private String getSubStringX(long pos, int length) throws SqlException {
        checkForClosedConnection();
        long actualLength = Math.min(this.length() - pos + 1, (long) length);
        return string_.substring((int) pos - 1, (int) (pos - 1 + actualLength));
    }

    public java.io.Reader getCharacterStream() throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream");
            }

            java.io.Reader retVal = getCharacterStreamX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getCharacterStream", retVal);
            }
            return retVal;
        }
    }

    private java.io.Reader getCharacterStreamX() throws SqlException {
        checkForClosedConnection();

        if (isCharacterStream())  // this Lob is used for input
        {
            return characterStream_;
        }

        return new java.io.StringReader(string_);
    }

    public java.io.InputStream getAsciiStream() throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getAsciiStream");
            }

            java.io.InputStream retVal = getAsciiStreamX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getAsciiStream", retVal);
            }
            return retVal;
        }
    }

    private java.io.InputStream getAsciiStreamX() throws SqlException {
        checkForClosedConnection();

        if (isAsciiStream())  // this Lob is used for input
        {
            return asciiStream_;
        }

        return new AsciiStream(string_, new java.io.StringReader(string_));
    }

    public long position(String searchstr, long start) throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this,
                        "position(String, long)",
                        searchstr,
                        start);
            }
            if (searchstr == null) {
                throw new SqlException(agent_.logWriter_, "Search string cannot be null.");
            }

            long pos = positionX(searchstr, start);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "position(String, long)", pos);
            }
            return pos;
        }
    }

    private long positionX(String searchstr, long start) throws SqlException {
        checkForClosedConnection();


        if (start <= 0) {
            throw new SqlException(agent_.logWriter_, "Clob.position(): start must be >= 1.");
        }

        int index = string_.indexOf(searchstr, (int) start - 1);
        if (index != -1) {
            index++; // api index starts at 1
        }
        return (long) index;
    }

    public long position(java.sql.Clob searchstr, long start) throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this,
                        "position(Clob, long)",
                        searchstr,
                        start);
            }
            if (searchstr == null) {
                throw new SqlException(agent_.logWriter_, "Search string cannot be null.");
            }
            long pos = positionX(searchstr, start);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "position(Clob, long)", pos);
            }
            return pos;
        }
    }

    private long positionX(java.sql.Clob searchstr, long start) throws SqlException {
        checkForClosedConnection();

        if (start <= 0) {
            throw new SqlException(agent_.logWriter_, "Clob.position(): start must be >= 1.");
        }

        // if the searchstr is longer than the source, no match
        int index;
        try {
            if (searchstr.length() > length()) {
                return -1;
            }

            index = string_.indexOf(searchstr.getSubString(1L, (int) searchstr.length()), (int) start - 1);
        } catch (java.sql.SQLException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }
        if (index != -1) {
            index++; // api index starts at 1
        }
        return (long) index;
    }

    //---------------------------- jdbc 3.0 -----------------------------------

    public int setString(long pos, String str) throws SqlException {
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

    public int setString(long pos, String str, int offset, int len) throws SqlException {
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

    public int setStringX(long pos, String str, int offset, int len) throws SqlException {
        if ((int) pos <= 0 || pos - 1 > sqlLength_) {
            throw new SqlException(agent_.logWriter_, "Invalid position " + pos
                    + " , offset " + offset + " or length " + len);
        }
        if ((offset < 0) || offset > str.length() || len < 0) {
            throw new SqlException(agent_.logWriter_, "Invalid position " + pos
                    + " , offset " + offset + " or length " + len);
        }
        if (len == 0) {
            return 0;
        }

        int length = 0;
        length = Math.min((str.length() - offset), len);
        String newString = string_.substring(0, (int) pos - 1);
        string_ = newString.concat(str.substring(offset, offset + length));
        asciiStream_ = new java.io.StringBufferInputStream(string_);
        unicodeStream_ = new java.io.StringBufferInputStream(string_);
        characterStream_ = new java.io.StringReader(string_);
        sqlLength_ = string_.length();
        return length;
    }

    public java.io.OutputStream setAsciiStream(long pos) throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setAsciiStream", (int) pos);
            }
            ClobOutputStream outStream = new ClobOutputStream(this, pos);

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "setAsciiStream", outStream);
            }
            return outStream;
        }
    }

    public java.io.Writer setCharacterStream(long pos) throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setCharacterStream", (int) pos);
            }
            ClobWriter writer = new ClobWriter(this, pos);

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "setCharacterStream", writer);
            }
            return writer;
        }
    }

    public void truncate(long len) throws SqlException {
        synchronized (agent_.connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, " truncate", (int) len);
            }
            if (len < 0 || len > this.length()) {
                throw new SqlException(agent_.logWriter_, "Invalid length " + len);
            }
            if (len == this.length()) {
                return;
            }
            String newstr = string_.substring(0, (int) len);
            string_ = newstr;
            asciiStream_ = new java.io.StringBufferInputStream(string_);
            unicodeStream_ = new java.io.StringBufferInputStream(string_);
            characterStream_ = new java.io.StringReader(string_);
            sqlLength_ = string_.length();
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

    public java.io.InputStream getUnicodeStream() {
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
    public int getUTF8Length() throws SqlException {
        if (utf8String_ != null) {
            return utf8String_.length;
        }

        try {
            utf8String_ = string_.getBytes("UTF-8");
            return utf8String_.length;
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }
    }

    // auxiliary method for position (Clob, long)
    protected Clob createClobWrapper(java.sql.Clob clob) throws SqlException {
        long length;
        java.io.Reader rdr;

        try {
            length = clob.length();
        } catch (java.sql.SQLException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }

        if (length > java.lang.Integer.MAX_VALUE) {
            throw new SqlException(agent_.logWriter_, "searchstr Clob object is too large");
        }

        try {
            rdr = clob.getCharacterStream();
        } catch (java.sql.SQLException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }

        return new Clob(this.agent_, rdr, (int) length);
    }

    public void convertFromAsciiToCharacterStream() throws SqlException {
        try {
            characterStream_ =
                    new java.io.InputStreamReader(asciiStream_, "US-ASCII");
            dataType_ = CHARACTER_STREAM;
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(agent_.logWriter_, e.getMessage());
        }
    }

    // this method is primarily for mixed clob length calculations.
    // it was introduced to prevent recursion in the actual char length calculation
    public long getByteLength() throws SqlException {
        if (lengthObtained_ == true) {
            return lengthInBytes_;
        }

        length();
        return lengthInBytes_;
    }
}

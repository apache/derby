/*

   Derby - Class org.apache.derby.client.am.Cursor

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
import org.apache.derby.shared.common.reference.SQLState;

import java.sql.SQLException;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

// When we calculate column offsets make sure we calculate the correct offsets for double byte charactr5er data
// length from server is number of chars, not bytes
// Direct byte-level converters are called directly by this class, cross converters are deferred to the CrossConverters class.

public abstract class Cursor {
    protected Agent agent_;

    //-----------------------------varchar representations------------------------

    public final static int STRING = 0;
    public final static int VARIABLE_STRING = 2;       // uses a 2-byte length indicator
    public final static int VARIABLE_SHORT_STRING = 1; // aka Pascal L; uses a 1-byte length indicator
    public final static int NULL_TERMINATED_STRING = 3;

    public final static int BYTES = 4;
    // unused protocol element: VARIABLE_BYTES = 5;
    // unused protocol element: VARIABLE_SHORT_BYTES = 6;
    public final static int NULL_TERMINATED_BYTES = 7;

    // Charsets
    static final Charset UTF_16BE = Charset.forName("UTF-16BE");
    static final Charset UTF_8 = Charset.forName("UTF-8");
    static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

    // unused protocol element: SBCS_CLOB = 8;
    // unused protocol element: MBCS_CLOB = 9;
    // unused protocol element: DBCS_CLOB = 10;
    //-----------------------------internal state---------------------------------

    //-------------Structures for holding and scrolling the data -----------------
    public byte[] dataBuffer_;
    public ByteArrayOutputStream dataBufferStream_;
    public int position_; // This is the read head
    public int lastValidBytePosition_;
    public boolean hasLobs_; // is there at least one LOB column?

    // Current row positioning
    protected int currentRowPosition_;
    private int nextRowPosition_;
    // Let's new up a 2-dimensional array based on fetch-size and reuse so that
    protected int[] columnDataPosition_;

    // This is the actual, computed lengths of varchar fields, not the max length from query descriptor or DA
    protected int[] columnDataComputedLength_;
    // populate this for

    // All the data is in the buffers, but user may not have necessarily stepped to the last row yet.
    // This flag indicates that the server has returned all the rows, and is positioned
    // after last, for both scrollable and forward-only cursors.
    // For singleton cursors, this memeber will be set to true as soon as next is called.
    private boolean allRowsReceivedFromServer_;

    // Total number of rows read so far.
    // This should never exceed this.statement.maxRows
    long rowsRead_;

    // Maximum column size limit in bytes.
    int maxFieldSize_ = 0;

    // Row positioning for all cached rows
    // For scrollable result sets, these lists hold the offsets into the cached rowset buffer for each row of data.
    protected ArrayList<int[]> columnDataPositionCache_ = new ArrayList<int[]>();
    protected ArrayList<int[]> columnDataLengthCache_ = new ArrayList<int[]>();
    protected ArrayList<boolean[]> columnDataIsNullCache_ = new ArrayList<boolean[]>();
    ArrayList<Boolean> isUpdateDeleteHoleCache_ = new ArrayList<Boolean>();
    boolean isUpdateDeleteHole_;

    // State to keep track of when a row has been updated,
    // cf. corresponding set and get accessors.  Only implemented for
    // scrollable updatable insensitive result sets for now.
    private boolean isRowUpdated_;

    final static Boolean ROW_IS_NULL = Boolean.TRUE;
    private final static Boolean ROW_IS_NOT_NULL = Boolean.FALSE;

    private Calendar recyclableCalendar_ = null;

    // For the net, this data comes from the query descriptor.

    public int[] jdbcTypes_;
    public int columns_;
    public boolean[] nullable_;
    public Charset[] charset_;
    public boolean[] isNull_;
    public int[] fdocaLength_; // this is the max length for

    //----------------------------------------------------------------------------

    public int[] ccsid_;
    private char[] charBuffer_;

    //---------------------constructors/finalizer---------------------------------

    public Cursor(Agent agent) {
        agent_ = agent;
        isRowUpdated_ = false;
        dataBufferStream_ = new ByteArrayOutputStream();
    }

    public void setNumberOfColumns(int numberOfColumns) {
        columnDataPosition_ = new int[numberOfColumns];
        columnDataComputedLength_ = new int[numberOfColumns];

        columns_ = numberOfColumns;
        nullable_ = new boolean[numberOfColumns];
        charset_ = new Charset[numberOfColumns];

        ccsid_ = new int[numberOfColumns];

        isNull_ = new boolean[numberOfColumns];
        jdbcTypes_ = new int[numberOfColumns];
    }

    /**
     * Makes the next row the current row. Returns true if the current
     * row position is a valid row position.
     *
     * @param allowServerFetch if false, don't fetch more data from
     * the server even if more data is needed
     * @return {@code true} if current row position is valid
     * @exception SqlException if an error occurs
     */
    protected boolean stepNext(boolean allowServerFetch) throws SqlException {
        // reset lob data
        // clears out Cursor.lobs_ calculated for the current row when cursor is moved.
        clearLobData_();

        // mark the start of a new row.
        makeNextRowPositionCurrent();
        
        // Moving out of the hole, set isUpdateDeleteHole to false
        isUpdateDeleteHole_ = false;

        isRowUpdated_ = false;

        // Drive the CNTQRY outside of calculateColumnOffsetsForRow() if the dataBuffer_
        // contains no data since it has no abilities to handle replies other than
        // the QRYDTA, i.e. ENDQRYRM when the result set contains no more rows.
        while (!dataBufferHasUnprocessedData()) {
            if (allRowsReceivedFromServer_) {
                return false;
            }
            getMoreData_();
        }

        // The parameter passed in here is used as an index into the cached rowset for
        // scrollable cursors, for the arrays to be reused.  It is not used for forward-only
        // cursors, so just pass in 0.
        boolean rowPositionIsValid =
            calculateColumnOffsetsForRow_(0, allowServerFetch);
        markNextRowPosition();
        return rowPositionIsValid;
    }

    /**
     * Makes the next row the current row. Returns true if the current
     * row position is a valid row position.
     *
     * @return {@code true} if current row position is valid
     * @exception SqlException if an error occurs
     */
    public boolean next() throws SqlException {
        return stepNext(true);
    }

    //--------------------------reseting cursor state-----------------------------

    /**
     * Set the value of value of allRowsReceivedFromServer_.
     *
     * @param b a {@code boolean} value indicating whether all
     * rows are received from the server
     */
    public void setAllRowsReceivedFromServer(boolean b) {
        allRowsReceivedFromServer_ = b;
    }

    /**
     * Return {@code true} if all rows are received from the
     * server.
     *
     * @return {@code true} if all rows are received from the
     * server.
     */
    public final boolean allRowsReceivedFromServer() {
        return allRowsReceivedFromServer_;
    }

    final boolean currentRowPositionIsEqualToNextRowPosition() {
        return (currentRowPosition_ == nextRowPosition_);
    }

    // reset the beginning and ending position in the data buffer to 0
    // reset the currentRowPosition and nextRowPosition to 0
    // reset lastRowReached and sqlcode100Received to false
    // clear the column data offsets cache
    public final void resetDataBuffer() {
        position_ = 0;
        lastValidBytePosition_ = 0;
        currentRowPosition_ = 0;
        nextRowPosition_ = 0;
        setAllRowsReceivedFromServer(false);
        dataBufferStream_.reset();
    }

    final boolean dataBufferHasUnprocessedData() {
        return (lastValidBytePosition_ - position_) > 0;
    }

    /**
     * Calculate the column offsets for a row.
     *
     * @param row row index
     * @param allowServerFetch if true, allow fetching more data from
     * server
     * @return {@code true} if the current row position is a
     * valid row position.
     * @exception SqlException
     * @exception DisconnectException
     */
    protected abstract boolean
        calculateColumnOffsetsForRow_(int row, boolean allowServerFetch)
        throws SqlException, DisconnectException;

    protected abstract void clearLobData_();

    protected abstract void getMoreData_() throws SqlException;

    public final void setIsUpdataDeleteHole(int row, boolean isRowNull) {
        isUpdateDeleteHole_ = isRowNull;
        Boolean nullIndicator = (isUpdateDeleteHole_ == true) ? ROW_IS_NULL : ROW_IS_NOT_NULL;
        if (isUpdateDeleteHoleCache_.size() == row) {
            isUpdateDeleteHoleCache_.add(nullIndicator);
        } else {
            isUpdateDeleteHoleCache_.set(row, nullIndicator);
        }
    }

    /**
     * Keep track of updated status for this row.
     *
     * @param isRowUpdated true if row has been updated
     *
     * @see Cursor#getIsRowUpdated
     */
    public final void setIsRowUpdated(boolean isRowUpdated) {
        isRowUpdated_ = isRowUpdated;
    }

    /**
     * Get updated status for this row. 
     * Minion of ResultSet#rowUpdated.
     *
     * @see Cursor#setIsRowUpdated
     */
    public final boolean getIsRowUpdated() {
        return isRowUpdated_;
    }

    /**
     * Get deleted status for this row. 
     * Minion of ResultSet#rowDeleted.
     *
     * @see Cursor#setIsUpdataDeleteHole
     */
    public final boolean getIsUpdateDeleteHole() {
        return isUpdateDeleteHole_;
    }
    
    //---------------------------cursor positioning-------------------------------

    protected final void markNextRowPosition() {
        nextRowPosition_ = position_;
    }

    protected final void makeNextRowPositionCurrent() {
        currentRowPosition_ = nextRowPosition_;
    }

    // This tracks the total number of rows read into the client side buffer for
    // this result set, irregardless of scrolling.
    // Per jdbc semantics, this should never exceed statement.maxRows.
    // This event should be generated in the materialized cursor's implementation
    // of calculateColumnOffsetsForRow().
    public final void incrementRowsReadEvent() {
        rowsRead_++;
    }

    //------- the following getters are called on known column types -------------
    // Direct conversions only, cross conversions are handled by another set of getters.

    // Build a Java boolean from a 1-byte signed binary representation.
    private boolean get_BOOLEAN(int column) {
        if ( SignedBinary.getByte
             ( dataBuffer_, columnDataPosition_[column - 1] ) == 0 )
        { return false; }
        else { return true; }
    }

    // Build a Java short from a 2-byte signed binary representation.
    private final short get_SMALLINT(int column) {
        return SignedBinary.getShort(dataBuffer_,
                columnDataPosition_[column - 1]);
    }

    // Build a Java int from a 4-byte signed binary representation.
    protected final int get_INTEGER(int column) {
        return SignedBinary.getInt(dataBuffer_,
                columnDataPosition_[column - 1]);
    }

    // Build a Java long from an 8-byte signed binary representation.
    private final long get_BIGINT(int column) {
        return SignedBinary.getLong(dataBuffer_,
                columnDataPosition_[column - 1]);
    }

    // Build a Java float from a 4-byte floating point representation.
    private final float get_FLOAT(int column) {
        return FloatingPoint.getFloat(dataBuffer_,
                columnDataPosition_[column - 1]);
    }

    // Build a Java double from an 8-byte floating point representation.
    private final double get_DOUBLE(int column) {
        return FloatingPoint.getDouble(dataBuffer_,
                columnDataPosition_[column - 1]);
    }

    // Build a java.math.BigDecimal from a fixed point decimal byte representation.
    private final BigDecimal get_DECIMAL(int column) throws SqlException {
        return Decimal.getBigDecimal(dataBuffer_,
                columnDataPosition_[column - 1],
                getColumnPrecision(column - 1),
                getColumnScale(column - 1));
    }


    // Build a Java double from a fixed point decimal byte representation.
    private double getDoubleFromDECIMAL(int column) throws SqlException {
        try {
            return Decimal.getDouble(dataBuffer_,
                    columnDataPosition_[column - 1],
                    getColumnPrecision(column - 1),
                    getColumnScale(column - 1));
        } catch (IllegalArgumentException e) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE),
                e, "double");
        }
    }

    // Build a Java long from a fixed point decimal byte representation.
    private long getLongFromDECIMAL(int column, String targetType)
            throws SqlException {
        try {
            return Decimal.getLong(dataBuffer_,
                    columnDataPosition_[column - 1],
                    getColumnPrecision(column - 1),
                    getColumnScale(column - 1));
        } catch (ArithmeticException e) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId (SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE),
                e, targetType);
        } catch (IllegalArgumentException e) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId (SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE),
                e, targetType);
        }
    }

    // Build a Java String from a database VARCHAR or LONGVARCHAR field.
    //
    // Depending on the ccsid, length is the number of chars or number of bytes.
    // For 2-byte character ccsids, length is the number of characters,
    // for all other cases length is the number of bytes.
    // The length does not include the null terminator.
    private String getVARCHAR(int column) throws SqlException {
        if (ccsid_[column - 1] == 1200) {
            return getStringWithoutConvert(columnDataPosition_[column - 1] + 2,
                    columnDataComputedLength_[column - 1] - 2);
        }

        // check for null encoding is needed because the net layer
        // will no longer throw an exception if the server didn't specify
        // a mixed or double byte ccsid (ccsid = 0).  this check for null in the
        // cursor is only required for types which can have mixed or double
        // byte ccsids.
        if (charset_[column - 1] == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.CHARACTER_CONVERTER_NOT_AVAILABLE));
        }

        String tempString = new String(dataBuffer_,
                columnDataPosition_[column - 1] + 2,
                columnDataComputedLength_[column - 1] - 2,
                charset_[column - 1]);
        return (maxFieldSize_ == 0) ? tempString :
                tempString.substring(0, Math.min(maxFieldSize_,
                                                 tempString.length()));
    }

    // Build a Java String from a database CHAR field.
    private String getCHAR(int column) throws SqlException {
        if (ccsid_[column - 1] == 1200) {
            return getStringWithoutConvert(columnDataPosition_[column - 1], columnDataComputedLength_[column - 1]);
        }

        // check for null encoding is needed because the net layer
        // will no longer throw an exception if the server didn't specify
        // a mixed or double byte ccsid (ccsid = 0).  this check for null in the
        // cursor is only required for types which can have mixed or double
        // byte ccsids.
        if (charset_[column - 1] == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.CHARACTER_CONVERTER_NOT_AVAILABLE));
        }

        String tempString = new String(dataBuffer_,
                columnDataPosition_[column - 1],
                columnDataComputedLength_[column - 1],
                charset_[column - 1]);
        return (maxFieldSize_ == 0) ? tempString :
                tempString.substring(0, Math.min(maxFieldSize_,
                                                 tempString.length()));
    }

    // Build a JDBC Date object from the DERBY ISO DATE field.
    private Date getDATE(int column, Calendar cal) throws SqlException {
        return DateTime.dateBytesToDate(dataBuffer_,
            columnDataPosition_[column - 1],
            cal,
            charset_[column - 1]);
    }

    // Build a JDBC Time object from the DERBY ISO TIME field.
    private Time getTIME(int column, Calendar cal) throws SqlException {
        return DateTime.timeBytesToTime(dataBuffer_,
                columnDataPosition_[column - 1],
                cal,
                charset_[column - 1]);
    }

    // Build a JDBC Timestamp object from the DERBY ISO TIMESTAMP field.
    private final Timestamp getTIMESTAMP(int column, Calendar cal)
            throws SqlException {
        return DateTime.timestampBytesToTimestamp(
            dataBuffer_,
            columnDataPosition_[column - 1],
            cal,
            charset_[column - 1],
            agent_.connection_.serverSupportsTimestampNanoseconds());
    }

    // Build a JDBC Timestamp object from the DERBY ISO DATE field.
    private final Timestamp getTimestampFromDATE(
            int column, Calendar cal) throws SqlException {
        return DateTime.dateBytesToTimestamp(dataBuffer_,
                columnDataPosition_[column - 1],
                cal,
                charset_[column -1]);
    }

    // Build a JDBC Timestamp object from the DERBY ISO TIME field.
    private final Timestamp getTimestampFromTIME(
            int column, Calendar cal) throws SqlException {
        return DateTime.timeBytesToTimestamp(dataBuffer_,
                columnDataPosition_[column - 1],
                cal,
                charset_[column -1]);
    }

    // Build a JDBC Date object from the DERBY ISO TIMESTAMP field.
    private final Date getDateFromTIMESTAMP(int column, Calendar cal)
            throws SqlException {
        return DateTime.timestampBytesToDate(dataBuffer_,
                columnDataPosition_[column - 1],
                cal,
                charset_[column -1]);
    }

    // Build a JDBC Time object from the DERBY ISO TIMESTAMP field.
    private final Time getTimeFromTIMESTAMP(int column, Calendar cal)
            throws SqlException {
        return DateTime.timestampBytesToTime(dataBuffer_,
                columnDataPosition_[column - 1],
                cal,
                charset_[column -1]);
    }

    private String getStringFromDATE(int column) throws SqlException {
        return getDATE(column, getRecyclableCalendar()).toString();
    }

    // Build a string object from the DERBY byte TIME representation.
    private String getStringFromTIME(int column) throws SqlException {
        return getTIME(column, getRecyclableCalendar()).toString();
    }

    // Build a string object from the DERBY byte TIMESTAMP representation.
    private String getStringFromTIMESTAMP(int column) throws SqlException {
        return getTIMESTAMP(column, getRecyclableCalendar()).toString();
    }

    // Extract bytes from a database Types.BINARY field.
    // This is the DERBY type CHAR(n) FOR BIT DATA.
    private byte[] get_CHAR_FOR_BIT_DATA(int column) throws SqlException {
        // There is no limit to the size of a column if maxFieldSize is zero.
        // Otherwise, use the smaller of maxFieldSize and the actual column length.
        int columnLength = (maxFieldSize_ == 0) ? columnDataComputedLength_[column - 1] :
                Math.min(maxFieldSize_, columnDataComputedLength_[column - 1]);

        byte[] bytes = new byte[columnLength];
        System.arraycopy(dataBuffer_, columnDataPosition_[column - 1], bytes, 0, columnLength);
        return bytes;
    }

    // Extract bytes from a database Types.VARBINARY or LONGVARBINARY field.
    // This includes the DERBY types:
    //   VARCHAR(n) FOR BIT DATA
    //   LONG VARCHAR(n) FOR BIT DATA
    private byte[] get_VARCHAR_FOR_BIT_DATA(int column) throws SqlException {
        byte[] bytes;
        int columnLength =
            (maxFieldSize_ == 0) ? columnDataComputedLength_[column - 1] - 2 :
            Math.min(maxFieldSize_, columnDataComputedLength_[column - 1] - 2);
        bytes = new byte[columnLength];
        System.arraycopy(dataBuffer_, columnDataPosition_[column - 1] + 2, bytes, 0, bytes.length);
        return bytes;
    }

    // Deserialize a UDT from a database Types.JAVA_OBJECT field.
    // This is used for user defined types.
    private Object get_UDT(int column) throws SqlException {
        byte[] bytes;
        int columnLength =
            (maxFieldSize_ == 0) ? columnDataComputedLength_[column - 1] - 2 :
            Math.min(maxFieldSize_, columnDataComputedLength_[column - 1] - 2);
        bytes = new byte[columnLength];
        System.arraycopy(dataBuffer_, columnDataPosition_[column - 1] + 2, bytes, 0, bytes.length);

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream( bytes );
            ObjectInputStream ois = new ObjectInputStream( bais );

            return ois.readObject();
        }
        catch (Exception e)
        {
            throw new SqlException
                (
                 agent_.logWriter_, 
                 new ClientMessageId (SQLState.NET_MARSHALLING_UDT_ERROR),
                 e,
                 e.getMessage()
                 );
        }
    }

    /**
     * Instantiate an instance of Calendar that can be re-used for getting
     * Time, Timestamp, and Date values from this cursor.  Assumption is
     * that all users of the returned Calendar object will clear it as
     * appropriate before using it.
     */
    private Calendar getRecyclableCalendar()
    {
        if (recyclableCalendar_ == null)
            recyclableCalendar_ = new GregorianCalendar();

        return recyclableCalendar_;
    }

    /**
     * Returns a reference to the locator procedures.
     * <p>
     * These procedures are used to operate on large objects referenced on the
     * server by locators.
     *
     * @return The locator procedures object.
     */
    CallableLocatorProcedures getLocatorProcedures() {
        return agent_.connection_.locatorProcedureCall();
    }

    /**
     * Returns the locator for the specified LOB column, or {@link
     * Lob#INVALID_LOCATOR} if the LOB was not sent as a locator. The server
     * may send the LOB value instead of a locator if it is running an old
     * version which doesn't support locators, or if the database it accesses
     * is soft upgraded from a version that doesn't have the necessary
     * stored procedures for locator support.
     * <p>
     * Note that this method cannot be invoked on a LOB column that is NULL.
     *
     * @param column 1-based column index
     * @return A positive integer locator if valid, {@link Lob#INVALID_LOCATOR}
     *      otherwise.
     */
    protected abstract int locator(int column);

    /**
     * Returns a {@code Blob} object.
     *
     * @param column 1-based column index
     * @param agent associated agent
     * @param toBePublished whether the Blob will be published to the user
     * @return A {@linkplain java.sql.Blob Blob} object.
     * @throws SqlException if getting the {@code Blob} fails
     */
    public abstract ClientBlob getBlobColumn_(int column, Agent agent,
                                        boolean toBePublished)
            throws SqlException;

    /**
     * Returns a {@code Clob} object.
     *
     * @param column 1-based column index
     * @param agent associated agent
     * @param toBePublished whether the Clob will be published to the user
     * @return A {@code java.sql.Clob} object.
     * @throws SqlException if getting the {@code Clob} fails
     */
    public abstract ClientClob getClobColumn_(int column, Agent agent,
                                        boolean toBePublished)
            throws SqlException;

    //------- the following getters perform any necessary cross-conversion _------

    final boolean getBoolean(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return get_BOOLEAN(column);
        case Types.SMALLINT:
            return agent_.crossConverters_.getBooleanFromShort(get_SMALLINT(column));
        case Types.INTEGER:
            return agent_.crossConverters_.getBooleanFromInt(get_INTEGER(column));
        case Types.BIGINT:
            return agent_.crossConverters_.getBooleanFromLong(get_BIGINT(column));
        case Types.REAL:
            return agent_.crossConverters_.getBooleanFromFloat(get_FLOAT(column));
        case Types.DOUBLE:
            return agent_.crossConverters_.getBooleanFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return agent_.crossConverters_.getBooleanFromLong(
                getLongFromDECIMAL(column, "boolean"));
        case Types.CHAR:
            return agent_.crossConverters_.getBooleanFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getBooleanFromString(getVARCHAR(column));
        default:
            throw coercionError( "boolean", column );
        }
    }

    final byte getByte(int column) throws SqlException {
        // This needs to be changed to use jdbcTypes[]
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getByteFromBoolean(get_BOOLEAN(column));
        case Types.SMALLINT:
            return agent_.crossConverters_.getByteFromShort(get_SMALLINT(column));
        case Types.INTEGER:
            return agent_.crossConverters_.getByteFromInt(get_INTEGER(column));
        case Types.BIGINT:
            return agent_.crossConverters_.getByteFromLong(get_BIGINT(column));
        case Types.REAL:
            return agent_.crossConverters_.getByteFromFloat(get_FLOAT(column));
        case Types.DOUBLE:
            return agent_.crossConverters_.getByteFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return agent_.crossConverters_.getByteFromLong(
                getLongFromDECIMAL(column, "byte"));
        case Types.CHAR:
            return agent_.crossConverters_.getByteFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getByteFromString(getVARCHAR(column));
        default:
            throw coercionError( "byte", column );
        }
    }

    final short getShort(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getShortFromBoolean(get_BOOLEAN(column));
        case Types.SMALLINT:
            return get_SMALLINT(column);
        case Types.INTEGER:
            return agent_.crossConverters_.getShortFromInt(get_INTEGER(column));
        case Types.BIGINT:
            return agent_.crossConverters_.getShortFromLong(get_BIGINT(column));
        case Types.REAL:
            return agent_.crossConverters_.getShortFromFloat(get_FLOAT(column));
        case Types.DOUBLE:
            return agent_.crossConverters_.getShortFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return agent_.crossConverters_.getShortFromLong(
                getLongFromDECIMAL(column, "short"));
        case Types.CHAR:
            return agent_.crossConverters_.getShortFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getShortFromString(getVARCHAR(column));
        default:
            throw coercionError( "short", column );
        }
    }

    final int getInt(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getIntFromBoolean(get_BOOLEAN(column));
        case Types.SMALLINT:
            return (int) get_SMALLINT(column);
        case Types.INTEGER:
            return get_INTEGER(column);
        case Types.BIGINT:
            return agent_.crossConverters_.getIntFromLong(get_BIGINT(column));
        case Types.REAL:
            return agent_.crossConverters_.getIntFromFloat(get_FLOAT(column));
        case Types.DOUBLE:
            return agent_.crossConverters_.getIntFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return agent_.crossConverters_.getIntFromLong(
                getLongFromDECIMAL(column, "int"));
        case Types.CHAR:
            return agent_.crossConverters_.getIntFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getIntFromString(getVARCHAR(column));
        default:
            throw coercionError(  "int", column );
        }
    }

    final long getLong(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getLongFromBoolean(get_BOOLEAN(column));
        case Types.SMALLINT:
            return (long) get_SMALLINT(column);
        case Types.INTEGER:
            return (long) get_INTEGER(column);
        case Types.BIGINT:
            return get_BIGINT(column);
        case Types.REAL:
            return agent_.crossConverters_.getLongFromFloat(get_FLOAT(column));
        case Types.DOUBLE:
            return agent_.crossConverters_.getLongFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return getLongFromDECIMAL(column, "long");
        case Types.CHAR:
            return agent_.crossConverters_.getLongFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getLongFromString(getVARCHAR(column));
        default:
            throw coercionError( "long", column );
        }
    }

    final float getFloat(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getFloatFromBoolean(get_BOOLEAN(column));
        case Types.REAL:
            return get_FLOAT(column);
        case Types.DOUBLE:
            return agent_.crossConverters_.getFloatFromDouble(get_DOUBLE(column));
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return agent_.crossConverters_.getFloatFromDouble(getDoubleFromDECIMAL(column));
        case Types.SMALLINT:
            return (float) get_SMALLINT(column);
        case Types.INTEGER:
            return (float) get_INTEGER(column);
        case Types.BIGINT:
            return (float) get_BIGINT(column);
        case Types.CHAR:
            return agent_.crossConverters_.getFloatFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getFloatFromString(getVARCHAR(column));
        default:
            throw coercionError( "float", column );
        }
    }

    final double getDouble(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return agent_.crossConverters_.getDoubleFromBoolean(get_BOOLEAN(column));
        case Types.REAL:
            double d = (double) get_FLOAT(column);
            return d;
            //return (double) get_FLOAT (column);
        case Types.DOUBLE:
            return get_DOUBLE(column);
        case Types.DECIMAL:
            // For performance we don't materialize the BigDecimal, but convert directly from decimal bytes to a long.
            return getDoubleFromDECIMAL(column);
        case Types.SMALLINT:
            return (double) get_SMALLINT(column);
        case Types.INTEGER:
            return (double) get_INTEGER(column);
        case Types.BIGINT:
            return (double) get_BIGINT(column);
        case Types.CHAR:
            return agent_.crossConverters_.getDoubleFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getDoubleFromString(getVARCHAR(column));
        default:
            throw coercionError( "double", column );
        }
    }

    final BigDecimal getBigDecimal(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return BigDecimal.valueOf(getLong(column));
        case Types.DECIMAL:
            return get_DECIMAL(column);
        case Types.REAL:
            // Can't use the following commented out line because it changes precision of the result.
            //return new java.math.BigDecimal (get_FLOAT (column));
            float f = get_FLOAT(column);
            return new BigDecimal(String.valueOf(f));
        case Types.DOUBLE:
            return BigDecimal.valueOf(get_DOUBLE(column));
        case Types.SMALLINT:
            return BigDecimal.valueOf(get_SMALLINT(column));
        case Types.INTEGER:
            return BigDecimal.valueOf(get_INTEGER(column));
        case Types.BIGINT:
            return BigDecimal.valueOf(get_BIGINT(column));
        case Types.CHAR:
            return agent_.crossConverters_.getBigDecimalFromString(getCHAR(column));
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.getBigDecimalFromString(getVARCHAR(column));
        default:
            throw coercionError( "java.math.BigDecimal", column );
        }
    }

    final Date getDate(int column, Calendar cal) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.DATE:
            return getDATE(column, cal);
        case Types.TIMESTAMP:
            return getDateFromTIMESTAMP(column, cal);
        case Types.CHAR:
            return agent_.crossConverters_.
                    getDateFromString(getCHAR(column), cal);
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.
                    getDateFromString(getVARCHAR(column), cal);
        default:
            throw coercionError( "java.sql.Date", column );
        }
    }

    final Time getTime(int column, Calendar cal) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.TIME:
            return getTIME(column, cal);
        case Types.TIMESTAMP:
            return getTimeFromTIMESTAMP(column, cal);
        case Types.CHAR:
            return agent_.crossConverters_.
                    getTimeFromString(getCHAR(column), cal);
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.
                    getTimeFromString(getVARCHAR(column), cal);
        default:
            throw coercionError( "java.sql.Time", column );
        }
    }

    final Timestamp getTimestamp(int column, Calendar cal)
            throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.TIMESTAMP:
            return getTIMESTAMP(column, cal);
        case Types.DATE:
            return getTimestampFromDATE(column, cal);
        case Types.TIME:
            return getTimestampFromTIME(column, cal);
        case Types.CHAR:
            return agent_.crossConverters_.
                    getTimestampFromString(getCHAR(column), cal);
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return agent_.crossConverters_.
                    getTimestampFromString(getVARCHAR(column), cal);
        default:
            throw coercionError( "java.sql.Timestamp", column );
        }
    }

    final String getString(int column) throws SqlException {
        try {
            String tempString;
            switch (jdbcTypes_[column - 1]) {
            case Types.BOOLEAN:
                if ( get_BOOLEAN( column ) ) { return Boolean.TRUE.toString(); }
                else { return Boolean.FALSE.toString(); }
            case Types.CHAR:
                return getCHAR(column);
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return getVARCHAR(column);

            case Types.SMALLINT:
                return String.valueOf(get_SMALLINT(column));
            case Types.INTEGER:
                return String.valueOf(get_INTEGER(column));
            case Types.BIGINT:
                return String.valueOf(get_BIGINT(column));
            case Types.REAL:
                return String.valueOf(get_FLOAT(column));
            case Types.DOUBLE:
                return String.valueOf(get_DOUBLE(column));
            case Types.DECIMAL:
                // We could get better performance here if we didn't materialize the BigDecimal,
                // but converted directly from decimal bytes to a string.
                return String.valueOf(get_DECIMAL(column));
            case Types.DATE:
                return getStringFromDATE(column);
            case Types.TIME:
                return getStringFromTIME(column);
            case Types.TIMESTAMP:
                return getStringFromTIMESTAMP(column);
            case ClientTypes.BINARY:
                tempString =
                        agent_.crossConverters_.getStringFromBytes(get_CHAR_FOR_BIT_DATA(column));
                return (maxFieldSize_ == 0) ? tempString :
                        tempString.substring(0, Math.min(maxFieldSize_,
                                                         tempString.length()));
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                tempString =
                        agent_.crossConverters_.getStringFromBytes(get_VARCHAR_FOR_BIT_DATA(column));
                return (maxFieldSize_ == 0) ? tempString :
                        tempString.substring(0, Math.min(maxFieldSize_,
                                                         tempString.length()));
            case Types.JAVA_OBJECT:
                Object obj = get_UDT( column );
                if ( obj == null ) { return null; }
                else { return obj.toString(); }
            case Types.BLOB:
                ClientBlob b = getBlobColumn_(column, agent_, false);
                tempString = agent_.crossConverters_.
                        getStringFromBytes(b.getBytes(1, (int) b.length()));
                return tempString;
            case Types.CLOB:
                ClientClob c = getClobColumn_(column, agent_, false);
                tempString = c.getSubString(1, (int) c.length());
                return tempString;
            default:
                throw coercionError( "String", column );
            }
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
    }

    final byte[] getBytes(int column) throws SqlException {
        try {
            switch (jdbcTypes_[column - 1]) {
            case Types.BINARY:
                return get_CHAR_FOR_BIT_DATA(column);
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return get_VARCHAR_FOR_BIT_DATA(column);
            case Types.BLOB:
                ClientBlob b = getBlobColumn_(column, agent_, false);
                byte[] bytes = b.getBytes(1, (int) b.length());
                return bytes;
            default:
                throw coercionError( "byte[]", column );
            }
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
    }

    final InputStream getBinaryStream(int column) throws SqlException
    {
        switch (jdbcTypes_[column - 1]) {
            case Types.BINARY:
                return new ByteArrayInputStream(get_CHAR_FOR_BIT_DATA(column));
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return
                    new ByteArrayInputStream(get_VARCHAR_FOR_BIT_DATA(column));
            case Types.BLOB:
                ClientBlob b = getBlobColumn_(column, agent_, false);
                if (b.isLocator()) {
                    BlobLocatorInputStream is 
                            = new BlobLocatorInputStream(agent_.connection_, b);
                    return new BufferedInputStream(is);
                } else {
                    return b.getBinaryStreamX();
                }
            default:
                throw coercionError( "java.io.InputStream", column );
        }
    }

    final InputStream getAsciiStream(int column) throws SqlException
    {
        switch (jdbcTypes_[column - 1]) {
            case Types.CLOB:
                ClientClob c = getClobColumn_(column, agent_, false);
                if (c.isLocator()) {
                    ClobLocatorInputStream is 
                            = new ClobLocatorInputStream(agent_.connection_, c);
                    return new BufferedInputStream(is);
                } else {
                    return c.getAsciiStreamX();
                }
            case Types.CHAR:
                return new ByteArrayInputStream(
                        getCHAR(column).getBytes(ISO_8859_1));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return new ByteArrayInputStream(
                        getVARCHAR(column).getBytes(ISO_8859_1));
            case Types.BINARY:
                return new ByteArrayInputStream(get_CHAR_FOR_BIT_DATA(column));
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return
                    new ByteArrayInputStream(get_VARCHAR_FOR_BIT_DATA(column));
            case Types.BLOB:
                return getBinaryStream(column);
            default:
                throw coercionError( "java.io.InputStream", column );
        }
    }
 
    final Reader getCharacterStream(int column)
            throws SqlException 
    {
        switch (jdbcTypes_[column - 1]) {
            case Types.CLOB:
                ClientClob c = getClobColumn_(column, agent_, false);
                if (c.isLocator()) {
                    ClobLocatorReader reader
                            = new ClobLocatorReader(agent_.connection_, c);
                    return new BufferedReader(reader);
                } else {
                    return c.getCharacterStreamX();
                }
            case Types.CHAR:
                return new StringReader(getCHAR(column));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return new StringReader(getVARCHAR(column));
            case Types.BINARY:
                return new InputStreamReader(
                    new ByteArrayInputStream(
                        get_CHAR_FOR_BIT_DATA(column)), UTF_16BE);
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return new InputStreamReader(
                    new ByteArrayInputStream(
                        get_VARCHAR_FOR_BIT_DATA(column)), UTF_16BE);
            case Types.BLOB:
                return new InputStreamReader(getBinaryStream(column), UTF_16BE);
            default:
                throw coercionError( "java.io.Reader", column );
            }
    }

    final Blob getBlob(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case ClientTypes.BLOB:
            return getBlobColumn_(column, agent_, true);
        default:
            throw coercionError( "java.sql.Blob", column );
        }
    }

    final Clob getClob(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case ClientTypes.CLOB:
            return getClobColumn_(column, agent_, true);
        default:
            throw coercionError( "java.sql.Clob", column );
        }
    }

    final Array getArray(int column) throws SqlException {
        throw new SqlException(agent_.logWriter_, 
            new ClientMessageId (SQLState.NOT_IMPLEMENTED),
            "getArray(int)");
    }

    final Ref getRef(int column) throws SqlException {
        throw new SqlException(agent_.logWriter_, 
            new ClientMessageId (SQLState.NOT_IMPLEMENTED), "getRef(int)");
    }

    final Object getObject(int column) throws SqlException {
        switch (jdbcTypes_[column - 1]) {
        case Types.BOOLEAN:
            return get_BOOLEAN(column);
        case Types.SMALLINT:
            // See Table 4 in JDBC 1 spec (pg. 932 in jdbc book)
            return Integer.valueOf(get_SMALLINT(column));
        case Types.INTEGER:
            return get_INTEGER(column);
        case Types.BIGINT:
            return get_BIGINT(column);
        case Types.REAL:
            return get_FLOAT(column);
        case Types.DOUBLE:
            return get_DOUBLE(column);
        case Types.DECIMAL:
            return get_DECIMAL(column);
        case Types.DATE:
            return getDATE(column, getRecyclableCalendar());
        case Types.TIME:
            return getTIME(column, getRecyclableCalendar());
        case Types.TIMESTAMP:
            return getTIMESTAMP(column, getRecyclableCalendar());
        case Types.CHAR:
            return getCHAR(column);
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return getVARCHAR(column);
        case ClientTypes.BINARY:
            return get_CHAR_FOR_BIT_DATA(column);
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return get_VARCHAR_FOR_BIT_DATA(column);
        case Types.JAVA_OBJECT:
            return get_UDT( column );
        case Types.BLOB:
            return getBlobColumn_(column, agent_, true);
        case Types.CLOB:
            return getClobColumn_(column, agent_, true);
        default:
            throw coercionError( "Object", column );
        }
    }

    public final void allocateCharBuffer() {
        // compute the maximum char length
        int maxCharLength = 0;
        for (int i = 0; i < columns_; i++) {
            switch (jdbcTypes_[i]) {
            case ClientTypes.CHAR:
            case ClientTypes.VARCHAR:
            case ClientTypes.LONGVARCHAR:
                if (fdocaLength_[i] > maxCharLength) {
                    maxCharLength = fdocaLength_[i];
                }
            }
        }

        // allocate char buffer to accomodate largest result column
        charBuffer_ = new char[maxCharLength];
    }

    private String getStringWithoutConvert(int position, int actualLength) {
        int start = position;
        int end = position + actualLength;

        int charCount = 0;
        while (start < end) {
            charBuffer_[charCount++] = (char) (((dataBuffer_[start] & 0xff) << 8) | (dataBuffer_[start + 1] & 0xff));
            start += 2;
        }

        return new String(charBuffer_, 0, charCount);
    }

    private ColumnTypeConversionException coercionError
        ( String targetType, int sourceColumn )
    {
        return new ColumnTypeConversionException
            (agent_.logWriter_,
             targetType,
             ClientTypes.getTypeString(jdbcTypes_[sourceColumn -1]));
    }

    public void nullDataForGC() {
        dataBuffer_ = null;
        dataBufferStream_ = null;
        columnDataPosition_ = null;
        columnDataComputedLength_ = null;
        columnDataPositionCache_ = null;
        columnDataLengthCache_ = null;
        columnDataIsNullCache_ = null;
        jdbcTypes_ = null;
        nullable_ = null;
        charset_ = null;
        this.ccsid_ = null;
        isUpdateDeleteHoleCache_ = null;
        isNull_ = null;
        fdocaLength_ = null;
        charBuffer_ = null;
    }

    private int getColumnPrecision(int column) {
        return ((fdocaLength_[column] >> 8) & 0xff);
    }

    private int getColumnScale(int column) {
        return (fdocaLength_[column] & 0xff);
    }
}

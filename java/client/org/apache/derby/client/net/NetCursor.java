/*

   Derby - Class org.apache.derby.client.net.NetCursor

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

package org.apache.derby.client.net;

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.Blob;
import org.apache.derby.client.am.Clob;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SignedBinary;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlWarning;
import org.apache.derby.client.am.Types;
import org.apache.derby.client.am.SqlCode;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

public class NetCursor extends org.apache.derby.client.am.Cursor {

    NetResultSet netResultSet_;
    NetAgent netAgent_;

    Typdef qrydscTypdef_;

    int targetSqlamForTypdef_;


    // override column meta data
    int numMddOverrides_;
    int maximumRowSize_;
    boolean blocking_;  // if true, multiple rows may be "blocked" in a single reply

    // Raw fdoca column meta data.
    int[] typeToUseForComputingDataLength_;
    boolean[] isGraphic_;

    // key = column position, value = index into extdtaData_
    java.util.HashMap extdtaPositions_;
    java.util.ArrayList extdtaData_; // queue to hold EXTDTA data that hasn't been correlated to its column #


    boolean rtnextrow_ = true;

    /** Flag indicating whether the result set on the server is
     * implicitly closed when end-of-data is received. */
    private boolean qryclsimpEnabled_;

    //-----------------------------constants--------------------------------------

    //---------------------constructors/finalizer---------------------------------

    NetCursor(NetAgent netAgent) {
        super(netAgent);
        netAgent_ = netAgent;
        numMddOverrides_ = 0;
        maximumRowSize_ = 0;
        extdtaPositions_ = new java.util.HashMap();
        extdtaData_ = new java.util.ArrayList();
    }

    NetCursor(NetAgent netAgent,
              int qryprctyp)  //protocolType, CodePoint.FIXROWPRC | CodePoint.LMTBLKPRC
    {
        this(netAgent);
        if (qryprctyp == CodePoint.FIXROWPRC) {
            blocking_ = false;
        } else if (qryprctyp == CodePoint.LMTBLKPRC) {
            blocking_ = true;
        }
    }
    //-----------------------------parsing the data buffer------------------------

    /**
     * Calculate the column offsets for a row.
     * <p>
     * Pseudo-code:
     * <ol>
     * <li>parse thru the current row in dataBuffer computing column
     * offsets</li>
     * <li>if (we hit the super.lastValidBytePosition, ie. encounter
     * partial row)
     *   <ol>
     *     <li>shift partial row bytes to beginning of dataBuffer
     *     (this.shiftPartialRowToBeginning())</li>
     *     <li>reset current row position (also done by
     *     this.shiftPartialRowToBeginning())</li>
     *     <li>send and recv continue-query into commBuffer
     *     (rs.flowContinueQuery())</li>
     *     <li>parse commBuffer up to QRYDTA
     *     (rs.flowContinueQuery())</li>
     *     <li>copy query data from reply's commBuffer to our
     *     dataBuffer (this.copyQrydta())</li>
     *   </ol>
     * </ol>
     *
     * @param rowIndex row index
     * @param allowServerFetch if true, allow fetching more data from
     * server
     * @return <code>true</code> if the current row position is a
     * valid row position.
     * @exception SqlException
     * @exception DisconnectException
     */
    protected
        boolean calculateColumnOffsetsForRow_(int rowIndex,
                                              boolean allowServerFetch)
        throws SqlException, DisconnectException
    {
        int daNullIndicator = CodePoint.NULLDATA;
        int colNullIndicator = CodePoint.NULLDATA;
        int length;

        if (hasLobs_) {
            extdtaPositions_.clear();  // reset positions for this row
        }

        int[] columnDataPosition = null;
        int[] columnDataComputedLength = null;
        boolean[] columnDataIsNull = null;
        boolean receivedDeleteHoleWarning = false;
        boolean receivedRowUpdatedWarning = false;

        if ((position_ == lastValidBytePosition_) &&
                (netResultSet_ != null) && (netResultSet_.scrollable_)) {
            return false;
        }

        NetSqlca[] netSqlca = this.parseSQLCARD(qrydscTypdef_);

        if (netSqlca != null) {
            for (int i=0;i<netSqlca.length; i++) {
                int sqlcode = netSqlca[i].getSqlCode();
                if (sqlcode < 0) {
                    throw new SqlException(netAgent_.logWriter_, 
                            netSqlca[i]);
                } else {
                    if (sqlcode == SqlCode.END_OF_DATA.getCode()) {
                        setAllRowsReceivedFromServer(true);
                        if (netResultSet_ != null && 
                                netSqlca[i].containsSqlcax()) {
                            netResultSet_.setRowCountEvent(
                                    netSqlca[i].getRowCount(
                                        qrydscTypdef_));
                        }
                    } else if (netResultSet_ != null && sqlcode > 0) {
                        String sqlState = netSqlca[i].getSqlState();
                        if (!sqlState.equals(SQLState.ROW_DELETED) && 
                                !sqlState.equals(SQLState.ROW_UPDATED)) {
                            netResultSet_.accumulateWarning(
                                    new SqlWarning(agent_.logWriter_, 
                                        netSqlca[i]));
                        } else {
                            receivedDeleteHoleWarning 
                                    |= sqlState.equals(SQLState.ROW_DELETED);
                            receivedRowUpdatedWarning 
                                    |= sqlState.equals(SQLState.ROW_UPDATED);
                        }
                    }
                }
            }
        }

        setIsUpdataDeleteHole(rowIndex, receivedDeleteHoleWarning);
        setIsRowUpdated(receivedRowUpdatedWarning);
        
        // If we don't have at least one byte in the buffer for the DA null indicator,
        // then we need to send a CNTQRY request to fetch the next block of data.
        // Read the DA null indicator.
        daNullIndicator = readFdocaOneByte();

        // In the case for held cursors, the +100 comes back as part of the QRYDTA, and as
        // we are parsing through the row that contains the SQLCA with +100, we mark the
        // nextRowPosition_ which is the lastValidBytePosition_, but we don't mark the
        // currentRowPosition_ until the next time next() is called causing the check
        // cursor_.currentRowPositionIsEqualToNextRowPosition () to fail in getRow() and thus
        // not returning 0 when it should. So we need to mark the current row position immediately
        // in order for getRow() to be able to pick it up.

        // markNextRowPosition() is called again once this method returns, but it is ok
        // since it's only resetting nextRowPosition_ to position_ and position_ will
        // not change again from this point.

        if (allRowsReceivedFromServer() &&
            (position_ == lastValidBytePosition_)) {
            markNextRowPosition();
            makeNextRowPositionCurrent();
            return false;
        }

        // If data flows....
        if (daNullIndicator == 0x0) {

	    if (SanityManager.DEBUG && receivedDeleteHoleWarning) {
		SanityManager.THROWASSERT("Delete hole warning received: nulldata expected");
	    }
            incrementRowsReadEvent();

            // netResultSet_ is null if this method is invoked from Lob.position()
            // If row has exceeded the size of the ArrayList, new up a new int[] and add it to the
            // ArrayList, otherwise just reuse the int[].
            if (netResultSet_ != null && netResultSet_.scrollable_) {
                columnDataPosition = allocateColumnDataPositionArray(rowIndex);
                columnDataComputedLength = allocateColumnDataComputedLengthArray(rowIndex);
                columnDataIsNull = allocateColumnDataIsNullArray(rowIndex);
                // Since we are no longer setting the int[]'s to null for a delete/update hole, we need
                // another way of keeping track of the delete/update holes.
                setIsUpdataDeleteHole(rowIndex, false);
            } else {
                // Use the arrays defined on the Cursor for forward-only cursors.
                // can they ever be null
                if (columnDataPosition_ == null || columnDataComputedLength_ == null || isNull_ == null) {
                    allocateColumnOffsetAndLengthArrays();
                }
                columnDataPosition = columnDataPosition_;
                columnDataComputedLength = columnDataComputedLength_;
                columnDataIsNull = isNull_;
            }

            // Loop through the columns
            for (int index = 0; index < columns_; index++) {
                // If column is nullable, read the 1-byte null indicator.
                if (nullable_[index])
                // Need to pass the column index so all previously calculated offsets can be
                // readjusted if the query block splits on a column null indicator.

                // null indicators from FD:OCA data
                // 0 to 127: a data value will flow.
                // -1 to -128: no data value will flow.
                {
                    colNullIndicator = readFdocaOneByte(index);
                }

                // If non-null column data
                if (!nullable_[index] || (colNullIndicator >= 0 && colNullIndicator <= 127)) {

                    // Set the isNull indicator to false
                    columnDataIsNull[index] = false;

                    switch (typeToUseForComputingDataLength_[index]) {
                    // for fixed length data
                    case Typdef.FIXEDLENGTH:
                        columnDataPosition[index] = position_;
                        if (isGraphic_[index]) {
                            columnDataComputedLength[index] = skipFdocaBytes(fdocaLength_[index] * 2, index);
                        } else {
                            columnDataComputedLength[index] = skipFdocaBytes(fdocaLength_[index], index);
                        }
                        break;

                        // for variable character string and variable byte string,
                        // there are 2-byte of length in front of the data
                    case Typdef.TWOBYTELENGTH:
                        columnDataPosition[index] = position_;
                        length = readFdocaTwoByteLength(index);
                        // skip length + the 2-byte length field
                        if (isGraphic_[index]) {
                            columnDataComputedLength[index] = skipFdocaBytes(length * 2, index) + 2;
                        } else {
                            columnDataComputedLength[index] = skipFdocaBytes(length, index) + 2;
                        }
                        break;

                        // For decimal columns, determine the precision, scale, and the representation
                    case Typdef.DECIMALLENGTH:
                        columnDataPosition[index] = position_;
                        columnDataComputedLength[index] = skipFdocaBytes(getDecimalLength(index), index);
                        break;

                    case Typdef.LOBLENGTH:
                        columnDataPosition[index] = position_;
                        columnDataComputedLength[index] = this.skipFdocaBytes(fdocaLength_[index] & 0x7fff, index);
                        break;

                        // for short variable character string and short variable byte string,
                        // there is a 1-byte length in front of the data
                    case Typdef.ONEBYTELENGTH:
                        columnDataPosition[index] = position_;
                        length = readFdocaOneByte(index);
                        // skip length + the 1-byte length field
                        if (isGraphic_[index]) {
                            columnDataComputedLength[index] = skipFdocaBytes(length * 2, index) + 1;
                        } else {
                            columnDataComputedLength[index] = skipFdocaBytes(length, index) + 1;
                        }
                        break;

                    default:
                        columnDataPosition[index] = position_;
                        if (isGraphic_[index]) {
                            columnDataComputedLength[index] = skipFdocaBytes(fdocaLength_[index] * 2, index);
                        } else {
                            columnDataComputedLength[index] = skipFdocaBytes(fdocaLength_[index], index);
                        }
                        break;
                    }
                } else if ((colNullIndicator & 0x80) == 0x80) {
                    // Null data. Set the isNull indicator to true.
                    columnDataIsNull[index] = true;
                }
            }

            // set column offsets for the current row.
            columnDataPosition_ = columnDataPosition;
            columnDataComputedLength_ = columnDataComputedLength;
            isNull_ = columnDataIsNull;

            if (!allRowsReceivedFromServer()) {
                calculateLobColumnPositionsForRow();
                // Flow another CNTQRY if we are blocking, are using rtnextrow, and expect
                // non-trivial EXTDTAs for forward only cursors.  Note we do not support
                // EXTDTA retrieval for scrollable cursors.
                // if qryrowset was sent on excsqlstt for a sp call, which is only the case
                if (blocking_ && rtnextrow_ &&
                    !netResultSet_.scrollable_ &&
                    !extdtaPositions_.isEmpty()) {
                    if (allowServerFetch) {
                        netResultSet_.flowFetch();
                    } else {
                        return false;
                    }
                }
            }
        } else {
            if (netResultSet_ != null && netResultSet_.scrollable_) {
		if (receivedDeleteHoleWarning) {
		    setIsUpdataDeleteHole(rowIndex, true);
		} else {
		    if (SanityManager.DEBUG) {
			// Invariant: for SUR, we introduced the warning
			// in addition to null data.
			SanityManager
			    .THROWASSERT("Delete hole warning expected");
		    }
		}
            }
        }

        // If blocking protocol is used, we could have already received an ENDQRYRM,
        // which sets allRowsReceivedFromServer_ to true.  It's safe to assume that all of
        // our QRYDTA's have been successfully copied to the dataBuffer.  And even though
        // the flag for allRowsReceivedFromServer_ is set, we still want to continue to parse through
        // the data in the dataBuffer.
        // But in the case where fixed row protocol is used,
        if (!blocking_ && allRowsReceivedFromServer() &&
            daNullIndicator == 0xFF) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Scan the data buffer to see if end of data (SQL state 02000)
     * has been received. This method should only be called when the
     * cursor is being closed since the pointer to the current row can
     * be modified.
     *
     * @exception SqlException
     */
    void scanDataBufferForEndOfData() throws SqlException {
        while (!allRowsReceivedFromServer() &&
               (position_ != lastValidBytePosition_)) {
            stepNext(false);
        }
    }

    protected boolean isDataBufferNull() {
        if (dataBuffer_ == null) {
            return true;
        } else {
            return false;
        }
    }

    protected void allocateDataBuffer() {
        int length;
        if (maximumRowSize_ > DssConstants.MAX_DSS_LEN) {
            length = maximumRowSize_;
        } else {
            length = DssConstants.MAX_DSS_LEN;
        }

        dataBuffer_ = new byte[length];
        position_ = 0;
        lastValidBytePosition_ = 0;
    }

    protected void allocateDataBuffer(int length) {
        dataBuffer_ = new byte[length];
    }


    private int readFdocaInt() throws org.apache.derby.client.am.DisconnectException, SqlException {
        if ((position_ + 4) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        int i = SignedBinary.getInt(dataBuffer_, position_);
        position_ += 4;
        return i;
    }

    // Reads 8-bytes from the dataBuffer from the current position.
    // If position is already at the end of the buffer, send CNTQRY to get more 
    // data.
    private long readFdocaLong() throws 
            org.apache.derby.client.am.DisconnectException, SqlException {
        if ((position_ + 8) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was 
            // received, throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        long i = SignedBinary.getLong(dataBuffer_, position_);
        position_ += 8;
        return i;
    }
        
    // Reads 1-byte from the dataBuffer from the current position.
    // If position is already at the end of the buffer, send CNTQRY to get more data.
    private int readFdocaOneByte() throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if (position_ == lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }
        return dataBuffer_[position_++] & 0xff;
    }

    // Reads 1-byte from the dataBuffer from the current position.
    // If position is already at the end of the buffer, send CNTQRY to get more data.
    private int readFdocaOneByte(int index) throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if (position_ == lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow(index);

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }
        return dataBuffer_[position_++] & 0xff;
    }

    // Reads <i>length</i> number of bytes from the dataBuffer starting from the
    // current position.  Returns a new byte array which contains the bytes read.
    // If current position plus length goes past the lastValidBytePosition, send
    // CNTQRY to get more data.
    private byte[] readFdocaBytes(int length) throws org.apache.derby.client.am.DisconnectException, SqlException {
        byte[] b = new byte[length];
        ;

        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + length) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        for (int i = 0; i < length; i++) {
            b[i] = dataBuffer_[position_++];
        }

        return b;
    }

    // Reads 2-bytes from the dataBuffer starting from the current position, and
    // returns an integer constructed from the 2-bytes.  If current position plus
    // 2 bytes goes past the lastValidBytePosition, send CNTQRY to get more data.
    private int readFdocaTwoByteLength() throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + 2) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        return
                ((dataBuffer_[position_++] & 0xff) << 8) +
                ((dataBuffer_[position_++] & 0xff) << 0);
    }

    private int readFdocaTwoByteLength(int index) throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + 2) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow(index);

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        return
                ((dataBuffer_[position_++] & 0xff) << 8) +
                ((dataBuffer_[position_++] & 0xff) << 0);
    }

    // Check if position plus length goes past the lastValidBytePosition.
    // If so, send CNTQRY to get more data.
    // length - number of bytes to skip
    // returns the number of bytes skipped
    private int skipFdocaBytes(int length) throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + length) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }
        position_ += length;
        return length;
    }

    private int skipFdocaBytes(int length, int index) throws org.apache.derby.client.am.DisconnectException, SqlException {
        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + length) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow(index);

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        position_ += length;
        return length;
    }

    // Shift partial row bytes to beginning of dataBuffer,
    // and resets current row position, and lastValidBytePosition.
    // When we shift partial row, we'll have to recalculate column offsets
    // up to this column.
    private void shiftPartialRowToBeginning() {
        // Get the length to shift from the beginning of the partial row.
        int length = lastValidBytePosition_ - currentRowPosition_;

        // shift the data in the dataBufferStream
        dataBufferStream_.reset();
        if (dataBuffer_ != null) {
            dataBufferStream_.write(dataBuffer_, currentRowPosition_, length);
        }

        for (int i = 0; i < length; i++) {
            dataBuffer_[i] = dataBuffer_[currentRowPosition_ + i];
        }

        position_ = length - (lastValidBytePosition_ - position_);
        lastValidBytePosition_ = length;
    }

    private void adjustColumnOffsetsForColumnsPreviouslyCalculated(int index) {
        for (int j = 0; j <= index; j++) {
            columnDataPosition_[j] -= currentRowPosition_;
        }
    }

    private void resetCurrentRowPosition() {
        currentRowPosition_ = 0;
    }

    // Calculates the column index for Lob objects constructed from EXTDTA data.
    // Describe information isn't sufficient because we have to check
    // for trivial values (nulls or zero-length) and exclude them.
    void calculateLobColumnPositionsForRow() {
        int currentPosition = 0;

        for (int i = 0; i < columns_; i++) {
            if (isNonTrivialDataLob(i))
            // key = column position, data = index to corresponding data in extdtaData_
            // ASSERT: the server always returns the EXTDTA objects in ascending order
            {
                extdtaPositions_.put(new Integer(i + 1), new Integer(currentPosition++));
            }
        }
    }

    // prereq: the base data for the cursor has been processed for offsets and lengths
    boolean isNonTrivialDataLob(int index) {
        long length = 0L;

        if (isNull_[index] ||
                (jdbcTypes_[index] != Types.BLOB &&
                jdbcTypes_[index] != Types.CLOB)) {
            return false;
        }

        int position = columnDataPosition_[index];

        // if the high-order bit is set, length is unknown -> set value to x'FF..FF'
        if (((dataBuffer_[position]) & 0x80) == 0x80) {
            length = -1;
        } else {

            byte[] lengthBytes = new byte[columnDataComputedLength_[index]];
            byte[] longBytes = new byte[8];

            System.arraycopy(dataBuffer_,
                    position,
                    lengthBytes,
                    0,
                    columnDataComputedLength_[index]);

            // right-justify for BIG ENDIAN
            int j = 0;
            for (int i = 8 - columnDataComputedLength_[index]; i < 8; i++) {
                longBytes[i] = lengthBytes[j];
                j++;
            }
            length = SignedBinary.getLong(longBytes, 0);
        }
        return (length != 0L) ? true : false;
    }

    protected void clearLobData_() {
        extdtaData_.clear();
        extdtaPositions_.clear();
    }

    // SQLCARD : FDOCA EARLY ROW
    // SQL Communications Area Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLCAGRP; GROUP LID 0x54; ELEMENT TAKEN 0(all); REP FACTOR 1
    NetSqlca[] parseSQLCARD(Typdef typdef) throws org.apache.derby.client.am.DisconnectException, SqlException {
        return parseSQLCAGRP(typdef);
    }

    // SQLCAGRP : FDOCA EARLY GROUP
    // SQL Communcations Area Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //   SQLDIAGGRP; PROTOCOL TYPE N-GDA; ENVLID 0x56; Length Override 0
    private NetSqlca[] parseSQLCAGRP(Typdef typdef) throws org.apache.derby.client.am.DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return null;
        }
        int sqlcode = readFdocaInt();
        byte[] sqlstate = readFdocaBytes(5);
        byte[] sqlerrproc = readFdocaBytes(8);
        NetSqlca netSqlca = new NetSqlca(netAgent_.netConnection_, sqlcode, sqlstate, sqlerrproc);

        parseSQLCAXGRP(typdef, netSqlca);

        NetSqlca[] sqlCa = parseSQLDIAGGRP();

        NetSqlca[] ret_val;
        if (sqlCa != null) {
            ret_val = new NetSqlca[sqlCa.length + 1];
            System.arraycopy(sqlCa, 0, ret_val, 1, sqlCa.length);
        } else {
            ret_val = new NetSqlca[1];
        }
        ret_val[0] = netSqlca;
        
        return ret_val;
    }

    // SQLCAXGRP : EARLY FDOCA GROUP
    // SQL Communications Area Exceptions Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLRDBNME; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 18
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    private void parseSQLCAXGRP(Typdef typdef, NetSqlca netSqlca) throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            netSqlca.setContainsSqlcax(false);
            return;
        }


        //   SQLERRD1 to SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
        int[] sqlerrd = new int[6];
        for (int i = 0; i < sqlerrd.length; i++) {
            sqlerrd[i] = readFdocaInt();
        }

        //   SQLWARN0 to SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
        byte[] sqlwarn = readFdocaBytes(11);

        // skip over the rdbnam for now
        // SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        parseVCS(typdef);

        //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
        //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
        int varcharLength = readFdocaTwoByteLength();  // mixed length
        byte[] sqlerrmc = null;
        int sqlerrmcCcsid = 0;
        if (varcharLength != 0) {                    // if mixed
            sqlerrmc = readFdocaBytes(varcharLength);      // read mixed bytes
            sqlerrmcCcsid = typdef.getCcsidMbc();
            skipFdocaBytes(2);                          // skip single length
        } else {
            varcharLength = readFdocaTwoByteLength();  // read single length
            sqlerrmc = readFdocaBytes(varcharLength);     // read single bytes
            sqlerrmcCcsid = typdef.getCcsidSbc();
        }

        netSqlca.setSqlerrd(sqlerrd);
        netSqlca.setSqlwarnBytes(sqlwarn);
        netSqlca.setSqlerrmcBytes(sqlerrmc, sqlerrmcCcsid);
    }

    // SQLDIAGGRP : FDOCA EARLY GROUP
    private NetSqlca[] parseSQLDIAGGRP() throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return null;
        }

        parseSQLDIAGSTT();
        NetSqlca[] sqlca = parseSQLDIAGCI();
        parseSQLDIAGCN();

        return sqlca;
    }

    // SQL Diagnostics Statement Group Description - Identity 0xD3
    // NULLDATA will be received for now
    private void parseSQLDIAGSTT() throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return;
        }

        // The server should send NULLDATA
        netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(netAgent_, 
                    new ClientMessageId(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED),
                    "parseSQLDIAGSTT"));
    }

    // SQL Diagnostics Condition Information Array - Identity 0xF5
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLDCIROW; ROW LID 0xE5; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private NetSqlca[] parseSQLDIAGCI() 
            throws DisconnectException, SqlException {
        int num = readFdocaTwoByteLength(); // SQLNUMGRP - SQLNUMROW
        NetSqlca[] ret_val = null;
        if (num != 0) {
            ret_val = new NetSqlca[num];
        } 

        for (int i = 0; i < num; i++) {
            ret_val[i] = parseSQLDCROW();
        }
        return ret_val;
    }

    // SQL Diagnostics Connection Array - Identity 0xF6
    // NULLDATA will be received for now
    private void parseSQLDIAGCN() throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return;
        }
        
        // The server should send NULLDATA
        netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(netAgent_, 
                    new ClientMessageId(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED),
                    "parseSQLDIAGCN"));
    }

    // SQL Diagnostics Condition Group Description
    //
    // SQLDCCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCSTATE; PROTOCOL TYPE FCS; ENVLID Ox30; Lengeh Override 5
    // SQLDCREASON; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCLINEN; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCROWN; PROTOCOL TYPE I8; ENVLID 0x16; Lengeh Override 8
    // SQLDCER01; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER02; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER03; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER04; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPART; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPPOP; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCMSGID; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 10
    // SQLDCMDE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    // SQLDCPMOD; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    // SQLDCRDB; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLDCTOKS; PROTOCOL TYPE N-RLO; ENVLID 0xF7; Length Override 0
    // SQLDCMSG_m; PROTOCOL TYPE NVMC; ENVLID 0x3F; Length Override 32672
    // SQLDCMSG_S; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 32672
    // SQLDCCOLN_m; PROTOCOL TYPE NVCM ; ENVLID 0x3F; Length Override 255
    // SQLDCCOLN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCCURN_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCCURN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCPNAM_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCPNAM_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXGRP; PROTOCOL TYPE N-GDA; ENVLID 0xD3; Length Override 1
    private NetSqlca parseSQLDCGRP() 
            throws DisconnectException, SqlException {
        
        int sqldcCode = readFdocaInt(); // SQLCODE
        String sqldcState = readFdocaString(5, 
                netAgent_.targetTypdef_.getCcsidSbcEncoding()); // SQLSTATE
        int sqldcReason = readFdocaInt();  // REASON_CODE

        skipFdocaBytes(12); // LINE_NUMBER + ROW_NUMBER

        NetSqlca sqlca = new NetSqlca(netAgent_.netConnection_,
                    sqldcCode,
                    sqldcState,
                    (byte[]) null);

        skipFdocaBytes(49); // SQLDCER01-04 + SQLDCPART + SQLDCPPOP + SQLDCMSGID
                            // SQLDCMDE + SQLDCPMOD + RDBNAME
        parseSQLDCTOKS(); // MESSAGE_TOKENS

        String sqldcMsg = parseVCS(qrydscTypdef_); // MESSAGE_TEXT

        if (sqldcMsg != null) {
            sqlca.setSqlerrmcBytes(sqldcMsg.getBytes(), 
                    netAgent_.targetTypdef_.getByteOrder());
        }

        skipFdocaBytes(12);  // COLUMN_NAME + PARAMETER_NAME + EXTENDED_NAMES

        parseSQLDCXGRP(); // SQLDCXGRP
        return sqlca;
    }

    // SQL Diagnostics Condition Row - Identity 0xE5
    // SQLDCGRP; GROUP LID 0xD5; ELEMENT TAKEN 0(all); REP FACTOR 1
    private NetSqlca parseSQLDCROW() throws DisconnectException, SqlException {
        return parseSQLDCGRP();
    }
    
    // SQL Diagnostics Condition Token Array - Identity 0xF7
    // NULLDATA will be received for now
    void parseSQLDCTOKS() throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return;
        }

        // The server should send NULLDATA
        netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(netAgent_, 
                    new ClientMessageId(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED),
                    "parseSQLDCTOKS"));
    }

    // SQL Diagnostics Extended Names Group Description - Identity 0xD5
    // NULLDATA will be received for now
    private void parseSQLDCXGRP() throws DisconnectException, SqlException {
        if (readFdocaOneByte() == CodePoint.NULLDATA) {
            return;
        }

        // The server should send NULLDATA
        netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(netAgent_, 
                    new ClientMessageId(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED),
                    "parseSQLDCXGRP"));
    }

    private String parseVCS(Typdef typdefInEffect) throws DisconnectException, SqlException {
        return readFdocaString(readFdocaTwoByteLength(),
                typdefInEffect.getCcsidSbcEncoding());
    }

    // This is not used for column data.
    private String readFdocaString(int length, String encoding) throws DisconnectException, SqlException {
        if (length == 0) {
            return null;
        }

        // For singleton select, the complete row always comes back, even if multiple query blocks are required,
        // so there is no need to drive a flowFetch (continue query) request for singleton select.
        if ((position_ + length) > lastValidBytePosition_) {
            // Check for ENDQRYRM, throw SqlException if already received one.
            checkAndThrowReceivedEndqryrm();

            // Send CNTQRY to complete the row/rowset.
            int lastValidByteBeforeFetch = completeSplitRow();

            // if lastValidBytePosition_ has not changed, and an ENDQRYRM was received,
            // throw a SqlException for the ENDQRYRM.
            checkAndThrowReceivedEndqryrm(lastValidByteBeforeFetch);
        }

        String s = null;

        try {
            s = new String(dataBuffer_, position_, length, encoding);
        } catch (java.io.UnsupportedEncodingException e) {
            netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                new org.apache.derby.client.am.DisconnectException(
                    netAgent_, 
                    new ClientMessageId(SQLState.NET_ENCODING_NOT_SUPPORTED), 
                    e));
        }

        position_ += length;
        return s;
    }

    void allocateColumnOffsetAndLengthArrays() {
        columnDataPosition_ = new int[columns_];
        columnDataComputedLength_ = new int[columns_];
        isNull_ = new boolean[columns_];
    }

    void setBlocking(int queryProtocolType) {
        if (queryProtocolType == CodePoint.LMTBLKPRC) {
            blocking_ = true;
        } else {
            blocking_ = false;
        }
    }

    protected byte[] findExtdtaData(int column) {
        byte[] data = null;

        // locate the EXTDTA bytes, if any
        Integer key = new Integer(column);

        if (extdtaPositions_.containsKey(key)) {
            //  found, get the data
            int extdtaQueuePosition = ((Integer) extdtaPositions_.get(key)).intValue();
            data = (byte[]) (extdtaData_.get(extdtaQueuePosition));
        }

        return data;
    }

    public Blob getBlobColumn_(int column, Agent agent) throws SqlException {
        int index = column - 1;
        int dataOffset;
        byte[] data;
        Blob blob = null;

        // locate the EXTDTA bytes, if any
        data = findExtdtaData(column);

        if (data != null) {
            // data found
            // set data offset based on the presence of a null indicator
            if (!nullable_[index]) {
                dataOffset = 0;
            } else {
                dataOffset = 1;
            }

            blob = new Blob(data, agent, dataOffset);
        } else {
            blob = new Blob(new byte[0], agent, 0);
        }

        return blob;
    }


    public Clob getClobColumn_(int column, Agent agent) throws SqlException {
        int index = column - 1;
        int dataOffset;
        byte[] data;
        Clob clob = null;

        // locate the EXTDTA bytes, if any
        data = findExtdtaData(column);

        if (data != null) {
            // data found
            // set data offset based on the presence of a null indicator
            if (!nullable_[index]) {
                dataOffset = 0;
            } else {
                dataOffset = 1;
            }
            clob = new Clob(agent, data, charsetName_[index], dataOffset);
        } else {
            // the locator is not valid, it is a zero-length LOB
            clob = new Clob(agent, "");
        }

        return clob;
    }

    public byte[] getClobBytes_(int column, int[] dataOffset /*output*/) throws SqlException {
        int index = column - 1;
        byte[] data = null;

        // locate the EXTDTA bytes, if any
        data = findExtdtaData(column);

        if (data != null) {
            // data found
            // set data offset based on the presence of a null indicator
            if (!nullable_[index]) {
                dataOffset[0] = 0;
            } else {
                dataOffset[0] = 1;
            }
        }

        return data;
    }

    // this is really an event-callback from NetStatementReply.parseSQLDTARDarray()
    void initializeColumnInfoArrays(Typdef typdef,
                                    int columnCount, int targetSqlamForTypdef) throws DisconnectException {
        qrydscTypdef_ = typdef;

        // Allocate  arrays to hold the descriptor information.
        setNumberOfColumns(columnCount);
        fdocaLength_ = new int[columnCount];
        isGraphic_ = new boolean[columnCount];
        typeToUseForComputingDataLength_ = new int[columnCount];
        targetSqlamForTypdef_ = targetSqlamForTypdef;
    }


    int ensureSpaceForDataBuffer(int ddmLength) {
        if (dataBuffer_ == null) {
            allocateDataBuffer();
        }
        //super.resultSet.cursor.clearColumnDataOffsetsCache();
        // Need to know how many bytes to ask from the Reply object,
        // and handle the case where buffer is not big enough for all the bytes.
        // Get the length in front of the code point first.

        int bytesAvailableInDataBuffer = dataBuffer_.length - lastValidBytePosition_;

        // Make sure the buffer has at least ddmLength amount of room left.
        // If not, expand the buffer before calling the getQrydtaData() method.
        if (bytesAvailableInDataBuffer < ddmLength) {

            // Get a new buffer that is twice the size of the current buffer.
            // Copy the contents from the old buffer to the new buffer.
            int newBufferSize = 2 * dataBuffer_.length;

            while (newBufferSize < ddmLength) {
                newBufferSize = 2 * newBufferSize;
            }

            byte[] tempBuffer = new byte[newBufferSize];

            System.arraycopy(dataBuffer_,
                    0,
                    tempBuffer,
                    0,
                    lastValidBytePosition_);

            // Make the new buffer the dataBuffer.
            dataBuffer_ = tempBuffer;

            // Recalculate bytesAvailableInDataBuffer
            bytesAvailableInDataBuffer = dataBuffer_.length - lastValidBytePosition_;
        }
        return bytesAvailableInDataBuffer;
    }

    protected void getMoreData_() throws SqlException {
        // reset the dataBuffer_ before getting more data if cursor is foward-only.
        // getMoreData() is only called in Cursor.next() when current position is
        // equal to lastValidBytePosition_.
        if (netResultSet_.resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
            resetDataBuffer();
        }
        netResultSet_.flowFetch();
    }

    public void nullDataForGC()       // memory leak fix
    {
        super.nullDataForGC();
        qrydscTypdef_ = null;
        typeToUseForComputingDataLength_ = null;
        isGraphic_ = null;

        if (extdtaPositions_ != null) {
            extdtaPositions_.clear();
        }
        extdtaPositions_ = null;

        if (extdtaData_ != null) {
            extdtaData_.clear();
        }
        extdtaData_ = null;
    }

    // It is possible for the driver to have received an QRYDTA(with incomplete row)+ENDQRYRM+SQLCARD.
    // This means some error has occurred on the server and the server is terminating the query.
    // Before sending a CNTQRY to retrieve the rest of the split row, check if an ENDQRYRM has already
    // been received.  If so, do not send CNTQRY because the cursor is already closed on the server.
    // Instead, throw a SqlException.  Since we did not receive a complete row, it is not safe to
    // allow the application to continue to access the ResultSet, so we close it.
    private void checkAndThrowReceivedEndqryrm() throws SqlException {
        // If we are in a split row, and before sending CNTQRY, check whether an ENDQRYRM
        // has been received.
        if (!netResultSet_.openOnServer_) {
            SqlException sqlException = null;
            int sqlcode = org.apache.derby.client.am.Utils.getSqlcodeFromSqlca(netResultSet_.queryTerminatingSqlca_);
            if (sqlcode < 0) {
                sqlException = new SqlException(agent_.logWriter_, netResultSet_.queryTerminatingSqlca_);
            } else {
                sqlException = new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.NET_QUERY_PROCESSING_TERMINATED));
            }
            try {
                netResultSet_.closeX(); // the auto commit logic is in closeX()
            } catch (SqlException e) {
                sqlException.setNextException(e);
            }
            throw sqlException;
        }
    }

    private void checkAndThrowReceivedEndqryrm(int lastValidBytePositionBeforeFetch) throws SqlException {
        // if we have received more data in the dataBuffer_, just return.
        if (lastValidBytePosition_ > lastValidBytePositionBeforeFetch) {
            return;
        }
        checkAndThrowReceivedEndqryrm();
    }

    private int completeSplitRow() throws DisconnectException, SqlException {
        int lastValidBytePositionBeforeFetch = 0;
        if (netResultSet_ != null && netResultSet_.scrollable_) {
            lastValidBytePositionBeforeFetch = lastValidBytePosition_;
            netResultSet_.flowFetchToCompleteRowset();
        } else {
            // Shift partial row to the beginning of the dataBuffer
            shiftPartialRowToBeginning();
            resetCurrentRowPosition();
            lastValidBytePositionBeforeFetch = lastValidBytePosition_;
            netResultSet_.flowFetch();
        }
        return lastValidBytePositionBeforeFetch;
    }

    private int completeSplitRow(int index) throws DisconnectException, SqlException {
        int lastValidBytePositionBeforeFetch = 0;
        if (netResultSet_ != null && netResultSet_.scrollable_) {
            lastValidBytePositionBeforeFetch = lastValidBytePosition_;
            netResultSet_.flowFetchToCompleteRowset();
        } else {
            // Shift partial row to the beginning of the dataBuffer
            shiftPartialRowToBeginning();
            adjustColumnOffsetsForColumnsPreviouslyCalculated(index);
            resetCurrentRowPosition();
            lastValidBytePositionBeforeFetch = lastValidBytePosition_;
            netResultSet_.flowFetch();
        }
        return lastValidBytePositionBeforeFetch;
    }

    private int[] allocateColumnDataPositionArray(int row) {
        int[] columnDataPosition;
        if (columnDataPositionCache_.size() == row) {
            columnDataPosition = new int[columns_];
            columnDataPositionCache_.add(columnDataPosition);
        } else {
            columnDataPosition = (int[]) columnDataPositionCache_.get(row);
        }
        return columnDataPosition;
    }

    private int[] allocateColumnDataComputedLengthArray(int row) {
        int[] columnDataComputedLength;
        if (columnDataLengthCache_.size() == row) {
            columnDataComputedLength = new int[columns_];
            columnDataLengthCache_.add(columnDataComputedLength);
        } else {
            columnDataComputedLength = (int[]) columnDataLengthCache_.get(row);
        }
        return columnDataComputedLength;
    }

    private boolean[] allocateColumnDataIsNullArray(int row) {
        boolean[] columnDataIsNull;
        if (columnDataIsNullCache_.size() <= row) {
            columnDataIsNull = new boolean[columns_];
            columnDataIsNullCache_.add(columnDataIsNull);
        } else {
            columnDataIsNull = (boolean[]) columnDataIsNullCache_.get(row);
        }
        return columnDataIsNull;
    }

    protected int getDecimalLength(int index) {
        return (((fdocaLength_[index] >> 8) & 0xff) + 2) / 2;
    }

    /**
     * Set the value of value of allRowsReceivedFromServer_.
     *
     * @param b a <code>boolean</code> value indicating whether all
     * rows are received from the server
     */
    public final void setAllRowsReceivedFromServer(boolean b) {
        if (b && qryclsimpEnabled_) {
            netResultSet_.markClosedOnServer();
        }
        super.setAllRowsReceivedFromServer(b);
    }

    /**
     * Set a flag indicating whether QRYCLSIMP is enabled.
     *
     * @param flag true if QRYCLSIMP is enabled
     */
    final void setQryclsimpEnabled(boolean flag) {
        qryclsimpEnabled_ = flag;
    }

    /**
     * Check whether QRYCLSIMP is enabled on this cursor.
     *
     * @return true if QRYCLSIMP is enabled
     */
    final boolean getQryclsimpEnabled() {
        return qryclsimpEnabled_;
    }
}

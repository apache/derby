/*

   Derby - Class org.apache.derby.client.net.NetSqldta

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

package org.apache.derby.client.net;

import org.apache.derby.client.am.SqlException;


class NetSqldta extends NetCursor {

    NetSqldta(NetAgent netAgent) {
        super(netAgent);
    }

    public boolean next() throws SqlException {
        if (allRowsReceivedFromServer()) {
            return false;
        } else {
            setAllRowsReceivedFromServer(true);
            return true;
        }
    }

    protected boolean calculateColumnOffsetsForRow() {
        int colNullIndicator = CodePoint.NULLDATA;
        int length;

        extdtaPositions_.clear();  // reset positions for this row

        // read the da null indicator
        if (readFdocaOneByte() == 0xff) {
            return false;
        }

        incrementRowsReadEvent();
        // Use the arrays defined on the Cursor for forward-only cursors.
        // can they ever be null
        if (columnDataPosition_ == null || columnDataComputedLength_ == null || isNull_ == null) {
            allocateColumnOffsetAndLengthArrays();
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
                colNullIndicator = readFdocaOneByte();
            }

            // If non-null column data
            if (!nullable_[index] || (colNullIndicator >= 0 && colNullIndicator <= 127)) {
                isNull_[index] = false;

                switch (typeToUseForComputingDataLength_[index]) {
                // for variable character string and variable byte string,
                // there are 2-byte of length in front of the data
                case Typdef.TWOBYTELENGTH:
                    columnDataPosition_[index] = position_;
                    length = readFdocaTwoByteLength();
                    // skip length + the 2-byte length field
                    if (isGraphic_[index]) {
                        columnDataComputedLength_[index] = skipFdocaBytes(length * 2) + 2;
                    } else {
                        columnDataComputedLength_[index] = skipFdocaBytes(length) + 2;
                    }
                    break;

                    // for short variable character string and short variable byte string,
                    // there is a 1-byte length in front of the data
                case Typdef.ONEBYTELENGTH:
                    columnDataPosition_[index] = position_;
                    length = readFdocaOneByte();
                    // skip length + the 1-byte length field
                    if (isGraphic_[index]) {
                        columnDataComputedLength_[index] = skipFdocaBytes(length * 2) + 1;
                    } else {
                        columnDataComputedLength_[index] = skipFdocaBytes(length) + 1;
                    }
                    break;

                    // For decimal columns, determine the precision, scale, and the representation
                case Typdef.DECIMALLENGTH:
                    columnDataPosition_[index] = position_;
                    columnDataComputedLength_[index] = skipFdocaBytes(getDecimalLength(index));
                    break;

                case Typdef.LOBLENGTH:
                    columnDataPosition_[index] = position_;
                    columnDataComputedLength_[index] = this.skipFdocaBytes(fdocaLength_[index] & 0x7fff);
                    break;

                default:
                    columnDataPosition_[index] = position_;
                    if (isGraphic_[index]) {
                        columnDataComputedLength_[index] = skipFdocaBytes(fdocaLength_[index] * 2);
                    } else {
                        columnDataComputedLength_[index] = skipFdocaBytes(fdocaLength_[index]);
                    }
                    break;
                }
            } else if ((colNullIndicator & 0x80) == 0x80) {
                // Null data. Set the isNull indicator to true.
                isNull_[index] = true;
            }
        }

        if (!allRowsReceivedFromServer()) {
            calculateLobColumnPositionsForRow();
        }

        return true; // hardwired for now, this means the current row position is a valid position
    }


    private int skipFdocaBytes(int length) {
        position_ += length;
        return length;
    }

    private int readFdocaOneByte() {
        return dataBuffer_[position_++] & 0xff;
    }


    private int readFdocaTwoByteLength() {
        return
                ((dataBuffer_[position_++] & 0xff) << 8) +
                ((dataBuffer_[position_++] & 0xff) << 0);
    }


}


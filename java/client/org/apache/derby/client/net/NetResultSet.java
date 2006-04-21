/*

   Derby - Class org.apache.derby.client.net.NetResultSet

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

import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;


public class NetResultSet extends org.apache.derby.client.am.ResultSet {
    // Alias for (NetConnection) super.statement.connection
    private final NetConnection netConnection_;

    // Alias for (NetStatement) super.statement
    private final NetStatement netStatement_;

    // Alias for (NetCursor) super.cursor
    final NetCursor netCursor_;

    // Alias for (NetAgent) super.agent
    final private NetAgent netAgent_;

    // Indicates whether the fixed row protocol is being used. If so,
    // the fetch size will always be 1.
    private boolean isFixedRowProtocol = false;
    
    //-----------------------------state------------------------------------------

    // This is used to avoid sending multiple outovr over subsequent next()'s
    public boolean firstOutovrBuilt_ = false;

    //---------------------constructors/finalizer---------------------------------

    // parseOpnqrym() is called right after this constructor is called.

    NetResultSet(NetAgent netAgent,
                 NetStatement netStatement,
                 Cursor cursor,
                 int qryprctyp,  //protocolType, CodePoint.FIXROWPRC |
                                 //              CodePoint.LMTBLKPRC
                 int sqlcsrhld, // holdOption, 0xF0 for false (default) | 0xF1 for true.
                 int qryattscr, // scrollOption, 0xF0 for false (default) | 0xF1 for true.
                 int qryattsns, // sensitivity, CodePoint.QRYUNK | 
                                //              CodePoint.QRYINS |
                                //              CodePoint.QRYSNSSTC
                 int qryattset, // rowsetCursor, 0xF0 for false (default) | 0xF1 for true.
                 long qryinsid, // instanceIdentifier, 0 (if not returned, check default) or number
                 int actualResultSetType,
                 int actualResultSetConcurrency,
                 int actualResultSetHoldability) //throws DisconnectException
    {
        super(netAgent,
                netStatement.statement_,
                //new NetCursor (netAgent, qryprctyp),
                cursor,
                // call the constructor with the real resultSetType and resultSetConcurrency
                // returned from the server
                actualResultSetType,
                actualResultSetConcurrency,
                actualResultSetHoldability);

        netAgent_ = netAgent;

        // Set up cheat-links
        netCursor_ = (NetCursor) cursor_;
        netStatement_ = netStatement;
        netConnection_ = netStatement.netConnection_;

        netCursor_.netResultSet_ = this;

        cursorHold_ = (sqlcsrhld != 0xf0);
        if (qryattscr == 0xF1) {
            scrollable_ = true;
        }

        // The number of rows returned by the server will always be 1 when the
        // Fixed Row Protocol is being used.
        if (qryprctyp == CodePoint.FIXROWPRC) {
            isFixedRowProtocol = true;
            fetchSize_ = 1;
        } else {
            fetchSize_ = suggestedFetchSize_;
        }

        switch (qryattsns) {
        case CodePoint.QRYUNK:
            sensitivity_ = sensitivity_unknown__;
            break;
        case CodePoint.QRYINS:
            sensitivity_ = sensitivity_insensitive__;
            break;
        case CodePoint.QRYSNSSTC:
            sensitivity_ = sensitivity_sensitive_static__;
            break;
        default:   // shouldn't happen
            break;
        }

        if (qryattset == 0xF1) {
            isRowsetCursor_ = true;
        }

        queryInstanceIdentifier_ = qryinsid;
        nestingLevel_ = (int) ((queryInstanceIdentifier_ >>> 48) & 0xFFFF);
    }


    //-------------------------------flow methods---------------------------------

    // Go through the QRYDTA's received, and calculate the column offsets for each row.
    protected void parseRowset_() throws SqlException {
        int row = 0;
        // Parse all the rows received in the rowset
        // The index we are passing will keep track of which row in the rowset we are parsing
        // so we can reuse the columnDataPosition/Length/IsNull arrays.
        while (netCursor_.calculateColumnOffsetsForRow_(row, true)) {
            rowsReceivedInCurrentRowset_++;
            row++;
        }

        // if rowset is not complete and an endqryrm was received, will skip the while loop
        // and go to the checkAndThrow method.  otherwise flow an cntqry to try to complete
        // the rowset.
        // -- there is no need to complete the rowset for rowset cursors.  fetching stops when
        //    the end of data is returned or when an error occurs.  all successfully fetched rows
        //    are returned to the user.  the specific error is not returned until the next fetch.
        while (rowsReceivedInCurrentRowset_ != fetchSize_ &&
                !netCursor_.allRowsReceivedFromServer() && !isRowsetCursor_ &&
                sensitivity_ != sensitivity_sensitive_dynamic__ &&
                sensitivity_ != sensitivity_sensitive_static__) {
            flowFetchToCompleteRowset();
            while (netCursor_.calculateColumnOffsetsForRow_(row, true)) {
                rowsReceivedInCurrentRowset_++;
                row++;
            }
        }
        checkAndThrowReceivedQueryTerminatingException();
    }

    public void setFetchSize_(int rows) {
        // Do not change the fetchSize for Fixed Row Protocol
        suggestedFetchSize_ = (rows == 0) ? 64 : rows;
        if (!isFixedRowProtocol) {
            fetchSize_ = suggestedFetchSize_;
        }
    }

    //-----------------------------helper methods---------------------------------

    void flowFetchToCompleteRowset() throws DisconnectException {
        try {
            agent_.beginWriteChain(statement_);

            writeScrollableFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
                    fetchSize_ - rowsReceivedInCurrentRowset_,
                    scrollOrientation_relative__,
                    1,
                    false);  // false means do not disard pending
            // partial row and pending query blocks

            agent_.flow(statement_);
            readScrollableFetch_();
            agent_.endReadChain();
        } catch (SqlException e) {
            throw new DisconnectException(agent_, e);
        }
    }

    void queryDataWasReturnedOnOpen() throws DisconnectException {
    }

    // ------------------------------- abstract box car methods --------------------------------------
    public void writeFetch_(Section section) throws SqlException {
        if (resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY && fetchSize_ != 0 &&
                rowsYetToBeReceivedForRowset_ > 0) {
            netAgent_.resultSetRequest_.writeFetch(this,
                    section,
                    rowsYetToBeReceivedForRowset_);
        } else {
            netAgent_.resultSetRequest_.writeFetch(this,
                    section,
                    fetchSize_);
        }
    }

    public void readFetch_() throws SqlException {
        netAgent_.resultSetReply_.readFetch(this);
    }

    public void writeScrollableFetch_(Section section,
                                      int fetchSize,
                                      int orientation,
                                      long rowToFetch,
                                      boolean resetQueryBlocks) throws SqlException {
        netAgent_.resultSetRequest_.writeScrollableFetch(this,
                section,
                fetchSize,
                orientation,
                rowToFetch,
                resetQueryBlocks);
    }

    // think about splitting out the position cursor stuff from the fetch stuff
    // use commented out abstract position cursor methods above
    public void readScrollableFetch_() throws SqlException {
        netAgent_.resultSetReply_.readScrollableFetch(this);
    }

    public void writePositioningFetch_(Section section,
                                       int orientation,
                                       long rowToFetch) throws SqlException {
        netAgent_.resultSetRequest_.writePositioningFetch(this,
                section,
                orientation,
                rowToFetch);
    }

    public void readPositioningFetch_() throws SqlException {
        netAgent_.resultSetReply_.readPositioningFetch(this);
    }

    public void writeCursorClose_(Section section) throws SqlException {
        netAgent_.resultSetRequest_.writeCursorClose(this, section);
    }

    public void readCursorClose_() throws SqlException {
        netAgent_.resultSetReply_.readCursorClose(this);
    }

    /**
     * Method that is invoked by <code>closeX()</code> before the
     * result set is actually being closed. If QRYCLSIMP is enabled on
     * the cursor, scan data buffer for end of data (SQL state
     * 02000). If end of data is received, the result set is closed on
     * the server.
     *
     * @exception SqlException
     */
    protected void preClose_() throws SqlException {
        if (netCursor_.getQryclsimpEnabled()) {
            netCursor_.scanDataBufferForEndOfData();
        }
    }
}

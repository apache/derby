/*

   Derby - Class org.apache.derby.client.am.ResultSet

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

public abstract class ClientResultSet implements ResultSet,
        ResultSetCallbackInterface {
    //---------------------navigational members-----------------------------------

    public ClientStatement statement_;
    ClientStatement outerStatement_; // for auto-generated keys
    public ColumnMetaData resultSetMetaData_; // As obtained from the SQLDA
    private SqlWarning warnings_;
    public Cursor cursor_;
    /** Tracker object for LOB state, used to free locators on the server. */
    private LOBStateTracker lobState = null;
    protected Agent agent_;

    public Section generatedSection_ = null;

    private CloseFilterInputStream currentStream;
    private Reader currentReader;

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    // Alias for statement_.connection
    private final ClientConnection connection_;

    //----------------------------- constants ------------------------------------

    public final static int scrollOrientation_relative__ = 1;
    public final static int scrollOrientation_absolute__ = 2;
    public final static int scrollOrientation_after__ = 3;
    public final static int scrollOrientation_before__ = 4;
    private final static int scrollOrientation_prior__ = 5;
    private final static int scrollOrientation_first__ = 6;
    private final static int scrollOrientation_last__ = 7;
    private final static int scrollOrientation_current__ = 8;
    private final static int scrollOrientation_next__ = 0;

    public final static int sensitivity_unknown__ = 0;
    public final static int sensitivity_insensitive__ = 1;
    public final static int sensitivity_sensitive_static__ = 2;
    public final static int sensitivity_sensitive_dynamic__ = 3;

    static final private int WAS_NULL = 1;
    static final private int WAS_NOT_NULL = 2;
    static final private int WAS_NULL_UNSET = 0;

    //  determines if a cursor is a:
    //    Return to Client - not to be read by the stored procedure only by client
    //    Return to Caller
    private static final byte DDM_RETURN_CALLER = 0x01;
    private static final byte DDM_RETURN_CLIENT = 0x02;

    //-----------------------------state------------------------------------------

    // Note:
    //   Result set meta data as described by the SQLDA is described in ColumnMetaData.

    private int wasNull_ = WAS_NULL_UNSET;

    // ResultSet returnability for Stored Procedure cursors
    //  determines if a cursor is a:
    //    Return to Client - not to be read by the stored procedure only by client
    //    Return to Caller - only calling JSP can read it, not the client
    private byte rsReturnability_ = DDM_RETURN_CLIENT;

    // This means the client-side jdbc result set object is open.
    boolean openOnClient_ = true;
    // This means a server-side DERBY query section (cursor) for this result set is in the open state.
    // A jdbc result set may remain open even after the server has closed its cursor
    // (openOnClient=true, openOnServer=false); this is known as the "close-only" state.
    public boolean openOnServer_ = true;

    // there is a query terminating sqlca returned from the server when the server closes
    // it's cursor and the client moves to the close-only state.
    public Sqlca queryTerminatingSqlca_;

    // Only true for forward cursors after next() returns false (+100).
    // Used to prevent multiple commits for subsequent next() calls.
    boolean autoCommitted_ = false;

    // Before the first call to next() or any cursor positioning method, the cursor position is invalid
    // and getter methods cannot be called.
    // Also, if a cursor is exhausted (+100), the cursor position is invalid.
    private boolean isValidCursorPosition_ = false;
    private boolean savedIsValidCursorPosition_ = false;

    public boolean cursorHold_;

    // query instance identifier returned on open by uplevel servers.
    // this value plus the package information uniquely identifies a query.
    // it is 64 bits long and it's value is unarchitected.
    public long queryInstanceIdentifier_ = 0;

    public int resultSetType_;
    int resultSetConcurrency_;
    int resultSetHoldability_;
    public boolean scrollable_ = false;
    public int sensitivity_;
    public boolean isRowsetCursor_ = false;
    private boolean isBeforeFirst_ = true;
    private boolean isAfterLast_ = false;
    private boolean isFirst_ = false;
    private boolean isLast_ = false;
    public Sqlca[] rowsetSqlca_;

    // Gets its initial value from the statement when the result set is created.
    // It can be modified by setFetchSize and retrieved via getFetchSize.
    protected int suggestedFetchSize_;

    // Set by the net layer based on suggestedFetchSize_, protocol
    // type, scrollability and presence of lobs.
    public int fetchSize_;

    private int fetchDirection_;

    private long rowCount_ = -1;

    private long absolutePosition_ = 0; // absolute position of the current row
    private long firstRowInRowset_ = 0; // absolute position of the first row
                                        // in the current rowset
    private long lastRowInRowset_ = 0;  // absolute position of the last row in
                                        // the current rowset
    private long currentRowInRowset_ = -1; // relative position to the first
                                           // row in the current rowsetwel

    private boolean isOnInsertRow_ = false;  // reserved for later
    private boolean isOnCurrentRow_ = true;
    public int rowsReceivedInCurrentRowset_ = 0;  // keep track of the number of rows received in the
    // current rowset so far

    // maybe be able to consolidate with rowsReceivedInCurrentRowset_
    // Could use the rowsReceivedInCurrentRowset_ flag. But since we are going to set it to the
    // fetchSize and decrement it each time we successfully receiveds a row, the name will be confusing.
    // Fetch size can be changed in the middle of a rowset, and since we don't pre-parse all the rows \
    // for forward-only cursors like we do for scrollable cursors, we will lose the original fetchSize
    // when it's reset.  By decrementing rowsYetToBeReceivedInRowset_, when we come across a fetch
    // request, if rowsYetToBeReceivedInRowset_ is 0, then we can fetch using the "new" fetchSize,
    // otherwise, we will use rowsYetToBeReceivedInRowset_ to complete the rowset.
    public int rowsYetToBeReceivedForRowset_ = 0; // keep track of the number of rows still need to
    // be received to complete the rowset

    private Object updatedColumns_[];

    // Keeps track of whether a column has been updated.  If a column is updated to null,
    // the object array updatedColumns_ entry is null, and we will use this array to distinguish
    // between column not updated and column updated to null.
    private boolean columnUpdated_[];

    private ClientPreparedStatement preparedStatementForUpdate_;
    private ClientPreparedStatement preparedStatementForDelete_;
    private ClientPreparedStatement preparedStatementForInsert_;

    // Whenever a commit occurs, it unpositions the cursor on the server.  We need to
    // reposition the cursor before updating/deleting again.  This flag will be set to true
    // whenever a commit happens, and reset to false again after we repositoin the cursor.
    private boolean cursorUnpositionedOnServer_ = false;
    
    // Keep maxRows in the ResultSet, so that changes to maxRow in the statement
    // do not affect the resultSet after it has been created
    private long maxRows_;

    /**
     * Indicates which columns have been fetched as a stream or as a LOB for a
     * row. Created on-demand by a getXXXStream or a get[BC]lob call. Note that
     * we only track columns that can be accessed as a stream or a LOB object.
     */
    private boolean[] columnUsedFlags_;
    
    //---------------------constructors/finalizer---------------------------------

    protected ClientResultSet(Agent agent,
                        ClientStatement statement,
                        Cursor cursor,
                        int resultSetType,
                        int resultSetConcurrency,
                        int resultSetHoldability) {
        agent_ = agent;
        statement_ = statement;
        connection_ = statement_.connection_;
        cursor_ = cursor;
        if (cursor_ != null) {
            cursor_.maxFieldSize_ = statement_.maxFieldSize_;
        }
        resultSetType_ = resultSetType;
        resultSetConcurrency_ = resultSetConcurrency;
        resultSetHoldability_ = resultSetHoldability;
        fetchDirection_ = statement_.fetchDirection_;
        suggestedFetchSize_ = statement_.fetchSize_;

        maxRows_ = statement_.maxRows_;
        
        // Only set the warning if actual resultSetType returned by the server is less
        // than the application requested resultSetType.
        // TYPE_FORWARD_ONLY = 1003
        // TYPE_SCROLL_INSENSITIVE = 1004
        // TYPE_SCROLL_SENSITIVE = 1005
        if (resultSetType_ < statement_.resultSetType_) {
            statement_.accumulateWarning(
                new SqlWarning(
                    agent_.logWriter_, 
                    new ClientMessageId(SQLState.INVALID_RESULTSET_TYPE),
                        statement_.resultSetType_, resultSetType_));
        }

        // Only set the warning if actual resultSetConcurrency returned by the server is
        // less than the application requested resultSetConcurrency.
        // CONCUR_READ_ONLY = 1007
        // CONCUR_UPDATABLE = 1008
        if (resultSetConcurrency_ < statement_.resultSetConcurrency_) {
            accumulateWarning(
                new SqlWarning(
                    agent_.logWriter_,
                    new ClientMessageId(
                    SQLState.QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET)));
                
        }

        listenToUnitOfWork();
    }

    // ---------------------------jdbc 1------------------------------------------

    public final boolean next() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "next");
                }
                boolean isValidCursorPosition = nextX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "next", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // used by DBMD
    boolean nextX() throws SqlException {
        checkForClosedResultSet("next");
        clearWarningsX();

        moveToCurrentRowX();
        
        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();

        unuseStreamsAndLOBs();

        // for TYPE_FORWARD_ONLY ResultSet, just call cursor.next()
        if (resultSetType_ == ResultSet.TYPE_FORWARD_ONLY) {
            // cursor is null for singleton selects that do not return data.
            isValidCursorPosition_ = (cursor_ == null) ? false : cursor_.next();

            // for forward-only cursors, if qryrowset was specificed on OPNQRY or EXCSQLSTT,
            // then we must count the rows returned in the rowset to make sure we received a
            // complete rowset.  if not, we need to complete the rowset on the next fetch.
            if (fetchSize_ != 0) {
                if (rowsYetToBeReceivedForRowset_ == 0) {
                    rowsYetToBeReceivedForRowset_ = fetchSize_;
                }
                if (isValidCursorPosition_) {
                    rowsYetToBeReceivedForRowset_--;
                }
            }

            // Auto-commit semantics for exhausted cursors follows.
            // From Connection.setAutoCommit() javadoc:
            //   The commit occurs when the statement completes or the next execute occurs, whichever comes first.
            //   In the case of statements returning a ResultSet object, the statement completes when the
            //   last row of the ResultSet object has been retrieved or the ResultSet object has been closed.
            //   In advanced cases, a single statement may return multiple results as well as output parameter values.
            //   In these cases, the commit occurs when all results and output parameter values have been retrieved.
            // we will check to see if the forward only result set has gone past the end,
            // we will close the result set, the autocommit logic is in the closeX() method
            //
            //Aug 24, 2005: Auto-commit logic is no longer in the closeX() method. Insted it has been 
            //moved to Statement and is handled in a manner similar to the embedded driver.
//    if (!isValidCursorPosition_ && // We've gone past the end (+100)
//        cursor_ != null) {
            if ((!isValidCursorPosition_ && cursor_ != null) ||
                    (maxRows_ > 0 && cursor_.rowsRead_ > maxRows_)) {
                isValidCursorPosition_ = false;

                // if not on a valid row and the query is closed at the server.
                // check for an error which may have caused the cursor to terminate.
                // if there were no more rows because of an error, then this method
                // should throw an SqlException rather than just returning false.
                // depending on how this works with scrollable cursors, there may be
                // a better way/more common place for this logic.
                SqlException sqlException = null;
                if (!openOnServer_) {
                    int sqlcode = Utils.getSqlcodeFromSqlca(queryTerminatingSqlca_);
                    if (sqlcode > 0 && sqlcode != 100) {
                        accumulateWarning(new SqlWarning(agent_.logWriter_, queryTerminatingSqlca_));
                    } else if (sqlcode < 0) {
                        sqlException = new SqlException(agent_.logWriter_, queryTerminatingSqlca_);
                    }                    
                }
            
                try {
                    statement_.resultSetCommitting(this);
                } catch (SqlException sqle) {
                    sqlException = Utils.accumulateSQLException(sqle, sqlException);
                }
                
                if (sqlException != null)
                    throw sqlException;
            }
        }

        // for scrollable ResultSet's,
        // if the "next" request is still fetching within the current rowset,
        //   update column info from cache and increment the current row index
        // else
        //   fetch the next rowset from the server
        else {
            // These flags will only be used for dynamic cursors where we don't know the row count
            // and can't keep track of the absolute position of the cursor.
            isAfterLast_ = false;
            isLast_ = false;

            // if the next row is still within the current rowset
            if (rowIsInCurrentRowset(firstRowInRowset_ + currentRowInRowset_ + 1, scrollOrientation_next__)) {
                isValidCursorPosition_ = true;
                currentRowInRowset_++;
            } else {
                checkAndThrowReceivedQueryTerminatingException();
                isValidCursorPosition_ = getNextRowset();
            }

            if (isValidCursorPosition_) {
                updateColumnInfoFromCache();
                // check if there is a non-null SQLCA for the current row for rowset cursors
                checkRowsetSqlca();
                if (isBeforeFirst_) {
                    isFirst_ = true;
                }
                isBeforeFirst_ = false;
            } else {
                isFirst_ = false;
                return isValidCursorPosition_;
            }
        }

        // for forward-only cursors, check if rowsRead_ > maxRows_.
        // for scrollable cursors, check if absolute row number > maxRows_.
        // maxRows_ will be ignored by sensitive dynamic cursors since we don't know the rowCount
        if (!openOnClient_) {
            isValidCursorPosition_ = false;
        } else if (sensitivity_ != sensitivity_sensitive_dynamic__ && maxRows_ > 0 &&
                (firstRowInRowset_ + currentRowInRowset_ > maxRows_)) {
            isValidCursorPosition_ = false;
        }
        return isValidCursorPosition_;
    }


    public void close() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "close");
                }
                closeX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // TO DO: when parseEndqryrm() notifies common w/ endQueryCloseOnlyEvent() we need to mark something
    // that we later check to drive a commit.
    // An untraced version of close()
    public final void closeX() throws SqlException {
        if (!openOnClient_) {
            return;
        }
        closeOpenStreams();
        // See if there are open locators on the current row, if valid.
        if (isValidCursorPosition_ && !isOnInsertRow_) {
            lobState.checkCurrentRow(cursor_);
        }
        // NOTE: The preClose_ method must also check for locators if
        //       prefetching of data is enabled for result sets containing LOBs.
        preClose_();
        try {
            if (openOnServer_) {
                flowCloseAndAutoCommitIfNotAutoCommitted();
            } else {
                statement_.resultSetCommitting(this);
            }
        } finally {
            markClosed(true);
        }

        if (statement_.openOnClient_ && statement_.isCatalogQuery_) {
            statement_.closeX();
        }
        
        nullDataForGC();
    }

    /** Close Statement if it is set to closeOnCompletion */
    private void    closeStatementOnCompletion()
    {
        statement_.closeMeOnCompletion();
        if ( (outerStatement_ != null) && (outerStatement_ != statement_) ) { outerStatement_.closeMeOnCompletion(); }
        outerStatement_ = null;
    }

    private void nullDataForGC() {
        // This method is called by closeX().  We cannot call this if cursor is cached,
        // otherwise it will cause NullPointerException's when cursor is reused.
        // Cursor is only cached for PreparedStatement's.
        if (cursor_ != null && !statement_.isPreparedStatement_) {
            cursor_.nullDataForGC();
        }
        cursor_ = null;
        resultSetMetaData_ = null;
    }

    private void flowCloseAndAutoCommitIfNotAutoCommitted()
            throws SqlException {
        agent_.beginWriteChain(statement_);
        boolean performedAutoCommit = writeCloseAndAutoCommit();
        agent_.flow(statement_);
        readCloseAndAutoCommit(performedAutoCommit);
        agent_.endReadChain();
    }

    private boolean writeCloseAndAutoCommit() throws SqlException {
        // set autoCommitted_ to false so commit will flow following
        // close cursor if autoCommit is true.
        autoCommitted_ = false;
        if (generatedSection_ == null) { // none call statement result set case
            writeCursorClose_(statement_.getSection());
        } else { // call statement result set(s) case
            writeCursorClose_(generatedSection_);
        }
        return statement_.resultSetCommitting(this, true);
    }

    private void readCloseAndAutoCommit(boolean readAutoCommit) throws SqlException {
        readCursorClose_();
        if (readAutoCommit) 
            readAutoCommitIfNotAutoCommitted();
    }

    void writeClose() throws SqlException {
        // set autoCommitted_ to false so commit will flow following
        // close cursor if autoCommit is true.
        autoCommitted_ = false;
        if (generatedSection_ == null) { // none call statement result set case
            writeCursorClose_(statement_.getSection());
        } else { // call statement result set(s) case
            writeCursorClose_(generatedSection_);
        }
    }

    void readClose() throws SqlException {
        try {
            if (generatedSection_ == null) { // none call statement result set case
                readCursorClose_();
            } else { // call statement result set(s) case
                readCursorClose_();
            }
        } finally {
            markClosed();
        }
    }

    private void readAutoCommitIfNotAutoCommitted() throws SqlException {
        if (connection_.autoCommit_ && !autoCommitted_) {
            connection_.readAutoCommit();
            markAutoCommitted();
        }
    }

    public boolean wasNull() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "wasNull");
            }
            checkForClosedResultSet("wasNull");

            if (wasNull_ == ClientResultSet.WAS_NULL_UNSET) {
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.WASNULL_INVALID));
            }

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(
                    this, "wasNull", wasNull_ == ClientResultSet.WAS_NULL);
            }
            return wasNull_ == ClientResultSet.WAS_NULL;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //------------------- getters on column index --------------------------------

    // Live life on the edge and run unsynchronized
    public boolean getBoolean(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBoolean", column);
            }
            checkGetterPreconditions(column, "getBoolean");
            boolean result = false;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = false;
                } else {
                    result = agent_.crossConverters_.setBooleanFromObject(
                            updatedColumns_[column - 1],
                            resultSetMetaData_.types_[column - 1]);
                }
            } else {
                result = isNull(column) ? false : cursor_.getBoolean(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBoolean", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public byte getByte(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getByte", column);
            }
            checkGetterPreconditions(column, "getByte");
            byte result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if ((isOnInsertRow_) && (updatedColumns_[column - 1] == null)) {
                    result = 0;
                } else {
                    result = agent_.crossConverters_.setByteFromObject(
                            updatedColumns_[column - 1],
                            resultSetMetaData_.types_[column - 1]);
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getByte(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getByte", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public short getShort(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getShort", column);
            }
            checkGetterPreconditions(column, "getShort");
            short result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Short) agent_.crossConverters_.setObject(
                            Types.SMALLINT,
                            updatedColumns_[column - 1])).shortValue();
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getShort(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getShort", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public int getInt(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getInt", column);
            }
            checkGetterPreconditions(column, "getInt");
            int result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Integer) agent_.crossConverters_.setObject(
                            Types.INTEGER,
                            updatedColumns_[column - 1])).intValue();
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getInt(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getInt", result);
            }
            setWasNull(column); // this is placed here close to the return to minimize risk of race condition.
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public long getLong(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getLong", column);
            }
            checkGetterPreconditions(column, "getLong");
            long result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Long) agent_.crossConverters_.setObject(
                            Types.BIGINT,
                            updatedColumns_[column - 1])).longValue();
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getLong(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getLong", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public float getFloat(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getFloat", column);
            }
            checkGetterPreconditions(column, "getFloat");
            float result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if ((isOnInsertRow_ && updatedColumns_[column - 1] == null)) {
                    result = 0;
                } else {
                    result = ((Float) agent_.crossConverters_.setObject(
                            Types.REAL,
                            updatedColumns_[column - 1])).floatValue();
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getFloat(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getFloat", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public double getDouble(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDouble", column);
            }
            checkGetterPreconditions(column, "getDouble");
            double result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Double) agent_.crossConverters_.setObject(
                            Types.DOUBLE,
                            updatedColumns_[column - 1])).doubleValue();
                }
            } else {
                result = isNull(column) ? 0 : cursor_.getDouble(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDouble", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    /** @deprecated */
    public BigDecimal getBigDecimal(int column, int scale) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getBigDecimal", column, scale);
            }
            checkGetterPreconditions(column, "getBigDecimal");
            BigDecimal result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = ((BigDecimal) agent_.crossConverters_.setObject(
                              Types.DECIMAL,
                              updatedColumns_[column - 1])).
                    setScale(scale, BigDecimal.ROUND_DOWN);
            } else {
                result = isNull(column) ? null :
                    cursor_.getBigDecimal(column).setScale(
                        scale, BigDecimal.ROUND_DOWN);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedExit(this, "getBigDecimal", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public BigDecimal getBigDecimal(int column) throws SQLException {
        try
        {

            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBigDecimal", column);
            }
            checkGetterPreconditions(column, "getBigDecimal");
            BigDecimal result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (BigDecimal)agent_.crossConverters_.setObject(
                    Types.DECIMAL,
                    updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getBigDecimal(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBigDecimal", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Date getDate(int column, Calendar cal) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", column);
            }
            checkGetterPreconditions(column, "getDate");

            if (cal == null) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CALENDAR_IS_NULL));
            }

            Date result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (Date)agent_.crossConverters_.setObject(
                    Types.DATE, updatedColumns_[column - 1]);
                // updateDate() doesn't take a calendar, so the retrieved
                // value will be in the default calendar. Convert it to
                // the requested calendar before returning it.
                result = convertFromDefaultCalendar(result, cal);
            } else {
                result = isNull(column) ? null : cursor_.getDate(column, cal);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDate", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Date getDate(int column) throws SQLException {
        return getDate(column, Calendar.getInstance());
    }

    // Live life on the edge and run unsynchronized
    public Time getTime(int column, Calendar cal) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", column, cal);
            }
            checkGetterPreconditions(column, "getTime");

            if (cal == null) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CALENDAR_IS_NULL));
            }

            Time result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (Time)agent_.crossConverters_.setObject(
                    Types.TIME, updatedColumns_[column - 1]);
                // updateTime() doesn't take a calendar, so the retrieved
                // value will be in the default calendar. Convert it to
                // the requested calendar before returning it.
                result = convertFromDefaultCalendar(result, cal);
            } else {
                result = isNull(column) ? null : cursor_.getTime(column, cal);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTime", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Time getTime(int column) throws SQLException {
        return getTime(column, Calendar.getInstance());
    }

    // Live life on the edge and run unsynchronized
    public Timestamp getTimestamp(int column, Calendar calendar)
            throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(
                        this, "getTimestamp", column, calendar);
            }
            checkGetterPreconditions(column, "getTimestamp");

            if (calendar == null) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CALENDAR_IS_NULL));
            }

            Timestamp result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (Timestamp)agent_.crossConverters_.setObject(
                    Types.TIMESTAMP, updatedColumns_[column - 1]);
                // updateTimestamp() doesn't take a calendar, so the retrieved
                // value will be in the default calendar. Convert it to
                // the requested calendar before returning it.
                result = convertFromDefaultCalendar(result, calendar);
            } else if (!isNull(column)) {
                result = cursor_.getTimestamp(column, calendar);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTimestamp", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Timestamp getTimestamp(int column) throws SQLException {
        return getTimestamp(column, Calendar.getInstance());
    }

    /**
     * Create a calendar with default locale and time zone.
     * @param date the initial time of the calendar
     * @return a calendar initialized to the specified time
     */
    private static Calendar createCalendar(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal;
    }

    /**
     * Convert a date originally set using the default calendar to a value
     * representing the same date in a different calendar.
     *
     * @param date the date to convert
     * @param cal the calendar to convert it to
     * @return a date object that represents the date in {@code cal}
     */
    private Date convertFromDefaultCalendar(Date date, Calendar cal) {
        Calendar from = createCalendar(date);
        cal.clear();
        cal.set(from.get(Calendar.YEAR),
                from.get(Calendar.MONTH),
                from.get(Calendar.DAY_OF_MONTH));
        return new Date(cal.getTimeInMillis());
    }

    /**
     * Convert a time originally set using the default calendar to a value
     * representing the same time in a different calendar.
     *
     * @param time the time to convert
     * @param cal the calendar to convert it to
     * @return a time object that represents the time in {@code cal}
     */
    private Time convertFromDefaultCalendar(Time time, Calendar cal) {
        Calendar from = createCalendar(time);
        cal.clear();
        cal.set(1970, Calendar.JANUARY, 1, // normalized date: 1970-01-01
                from.get(Calendar.HOUR_OF_DAY),
                from.get(Calendar.MINUTE),
                from.get(Calendar.SECOND));
        return new Time(cal.getTimeInMillis());
    }

    /**
     * Convert a timestamp originally set using the default calendar to a value
     * representing the same timestamp in a different calendar.
     *
     * @param ts the timestamp to convert
     * @param cal the calendar to convert it to
     * @return a timestamp object that represents the timestamp in {@code cal}
     */
    private Timestamp convertFromDefaultCalendar(Timestamp ts, Calendar cal) {
        Calendar from = createCalendar(ts);
        cal.clear();
        cal.set(from.get(Calendar.YEAR),
                from.get(Calendar.MONTH),
                from.get(Calendar.DAY_OF_MONTH),
                from.get(Calendar.HOUR_OF_DAY),
                from.get(Calendar.MINUTE),
                from.get(Calendar.SECOND));

        Timestamp result = new Timestamp(cal.getTimeInMillis());
        result.setNanos(ts.getNanos());
        return result;
    }

    // Live life on the edge and run unsynchronized
    public String getString(int column) throws SQLException {
        try
        {
            closeOpenStreams();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getString", column);
            }
            checkGetterPreconditions(column, "getString");
            int type = resultSetMetaData_.types_[column - 1];
            if (type == ClientTypes.BLOB || type == ClientTypes.CLOB) {
                checkLOBMultiCall(column);
                // If the above didn't fail, this is the first getter
                // invocation, or only getBytes and/or getString have been
                // invoked previously. The special treatment of these getters
                // is allowed for backwards compatibility.
            }
            String result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (String)agent_.crossConverters_.setObject(
                    Types.CHAR, updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getString(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getString", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public byte[] getBytes(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBytes", column);
            }
            checkGetterPreconditions(column, "getBytes");
            int type = resultSetMetaData_.types_[column - 1];
            if (type == ClientTypes.BLOB) {
                checkLOBMultiCall(column);
                // If the above didn't fail, this is the first getter
                // invocation, or only getBytes has been invoked previously.
                // The special treatment of this getter is allowed for
                // backwards compatibility.
            }
            byte[] result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (byte[])agent_.crossConverters_.setObject(
                    Types.BINARY, updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getBytes(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBytes", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public InputStream getBinaryStream(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBinaryStream", column);
            }

            checkGetterPreconditions(column, "getBinaryStream");
            useStreamOrLOB(column);

            InputStream result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = new ByteArrayInputStream(
                    (byte[])agent_.crossConverters_.setObject(
                        Types.BINARY, updatedColumns_[column - 1]));
            } else {
                result = isNull(column) ? null : cursor_.getBinaryStream(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBinaryStream", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return createCloseFilterInputStream(result);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public InputStream getAsciiStream(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getAsciiStream", column);
            }

            checkGetterPreconditions(column, "getAsciiStream");
            useStreamOrLOB(column);

            InputStream result = null;
            if (wasNonNullSensitiveUpdate(column)) {

            result = new AsciiStream(
                (String)agent_.crossConverters_.setObject(
                    Types.CHAR, updatedColumns_[column - 1]));
            } else {
                result = isNull(column) ? null : cursor_.getAsciiStream(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getAsciiStream", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return createCloseFilterInputStream(result);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /**
     * Retrieve the value of the specified column as a stream of two-byte
     * Unicode characters. Deprecated in JDBC 2.0 and this method will just
     * throw a feature not implemented exception.
     *
     * @param column the column to retrieve as a Unicode stream
     * @exception SQLException throws feature not implemented
     * @deprecated
     */
    public InputStream getUnicodeStream(int column) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceDeprecatedEntry(this, "getUnicodeStream",
                                                   column);
        }

        throw SQLExceptionFactory.notImplemented ("getUnicodeStream");
    }

    // Live life on the edge and run unsynchronized
    public Reader getCharacterStream(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream", column);
            }

            checkGetterPreconditions(column, "getCharacterStream");
            useStreamOrLOB(column);

            Reader result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = new StringReader
                    ((String)agent_.crossConverters_.setObject(
                        Types.CHAR, updatedColumns_[column - 1]));
            } else {
                result = isNull(column) ? null : cursor_.getCharacterStream(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getCharacterStream", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            currentReader = result;
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Blob getBlob(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBlob", column);
            }
            checkGetterPreconditions(column, "getBlob");
            useStreamOrLOB(column);
            Blob result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (Blob) agent_.crossConverters_.setObject(Types.BLOB,
                        updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getBlob(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBlob", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Clob getClob(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getClob", column);
            }
            checkGetterPreconditions(column, "getClob");
            useStreamOrLOB(column);
            Clob result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (Clob) agent_.crossConverters_.setObject(Types.CLOB,
                        updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getClob(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getClob", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Ref getRef(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getRef", column);
            }
            checkGetterPreconditions(column, "getRef");
            Ref result = isNull(column) ? null : cursor_.getRef(column);
            if (true) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED));
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getRef", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Array getArray(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getArray", column);
            }
            checkGetterPreconditions(column, "getArray");
            Array result = isNull(column) ? null : cursor_.getArray(column);
            if (true) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED));
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getArray", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public Object getObject(int column) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", column);
            }
            Object result = getObjectX(column);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getObject", result);
            }
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // used by DBMD
    Object getObjectX(int column) throws SqlException {
        checkGetterPreconditions(column, "getObject");
        int type = resultSetMetaData_.types_[column - 1];
        if (type == ClientTypes.BLOB || type == ClientTypes.CLOB) {
            useStreamOrLOB(column);
        }
        Object result = null;
        if (wasNonNullSensitiveUpdate(column)) {
            result = updatedColumns_[column - 1];
        } else {
            result = isNull(column) ? null : cursor_.getObject(column);
        }
        setWasNull(column);  // Placed close to the return to minimize risk of thread interference
        return result;
    }

    // Live life on the edge and run unsynchronized
    public Object getObject(int column, Map map) throws SQLException {
        try
        {
            closeOpenStreams();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", column, map);
            }
            checkGetterPreconditions(column, "getObject");
            Object result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = updatedColumns_[column - 1];
            } else {
                result = isNull(column) ? null : cursor_.getObject(column);
            }
            if (true) {
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED));
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getObject", result);
            }
            setWasNull(column);  // Placed close to the return to minimize risk of thread interference
            return result;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //----------------------------------------------------------------------------

    // This method only returns true if there is a new non-null updated value.
    // If the resultset is updatable, sensitive, and updated, return the new non-null updated value.
    // Otherwise this method will return false.
    // If the column is updated to null, or if the column has not been update but is null,
    // a null will be returned by isNull(), which first calls wasNullSensitiveUpdate() to check for a column
    // that is updated to null, and columnUpdated_ is checked there.
    private boolean wasNonNullSensitiveUpdate(int column) {
        return
                updatedColumns_ != null &&
                updatedColumns_[column - 1] != null;
    }

    // if updatedColumns_ entry is null, but columnUpdated_ entry
    // indicates column has been updated, then column is updated to null.
    private boolean wasNullSensitiveUpdate(int column) {
        return
                updatedColumns_ != null &&
                updatedColumns_[column - 1] == null &&
                columnUpdated_[column - 1];
    }

    private void setWasNull(int column) {
        if (wasNullSensitiveUpdate(column) || (isOnInsertRow_ && updatedColumns_[column - 1] == null)) {
            wasNull_ = WAS_NULL;
        } else {
            wasNull_ = (cursor_.isNull_ == null || cursor_.isNull_[column - 1]) ? WAS_NULL : WAS_NOT_NULL;
        }
    }

    private boolean isNull(int column) {
        if (wasNullSensitiveUpdate(column)) {
            return true;
        } else {
            return (cursor_.isUpdateDeleteHole_ == true || cursor_.isNull_[column - 1]);
        }
    }

    // ------------- Methods for accessing results by column name ----------------

    public final boolean getBoolean(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBoolean", columnName);
            }
            return getBoolean(findColumnX(columnName, "getBoolean"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final byte getByte(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getByte", columnName);
            }
            return getByte(findColumnX(columnName, "getByte"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final short getShort(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getShort", columnName);
            }
            return getShort(findColumnX(columnName, "getShort"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final int getInt(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getInt", columnName);
            }
            return getInt(findColumnX(columnName, "getInt"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final long getLong(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getLong", columnName);
            }
            return getLong(findColumnX(columnName, "getLong"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final float getFloat(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getFloat", columnName);
            }
            return getFloat(findColumnX(columnName, "getFloat"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final double getDouble(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDouble", columnName);
            }
            return getDouble(findColumnX(columnName, "getDouble"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /** @deprecated */
    public final BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getBigDecimal", columnName, scale);
            }
            return getBigDecimal(findColumnX(columnName, "getBigDecimal"), scale);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final BigDecimal getBigDecimal(String columnName)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBigDecimal", columnName);
            }
            return getBigDecimal(findColumnX(columnName, "getBigDecimal"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Date getDate(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", columnName);
            }
            return getDate(findColumnX(columnName, "getDate"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Date getDate(String columnName, Calendar cal)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", columnName, cal);
            }
            return getDate(findColumnX(columnName, "getDate"), cal);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Time getTime(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", columnName);
            }
            return getTime(findColumnX(columnName, "getTime"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Time getTime(String columnName, Calendar cal)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", columnName, cal);
            }
            return getTime(findColumnX(columnName, "getTime"), cal);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Timestamp getTimestamp(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", columnName);
            }
            return getTimestamp(findColumnX(columnName, "getTimestamp"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", columnName, cal);
            }
            return getTimestamp(findColumnX(columnName, "getTimestamp"), cal);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final String getString(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getString", columnName);
            }
            return getString(findColumnX(columnName, "getString"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final byte[] getBytes(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBytes", columnName);
            }
            return getBytes(findColumnX(columnName, "getBytes"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final InputStream getBinaryStream(String columnName)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBinaryStream", columnName);
            }
            return getBinaryStream(findColumnX(columnName, "getBinaryStream"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final InputStream getAsciiStream(String columnName)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getAsciiStream", columnName);
            }
            return getAsciiStream(findColumnX(columnName, "getAsciiStream"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /** @deprecated */
    public final InputStream getUnicodeStream(String columnName)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getUnicodeStream", columnName);
            }
            return getUnicodeStream(findColumnX(columnName, "getUnicodeStream"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Reader getCharacterStream(String columnName)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream", columnName);
            }
            return getCharacterStream(findColumnX(columnName, "getCharacterStream"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Blob getBlob(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBlob", columnName);
            }
            return getBlob(findColumnX(columnName, "getBlob"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Clob getClob(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getClob", columnName);
            }
            return getClob(findColumnX(columnName, "getClob"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Array getArray(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getArray", columnName);
            }
            return getArray(findColumnX(columnName, "getArray"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Ref getRef(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getRef", columnName);
            }
            return getRef(findColumnX(columnName, "getRef"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Object getObject(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", columnName);
            }
            return getObject(findColumnX(columnName, "getObject"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Object getObject(String columnName, Map map)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", columnName, map);
            }
            return getObject(findColumnX(columnName, "getObject"), map);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // ----------------Advanced features -----------------------------------------

    /**
     * Returns the first <code>SQLWarning</code> reported on this
     * <code>ResultSet</code> object, or <code>null</code> if there
     * are no warnings. Subsequent warnings are chained on the
     * returned object.
     *
     * @return the first <code>SQLWarning</code> in the chain, or
     * <code>null</code> if no warnings are reported
     * @exception SQLException if a database error occurs or the
     * result set is closed
     */
    public final SQLWarning getWarnings() throws SQLException {
        try {
            checkForClosedResultSet("getWarnings");
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getWarnings", warnings_);
        }
        return warnings_ == null ? null : warnings_.getSQLWarning();
    }

    /**
     * Clear all warnings on this <code>ResultSet</code> and make
     * subsequent calls to <code>getWarnings()</code> return
     * <code>null</code> until a new warning is reported.
     *
     * @exception SQLException if a database error occurs or the
     * result set is closed
     */
    public final void clearWarnings() throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "clearWarnings");
            }
            try {
                checkForClosedResultSet("clearWarnings");
            } catch (SqlException se) {
                throw se.getSQLException();
            }
            clearWarningsX();
        }
    }

    // An untraced version of clearWarnings()
    private void clearWarningsX() {
        warnings_ = null;
    }

    public String getCursorName() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getCursorName");
                }
                checkForClosedResultSet("getCursorName");
                if (generatedSection_ != null) {
                    return "stored procedure generated cursor:" + generatedSection_.getServerCursorName();
                }
                if (statement_.cursorName_ == null) {// cursor name is not assigned yet
                    statement_.cursorName_ = statement_.getSection().getServerCursorName();
                }
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getCursorName", statement_.cursorName_);
                }
                return statement_.cursorName_;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getMetaData");
            }

            ResultSetMetaData resultSetMetaData = getMetaDataX();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getMetaData", resultSetMetaData);
            }
            return resultSetMetaData;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // used by DBMD
    ColumnMetaData getMetaDataX() throws SqlException {
        checkForClosedResultSet("getMetaData");
        return resultSetMetaData_;
    }


    public final int findColumn(String columnName) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "findColumn", columnName);
                }
                int column = findColumnX(columnName, "findColumn");
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "findColumn", column);
                }
                return column;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // An untraced version of findColumn()
    protected final int findColumnX(String columnName, String operation) throws SqlException {
        checkForClosedResultSet(operation);
        return resultSetMetaData_.findColumnX(columnName);
    }

    //-------------------------- Traversal/Positioning ---------------------------

    public boolean isBeforeFirst() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "isBeforeFirst");
            }
            checkForClosedResultSet("isBeforeFirst");
            checkThatResultSetTypeIsScrollable();
            // Returns false if the ResultSet contains no rows.
            boolean isBeforeFirst = isBeforeFirstX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "isBeforeFirst", isBeforeFirst);
            }
            return isBeforeFirst;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean isBeforeFirstX() throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return isBeforeFirst_;
        } else
        //return ((resultSetContainsNoRows()) ? false : (currentRowInRowset_ == -1));
        {
            return ((currentRowInRowset_ == -1) && !resultSetContainsNoRows());
        }
    }

    public boolean isAfterLast() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "isAfterLast");
            }
            checkForClosedResultSet("isAfterLast");
            checkThatResultSetTypeIsScrollable();
            // Returns false if the ResultSet contains no rows.
            boolean isAfterLast = isAfterLastX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "isAfterLast", isAfterLast);
            }
            return isAfterLast;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean isAfterLastX() throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return isAfterLast_;
        } else {
            return (resultSetContainsNoRows() ? false :
                    (firstRowInRowset_ == currentRowInRowset_ &&
                    currentRowInRowset_ == lastRowInRowset_ &&
                    lastRowInRowset_ == 0 &&
                    absolutePosition_ == (maxRows_ == 0 ? rowCount_ + 1 : maxRows_ + 1)));
        }
    }

    public boolean isFirst() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "isFirst");
            }
            checkForClosedResultSet("isFirst");
            checkThatResultSetTypeIsScrollable();
            // Not necessary to get the rowCount_ since currentRowInRowset_ is initialized to -1,
            // and it will not be changed if there is no rows in the ResultSet.
            boolean isFirst = isFirstX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "isFirst", isFirst);
            }
            return isFirst;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean isFirstX() {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return isFirst_;
        }
        return (firstRowInRowset_ == 1 && currentRowInRowset_ == 0);
    }

    public boolean isLast() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "isLast");
            }
            checkForClosedResultSet("isLast");
            checkThatResultSetTypeIsScrollable();
            // Returns false if the ResultSet contains no rows.
            boolean isLast = isLastX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "isLast", isLast);
            }
            return isLast;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean isLastX() throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return isLast_;
        } else {
            return (resultSetContainsNoRows() ? false :
                    (firstRowInRowset_ + currentRowInRowset_) == rowCount_);
        }
    }

    public void beforeFirst() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "beforeFirst");
                }
                checkForClosedResultSet("beforeFirst");
                checkThatResultSetTypeIsScrollable();
                clearWarningsX();
                beforeFirstX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void beforeFirstX() throws SqlException {
        
        resetRowsetFlags();
        unuseStreamsAndLOBs();

        moveToCurrentRowX();

        // this method has no effect if the result set has no rows.
        // only send cntqry to position the cursor before first if
        // resultset contains rows and it is not already before first, or
        // if the cursor is a dynamic cursor.
        if (sensitivity_ == sensitivity_sensitive_dynamic__ ||
                (!resultSetContainsNoRows() && !isServersCursorPositionBeforeFirst())) {
            moveToBeforeFirst();
        }
        isBeforeFirst_ = true;
        setRowsetBeforeFirstEvent();
        cursor_.resetDataBuffer();
        resetRowsetSqlca();
        isValidCursorPosition_ = false;
    }

    public void afterLast() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "afterLast");
                }
                checkForClosedResultSet("afterLast");
                checkThatResultSetTypeIsScrollable();
                clearWarningsX();
                afterLastX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void afterLastX() throws SqlException {
        resetRowsetFlags();
        unuseStreamsAndLOBs();
    
        moveToCurrentRowX();

        // this method has no effect if the result set has no rows.
        // only send cntqry to position the cursor after last if
        // resultset contains rows and it is not already after last, or
        // if the cursor is a dynamic cursor.
        if (sensitivity_ == sensitivity_sensitive_dynamic__ ||
                (!resultSetContainsNoRows() && !isServerCursorPositionAfterLast())) {
            moveToAfterLast();
        }
        isAfterLast_ = true;
        setRowsetAfterLastEvent();
        cursor_.resetDataBuffer();
        resetRowsetSqlca();
        isValidCursorPosition_ = false;
    }

    public boolean first() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "first");
                }
                boolean isValidCursorPosition = firstX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "first", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean firstX() throws SqlException {
        checkForClosedResultSet("first");
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();
        
        moveToCurrentRowX();

        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();

        resetRowsetFlags();
        unuseStreamsAndLOBs();

        // if first row is not in the current rowset, fetch the first rowset from the server.
        // rowIsInCurrentRowset with orientation first will always return false for dynamic cursors.
        if (rowIsInCurrentRowset(1, scrollOrientation_first__)) {
            isValidCursorPosition_ = true;
            currentRowInRowset_ = 0;
        } else {
            checkAndThrowReceivedQueryTerminatingException();
            isValidCursorPosition_ = getFirstRowset();
        }

        if (isValidCursorPosition_) {
            updateColumnInfoFromCache();
            isFirst_ = true;
            // check if there is a non-null SQLCA for the row for rowset cursors
            checkRowsetSqlca();
        }

        return isValidCursorPosition_;
    }

    public boolean last() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "last");
                }
                boolean isValidCursorPosition = lastX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "last", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean lastX() throws SqlException {
        checkForClosedResultSet("last");
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();
        
        moveToCurrentRowX();

        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();

        resetRowsetFlags();
        unuseStreamsAndLOBs();

        // only get the rowCount for static cursors.
        if (rowCountIsUnknown()) {
            getRowCount();
        }
        long row = rowCount_;
        if (sensitivity_ != sensitivity_sensitive_dynamic__ && maxRows_ > 0) {
            if (rowCount_ > maxRows_) {
                row = maxRows_;
            }
        }

        // rowIsInCurrentRowset with orientation last will always return false for dynamic cursors.
        if (rowIsInCurrentRowset(row, scrollOrientation_last__)) {
            isValidCursorPosition_ = true;
            currentRowInRowset_ = row - firstRowInRowset_;
        } else {
            checkAndThrowReceivedQueryTerminatingException();
            isValidCursorPosition_ = getLastRowset(row);
        }

        if (isValidCursorPosition_) {
            updateColumnInfoFromCache();
            isLast_ = true;
            // check if there is a non-null SQLCA for the current row for rowset cursors
            checkRowsetSqlca();
        }

        return isValidCursorPosition_;
    }

    public int getRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getRow");
                }
                int row = getRowX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getRow", row);
                }
                return row;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private int getRowX() throws SqlException {
        checkForClosedResultSet("getRow");
        long row;
        checkThatResultSetIsNotDynamic();
        if (resultSetType_ == ResultSet.TYPE_FORWARD_ONLY)
        // for forward-only cursors, getRow() should return 0 if cursor is not on a valid row,
        // i.e. afterlast.
        {
            row = (cursor_.allRowsReceivedFromServer() &&
                    cursor_.currentRowPositionIsEqualToNextRowPosition()) ? 0 : cursor_.rowsRead_;
        } else {
            if (rowCountIsUnknown()) {
                // commented out here because the following method is called the first thing
                // inside getRowCount();
                //checkAndThrowReceivedQueryTerminatingException();
                getRowCount();
            }
            if (rowCount_ == 0 || currentRowInRowset_ < 0) // || currentRowInRowset_ > rowCount_)
            {
                row = 0;
            } else {
                row = firstRowInRowset_ + currentRowInRowset_;
            }
        }
        if (row > Integer.MAX_VALUE) {
            this.accumulateWarning(new SqlWarning(agent_.logWriter_, 
                new ClientMessageId(SQLState.NUMBER_OF_ROWS_TOO_LARGE_FOR_INT),
                row));
        }
        return (int) row;
    }

    public boolean absolute(int row) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "absolute", row);
                }
                boolean isValidCursorPosition = absoluteX(row);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "absolute", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean absoluteX(int row) throws SqlException {
        checkForClosedResultSet("absolute");
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();

        moveToCurrentRowX();

        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();

        resetRowsetFlags();
        unuseStreamsAndLOBs();

        if (maxRows_ > 0) {
            // if "row" is positive and > maxRows, fetch afterLast
            // else if "row" is negative, and abs(row) > maxRows, fetch beforeFirst
            if (row > 0 && row > maxRows_) {
                afterLastX();
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            } else if (row <= 0 && Math.abs(row) > maxRows_) {
                beforeFirstX();
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            }
        }

        int fetchAbsoluteRow = 0;
        if (rowCountIsUnknown()) {
            getRowCount();
        }
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            fetchAbsoluteRow = row;
        } else
        // calculate the positive absolute row number based on rowCount for static or insensitive cursors.
        {
            fetchAbsoluteRow = (row >= 0) ? row : (int) (rowCount_ + row + 1);
        }

        // rowIsInCurrentRowset with orientation absolute will always return false for dynamic cursors.
        if (rowIsInCurrentRowset(fetchAbsoluteRow, scrollOrientation_absolute__)) {
            isValidCursorPosition_ = true;
            currentRowInRowset_ = fetchAbsoluteRow - firstRowInRowset_;
        } else {
            checkAndThrowReceivedQueryTerminatingException();
            isValidCursorPosition_ = getAbsoluteRowset(fetchAbsoluteRow);
        }

        if (isValidCursorPosition_) {
            updateColumnInfoFromCache();
            if (row == 1) {
                isFirst_ = true;
            }
            if (row == -1) {
                isLast_ = true;
            }
            // check if there is a non-null SQLCA for the row for rowset cursors
            checkRowsetSqlca();
        }

        return isValidCursorPosition_;
    }

    public boolean relative(int rows) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "relative", rows);
                }
                boolean isValidCursorPosition = relativeX(rows);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "relative", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean relativeX(int rows) throws SqlException {
        checkForClosedResultSet("relative");
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();
        
        moveToCurrentRowX();
        
        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();
        unuseStreamsAndLOBs();

        // If the resultset is empty, relative(n) is a null operation
        if (resultSetContainsNoRows()) {
            isValidCursorPosition_ = false;
            return isValidCursorPosition_;
        }
        
        // relative(0) is a null-operation, but the retruned result is
        // dependent on wether the cursorposition is on a row or not.
        // Scroll insensitive updatable should see own changes, so relative(0)
        // has to refetch the row.
        if (rows == 0) {
            if (resultSetConcurrency_ == ResultSet.CONCUR_UPDATABLE &&
                resultSetType_ == ResultSet.TYPE_SCROLL_INSENSITIVE) {
                // re-fetch currentRow
                isValidCursorPosition_ = getAbsoluteRowset(absolutePosition_);
            } else {
                if (isBeforeFirstX() || isAfterLastX()) {
                    isValidCursorPosition_ = false;
                } else {
                    isValidCursorPosition_ = true;
                }
            }
            return isValidCursorPosition_;
        }

        // Handle special cases when the cursor is before first or
        // after last, since the following code assumes we ar on a
        // valid cursor
        if (isBeforeFirstX()) {
            if (rows > 0) {
                nextX();
                return relativeX(rows-1);
            } else {
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            }
        }
        if (isAfterLastX()) {
            if (rows < 0) {
                previousX();
                return relativeX(rows+1);
            } else {
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            }
        }
        // Ok, now we are on a row and ready to do some real positioning.....

        resetRowsetFlags();

        // currentAbsoluteRowNumber is used for static cursors only.
        long currentAbsoluteRowNumber = firstRowInRowset_ + currentRowInRowset_;

        // if "rows" is positive, and currentRow+rows > maxRows, fetch afterLast.
        // if "rows" is negative, and if the absolute value of "rows" is greater than
        // the currentrow number, will fetch beforeFirst anyways.  do not need to check
        // for maxRows.
        if (sensitivity_ != sensitivity_sensitive_dynamic__ &&
                maxRows_ > 0 && rows > 0 && currentAbsoluteRowNumber + rows > maxRows_) {
            afterLastX();
            isValidCursorPosition_ = false;
            return isValidCursorPosition_;
        }

        if (rowIsInCurrentRowset(currentAbsoluteRowNumber + rows, scrollOrientation_relative__)) {
            currentRowInRowset_ += rows;
            isValidCursorPosition_ = true;
        } else {
            checkAndThrowReceivedQueryTerminatingException();
            long rowNumber =
                    (sensitivity_ == sensitivity_sensitive_dynamic__) ? currentRowInRowset_ + rows :
                    currentAbsoluteRowNumber + rows - absolutePosition_;
            if (maxRows_ < Math.abs(rowNumber) && maxRows_ != 0) {
                if (rowNumber > 0) {
                    afterLastX();
                } else {
                    beforeFirstX();
                }
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            }
            isValidCursorPosition_ = getRelativeRowset(rowNumber);
        }

        if (isValidCursorPosition_) {
            updateColumnInfoFromCache();
            // check if there is a non-null SQLCA for the row for rowset cursors
            checkRowsetSqlca();
        }

        return isValidCursorPosition_;
    }

    public boolean previous() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "previous");
                }
                boolean isValidCursorPosition = previousX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "previous", isValidCursorPosition);
                }
                return isValidCursorPosition;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean previousX() throws SqlException {
        checkForClosedResultSet("previous");
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();
        
        moveToCurrentRowX();

        wasNull_ = ClientResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();
        unuseStreamsAndLOBs();

        isBeforeFirst_ = false;
        isFirst_ = false;

        if (rowIsInCurrentRowset(firstRowInRowset_ + currentRowInRowset_ - 1, scrollOrientation_prior__)) {
            isValidCursorPosition_ = true;
            currentRowInRowset_--;
        } else {
            checkAndThrowReceivedQueryTerminatingException();
            isValidCursorPosition_ = getPreviousRowset();
        }

        if (isValidCursorPosition_) {
            updateColumnInfoFromCache();
            // check if there is a non-null SQLCA for the row for rowset cursors
            checkRowsetSqlca();
            if (isAfterLast_) {
                isLast_ = true;
            }
            isAfterLast_ = false;
        } else {
            return isValidCursorPosition_;
        }

        if (sensitivity_ != sensitivity_sensitive_dynamic__ && maxRows_ > 0 &&
                (firstRowInRowset_ + currentRowInRowset_ > maxRows_)) {
            isValidCursorPosition_ = false;
        }
        // auto-close result set if this is the last row from server and return false
        return isValidCursorPosition_;
    }

    public void setFetchDirection(int direction) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setFetchDirection", direction);
                }
                checkForClosedResultSet("setFetchDirection");
                checkThatResultSetTypeIsScrollable();

                switch (direction) {
                case ResultSet.FETCH_FORWARD:
                case ResultSet.FETCH_REVERSE:
                case ResultSet.FETCH_UNKNOWN:
                    fetchDirection_ = direction;
                    break;
                default:
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.INVALID_FETCH_DIRECTION),
                        direction);
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getFetchDirection() throws SQLException {
        try
        {
            checkForClosedResultSet("getFetchDirection");
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getFetchDirection", fetchDirection_);
            }
            return fetchDirection_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setFetchSize", rows);
                }
                checkForClosedResultSet("setFetchSize");
                if (rows < 0) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.INVALID_FETCH_SIZE),
                        rows).getSQLException();
                }
                setFetchSize_(rows);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getFetchSize() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getFetchSize", fetchSize_);
            }
            checkForClosedResultSet("getFetchSize");
            return suggestedFetchSize_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getType() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getType", resultSetType_);
            }
            checkForClosedResultSet("getType");
            return resultSetType_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getConcurrency() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getConcurrency", resultSetConcurrency_);
            }
            checkForClosedResultSet("getConcurrency");
            return resultSetConcurrency_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //----------------------------- Updates --------------------------------------

    public boolean rowUpdated() throws SQLException {
        try
        {
            checkForClosedResultSet("rowUpdated");
            checkPositionedOnPlainRow();

            boolean rowUpdated = cursor_.getIsRowUpdated();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "rowUpdated", rowUpdated);
            }
            return rowUpdated;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean rowInserted() throws SQLException {
        try
        {
            checkForClosedResultSet("rowInserted");
            checkPositionedOnPlainRow();

            boolean rowInserted = false;

            // Not implemented for any result set type,
            // so it always returns false.

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "rowInserted", rowInserted);
            }
            return rowInserted;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean rowDeleted() throws SQLException {
        try
        {
            checkForClosedResultSet("rowDeleted");
            checkPositionedOnPlainRow();

            boolean rowDeleted = 
                (resultSetType_ == ResultSet.TYPE_SCROLL_INSENSITIVE) ?
                cursor_.getIsUpdateDeleteHole() : false;

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "rowDeleted", rowDeleted);
            }
            return rowDeleted;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // --------------------------- update column methods -------------------------

    public void updateNull(int column) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateNull", column);
                }
                checkUpdatePreconditions(column, "updateNull");
                if (!resultSetMetaData_.nullable_[column - 1]) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.LANG_NULL_INTO_NON_NULL),
                        column);
                }
                updateColumn(column, null);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBoolean(int column, boolean x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateBoolean", column, x);
                }
                checkUpdatePreconditions(column, "updateBoolean");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateByte(int column, byte x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateByte", column, x);
                }
                checkUpdatePreconditions(column, "updateByte");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateShort(int column, short x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateShort", column, x);
                }
                checkUpdatePreconditions(column, "updateShort");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateInt(int column, int x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateInt", column, x);
                }
                checkUpdatePreconditions(column, "updateInt");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateLong(int column, long x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateLong", column, x);
                }
                checkUpdatePreconditions(column, "updateLong");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateFloat(int column, float x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateFloat", column, x);
                }
                checkUpdatePreconditions(column, "updateFloat");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDouble(int column, double x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateDouble", column, x);
                }
                checkUpdatePreconditions(column, "updateDouble");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBigDecimal(int column, BigDecimal x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateBigDecimal", column, x);
                }
                checkUpdatePreconditions(column, "updateBigDecimal");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDate(int column, Date x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateDate", column, x);
                }
                checkUpdatePreconditions(column, "updateDate");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTime(int column, Time x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateTime", column, x);
                }
                checkUpdatePreconditions(column, "updateTime");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTimestamp(int column, Timestamp x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateTimestamp", column, x);
                }
                checkUpdatePreconditions(column, "updateTimestamp");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateString(int column, String x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateString", column, x);
                }
                checkUpdatePreconditions(column, "updateString");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBytes(int column, byte x[]) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateBytes", column, x);
                }
                checkUpdatePreconditions(column, "updateBytes");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBinaryStream(int column,
                                   InputStream x,
                                   int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "", column, x, length);
                }
                checkUpdatePreconditions(column, "updateBinaryStream");
                updateColumn(column, agent_.crossConverters_.setObjectFromBinaryStream(resultSetMetaData_.types_[column - 1], x, length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateAsciiStream(int column,
                                  InputStream x,
                                  int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateAsciiStream", column, x, length);
                }
                checkUpdatePreconditions(column, "updateAsciiStream");
                updateColumn(column, agent_.crossConverters_.setObjectFromCharacterStream(
                        resultSetMetaData_.types_[column - 1], x, Cursor.ISO_8859_1, length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateCharacterStream(int column,
                                      Reader x,
                                      int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateCharacterStream", column, x, length);
                }
                checkUpdatePreconditions(column, "updateCharacterStream");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x, length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateObject(int column, Object x, int scale) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateObject", column, x, scale);
                }
                checkUpdatePreconditions(column, "updateObject");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateObject(int column, Object x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateObject", column, x);
                }
                checkUpdatePreconditions(column, "updateObject");
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    // ---------------------- update on column name methods ----------------------

    public void updateNull(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateNull", columnName);
            }
            updateNull(findColumnX(columnName, "updateNull"));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBoolean", columnName, x);
            }
            updateBoolean(findColumnX(columnName, "updateBoolean"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateByte", columnName, x);
            }
            updateByte(findColumnX(columnName, "updateByte"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateShort(String columnName, short x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateShort", columnName, x);
            }
            updateShort(findColumnX(columnName, "updateShort"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateInt(String columnName, int x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateInt", columnName, x);
            }
            updateInt(findColumnX(columnName, "updateInt"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateLong(String columnName, long x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateLong", columnName, x);
            }
            updateLong(findColumnX(columnName, "updateLong"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateFloat", columnName, x);
            }
            updateFloat(findColumnX(columnName, "updateFloat"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateDouble", columnName, x);
            }
            updateDouble(findColumnX(columnName, "updateDouble"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBigDecimal", columnName, x);
            }
            updateBigDecimal(findColumnX(columnName, "updateBigDecimal"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateDate", columnName, x);
            }
            updateDate(findColumnX(columnName, "updateDate"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateTime", columnName, x);
            }
            updateTime(findColumnX(columnName, "updateTime"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateTimestamp", columnName, x);
            }
            updateTimestamp(findColumnX(columnName, "updateTimestamp"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateString(String columnName, String x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateString", columnName, x);
            }
            updateString(findColumnX(columnName, "updateString"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBytes(String columnName, byte x[]) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBytes", columnName, x);
            }
            updateBytes(findColumnX(columnName, "updateBytes"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBinaryStream(String columnName,
                                   InputStream x,
                                   int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBinaryStream", columnName, x, length);
            }
            updateBinaryStream(findColumnX(columnName, "updateBinaryStream"), x, length);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateAsciiStream(String columnName,
                                  InputStream x,
                                  int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateAsciiStream", columnName, x, length);
            }
            updateAsciiStream(findColumnX(columnName, "updateAsciiStream"), x, length);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateCharacterStream(String columnName,
                                      Reader x,
                                      int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateCharacterStream", columnName, x, length);
            }
            updateCharacterStream(findColumnX(columnName, "updateCharacterStream"), x, length);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateObject", columnName, x, scale);
            }
            updateObject(findColumnX(columnName, "updateObject"), x, scale);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateObject", columnName, x);
            }
            updateObject(findColumnX(columnName, "updateObject"), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateNCharacterStream(String columnName, Reader x)
            throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateNClob(String columnName, Reader reader)
            throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    // ---------------------------------------------------------------------------

    public void insertRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "insertRow");
                }
                insertRowX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void insertRowX() throws SqlException {
        checkForClosedResultSet("insertRow");
        checkForUpdatableResultSet("insertRow");
        if (isOnCurrentRow_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_NOT_POSITIONED_ON_INSERT_ROW));
       }
 
        // if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_INVALID_OPERATION_AT_CURRENT_POSITION));
        }

        // User might not be updating all the updatable columns selected in the
        // select sql and hence every insertRow on the same ResultSet can be
        // potentially different than the previous one. Because of that, we
        // should get a new prepared statement to do inserts every time
        getPreparedStatementForInsert();

        // build the inputs array for the prepared statement for insert
        int paramNumber = 0;
        for (int i = 0; i < updatedColumns_.length; i++) {
            if (resultSetMetaData_.sqlxUpdatable_[i] == 1) {
                // Since user may choose not to update all the columns in the
                // select list, check first if the column has been updated
                if (columnUpdated_[i] == true) {
                    paramNumber++;

                    // column is updated either if the updatedColumns_ entry is not null,
                    // or if the updatedColumns_ entry is null, but columnUpdated is
                    // set to true, which means columns is updated to a null.
                    if (updatedColumns_[i] != null ||
                            (updatedColumns_[i] == null && columnUpdated_[i])) {
                        preparedStatementForInsert_.setInput(
                                paramNumber, 
                                updatedColumns_[i]);
                    }
                }
            }
        }
        try {
            insert();
        } catch (SqlException e) {
            throw e;
        }
    }
    
    public void updateRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateRow");
                }
                // If updateXXX were issued on the row before updateRow and
                // the result set if forward only, then position the ResultSet
                // to right before the next row after updateRow.
                if (updateRowX() && (getType() == 
                                     ResultSet.TYPE_FORWARD_ONLY)) {
                    isValidCursorPosition_ = false;
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //if no updateXXX were issued before this updateRow, then return false
    private boolean updateRowX() throws SqlException {
        checkForClosedResultSet("updateRow");
        
        checkForUpdatableResultSet("updateRow");
        
        if (isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_CURRENT_ROW));
        }

        //if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_)
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_INVALID_OPERATION_AT_CURRENT_POSITION));

        // If no updateXXX has been called on this ResultSet object, then
        // updatedColumns_ will be null and hence no action required
        if (updatedColumns_ == null) {
            return false;
        }

        // updateXXX has been called on this ResultSet object, but check if it
        // has been called on the current row. If no column got updated on this
        // current row, then just return.
        boolean didAnyColumnGetUpdated = false;
        for (int i=0; i < updatedColumns_.length; i++) {
            if (columnUpdated_[i]) {
                didAnyColumnGetUpdated = true;
                break;
            }
        }
        if (didAnyColumnGetUpdated == false)
            return false;

        // User might not be updating all the updatable columns selected in the
        // select sql and hence every updateRow on the same ResultSet can be
        // potentially different than the previous one. Because of that, we
        // should get a new prepared statement to do updates every time
        getPreparedStatementForUpdate();

        // build the inputs array for the prepared statement for update
        int paramNumber = 0;
        for (int i = 0; i < updatedColumns_.length; i++) {
            if (resultSetMetaData_.sqlxUpdatable_[i] == 1) {
                // Since user may choose not to update all the columns in the
                // select list, check first if the column has been updated
                if (columnUpdated_[i] == false)
                    continue;
                paramNumber++;

                // column is updated either if the updatedColumns_ entry is not null,
                // or if the updatedColumns_ entry is null, but columnUpdated_ boolean is
                // set to true, which means columns is updated to a null.
                if (updatedColumns_[i] != null ||
                        (updatedColumns_[i] == null && columnUpdated_[i])) {
                    preparedStatementForUpdate_.setInput(paramNumber, updatedColumns_[i]);
                } else {
                    // Check if the original column is null.  Calling CrossConverters.setObject on a null
                    // column causes "Data Conversion" Exception.
                    Object originalObj;
                    try {
                        originalObj = getObject(i + 1);
                    } catch ( SQLException se ) {
                        throw new SqlException(se);
                    }
                    
                    if (originalObj == null) {
                        preparedStatementForUpdate_.setInput(paramNumber, null);
                    } else {
                        preparedStatementForUpdate_.setInput(paramNumber, agent_.crossConverters_.setObject(resultSetMetaData_.types_[i], originalObj));
                    }
                }
            }
        }
        // need to cancel updates if the actual update was not successful at the server.
        // alternative is to check for updateCount_ in "positionToCurrentRowAndUpdate".
        // cancelRowUpdates if updateCount_ != 1
        try {
            if (isRowsetCursor_ || 
                    sensitivity_ == sensitivity_sensitive_dynamic__ ||
                    sensitivity_ == sensitivity_sensitive_static__) {
                update();
            } else {
                positionToCurrentRowAndUpdate();
            }
        } finally {
            resetUpdatedColumns();
        }

        // Ensure the data is reset
        if (resultSetType_ == ResultSet.TYPE_SCROLL_INSENSITIVE) {
            if (preparedStatementForUpdate_.updateCount_ > 0) {
                // This causes a round-trip
                getAbsoluteRowset(absolutePosition_);
            }
        }

        return true;
    }

    public void deleteRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "deleteRow");
                }
                deleteRowX();
                //the cursor is not positioned on the deleted row after deleteRow.
                //User needs to issue ResultSet.next to reposition the ResultSet
                //on a valid row
                isValidCursorPosition_ = false;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void deleteRowX() throws SqlException {
        checkForClosedResultSet("deleteRow");
        
        checkForUpdatableResultSet("deleteRow");

        // discard all previous updates
        resetUpdatedColumns();

        if (isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_CURRENT_ROW));
        }

        if (preparedStatementForDelete_ == null) {
            getPreparedStatementForDelete();
        }

        if (isRowsetCursor_ || 
                sensitivity_ == sensitivity_sensitive_dynamic__ ||
                sensitivity_ == sensitivity_sensitive_static__) {
            delete();
        } else {
            positionToCurrentRowAndDelete();
        }

        if (resultSetType_ == ResultSet.TYPE_FORWARD_ONLY) {
            cursor_.isUpdateDeleteHole_ = true;
        } else {
            if (preparedStatementForDelete_.updateCount_ > 0) {
                
                cursor_.isUpdateDeleteHoleCache_.set((int) currentRowInRowset_,
                                                     Cursor.ROW_IS_NULL);
                cursor_.isUpdateDeleteHole_ = 
                    ((Boolean) cursor_.isUpdateDeleteHoleCache_.
                     get((int) currentRowInRowset_)).booleanValue();
            }
        }
    }

    public void refreshRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "refreshRow");
                }
                refreshRowX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void refreshRowX() throws SqlException {
        checkForClosedResultSet("refreshRow");
        checkThatResultSetTypeIsScrollable();
    checkForUpdatableResultSet("refreshRow");
        if (isBeforeFirstX() || isAfterLastX() || isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NO_CURRENT_ROW));
        }
    
        // this method does nothing if ResultSet is TYPE_SCROLL_INSENSITIVE
        if (resultSetType_ == ResultSet.TYPE_SCROLL_SENSITIVE) {
            isValidCursorPosition_ = getRefreshRowset();
            try {
                cancelRowUpdates();
            } catch ( SQLException sqle ) {
                throw new SqlException(sqle);
            }

            unuseStreamsAndLOBs();

        }
    }

    public void cancelRowUpdates() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "cancelRowUpdates");
                }
                checkForClosedResultSet("cancelRowUpdates");
                checkForUpdatableResultSet("cancelRowUpdates");
                if (isOnInsertRow_) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CURSOR_NOT_POSITIONED_ON_INSERT_ROW));
                }

                // if not on a valid row, then do not accept cancelRowUpdates call
                if (!isValidCursorPosition_)
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.CURSOR_INVALID_OPERATION_AT_CURRENT_POSITION));
                // Reset updated columns
                resetUpdatedColumns();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void moveToInsertRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "moveToInsertRow");
                }
                checkForClosedResultSet("moveToInsertRow");
                checkForUpdatableResultSet("moveToInsertRow");

                resetUpdatedColumnsForInsert();
                
                // Note that even though we navigate "away" from the current row
                // we do not clean up the current row (i.e. release locators), so
                // locators will still be valid when returning to the current row.
                // See DERBY-6228.
                isOnInsertRow_ = true;
                isOnCurrentRow_ = false;

                // It is possible to navigate from a row for which 
                // isValidCursorPosition_==false to the insert row. By
                // saving the old value here we can restore it when leaving
                // the insert row. This is important since attempting to 
                // release locators for a non-valid cursor position will trigger 
                // an error on the server. See DERBY-6228.
                savedIsValidCursorPosition_ = isValidCursorPosition_;
                isValidCursorPosition_ = true;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void moveToCurrentRow() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "moveToCurrentRow");
                }
                checkForClosedResultSet("moveToCurrentRow");
                checkForUpdatableResultSet("moveToCurrentRow");

                moveToCurrentRowX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }
    
    /**
     * Moves off the insert row if positioned there, and checks the current row
     * for releasable LOB locators if positioned on a valid data row.
     *
     * @throws SqlException if releasing a LOB locator fails
     */
    private void moveToCurrentRowX() throws SqlException {
        if (isOnInsertRow_) {
            resetUpdatedColumns();
            isOnInsertRow_ = false;
            isOnCurrentRow_ = true;
            if (currentRowInRowset_ > 0) {
                updateColumnInfoFromCache();
            }
            // Restore the old value when leaving the insert row. See DERBY-6228.
            isValidCursorPosition_ = savedIsValidCursorPosition_;
        }
        if (isValidCursorPosition_) {
            // isOnInsertRow must be false here.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!isOnInsertRow_,
                        "Cannot check current row if positioned on insert row");
            }
            lobState.checkCurrentRow(cursor_);
        }
    }

    /**
     * Retrieves the <code>Statement</code> object that produced this
     * object, or <code>null</code> if the <code>ResultSet</code> was
     * not produced by a <code>Statement</code> object.
     *
     * @return the <code>Statement</code> that produced this object or
     * <code>null</code>
     * @exception SQLException if a database error occurs or the
     * result set is closed
     */
    public Statement getStatement() throws SQLException {
        try {
            checkForClosedResultSet("getStatement");
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getStatement", statement_);
        }
        if (statement_.getOwner() != null) {
            return statement_.getOwner();
        } else {
            return statement_;
        }

    }

    //-------------------------- JDBC 3.0 ----------------------------------------

    public URL getURL(int columnIndex) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public URL getURL(String columnName) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBlob",
                        columnIndex, x);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateBlob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                    resultSetMetaData_.types_[columnIndex -1],
                                    x));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     * clause. If the SQL AS clause was not specified, then the label is the
     * name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateBlob(String columnName, Blob x) throws SQLException {
        try {
            updateBlob(findColumnX(columnName, "updateBlob"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }
  
    /**
     * Updates the designated column using the given input stream, which
     * will have the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @exception SQLException if the columnIndex is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateBlob(int columnIndex, InputStream x, long length)
                            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBlob",
                        columnIndex, x, (int)length);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateBlob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                    resultSetMetaData_.types_[columnIndex -1],
                                    new ClientBlob(agent_, x, (int)length)));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column using the given input stream, which
     * will have the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the label for the column specified with the
     * SQL AS clause.  If the SQL AS clause was not specified, then the
     * label is the name of the column
     * @param x An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @exception SQLException if the columnLabel is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateBlob(String columnName, InputStream x, long length)
                           throws SQLException {
        try {
            updateBlob(findColumnX(columnName, "updateBlob"), x, length);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    boolean repositionScrollableResultSetBeforeJDBC1PositionedUpdateDelete()
            throws SqlException {
        boolean repositionedCursor = false;

        // calculate the absolutePosition of the current row directly.
        long rowToFetch = getRowUncast() - absolutePosition_;

        // if rowToFetch is zero, already positioned on the current row
        if (rowToFetch != 0) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.getSection() : generatedSection_,
                    scrollOrientation_relative__,
                    rowToFetch);
            // adjust the absolute position on the client
            absolutePosition_ += rowToFetch;
            repositionedCursor = true;
        }
        return repositionedCursor;
    }
    //--------------------categorize the methods below -----------------

    private void flowPositioningFetch(int scrollOrientation,
                                     int rowToFetch) throws DisconnectException {
        // need the try-catch block here because agent_.beginWriteChain throws
        // an SqlException
        try {
            agent_.beginWriteChain(statement_);

            writePositioningFetch_((generatedSection_ == null) ? statement_.getSection() : generatedSection_,
                    scrollOrientation,
                    rowToFetch);

            agent_.flow(statement_);
            readPositioningFetch_();
            agent_.endReadChain();
        } catch (SqlException e) {
            throw new DisconnectException(agent_, e);
        }
    }

    private void positionToCurrentRowAndUpdate() throws SqlException {
        agent_.beginWriteChain(statement_);

        // calculate the position of the current row relative to the absolute position on server
        long currentRowPosRelativeToAbsoluteRowPos = getRowUncast() - absolutePosition_;

        // if currentRowPosRelativeToAbsoluteRowPos is zero, already on the current row
        // reposition only if a commit has been sent
        // do not reposition forward-only cursors
        if (resultSetType_ != ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.getSection() : generatedSection_,
                    scrollOrientation_relative__,
                    currentRowPosRelativeToAbsoluteRowPos);
        }
        
        try {
            writeUpdateRow(false);
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
        

        agent_.flow(statement_);

        // adjust the absolute position on the client
        absolutePosition_ += currentRowPosRelativeToAbsoluteRowPos;

        if (resultSetType_ != ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            readPositioningFetch_();
            cursorUnpositionedOnServer_ = false;
            listenToUnitOfWork();
        }

        readUpdateRow();

        agent_.endReadChain();
    }

    private void insert() throws SqlException {
        agent_.beginWriteChain(statement_);

        try {
            writeInsertRow(false);
        } catch (SQLException se ) {
            throw new SqlException(se);
        }

        agent_.flow(statement_);

        readInsertRow();

        agent_.endReadChain();
     }    

    
    private void update() throws SqlException {
        agent_.beginWriteChain(statement_);

        if (isRowsetCursor_) {
            try {
                preparedStatementForUpdate_.setInt(updatedColumns_.length + 1, (int) (currentRowInRowset_ + 1));
            } catch ( SQLException se ) {
                throw new SqlException(se);
            }
        }

        boolean chainAutoCommit = connection_.willAutoCommitGenerateFlow();
        try {
            writeUpdateRow(chainAutoCommit);
        } catch (SQLException se ) {
            throw new SqlException(se);
        }
        if (chainAutoCommit) {
            connection_.writeCommit();
        }

        agent_.flow(statement_);

        readUpdateRow();

        if (chainAutoCommit) {
            connection_.readCommit();
        }
        agent_.endReadChain();
    }

    private void positionToCurrentRowAndDelete() throws SqlException {
        agent_.beginWriteChain(statement_);

        // calculate the position of the current row relative to the absolute position on server
        long currentRowPosRelativeToAbsoluteRowPos = getRowUncast() - absolutePosition_;

        // if rowToFetch is zero, already positioned on the current row
        // do not reposition forward-only cursors.
        if (resultSetType_ != ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.getSection() : generatedSection_,
                    scrollOrientation_relative__,
                    currentRowPosRelativeToAbsoluteRowPos);
        }

        try {
            writeDeleteRow();
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }

        agent_.flow(statement_);

        // adjust the absolute position on the client.
        absolutePosition_ += currentRowPosRelativeToAbsoluteRowPos;

        if (resultSetType_ != ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            readPositioningFetch_();
            cursorUnpositionedOnServer_ = false;
            listenToUnitOfWork();
        }

        readDeleteRow();

        agent_.endReadChain();
    }

    private void delete() throws SqlException {
        try
        {
            agent_.beginWriteChain(statement_);

            if (isRowsetCursor_) {
                preparedStatementForDelete_.setInt(1, (int) (currentRowInRowset_ + 1));
            }

            writeDeleteRow();

            if (connection_.autoCommit_) {
                connection_.writeAutoCommit();
            }

            agent_.flow(statement_);

            readDeleteRow();
            if (connection_.autoCommit_) {
                connection_.readAutoCommit();
            }
            agent_.endReadChain();
        }
        catch ( SQLException se )
        {
            throw new SqlException(se);
        }
    }

    // Resets all rowset related flags.
    // Called by getRowSet() from material layer.
    private void setRowsetAfterLastEvent() throws SqlException {
        firstRowInRowset_ = 0;
        lastRowInRowset_ = 0;
        absolutePosition_ = (maxRows_ == 0) ? rowCount_ + 1 : maxRows_ + 1;
        currentRowInRowset_ = 0;
        rowsReceivedInCurrentRowset_ = 0;
    }

    private void setRowsetBeforeFirstEvent() throws SqlException {
        firstRowInRowset_ = 0;
        lastRowInRowset_ = 0;
        absolutePosition_ = 0;
        currentRowInRowset_ = -1;
        rowsReceivedInCurrentRowset_ = 0;
    }

    private void setRowsetNoRowsEvent() {
        rowCount_ = 0;
        firstRowInRowset_ = 0;
        lastRowInRowset_ = 0;
        absolutePosition_ = 0;
        currentRowInRowset_ = -1;
        rowsReceivedInCurrentRowset_ = 0;
    }

    private boolean isServersCursorPositionBeforeFirst() throws SqlException {
        return (isBeforeFirstX() && firstRowInRowset_ == 0 && lastRowInRowset_ == 0 && absolutePosition_ == 0);
    }

    private boolean isServerCursorPositionAfterLast() {
        return (absolutePosition_ == (rowCount_ + 1));
    }

    public void setValidCursorPosition(boolean isValidCursorPosition) {
        isValidCursorPosition_ = isValidCursorPosition;
    }

    private void moveToAfterLast() throws DisconnectException {
        flowPositioningFetch(ClientResultSet.scrollOrientation_after__, 0);
    }

    // Positions the cursor at before the first row.
    private void moveToBeforeFirst() throws DisconnectException {
        flowPositioningFetch(ClientResultSet.scrollOrientation_before__, 0);
    }

    private void writeInsertRow(boolean chainedWritesFollowingSetLob)
            throws SQLException {
        try
        {
            preparedStatementForInsert_.materialPreparedStatement_.writeExecute_(
                    preparedStatementForInsert_.getSection(),
                    preparedStatementForInsert_.parameterMetaData_,
                    preparedStatementForInsert_.parameters_,
                    (preparedStatementForInsert_.parameterMetaData_ == null ? 0 : 
                        preparedStatementForInsert_.parameterMetaData_.getColumnCount()),
                    false, // false means we're not expecting output
                    chainedWritesFollowingSetLob);  // chaining after the execute        
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }
    
    private void writeUpdateRow(boolean chainedWritesFollowingSetLob)
            throws SQLException {
        try
        {
            preparedStatementForUpdate_.materialPreparedStatement_.writeExecute_(preparedStatementForUpdate_.getSection(),
                    preparedStatementForUpdate_.parameterMetaData_,
                    preparedStatementForUpdate_.parameters_,
                    preparedStatementForUpdate_.parameterMetaData_.getColumnCount(),
                    false, // false means we're not expecting output
                    chainedWritesFollowingSetLob);  // chaining after the execute
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void writeDeleteRow() throws SQLException {
        try
        {
            if (isRowsetCursor_) {
                preparedStatementForDelete_.materialPreparedStatement_.writeExecute_(preparedStatementForDelete_.getSection(),
                        preparedStatementForDelete_.parameterMetaData_,
                        preparedStatementForDelete_.parameters_,
                        preparedStatementForDelete_.parameterMetaData_.getColumnCount(),
                        false, // false means we're not expecting output
                        false);  // false means we don't chain anything after the execute
            } else {
                preparedStatementForDelete_.materialPreparedStatement_.writeExecute_(preparedStatementForDelete_.getSection(),
                        null, // do not need parameterMetaData since there is no input
                        null, // no inputs
                        0, // number of input columns is 0 for positioned delete
                        false, // false means we're not expecting output
                        false);  // false means we don't chain anything after the execute
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void readInsertRow() throws DisconnectException, SqlException {
        preparedStatementForInsert_.materialPreparedStatement_.readExecute_();
    }

    private void readUpdateRow() throws DisconnectException, SqlException {
        preparedStatementForUpdate_.materialPreparedStatement_.readExecute_();
        accumulateWarning(preparedStatementForUpdate_.getSqlWarnings());
    }

    private void readDeleteRow() throws DisconnectException, SqlException {
        preparedStatementForDelete_.materialPreparedStatement_.readExecute_();
        accumulateWarning(preparedStatementForDelete_.getSqlWarnings());
    }

    //------------------material layer event callback methods-----------------------

    private boolean listenToUnitOfWork_ = false;

    public void listenToUnitOfWork() {
        if (!listenToUnitOfWork_) {
            listenToUnitOfWork_ = true;
            connection_.CommitAndRollbackListeners_.put(this, null);
        }
    }

    public void completeLocalCommit(Iterator listenerIterator) {
        cursorUnpositionedOnServer_ = true;
        lobState.discardState(); // Locators released on server side.
        markAutoCommitted();
        if (!cursorHold_) {
            // only non-held cursors need to be closed at commit
            markClosed();
            nullOutReferenceInStatement();
            // remove from listener list
            listenerIterator.remove();
            listenToUnitOfWork_ = false;
        }
    }

    public void completeLocalRollback(Iterator listenerIterator) {
        lobState.discardState(); // Locators released on server side.
        markAutoCommitted();
        // all cursors need to be closed at rollback
        markClosed();
        nullOutReferenceInStatement();
        // remove from listener list
        listenerIterator.remove();
        listenToUnitOfWork_ = false;
    }

    private void nullOutReferenceInStatement() {
        if (statement_.resultSet_ == this) {
            statement_.resultSet_ = null;
        }
        /*
         * Aug 10, 2005: Do we really only want to only null out the one resultSet? 
         * The only time this method is called is from completeLocalCommit or 
         * completeLocalRollback, both of which affect *all* ResultSets  
         */
        if (statement_.resultSetList_ != null) {
            for (int i = 0; i < statement_.resultSetList_.length; i++) {
                if (statement_.resultSetList_[i] == this) {
                    statement_.resultSetList_[i] = null;
                }
            }
        }
    }

    /**
     * Mark this ResultSet as closed. The ResultSet will not be
     * removed from the commit and rollback listeners list in
     * <code>Connection</code>.
     */
    void markClosed() {
        markClosed(false);
    }

    /**
     * Mark this ResultSet as closed.
     *
     * @param removeListener if true the ResultSet will be removed
     * from the commit and rollback listeners list in
     * <code>Connection</code>.
     */
    void markClosed(boolean removeListener) {
        openOnClient_ = false;
        openOnServer_ = false;
        statement_.resetCursorNameAndRemoveFromWhereCurrentOfMappings(); // for SELECT...FOR UPDATE queries
        statement_.removeClientCursorNameFromCache();
        markPositionedUpdateDeletePreparedStatementsClosed();
        if (removeListener) {
            connection_.CommitAndRollbackListeners_.remove(this);
        }
        closeStatementOnCompletion();
    }

    /**
     * Mark this ResultSet as closed on the server.
     */
    public void markClosedOnServer() {
        openOnServer_ = false;
    }

    void markAutoCommitted() {
        autoCommitted_ = true;
    }

    // The query was ended at the server because all rows have been retrieved.
    public void earlyCloseComplete(Sqlca sqlca) {
        markClosedOnServer();
        queryTerminatingSqlca_ = sqlca;
        cursor_.setAllRowsReceivedFromServer(true);
    }

    public int completeSqlca(Sqlca sqlca) {
        if (sqlca == null) {
            return 0;
        }
        int sqlcode = sqlca.getSqlCode();
        if (sqlcode == 100 || sqlcode == 20237) {
            cursor_.setAllRowsReceivedFromServer(true);
        } else if (sqlcode < 0) {
            connection_.agent_.accumulateReadException(new SqlException(agent_.logWriter_, sqlca));
        } else if (sqlcode > 0) {
            accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
        }
        return sqlcode;
    }

    // Set rowCount.
    public void setRowCountEvent(long rowCount) throws DisconnectException {
        // Only set the row count if it's unknown, to prevent clobbering of a valid value.
        if (rowCount_ == -1) {
            rowCount_ = rowCount;
        }
    }

    // This method will not work if e is chained.
    // It is assumed that e is a single warning and is not chained.
    public void accumulateWarning(SqlWarning e) {
        if (warnings_ == null) {
            warnings_ = e;
        } else {
            warnings_.setNextException(e);
        }
    }

    //-------------------------------helper methods-------------------------------
    boolean rowCountIsUnknown() {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return false;
        } else {
            return rowCount_ == -1;
        }
    }

    private void updateColumn(int column, Object value) {
        if (updatedColumns_ == null) {
            updatedColumns_ = new Object[resultSetMetaData_.columns_];
        }
        if (columnUpdated_ == null) {
            columnUpdated_ = new boolean[resultSetMetaData_.columns_];
        }

        updatedColumns_[column - 1] = value;
        columnUpdated_[column - 1] = true;
    }
    
    /*
     * Builds the insert statement that will be used well calling insertRow
     * 
     * If no values have been supplied for a column, it will be set 
     * to the column's default value, if any. 
     * If no default value had been defined, the default value of a 
     * nullable column is set to NULL.
     */
    private String buildInsertString() throws SqlException {
        int column;
        boolean foundOneUpdatedColumnAlready = false;
        
        StringBuffer insertSQL = new StringBuffer("INSERT INTO ");
        StringBuffer valuesSQL = new StringBuffer("VALUES (");

        insertSQL.append(getTableName());
        insertSQL.append(" (");
        
        for (column = 1; column <= resultSetMetaData_.columns_; column++) {
            if (foundOneUpdatedColumnAlready) {
                insertSQL.append(",");
                valuesSQL.append(",");
            }
            //using quotes around the column name to preserve case sensitivity
            try {
                insertSQL.append(Utils.quoteSqlIdentifier(
                        resultSetMetaData_.getColumnName(column)));
            } catch ( SQLException sqle ) {
                throw new SqlException(sqle);
            }
            if (columnUpdated_[column - 1]) { 
                valuesSQL.append("?");
            } else {
                valuesSQL.append("DEFAULT");
            }
            foundOneUpdatedColumnAlready = true;
        }
        
        insertSQL.append(") ");
        valuesSQL.append(") ");
        insertSQL.append(valuesSQL.toString());
        
        return(insertSQL.toString());
    }
    
    private String buildUpdateString() throws SqlException {
        int column;
        int numColumns = 0;

        // For Derby, eg update t1 set c1=?, c2=? where current of cursorname
        boolean foundOneUpdatedColumnAlready = false;
        StringBuffer updateString = new StringBuffer(64);
        updateString.append("UPDATE ").append(getTableName()).append(" SET ");

        for (column = 1; column <= resultSetMetaData_.columns_; column++) {
            if (columnUpdated_[column - 1]) {
                if (foundOneUpdatedColumnAlready) {
                    updateString.append(",");
                }
                try {
                    updateString.append(Utils.quoteSqlIdentifier(
                            resultSetMetaData_.getColumnName(column))).append(" = ? ");
                } catch ( SQLException sqle ) {
                    throw new SqlException(sqle);
                }
                numColumns++;
                foundOneUpdatedColumnAlready = true;
            }
        }
        if (foundOneUpdatedColumnAlready == false) //no columns updated on this row
        {
            return null;
        }
        updateString.append(" WHERE CURRENT OF ").append(getServerCursorName());

        if (isRowsetCursor_) {
            updateString.append(" FOR ROW ? OF ROWSET");
        }

        return updateString.toString();
    }

    private String buildDeleteString() throws SqlException {
        String deleteString = "DELETE FROM ";

        // build the delete string using the server's cursor name
        deleteString += (getTableName() + " WHERE CURRENT OF \"" + getServerCursorName() + "\"");

        if (isRowsetCursor_) {
            deleteString += " FOR ROW ? OF ROWSET";
        }

        return deleteString;
    }

    //Go through all the columns in the select list to see if we can find a
    //base table column and use that column's metadata to get the table name
    //But, it is possible to have a sql of the form
    //select 1,2 from t1 for update
    //This sql will not be a good candidate for updateXXX calls(both in embedded
    //and Network Server mode) since there is no updatable column in the select
    //list. But a user can use a sql like that to issue deleteRow. In Network
    //Server mode though, this sql will fail for deleteRow because none of the
    //columns are from base table and w/o a base table column, there is no way
    //to find the table name for delete
    private String getTableName() throws SqlException {
        String tableName = "";
        int baseTableColumn = 0;
        int totalColumns;
        try {
            totalColumns = resultSetMetaData_.getColumnCount();
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }
        for (; baseTableColumn < totalColumns; baseTableColumn++) {
            if (resultSetMetaData_.sqlxBasename_[baseTableColumn] != null)
                break;
        }

        //if following true, then there are no base table columns in select list
        if (baseTableColumn == totalColumns)
            baseTableColumn = 0;

        //dervied column like select 2 from t1, has null schema and table name
        if (resultSetMetaData_.sqlxSchema_[baseTableColumn] != null && !resultSetMetaData_.sqlxSchema_[baseTableColumn].equals("")) {
            tableName += Utils.quoteSqlIdentifier(
                    resultSetMetaData_.sqlxSchema_[baseTableColumn]) + ".";
        }
        if (resultSetMetaData_.sqlxBasename_[baseTableColumn] != null) {
            tableName += Utils.quoteSqlIdentifier(
                    resultSetMetaData_.sqlxBasename_[baseTableColumn]);
        }
        return tableName;
    }

    private String getServerCursorName() throws SqlException {
        return statement_.getSection().getServerCursorName();
    }

    private void getPreparedStatementForInsert() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String insertString = buildInsertString();

        try {
            preparedStatementForInsert_ = (ClientPreparedStatement)statement_.
                connection_.prepareStatement(insertString);
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }
    }

    private void getPreparedStatementForUpdate() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String updateString = buildUpdateString();

        if (updateString == null) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_NO_UPDATE_CALLS_ON_CURRENT_ROW));
        }
        preparedStatementForUpdate_ =
                statement_.connection_.preparePositionedUpdateStatement(updateString,
                        statement_.getSection().getPositionedUpdateSection());

    }

    private void getPreparedStatementForDelete() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String deleteString = buildDeleteString();

        preparedStatementForDelete_ =
                statement_.connection_.preparePositionedUpdateStatement(deleteString,
                        statement_.getSection().getPositionedUpdateSection()); // update section
    }

    private final void resetUpdatedColumnsForInsert() {
        // initialize updateColumns with nulls for all columns
        for (int i = 0; i < resultSetMetaData_.columns_; i++) {
            updateColumn(i+1, null);
            columnUpdated_[i] = false;
        }
    }

    private final void resetUpdatedColumns() {
        if (updatedColumns_ != null) {
            Arrays.fill(updatedColumns_, null);
        }
        if (columnUpdated_ != null) {
            Arrays.fill(columnUpdated_, false);
        }
    }

    private final long getRowUncast() {
        return firstRowInRowset_ + currentRowInRowset_;
    }

    private final void checkGetterPreconditions(int column, String operation)
            throws SqlException {
        checkForClosedResultSet(operation);
        checkForValidColumnIndex(column);
        checkForValidCursorPosition();
    }

    private final void checkUpdatePreconditions(int column, 
                        String operation)
    throws SqlException {

        checkForClosedResultSet(operation);
        checkForValidColumnIndex(column);
    checkForUpdatableResultSet(operation);

        if (!isOnCurrentRow_ && !isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_NOT_ON_CURRENT_OR_INSERT_ROW));
        }

        if (resultSetMetaData_.sqlxUpdatable_ == null || resultSetMetaData_.sqlxUpdatable_[column - 1] != 1) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_COLUMN_NOT_UPDATABLE));
        }

        //if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_)
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_INVALID_OPERATION_AT_CURRENT_POSITION));
    }

    private void checkForValidColumnIndex(int column) throws SqlException {
        if (column < 1 || column > resultSetMetaData_.columns_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.LANG_INVALID_COLUMN_POSITION),
                column, resultSetMetaData_.columns_);
        }
    }

    protected final void checkForClosedResultSet(String operation)
            throws SqlException {
        if (!openOnClient_) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, new ClientMessageId(
                    SQLState.LANG_RESULT_SET_NOT_OPEN), operation);
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    private final void checkForUpdatableResultSet(String operation) 
        throws SqlException {
        if (resultSetConcurrency_ == ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.UPDATABLE_RESULTSET_API_DISALLOWED),
                    operation);
        }
    }
    
    private final void checkForValidCursorPosition() throws SqlException {
        if (!isValidCursorPosition_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_INVALID_OPERATION_AT_CURRENT_POSITION));
        }
    }


    private final void checkPositionedOnPlainRow() throws SqlException {
        if (isOnInsertRow_ || !isValidCursorPosition_) {
            throw new SqlException
                (agent_.logWriter_, 
                 new ClientMessageId(SQLState.NO_CURRENT_ROW));
        }
    }


    private final void checkThatResultSetTypeIsScrollable() throws SqlException {
        if (resultSetType_ == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_MUST_BE_SCROLLABLE));
        }
    }

    private final void checkThatResultSetIsNotDynamic() throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CURSOR_INVALID_FOR_SENSITIVE_DYNAMIC));
        }
    }

    private boolean resultSetContainsNoRows() throws SqlException {
        if (rowCountIsUnknown()) {
            getRowCount();
        }
        return (rowCount_ == 0);
    }

    private boolean rowIsInCurrentRowset(long rowNumber, int orientation) throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            switch (orientation) {
            case scrollOrientation_next__:
                if (isAfterLast_) {
                    return false;
                } else {
                    return (currentRowInRowset_ + 1 < rowsReceivedInCurrentRowset_);
                }
            case scrollOrientation_prior__:
                if (isBeforeFirst_) {
                    return false;
                } else {
                    return (currentRowInRowset_ - 1 >= 0);
                }
            case scrollOrientation_relative__:
                return (rowNumber < rowsReceivedInCurrentRowset_ && rowNumber >= 0);

                // for dynamic cursors, we will not be able to check whether a row is in the cache for
                // FIRST, ABSOLUTE, AND LAST
            case scrollOrientation_first__:
            case scrollOrientation_absolute__:
            case scrollOrientation_last__:
                return false;
            default:
                return false;
            }
        } else if (resultSetMetaData_.hasLobColumns()) {
            // DERBY-6737: If the result set has LOB columns, we cannot use
            // the cached locator since it might have been released, so
            // always fetch the row from the server.
            return false;
        } else {
            return rowIsInCurrentRowset(rowNumber);
        }
    }

    private boolean rowIsInCurrentRowset(long rowNumber) {
        // firstRowInRowset_ is equal to lastRowInRowset_ if there is only one row
        // in the rowset or if fetchSize_ is 1.  however, return false if firstRowInRowset_
        // is equal to lastRowInRowset_ and also equal to zero which means there is no
        // valid row in the current rowset.
        if (firstRowInRowset_ == lastRowInRowset_ && firstRowInRowset_ == 0) {
            return false;
        }
        if (rowNumber >= firstRowInRowset_ &&
                rowNumber <= lastRowInRowset_) {
            return true;
        } else {
            return false;
        }
    }

    private void markPositionedUpdateDeletePreparedStatementsClosed() {
        if (preparedStatementForUpdate_ != null) {
            preparedStatementForUpdate_.markClosed();
            preparedStatementForUpdate_ = null;
        }
        if (preparedStatementForDelete_ != null) {
            preparedStatementForDelete_.markClosed();
            preparedStatementForDelete_ = null;
        }
    }

    private void updateColumnInfoFromCache() {
        // currentRowInRowset_ should never be bigger than the max value of an int,
        // because we have a driver imposed limit of fetch size 1000.
        cursor_.columnDataPosition_ =
                cursor_.columnDataPositionCache_.get((int) currentRowInRowset_);
        cursor_.columnDataComputedLength_ =
                cursor_.columnDataLengthCache_.get((int) currentRowInRowset_);
        cursor_.isNull_ =
                cursor_.columnDataIsNullCache_.get((int) currentRowInRowset_);
        cursor_.isUpdateDeleteHole_ = ((Boolean) cursor_.isUpdateDeleteHoleCache_.get((int) currentRowInRowset_)).booleanValue();
    }

    protected final void checkAndThrowReceivedQueryTerminatingException() throws SqlException {
        // If we are in a split row, and before sending CNTQRY, check whether an ENDQRYRM
        // has been received.
        if (!openOnServer_) {
            SqlException sqlException = null;
            int sqlcode = Utils.getSqlcodeFromSqlca(queryTerminatingSqlca_);
            if (sqlcode < 0) {
                sqlException = new SqlException(agent_.logWriter_, queryTerminatingSqlca_);
            } else if (sqlcode > 0 && sqlcode != 100) {
                accumulateWarning(new SqlWarning(agent_.logWriter_, queryTerminatingSqlca_));
            }
            try {
                closeX(); // the auto commit logic is in closeX()
            } catch (SqlException e) {
                sqlException.setNextException(e);
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
    }

    void parseScrollableRowset() throws SqlException {
        // modified check from qrydtaReturned to cursor.dataBufferHasUnprocesseData()
        if (cursor_.dataBufferHasUnprocessedData() && scrollable_) {
            parseRowset_();
            adjustFirstRowset();

            // This method is only called after open query to parse out the very first rowset
            // received.
            if (cursor_.allRowsReceivedFromServer() &&
                rowsReceivedInCurrentRowset_ == 0) {
                setRowsetNoRowsEvent();
            }
        }
    }

    //  determines if a cursor is a:
    //    Return to Client - not to be read by the stored procedure only by client
    //    Return to Caller - only calling JSP can read it, not the client
    public byte getRSReturnability() {
        return rsReturnability_;
    }

    public void setRSReturnability(byte rsReturnability) { // valid return type set it
        if ((rsReturnability == DDM_RETURN_CALLER) ||
                (rsReturnability == DDM_RETURN_CLIENT)) {
            rsReturnability_ = rsReturnability;
        } else { // unknown return type, set DDM_RETURN_CALLER
            rsReturnability_ = DDM_RETURN_CALLER;
        }
    }

//------------------------------------------------------------------------------
    // Push the getXXXRowset_() methods up from the material layers
//------------------------------------------------------------------------------
    // This method is called to retrieve the total number of rows in the result set
    // and then reposition the cursor to where it was before.
    // The rowCount_ comes back in the SQLCA in fields SQLERRD1 and SQLERRD2 when
    // sqlcode is +100.
    protected void getRowCount() throws SqlException {
        // if already received an ENDQRYRM at open, check sqlcode and throw an exception
        checkAndThrowReceivedQueryTerminatingException();

        agent_.beginWriteChain(statement_);

        Section section = (generatedSection_ == null) ? statement_.getSection() : generatedSection_;

        // send the first CNTQRY to place cursor after last to retrieve the rowCount_.
        writePositioningFetch_(section, scrollOrientation_after__, 0);
        // if this is a non-dynamic rowset cursor, reposition the cursor to the first row in rowset
        // after the fetch after.  Cache info and cursor positions on the client should not change.
        if (isRowsetCursor_ && sensitivity_ != sensitivity_sensitive_dynamic__ && firstRowInRowset_ != 0) {
            writePositioningFetch_(section, scrollOrientation_absolute__, firstRowInRowset_);
        }

        agent_.flow(statement_);

        readPositioningFetch_();
        if (isRowsetCursor_ && sensitivity_ != sensitivity_sensitive_dynamic__ && firstRowInRowset_ != 0) {
            readPositioningFetch_();
        }

        agent_.endReadChain();

        // if rowCount_ is not updated, check for exceptions
        if (rowCount_ == -1) {
            checkAndThrowReceivedQueryTerminatingException();
        }

        if (isRowsetCursor_ && sensitivity_ != sensitivity_sensitive_dynamic__ && firstRowInRowset_ != 0) {
            absolutePosition_ = firstRowInRowset_;
        } else {
            absolutePosition_ = (maxRows_ == 0) ? rowCount_ + 1 : maxRows_ + 1;
        }
    }

    private void flowGetRowset(int orientation, long rowNumber) throws SqlException {
        // clear lobs before fetching rows
        cursor_.clearLobData_();
        cursor_.resetDataBuffer();
        agent_.beginWriteChain(statement_);
        
        writeScrollableFetch_((generatedSection_ == null) ? statement_.getSection() : generatedSection_,
                fetchSize_,
                orientation,
                rowNumber,
                true);  // true means to discard any pending partial
        // row and pending query blocks

        // reset the number of rows received to 0.
        // cannot use rowsRead_ here because this is reset every time a new rowset is fetched
        rowsReceivedInCurrentRowset_ = 0;

        agent_.flow(statement_);
        readScrollableFetch_();
        agent_.endReadChain();
    }

    private boolean getNextRowset() throws SqlException {
        // for rowset cursors or dynamic, non-rowset cursors
        if (isRowsetCursor_ || sensitivity_ == sensitivity_sensitive_dynamic__) {
            // check if the next row contains a +100 before fetching from the server.
            // since index starts from 0, the actual row number for the current row is (currentRowInRowset_+1)
            // and the actual row number for the next row is (currentRowInRowset_+2)
            int sqlcode = checkRowsetSqlca((int) currentRowInRowset_ + 2);
            if (sqlcode == 100) {
                isAfterLast_ = true;
                return false;
            }
            flowGetRowset(scrollOrientation_next__, 0);
        } else {
            // for all other cursors:
            //   sensitive static, insensitive, non-rowset cursors
            // if already afterLast, return false
            // if already on the last row and have received a +100, do not fetch again, return false
            if (resultSetContainsNoRows() || isAfterLastX()) {
                return false;
            } else if (firstRowInRowset_ + currentRowInRowset_ == lastRowInRowset_ &&
                    cursor_.allRowsReceivedFromServer()) {
                isAfterLast_ = true;
                setRowsetAfterLastEvent();
                return false;
            }

            // rowNumber to fetch is 1 if absolute position is equal to the last row
            // in the rowset
            long rowNumber = 1;
            int orientation = scrollOrientation_relative__;

            // Normally absolute position is equal to last row in the rowset, but in cases
            // of previous "update where current of"s, absolute position may be somewhere
            // else in the current rowset, thus rowNumber needs to be recalculated based on
            // where in the rowset the absolute position is.
            if (absolutePosition_ < lastRowInRowset_) {
                rowNumber = lastRowInRowset_ - absolutePosition_ + 1;
                absolutePosition_ = lastRowInRowset_;
            }
            // the following case happens when a getRowCount() is called and we flow a fetch after
            // to get the rowCount.
            else if (absolutePosition_ > lastRowInRowset_) {
                rowNumber = lastRowInRowset_ + 1;
                orientation = scrollOrientation_absolute__;
            }

            flowGetRowset(orientation, rowNumber);
        }

        parseRowset_();

        // If no row was received but received sqlcode +100, then the cursor is
        // positioned after last.
        if (rowsReceivedInCurrentRowset_ == 0 &&
            cursor_.allRowsReceivedFromServer()) {
            isAfterLast_ = true;
            setRowsetAfterLastEvent();
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustNextRowset();
        }
        currentRowInRowset_ = 0;
        return true;
    }

    private void adjustNextRowset() {
        firstRowInRowset_ = lastRowInRowset_ + 1;
        lastRowInRowset_ = lastRowInRowset_ + rowsReceivedInCurrentRowset_;
        setAbsolutePositionBasedOnAllRowsReceived();
        //currentRowInRowset_ = 0;
    }

    private boolean getPreviousRowset() throws SqlException {
        int orientation = scrollOrientation_relative__;
        long rowNumber = 0;
        boolean isAfterLast = false;

        // for rowset cursors or dynamic, non-rowset cursors
        if (isRowsetCursor_ || sensitivity_ == sensitivity_sensitive_dynamic__) {
            // check if we already received a +20237 before fetching from the server.
            if (currentRowInRowset_ == 0 && rowsetSqlca_ != null && rowsetSqlca_[0] != null &&
                    rowsetSqlca_[0].getSqlCode() == 20237) {
                isBeforeFirst_ = true;
                setRowsetBeforeFirstEvent();
                return false;
            }
            flowGetRowset(scrollOrientation_prior__, 0);
        } else {
            // for all other cursors:
            //   sensitive static, insensitive, non-rowset cursors
            if (resultSetContainsNoRows() || isBeforeFirstX()) {
                return false;
            }
            rowNumber = firstRowInRowset_ - absolutePosition_ - fetchSize_;
            isAfterLast = isAfterLastX();
            if (isFirstX()) {
                rowNumber = 0;
                orientation = scrollOrientation_absolute__;
            }
            // If the cursor is after last, fetch the last rowset which includes the last row.
            else if (isAfterLast) {
                rowNumber = (-1) * fetchSize_;
            }
            // if the distance from the absolute position is less than fetch size, fetch from row 1
            if (rowNumber * (-1) >= absolutePosition_) {
                rowNumber = 1;
                orientation = scrollOrientation_absolute__;
            }
            
            // If afterLast and maxRows > 0, go backward from maxRows and not 
            // from last row in the resultSet
            if (maxRows_ > 0 && orientation == scrollOrientation_relative__ && isAfterLast) {
                rowNumber += maxRows_ + 1;
                orientation = scrollOrientation_absolute__;
            }
            
            flowGetRowset(orientation, rowNumber);
        }

        parseRowset_();

        // If no row was received but received sqlcode +100, then the cursor is
        // positioned before first.
        if (rowsReceivedInCurrentRowset_ == 0 &&
            cursor_.allRowsReceivedFromServer()) {
            isBeforeFirst_ = true;
            setRowsetBeforeFirstEvent();
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustPreviousRowset(orientation, rowNumber, isAfterLast);
        } else {
            currentRowInRowset_ = rowsReceivedInCurrentRowset_ - 1;
        }
        return true;
    }

    private void adjustPreviousRowset(int orientation, long rowNumber, boolean isAfterLastRow) {
        if (orientation == scrollOrientation_absolute__ && rowNumber == 1) {
            // Subtracting 2 because the currentRowInRowset_ index starts at 0, and all
            // the other indexes starts at 1.
            currentRowInRowset_ = (isAfterLastRow) ? absolutePosition_ - 2 : firstRowInRowset_ - 2;
            firstRowInRowset_ = 1;
            lastRowInRowset_ = rowsReceivedInCurrentRowset_;
            absolutePosition_ = (isAfterLastRow) ? lastRowInRowset_ + 1 : lastRowInRowset_;
        } else {
            if (maxRows_ == 0)
                lastRowInRowset_ = (isAfterLastRow) ? rowCount_ : firstRowInRowset_ - 1;
            else
                lastRowInRowset_ = (isAfterLastRow) ? maxRows_ : firstRowInRowset_ - 1;
            firstRowInRowset_ = lastRowInRowset_ - rowsReceivedInCurrentRowset_ + 1;
            absolutePosition_ = lastRowInRowset_;
            currentRowInRowset_ = lastRowInRowset_ - firstRowInRowset_;
        }
    }

    private boolean getAbsoluteRowset(long row) throws SqlException {
        int orientation = scrollOrientation_absolute__;
        // absolute(0) is not allowed on a rowset cursor, will get -644 from the server
        // remap to fetch before.
        if (isRowsetCursor_ && row == 0) {
            orientation = scrollOrientation_before__;
        } else if (sensitivity_ != sensitivity_sensitive_dynamic__ && row < 0) {
            row = 0;
        }

        flowGetRowset(orientation, row);
        parseRowset_();

        // If no row was received but received sqlcode +100, then the cursor is
        // positioned after last or before first.
        if ((rowsReceivedInCurrentRowset_ == 0 &&
             cursor_.allRowsReceivedFromServer()) ||
                orientation == scrollOrientation_before__) {
            if (row > 0) {
                setRowsetAfterLastEvent();
                isAfterLast_ = true;
            } else {
                setRowsetBeforeFirstEvent();
                isBeforeFirst_ = true;
            }
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustAbsoluteRowset(row);
        }
        currentRowInRowset_ = 0;
        return true;
    }

    private void adjustAbsoluteRowset(long rowNumber) {
        firstRowInRowset_ = rowNumber;
        lastRowInRowset_ = firstRowInRowset_ + rowsReceivedInCurrentRowset_ - 1;
        setAbsolutePositionBasedOnAllRowsReceived();
        //currentRowInRowset_ = 0;
    }

    private boolean getRelativeRowset(long rows) throws SqlException {
        if (rows == 0 &&
                (cursor_.allRowsReceivedFromServer() ||
                 absolutePosition_ > rowCount_)) {
            setRowsetAfterLastEvent();
            isAfterLast_ = true;
            return false;
        }

        flowGetRowset(scrollOrientation_relative__, rows);
        parseRowset_();

        if (rowsReceivedInCurrentRowset_ == 0 &&
            cursor_.allRowsReceivedFromServer()) {
            if (rows > 0) {
                setRowsetAfterLastEvent();
                isAfterLast_ = true;
            } else {
                setRowsetBeforeFirstEvent();
                isBeforeFirst_ = true;
            }
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustRelativeRowset(rows);
        }
        currentRowInRowset_ = 0;
        return true;
    }

    private void adjustRelativeRowset(long rowNumber) {
        firstRowInRowset_ = absolutePosition_ + rowNumber;
        lastRowInRowset_ = firstRowInRowset_ + rowsReceivedInCurrentRowset_ - 1;
        setAbsolutePositionBasedOnAllRowsReceived();
    }

    private boolean getFirstRowset() throws SqlException {
        flowGetRowset(scrollOrientation_absolute__, 1);
        parseRowset_();

        // If no row was received but received sqlcode +100, then no row in the result set
        if (rowsReceivedInCurrentRowset_ == 0 &&
            cursor_.allRowsReceivedFromServer()) {
            resetRowsetFlags();
            this.setRowsetNoRowsEvent();
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustFirstRowset();
        }
        currentRowInRowset_ = 0;
        return true;
    }

    private void adjustFirstRowset() {
        firstRowInRowset_ = 1;
        lastRowInRowset_ = rowsReceivedInCurrentRowset_;
        setAbsolutePositionBasedOnAllRowsReceived();
        //currentRowInRowset_ = 0;
    }

    private boolean getLastRowset(long row) throws SqlException {
        if (sensitivity_ != sensitivity_sensitive_dynamic__ && rowCount_ == 0) {
            isAfterLast_ = false;
            isBeforeFirst_ = false;
            setRowsetNoRowsEvent();
            return false;
        } else if (isRowsetCursor_ || sensitivity_ == sensitivity_sensitive_dynamic__) {
            flowGetRowset(scrollOrientation_last__, 0);
        } else {
            // If fetchSize_ is smaller than the total number of rows in the ResultSet,
            // then fetch one rowset of fetchSize_ number of rows.  Otherwise, we will
            // fetch all rows in the ResultSet, so start fetching from row 1.
            long rowNumber;
            if (maxRows_ == 0) {
                rowNumber = (fetchSize_ < row) ? ((-1) * fetchSize_) : 1;
            } else {
                rowNumber = (fetchSize_ < row) ? (maxRows_ - fetchSize_) + 1 : 1;
            }
            flowGetRowset(scrollOrientation_absolute__, rowNumber);
        }
        parseRowset_();

        if (rowsReceivedInCurrentRowset_ == 0 &&
            cursor_.allRowsReceivedFromServer()) {
            isAfterLast_ = true;
            setRowsetAfterLastEvent();
            return false;
        }

        // adjust the cursor positions for sensitive static or insensitive cursors only
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustLastRowset(row);
        } else {
            currentRowInRowset_ = rowsReceivedInCurrentRowset_ - 1;
        }
        return true;
    }

    private void adjustLastRowset(long row) {
        lastRowInRowset_ = row;
        firstRowInRowset_ = lastRowInRowset_ - rowsReceivedInCurrentRowset_ + 1;
        if (firstRowInRowset_ <= 0) {
            firstRowInRowset_ = 1;
        }
        setAbsolutePositionBasedOnAllRowsReceived();
        currentRowInRowset_ = lastRowInRowset_ - firstRowInRowset_;
    }

    private boolean getRefreshRowset() throws SqlException {
        if (isRowsetCursor_) {
            flowGetRowset(scrollOrientation_current__, 0);
        } else {
            flowGetRowset(scrollOrientation_relative__, (-1) * (absolutePosition_ - firstRowInRowset_));
        }

        parseRowset_();

        // Rowset indexes do not change when rowset is refreshed.
        // The only exception is absolutePosition_.  It may be different after the refresh.
        if (sensitivity_ != sensitivity_sensitive_dynamic__) {
            adjustRefreshRowset();
        }
        return true;
    }

    private void adjustRefreshRowset() {
        setAbsolutePositionBasedOnAllRowsReceived();
        updateColumnInfoFromCache();
    }

    private void setAbsolutePositionBasedOnAllRowsReceived() {
        absolutePosition_ = (cursor_.allRowsReceivedFromServer()) ?
                lastRowInRowset_ + 1 : lastRowInRowset_;
    }

    // ------------------------------- abstract box car methods --------------------------------------
    public abstract void writeFetch_(Section section) throws SqlException;

    public abstract void readFetch_() throws SqlException;


    public abstract void writeScrollableFetch_(Section section,
                                               int fetchSize, // need to send fetchSize in case when we get an
                                               // incomplete rowset, the fetchSize is the remaining
                                               // number of the rows in the rowset.
                                               int orientation,
                                               long rowToFetch,
                                               boolean resetQueryBlocks) throws SqlException;

    public abstract void readScrollableFetch_() throws SqlException;

    public abstract void writePositioningFetch_(Section section,
                                                int orientation,
                                                long rowToFetch) throws SqlException;

    public abstract void readPositioningFetch_() throws SqlException;

    public abstract void writeCursorClose_(Section section) throws SqlException;

    public abstract void readCursorClose_() throws SqlException;

    protected abstract void parseRowset_() throws SqlException;

    public abstract void setFetchSize_(int rows);

    /**
     * Method that is invoked by <code>closeX()</code> before the
     * result set is actually being closed. Subclasses may override
     * this method if work needs to be done before closing.
     *
     * @exception SqlException
     */
    protected abstract void preClose_() throws SqlException;

    public ConnectionCallbackInterface getConnectionCallbackInterface() {
        return connection_;
    }

    public StatementCallbackInterface getStatementCallbackInterface() {
        return statement_;
    }

    public void expandRowsetSqlca() {
        // rowsetSqlca_ index starts from 1.  entry 0 is reserved for warning +20237
        // if rowset size is n, then the (n+1)th entry is reserved for the +100 if one is received.
        // thus the size of the rowsetSqlca_ needs to be fetchSize_+2
        if (isRowsetCursor_ &&
                (rowsetSqlca_ == null || rowsetSqlca_.length < fetchSize_ + 2)) {
            rowsetSqlca_ = new Sqlca[fetchSize_ + 2];
        }
    }

    private final int checkRowsetSqlca() throws SqlException {
        return checkRowsetSqlca((int) currentRowInRowset_ + 1);
    }

    private final int checkRowsetSqlca(int row) throws SqlException {
        int sqlcode = 0;
        if (!isRowsetCursor_ || rowsetSqlca_ == null || rowsetSqlca_[row] == null) {
            return sqlcode;
        }

        Sqlca sqlca = rowsetSqlca_[row];
        if (sqlca != null) {
            sqlcode = sqlca.getSqlCode();
            if (sqlcode < 0) {
                throw new SqlException(agent_.logWriter_, sqlca);
            } else if (sqlcode > 0 && (sqlcode != 100 && sqlcode != +20237)) {
                accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
            }
        }
        return sqlcode;
    }

    private void resetRowsetFlags() {
        isBeforeFirst_ = false;
        isAfterLast_ = false;
        isFirst_ = false;
        isLast_ = false;
    }

    private void resetRowsetSqlca() {
        if (rowsetSqlca_ != null) {
            Arrays.fill(rowsetSqlca_, null);
        }
    }
    
    
    private CloseFilterInputStream createCloseFilterInputStream(InputStream is)
            throws SqlException {
        
        if(is == null){
            return null;
        }

        if( currentStream == is ){
            return currentStream;
        }
        
        closeOpenStreams();
        
        currentStream = new CloseFilterInputStream(is);
        
        return currentStream;
        
    }
    
    
    /**
     * Closes the current stream, if there is one.
     * <p>
     * Note that streams are implicitly closed when the next value is fetched.
     *
     * @throws SqlException if closing the stream fails
     */
    private void closeOpenStreams() throws SqlException {
        // There will be zero or one stream to close (both cannot be current).
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException ioe) {                
                throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.JAVA_EXCEPTION), 
                        ioe, "java.io.IOException", ioe.getMessage());
            }
            currentStream = null;
        }

        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException ioe) {                
                throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.JAVA_EXCEPTION), 
                        ioe, "java.io.IOException", ioe.getMessage());
            }
            currentReader = null;
        }
    }
    
    
    /**
     * Mark a column as already having a stream or LOB accessed from it.
     * If the column was already accessed, throw an exception.
     *
     * @param columnIndex 1-based column index
     * @throws SQLException if the column has already been accessed
     */
    private void useStreamOrLOB(int columnIndex) throws SqlException {
        checkLOBMultiCall(columnIndex);
        columnUsedFlags_[columnIndex - 1] = true;
    }

    /**
     * Checks if a stream or a LOB object has already been created for the
     * specified LOB column.
     * <p>
     * Accessing a LOB column more than once is not forbidden by the JDBC
     * specification, but the Java API states that for maximum portability,
     * result set columns within each row should be read in left-to-right order,
     * and each column should be read only once. The restriction was implemented
     * in Derby due to complexities with the positioning of store streams when
     * the user was given multiple handles to the stream.
     *
     * @param columnIndex 1-based index of the LOB column
     * @throws SqlException if the column has already been accessed
     */
    private void checkLOBMultiCall(int columnIndex)
            throws SqlException {
        if (columnUsedFlags_ == null) {
            columnUsedFlags_ = new boolean[resultSetMetaData_.columns_];
        }
        if (columnUsedFlags_[columnIndex - 1]) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.LANG_STREAM_RETRIEVED_ALREADY));
        }
    }

    /**
     * Clears the flags for used columns, typically invoked when changing the
     * result set position.
     */
    private void unuseStreamsAndLOBs() {
        if(columnUsedFlags_ != null){
            Arrays.fill(columnUsedFlags_, false);
        }
    }

    private SQLException jdbc3MethodNotSupported()
    {
        return new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED)).
            getSQLException();
    }
    
    // -------------------------- JDBC 4.0 --------------------------

    /**
     * Retrieves the holdability for this <code>ResultSet</code>
     * object.
     *
     * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     * or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @exception SQLException if a database error occurs
     */
    public final int getHoldability() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getHoldability");
        }
        try {
            checkForClosedResultSet("getHoldability");
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getHoldability",
                                        resultSetHoldability_);
        }
        return resultSetHoldability_;
    }
    
    /**
     * Checks whether this <code>ResultSet</code> object has been
     * closed, either automatically or because <code>close()</code>
     * has been called.
     *
     * @return <code>true</code> if the <code>ResultSet</code> is
     * closed, <code>false</code> otherwise
     * @exception SQLException if a database error occurs
     */
    public final boolean isClosed() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "isClosed");
        }
        final boolean isClosed = !openOnClient_;
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "isClosed", isClosed);
        }
        return isClosed;
    }

    /**
     * Updates the designated column with an ascii stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateAsciiStream",
                        columnIndex, x);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateAsciiStream");
                updateColumn(columnIndex,
                        agent_.crossConverters_.setObjectFromCharacterStream(
                            resultSetMetaData_.types_[columnIndex -1],
                            x,
                            Cursor.ISO_8859_1,
                            CrossConverters.UNKNOWN_LENGTH));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateAsciiStream(int columnIndex, InputStream x,
                    long length) throws SQLException {
        if(length > Integer.MAX_VALUE)
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CLIENT_LENGTH_OUTSIDE_RANGE_FOR_DATATYPE),
                    length, Integer.MAX_VALUE).getSQLException();
        else
            updateAsciiStream(columnIndex,x,(int)length);
    }

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value 
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBinaryStream",
                        columnIndex, x);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateBinaryStream");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObjectFromBinaryStream(
                                    resultSetMetaData_.types_[columnIndex -1],
                                    x,
                                    CrossConverters.UNKNOWN_LENGTH));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateBinaryStream(int columnIndex, InputStream x,
        long length) throws SQLException {
         if(length > Integer.MAX_VALUE)
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CLIENT_LENGTH_OUTSIDE_RANGE_FOR_DATATYPE),
                    length, Integer.MAX_VALUE).getSQLException();
        else
            updateBinaryStream(columnIndex,x,(int)length);

     }

    /**
     * Updates the designated column using the given input stream.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBlob(int columnIndex, InputStream x)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBlob",
                        columnIndex, x);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateBlob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                    resultSetMetaData_.types_[columnIndex -1],
                                    new ClientBlob(agent_, x)));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateCharacterStream(int columnIndex, Reader reader)
            throws SQLException {
        synchronized (connection_) {
            try {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateCharacterStream",
                            columnIndex, reader);
                }
                checkUpdatePreconditions(columnIndex, "updateCharacterStream");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                    resultSetMetaData_.types_[columnIndex -1],
                                    reader,
                                    CrossConverters.UNKNOWN_LENGTH));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param length
     *            the length of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateCharacterStream(int columnIndex, Reader x,
                    long length) throws SQLException {
        if(length > Integer.MAX_VALUE)
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.CLIENT_LENGTH_OUTSIDE_RANGE_FOR_DATATYPE),
                    length, Integer.MAX_VALUE).getSQLException();
        else
            updateCharacterStream(columnIndex,x,(int)length);
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object. 
     * The data will be read from the stream as needed until end-of-stream is
     * reached. The JDBC driver will do any necessary conversion from UNICODE
     * to the database char format.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader an object that contains the data to set the parameter
     *      value to. 
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateClob",
                        columnIndex, reader);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateClob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                 resultSetMetaData_.types_[columnIndex -1], 
                                 new ClientClob(agent_, reader)));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column with an ascii stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateAsciiStream(String columnName, InputStream x)
            throws SQLException {
        try {
            updateAsciiStream(findColumnX(columnName, "updateAsciiStream"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param length
     *            of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateAsciiStream(String columnName, InputStream x,
                    long length) throws SQLException {
        try {
            updateAsciiStream(findColumnX(columnName, "updateAsciiStream"), x, length);
        }
        catch(SqlException sqle) {
            throw sqle.getSQLException();
        }
    }

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param x the new column value 
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        try {
            updateBinaryStream(findColumnX(columnLabel, "updateBinaryStream"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param length
     *            of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateBinaryStream(String columnName, InputStream x,
                    long length) throws SQLException {
        try {
            updateBinaryStream(findColumnX(columnName, "updateBinaryStream"), x, length);
        }
        catch(SqlException sqle) {
            throw sqle.getSQLException();
        }
    }

    /**
     * Updates the designated column using the given input stream.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBlob(String columnLabel, InputStream x)
            throws SQLException {
        try {
            updateBlob(findColumnX(columnLabel, "updateBlob"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param reader the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        try {
            updateCharacterStream(findColumnX(columnLabel, "updateCharacterStream"), reader);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the current
     * row, or the insert row. The updateXXX() methods do not update the
     * underlying database, instead the updateRow() or insertRow() methods are
     * called to update the database.
     *
     * @param columnName
     *            the name of the column
     * @param reader
     *            the new column value
     * @param length
     *            length of the stream
     * @exception SQLException
     *                if a database-access error occurs
     */
    public void updateCharacterStream(String columnName, Reader reader,
        long length) throws SQLException {
         try {
             updateCharacterStream(findColumnX(columnName, "updateCharacterStream"), reader, length);
         }
         catch(SqlException sqle) {
             throw sqle.getSQLException();
         }
     }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object. 
     * The data will be read from the stream as needed until end-of-stream is
     * reached. The JDBC driver will do any necessary conversion from UNICODE
     * to the database char format.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param reader an object that contains the data to set the parameter
     *      value to. 
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        try {
            updateClob(findColumnX(columnLabel, "updateClob"), reader);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }
      
    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @exception SQLException if the columnIndex is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not
     * support this method
     */
    public void updateClob(int columnIndex, Reader x, long length)
                throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateClob",
                        columnIndex, x, (int)length);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateClob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                 resultSetMetaData_.types_[columnIndex -1],
                                 new ClientClob(agent_, x, (int)length)));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the label for the column specified with the
     * SQL AS clause.  If the SQL AS clause was not specified,
     * then the label is the name of the column
     * @param x An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @exception SQLException if the columnLabel is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not
     * support this method
     */

    public void updateClob(String columnName, Reader x, long length)
                           throws SQLException {
        try {
            updateClob(findColumnX(columnName, "updateClob"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }
   
    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the label for the column specified with the SQL AS
     *                    clause. If the SQL AS clause was not specified, then
     *                    the label is the name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateClob(int columnIndex, Clob x)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateClob",
                        columnIndex, x);
            }
            try {
                checkUpdatePreconditions(columnIndex, "updateClob");
                updateColumn(columnIndex,
                             agent_.crossConverters_.setObject(
                                 resultSetMetaData_.types_[columnIndex -1],
                                 x));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS
     *                    clause. If the SQL AS clause was not specified, then
     *                    the label is the name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid;
     * if a database access error occurs;
     * the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * or this method is called on a closed result set
     */
    public void updateClob(String columnLabel, Clob x)
            throws SQLException {
        try {
            updateClob(findColumnX(columnLabel, "updateClob"), x);
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Marks the LOB at the specified column as published.
     * <p>
     * When a LOB is marked as published, the release mechanism will not be
     * invoked by the result set. It is expected that the code accessing the
     * LOB releases the locator when it is done with the LOB, or that the
     * commit/rollback handles the release.
     *
     * @param index 1-based column index
     */
    public final void markLOBAsPublished(int index) {
        this.lobState.markAsPublished(index);
    }

    /**
     * Initializes the LOB state tracker.
     * <p>
     * The state tracker is used to free LOB locators on the server. If the
     * server doesn't support locators, or there are no LOBs in the result set,
     * a no-op tracker will be used.
     */
    final void createLOBColumnTracker() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(this.lobState == null,
                    "LOB state tracker already initialized.");
        }
        if (this.connection_.supportsSessionDataCaching() &&
                this.resultSetMetaData_.hasLobColumns()) {
            final int columnCount = this.resultSetMetaData_.columns_;
            int lobCount = 0;
            int[] tmpIndexes = new int[columnCount];
            boolean[] tmpIsBlob = new boolean[columnCount];
            for (int i=0; i < columnCount; i++) {
                int type = this.resultSetMetaData_.types_[i];
                if (type == ClientTypes.BLOB || type == ClientTypes.CLOB) {
                    tmpIndexes[lobCount] = i +1; // Convert to 1-based index.
                    tmpIsBlob[lobCount++] = (type == ClientTypes.BLOB);
                }
            }
            // Create a tracker for the LOB columns found.
            int[] lobIndexes = new int[lobCount];
            boolean[] isBlob = new boolean[lobCount];
            System.arraycopy(tmpIndexes, 0, lobIndexes, 0, lobCount);
            System.arraycopy(tmpIsBlob, 0, isBlob, 0, lobCount);
            this.lobState = new LOBStateTracker(lobIndexes, isBlob, true);
        } else {
            // Use a no-op state tracker to simplify code expecting a tracker.
            this.lobState = LOBStateTracker.NO_OP_TRACKER;
        }
    }

    public Reader getNCharacterStream(int columnIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNCharacterStream(int)");
    }

    public Reader getNCharacterStream(String columnName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNCharacterStream(String)");
    }

    public String getNString(int columnIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(int)");
    }

    public String getNString(String columnName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(String)");
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (int)");
    }


    public RowId getRowId(String columnName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (String)");
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateRowId (int, RowId)");
    }

    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateRowId (String, RowId)");
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNString (int, String)");
    }

    public void updateNString(String columnName, String nString) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNString (String, String)");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "updateNCharacterStream(int,Reader,long)");
    }

    public void updateNCharacterStream(String columnName, Reader x, long length)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "updateNCharacterStream(String,Reader,long)");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNClob (int, NClob)");
    }

    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNClob (String, NClob)");
    }

    public NClob getNClob(int i) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getNClob (int)");
    }

    public NClob getNClob(String colName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getNClob (String)");
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (int)");
    }

    public SQLXML getSQLXML(String colName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (String)");
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateSQLXML (int, SQLXML)");
    }

    public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateSQLXML (String, SQLXML)");
    }

    /**
     * Returns false unless <code>interfaces</code> is implemented
     *
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or
     *                                directly or indirectly wraps an object
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining
     *                                whether this is a wrapper for an object
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        try {
            checkForClosedResultSet("isWrapperFor");
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        return interfaces.isInstance(this);
    }

    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object if found that implements the
     * interface
     */
    public <T> T unwrap(Class<T> interfaces)
                                   throws SQLException {
        try {
            checkForClosedResultSet("unwrap");
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                interfaces).getSQLException();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }


     /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnIndex -
     *            the first column is 1, the second is 2
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */
    public void updateNClob(int columnIndex, Reader x, long length)
                throws SQLException {
        throw SQLExceptionFactory.notImplemented("updateNClob(int,Reader,long)");
    }

    /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateNClob(String columnName, InputStream x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateNClob(String,InputStream,long)");
     }

     /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateNClob(String columnName, Reader x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateNClob(String,Reader,long)");
     }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Retrieve the column as an object of the desired type.
     */
    public  <T> T getObject( int columnIndex, Class<T> type )
            throws SQLException
    {
        try {
            checkForClosedResultSet("getObject");
        } catch (SqlException se) {
            throw se.getSQLException();
        }

        // closeCloseFilterInputStream() should be called by all of the
        // more specific methods to which we forward this call

        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getObject", columnIndex );
        }

        if ( type == null )
        {
            throw mismatchException( "NULL", columnIndex );
        }

        Object   retval;

        if ( String.class.equals( type ) ) { retval = getString( columnIndex ); }
        else if ( BigDecimal.class.equals( type ) ) { retval = getBigDecimal( columnIndex ); }
        else if ( Boolean.class.equals( type ) ) { retval = Boolean.valueOf( getBoolean(columnIndex ) ); }
        else if ( Byte.class.equals( type ) ) { retval = Byte.valueOf( getByte( columnIndex ) ); }
        else if ( Short.class.equals( type ) ) { retval = Short.valueOf( getShort( columnIndex ) ); }
        else if ( Integer.class.equals( type ) ) { retval = Integer.valueOf( getInt( columnIndex ) ); }
        else if ( Long.class.equals( type ) ) { retval = Long.valueOf( getLong( columnIndex ) ); }
        else if ( Float.class.equals( type ) ) { retval = Float.valueOf( getFloat( columnIndex ) ); }
        else if ( Double.class.equals( type ) ) { retval = Double.valueOf( getDouble( columnIndex ) ); }
        else if ( Date.class.equals( type ) ) { retval = getDate( columnIndex ); }
        else if ( Time.class.equals( type ) ) { retval = getTime( columnIndex ); }
        else if ( Timestamp.class.equals( type ) ) { retval = getTimestamp( columnIndex ); }

        else if ( Blob.class.equals( type ) ) {
            retval = getBlob( columnIndex );

        } else if ( Clob.class.equals( type ) ) {
            retval = getClob( columnIndex );

        } else if ( type.isArray() &&
                    type.getComponentType().equals( byte.class ) ) {
            retval = getBytes( columnIndex );

        } else { retval = getObject( columnIndex );
        }

        if ( wasNull() ) { retval = null; }

        if ( (retval == null) || (type.isInstance( retval )) ) { return type.cast( retval ); }

        throw mismatchException( type.getName(), columnIndex );
    }
    private SQLException    mismatchException( String targetTypeName, int columnIndex )
        throws SQLException
    {
        String sourceTypeName = getMetaData().getColumnTypeName( columnIndex );
        ClientMessageId cmi = new ClientMessageId( SQLState.LANG_DATA_TYPE_GET_MISMATCH );
        SqlException se = new SqlException( agent_.logWriter_, cmi, targetTypeName, sourceTypeName );

        return se.getSQLException();
    }

    public  <T> T getObject( String columnName, Class<T> type )
            throws SQLException
    {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", columnName);
            }
            return getObject( findColumnX(columnName, "getObject"), type );
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

}

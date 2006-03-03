/*

   Derby - Class org.apache.derby.client.am.ResultSet

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

import java.io.IOException;
import java.sql.SQLException;
import org.apache.derby.shared.common.reference.SQLState;

public abstract class ResultSet implements java.sql.ResultSet,
        ResultSetCallbackInterface,
        UnitOfWorkListener {
    //---------------------navigational members-----------------------------------

    public Statement statement_;
    public ColumnMetaData resultSetMetaData_; // As obtained from the SQLDA
    private SqlWarning warnings_;
    public Cursor cursor_;
    protected Agent agent_;

    public Section generatedSection_ = null;

	private CloseFilterInputStream is_;

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    // Alias for statement_.connection
    public final Connection connection_;

    //----------------------------- constants ------------------------------------

    public final static int scrollOrientation_relative__ = 1;
    public final static int scrollOrientation_absolute__ = 2;
    public final static int scrollOrientation_after__ = 3;
    public final static int scrollOrientation_before__ = 4;
    public final static int scrollOrientation_prior__ = 5;
    public final static int scrollOrientation_first__ = 6;
    public final static int scrollOrientation_last__ = 7;
    public final static int scrollOrientation_current__ = 8;
    public final static int scrollOrientation_next__ = 0;

    public final static int updatability_unknown__ = 0;
    public final static int updatability_readOnly__ = 1;
    public final static int updatability_delete__ = 2;
    public final static int updatability_update__ = 4;

    public final static int sensitivity_unknown__ = 0;
    public final static int sensitivity_insensitive__ = 1;
    public final static int sensitivity_sensitive_static__ = 2;
    public final static int sensitivity_sensitive_dynamic__ = 3;

    static final private int WAS_NULL = 1;
    static final private int WAS_NOT_NULL = 2;
    static final private int WAS_NULL_UNSET = 0;

    static final public int NEXT_ROWSET = 1;
    static final public int PREVIOUS_ROWSET = 2;
    static final public int ABSOLUTE_ROWSET = 3;
    static final public int FIRST_ROWSET = 4;
    static final public int LAST_ROWSET = 5;
    static final public int RELATIVE_ROWSET = 6;
    static final public int REFRESH_ROWSET = 7;
    //  determines if a cursor is a:
    //    Return to Client - not to be read by the stored procedure only by client
    //    Return to Caller
    public static final byte DDM_RETURN_CALLER = 0x01;
    public static final byte DDM_RETURN_CLIENT = 0x02;

    //-----------------------------state------------------------------------------

    // Note:
    //   Result set meta data as described by the SQLDA is described in ColumnMetaData.

    private int wasNull_ = WAS_NULL_UNSET;

    // ResultSet returnability for Stored Procedure cursors
    //  determines if a cursor is a:
    //    Return to Client - not to be read by the stored procedure only by client
    //    Return to Caller - only calling JSP can read it, not the client
    protected byte rsReturnability_ = DDM_RETURN_CLIENT;

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
    public boolean isValidCursorPosition_ = false;

    public boolean cursorHold_;

    // query instance identifier returned on open by uplevel servers.
    // this value plus the package information uniquely identifies a query.
    // it is 64 bits long and it's value is unarchitected.
    public long queryInstanceIdentifier_ = 0;

    public int resultSetType_;
    public int resultSetConcurrency_;
    public int resultSetHoldability_;
    public boolean scrollable_ = false;
    public int sensitivity_;
    public boolean isRowsetCursor_ = false;
    public boolean isBeforeFirst_ = true;
    public boolean isAfterLast_ = false;
    public boolean isFirst_ = false;
    public boolean isLast_ = false;
    public boolean rowsetContainsLastRow_ = false;
    public Sqlca[] rowsetSqlca_;
    public int fetchSize_;
    public int fetchDirection_;

    public long rowCount_ = -1;

    protected long absolutePosition_ = 0;       // absolute position of the current row
    protected long firstRowInRowset_ = 0;       // absolute position of the first row in the current rowset
    protected long lastRowInRowset_ = 0;        // absolute position of the last row in the current rowset
    protected long currentRowInRowset_ = -1;     // relative position to the first row in the current rowsetwel

    protected long absoluteRowNumberForTheIntendedRow_;

    // This variable helps keep track of whether cancelRowUpdates() should have any effect.
    protected boolean updateRowCalled_ = false;
    private boolean isOnInsertRow_ = false;  // reserved for later
    protected boolean isOnCurrentRow_ = true;
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

    public PreparedStatement preparedStatementForUpdate_;
    public PreparedStatement preparedStatementForDelete_;
    public PreparedStatement preparedStatementForInsert_;

    // Nesting level of the result set in a stored procedure
    public int nestingLevel_ = -1;

    // Whenever a commit occurs, it unpositions the cursor on the server.  We need to
    // reposition the cursor before updating/deleting again.  This flag will be set to true
    // whenever a commit happens, and reset to false again after we repositoin the cursor.
    public boolean cursorUnpositionedOnServer_ = false;
    
    // Keep maxRows in the ResultSet, so that changes to maxRow in the statement
    // do not affect the resultSet after it has been created
    private int maxRows_;
    
    private boolean[] streamUsedFlags_;
    
    //---------------------constructors/finalizer---------------------------------

    protected ResultSet(Agent agent,
                        Statement statement,
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
        fetchSize_ = statement_.fetchSize_;

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
                    new MessageId(SQLState.INVALID_RESULTSET_TYPE),
                        new Integer(statement_.resultSetType_),
                        new Integer(resultSetType_)));
        }

        // Only set the warning if actual resultSetConcurrency returned by the server is
        // less than the application requested resultSetConcurrency.
        // CONCUR_READ_ONLY = 1007
        // CONCUR_UPDATABLE = 1008
        if (resultSetConcurrency_ < statement_.resultSetConcurrency_) {
            statement_.accumulateWarning(
                new SqlWarning(
                    agent_.logWriter_,
                    new MessageId(SQLState.INVALID_RESULTSET_CONCURRENCY),
                        new Integer(resultSetConcurrency_),
                        new Integer(statement_.resultSetConcurrency_)));
                
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
        checkForClosedResultSet();
        clearWarningsX();

        if (isOnInsertRow_) {
            isOnInsertRow_ = false;
            isOnCurrentRow_ = true;
        }
        
        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();
	
	unuseStreams();

        // for TYPE_FORWARD_ONLY ResultSet, just call cursor.next()
        if (resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
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

    public void nullDataForGC() {
        // This method is called by closeX().  We cannot call this if cursor is cached,
        // otherwise it will cause NullPointerException's when cursor is reused.
        // Cursor is only cached for PreparedStatement's.
        if (cursor_ != null && !statement_.isPreparedStatement_) {
            cursor_.nullDataForGC();
        }
        cursor_ = null;
        resultSetMetaData_ = null;
    }

    void flowCloseAndAutoCommitIfNotAutoCommitted() throws SqlException {
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
            writeCursorClose_(statement_.section_);
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
            writeCursorClose_(statement_.section_);
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

    // precondition: transaction state allows for auto commit to generate flow
    private void writeAutoCommitIfNotAutoCommitted() throws SqlException {
        if (connection_.autoCommit_ && !autoCommitted_) {
            connection_.writeAutoCommit();
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
            checkForClosedResultSet();

            if (wasNull_ == ResultSet.WAS_NULL_UNSET) {
                throw new SqlException(agent_.logWriter_, "Invalid operation: wasNull() called with no data retrieved");
            }

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "wasNull", wasNull_ == ResultSet.WAS_NULL);
            }
            return wasNull_ == ResultSet.WAS_NULL;
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBoolean", column);
            }
            checkGetterPreconditions(column);
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getByte", column);
            }
            checkGetterPreconditions(column);
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getShort", column);
            }
            checkGetterPreconditions(column);
            short result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Short) agent_.crossConverters_.setObject(
                            java.sql.Types.SMALLINT,
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getInt", column);
            }
            checkGetterPreconditions(column);
            int result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Integer) agent_.crossConverters_.setObject(
                            java.sql.Types.INTEGER,
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getLong", column);
            }
            checkGetterPreconditions(column);
            long result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Long) agent_.crossConverters_.setObject(
                            java.sql.Types.BIGINT,
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getFloat", column);
            }
            checkGetterPreconditions(column);
            float result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if ((isOnInsertRow_ && updatedColumns_[column - 1] == null)) {
                    result = 0;
                } else {
                    result = ((Float) agent_.crossConverters_.setObject(
                            java.sql.Types.REAL,
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDouble", column);
            }
            checkGetterPreconditions(column);
            double result = 0;
            if (wasNonNullSensitiveUpdate(column) || isOnInsertRow_) {
                if (isOnInsertRow_ && updatedColumns_[column - 1] == null) {
                    result = 0;
                } else {
                    result = ((Double) agent_.crossConverters_.setObject(
                            java.sql.Types.DOUBLE,
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
    public java.math.BigDecimal getBigDecimal(int column, int scale) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getBigDecimal", column, scale);
            }
            checkGetterPreconditions(column);
            java.math.BigDecimal result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result =
                        ((java.math.BigDecimal) agent_.crossConverters_.setObject(java.sql.Types.DECIMAL,
                                updatedColumns_[column - 1])).setScale(scale, java.math.BigDecimal.ROUND_DOWN);
            } else {
                result =
                        isNull(column) ? null : cursor_.getBigDecimal(column).setScale(scale, java.math.BigDecimal.ROUND_DOWN);
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
    public java.math.BigDecimal getBigDecimal(int column) throws SQLException {
        try
        {

            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBigDecimal", column);
            }
            checkGetterPreconditions(column);
            java.math.BigDecimal result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result =
                        (java.math.BigDecimal) agent_.crossConverters_.setObject(java.sql.Types.DECIMAL,
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
    public java.sql.Date getDate(int column) throws SQLException {
	    try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", column);
            }
            checkGetterPreconditions(column);
            java.sql.Date result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (java.sql.Date) agent_.crossConverters_.setObject(java.sql.Types.DATE, updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getDate(column);
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
    public java.sql.Date getDate(int column, java.util.Calendar calendar) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", column, calendar);
            }
            if (calendar == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Date date = getDate(column);
            if (date != null) {
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(calendar.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(date);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(date);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                date.setTime(date.getTime() - timeZoneOffset);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDate", date);
            }
            return date;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }            
    }

    // Live life on the edge and run unsynchronized
    public java.sql.Time getTime(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", column);
            }
            checkGetterPreconditions(column);
            java.sql.Time result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (java.sql.Time) agent_.crossConverters_.setObject(java.sql.Types.TIME, updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getTime(column);
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
    public java.sql.Time getTime(int column, java.util.Calendar calendar) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", column, calendar);
            }
            if (calendar == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Time time = getTime(column);
            if (time != null) {
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(calendar.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(time);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(time);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                time.setTime(time.getTime() - timeZoneOffset);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTime", time);
            }
            return time;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public java.sql.Timestamp getTimestamp(int column) throws SQLException {
	    try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", column);
            }
            checkGetterPreconditions(column);
            java.sql.Timestamp result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (java.sql.Timestamp) agent_.crossConverters_.setObject(java.sql.Types.TIMESTAMP, updatedColumns_[column - 1]);
            } else {
                result = isNull(column) ? null : cursor_.getTimestamp(column);
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
    public java.sql.Timestamp getTimestamp(int column, java.util.Calendar calendar) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", column, calendar);
            }
            if (calendar == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Timestamp timestamp = getTimestamp(column);
            if (timestamp != null) {
                int nano = timestamp.getNanos();
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(calendar.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(timestamp);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(timestamp);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                timestamp.setTime(timestamp.getTime() - timeZoneOffset);
                timestamp.setNanos(nano);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTimestamp", timestamp);
            }
            return timestamp;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Live life on the edge and run unsynchronized
    public String getString(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getString", column);
            }
            checkGetterPreconditions(column);
            String result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (String) agent_.crossConverters_.setObject(java.sql.Types.CHAR, updatedColumns_[column - 1]);
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
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBytes", column);
            }
            checkGetterPreconditions(column);
            byte[] result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (byte[]) agent_.crossConverters_.setObject(java.sql.Types.BINARY, updatedColumns_[column - 1]);
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
    public java.io.InputStream getBinaryStream(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBinaryStream", column);
            }

            checkGetterPreconditions(column);
        useStream(column);

            java.io.InputStream result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = new java.io.ByteArrayInputStream((byte[]) agent_.crossConverters_.setObject(java.sql.Types.BINARY, updatedColumns_[column - 1]));
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
    public java.io.InputStream getAsciiStream(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getAsciiStream", column);
            }

            checkGetterPreconditions(column);
        useStream(column);

            java.io.InputStream result = null;
            if (wasNonNullSensitiveUpdate(column)) {

            result = new AsciiStream((String) agent_.crossConverters_.setObject(java.sql.Types.CHAR,
                                                updatedColumns_[column - 1]));
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

    // Live life on the edge and run unsynchronized
    public java.io.InputStream getUnicodeStream(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getUnicodeStream", column);
            }

            checkGetterPreconditions(column);
        useStream(column);

            java.io.InputStream result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                try {
                    result = new java.io.ByteArrayInputStream
                            (((String) agent_.crossConverters_.setObject(java.sql.Types.CHAR,
                                    updatedColumns_[column - 1])).getBytes("UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    throw new SqlException(agent_.logWriter_, e, e.getMessage());
                }
            } else {
                result = isNull(column) ? null : cursor_.getUnicodeStream(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedExit(this, "getUnicodeStream", result);
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
    public java.io.Reader getCharacterStream(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream", column);
            }

            checkGetterPreconditions(column);
        useStream(column);

            java.io.Reader result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = new java.io.StringReader
                        ((String) agent_.crossConverters_.setObject(java.sql.Types.CHAR, updatedColumns_[column - 1]));
            } else {
                result = isNull(column) ? null : cursor_.getCharacterStream(column);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getCharacterStream", result);
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
    public java.sql.Blob getBlob(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBlob", column);
            }
            checkGetterPreconditions(column);
            java.sql.Blob result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (java.sql.Blob) agent_.crossConverters_.setObject(java.sql.Types.BLOB,
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
    public java.sql.Clob getClob(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getClob", column);
            }
            checkGetterPreconditions(column);
            java.sql.Clob result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = (java.sql.Clob) agent_.crossConverters_.setObject(java.sql.Types.CLOB,
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
    public java.sql.Ref getRef(int column) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getRef", column);
            }
            checkGetterPreconditions(column);
            java.sql.Ref result = isNull(column) ? null : cursor_.getRef(column);
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method not yet implemented");
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
    public java.sql.Array getArray(int column) throws SQLException {
	    try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getArray", column);
            }
            checkGetterPreconditions(column);
            java.sql.Array result = isNull(column) ? null : cursor_.getArray(column);
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method not yet implemented");
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
            closeCloseFilterInputStream();

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
        checkGetterPreconditions(column);
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
    public Object getObject(int column, java.util.Map map) throws SQLException {
        try
        {
            closeCloseFilterInputStream();

            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", column, map);
            }
            checkGetterPreconditions(column);
            Object result = null;
            if (wasNonNullSensitiveUpdate(column)) {
                result = updatedColumns_[column - 1];
            } else {
                result = isNull(column) ? null : cursor_.getObject(column);
            }
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method not yet implemented");
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
                resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE &&
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
            return getBoolean(findColumnX(columnName));
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
            return getByte(findColumnX(columnName));
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
            return getShort(findColumnX(columnName));
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
            return getInt(findColumnX(columnName));
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
            return getLong(findColumnX(columnName));
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
            return getFloat(findColumnX(columnName));
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
            return getDouble(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.math.BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getBigDecimal", columnName, scale);
            }
            return getBigDecimal(findColumnX(columnName), scale);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.math.BigDecimal getBigDecimal(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBigDecimal", columnName);
            }
            return getBigDecimal(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Date getDate(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", columnName);
            }
            return getDate(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Date getDate(String columnName, java.util.Calendar cal) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", columnName, cal);
            }
            return getDate(findColumnX(columnName), cal);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Time getTime(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", columnName);
            }
            return getTime(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Time getTime(String columnName, java.util.Calendar cal) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", columnName, cal);
            }
            return getTime(findColumnX(columnName), cal);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", columnName);
            }
            return getTimestamp(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Timestamp getTimestamp(String columnName, java.util.Calendar cal) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", columnName, cal);
            }
            return getTimestamp(findColumnX(columnName), cal);
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
            return getString(findColumnX(columnName));
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
            return getBytes(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.io.InputStream getBinaryStream(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBinaryStream", columnName);
            }
            return getBinaryStream(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getAsciiStream", columnName);
            }
            return getAsciiStream(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getUnicodeStream", columnName);
            }
            return getUnicodeStream(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.io.Reader getCharacterStream(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getCharacterStream", columnName);
            }
            return getCharacterStream(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Blob getBlob(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBlob", columnName);
            }
            return getBlob(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Clob getClob(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getClob", columnName);
            }
            return getClob(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Array getArray(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getArray", columnName);
            }
            return getArray(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final java.sql.Ref getRef(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getRef", columnName);
            }
            return getRef(findColumnX(columnName));
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
            return getObject(findColumnX(columnName));
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public final Object getObject(String columnName, java.util.Map map) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", columnName, map);
            }
            return getObject(findColumnX(columnName), map);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // ----------------Advanced features -----------------------------------------

    public final java.sql.SQLWarning getWarnings() {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getWarnings", warnings_);
        }
        return warnings_ == null ? null : warnings_.getSQLWarning();
    }

    public final void clearWarnings() throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "clearWarnings");
            }
            warnings_ = null;
        }
    }

    // An untraced version of clearWarnings()
    public final void clearWarningsX() {
        warnings_ = null;
    }

    public String getCursorName() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getCursorName");
                }
                checkForClosedResultSet();
                if (generatedSection_ != null) {
                    return "stored procedure generated cursor:" + generatedSection_.getServerCursorName();
                }
                if (statement_.cursorName_ == null) {// cursor name is not in the maps yet.
                    statement_.cursorName_ = statement_.section_.getServerCursorName();
                    if (statement_.section_ instanceof Section) {
                        agent_.sectionManager_.mapCursorNameToQuerySection(statement_.cursorName_,
                                (Section) statement_.section_);
                    }
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

    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getMetaData");
            }
            java.sql.ResultSetMetaData resultSetMetaData = getMetaDataX();
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
        checkForClosedResultSet();
        return resultSetMetaData_;
    }


    public final int findColumn(String columnName) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "findColumn", columnName);
                }
                int column = findColumnX(columnName);
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
    private final int findColumnX(String columnName) throws SqlException {
        checkForClosedResultSet();
        return resultSetMetaData_.findColumnX(columnName);
    }

    //-------------------------- Traversal/Positioning ---------------------------

    public boolean isBeforeFirst() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "isBeforeFirst");
            }
            checkForClosedResultSet();
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
            checkForClosedResultSet();
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
            checkForClosedResultSet();
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
            checkForClosedResultSet();
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
                checkForClosedResultSet();
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
	unuseStreams();

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
                checkForClosedResultSet();
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
    unuseStreams();

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
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();

        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();

        resetRowsetFlags();
    unuseStreams();

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
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();

        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor
        resetUpdatedColumns();

        resetRowsetFlags();
	unuseStreams();

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
        checkForClosedResultSet();
        long row;
        checkThatResultSetIsNotDynamic();
        if (resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY)
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
                new MessageId(SQLState.NUMBER_OF_ROWS_TOO_LARGE_FOR_INT),
                new Long(row)));
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

    public boolean absoluteX(int row) throws SqlException {
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();

        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();

        resetRowsetFlags();
	unuseStreams();

        if (maxRows_ > 0) {
            // if "row" is positive and > maxRows, fetch afterLast
            // else if "row" is negative, and abs(row) > maxRows, fetch beforeFirst
            if (row > 0 && row > maxRows_) {
                afterLastX();
                isValidCursorPosition_ = false;
                return isValidCursorPosition_;
            } else if (row <= 0 && java.lang.Math.abs(row) > maxRows_) {
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
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();
        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();
	
	unuseStreams();

        // this method may not be called when the cursor on the insert row
        if (isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_, "Cursor is Not on a Valid Row");
        }

        // If the resultset is empty, relative(n) is a null operation
        if (resultSetContainsNoRows()) {
            isValidCursorPosition_ = false;
            return isValidCursorPosition_;
        }
        
        // relative(0) is a null-operation, but the retruned result is
        // dependent on wether the cursorposition is on a row or not.
        if (rows == 0) {
            if (isBeforeFirstX() || isAfterLastX()) {
                isValidCursorPosition_ = false;
            } else {
                isValidCursorPosition_ = true;
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
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        clearWarningsX();

        wasNull_ = ResultSet.WAS_NULL_UNSET;

        // discard all previous updates when moving the cursor.
        resetUpdatedColumns();
	
	unuseStreams();

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
                checkForClosedResultSet();
                checkThatResultSetTypeIsScrollable();

                switch (direction) {
                case java.sql.ResultSet.FETCH_FORWARD:
                case java.sql.ResultSet.FETCH_REVERSE:
                case java.sql.ResultSet.FETCH_UNKNOWN:
                    fetchDirection_ = direction;
                    break;
                default:
                    throw new SqlException(agent_.logWriter_, "Invalid fetch direction " + direction);
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
            checkForClosedResultSet();
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
                checkForClosedResultSet();
                if (rows < 0 || (maxRows_ != 0 && rows > maxRows_)) {
                    throw new SqlException(agent_.logWriter_, "Invalid fetch size " + rows).getSQLException();
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
            checkForClosedResultSet();
            return fetchSize_;
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
            checkForClosedResultSet();
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
            checkForClosedResultSet();
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
            // we cannot tell whether the ResultSet has been updated, so always return false here.
            boolean rowUpdated = false;
            checkForClosedResultSet();
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
            boolean rowInserted = false;
            checkForClosedResultSet();
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
            // rowDeleted is visible through a delete hole, (sqlcode +222).
            // Always return false and do not check the return code for now.
            boolean rowDeleted = false;
            checkForClosedResultSet();
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
                checkUpdatePreconditions(column);
                if (!resultSetMetaData_.nullable_[column - 1]) {
                    throw new SqlException(agent_.logWriter_, "Invalid operation to update a non-nullable column to null.");
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBigDecimal(int column, java.math.BigDecimal x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateBigDecimal", column, x);
                }
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDate(int column, java.sql.Date x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateDate", column, x);
                }
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTime(int column, java.sql.Time x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateTime", column, x);
                }
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTimestamp(int column, java.sql.Timestamp x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateTimestamp", column, x);
                }
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBinaryStream(int column,
                                   java.io.InputStream x,
                                   int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateBinaryStream", column, x, length);
                }
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObjectFromBinaryStream(resultSetMetaData_.types_[column - 1], x, length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateAsciiStream(int column,
                                  java.io.InputStream x,
                                  int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateAsciiStream", column, x, length);
                }
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObjectFromCharacterStream(resultSetMetaData_.types_[column - 1], x, "US-ASCII", length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateCharacterStream(int column,
                                      java.io.Reader x,
                                      int length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateCharacterStream", column, x, length);
                }
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
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
                checkUpdatePreconditions(column);
                updateColumn(column, agent_.crossConverters_.setObject(resultSetMetaData_.types_[column - 1], x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // ---------------------- update on column name methods ----------------------

    public void updateNull(String columnName) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateNull", columnName);
            }
            updateNull(findColumnX(columnName));
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
            updateBoolean(findColumnX(columnName), x);
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
            updateByte(findColumnX(columnName), x);
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
            updateShort(findColumnX(columnName), x);
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
            updateInt(findColumnX(columnName), x);
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
            updateLong(findColumnX(columnName), x);
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
            updateFloat(findColumnX(columnName), x);
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
            updateDouble(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBigDecimal(String columnName, java.math.BigDecimal x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBigDecimal", columnName, x);
            }
            updateBigDecimal(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateDate(String columnName, java.sql.Date x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateDate", columnName, x);
            }
            updateDate(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTime(String columnName, java.sql.Time x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateTime", columnName, x);
            }
            updateTime(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateTimestamp", columnName, x);
            }
            updateTimestamp(findColumnX(columnName), x);
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
            updateString(findColumnX(columnName), x);
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
            updateBytes(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateBinaryStream(String columnName,
                                   java.io.InputStream x,
                                   int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateBinaryStream", columnName, x, length);
            }
            updateBinaryStream(findColumnX(columnName), x, length);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateAsciiStream(String columnName,
                                  java.io.InputStream x,
                                  int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateAsciiStream", columnName, x, length);
            }
            updateAsciiStream(findColumnX(columnName), x, length);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void updateCharacterStream(String columnName,
                                      java.io.Reader x,
                                      int length) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "updateCharacterStream", columnName, x, length);
            }
            updateCharacterStream(findColumnX(columnName), x, length);
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
            updateObject(findColumnX(columnName), x, scale);
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
            updateObject(findColumnX(columnName), x);
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
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
        checkForClosedResultSet();
        if (isOnCurrentRow_ || resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_, "This method cannot be invoked while the cursor is not on the insert " +
                    "row or if the concurrency of this ResultSet object is CONCUR_READ_ONLY.");
       }
 
        // if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_) {
            throw new SqlException(agent_.logWriter_, "Invalid operation to " +
                    "insert at current cursor position");
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
    
    public void updateRow() throws java.sql.SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "updateRow");
                }
                //If updateXXX were issued on the row before updateRow, then
                //position the ResultSet to right before the next row after updateRow
                if (updateRowX())
                    isValidCursorPosition_ = false;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //if no updateXXX were issued before this updateRow, then return false
    private boolean updateRowX() throws SqlException {
        checkForClosedResultSet();
        if (isOnInsertRow_ || resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_, "This method cannot be invoked while the cursor is on the insert " +
                    "row or if the concurrency of this ResultSet object is CONCUR_READ_ONLY.");
        }

        //if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_)
            throw new SqlException(agent_.logWriter_, "Invalid operation to " +
                    "update at current cursor position");

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
        // cancelRowUpdates if updateCount_ != 1, else set updateRowCalled_ to true.
        try {
            if (isRowsetCursor_ || sensitivity_ == sensitivity_sensitive_dynamic__) {
                update();
            } else {
                positionToCurrentRowAndUpdate();
            }
            updateRowCalled_ = true;
        } catch (SqlException e) {
            try {
                cancelRowUpdates();
            } catch ( SQLException se ) {
                throw new SqlException(se);
            }
            throw e;
        }
        return true;
    }

    public void deleteRow() throws java.sql.SQLException {
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
        checkForClosedResultSet();

        // discard all previous updates
        resetUpdatedColumns();

        if (isOnInsertRow_ || resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_, "This method cannot be invoked while the cursor is on the insert " +
                    "row or if the concurrency of this ResultSet object is CONCUR_READ_ONLY.");
        }

        if (preparedStatementForDelete_ == null) {
            getPreparedStatementForDelete();
        }

        if (isRowsetCursor_ || sensitivity_ == sensitivity_sensitive_dynamic__) {
            delete();
        } else {
            positionToCurrentRowAndDelete();
        }

        Boolean nullIndicator = Cursor.ROW_IS_NULL;
        if (resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
            cursor_.isUpdateDeleteHole_ = true;
        } else {
            cursor_.isUpdateDeleteHoleCache_.set((int) currentRowInRowset_, nullIndicator);
            cursor_.isUpdateDeleteHole_ = ((Boolean) cursor_.isUpdateDeleteHoleCache_.get((int) currentRowInRowset_)).booleanValue();
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
        checkForClosedResultSet();
        checkThatResultSetTypeIsScrollable();
        if (isBeforeFirstX() || isAfterLastX() || isOnInsertRow_ ||
                resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_,
                    "This method cannot be invoked while the cursor is on the insert " +
                    "row, if the cursor is not on a valid row, or if this ResultSet " +
                    "object has a concurrency of CONCUR_READ_ONLY.");
        }

	
        // this method does nothing if ResultSet is TYPE_SCROLL_INSENSITIVE
        if (resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) {
            isValidCursorPosition_ = getRefreshRowset();
            try {
                cancelRowUpdates();
            } catch ( SQLException sqle ) {
                throw new SqlException(sqle);
            }
	    
    	    unuseStreams();
	    
        }
    }

    public void cancelRowUpdates() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "cancelRowUpdates");
                }
                checkForClosedResultSet();
                if (isOnInsertRow_ || resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
                    throw new SqlException(agent_.logWriter_, "This method cannot be invoked while the cursor is on the insert " +
                            "row or if this ResultSet object has a concurrency of CONCUR_READ_ONLY.");
                }

                // if not on a valid row, then do not accept cancelRowUpdates call
                if (!isValidCursorPosition_)
                    throw new SqlException(agent_.logWriter_, "Invalid operation " +
                            "at current cursor position.");

                // if updateRow() has already been called, then cancelRowUpdates should have
                // no effect.  updateRowCalled_ is reset to false as soon as the cursor moves to a new row.
                if (!updateRowCalled_) {
                    resetUpdatedColumns();
                }
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
                checkForClosedResultSet();
                checkUpdatableCursor();

                resetUpdatedColumnsForInsert();

                isOnInsertRow_ = true;
                isOnCurrentRow_ = false;
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
                checkForClosedResultSet();
                checkUpdatableCursor();

                if (!isOnInsertRow_) {
                    // no affect
                } else {
                    resetUpdatedColumns();
                    isOnInsertRow_ = false;
                    isOnCurrentRow_ = true;
                    if (currentRowInRowset_ > 0) {
                        updateColumnInfoFromCache();
                    }
                    isValidCursorPosition_ = true;
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public java.sql.Statement getStatement() {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getStatement", statement_);
        }
        return statement_;
    }

    //-------------------------- JDBC 3.0 ----------------------------------------

    public java.net.URL getURL(int columnIndex) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public java.net.URL getURL(String columnName) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateRef(int columnIndex, java.sql.Ref x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateRef(String columnName, java.sql.Ref x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateBlob(int columnIndex, java.sql.Blob x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateBlob(String columnName, java.sql.Blob x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateClob(int columnIndex, java.sql.Clob x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateClob(String columnName, java.sql.Clob x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateArray(int columnIndex, java.sql.Array x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public void updateArray(String columnName, java.sql.Array x) throws SQLException {
        throw jdbc3MethodNotSupported();
    }

    public boolean repositionScrollableResultSetBeforeJDBC1PositionedUpdateDelete() throws SqlException {
        boolean repositionedCursor = false;

        // calculate the absolutePosition of the current row directly.
        long rowToFetch = getRowUncast() - absolutePosition_;

        // if rowToFetch is zero, already positioned on the current row
        if (rowToFetch != 0 || cursorUnpositionedOnServer_) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
                    scrollOrientation_relative__,
                    rowToFetch);
            // adjust the absolute position on the client
            absolutePosition_ += rowToFetch;
            repositionedCursor = true;
        }
        return repositionedCursor;
    }
    //--------------------categorize the methods below -----------------

    public void flowPositioningFetch(int scrollOrientation,
                                     int rowToFetch) throws DisconnectException {
        // need the try-catch block here because agent_.beginWriteChain throws
        // an SqlException
        try {
            agent_.beginWriteChain(statement_);

            writePositioningFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
                    scrollOrientation,
                    rowToFetch);

            agent_.flow(statement_);
            readPositioningFetch_();
            agent_.endReadChain();
        } catch (SqlException e) {
            throw new DisconnectException(agent_, e);
        }
    }

    protected void positionToCurrentRowAndUpdate() throws SqlException {
        agent_.beginWriteChain(statement_);

        // calculate the position of the current row relative to the absolute position on server
        long currentRowPosRelativeToAbsoluteRowPos = getRowUncast() - absolutePosition_;

        // if currentRowPosRelativeToAbsoluteRowPos is zero, already on the current row
        // reposition only if a commit has been sent
        // do not reposition forward-only cursors
        if (resultSetType_ != java.sql.ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
                    scrollOrientation_relative__,
                    currentRowPosRelativeToAbsoluteRowPos);
        }

        // re-prepare the update statement if repreparing is needed after a commit.
        if (!preparedStatementForUpdate_.openOnServer_) {
            preparedStatementForUpdate_.materialPreparedStatement_.writePrepare_(preparedStatementForUpdate_.sql_,
                    preparedStatementForUpdate_.section_);
        }
        
        try {
            writeUpdateRow(false);
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
        

        agent_.flow(statement_);

        // adjust the absolute position on the client
        absolutePosition_ += currentRowPosRelativeToAbsoluteRowPos;

        if (resultSetType_ != java.sql.ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            readPositioningFetch_();
            cursorUnpositionedOnServer_ = false;
            listenToUnitOfWork();
        }

        // read prepare replies if the update statement is re-prepared after a commit.
        if (!preparedStatementForUpdate_.openOnServer_) {
            preparedStatementForUpdate_.materialPreparedStatement_.readPrepare_();
        }
        readUpdateRow();

        agent_.endReadChain();
    }

    protected void insert() throws SqlException {
        agent_.beginWriteChain(statement_);

        // re-prepare the insert statement if repreparing is needed after a commit.
        if (!preparedStatementForInsert_.openOnServer_) {
            preparedStatementForInsert_.materialPreparedStatement_.writePrepare_(
                    preparedStatementForInsert_.sql_,
                    preparedStatementForInsert_.section_);
        }

        try {
            writeInsertRow(false);
        } catch (SQLException se ) {
            throw new SqlException(se);
        }

        agent_.flow(statement_);

        // read prepare replies if the update statement is re-prepared after a commit.
        if (!preparedStatementForInsert_.openOnServer_) {
            preparedStatementForInsert_.materialPreparedStatement_.readPrepare_();
        }

        readInsertRow();

        agent_.endReadChain();
     }    

    
    protected void update() throws SqlException {
        agent_.beginWriteChain(statement_);

        // re-prepare the update statement if repreparing is needed after a commit.
        if (!preparedStatementForUpdate_.openOnServer_) {
            preparedStatementForUpdate_.materialPreparedStatement_.writePrepare_(preparedStatementForUpdate_.sql_,
                    preparedStatementForUpdate_.section_);
        }

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

        // read prepare replies if the update statement is re-prepared after a commit.
        if (!preparedStatementForUpdate_.openOnServer_) {
            preparedStatementForUpdate_.materialPreparedStatement_.readPrepare_();
        }

        readUpdateRow();

        if (chainAutoCommit) {
            connection_.readCommit();
        }
        agent_.endReadChain();
    }

    protected void positionToCurrentRowAndDelete() throws SqlException {
        agent_.beginWriteChain(statement_);

        // calculate the position of the current row relative to the absolute position on server
        long currentRowPosRelativeToAbsoluteRowPos = getRowUncast() - absolutePosition_;

        // if rowToFetch is zero, already positioned on the current row
        // do not reposition forward-only cursors.
        if (resultSetType_ != java.sql.ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            writePositioningFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
                    scrollOrientation_relative__,
                    currentRowPosRelativeToAbsoluteRowPos);
        }

        // re-prepare the update statement if repreparing is needed after a commit.
        if (!preparedStatementForDelete_.openOnServer_) {
            preparedStatementForDelete_.materialPreparedStatement_.writePrepare_(preparedStatementForDelete_.sql_,
                    preparedStatementForDelete_.section_);
        }

        try {
            writeDeleteRow();
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }

        agent_.flow(statement_);

        // adjust the absolute position on the client.
        absolutePosition_ += currentRowPosRelativeToAbsoluteRowPos;

        if (resultSetType_ != java.sql.ResultSet.TYPE_FORWARD_ONLY &&
                (currentRowPosRelativeToAbsoluteRowPos != 0 ||
                (currentRowPosRelativeToAbsoluteRowPos == 0 && cursorUnpositionedOnServer_))) {
            readPositioningFetch_();
            cursorUnpositionedOnServer_ = false;
            listenToUnitOfWork();
        }

        // read prepare replies if the update statement is re-prepared after a commit.
        if (!preparedStatementForDelete_.openOnServer_) {
            preparedStatementForDelete_.materialPreparedStatement_.readPrepare_();
        }
        readDeleteRow();

        agent_.endReadChain();
    }

    protected void delete() throws SqlException {
        try
        {
            agent_.beginWriteChain(statement_);

            // re-prepare the update statement if repreparing is needed after a commit.
            if (!preparedStatementForDelete_.openOnServer_) {
                preparedStatementForDelete_.materialPreparedStatement_.writePrepare_(preparedStatementForDelete_.sql_,
                        preparedStatementForDelete_.section_);
            }

            if (isRowsetCursor_) {
                preparedStatementForDelete_.setInt(1, (int) (currentRowInRowset_ + 1));
            }

            writeDeleteRow();

            if (connection_.autoCommit_) {
                connection_.writeAutoCommit();
            }

            agent_.flow(statement_);

            // read prepare replies if the update statement is re-prepared after a commit.
            if (!preparedStatementForDelete_.openOnServer_) {
                preparedStatementForDelete_.materialPreparedStatement_.readPrepare_();
            }
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
    public void setRowsetAfterLastEvent() throws SqlException {
        firstRowInRowset_ = 0;
        lastRowInRowset_ = 0;
        absolutePosition_ = (maxRows_ == 0) ? rowCount_ + 1 : maxRows_ + 1;
        currentRowInRowset_ = 0;
        rowsReceivedInCurrentRowset_ = 0;
    }

    public void setRowsetBeforeFirstEvent() throws SqlException {
        firstRowInRowset_ = 0;
        lastRowInRowset_ = 0;
        absolutePosition_ = 0;
        currentRowInRowset_ = -1;
        rowsReceivedInCurrentRowset_ = 0;
    }

    public void setRowsetNoRowsEvent() {
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

    private boolean isServerCursorPositionAfterLast() throws SqlException {
        return (absolutePosition_ == (rowCount_ + 1));
    }

    public void setValidCursorPosition(boolean isValidCursorPosition) {
        isValidCursorPosition_ = isValidCursorPosition;
    }

    protected void moveToAfterLast() throws DisconnectException {
        flowPositioningFetch(ResultSet.scrollOrientation_after__, 0);
    }

    // Positions the cursor at before the first row.
    protected void moveToBeforeFirst() throws DisconnectException {
        flowPositioningFetch(ResultSet.scrollOrientation_before__, 0);
    }

    // analyze the error handling here, and whether or not can be pushed to common
    // can we make this the common layer fetch method
    // Called by the read/skip Fdoca bytes methods in the net
    // whenever data reads exhaust the internal buffer used by this reply
    public void flowFetch() throws DisconnectException, SqlException {
        agent_.beginWriteChain(statement_);
        writeFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_);
        agent_.flow(statement_);
        readFetch_();
        agent_.endReadChain();
    }

    public void writeInsertRow(boolean chainedWritesFollowingSetLob) throws SQLException {
        try
        {
            preparedStatementForInsert_.materialPreparedStatement_.writeExecute_(
                    preparedStatementForInsert_.section_,
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
    
    public void writeUpdateRow(boolean chainedWritesFollowingSetLob) throws SQLException {
        try
        {
            preparedStatementForUpdate_.materialPreparedStatement_.writeExecute_(preparedStatementForUpdate_.section_,
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

    public void writeDeleteRow() throws SQLException {
        try
        {
            if (isRowsetCursor_) {
                preparedStatementForDelete_.materialPreparedStatement_.writeExecute_(preparedStatementForDelete_.section_,
                        preparedStatementForDelete_.parameterMetaData_,
                        preparedStatementForDelete_.parameters_,
                        preparedStatementForDelete_.parameterMetaData_.getColumnCount(),
                        false, // false means we're not expecting output
                        false);  // false means we don't chain anything after the execute
            } else {
                preparedStatementForDelete_.materialPreparedStatement_.writeExecute_(preparedStatementForDelete_.section_,
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

    public void readInsertRow() throws DisconnectException, SqlException {
        preparedStatementForInsert_.materialPreparedStatement_.readExecute_();
    }

    public void readUpdateRow() throws DisconnectException, SqlException {
        preparedStatementForUpdate_.materialPreparedStatement_.readExecute_();
    }

    public void readDeleteRow() throws DisconnectException, SqlException {
        preparedStatementForDelete_.materialPreparedStatement_.readExecute_();
    }

    //------------------material layer event callback methods-----------------------

    boolean listenToUnitOfWork_ = false;

    public void listenToUnitOfWork() {
        if (!listenToUnitOfWork_) {
            listenToUnitOfWork_ = true;
            connection_.CommitAndRollbackListeners_.add(this);
        }
    }

    public void completeLocalCommit(java.util.Iterator listenerIterator) {
        cursorUnpositionedOnServer_ = true;
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

    public void completeLocalRollback(java.util.Iterator listenerIterator) {
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
     * <code>org.apache.derby.client.am.Connection</code>.
     */
    void markClosed() {
        markClosed(false);
    }

    /**
     * Mark this ResultSet as closed.
     *
     * @param removeListener if true the ResultSet will be removed
     * from the commit and rollback listeners list in
     * <code>org.apache.derby.client.am.Connection</code>.
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
    protected boolean rowCountIsUnknown() {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            return false;
        } else {
            return rowCount_ == -1;
        }
    }

    protected boolean rowCountIsKnown() {
        return rowCount_ != -1;
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
                insertSQL.append("\"" + resultSetMetaData_.getColumnName(column) + "\"");
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
        String updateString = "UPDATE " + getTableName() + " SET ";

        for (column = 1; column <= resultSetMetaData_.columns_; column++) {
            if (columnUpdated_[column - 1]) {
                if (foundOneUpdatedColumnAlready) {
                    updateString += ",";
                }
                try {
                    updateString += "\"" + resultSetMetaData_.getColumnName(column) + "\" = ? ";
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
        updateString = updateString + " WHERE CURRENT OF " + getServerCursorName();

        if (isRowsetCursor_) {
            updateString += " FOR ROW ? OF ROWSET";
        }

        return updateString;
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
            tableName += "\"" + resultSetMetaData_.sqlxSchema_[baseTableColumn] + "\".";
        }
        if (resultSetMetaData_.sqlxBasename_[baseTableColumn] != null) {
            tableName += "\"" + resultSetMetaData_.sqlxBasename_[baseTableColumn] + "\"";
        }
        return tableName;
    }

    private String getServerCursorName() throws SqlException {
        return statement_.section_.getServerCursorName();
    }

    private void getPreparedStatementForInsert() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String insertString = buildInsertString();

        try {
            preparedStatementForInsert_ = (PreparedStatement)statement_.connection_.
                    prepareStatement(insertString);
        } catch ( SQLException sqle ) {
            throw new SqlException(sqle);
        }
    }

    private void getPreparedStatementForUpdate() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String updateString = buildUpdateString();

        if (updateString == null) {
            throw new SqlException(agent_.logWriter_, "No updateXXX issued on this row.");
        }
        preparedStatementForUpdate_ =
                statement_.connection_.preparePositionedUpdateStatement(updateString,
                        statement_.section_.getPositionedUpdateSection());

    }

    private void getPreparedStatementForDelete() throws SqlException {
        // each column is associated with a tableName in the extended describe info.
        String deleteString = buildDeleteString();

        preparedStatementForDelete_ =
                statement_.connection_.preparePositionedUpdateStatement(deleteString,
                        statement_.section_.getPositionedUpdateSection()); // update section
    }

    private final void resetUpdatedColumnsForInsert() {
        // initialize updateColumns with nulls for all columns
        if (updatedColumns_ == null) {
            updatedColumns_ = new Object[resultSetMetaData_.columns_];
        }
        if (columnUpdated_ != null) {
            columnUpdated_ = new boolean[resultSetMetaData_.columns_];
        }
        for (int i = 0; i < updatedColumns_.length; i++) {
            updateColumn(i+1, null);
        }
        for (int i = 0; i < columnUpdated_.length; i++) {
            columnUpdated_[i] = false;
        }
    }

    private final void resetUpdatedColumns() {
        if (updatedColumns_ != null) {
            for (int i = 0; i < updatedColumns_.length; i++) {
                updatedColumns_[i] = null;
            }
        }
        if (columnUpdated_ != null) {
            for (int i = 0; i < columnUpdated_.length; i++) {
                columnUpdated_[i] = false;
            }
        }
        updateRowCalled_ = false;
    }

    private final long getRowUncast() {
        return firstRowInRowset_ + currentRowInRowset_;
    }

    private final void checkGetterPreconditions(int column) throws SqlException {
        checkForClosedResultSet();
        checkForValidColumnIndex(column);
        checkForValidCursorPosition();
    }

    private final void checkUpdatePreconditions(int column) throws SqlException {
        checkForClosedResultSet();
        checkForValidColumnIndex(column);
        if (resultSetConcurrency_ != java.sql.ResultSet.CONCUR_UPDATABLE) {
            throw new SqlException(agent_.logWriter_, "ResultSet is not updatable.");
        }
        if (!isOnCurrentRow_ && !isOnInsertRow_) {
            throw new SqlException(agent_.logWriter_, "This method must be called to update values in the current row " +
                    "or the insert row");
        }

        if (resultSetMetaData_.sqlxUpdatable_ == null || resultSetMetaData_.sqlxUpdatable_[column - 1] != 1) {
            throw new SqlException(agent_.logWriter_, "Column not updatable");
        }

        //if not on a valid row, then do not accept updateXXX calls
        if (!isValidCursorPosition_)
            throw new SqlException(agent_.logWriter_, "Invalid operation to " +
                    "update at current cursor position");
    }

    final void checkForValidColumnIndex(int column) throws SqlException {
        if (column < 1 || column > resultSetMetaData_.columns_) {
            throw new SqlException(agent_.logWriter_, "Invalid argument: parameter index " +
                    column + " is out of range.");
        }
    }

    private void checkUpdatableCursor() throws SqlException {
        if (resultSetConcurrency_ == java.sql.ResultSet.CONCUR_READ_ONLY) {
            throw new SqlException(agent_.logWriter_, 
                    "This method should only be called on ResultSet objects " +
                    "that are updatable(concurrency type CONCUR_UPDATABLE).");
        }
    }

    protected final void checkForClosedResultSet() throws SqlException {
        if (!openOnClient_) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, "Invalid operation: result set closed");
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    private final void checkForValidCursorPosition() throws SqlException {
        if (!isValidCursorPosition_) {
            throw new SqlException(agent_.logWriter_, "Invalid operation to read at current cursor position.");
        }
    }

    private final void checkThatResultSetTypeIsScrollable() throws SqlException {
        if (resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
            throw new SqlException(agent_.logWriter_, "This method should only be called on ResultSet objects that are " +
                    "scrollable(type TYPE_SCROLL_SENSITIVE or TYPE_SCROLL_INSENSITIVE)");
        }
    }

    private final void checkThatResultSetIsNotDynamic() throws SqlException {
        if (sensitivity_ == sensitivity_sensitive_dynamic__) {
            throw new SqlException(agent_.logWriter_, "This method should not be called on sensitive dynamic cursors.");
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
        } else {
            return rowIsInCurrentRowset(rowNumber);
        }
    }

    private boolean rowIsInCurrentRowset(long rowNumber) throws SqlException {
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

    protected void updateColumnInfoFromCache() {
        // currentRowInRowset_ should never be bigger than the max value of an int,
        // because we have a driver imposed limit of fetch size 1000.
        cursor_.columnDataPosition_ =
                (int[]) cursor_.columnDataPositionCache_.get((int) currentRowInRowset_);
        cursor_.columnDataComputedLength_ =
                (int[]) cursor_.columnDataLengthCache_.get((int) currentRowInRowset_);
        cursor_.isNull_ =
                (boolean[]) cursor_.columnDataIsNullCache_.get((int) currentRowInRowset_);
        cursor_.isUpdateDeleteHole_ = ((Boolean) cursor_.isUpdateDeleteHoleCache_.get((int) currentRowInRowset_)).booleanValue();
    }

    protected final void checkAndThrowReceivedQueryTerminatingException() throws SqlException {
        // If we are in a split row, and before sending CNTQRY, check whether an ENDQRYRM
        // has been received.
        if (!openOnServer_) {
            SqlException sqlException = null;
            int sqlcode = org.apache.derby.client.am.Utils.getSqlcodeFromSqlca(queryTerminatingSqlca_);
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

    public void parseScrollableRowset() throws SqlException {
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

        Section section = (generatedSection_ == null) ? statement_.section_ : generatedSection_;

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
        cursor_.resetDataBuffer();
        agent_.beginWriteChain(statement_);
        
        writeScrollableFetch_((generatedSection_ == null) ? statement_.section_ : generatedSection_,
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

    private void adjustPreviousRowset(int orientation, long rowNumber, boolean isAfterLastRow) throws SqlException {
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
            warnings_ = null;    // clear any previous warnings
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
            for (int i = 0; i < rowsetSqlca_.length; i++) {
                rowsetSqlca_[i] = null;
            }
        }
    }
	
	
	private CloseFilterInputStream createCloseFilterInputStream(java.io.InputStream is) throws SqlException {
		
		if(is == null){
			return null;
		}

		if( is_ == is ){
			return is_;
		}
		
		closeCloseFilterInputStream();
		
		is_ = new CloseFilterInputStream(is);
		
		return is_;
		
	}
	
	
	private void closeCloseFilterInputStream() throws SqlException {
		
		if(is_ != null){
			try{
				is_.close();
				
			}catch(IOException e){
				
				throw new SqlException(agent_.logWriter_ ,
						       e ,
						       "Failed to close inputStream.");
				
			}
			
			is_ = null;
			
		}
	}
    
    
    void useStream(int columnIndex) throws SqlException {
	
	if(streamUsedFlags_[columnIndex - 1]){
	    throw new SqlException(agent_.logWriter_,
				   "Stream of column value in result cannot be retrieved twice");
	}

	streamUsedFlags_[columnIndex - 1] = true;

    }


    private void unuseStreams(){
	
	if(streamUsedFlags_ == null){
	    streamUsedFlags_ = new boolean[ resultSetMetaData_.columns_ ];
	    return;
	}

	for(int i = 0;
	    i < streamUsedFlags_.length;
	    i ++){
	    
	    streamUsedFlags_[i] = false;
	    
	}
	
    }

    private SQLException jdbc3MethodNotSupported()
    {
        // This will get internationalized in another patch
        return new SqlException(agent_.logWriter_, 
            "JDBC 3 method called - not yet supported").getSQLException();
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
            checkForClosedResultSet();
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
}

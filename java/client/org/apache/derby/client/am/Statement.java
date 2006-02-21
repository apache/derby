/*

   Derby - Class org.apache.derby.client.am.Statement

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

import java.sql.SQLException;

import org.apache.derby.shared.common.reference.JDBC30Translation;

public class Statement implements java.sql.Statement, StatementCallbackInterface, UnitOfWorkListener {

    // JDBC 3 constant indicating that the current ResultSet object
    // should be closed when calling getMoreResults.
    // Constant value matches that defined by JDBC 3 java.sql.Statement.CLOSE_CURRENT_RESULT
    public final static int CLOSE_CURRENT_RESULT = 1;

    // JDBC 3 constant indicating that the current ResultSet object
    // should not be closed when calling getMoreResults.
    // Constant value matches that defined by JDBC 3 java.sql.Statement.KEEP_CURRENT_RESULT
    public final static int KEEP_CURRENT_RESULT = 2;

    // JDBC 3 constant indicating that all ResultSet objects that
    // have previously been kept open should be closed when calling getMoreResults.
    // Constant value matches that defined by JDBC 3 java.sql.Statement.CLOSE_ALL_RESULTS
    public final static int CLOSE_ALL_RESULTS = 3;

    //---------------------navigational members-----------------------------------

    public MaterialStatement materialStatement_ = null;

    public Connection connection_;
    private SqlWarning warnings_ = null;
    public Section section_;
    public Agent agent_;

    public ResultSet resultSet_ = null;

    // Use -1, if there is no update count returned, ie. when result set is returned. 0 is a valid update count for DDL.
    int updateCount_ = -1;
    int returnValueFromProcedure_;

    // Enumeration of the flavors of statement execute call used.
    static final int executeQueryMethod__ = 1;
    static final int executeUpdateMethod__ = 2;
    static final int executeMethod__ = 3;

    // sqlMode_ will be moved to PS as soon as we remove the hack reference in completeExecute()
    // Enumerated in Statement: S.sqlIsQuery__, S.sqlIsCall__, S.sqlIsUpdate__
    // Determines whether sql_ starts with SELECT/VALUES, CALL, or other (assumed to be an update).
    protected int sqlMode_ = 0;
    // Enum for sqlMode_:
    static final int isQuery__ = 0x1; // sql starts with SELECT.... or VALUES...
    static final int isCall__ = 0x2; // sql starts with CALL ...
    static final int isUpdate__ = 0x4; // All other sql is categorized as a update DML or DDL.

    // sqlUpdateMode_ is only set when the sqlMode_ == isUpdate__
    public int sqlUpdateMode_ = 0;
    // Enum for sqlUpdateMode_:
    public final static int isCommitSql__ = 0x1;
    public final static int isRollbackSql__ = 0x2;
    final static int isPositionedUpdateDeleteSql__ = 0x10;
    final static int isInsertSql__ = 0x20;        // used to recognize "insert" for auto-generated keys
    final static int isDeleteSql__ = 0x40;        // used to recognize "delete" for parsing cursorname
    final static int isUpdateSql__ = 0x80;        // used to recognize "update" for parsing cursorname


    public ColumnMetaData resultSetMetaData_; // type information for output sqlda

    // these two are used during parsing of literals for call statement.
    // please add a comment desribing what why you can't reuse inputs_ and parameterMetaData_
    // members for the literal inputs

    // Caching the Cursor object for reuse.
    public Cursor cachedCursor_ = null;
    public Cursor cachedSingletonRowData_ = null;
    public boolean isPreparedStatement_ = false;
    public boolean isCallableStatement_ = false; // we can get rid of this member once we define polymorphic reset() on S/PS/CS

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, state data that is set by the constructor and never changes.

    // Alias for connection_.databaseMetaData
    public DatabaseMetaData databaseMetaData_;

    //-----------------------------state------------------------------------------

    // Jdbc 1 positioned updates are implemented via
    // sql scan for "...where current of <users-cursor-name>",
    // the addition of mappings from cursor names to query sections,
    // and the subtitution of <users-cursor-name> with <canned-cursor-name> in the pass-thru sql string
    // "...where current of <canned-cursor-name>" when user-defined cursor names are used.
    // Both "canned" cursor names (from our jdbc package set) and user-defined cursor names are mapped.
    // Statement.cursorName_ is initialized to null until the cursor name is requested or set.
    // When set (s.setCursorName()) with a user-defined name, then it is added to the cursor map at that time;
    // When requested (rs.getCursorName()), if the cursor name is still null,
    // then is given the canned cursor name as defined by our jdbc package set and added to the cursor map.
    // Still need to consider how positioned updates should interact with multiple result sets from a stored.
    String cursorName_ = null;

    // This means the client-side jdbc statement object is open.
    // This value is set to true when the statement object is constructed, and will not change
    // until statement.close() is called either directly or via connection.close(), finalizer, or other methods.
    boolean openOnClient_ = true;
    // This means a DERBY server-side section for this statement is in the prepared state.
    // A client-side jdbc statement may remain open across commits (openOnClient=true),
    // but the server-side DERBY section moves to an unprepared state (openOnServer=false) across commits,
    // requiring an implicit re-prepare "under the covers" by the driver.
    // Unprepared jdbc query statements still have prepared sections on the server.
    // This openOnServer_ only has implications for preparedstatement
    boolean openOnServer_ = false;


    //private int indexOfCurrentResultSet_ = -1;
    protected int indexOfCurrentResultSet_ = -1;
    ResultSet[] resultSetList_ = null;   // array of ResultSet objects

    protected final static String TIMEOUT_STATEMENT = "SET STATEMENT_TIMEOUT ";
    protected java.util.ArrayList timeoutArrayList = new java.util.ArrayList(1);
    protected boolean doWriteTimeout = false;
    int timeout_ = 0; // for query timeout in seconds
    int maxRows_ = 0;
    int maxFieldSize_ = 0; // zero means that there is no limit to the size of a column.
    boolean escapedProcedureCallWithResult_ = false;

    // When this is false we skip autocommit for this PreparedStatement.
    // This is needed when the PreparedStatement object is used internally by
    // the driver and a commit is not desired, e.g., Blob/Clob API calls
    public boolean isAutoCommittableStatement_ = true;

    // The user has no control over the statement that owns a catalog query, and has no ability to close that statement.
    // We need a special member variable on our internal catalog query statements so that
    // when the catalog query is closed, the result set will know to close it's owning statement.
    boolean isCatalogQuery_ = false;


    // This collection is used for two different purposes:
    //   For statement batching it contains the batched SQL strings.
    //   For prepared statement batching it contains the batched input rows.
    java.util.ArrayList batch_ = new java.util.ArrayList();


    // Scrollable cursor attributes
    public int resultSetType_ = java.sql.ResultSet.TYPE_FORWARD_ONLY;
    public int resultSetConcurrency_ = java.sql.ResultSet.CONCUR_READ_ONLY;
    public int resultSetHoldability_;
    // This is ignored by the driver if this is zero.
    // For the net forward-only result set, if fetchSize is unset, we let the server return however many rows will fit in a query block.
    // For the net scrollable result set, then we use a default of 64 rows.
    public int fetchSize_ = 0;
    public int fetchDirection_ = java.sql.ResultSet.FETCH_FORWARD;

    // Conceptually this doesn't belong in Statement, but belongs in PreparedStatement,
    // since Statement doesn't know about params, so we're just putting it here perhaps temporarily,
    // Used for callable statement OUT paramters.
    public Cursor singletonRowData_ = null;

    // number of invisible result sets returned from a stored procedure.
    public int numInvisibleRS_ = 0;

    // This is a cache of the attributes to be sent on prepare.
    // Think about caching the entire prepare DDM string for the re-prepares
    public String cursorAttributesToSendOnPrepare_ = null;

    // The following members are for the exclusive use of prepared statements that require auto-generated keys to be returned
    public PreparedStatement preparedStatementForAutoGeneratedKeys_;
    public ResultSet generatedKeysResultSet_;
    public String[] generatedKeysColumnNames_;
    public int autoGeneratedKeys_ = java.sql.Statement.NO_GENERATED_KEYS;

    // This flag makes sure that only one copy of this statement
    // will be in connection_.commitListeners_.


    //---------------------constructors/finalizer---------------------------------

    private Statement() throws SqlException {
        initStatement();
    }

    private void resetStatement() throws SqlException {
        initStatement();
    }

    private void initStatement() throws SqlException {
        materialStatement_ = null;
        connection_ = null;
        agent_ = null;
        databaseMetaData_ = null;
        resultSetType_ = java.sql.ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency_ = java.sql.ResultSet.CONCUR_READ_ONLY;
        resultSetHoldability_ = 0;
        cursorAttributesToSendOnPrepare_ = null;
        if (timeoutArrayList.size() == 0) {
            timeoutArrayList.add(null); // Make sure the list's length is 1
        }

        initResetStatement();
    }

    private void initResetStatement() throws SqlException {
        initResetPreparedStatement();

        //section_ = null; // don't set section to null because write piggyback command require a section
        if (section_ != null) {
            section_.free();
        }
        sqlMode_ = 0;
        sqlUpdateMode_ = 0;
        resultSetMetaData_ = null;
    }

    protected void initResetPreparedStatement() {
        warnings_ = null;
        //section_ = null;
        resultSet_ = null;
        updateCount_ = -1;
        returnValueFromProcedure_ = 0;
        cursorName_ = null;
        openOnClient_ = true;
        openOnServer_ = false;
        indexOfCurrentResultSet_ = -1;
        resultSetList_ = null;
        timeout_ = 0;
        doWriteTimeout = false;
        maxRows_ = 0;
        maxFieldSize_ = 0;
        escapedProcedureCallWithResult_ = false;
        isCatalogQuery_ = false;
        isAutoCommittableStatement_ = true;

        if (batch_ == null) {
            batch_ = new java.util.ArrayList();
        } else {
            batch_.clear();
        }
        fetchSize_ = 0;
        fetchDirection_ = java.sql.ResultSet.FETCH_FORWARD;
        singletonRowData_ = null;
        numInvisibleRS_ = 0;
        preparedStatementForAutoGeneratedKeys_ = null;
        generatedKeysResultSet_ = null;
        generatedKeysColumnNames_ = null;
        autoGeneratedKeys_ = java.sql.Statement.NO_GENERATED_KEYS;

        // these members were not initialized
        isPreparedStatement_ = false;
    }

    // If a dataSource is passed into resetClientConnection(), then we will assume
    // properties on the dataSource may have changed, and we will need to go through
    // the open-statement list on the connection and do a full reset on all statements,
    // including preparedStatement's and callableStatement's.  This is because property
    // change may influence the section we allocate for the preparedStatement, and
    // also the cursor attributes, i.e. setCursorSensitivity().
    // If no dataSource is passed into resetClientConnection(), then we will do the
    // minimum reset required for preparedStatement's and callableStatement's.
    public void reset(boolean fullReset) throws SqlException {
        if (fullReset) {
            connection_.resetStatement(this);
        } else {
            initResetStatement();
            materialStatement_.reset_();
        }
    }

    public Statement(Agent agent, Connection connection) throws SqlException {
        this();
        initStatement(agent, connection);
    }

    public void resetStatement(Agent agent, Connection connection) throws SqlException {
        resetStatement();
        initStatement(agent, connection);
    }

    private void initStatement(Agent agent, Connection connection) {
        agent_ = agent;
        connection_ = connection;
        databaseMetaData_ = connection.databaseMetaData_;
    }

    // For jdbc 2 statements with scroll attributes
    public Statement(Agent agent, Connection connection, int type, int concurrency, int holdability,
                     int autoGeneratedKeys, String[] columnNames) throws SqlException {
        this(agent, connection);
        initStatement(type, concurrency, holdability, autoGeneratedKeys, columnNames);
    }

    public void resetStatement(Agent agent, Connection connection, int type, int concurrency, int holdability,
                               int autoGeneratedKeys, String[] columnNames) throws SqlException {
        resetStatement(agent, connection);
        initStatement(type, concurrency, holdability, autoGeneratedKeys, columnNames);
    }

    private void initStatement(int type, int concurrency, int holdability,
                               int autoGeneratedKeys, String[] columnNames) throws SqlException {
        switch (type) {
        case java.sql.ResultSet.TYPE_FORWARD_ONLY:
        case java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE:
        case java.sql.ResultSet.TYPE_SCROLL_SENSITIVE:
            resultSetType_ = type;
            break;
        default:
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "ResultSet Type " + type + " is invalid.");
        }

        switch (concurrency) {
        case java.sql.ResultSet.CONCUR_READ_ONLY:
        case java.sql.ResultSet.CONCUR_UPDATABLE:
            resultSetConcurrency_ = concurrency;
            break;
        default:
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "ResultSet Concurrency " + concurrency + " is invalid.");
        }

        switch (holdability) {
        case JDBC30Translation.CLOSE_CURSORS_AT_COMMIT:
        case JDBC30Translation.HOLD_CURSORS_OVER_COMMIT:
            resultSetHoldability_ = holdability;
            break;
        default:
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "ResultSet holdability " + holdability + " is invalid.");
        }

        switch (autoGeneratedKeys) {
        case java.sql.Statement.NO_GENERATED_KEYS:
        case java.sql.Statement.RETURN_GENERATED_KEYS:
            autoGeneratedKeys_ = autoGeneratedKeys;
            break;
        default:
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "Statement auto-generated keys value " + autoGeneratedKeys +
                    " is invalid.");
        }

        generatedKeysColumnNames_ = columnNames;
    }

    protected void finalize() throws java.lang.Throwable {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "finalize");
        }
        if (openOnClient_) {
            synchronized (connection_) {
                closeX();
            }
        }
        super.finalize();
    }

    // ---------------------------jdbc 1------------------------------------------

    public java.sql.ResultSet executeQuery(String sql) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeQuery", sql);
                }
                ResultSet resultSet = executeQueryX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeQuery", resultSet);
                }
                return resultSet;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
        
    }

    private ResultSet executeQueryX(String sql) throws SqlException {
        flowExecute(executeQueryMethod__, sql);

        checkExecuteQueryPostConditions("java.sql.Statement");
        return resultSet_;
    }

    void checkExecuteQueryPostConditions(String jdbcStatementInterfaceName) throws SqlException {
        // We'll just rely on finalizers to close the dangling result sets.
        if (resultSetList_ != null && resultSetList_.length != 1) {
            throw new SqlException(agent_.logWriter_, jdbcStatementInterfaceName + ".executeQuery() cannot be called " +
                    "because multiple result sets were returned." +
                    " Use " + jdbcStatementInterfaceName + ".execute() to obtain multiple results.");
        }

        if (resultSet_ == null) {
            throw new SqlException(agent_.logWriter_, jdbcStatementInterfaceName + ".executeQuery() was called " +
                    "but no result set was returned." +
                    " Use " + jdbcStatementInterfaceName + ".executeUpdate() for non-queries.");
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeUpdate", sql);
                }
                int updateValue = executeUpdateX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeUpdate", updateValue);
                }
                return updateValue;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private int executeUpdateX(String sql) throws SqlException {
        flowExecute(executeUpdateMethod__, sql);

        checkExecuteUpdatePostConditions("java.sql.Statement");
        return updateCount_;
    }

    void checkExecuteUpdatePostConditions(String jdbcStatementInterfaceName) throws SqlException {
        // We'll just rely on finalizers to close the dangling result sets.
        if (resultSetList_ != null) {
            throw new SqlException(agent_.logWriter_, jdbcStatementInterfaceName + ".executeUpdate() cannot be called " +
                    "because multiple result sets returned." +
                    " Use " + jdbcStatementInterfaceName + ".execute() to obtain multiple results.");
        }

        // We'll just rely on the finalizer to close the dangling result set.
        if (resultSet_ != null) {
            throw new SqlException(agent_.logWriter_, jdbcStatementInterfaceName + ".executeUpdate() was called " +
                    "but a result set was returned." +
                    " Use " + jdbcStatementInterfaceName + ".executeQuery() to obtain a result set.");
        }
    }

    // The server holds statement resources until transaction end.
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

    // An untraced version of close()
    public void closeX() throws SqlException {
        if (!openOnClient_) {
            return;
        }
        // Regardless of whether or not this statement is in the prepared state,
        // we need to close any open cursors for this statement on the server.
        int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : resultSetList_.length;
        boolean willTickleServer = willTickleServer(numberOfResultSetsToClose, true);
        try {
            if (willTickleServer) {
                flowClose();
            } else {
                flowCloseOutsideUOW();
            }
        } finally {
            markClosed();
            connection_.openStatements_.remove(this);
        }
        // push the mark close of rsmd into Statement.markClosed() method
        if (resultSetMetaData_ != null) {
            resultSetMetaData_.markClosed();
            resultSetMetaData_ = null;
        }
    }

    public int getMaxFieldSize() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getMaxFieldSize");
            }
            checkForClosedStatement();
            return maxFieldSize_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setMaxFieldSize(int max) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setMaxFieldSize", max);
                }
                checkForClosedStatement();
                if (max < 0) {
                    throw new SqlException(agent_.logWriter_, "Invalid maxFieldSize value: " + max);
                }
                maxFieldSize_ = max;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getMaxRows() throws SQLException {
        try
        {
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getMaxRows", maxRows_);
            }
            return maxRows_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setMaxRows(int maxRows) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setMaxRows", maxRows);
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
                if (maxRows < 0) {
                    throw new SqlException(agent_.logWriter_, "Invalid maxRows value: " + maxRows);
                }
                maxRows_ = maxRows;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setEscapeProcessing", enable);
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getQueryTimeout() throws SQLException {
        try
        {
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getQueryTimeout", timeout_);
            }
            return timeout_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setQueryTimeout", seconds);
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
                if (seconds < 0) {
                    throw new SqlException(agent_.logWriter_,
                                           "Attempt to set a negative query timeout",
                                           "XJ074.S");
                }
                if (seconds != timeout_) {
                    timeout_ = seconds;
                    doWriteTimeout = true;
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void cancel() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "cancel");
            }
            checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
            throw new SqlException(agent_.logWriter_, "cancel() not supported by server");
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public java.sql.SQLWarning getWarnings() throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "getWarnings", warnings_);
        }
        return warnings_ == null ? null : warnings_.getSQLWarning();
    }

    public void clearWarnings() throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "clearWarnings");
            }
            clearWarningsX();
        }
    }

    // An untraced version of clearWarnings()
    public void clearWarningsX() {
        warnings_ = null;
    }

    // Dnc statements are already associated with a unique cursor name as defined
    // by our canned dnc package set.
    // ResultSet.getCursorName() should be used to
    // obtain the for update cursor name to use when executing a positioned update statement.
    // See Jdbc 3 spec section 14.2.4.4.
    public void setCursorName(String name) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setCursorName", name);
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
                if (name == null || name.equals("")) {
                    throw new SqlException(agent_.logWriter_, "Invalid cursor name.");
                }

                // Invalid to set the cursor name if there are ResultSet's open on the Statement.
                if (resultSet_ != null && resultSet_.openOnClient_) {
                    throw new SqlException(agent_.logWriter_, "Invalid operation: setCursorName() " +
                            "called when there are open ResultSet's on the Statement.");
                }

                // Duplicate cursor names not allowed.
                if (connection_.clientCursorNameCache_.containsKey(name)) {
                    throw new SqlException(agent_.logWriter_, "Duplicate cursor names are not allowed.");
                }
                connection_.clientCursorNameCache_.put(name, name);

                // section_ is null for Statement objects.  We will defer the mapping of cursorName
                // to section until when the query is executed.
                if (section_ != null) {
                    agent_.sectionManager_.mapCursorNameToQuerySection(name, (Section) section_);

                    // This means we must subtitute the <users-cursor-name> with the <canned-cursor-name>
                    // in the pass-thru sql string "...where current of <canned-cursor-name>".
                    section_.setClientCursorName(name);
                }
                cursorName_ = name;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //----------------------- Multiple Results --------------------------


    public boolean execute(String sql) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "execute", sql);
                }
                boolean b = executeX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "execute", b);
                }
                return b;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    boolean executeX(String sql) throws SqlException {
        flowExecute(executeMethod__, sql);
        return resultSet_ != null;
    }

    public java.sql.ResultSet getResultSet() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getResultSet");
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getResultSet", resultSet_);
                }
                return resultSet_;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getUpdateCount() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getUpdateCount");
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getUpdateCount", updateCount_);
                }
                return updateCount_;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean getMoreResults() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getMoreResults");
                }
                boolean resultIsResultSet = getMoreResultsX(CLOSE_ALL_RESULTS);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getMoreResults", resultIsResultSet);
                }
                return resultIsResultSet;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    public void setFetchDirection(int direction) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setFetchDirection", direction);
                }
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
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
            checkForClosedStatement();
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
                checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)

                if (rows < 0 || (maxRows_ != 0 && rows > maxRows_)) {
                    throw new SqlException(agent_.logWriter_, "Invalid fetch size " + rows).getSQLException();
                }
                fetchSize_ = rows;
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
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getFetchSize", fetchSize_);
            }
            return fetchSize_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getResultSetConcurrency() throws SQLException {
        try
        {
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getResultSetConcurrency", resultSetConcurrency_);
            }
            return resultSetConcurrency_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getResultSetType() throws SQLException {
        try
        {
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getResultSetType", resultSetType_);
            }
            return resultSetType_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void addBatch(String sql) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "addBatch", sql);
                }
                checkForClosedStatement();
                sql = connection_.nativeSQLX(sql);
                batch_.add(sql);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void clearBatch() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "clearBatch");
                }
                checkForClosedStatement();
                batch_.clear();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int[] executeBatch() throws SQLException, BatchUpdateException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeBatch");
                }
                int[] updateCounts = executeBatchX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeBatch", updateCounts);
                }
                return updateCounts;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private int[] executeBatchX() throws SqlException, BatchUpdateException {
        checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
        clearWarningsX(); // Per jdbc spec 0.7, and getWarnings() javadoc
        resultSetList_ = null;
        // Initialize all the updateCounts to indicate failure
        // This is done to account for "chain-breaking" errors where we cannot
        // read any more replies
        int[] updateCounts = new int[batch_.size()];
        for (int i = 0; i < batch_.size(); i++) {
            updateCounts[i] = -3;
        }
        flowExecuteBatch(updateCounts);
        return updateCounts;
    }

    public java.sql.Connection getConnection() throws SQLException {
        try
        {
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getConnection", connection_);
            }
            return connection_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //--------------------------JDBC 3.0-----------------------------

    public boolean getMoreResults(int current) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getMoreResults", current);
                }
                boolean resultIsResultSet = getMoreResultsX(current);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getMoreResults", resultIsResultSet);
                }
                return resultIsResultSet;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean getMoreResultsX(int current) throws SqlException {
        checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
        boolean resultIsResultSet;
        updateCount_ = -1;
        if (resultSetList_ == null) {
            if (resultSet_ != null) {
                if (current != KEEP_CURRENT_RESULT) {
                    resultSet_.closeX();
                }
                resultSet_ = null;
            }
            resultIsResultSet = false;
        } else {
            if (numInvisibleRS_ == 0 &&
                    current == CLOSE_CURRENT_RESULT &&
                    resultSetList_[indexOfCurrentResultSet_] != null) {
                resultSetList_[indexOfCurrentResultSet_].closeX();
            }
            resultIsResultSet = indexOfCurrentResultSet_ + 1 < resultSetList_.length;
        }
        if ((current == CLOSE_ALL_RESULTS) && (numInvisibleRS_ == 0)) {
            int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : indexOfCurrentResultSet_ + 1;
            boolean willTickleServer = willTickleServer(numberOfResultSetsToClose, false);
            if (willTickleServer) {
                flowCloseRetrievedResultSets();
            } else {
                flowCloseRetrievedResultSetsOutsideUOW();
            }
        }
        if (resultIsResultSet) {
            resultSet_ = resultSetList_[++indexOfCurrentResultSet_];
        } else {
            resultSet_ = null;
        }

        return resultIsResultSet;
    }

    public java.sql.ResultSet getGeneratedKeys() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getGeneratedKeys");
            }
            checkForClosedStatement();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getGeneratedKeys", generatedKeysResultSet_);
            }
            return generatedKeysResultSet_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeUpdate", sql, autoGeneratedKeys);
                }
                autoGeneratedKeys_ = autoGeneratedKeys;
                int updateValue = executeUpdateX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeUpdate", updateValue);
                }
                return updateValue;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "executeUpdate", sql, columnIndexes);
            }
            checkForClosedStatement();
            throw new SqlException(agent_.logWriter_, "Driver not capable");
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int executeUpdate(String sql, String columnNames[]) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeUpdate", sql, columnNames);
                }
                generatedKeysColumnNames_ = columnNames;
                int updateValue = executeUpdateX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeUpdate", updateValue);
                }
                return updateValue;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "execute", sql, autoGeneratedKeys);
                }
                autoGeneratedKeys_ = autoGeneratedKeys;
                boolean b = executeX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "execute", b);
                }
                return b;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean execute(String sql, int columnIndexes[]) throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "execute", sql, columnIndexes);
            }
            checkForClosedStatement();
            throw new SqlException(agent_.logWriter_, "Driver not capable");
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean execute(String sql, String columnNames[]) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "execute", sql, columnNames);
                }
                generatedKeysColumnNames_ = columnNames;
                boolean b = executeX(sql);
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "execute", b);
                }
                return b;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public int getResultSetHoldability() throws SQLException {
        try
        {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getResultSetHoldability");
            }
            checkForClosedStatement();
            return resultSetHoldability_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // ----------------------- box car and callback methods ---------------------
    // All callbacks must be client-side only operations.
    // Use of MaterialStatement interface is necessary to avoid multiple inheritance problem in Java.
    public void writeSetSpecialRegister(java.util.ArrayList sqlsttList) throws SqlException {
        materialStatement_.writeSetSpecialRegister_(sqlsttList);
    }

    public void readSetSpecialRegister() throws SqlException {
        materialStatement_.readSetSpecialRegister_();
    }

    public void writeExecuteImmediate(String sql,
                                      Section section) throws SqlException {
        materialStatement_.writeExecuteImmediate_(sql, section);
    }

    public void readExecuteImmediate() throws SqlException {
        materialStatement_.readExecuteImmediate_();
    }

    public void completeExecuteImmediate(Sqlca sqlca) {
        int sqlcode = completeSqlca(sqlca);
        if (sqlcode < 0) {
            return;
        }
        if (sqlca != null) {
            updateCount_ = sqlca.getUpdateCount();
        }
    }

    public void readExecuteImmediateForBatch(String sql) throws SqlException {
        materialStatement_.readExecuteImmediateForBatch_(sql);
    }

    public void writePrepareDescribeOutput(String sql,
                                           Section section) throws SqlException {
        materialStatement_.writePrepareDescribeOutput_(sql, section);
    }

    public void readPrepareDescribeOutput() throws SqlException {
        materialStatement_.readPrepareDescribeOutput_();
    }

    public void completePrepareDescribeOutput(ColumnMetaData resultSetMetaData,
                                              Sqlca sqlca) {
        completePrepare(sqlca);
        resultSetMetaData_ = resultSetMetaData;
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceResultSetMetaData(this, resultSetMetaData_);
        }
    }

    // Used for re-prepares across commit only
    public void writePrepare(String sql, Section section) throws SqlException {
        materialStatement_.writePrepare_(sql, section);
    }

    public void readPrepare() throws SqlException {
        materialStatement_.readPrepare_();
    }

    public void completePrepare(Sqlca sqlca) {
        int sqlcode = completeSqlca(sqlca);
        if (sqlcode < 0) {
            return;
        }
        markPrepared();
    }

    public void writeOpenQuery(Section section,
                               int fetchSize,
                               int resultSetType) throws SqlException {
        materialStatement_.writeOpenQuery_(section,
                fetchSize,
                resultSetType);
    }

    public void readOpenQuery() throws SqlException {
        materialStatement_.readOpenQuery_();
    }

    public void completeOpenQuery(Sqlca sqlca, ResultSet resultSet) {
        completeSqlca(sqlca);
        resultSet_ = resultSet;
        // For NET, resultSet_ == null when open query fails and receives OPNQFLRM.
        // Then, in NetStatementReply.parseOpenQueryFailure(), completeOpenQuery() is
        // invoked with resultSet explicitly set to null.
        if (resultSet == null) {
            return;
        }
        resultSet.resultSetMetaData_ = resultSetMetaData_;
        resultSet.resultSetMetaData_.resultSetConcurrency_ = resultSet.resultSetConcurrency_;

        // only cache the Cursor object for a PreparedStatement and if a Cursor object is
        // not already cached.
        if (cachedCursor_ == null && isPreparedStatement_) {
            cachedCursor_ = resultSet_.cursor_;
        }

        // The following two assignments should have already happened via prepareEvent(),
        // but are included here for safety for the time being.
        if (sqlca != null && sqlca.getSqlCode() < 0) {
            return;
        }
        openOnServer_ = true;
        resultSet.cursor_.rowsRead_ = 0;

        // Set fetchSize_ to the default(64) if not set by the user if the resultset is scrollable.
        // This fetchSize_ is used to check for a complete rowset when rowsets are parsed.
        // For scrollable cursors when the fetchSize_ is not set, (fetchSize_ == 0), a default
        // fetchSize of 64 is sent on behalf of the application, so we need to update the fetchSize_
        // here to 64.
        if (resultSet_.fetchSize_ == 0 &&
                (resultSet_.resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE ||
                resultSet_.resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE)) {
            resultSet_.fetchSize_ = org.apache.derby.client.am.Configuration.defaultFetchSize;
        }
    }

    public void completeExecuteCallOpenQuery(Sqlca sqlca, ResultSet resultSet, ColumnMetaData resultSetMetaData, Section generatedSection) {
        resultSet.completeSqlca(sqlca);
        // For CallableStatements we can't just clobber the resultSet_ here, must use setResultSetEvent() separately
        resultSet.resultSetMetaData_ = resultSetMetaData;

        // The following two assignments should have already happened via prepareEvent(),
        // but are included here for safety for the time being.
        if (sqlca != null && sqlca.getSqlCode() < 0) {
            return;
        }
        openOnServer_ = true;
        resultSet.cursor_.rowsRead_ = 0;

        resultSet.generatedSection_ = generatedSection;

        // We are always sending the default fetchSize of 64 if not set for stored procedure calls.
        // This is different from the "normal" cursor case for forward_only cursors, where if
        // fetchSize_ is not set, we do not send any default value.  Here since we always send
        // the fetchSize_, we need to set it to what we sent.
        if (resultSet.fetchSize_ == 0) {
            resultSet.fetchSize_ = org.apache.derby.client.am.Configuration.defaultFetchSize;
        }
    }

    public void writeExecuteCall(boolean outputExpected,
                                 String procedureName,
                                 Section section,
                                 int fetchSize,
                                 boolean suppressResultSets, // for batch updates == true
                                 int resultSetType,
                                 ColumnMetaData parameterMetaData,
                                 Object[] inputs) throws SqlException {
        materialStatement_.writeExecuteCall_(outputExpected,
                procedureName,
                section,
                fetchSize,
                suppressResultSets,
                resultSetType,
                parameterMetaData,
                inputs);
    }

    public void readExecuteCall() throws SqlException {
        materialStatement_.readExecuteCall_();
    }

    public void completeExecuteCall(Sqlca sqlca, Cursor singletonParams, ResultSet[] resultSets) {
        completeExecuteCall(sqlca, singletonParams);
        resultSetList_ = resultSets;
        if (resultSets != null) {
            resultSet_ = resultSets[0];
        }
        indexOfCurrentResultSet_ = 0;
    }

    public void completeExecuteCall(Sqlca sqlca, Cursor singletonParams) // no result sets returned
    {
        completeExecute(sqlca);
        //if ((sqlca != null) && ((sqlca.getSqlCode() < 0) || (sqlca.getSqlCode() == 100)))
        if (sqlca != null && sqlca.getSqlCode() < 0) {
            singletonRowData_ = null;
        } else {
            singletonRowData_ = singletonParams;
            if (cachedSingletonRowData_ == null && isPreparedStatement_) {
                cachedSingletonRowData_ = singletonRowData_;
            }
        }
    }

    // Callback for CALLS, and PreparedStatement updates.
    public void completeExecute(Sqlca sqlca) {
        if (sqlca == null) {
            return;
        }

        int sqlcode = sqlca.getSqlCode();
        if (sqlcode < 0) {
            agent_.accumulateReadException(new SqlException(agent_.logWriter_, sqlca));
            returnValueFromProcedure_ = sqlcode;
        } else {
            updateCount_ = sqlca.getUpdateCount();
            // sometime for call statement, protocol will return updateCount_, we will always set that to 0
            // sqlMode_ is not set for statements, only for prepared statements
            if (sqlMode_ == isCall__) {
                updateCount_ = -1;
                returnValueFromProcedure_ = sqlca.getSqlErrd()[0];  ////what is this for??
            }
            // Sqlcode 466 indicates a call statement has issued and result sets returned.
            // This is a good place to set some state variable to indicate result sets are open
            // for call, so that when autocommit is true, commit will not be issued until the
            // result sets are closed.
            // Currently, commit is not issued even there is no result set.
            // do not externalize sqlcode +100
            if (sqlcode > 0 && sqlcode != 466 && sqlcode != 100) {
                accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
            }
        }
    }


    public void setUpdateCount(int updateCount) {
        updateCount_ = updateCount;
    }


    private boolean willTickleServer(int number, boolean allowAutoCommits) throws SqlException {
        boolean requiresAutocommit = false;
        if (resultSetList_ != null) {
            for (int i = 0; i < number; i++) {
                if (resultSetList_[i] != null) {
                    if (resultSetList_[i].openOnServer_) {
                        return true; // for the writeClose flow
                    }
                    if (!resultSetList_[i].autoCommitted_ && allowAutoCommits) {
                        requiresAutocommit = true; // for the commit flow
                    }
                }
            }
        } else if (generatedKeysResultSet_ != null && generatedKeysResultSet_.openOnServer_) {
            generatedKeysResultSet_.writeClose();
        } else if (resultSet_ != null) {
            if (resultSet_.openOnServer_) {
                return true; // for the writeClose flow
            }
            if (!resultSet_.autoCommitted_ && allowAutoCommits) {
                requiresAutocommit = true;
            }
        }
        if (connection_.autoCommit_ && requiresAutocommit) { // for the auto-commit;
            if (connection_.isXAConnection_) {
                return (connection_.xaState_ == Connection.XA_T0_NOT_ASSOCIATED) ;
            } else {
                return true;
            }
        }
        return false;
    }

    private void flowClose() throws SqlException {
        agent_.beginWriteChain(this);
        writeClose(true);  // true means permit auto-commits
        agent_.flow(this);
        readClose(true);  // true means permit auto-commits
        agent_.endReadChain();
    }

    private void flowCloseOutsideUOW() throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        writeClose(true);  // true means permit auto-commits
        agent_.flowOutsideUOW();
        readClose(true);  // true means permit auto-commits
        agent_.endReadChain();
    }

    final void writeClose(boolean allowAutoCommits) throws SqlException {
        writeCloseResultSets(allowAutoCommits);
    }

    final void readClose(boolean allowAutoCommits) throws SqlException {
        readCloseResultSets(allowAutoCommits);
    }

    boolean writeCloseResultSets(boolean allowAutoCommits) throws SqlException {
        int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : resultSetList_.length;
        return writeCloseResultSets(numberOfResultSetsToClose, allowAutoCommits);
    }

    // The connection close processing passes allowAutoCommits=false because if we drove an
    // autocommits after each statement close, then when we issue close requests on non-held cursors
    // the server would complain that the non-held cursor was already closed from the previous statement's auto-commit.
    // So the solution is to never autocommit statements during connection close processing.
    //
    // Here's the operative explanation:
    // Given a sequence of open statements S1, S2, .... a logic problem is occuring after S1 close-query
    // drives an auto-commit, and S2 close-query is driven against a non-held cursor.
    // The first auto-commit driven by S1 triggers a callback that closes S2's non-held cursor,
    // and so the subsequent S2 close-query request generates an error from the server saying
    // that the cursor is already closed.
    //
    // This is fixed by passing a flag to our statement close processing that prevents
    // driving additional auto-commits after each statement close.
    // Connectino close drives its own final auto-commit.
    //
    boolean writeCloseResultSets(int number, boolean allowAutoCommits) throws SqlException {
        boolean requiresAutocommit = false;
        if (resultSetList_ != null) {
            for (int i = 0; i < number; i++) {
                if (resultSetList_[i] != null) {
                    if (resultSetList_[i].openOnServer_) {
                        resultSetList_[i].writeClose();
                    }
                    if (!resultSetList_[i].autoCommitted_ && allowAutoCommits) {
                        requiresAutocommit = true;
                    }
                }
            }
        } else if (generatedKeysResultSet_ != null && generatedKeysResultSet_.openOnServer_) {
            generatedKeysResultSet_.writeClose();
        } else if (resultSet_ != null) {
            if (resultSet_.openOnServer_) {
                resultSet_.writeClose();
            }
            if (!resultSet_.autoCommitted_ && allowAutoCommits) {
                requiresAutocommit = true;
            }
        }
        if (connection_.autoCommit_ && requiresAutocommit && isAutoCommittableStatement_) {
            connection_.writeAutoCommit();
            if (connection_.isXAConnection_) {
                return (connection_.xaState_ == Connection.XA_T0_NOT_ASSOCIATED) ;
            } else {
                return true;
            }
        }
        return false;
    }

    // Helper method for S.flowCloseResultSets() and PS.flowExecute()
    void readCloseResultSets(boolean allowAutoCommits) throws SqlException {
        int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : resultSetList_.length;
        readCloseResultSets(numberOfResultSetsToClose, allowAutoCommits);
    }

    void readCloseResultSets(int number, boolean allowAutoCommits) throws SqlException {
        boolean requiredAutocommit = false;
        if (resultSetList_ != null) {
            for (int i = 0; i < number; i++) {
                if (resultSetList_[i] != null) {
                    if (resultSetList_[i].openOnServer_) {
                        resultSetList_[i].readClose();
                    } else {
                        resultSetList_[i].markClosed();
                    }
                    if (!resultSetList_[i].autoCommitted_ && allowAutoCommits) {
                        requiredAutocommit = true;
                    }
                }
            }
        } else if (generatedKeysResultSet_ != null) {
            if (generatedKeysResultSet_.openOnServer_) {
                generatedKeysResultSet_.readClose();
            } else {
                generatedKeysResultSet_.markClosed();
            }
        } else if (resultSet_ != null) {
            if (resultSet_.openOnServer_) {
                resultSet_.readClose();
            } else {
                resultSet_.markClosed();
            }
            if (!resultSet_.autoCommitted_ && allowAutoCommits) {
                requiredAutocommit = true;
            }
        }
        // we only commit when auto commit is turned on and at least one result set needed closing on server.
        if (connection_.autoCommit_ && requiredAutocommit && isAutoCommittableStatement_) {
            connection_.readAutoCommit();
        }
    }

    private void flowCloseRetrievedResultSets() throws SqlException {
        int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : indexOfCurrentResultSet_ + 1;
        agent_.beginWriteChain(this);
        // Need to refactor the ResultSet.readClose() path to check if we are the
        // last result set closed in a set of multiple result sets of the owning statement,
        // if so, we need to flow the auto-commit (but only then).
        // currently, the code to do this is only in the closeX() path, which isn't called here
        writeCloseResultSets(numberOfResultSetsToClose, false);
        agent_.flow(this);
        readCloseResultSets(numberOfResultSetsToClose, false);  // true means permit auto-commits
        agent_.endReadChain();
    }

    private void flowCloseRetrievedResultSetsOutsideUOW() throws SqlException {
        int numberOfResultSetsToClose = (resultSetList_ == null) ? 0 : indexOfCurrentResultSet_ + 1;
        agent_.beginWriteChainOutsideUOW();
        // Need to refactor the ResultSet.readClose() path to check if we are the
        // last result set closed in a set of multiple result sets of the owning statement,
        // if so, we need to flow the auto-commit (but only then).
        // currently, the code to do this is only in the closeX() path, which isn't called here
        writeCloseResultSets(numberOfResultSetsToClose, false);
        agent_.flowOutsideUOW();
        readCloseResultSets(numberOfResultSetsToClose, false);  // true means permit auto-commits
        agent_.endReadChain();
    }

    public int completeSqlca(Sqlca sqlca) {
        if (sqlca == null) {
            return 0;
        }
        int sqlcode = sqlca.getSqlCode();
        if (sqlcode < 0) {
            connection_.agent_.accumulateReadException(new SqlException(agent_.logWriter_, sqlca));
        } else if (sqlcode > 0) {
            accumulateWarning(new SqlWarning(agent_.logWriter_, sqlca));
        }
        return sqlcode;
    }

    public void completeExecuteSetStatement(Sqlca sqlca) {
    }

    void markClosedOnServer() {
        if (section_ != null) {
            section_.free();
            section_ = null;
        }
        openOnServer_ = false;
        // if an error occurs during the middle of the reset, before the statement
        // has a chance to reset its materialStatement_, and Agent.disconnectEvent() is called,
        // then the materialStatement_ here can be null.
        if (materialStatement_ != null) {
            materialStatement_.markClosedOnServer_();
        }
    }

    void markClosed() {
        openOnClient_ = false;
        markResultSetsClosed();
        // in case a cursorName was set on the Statement but the Statement was
        // never used to execute a query, the cursorName will not be removed
        // when the resultSets are mark closed, so we need to remove the
        // cursorName form the cache.
        removeClientCursorNameFromCache();
        markPreparedStatementForAutoGeneratedKeysClosed();
        markClosedOnServer();
    }

    void markPreparedStatementForAutoGeneratedKeysClosed() {
        if (preparedStatementForAutoGeneratedKeys_ != null) {
            preparedStatementForAutoGeneratedKeys_.markClosed();
        }
    }

    /**
     * Mark all ResultSets associated with this statement as
     * closed. The ResultSets will not be removed from the commit and
     * rollback listeners list in
     * <code>org.apache.derby.client.am.Connection</code>.
     */
    void markResultSetsClosed() {
        markResultSetsClosed(false);
    }

    /**
     * Mark all ResultSets associated with this statement as
     * closed.
     *
     * @param removeListener if true the ResultSets will be removed
     * from the commit and rollback listeners list in
     * <code>org.apache.derby.client.am.Connection</code>.
     */
    void markResultSetsClosed(boolean removeListener) {
        if (resultSetList_ != null) {
            for (int i = 0; i < resultSetList_.length; i++) {
                if (resultSetList_[i] != null) {
                    resultSetList_[i].markClosed(removeListener);
                }
                resultSetList_[i] = null;
            }
        }
        if (generatedKeysResultSet_ != null) {
            generatedKeysResultSet_.markClosed(removeListener);
        }
        if (resultSet_ != null) {
            resultSet_.markClosed(removeListener);
        }
        resultSet_ = null;
        resultSetList_ = null;
        generatedKeysResultSet_ = null;
    }

    private void flowExecute(int executeType, String sql) throws SqlException {
        checkForClosedStatement(); // Per jdbc spec (see java.sql.Statement.close() javadoc)
        checkAutoGeneratedKeysParameters();
        clearWarningsX(); // Per jdbc spec 0.7, and getWarnings() javadoc

        sql = escape(sql);
        parseSqlAndSetSqlModes(sql);
        if (sqlMode_ == isUpdate__) {
            updateCount_ = 0;
        } else {
            updateCount_ = -1;
        }

        checkForAppropriateSqlMode(executeType, sqlMode_);

        boolean timeoutSent = false;

            agent_.beginWriteChain(this);
            boolean piggybackedAutoCommit = writeCloseResultSets(true);  // true means permit auto-commits

            ResultSet scrollableRS = null;
            Section newSection = null;
            boolean repositionedCursor = false;

            if (doWriteTimeout) {
                timeoutArrayList.set(0, TIMEOUT_STATEMENT + timeout_);
                writeSetSpecialRegister(timeoutArrayList);
                doWriteTimeout = false;
                timeoutSent = true;
            }
            switch (sqlMode_) {
            case isQuery__:
                newSection = agent_.sectionManager_.getDynamicSection(resultSetHoldability_);

                // if client's cursor name is set, map it to the query section in the hashtable
                // after we obtain the section.
                if (cursorName_ != null) {
                    agent_.sectionManager_.mapCursorNameToQuerySection(cursorName_, newSection);

                    // This means we must subtitute the <users-cursor-name> with the <canned-cursor-name>
                    // in the pass-thru sql string "...where current of <canned-cursor-name>".
                    newSection.setClientCursorName(cursorName_);
                }

                writePrepareDescribeOutput(sql, newSection);
                writeOpenQuery(newSection,
                        fetchSize_,
                        resultSetType_);
                break;
            case isUpdate__:
                String cursorName = null;
                if (sqlUpdateMode_ == isDeleteSql__ || sqlUpdateMode_ == isUpdateSql__) {
                    String[] sqlAndCursorName = extractCursorNameFromWhereCurrentOf(sql);
                    if (sqlAndCursorName != null) {
                        cursorName = sqlAndCursorName[0];
                        sql = sqlAndCursorName[1];
                    }
                }
                if (cursorName != null) {
                    newSection = agent_.sectionManager_.getPositionedUpdateSection(cursorName, true); // true means get an execute immediate section
                    if (newSection == null) {
                        throw new SqlException(agent_.logWriter_, "Invalid cursor name \"" + cursorName +
                                "\" in the Update/Delete statement.");
                    }
                    scrollableRS = agent_.sectionManager_.getPositionedUpdateResultSet(cursorName);
                    // do not need to reposition for rowset cursors
                    if (scrollableRS != null && !scrollableRS.isRowsetCursor_) {
                        repositionedCursor =
                                scrollableRS.repositionScrollableResultSetBeforeJDBC1PositionedUpdateDelete();
                        if (!repositionedCursor) {
                            scrollableRS = null;
                        }
                    }

                    // if client's cursor name is set, and the cursor name in the positioned update
                    // string is the same as the client's cursor name, replace client's cursor name
                    // with the server's cursor name.
                    if (newSection.getClientCursorName() != null &&
                            cursorName.compareTo(newSection.getClientCursorName()) == 0) {
                        // substitute cusor name in pass thru sql string
                        sql = substituteClientCursorNameWithServerCursorName(sql, newSection);
                    }
                    writeExecuteImmediate(sql, newSection);
                }
                // if sql is an insert and columnNames is not null, and
                // then transform the insert statement into an
                // select from insert statement.
                // else chain an "select from identity_val_local()" to the insert statement
                else if (sqlUpdateMode_ == isInsertSql__ && generatedKeysColumnNames_ != null) {
                    newSection = agent_.sectionManager_.getDynamicSection(resultSetHoldability_);
                    writePrepareDescribeOutput(constructSelectFromInsertSQL(sql), newSection);
                    writeOpenQuery(newSection,
                            fetchSize_,
                            resultSetType_);
                } else {
                    newSection = agent_.sectionManager_.getDynamicSection(resultSetHoldability_);

                    writeExecuteImmediate(sql, newSection);
                    if (sqlUpdateMode_ == isInsertSql__ && autoGeneratedKeys_ == RETURN_GENERATED_KEYS) {
                        prepareAutoGeneratedKeysStatement();
                        writeOpenQuery(preparedStatementForAutoGeneratedKeys_.section_,
                                preparedStatementForAutoGeneratedKeys_.fetchSize_,
                                preparedStatementForAutoGeneratedKeys_.resultSetType_);
                    }
                }

                // maybe duplicate a commit here if the sql is a "commit"
                if (connection_.autoCommit_) {
                    connection_.writeAutoCommit();
                }
                break;
            case isCall__:
                newSection = writeExecuteCall(sql, false);

                break;
            }

            agent_.flow(this);

            readCloseResultSets(true);  // true means permit auto-commits

            if (timeoutSent) {
                readSetSpecialRegister(); // Read response to the EXCSQLSET
            }

            // turn inUnitOfWork_ flag back on and add statement
            // back on commitListeners_ list if they were off
            // by an autocommit chained to a close cursor.
            if (piggybackedAutoCommit) {
                connection_.completeTransactionStart();
            }

            markResultSetsClosed(true); // true means remove from list of commit and rollback listeners
            markClosedOnServer();
            section_ = newSection;

            switch (sqlMode_) {
            case isQuery__:
                // parse out the reply to a chained prepare and open request
                readPrepareDescribeOutput();
                // This establishes statement.resultSet
                readOpenQuery();

                // resultSet_ is null if open query failed.
                // check for null resultSet_ before using it.
                // the first rowset comes back on OPEN for static non-rowset cursors.
                // no row is returned on open for rowset cursors.
                if (resultSet_ != null) {
                    resultSet_.parseScrollableRowset();
                    // If client's cursor name is set, map the client's cursor name to the ResultSet
                    // Else map the server's cursor name to the ResultSet
                    mapCursorNameToResultSet();
                }

                break;

            case isUpdate__:

                // do not need to reposition for rowset cursors.
                if (scrollableRS != null && !scrollableRS.isRowsetCursor_) {
                    scrollableRS.readPositioningFetch_();
                }

                if (sqlUpdateMode_ == isInsertSql__ && generatedKeysColumnNames_ != null) {
                    readPrepareDescribeOutput();
                    readOpenQuery();
                    if (resultSet_ != null) {
                        generatedKeysResultSet_ = resultSet_;
                        resultSet_ = null;
                        updateCount_ = 1;
                    }
                } else {
                    readExecuteImmediate();

                    if (sqlUpdateMode_ == isInsertSql__ && autoGeneratedKeys_ == RETURN_GENERATED_KEYS) {
                        readPrepareAutoGeneratedKeysStatement();
                        preparedStatementForAutoGeneratedKeys_.readOpenQuery();
                        generatedKeysResultSet_ = preparedStatementForAutoGeneratedKeys_.resultSet_;
                        preparedStatementForAutoGeneratedKeys_.resultSet_ = null;
                    }
                }

                if (connection_.autoCommit_) {
                    connection_.readAutoCommit();
                }
                break;

            case isCall__:
                readPrepare();
                readExecuteCall();
                break;

            }

            // in the case the stored procedure call is uncatalogued, we need to catch that
            // kind exception and changed the call from dynamic to static
            agent_.endReadChain();

            //  If we hear from Sun that we can just set a warning for this, then move this code to the ResultSet constructor.
            // Throw an exception if holdability returned by the server is different from requested.
            if (resultSet_ != null && resultSet_.resultSetHoldability_ != resultSetHoldability_ && sqlMode_ != isCall__) {
                throw new SqlException(agent_.logWriter_, "Unable to open resultSet with requested " +
                        "holdability " + resultSetHoldability_ + ".");
            }

        // In the case of executing a call to a stored procedure.
        if (sqlMode_ == isCall__) {
            parseStorProcReturnedScrollableRowset();
            // When there is no result sets back, we will commit immediately when autocommit is true.
            if (connection_.autoCommit_ && resultSet_ == null && resultSetList_ == null) {
                connection_.flowAutoCommit();
            }
        }
    }

    void flowExecuteBatch(int[] updateCounts) throws SqlException, BatchUpdateException {
        SqlException chainBreaker = null;
        boolean isCallCataloguedBestGuess = true;
        agent_.beginBatchedWriteChain(this);
        for (int i = 0; i < batch_.size(); i++) {
            boolean flowSQL = true;
            String sql = (String) batch_.get(i);
            parseSqlAndSetSqlModes(sql);
            try {
                checkForInvalidBatchedSql(sql);
            } catch (SqlException e) {
                flowSQL = false;
            }

            // if you have a length mismatch for a lob flow, then we need to return a -3
            // need to trap the exceptions coming back from writeExecuteImmediate and continue on with a -3
            // net will need to be able to reset the send buffer
            if (flowSQL) {
                if (section_ != null) {
                    section_.free();
                }
                if (sqlMode_ != isCall__) {
                    section_ =
                            agent_.sectionManager_.getDynamicSection(resultSetHoldability_);
                    writeExecuteImmediate(sql, section_);
                } else {
                    section_ = writeExecuteCall(sql, true);
                }
            }
        }

        if (connection_.autoCommit_) {
            connection_.writeAutoCommit();
        }

        agent_.flowBatch(this, batch_.size());

        try {
            for (int i = 0; i < batch_.size(); i++) {
                agent_.setBatchedExceptionLabelIndex(i);
                SqlException invalidSQLCaughtByClient = null;
                String sql = (String) batch_.get(i);
                parseSqlAndSetSqlModes(sql);
                try {
                    checkForInvalidBatchedSql(sql);
                } catch (SqlException e) {
                    invalidSQLCaughtByClient = e;
                }
                if (invalidSQLCaughtByClient == null) {
                    updateCount_ = -1;
                    if (sqlMode_ != isCall__) {
                        readExecuteImmediateForBatch(sql);
                    } else {
                        if (isCallCataloguedBestGuess) {
                            readPrepare();
                        }
                        readExecuteCall();
                    }
                } else {
                    agent_.accumulateReadException(invalidSQLCaughtByClient);
                    updateCount_ = java.sql.Statement.EXECUTE_FAILED;
                    invalidSQLCaughtByClient = null;
                }

                updateCounts[i] = updateCount_;

                // DERBY doesn't return an update count for DDL statements, so we need to
                // remap our initial value of -1 (represents invalid update count) to a
                // valid update count of zero.
                if (updateCounts[i] == -1) {
                    updateCounts[i] = 0;
                }
            }
            agent_.disableBatchedExceptionTracking(); // to prvent the following readCommit() from getting a batch label
            if (connection_.autoCommit_) {
                connection_.readAutoCommit(); // this could throw a chainbreaker too
            }
        }
                // for chain-breaking exception only, all read() methods do their own accumulation
                // this catches the entire accumulated chain, we need to be careful not to
                // reaccumulate it on the agent since the batch labels will be overwritten if
                // batch exception tracking is enabled.
        catch (SqlException e) {
            chainBreaker = e;
            chainBreaker.setNextException(new SqlException(agent_.logWriter_,
                    "Non-recoverable chain-breaking exception occurred during batch processing.  " +
                    "The batch is terminated non-atomically."));
        }
        // We need to clear the batch before any exception is thrown from agent_.endBatchedReadChain().
        batch_.clear();
        agent_.endBatchedReadChain(updateCounts, chainBreaker);
    }

    private Section writeExecuteCall(String sql,
                                     boolean isBatch) throws SqlException {
        Section newSection = null;

        newSection = agent_.sectionManager_.getDynamicSection(resultSetHoldability_);
        // this code is beneficial only if there is literal in the sql call statement
        writePrepare(sql, newSection);
        writeExecuteCall(false, // no out parameters, outputExpected = false
                null, // sql is prepared, procedureName = null
                newSection,
                fetchSize_,
                isBatch, // do not suppress ResultSets for regular CALLs
                resultSetType_,
                null, // no parameters, parameterMetaData = null
                null); // no parameters, inputs = null

        return newSection;
    }

    //------------------material layer event callbacks follow---------------------
    // All callbacks are client-side only operations

    public void listenToUnitOfWork() {
    } // do nothing for now.

    public void completeLocalCommit(java.util.Iterator listenerIterator) {
    } // do nothing for now.

    public void completeLocalRollback(java.util.Iterator listenerIterator) {
    } // do nothing for now.

    // This method will not work if e is chained.
    // It is assumed that e is a single warning and is not chained.
    public void accumulateWarning(SqlWarning e) {
        if (warnings_ == null) {
            warnings_ = e;
        } else {
            warnings_.setNextWarning(e);
        }
    }

    private void markPrepared() {
        //openOnClient_ = true;
        openOnServer_ = true;
        listenToUnitOfWork();
    }

    //-------------------------------helper methods-------------------------------

    // Should investigate if it can be optimized..  if we can avoid this parsing..
    //
    void parseSqlAndSetSqlModes(String sql) throws SqlException {
        String delims = "\t\n\r\f=? (";
        java.util.StringTokenizer tokenizer = null;
        String firstToken = null;

        // See if the statement starts with a comment; if so, move
        // past the comment and get the first token of the actual
        // statement to be executed.  Note: must use "startsWith"
        // when looking for the comment delimiters instead of
        // "equals" because there may not be whitespace between the
        // the delimiter and the comment itself, ex "--my comment".
        if (sql.trim().startsWith("--")) {

            // Read each line of the statement until we find a
            // line that is NOT a comment.
            int lastEndLine = -1;
            String endline = "\n\r\f";
            tokenizer = new java.util.StringTokenizer(sql, endline, true);
            while (tokenizer.hasMoreTokens()) {
                firstToken = tokenizer.nextToken();
                if (endline.indexOf(firstToken) != -1)
                // this is some sort of newline ("\n", "\r", or "\f").
                    lastEndLine = sql.indexOf(firstToken, lastEndLine+1);
                else if (!firstToken.trim().startsWith("--"))
                    break;
            }

            if (firstToken.startsWith("--")) {
            // entire statement was just one or more comments; pass it as
            // a query to the server and let the server deal with it.
                sqlMode_ = isQuery__;
                return;
            }
            else {
            // we have a non-comment line; get a tokenizer for the
            // statement beginning at the start of this line.
                tokenizer = new java.util.StringTokenizer(
                    sql.substring(lastEndLine+1), delims);
            }

        }
        else {
        // there aren't any leading comments, so just get the first token
        // in the SQL statement.
            tokenizer = new java.util.StringTokenizer(sql, delims);
        }

        if (!tokenizer.hasMoreTokens()) {
            throw new SqlException(agent_.logWriter_, "SQL passed with no tokens");
        }

        sqlUpdateMode_ = 0;
        firstToken = tokenizer.nextToken();

        if (firstToken.equalsIgnoreCase("select") || // captures <subselect> production
                firstToken.equalsIgnoreCase("values")) // captures <values-clause> production
        {
            sqlMode_ = isQuery__;
        } else if (firstToken.equalsIgnoreCase("call")) // captures CALL...and ?=CALL...
        {
            sqlMode_ = isCall__;
        } else {
            parseUpdateSql(firstToken);
        }
    }

    private void parseUpdateSql(String firstToken) throws SqlException {
        sqlMode_ = isUpdate__;
        if (firstToken.equalsIgnoreCase("insert")) {
            sqlUpdateMode_ = isInsertSql__;
        }
        if (firstToken.equalsIgnoreCase("delete")) {
            sqlUpdateMode_ = isDeleteSql__;
        }
        if (firstToken.equalsIgnoreCase("update")) {
            sqlUpdateMode_ = isUpdateSql__;
        }
    }

    // the sql is assumed to start with CALL... or ?=CALL...
    String getProcedureName(String sql) throws SqlException {
        java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(sql, "\t\n\r\f= (?");
        if (!tokenizer.hasMoreTokens()) {
            throw new SqlException(agent_.logWriter_, "bugcheck");
        }
        String firstToken = tokenizer.nextToken();
        if (!firstToken.equalsIgnoreCase("call")) {
            throw new SqlException(agent_.logWriter_, "bugcheck");
        }
        if (!tokenizer.hasMoreTokens()) {
            throw new SqlException(agent_.logWriter_, "Invalid CALL syntax");
        }
        return tokenizer.nextToken();
    }

    // Try to enforce the use of this method later.
    public static String upperCaseProcedureName(String procedureName) throws SqlException {
        // upper case the parts of a 3-part procedure name unless the part is in a double quotes

        // Loop thru every character, if we're in double quotes just echo it,
        // if we're not in double quotes, upper case it.
        char[] charArray = null;
        if (procedureName.indexOf("\"") == -1) {
            return procedureName.toUpperCase();
        } else {
            charArray = procedureName.toCharArray();
            boolean inStringLiteral = false;
            for (int i = 0; i < charArray.length; i++) {
                if (charArray[i] == '"') {
                    inStringLiteral = !inStringLiteral;
                } else if (!inStringLiteral && charArray[i] != '.') {
                    charArray[i] = Character.toUpperCase(charArray[i]);
                }
            }
        }
        return new String(charArray);
    }

    void checkForAppropriateSqlMode(int executeType, int sqlMode) throws SqlException {
        if (executeType == executeQueryMethod__ && sqlMode == isUpdate__) {
            throw new SqlException(agent_.logWriter_, "executeQuery method cannot be used for update.");
        }
        if (executeType == executeUpdateMethod__ && sqlMode == isQuery__) {
            throw new SqlException(agent_.logWriter_, "executeUpdate method cannot be used for query.");
        }
    }

    void checkForClosedStatement() throws SqlException {
        // For some odd reason, there was a JVM hotspot error with Sun's 1.4 JDK
        // when the code was written like this:
        // agent_checkForDeferredExceptions();
        // if (!openOnClient_)
        //   throw new SqlException (agent_.logWriter_, "Invalid operation: statement closed");
        //
        if (!openOnClient_) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, "Invalid operation: statement closed");
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    // precondition: parseSqlAndSetSqlModes() must be called on the supplied sql string before invoking this method
    void checkForInvalidBatchedSql(String sql) throws SqlException {
        if (sql == null) {
            throw new SqlException(agent_.logWriter_, "Null batched SQL string passed.");
        }

        if (sqlMode_ != isCall__
                && !(sqlMode_ == isUpdate__
                && (sqlUpdateMode_ == isInsertSql__
                || sqlUpdateMode_ == isDeleteSql__
                || sqlUpdateMode_ == isUpdateSql__
                || sqlUpdateMode_ == 0)))// For any undefined pass thru statement like drop create
        {
            throw new SqlException(agent_.logWriter_, "Invalid SQL in Batch");
        }
    }


    String[] extractCursorNameFromWhereCurrentOf(String sql) {
        String lowerSql = sql.toLowerCase();
        int currentIndex = lowerSql.lastIndexOf("current");
        if (currentIndex != -1) {
            int whereIndex = lowerSql.lastIndexOf("where");
            if (whereIndex != -1) {
                String[] whereCurrentOf = {"where", "current", "of"};
                java.util.StringTokenizer st = new java.util.StringTokenizer(sql.substring(whereIndex));
                while (st.hasMoreTokens()) {
                    if (st.nextToken().equalsIgnoreCase(whereCurrentOf[0]) &&
                            st.nextToken().equalsIgnoreCase(whereCurrentOf[1]) &&
                            st.nextToken().equalsIgnoreCase(whereCurrentOf[2])) {
                        String cursorName = st.nextToken();
                        String oldCursorName = cursorName;
                        int originalCursorNameLength = cursorName.length();
                        int index = sql.lastIndexOf(cursorName);
                        if (cursorName.charAt(0) == '\"' && cursorName.charAt(cursorName.length() - 1) == '\"') {
                            cursorName = cursorName.substring(1, cursorName.length() - 1);
                        } else {
                            cursorName = cursorName.toUpperCase();
                        }
                        // we cannot assume "where current of cursorName" is always the end of the sql string
                        // with rowset cursors, it can be "where current of cursorName for row X of rowset"
                        if (sql.length() > index + originalCursorNameLength) {
                            sql = sql.substring(0, index) + cursorName + sql.substring(index + oldCursorName.length(), sql.length());
                        } else {
                            sql = sql.substring(0, index) + cursorName;
                        }
                        return new String[]{cursorName, sql}; // delimited name, so just extract the name.
                    }
                }
            }
        }
        return null;
    }

    // Substitute the client cursor name in the SQL string with the server's cursor name.
    // Only called on positioned update statements.
    protected String substituteClientCursorNameWithServerCursorName(String sql,
                                                                    Section section) throws SqlException {
        String clientCursorName = section.getClientCursorName();
        int index = sql.lastIndexOf(clientCursorName);
        if (sql.length() > index + clientCursorName.length()) {
            return sql.substring(0, index) + section.getServerCursorNameForPositionedUpdate()
                    + sql.substring(index + clientCursorName.length(), sql.length());
        } else {
            return sql.substring(0, index) + section.getServerCursorNameForPositionedUpdate();
        }
    }

    public ConnectionCallbackInterface getConnectionCallbackInterface() {
        return connection_;
    }

    // Only called on positioned upate statements
    void resetCursorNameAndRemoveFromWhereCurrentOfMappings() {
        // Remove client/server cursorName -> ResultSet mapping from the hashtable.
        // If Statement.close() is called before ResultSet.close(), then statement_.section is null.
        if (section_ != null) {
            agent_.sectionManager_.removeCursorNameToResultSetMapping(cursorName_,
                    section_.getServerCursorNameForPositionedUpdate());

            // Remove client and server cursorName -> QuerySection mapping from the hashtable
            // if one exists
            agent_.sectionManager_.removeCursorNameToQuerySectionMapping(cursorName_,
                    section_.getServerCursorNameForPositionedUpdate());
        }

        // client cursor name will be set to null when it is removed from the
        // clientCursorNameCache.
        //cursorName_ = null;
    }

    void mapCursorNameToResultSet() {
        if (cursorName_ != null) {
            agent_.sectionManager_.mapCursorNameToResultSet(cursorName_, resultSet_);
        } else {
            agent_.sectionManager_.mapCursorNameToResultSet(section_.getServerCursorName(), resultSet_);
        }
    }

    void parseStorProcReturnedScrollableRowset() throws SqlException {
        if (resultSetList_ != null) {
            for (int i = 0; i < resultSetList_.length; i++) {
                if (resultSetList_[i].scrollable_ && resultSetList_[i].cursor_.dataBufferHasUnprocessedData()) {
                    resultSetList_[i].parseScrollableRowset();
                    if (resultSetList_[i].rowCountIsUnknown()) {
                        resultSetList_[i].getRowCount();
                    }
                }
            }
        }
    }

    String escape(String sql) throws SqlException {
        String nativeSQL = sql;

        nativeSQL = connection_.nativeSQLX(sql);
        return nativeSQL;
    }

    // Called by statement constructor only for jdbc 2 statements with scroll attributes.
    // This method is not in the StatementRequest class because it is not building into
    // the request buffer directly, it is building into a cache to be written into the request
    // buffer at prepare-time.
    String cacheCursorAttributesToSendOnPrepare() throws SqlException {
        StringBuffer cursorAttributes = new StringBuffer();
        if (resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) {
            // append "SENSITIVE STATIC SCROLL"
            cursorAttributes.append(Configuration.cursorAttribute_SensitiveStatic);
        } else if (resultSetType_ == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
            // if "insensitve, updatable" cursor is asked, then server sends back error
            // and we will pass that error back to the user.
            // we will not try to catch any errors/warnings here.
            // append "INSENSITIVE SCROLL"
            cursorAttributes.append(Configuration.cursorAttribute_Insensitive);
        }

        // Default is read-only, forward-only.  No attribute needs to be sent.
        if (resultSetConcurrency_ == java.sql.ResultSet.CONCUR_UPDATABLE) {
            cursorAttributes.append(Configuration.cursorAttribute_ForUpdate); // FOR UPDATE
        }

        if ((resultSetHoldability_ == java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            cursorAttributes.append(Configuration.cursorAttribute_WithHold); // WITH HOLD
        }

        return
                (cursorAttributes == null || cursorAttributes.toString().equals(""))
                ? null
                : cursorAttributes.toString();
    }

    protected String constructSelectFromInsertSQL(String sql) {
        String temp = "select ";
        int numOfColumns = generatedKeysColumnNames_.length;

        for (int i = 0; i < numOfColumns; i++) {
            temp += generatedKeysColumnNames_[i];
            if ((i + 1) < numOfColumns) {
                temp += ",";
            }
        }
        temp += (" from final table (" + sql + ")");
        return temp;
    }

    void getPreparedStatementForAutoGeneratedKeys() throws SqlException {
        if (preparedStatementForAutoGeneratedKeys_ == null) {
            String s = "select IDENTITY_VAL_LOCAL() from SYSIBM.SYSDUMMY1";
            preparedStatementForAutoGeneratedKeys_ =
                    connection_.newPreparedStatement_(s,
                            java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY,
                            java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT,
                            java.sql.Statement.NO_GENERATED_KEYS,
                            null);
            // need a special case for Derby, since the attribute has to go through the wire.
            // This same special casing for Derby is already in place in method PS.cacheCursorAttributesToSendOnPrepare() as called by prepareStatementX().
            //  We need to figure how some way to get code reuse here, ie. to consolidate to just one special casing rather than two?
            // Maybe just call prepareStatementX() or use some special purpose prepare method like the ones in Connection.
            // Something more abstract so that we don't have to special case the WITH HOLD thing twice.
            StringBuffer cursorAttributes = new StringBuffer();
            cursorAttributes.append(Configuration.cursorAttribute_WithHold);
            preparedStatementForAutoGeneratedKeys_.cursorAttributesToSendOnPrepare_ = cursorAttributes.toString();
        }
    }

    void prepareAutoGeneratedKeysStatement() throws SqlException {
        getPreparedStatementForAutoGeneratedKeys();
        if (!preparedStatementForAutoGeneratedKeys_.openOnServer_) {
            preparedStatementForAutoGeneratedKeys_.materialPreparedStatement_.writePrepareDescribeOutput_(preparedStatementForAutoGeneratedKeys_.sql_,
                    preparedStatementForAutoGeneratedKeys_.section_);
        }
    }

    void readPrepareAutoGeneratedKeysStatement() throws SqlException {
        if (!preparedStatementForAutoGeneratedKeys_.openOnServer_) {
            preparedStatementForAutoGeneratedKeys_.materialPreparedStatement_.readPrepareDescribeOutput_();
        }
    }

    void checkAutoGeneratedKeysParameters() throws SqlException {
        if (autoGeneratedKeys_ != java.sql.Statement.NO_GENERATED_KEYS &&
                autoGeneratedKeys_ != java.sql.Statement.RETURN_GENERATED_KEYS) {
            throw new SqlException(agent_.logWriter_, "Invalid argument: " +
                    "Statement auto-generated keys value " + autoGeneratedKeys_ +
                    " is invalid.");
        }

        if (generatedKeysColumnNames_ != null) {
            throw new SqlException(agent_.logWriter_, "Driver not capable");
        }
    }

    public ColumnMetaData getGuessedResultSetMetaData() {
        return resultSetMetaData_;
    }

    public boolean isQueryMode() {
        if (this.sqlMode_ == this.isQuery__) {
            return true;
        } else {
            return false;
        }
    }

    protected void removeClientCursorNameFromCache() {
        if (cursorName_ != null) {
            connection_.clientCursorNameCache_.remove(cursorName_);
            cursorName_ = null;
        }
    }
    
    /**
     * Convenience method for resultSetCommitting(ResultSet, boolean)
     * 
     * @see Statement#resultSetCommitting(ResultSet, boolean)
     * @param closingRS The ResultSet to be closed
     * @throws SqlException
     */
    public void resultSetCommitting(ResultSet closingRS) throws SqlException {
        resultSetCommitting(closingRS, false);
    }
    
    /**
     * Method that checks to see if any other ResultSets are open. If not
     * proceeds with the autocommit.
     * 
     * @param closingRS The ResultSet to be closed
     * @param writeChain A Boolean indicating whether this method
     * is part of a chain of write from client to Server
     * @throws SqlException
     */
    public boolean resultSetCommitting(ResultSet closingRS, boolean writeChain) throws SqlException {

        // If the Connection is not in auto commit then this statement completion
        // cannot cause a commit.
        if (!connection_.autoCommit_ || closingRS.autoCommitted_)
            return false;

        // If we have multiple results, see if there is another result set open.
        // If so, then no commit. The last result set to close will close the statement.
        if (resultSetList_ != null) {
            for (int i = 0; i < resultSetList_.length; i++) {
                ResultSet crs = resultSetList_[i];
                if (crs == null)
                    continue;
                if (!crs.openOnClient_)
                    continue;
                if (crs == closingRS)
                    continue;

                // at least one still open so no commit now.
                return false;
            }
        }
        
        if (writeChain) {
            connection_.writeAutoCommit();
            return true;
        } else {
            if (connection_.flowAutoCommit()) {
                markAutoCommitted();
                return true;
            }
            return false;
        }
    }
    
    /**
     * Mark all ResultSets associated with this statement as auto-committed.   
     */
    public void markAutoCommitted() {
        if (resultSetList_ != null) {
            for (int i = 0; i < resultSetList_.length; i++)
                if (resultSetList_[i] != null) {
                    resultSetList_[i].markAutoCommitted();
                }
        } else if (resultSet_ != null) {
            resultSet_.markAutoCommitted();
        }
    }
    
    protected SQLException jdbc3FeatureNotSupported(boolean checkStatement)
        throws SQLException
    {
        try
        {
            if ( checkStatement )
                checkForClosedStatement();
            
            throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }
    
    protected SQLException jdbc3FeatureNotSupported() throws SQLException
    {
        return jdbc3FeatureNotSupported(true);
    }

}

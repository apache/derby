/*

   Derby - Class org.apache.derby.client.am.PreparedStatement

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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.ClientAutoloadedDriver;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

public class ClientPreparedStatement extends ClientStatement
    implements PreparedStatement, PreparedStatementCallbackInterface {

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    // Alias for downcast (MaterialPreparedStatementProxy) super.materialStatement.
    public MaterialPreparedStatement materialPreparedStatement_ = null;

    //-----------------------------state------------------------------------------

    String sql_;

    // This variable is only used by Batch.
    // True if a call sql statement has an OUT or INOUT parameter registered.
    boolean outputRegistered_ = false;

    // Parameter inputs are cached as objects so they may be sent on execute()
    Object[] parameters_;

    private boolean[] parameterSet_;
    boolean[] parameterRegistered_;
    
    void setInput(int parameterIndex, Object input) {
        parameters_[parameterIndex - 1] = input;
        parameterSet_[parameterIndex - 1] = true;
    }

    ColumnMetaData parameterMetaData_; // type information for input sqlda
    
    private ArrayList<int[]> parameterTypeList;


    // The problem with storing the scrollable ResultSet associated with cursorName in scrollableRS_ is
    // that when the PreparedStatement is re-executed, it has a new ResultSet, however, we always do
    // the reposition on the ResultSet that was stored in scrollableRS_, and we never update scrollableRS_
    // when PreparedStatement is re-execute.  So the new ResultSet that needs to be repositioned never
    // gets repositioned.
    // So instead of caching the scrollableRS_, we will cache the cursorName.  And re-retrieve the scrollable
    // result set from the map using this cursorName every time the PreparedStatement excutes.
    private String positionedUpdateCursorName_ = null;
    
    // the ClientPooledConnection object used to notify of the events that occur
    // on this prepared statement object
    private final ClientPooledConnection pooledConnection_;


    private void initPreparedStatement() {
        materialPreparedStatement_ = null;
        sql_ = null;
        outputRegistered_ = false;
        parameters_ = null;
        parameterSet_ = null;
        parameterRegistered_ = null;
        parameterMetaData_ = null;
        parameterTypeList = null;
        isAutoCommittableStatement_ = true;
        isPreparedStatement_ = true;
    }

    protected void initResetPreparedStatement() {
        outputRegistered_ = false;
        isPreparedStatement_ = true;
        resetParameters();
    }

    public void reset(boolean fullReset) throws SqlException {
        if (fullReset) {
            connection_.resetPrepareStatement(this);
        } else {
            super.initResetPreparedStatement();
            initResetPreparedStatement();
        }
    }

    /**
     * Resets the prepared statement for reuse in a statement pool.
     *
     * @throws SqlException if the reset fails
     * @see ClientStatement#resetForReuse
     */
    void resetForReuse()
            throws SqlException {
        resetParameters();
        super.resetForReuse();
    }

    private void resetParameters() {
        if (parameterMetaData_ != null) {
            Arrays.fill(parameters_, null);
            Arrays.fill(parameterSet_, false);
            Arrays.fill(parameterRegistered_, false);
        }
    }

    /**
     *
     * The PreparedStatement constructor used for JDBC 2 positioned update
     * statements. Called by material statement constructors.
     * It has the ClientPooledConnection as one of its parameters 
     * this is used to raise the Statement Events when the prepared
     * statement is closed
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection The connection object associated with this
     *                   PreparedStatement Object.
     * @param sql        A String object that is the SQL statement to be sent
     *                   to the database.
     * @param section    Section
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement.
     *            It is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @throws SqlException on error
     *
     */

    public ClientPreparedStatement(Agent agent,
                             ClientConnection connection,
                             String sql,
                             Section section,ClientPooledConnection cpc)
                             throws SqlException {
        super(agent, connection);
        // PreparedStatement is poolable by default
        isPoolable = true;
        initPreparedStatement(sql, section);
        pooledConnection_ = cpc;
    }
    
    private void resetPreparedStatement(Agent agent,
                                       ClientConnection connection,
                                       String sql,
                                       Section section) throws SqlException {
        super.resetStatement(agent, connection);
        initPreparedStatement();
        initPreparedStatement(sql, section);
    }

    private void initPreparedStatement(String sql, Section section) throws SqlException {
        sql_ = sql;
        isPreparedStatement_ = true;

        parseSqlAndSetSqlModes(sql_);
        setSection(section);
    }

    /**
     * The PreparedStatementConstructor used for jdbc 2 prepared statements 
     * with scroll attributes. Called by material statement constructors.
     * It has the ClientPooledConnection as one of its parameters 
     * this is used to raise the Statement Events when the prepared
     * statement is closed
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection  The connection object associated with this
     *                    PreparedStatement Object.
     * @param sql         A String object that is the SQL statement
     *                    to be sent to the database.
     * @param type        One of the ResultSet type constants.
     * @param concurrency One of the ResultSet concurrency constants.
     * @param holdability One of the ResultSet holdability constants.
     * @param autoGeneratedKeys a flag indicating whether auto-generated
     *                          keys should be returned.
     * @param columnNames an array of column names indicating the columns that
     *                    should be returned from the inserted row or rows.
     * @param columnIndexes an array of column names indicating the columns that
     *                   should be returned from the inserted row.                   
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement
     *            it is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @throws SqlException on error
     */
    public ClientPreparedStatement(Agent agent,
                             ClientConnection connection,
                             String sql,
                             int type, int concurrency, int holdability, 
                             int autoGeneratedKeys, String[] columnNames,
                             int[] columnIndexes,
                             ClientPooledConnection cpc) 
                             throws SqlException {
        super(agent, connection, type, concurrency, holdability, 
              autoGeneratedKeys, columnNames, columnIndexes);
        // PreparedStatement is poolable by default
        isPoolable = true;
        initPreparedStatement(sql);
        pooledConnection_ = cpc;
    }


    public void resetPreparedStatement(Agent agent,
                                       ClientConnection connection,
                                       String sql,
                                       int type, int concurrency, int holdability, int autoGeneratedKeys, String[] columnNames,
                                       int[] columnIndexes) throws SqlException {
        super.resetStatement(agent, connection, type, concurrency, holdability, autoGeneratedKeys, 
                columnNames, columnIndexes);
        initPreparedStatement();
        initPreparedStatement(sql);
    }

    private void initPreparedStatement(String sql) throws SqlException {
        sql_ = super.escape(sql);
        parseSqlAndSetSqlModes(sql_);
        isPreparedStatement_ = true;

        // Check for positioned update statement and assign a section from the
        // same package as the corresponding query section.
        // Scan the sql for an "update...where current of <cursor-name>".
        String cursorName = null;
        if (sqlUpdateMode_ == isDeleteSql__ || sqlUpdateMode_ == isUpdateSql__) {
            String[] sqlAndCursorName = extractCursorNameFromWhereCurrentOf(sql_);
            if (sqlAndCursorName != null) {
                cursorName = sqlAndCursorName[0];
                sql_ = sqlAndCursorName[1];
            }
        }
        if (cursorName != null) {
            positionedUpdateCursorName_ = cursorName;
            // Get a new section from the same package as the query section
            setSection(agent_.sectionManager_.getPositionedUpdateSection(cursorName, false)); // false means get a regular section
//IC see: https://issues.apache.org/jira/browse/DERBY-6082

            if (getSection() == null) {
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.CURSOR_INVALID_CURSOR_NAME), cursorName);
            }

            //scrollableRS_ = agent_.sectionManager_.getPositionedUpdateResultSet (cursorName);

            // if client's cursor name is set, and the cursor name in the positioned update
            // string is the same as the client's cursor name, replace client's cursor name
            // with the server's cursor name.
            // if the cursor name supplied in the sql string is different from the cursorName
            // set by setCursorName(), then server will return "cursor name not defined" error,
            // and no subsititution is made here.
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
            if (getSection().getClientCursorName() != null && // cursor name is user defined
                    cursorName.compareTo(getSection().getClientCursorName()) == 0)
            // client's cursor name is substituted with section's server cursor name
            {
                sql_ = substituteClientCursorNameWithServerCursorName(sql_, getSection());
            }
        } else {
            // We don't need to analyze the sql text to determine if it is a query or not.
            // This is up to the server to decide, we just pass thru the sql on flowPrepare().
            setSection(agent_.sectionManager_.getDynamicSection(resultSetHoldability_));
        }
    }

    // called immediately after the constructor by Connection prepare*() methods
    void prepare() throws SqlException {
        try {
            // flow prepare, no static initialization is needed
//IC see: https://issues.apache.org/jira/browse/DERBY-3426
            flowPrepareDescribeInputOutput();
        } catch (SqlException e) {
            this.markClosed();
            throw e;
        }
    }


    //------------------- Prohibited overrides from Statement --------------------

    public void addBatch(String sql) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "addBatch", sql);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "addBatch(String)").getSQLException();
    }

    public boolean execute(String sql) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "execute", sql);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "execute(String)").getSQLException();
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "executeQuery", sql);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "executeQuery(String)").getSQLException();
    }

    public int executeUpdate(String sql) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "executeUpdate", sql);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "executeUpdate(String)").getSQLException();
    }
    // ---------------------------jdbc 1------------------------------------------

    public ResultSet executeQuery() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeQuery");
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                ClientResultSet resultSet = executeQueryX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeQuery", resultSet);
                }
                return resultSet;
            }
        }
        catch ( SqlException se ) {
            checkStatementValidity(se);
            throw se.getSQLException();
        }
    }

    // also called by some DBMD methods
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClientResultSet executeQueryX() throws SqlException {
        flowExecute(executeQueryMethod__);
        return resultSet_;
    }


    public int executeUpdate() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeUpdate");
                }
                int updateValue = (int) executeUpdateX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeUpdate", updateValue);
                }
                return updateValue;
            }
        }
        catch ( SqlException se ) {
            checkStatementValidity(se);
            throw se.getSQLException();
        }
    }

    private long executeUpdateX() throws SqlException {
        flowExecute(executeUpdateMethod__);
        return updateCount_;
    }

    public void setNull(int parameterIndex, int jdbcType) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setNull", parameterIndex, jdbcType);
                }

                checkForClosedStatement();

                // JDBC 4.0 requires us to throw
                // SQLFeatureNotSupportedException for certain target types if
                // they are not supported. Check for these types before
                // checking type compatibility.
                agent_.checkForSupportedDataType(jdbcType);
                
                final int paramType = 
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.getPossibleTypesForNull( paramType ).checkType( jdbcType )){
                    
                    //This exception mimic embedded behavior.
                    //see http://issues.apache.org/jira/browse/DERBY-1610#action_12432568
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
                                                      jdbcType,
                                                      paramType );
                }
                
                setNullX(parameterIndex, jdbcType);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // also used by DBMD methods
    void setNullX(int parameterIndex, int jdbcType) throws SqlException {
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = jdbcType;

        if (!parameterMetaData_.nullable_[parameterIndex - 1]) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.LANG_NULL_INTO_NON_NULL),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                parameterIndex);
        }
        setInput(parameterIndex, null);
    }

    public void setNull(int parameterIndex, int jdbcType, String typeName) throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setNull", parameterIndex,
                                             jdbcType, typeName);
            }
            setNull(parameterIndex, jdbcType);
        }
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBoolean", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-1956

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ) {
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.BOOLEAN,
                                                      paramType);
                    
                }
                
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.BIT;
                setInput(parameterIndex, Boolean.valueOf(x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setByte", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType( paramType ) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.TINYINT,
                                                      paramType);
                    
                }
                
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.TINYINT;
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                setInput(parameterIndex, Short.valueOf(x));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setShort", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.SMALLINT,
                                                      paramType);
                                                  

                }
                
                setShortX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // also used by DBMD methods
    void setShortX(int parameterIndex, short x) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
            Types.SMALLINT;
        setInput(parameterIndex, x);

    }

    public void setInt(int parameterIndex, int x) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setInt", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
                                                      Types.INTEGER,
                                                      paramType);
                }
                
                setIntX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // also used by DBMD methods
    void setIntX(int parameterIndex, int x) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
            Types.INTEGER;
        setInput(parameterIndex, x);
    }


    public void setLong(int parameterIndex, long x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setLong", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.INTEGER,
                                                      paramType);
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-2495
                setLongX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    void setLongX(final int parameterIndex, final long x) 
    {
        // Column numbers starts at 1, clientParamtertype_[0] refers to column 1
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] 
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                = Types.BIGINT;
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
        setInput(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setFloat", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.FLOAT,
                                                      paramType);

                }
                
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.REAL;
                setInput(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setDouble", parameterIndex, x);
                }
                
                final int paramType = 
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.DOUBLE,
                                                      paramType);
                    
                }
                
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.DOUBLE;
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                Double d = x;
                setInput(parameterIndex, d);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBigDecimal", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
//IC see: https://issues.apache.org/jira/browse/DERBY-1956

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.checkType( paramType ) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.BIGINT,
                                                      paramType);
                    
                }

                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.DECIMAL;

                if (x == null) {
                    setNull(parameterIndex, Types.DECIMAL);
                    return;
                }
                int registerOutScale = 0;
                setInput(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setDate(int parameterIndex, Date x, Calendar calendar)
            throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(
                            this, "setDate", parameterIndex, x, calendar);
                }
                
                final int paramType = 
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_DATE.checkType(paramType) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_ ,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.DATE,
                                                      paramType);
                    
                }
                
                checkForClosedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-1234
//IC see: https://issues.apache.org/jira/browse/DERBY-1234
//IC see: https://issues.apache.org/jira/browse/DERBY-1234

                if (calendar == null) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                        "null", "calendar", "setDate()");
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.DATE;

                if (x == null) {
                    setNull(parameterIndex, Types.DATE);
                    return;
                }
                setInput(parameterIndex, new DateTimeValue(x, calendar));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, Calendar.getInstance());
    }

    public void setTime(int parameterIndex, Time x, Calendar calendar)
            throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setTime", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-1956

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_TIME.checkType( paramType ) ){
                    
                    PossibleTypes.throw22005Exception( agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                       Types.TIME,
                                                       paramType );
                }
                
                if (calendar == null) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                        "null", "calendar", "setTime()");
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.TIME;

                if (x == null) {
                    setNull(parameterIndex, Types.TIME);
                    return;
                }
                setInput(parameterIndex, new DateTimeValue(x, calendar));

            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, Calendar.getInstance());
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar calendar)
            throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setTimestamp", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-1956

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_TIMESTAMP.checkType( paramType ) ) {
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.TIMESTAMP,
                                                      paramType);
                    
                }
                
                if (calendar == null) {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                        "null", "calendar", "setTimestamp()");
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.TIMESTAMP;

                if (x == null) {
                    setNull(parameterIndex, Types.TIMESTAMP);
                    return;
                }
                setInput(parameterIndex, new DateTimeValue(x, calendar));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
        setTimestamp(parameterIndex, x, Calendar.getInstance());
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setString", parameterIndex, x);
                }
                
                final int paramType = 
                    getColumnMetaDataX().getColumnType(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-1956

                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_STRING.checkType( paramType ) ){
                    PossibleTypes.throw22005Exception(agent_.logWriter_ ,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.VARCHAR,
                                                      paramType);
                }
                
                setStringX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // also used by DBMD methods
    void setStringX(int parameterIndex, String x) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
            Types.LONGVARCHAR;

        if (x == null) {
            setNullX(parameterIndex, Types.LONGVARCHAR);
            return;
        }
        setInput(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBytes", parameterIndex, x);
                }
                
                final int paramType = 
//IC see: https://issues.apache.org/jira/browse/DERBY-1956
                    getColumnMetaDataX().getColumnType(parameterIndex);
                
                if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_BYTES.checkType( paramType ) ){
                    
                    PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                                      Types.VARBINARY,
                                                      paramType );
                }
                
                setBytesX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // also used by CallableLocatorProcedures
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void setBytesX(int parameterIndex, byte[] x) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
            Types.LONGVARBINARY;

        if (x == null) {
            setNullX(parameterIndex, Types.LONGVARBINARY);
            return;
        }
        setInput(parameterIndex, x);

    }
    
    /**
     * sets the parameter to the  Binary Stream object
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     */

    public void setBinaryStream(int parameterIndex,
                                InputStream x,
                                long length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                    agent_.logWriter_.traceEntry(this, "setBinaryStream",
                        parameterIndex, "<input stream>", Long.valueOf(length));
                }
                
                checkTypeForSetBinaryStream(parameterIndex);

//IC see: https://issues.apache.org/jira/browse/DERBY-3705
                checkStreamLength(length);
                setBinaryStreamX(parameterIndex, x, (int)length);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /**
     * sets the parameter to the  Binary Stream object
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     */

    public void setBinaryStream(int parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                InputStream x,
                                int length) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
        setBinaryStream(parameterIndex,x,(long)length);
    }

    private void setBinaryStreamX(int parameterIndex,
                                 InputStream x,
                                 int length) throws SqlException {
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = Types.BLOB;
        if (x == null) {
            setNullX(parameterIndex, Types.BLOB);
            return;
        }
        ClientBlob blob;
        if (length == -1) {
            // Create a blob of unknown length. This might cause an
            // OutOfMemoryError due to the temporary implementation in Blob.
            // The whole stream will be materialzied. See comments in Blob.
            blob = new ClientBlob(agent_, x);
        } else {
            blob = new ClientBlob(agent_, x, length);
        }
        setInput(parameterIndex, blob);
    }

    /**
     * We do this inefficiently and read it all in here. The target type
     * is assumed to be a String.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     */

    public void setAsciiStream(int parameterIndex,
                               InputStream x,
                               long length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setAsciiStream",
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        parameterIndex, "<input stream>", Long.valueOf(length));
                }
                
                checkTypeForSetAsciiStream(parameterIndex);

                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.CLOB;

                if (x == null) {
                    setNull(parameterIndex, Types.LONGVARCHAR);
                    return;
                }
                checkStreamLength(length);
                setInput(parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                    new ClientClob(agent_, x, Cursor.ISO_8859_1, (int) length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /**
     * We do this inefficiently and read it all in here. The target type
     * is assumed to be a String.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     */
    public void setAsciiStream(int parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                               InputStream x,
                               int length) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
        setAsciiStream(parameterIndex,x,(long)length);
    }
    
    /**
     * Check the length passed in for the stream that is to be set. If length is
     * larger than Integer.MAX_VALUE or smaller that 0, we fail by throwing an 
     * SQLException.
     * @param length The length of the stream being set
     * @throws SQLException Thrown for a negative or too large length.
     */
    private void checkStreamLength(long length) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3705
        if(length > Integer.MAX_VALUE) {
            throw new SqlException(
                        agent_.logWriter_,
                        new ClientMessageId(
                            SQLState.CLIENT_LENGTH_OUTSIDE_RANGE_FOR_DATATYPE),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        length,
                        Integer.MAX_VALUE
                    ).getSQLException();
        } else if (length < 0) {
            throw new SqlException(
                        agent_.logWriter_,
                        new ClientMessageId(SQLState.NEGATIVE_STREAM_LENGTH)
                    ).getSQLException();
        }
    }
    
    private void checkTypeForSetAsciiStream(int parameterIndex)
            throws SqlException, SQLException {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        if ( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_ASCIISTREAM.checkType( paramType ) ) {
            
            PossibleTypes.throw22005Exception(agent_.logWriter_,
                                              Types.LONGVARCHAR,
                                              paramType);
            
            
        }
    }
    
    private void checkTypeForSetBinaryStream(int parameterIndex)
            throws SqlException, SQLException {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        if (!PossibleTypes.POSSIBLE_TYPES_IN_SET_BINARYSTREAM.
                checkType(paramType)) {
            PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                              Types.VARBINARY,
                                              paramType);
        }
    }
    
    private void checkTypeForSetCharacterStream(int parameterIndex)
            throws SqlException, SQLException {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        if (!PossibleTypes.POSSIBLE_TYPES_IN_SET_CHARACTERSTREAM.
                checkType(paramType)) {
            PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                              Types.LONGVARCHAR,
                                              paramType);
        }
    }

    private void checkTypeForSetBlob(int parameterIndex)
            throws SqlException, SQLException {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_BLOB.checkType( paramType ) ){
            
            PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                              Types.BLOB,
                                              paramType);
        }
    }
    
    
    private void checkTypeForSetClob(int parameterIndex)
            throws SqlException, SQLException {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        if( ! PossibleTypes.POSSIBLE_TYPES_IN_SET_CLOB.checkType( paramType ) ){
                    
            PossibleTypes.throw22005Exception(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                              Types.CLOB,
                                              paramType);
                    
        }
        
    }
    
    
    /**
     * Sets the specified parameter to the given input stream. Deprecated
     * in JDBC 3.0 and this method will always just throw a feature not
     * implemented exception.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the UNICODE parameter
     * value
     * @param length the number of bytes in the stream
     * @exception SQLException throws feature not implemented.
     * @deprecated
     */
    public void setUnicodeStream(int parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                 InputStream x,
                                 int length) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-253
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceDeprecatedEntry(this, "setUnicodeStream",
                                                   parameterIndex,
                                                   "<input stream>", length);
        }

        throw SQLExceptionFactory.notImplemented ("setUnicodeStream");
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code> object.
     * When a very large UNICODE value is input to a LONGVARCHAR parameter, it
     * may be more practical to send it via a <code>java.io.Reader</code>
     * object. The data will be read from the stream as needed until
     * end-of-file is reached. The JDBC driver will do any necessary conversion
     * from UNICODE to the database char format.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the <code>java.io.Reader</code> object that contains the
     *      Unicode data
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public void setCharacterStream(int parameterIndex, Reader x)
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setCharacterStream",
                        parameterIndex, x);
            }
            try {
                checkTypeForSetCharacterStream(parameterIndex);
                parameterMetaData_.clientParamtertype_[parameterIndex -1] =
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                    Types.CLOB;
                if (x == null) {
                    setNull(parameterIndex, Types.LONGVARCHAR);
                    return;
                }
                setInput(parameterIndex, new ClientClob(agent_, x));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

     /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterIndex the index of the parameter to which this set
     *                       method is applied
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public void setCharacterStream(int parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                   Reader x,
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
                                   long length) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                    agent_.logWriter_.traceEntry(this, "setCharacterStream",
                            parameterIndex, x, Long.valueOf(length));
                }
                checkTypeForSetCharacterStream(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.CLOB;

                if (x == null) {
                    setNull(parameterIndex, Types.LONGVARCHAR);
                    return;
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-3705
//IC see: https://issues.apache.org/jira/browse/DERBY-3705
                checkStreamLength(length);
                setInput(parameterIndex,
                         new ClientClob(agent_, x, (int)length));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

     /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterIndex the index of the parameter to which this
     *                       set method is applied
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public void setCharacterStream(int parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                   Reader x,
                                   int length) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
        setCharacterStream(parameterIndex,x,(long)length);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setBlob", parameterIndex, x);
                }
                
                checkTypeForSetBlob(parameterIndex);
                setBlobX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void setBlobX(int parameterIndex, Blob x) throws SqlException {
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = Types.BLOB;
        if (x == null) {
            setNullX(parameterIndex, Types.BLOB);
            return;
        }
        setInput(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setClob", parameterIndex, x);
                }
                checkTypeForSetClob(parameterIndex);
                setClobX(parameterIndex, x);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void setClobX(int parameterIndex, Clob x) throws SqlException {
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = Types.CLOB;
        if (x == null) {
            this.setNullX(parameterIndex, ClientTypes.CLOB);
            return;
        }
        setInput(parameterIndex, x);
    }


    public void setArray(int parameterIndex, Array x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setArray", parameterIndex, x);
                }
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setRef", parameterIndex, x);
                }
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED));
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }            
    }

    // The Java compiler uses static binding, so we must use instanceof
    // rather than to rely on separate setObject() methods for
    // each of the Java Object instance types recognized below.
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setObject", parameterIndex, x);
                }

                int paramType = getColumnMetaDataX().getColumnType(parameterIndex);

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                if ( paramType == Types.JAVA_OBJECT )
                {
                    setUDTX( parameterIndex, x );
//IC see: https://issues.apache.org/jira/browse/DERBY-1938
                } else if (x == null) {
                    // DERBY-1938: Allow setting Java null also when the
                    //      column type isn't specified explicitly by the
                    //      user. Maps Java null to SQL NULL.
                    setNull(parameterIndex, paramType);
                } else if (x instanceof String) {
                    setString(parameterIndex, (String) x);
                } else if (x instanceof Integer) {
                    setInt(parameterIndex, ((Integer) x).intValue());
                } else if (x instanceof Double) {
                    setDouble(parameterIndex, ((Double) x).doubleValue());
                } else if (x instanceof Float) {
                    setFloat(parameterIndex, ((Float) x).floatValue());
                } else if (x instanceof Boolean) {
                    setBoolean(parameterIndex, ((Boolean) x).booleanValue());
                } else if (x instanceof Long) {
                    setLong(parameterIndex, ((Long) x).longValue());
                } else if (x instanceof byte[]) {
                    setBytes(parameterIndex, (byte[]) x);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                } else if (x instanceof BigDecimal) {
                    setBigDecimal(parameterIndex, (BigDecimal) x);
                } else if (x instanceof Date) {
                    setDate(parameterIndex, (Date) x);
                } else if (x instanceof Time) {
                    setTime(parameterIndex, (Time) x);
                } else if (x instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp) x);
                } else if (x instanceof Blob) {
                    setBlob(parameterIndex, (Blob) x);
                } else if (x instanceof Clob) {
                    setClob(parameterIndex, (Clob) x);
                } else if (x instanceof Array) {
                    setArray(parameterIndex, (Array) x);
                } else if (x instanceof Ref) {
                    setRef(parameterIndex, (Ref) x);
                } else if (x instanceof Short) {
                    setShort(parameterIndex, ((Short) x).shortValue());
                } else if (x instanceof BigInteger) {
                    setBigDecimal(parameterIndex,
                                  new BigDecimal((BigInteger)x));
                } else if (x instanceof java.util.Date) {
                    setTimestamp(parameterIndex,
                                 new Timestamp(((java.util.Date)x).getTime()));
                } else if (x instanceof Calendar) {
                    setTimestamp(
                        parameterIndex,
                        new Timestamp(((Calendar)x).getTime().getTime()));
                } else if (x instanceof Byte) {
                    setByte(parameterIndex, ((Byte) x).byteValue());
                } else {
                    checkForClosedStatement();
                    checkForValidParameterIndex(parameterIndex);
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.UNSUPPORTED_TYPE));
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }            
    }

    /**
     * Set a UDT parameter to an object value.
     */
    private void setUDTX(int parameterIndex, Object x) throws SqlException, SQLException
    {
        int paramType = getColumnMetaDataX().getColumnType(parameterIndex);
        int expectedType = Types.JAVA_OBJECT;
        
        if ( !( paramType == expectedType ) )
        {
            PossibleTypes.throw22005Exception
                (agent_.logWriter_, expectedType, paramType );
        }
        
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = expectedType;
        if (x == null) {
            setNullX(parameterIndex, expectedType );
            return;
        }

        //
        // Make sure that we are setting the parameter to an instance of the UDT.
        //
        
        Throwable problem = null;
        String sourceClassName = x.getClass().getName();
        String targetClassName = getColumnMetaDataX().getColumnClassName(parameterIndex);

        try {
            Class targetClass = Class.forName( targetClassName );
            if ( targetClass.isInstance( x ) )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                setInput(parameterIndex, x);
                return;
            }
        }
        catch (ClassNotFoundException e) { problem = e; }

        throw new SqlException
            (
             agent_.logWriter_,
             new ClientMessageId( SQLState.NET_UDT_COERCION_ERROR ),
             new Object[] { sourceClassName, targetClassName },
             problem
             );
    }

    public void setObject(int parameterIndex, Object x, int targetJdbcType) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setObject", parameterIndex, x, targetJdbcType);
                }
                checkForClosedStatement();
                setObjectX(parameterIndex, x, targetJdbcType, 0);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public void setObject(int parameterIndex,
                          Object x,
                          int targetJdbcType,
                          int scale) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "setObject", parameterIndex, x, targetJdbcType, scale);
                }
                checkForClosedStatement();
                setObjectX(parameterIndex, x, targetJdbcType, scale);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private void setObjectX(int parameterIndex,
                            Object x,
                            int targetJdbcType,
                            int scale) throws SqlException {
        checkForValidParameterIndex(parameterIndex);
        checkForValidScale(scale);

        // JDBC 4.0 requires us to throw SQLFeatureNotSupportedException for
        // certain target types if they are not supported.
        agent_.checkForSupportedDataType(targetJdbcType);

        if (x == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
            setNullX(parameterIndex, targetJdbcType);
            return;
        }

        // JDBC Spec specifies that conversion should occur on the client if
        // the targetJdbcType is specified.

        int inputParameterType = CrossConverters.getInputJdbcType(targetJdbcType);
//IC see: https://issues.apache.org/jira/browse/DERBY-250
        parameterMetaData_.clientParamtertype_[parameterIndex - 1] = inputParameterType;
        x = agent_.crossConverters_.setObject(inputParameterType, x);

        // Set to round down on setScale like embedded does in SQLDecimal
        try {
            if (targetJdbcType == Types.DECIMAL ||
                targetJdbcType == Types.NUMERIC) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                x = ((BigDecimal) x).setScale(scale, RoundingMode.DOWN);
            }
        } catch (ArithmeticException ae) {
            // Any problems with scale should have already been caught by
            // checkForvalidScale
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.JAVA_EXCEPTION),
                new Object[] {ae.getClass().getName(), ae.getMessage()}, ae);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try { 
            setObject(parameterIndex, x);
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
    }

    // Since parameters are cached as objects in parameters_[],
    // java null may be used to represent SQL null.
    public void clearParameters() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "clearParameters");
                }
                checkForClosedStatement();
                if (parameterMetaData_ != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
                    Arrays.fill(parameters_, null);
                    Arrays.fill(parameterSet_, false);
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public boolean execute() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "execute");
                }
                boolean b = executeX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "execute", b);
                }
                return b;
            }
        }
        catch ( SqlException se ) {
            checkStatementValidity(se);
            throw se.getSQLException();
        }
    }

    // also used by SQLCA
    boolean executeX() throws SqlException {
        flowExecute(executeMethod__);

        return resultSet_ != null;
    }

    //--------------------------JDBC 2.0-----------------------------

    public void addBatch() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "addBatch");
                }
                checkForClosedStatement();
                checkThatAllParametersAreSet();
                
                if (parameterTypeList == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                    parameterTypeList = new ArrayList<int[]>();
                }

                // ASSERT: since OUT/INOUT parameters are not allowed, there should
                //         be no problem in sharing the JDBC Wrapper object instances
                //         since they will not be modified by the driver.

                // batch up the parameter values -- deep copy req'd

                if (parameterMetaData_ != null) {
                    Object[] inputsClone = new Object[parameters_.length];
                    System.arraycopy(parameters_, 0, inputsClone, 0, parameters_.length);

                    batch_.add(inputsClone);
                    
                    // Get a copy of the parameter type data and save it in a list
                    // which will be used later on at the time of batch execution.
//IC see: https://issues.apache.org/jira/browse/DERBY-1292
                    parameterTypeList.add(parameterMetaData_.clientParamtertype_.clone());
                } else {
                    batch_.add(null);
                    parameterTypeList.add(null);
                }
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Batch requires that input types are exact, we perform no input cross conversion for Batch.
    // If so, this is an external semantic, and should go into the release notes
    public int[] executeBatch() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeBatch");
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
                long[] updateCounts = null;
                updateCounts = executeBatchX(false);

                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeBatch", updateCounts);
                }
                return Utils.squashLongs( updateCounts );
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
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getMetaData");
                }
                ColumnMetaData resultSetMetaData = getMetaDataX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getMetaData", resultSetMetaData);
                }
                return resultSetMetaData;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ColumnMetaData getMetaDataX() throws SqlException {
        super.checkForClosedStatement();
        return resultSetMetaData_;
    }

    //------------------------- JDBC 3.0 -----------------------------------

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "execute", sql, autoGeneratedKeys);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "execute(String, int)").getSQLException();
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "execute", sql, columnNames);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "execute(String, String[])").getSQLException();
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "execute", sql, columnIndexes);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "execute(String, int[])").getSQLException();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "executeUpdate", autoGeneratedKeys);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "executeUpdate(String, int)").getSQLException();
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "executeUpdate", columnNames);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "executeUpdate(String, String[])").getSQLException();
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "executeUpdate", columnIndexes);
        }
        throw new SqlException(agent_.logWriter_,
            new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),
            "execute(String, int[])").getSQLException();
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setURL", parameterIndex, x);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED)).
                getSQLException();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getParameterMetaData");
                }
                Object parameterMetaData = getParameterMetaDataX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getParameterMetaData", parameterMetaData);
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                return (ParameterMetaData) parameterMetaData;
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ClientParameterMetaData getParameterMetaDataX()
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SqlException {
        ClientParameterMetaData pm =
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            ClientAutoloadedDriver.getFactory().
            newParameterMetaData(getColumnMetaDataX());
        return pm;
    }

    private ColumnMetaData getColumnMetaDataX() throws SqlException {
        checkForClosedStatement();
        return 
            parameterMetaData_ != null ?
            parameterMetaData_ : 
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            ClientAutoloadedDriver.getFactory().newColumnMetaData(agent_.logWriter_, 0);
    }

    // ------------------------ box car and callback methods --------------------------------

    private void writeExecute(Section section,
                             ColumnMetaData parameterMetaData,
                             Object[] inputs,
                             int numInputColumns,
                             boolean outputExpected,
                             // This is a hint to the material layer that more write commands will follow.
                             // It is ignored by the driver in all cases except when blob data is written,
                             // in which case this boolean is used to optimize the implementation.
                             // Otherwise we wouldn't be able to chain after blob data is sent.
                             // Current servers have a restriction that blobs can only be chained with blobs
                             boolean chainedWritesFollowingSetLob) throws SqlException {
        materialPreparedStatement_.writeExecute_(section,
                parameterMetaData,
                inputs,
                numInputColumns,
                outputExpected,
                chainedWritesFollowingSetLob);
    }


    private void readExecute() throws SqlException {
        materialPreparedStatement_.readExecute_();
    }

    private void writeOpenQuery(Section section,
                               int fetchSize,
                               int resultSetType,
                               int numInputColumns,
                               ColumnMetaData parameterMetaData,
                               Object[] inputs) throws SqlException {
        materialPreparedStatement_.writeOpenQuery_(section,
                fetchSize,
                resultSetType,
                numInputColumns,
                parameterMetaData,
                inputs);
    }

    private void writeDescribeInput(Section section) throws SqlException {
        materialPreparedStatement_.writeDescribeInput_(section);
    }

    private void readDescribeInput() throws SqlException {
        materialPreparedStatement_.readDescribeInput_();
    }

    public void completeDescribeInput(ColumnMetaData parameterMetaData, Sqlca sqlca) {
        int sqlcode = super.completeSqlca(sqlca);
        if (sqlcode < 0) {
            return;
        }


        parameterMetaData_ = parameterMetaData;

        // The following code handles the case when
        // sqlxParmmode is not supported, in which case server will return 0 (unknown), and
        // this could clobber our guessed value for sqlxParmmode.  This is a problem.
        // We can solve this problem for Non-CALL statements, since the parmmode is always IN (1).
        // But what about CALL statements.  If CALLs are describable, then we have no
        // problem, we assume server won't return unknown.
        // If CALLs are not describable then nothing gets clobbered because we won't
        // parse out extended describe, so again  no problem.
        if (sqlMode_ != isCall__ && parameterMetaData_ != null) {
            // 1 means IN parameter
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
            Arrays.fill(parameterMetaData_.sqlxParmmode_, (short)1);
        }

        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceParameterMetaData(this, parameterMetaData_);
        }
    }

    public void completeDescribeOutput(ColumnMetaData resultSetMetaData, Sqlca sqlca) {
        int sqlcode = super.completeSqlca(sqlca);
        if (sqlcode < 0) {
            return;
        }
        resultSetMetaData_ = resultSetMetaData;
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceResultSetMetaData(this, resultSetMetaData);
        }
    }

    private void writePrepareDescribeInputOutput() throws SqlException {
        // Notice that sql_ is passed in since in general ad hoc sql must be passed in for unprepared statements
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
        writePrepareDescribeOutput(sql_, getSection());
        writeDescribeInput(getSection());
    }

    private void readPrepareDescribeInputOutput() throws SqlException {
        readPrepareDescribeOutput();
        readDescribeInput();
        completePrepareDescribe();
    }

    private void writePrepareDescribeInput() throws SqlException {
        // performance will be better if we flow prepare with output enable vs. prepare then describe input for callable
        // Notice that sql_ is passed in since in general ad hoc sql must be passed in for unprepared statements
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
        writePrepare(sql_, getSection());
        writeDescribeInput(getSection());
    }

    private void readPrepareDescribeInput() throws SqlException {
        readPrepare();
        readDescribeInput();
        completePrepareDescribe();
    }

    private void completePrepareDescribe() {
        if (parameterMetaData_ == null) {
            return;
        }
        parameters_ = expandObjectArray(parameters_, parameterMetaData_.columns_);
        parameterSet_ = expandBooleanArray(parameterSet_, parameterMetaData_.columns_);
        parameterRegistered_ = expandBooleanArray(parameterRegistered_, parameterMetaData_.columns_);
    }

    private Object[] expandObjectArray(Object[] array, int newLength) {
        if (array == null) {
            Object[] newArray = new Object[newLength];
            return newArray;
        }
        if (array.length < newLength) {
            Object[] newArray = new Object[newLength];
            System.arraycopy(array, 0, newArray, 0, array.length);
            return newArray;
        }
        return array;
    }

    private boolean[] expandBooleanArray(boolean[] array, int newLength) {
        if (array == null) {
            boolean[] newArray = new boolean[newLength];
            return newArray;
        }
        if (array.length < newLength) {
            boolean[] newArray = new boolean[newLength];
            System.arraycopy(array, 0, newArray, 0, array.length);
            return newArray;
        }
        return array;
    }

    void flowPrepareDescribeInputOutput() throws SqlException {
        agent_.beginWriteChain(this);
        if (sqlMode_ == isCall__) {
            writePrepareDescribeInput();
            agent_.flow(this);
            readPrepareDescribeInput();
            agent_.endReadChain();
        } else {
            writePrepareDescribeInputOutput();
            agent_.flow(this);
            readPrepareDescribeInputOutput();
            agent_.endReadChain();
        }
    }

    private void flowExecute(int executeType) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1234
        checkForClosedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-2653
        checkAutoGeneratedKeysParameters();
        clearWarningsX();
        checkForAppropriateSqlMode(executeType, sqlMode_);
        checkThatAllParametersAreSet();

        if (sqlMode_ == isUpdate__) {
            updateCount_ = 0;
        } else {
            updateCount_ = -1;
        }

        // DERBY-1036: Moved check till execute time to comply with embedded
        // behavior. Since we check here and not in setCursorName, several
        // statements can have the same cursor name as long as their result
        // sets are not simultaneously open.

//IC see: https://issues.apache.org/jira/browse/DERBY-1036
//IC see: https://issues.apache.org/jira/browse/DERBY-1183
//IC see: https://issues.apache.org/jira/browse/DERBY-210
        if (sqlMode_ == isQuery__) {
            checkForDuplicateCursorName();
        }

            agent_.beginWriteChain(this);

            boolean piggybackedAutocommit = writeCloseResultSets(true);  // true means permit auto-commits

            int numInputColumns;
            boolean outputExpected;
//IC see: https://issues.apache.org/jira/browse/DERBY-852
            try
            {
                numInputColumns = (parameterMetaData_ != null) ? parameterMetaData_.getColumnCount() : 0;
                outputExpected = (resultSetMetaData_ != null && resultSetMetaData_.getColumnCount() > 0);
            }
            catch ( SQLException se )
            {
                // Generate a SqlException for this, we don't want to throw
                // SQLException in this internal method
//IC see: https://issues.apache.org/jira/browse/DERBY-842
                throw new SqlException(se);
            }
            boolean chainAutoCommit = false;
            boolean commitSubstituted = false;
            boolean repositionedCursor = false;
            boolean timeoutSent = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientResultSet scrollableRS = null;
            boolean prepareSentForAutoGeneratedKeys = false;

//IC see: https://issues.apache.org/jira/browse/DERBY-506
            if (doWriteTimeout) {
                timeoutArrayList.set(0, TIMEOUT_STATEMENT + timeout_);
                writeSetSpecialRegister(timeoutArrayList);
                doWriteTimeout = false;
                timeoutSent = true;
            }
            switch (sqlMode_) {
            case isUpdate__:
                if (positionedUpdateCursorName_ != null) {
                    scrollableRS = agent_.sectionManager_.getPositionedUpdateResultSet(positionedUpdateCursorName_);
                }
                if (scrollableRS != null && !scrollableRS.isRowsetCursor_) {
                    repositionedCursor =
                            scrollableRS.repositionScrollableResultSetBeforeJDBC1PositionedUpdateDelete();
                    if (!repositionedCursor) {
                        scrollableRS = null;
                    }
                }

                chainAutoCommit = connection_.willAutoCommitGenerateFlow() && isAutoCommittableStatement_;

                boolean chainOpenQueryForAutoGeneratedKeys = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6753
                		((sqlUpdateMode_ == isInsertSql__ || sqlUpdateMode_ == isUpdateSql__) 
                				&& autoGeneratedKeys_ == RETURN_GENERATED_KEYS);
                writeExecute(getSection(),
                        parameterMetaData_,
                        parameters_,
                        numInputColumns,
                        outputExpected,
                        (chainAutoCommit || chainOpenQueryForAutoGeneratedKeys)// chain flag
                ); // chain flag

                if (chainOpenQueryForAutoGeneratedKeys) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                    if (preparedStatementForAutoGeneratedKeys_ == null) {
                        preparedStatementForAutoGeneratedKeys_ =
                                prepareAutoGeneratedKeysStatement(connection_);
                        prepareSentForAutoGeneratedKeys = true;
                    }
                  
                    writeOpenQuery(preparedStatementForAutoGeneratedKeys_.getSection(),
                            preparedStatementForAutoGeneratedKeys_.fetchSize_,
                            preparedStatementForAutoGeneratedKeys_.resultSetType_);
                }
                

                if (chainAutoCommit) {
                    // we have encountered an error in writing the execute, so do not
                    // flow an autocommit
                    if (agent_.accumulatedReadExceptions_ != null) {
                        // currently, the only write exception we encounter is for
                        // data truncation: SQLSTATE 01004, so we don't bother checking for this
                        connection_.writeCommitSubstitute_();
                        commitSubstituted = true;
                    } else {
                        // there is no write error, so flow the commit
                        connection_.writeCommit();
                    }
                }
                break;

            case isQuery__:
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                writeOpenQuery(getSection(),
                        fetchSize_,
                        resultSetType_,
                        numInputColumns,
                        parameterMetaData_,
                        parameters_);
                break;

            case isCall__:
                writeExecuteCall(outputRegistered_, // if no out/inout parameter, outputExpected = false
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                        null, getSection(),
                        fetchSize_,
                        false, // do not suppress ResultSets for regular CALLs
                        resultSetType_,
                        parameterMetaData_,
                        parameters_); // cross conversion
                break;
            }

            agent_.flow(this);

            super.readCloseResultSets(true);  // true means permit auto-commits

            // turn inUnitOfWork_ flag back on and add statement
            // back on commitListeners_ list if they were off
            // by an autocommit chained to a close cursor.
            if (piggybackedAutocommit) {
                connection_.completeTransactionStart();
            }

            markResultSetsClosed(true); // true means remove from list of commit and rollback listeners

//IC see: https://issues.apache.org/jira/browse/DERBY-506
            if (timeoutSent) {
                readSetSpecialRegister(); // Read response to the EXCSQLSET
            }

            switch (sqlMode_) {
            case isUpdate__:
                // do not need to reposition for a rowset cursor
                if (scrollableRS != null && !scrollableRS.isRowsetCursor_) {
                    scrollableRS.readPositioningFetch_();
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-3426
                else {
                    readExecute();

//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6753
            		if ((sqlUpdateMode_ == isInsertSql__ || sqlUpdateMode_ == isUpdateSql__) 
            				&& autoGeneratedKeys_ == RETURN_GENERATED_KEYS) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                        if (prepareSentForAutoGeneratedKeys) {
                            preparedStatementForAutoGeneratedKeys_.materialPreparedStatement_.readPrepareDescribeOutput_();
                        }
          
                        preparedStatementForAutoGeneratedKeys_.readOpenQuery();
                        generatedKeysResultSet_ = preparedStatementForAutoGeneratedKeys_.resultSet_;
                        preparedStatementForAutoGeneratedKeys_.resultSet_ = null;
                    }
                }

                if (chainAutoCommit) {
                    if (commitSubstituted) {
                        connection_.readCommitSubstitute_();
                    } else {
                        connection_.readCommit();
                    }
                }
                break;

            case isQuery__:
                try {
                    readOpenQuery();
                } catch (DisconnectException dise) {
                    throw dise;
                } catch (SqlException e) {
                    throw e;
                }
                // resultSet_ is null if open query failed.
                // check for null resultSet_ before using it.
                if (resultSet_ != null) {
                    resultSet_.parseScrollableRowset();
                    //if (resultSet_.scrollable_) resultSet_.getRowCount();

                    // DERBY-1183: If we set it up earlier, the entry in
                    // clientCursorNameCache_ gets wiped out by the closing of
                    // result sets happening during readCloseResultSets above
                    // because ResultSet#markClosed calls
                    // Statement#removeClientCursorNameFromCache.
//IC see: https://issues.apache.org/jira/browse/DERBY-1036
//IC see: https://issues.apache.org/jira/browse/DERBY-1183
//IC see: https://issues.apache.org/jira/browse/DERBY-210
                    setupCursorNameCacheAndMappings();
                }
                break;

            case isCall__:
                readExecuteCall();
                break;

            }


            try {
                agent_.endReadChain();
            } catch (SqlException e) {
                throw e;

            }

            if (sqlMode_ == isCall__) {
                parseStorProcReturnedScrollableRowset();
//IC see: https://issues.apache.org/jira/browse/DERBY-1364
                checkForStoredProcResultSetCount(executeType);
                // When there are no result sets back, we will commit immediately when autocommit is true.
                // make sure a commit is not performed when making the call to the sqlca message procedure
                if (connection_.autoCommit_ && resultSet_ == null && resultSetList_ == null && isAutoCommittableStatement_) {
                    connection_.flowAutoCommit();
                }
            }

            // The JDBC spec says that executeUpdate() should return 0
            // when no row count is returned.
//IC see: https://issues.apache.org/jira/browse/DERBY-1314
            if (executeType == executeUpdateMethod__ && updateCount_ < 0) {
                updateCount_ = 0;
            }

            // Throw an exception if holdability returned by the server is different from requested.
            if (resultSet_ != null && resultSet_.resultSetHoldability_ != resultSetHoldability_ && sqlMode_ != isCall__) {
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.UNABLE_TO_OPEN_RESULTSET_WITH_REQUESTED_HOLDABILTY),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        resultSetHoldability_);
            }
    }

    private long[] executeBatchX(boolean supportsQueryBatchRequest)
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        throws SqlException, SQLException {
        synchronized (connection_) {
            checkForClosedStatement(); // Per jdbc spec (see Statement.close() javadoc)
            clearWarningsX(); // Per jdbc spec 0.7, also see getWarnings() javadoc
            return executeBatchRequestX(supportsQueryBatchRequest);
        }
    }


    private long[] executeBatchRequestX(boolean supportsQueryBatchRequest)
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SqlException, BatchUpdateException {
        SqlException chainBreaker = null;
        int batchSize = batch_.size();
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        long[] updateCounts = new long[batchSize];
        int numInputColumns;
//IC see: https://issues.apache.org/jira/browse/DERBY-852
        try {
            numInputColumns = parameterMetaData_ == null ? 0 : parameterMetaData_.getColumnCount();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
        Object[] savedInputs = null;  // used to save/restore existing parameters
        boolean timeoutSent = false;

        if (batchSize == 0) {
            return updateCounts;
        }
        // The network client has a hard limit of 65,534 commands in a single
        // DRDA request. This is because DRDA uses a 2-byte correlation ID,
        // and the values 0 and 0xffff are reserved as special values. So
        // that imposes an upper limit on the batch size we can support:
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (batchSize > 65534)
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            throw ClientAutoloadedDriver.getFactory().newBatchUpdateException(agent_.logWriter_, 
                new ClientMessageId(SQLState.TOO_MANY_COMMANDS_FOR_BATCH), 
                new Object[] { 65534 }, updateCounts, null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

        // Initialize all the updateCounts to indicate failure
        // This is done to account for "chain-breaking" errors where we cannot
        // read any more replies
        for (int i = 0; i < batchSize; i++) {
            updateCounts[i] = -3;
        }

        if (!supportsQueryBatchRequest && sqlMode_ == isQuery__) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            throw ClientAutoloadedDriver.getFactory().newBatchUpdateException(agent_.logWriter_, 
            new ClientMessageId(SQLState.CANNOT_BATCH_QUERIES), (Object [])null, updateCounts, null);
        }
        if (supportsQueryBatchRequest && sqlMode_ != isQuery__) {
            throw ClientAutoloadedDriver.getFactory().newBatchUpdateException(agent_.logWriter_, 
                new ClientMessageId(SQLState.QUERY_BATCH_ON_NON_QUERY_STATEMENT), 
                (Object [])null, updateCounts, null);
        }

        resultSetList_ = null;


        if (sqlMode_ == isQuery__) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            resetResultSetList();
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            resultSetList_ = new ClientResultSet[batchSize];
        }


        //save the current input set so it can be restored
        savedInputs = parameters_;

        agent_.beginBatchedWriteChain(this);
        boolean chainAutoCommit = connection_.willAutoCommitGenerateFlow() && isAutoCommittableStatement_;

//IC see: https://issues.apache.org/jira/browse/DERBY-506
        if (doWriteTimeout) {
            timeoutArrayList.set(0, TIMEOUT_STATEMENT + timeout_);
            writeSetSpecialRegister(timeoutArrayList);
            doWriteTimeout = false;
            timeoutSent = true;
        }

        for (int i = 0; i < batchSize; i++) {
            if (parameterMetaData_ != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                parameterMetaData_.clientParamtertype_ = parameterTypeList.get(i);
                parameters_ = (Object[]) batch_.get(i);
            }
            
            if (sqlMode_ != isCall__) {
                boolean outputExpected;
//IC see: https://issues.apache.org/jira/browse/DERBY-852
                try {
                    outputExpected = (resultSetMetaData_ != null && resultSetMetaData_.getColumnCount() > 0);
                } catch ( SQLException se ) {
                    throw new SqlException(se);
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6082
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                writeExecute(getSection(),
                        parameterMetaData_,
                        parameters_,
                        numInputColumns,
                        outputExpected,
                        chainAutoCommit || (i != batchSize - 1));  // more statements to chain
            } else if (outputRegistered_) // make sure no output parameters are registered
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                throw ClientAutoloadedDriver.getFactory().newBatchUpdateException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.OUTPUT_PARAMS_NOT_ALLOWED),
                    (Object [])null, updateCounts, null );
            } else {
                writeExecuteCall(false, // no output expected for batched CALLs
//IC see: https://issues.apache.org/jira/browse/DERBY-6082
                        null, getSection(),
                        fetchSize_,
                        true, // suppress ResultSets for batch
                        resultSetType_,
                        parameterMetaData_,
                        parameters_);
            }
        }

        boolean commitSubstituted = false;
        if (chainAutoCommit) {
            // we have encountered an error in writing the execute, so do not
            // flow an autocommit
            if (agent_.accumulatedReadExceptions_ != null) {
                // currently, the only write exception we encounter is for
                // data truncation: SQLSTATE 01004, so we don't bother checking for this
                connection_.writeCommitSubstitute_();
                commitSubstituted = true;
            } else {
                // there is no write error, so flow the commit
                connection_.writeCommit();
            }
        }

        agent_.flowBatch(this, batchSize);

//IC see: https://issues.apache.org/jira/browse/DERBY-506
        if (timeoutSent) {
            readSetSpecialRegister(); // Read response to the EXCSQLSET
        }

        try {
            for (int i = 0; i < batchSize; i++) {
                agent_.setBatchedExceptionLabelIndex(i);
                parameters_ = (Object[]) batch_.get(i);
                if (sqlMode_ != isCall__) {
                    readExecute();
                } else {
                    readExecuteCall();
                }
                updateCounts[i] = updateCount_;

            }

            agent_.disableBatchedExceptionTracking(); // to prvent the following readCommit() from getting a batch label
            if (chainAutoCommit) {
                if (!commitSubstituted) {
                    connection_.readCommit();
                } else {
                    connection_.readCommitSubstitute_();
                }
            }
        }

                // for chain-breaking exception only, all read() methods do their own accumulation
                // this catches the entire accumulated chain, we need to be careful not to
                // reaccumulate it on the agent since the batch labels will be overwritten if
                // batch exception tracking is enabled.
        catch (SqlException e) { // for chain-breaking exception only
            chainBreaker = e;
            chainBreaker.setNextException(new SqlException(agent_.logWriter_,
//IC see: https://issues.apache.org/jira/browse/DERBY-1350
                new ClientMessageId(SQLState.BATCH_CHAIN_BREAKING_EXCEPTION)));
        }
        // We need to clear the batch before any exception is thrown from agent_.endBatchedReadChain().
        batch_.clear();
        parameterTypeList = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-1292

        // restore the saved input set, setting it to "current"
        parameters_ = savedInputs;

        agent_.endBatchedReadChain(updateCounts, chainBreaker);

        return updateCounts;

    }


    //------------------material layer event callbacks follow-----------------------

    private boolean listenToUnitOfWork_ = false;

    public void listenToUnitOfWork() {
        if (!listenToUnitOfWork_) {
            listenToUnitOfWork_ = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-210
//IC see: https://issues.apache.org/jira/browse/DERBY-557
//IC see: https://issues.apache.org/jira/browse/DERBY-210
            connection_.CommitAndRollbackListeners_.put(this,null);
        }
    }

    @Override
    public void completeLocalCommit(Iterator listenerIterator) {
        listenerIterator.remove();
        listenToUnitOfWork_ = false;
    }

    @Override
    public void completeLocalRollback(Iterator listenerIterator) {
        listenerIterator.remove();
        listenToUnitOfWork_ = false;
    }

    //----------------------------internal use only helper methods----------------

    /**
     * Returns the name of the java.sql interface implemented by this class.
     * @return name of java.sql interface
     */
    protected String getJdbcStatementInterfaceName() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1364
        return "java.sql.PreparedStatement";
    }

    void checkForValidParameterIndex(int parameterIndex) throws SqlException {
        if (parameterMetaData_ == null) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.NO_INPUT_PARAMETERS));

        if (parameterIndex < 1 || parameterIndex > parameterMetaData_.columns_) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.LANG_INVALID_PARAM_POSITION),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                parameterIndex, parameterMetaData_.columns_);
        }
    }

    private void checkThatAllParametersAreSet() throws SqlException {
        if (parameterMetaData_ != null) {
            for (int i = 0; i < parameterMetaData_.columns_; i++) {
                // Raise an exception if at least one of the parameters isn't
                // set. It is OK that a parameter isn't set if it is registered
                // as an output parameter. However, if it's an INOUT parameter,
                // it must be set even if it has been registered (DERBY-2516).
                if (!parameterSet_[i] &&
                        (!parameterRegistered_[i] ||
                         parameterMetaData_.sqlxParmmode_[i] ==
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            ClientParameterMetaData.parameterModeInOut)) {
                    throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId(SQLState.LANG_MISSING_PARMS));
                }
            }
        }
    }

    void checkForValidScale(int scale) throws SqlException {
        if (scale < 0 || scale > 31) {
            throw new SqlException(agent_.logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                new ClientMessageId(SQLState.BAD_SCALE_VALUE), scale);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.derby.client.am.Statement#markClosed(boolean)
     */
    protected void markClosed(boolean removeListener){
//IC see: https://issues.apache.org/jira/browse/DERBY-941
        if(pooledConnection_ != null)
            pooledConnection_.onStatementClose(this);
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        super.markClosed(removeListener);
        
        if (parameterMetaData_ != null) {
            parameterMetaData_.markClosed();
            parameterMetaData_ = null;
        }
        sql_ = null;

        // Apparently, the JVM is not smart enough to traverse parameters_[] and null
        // out its members when the entire array is set to null (parameters_=null;).
        if (parameters_ != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3441
            Arrays.fill(parameters_, null);
        }
        parameters_ = null;

        if(removeListener)
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            connection_.CommitAndRollbackListeners_.remove(this);
    }
    
    // JDBC 4.0 methods

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream as
     * needed until end-of-file is reached. The JDBC driver will do any
     * necessary conversion from ASCII to the database char format.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the Java input stream that contains the ASCII parameter value
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public void setAsciiStream(int parameterIndex, InputStream x)
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setAsciiStream",
                        parameterIndex, x);
            }
            try {
                checkTypeForSetAsciiStream(parameterIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                parameterMetaData_.clientParamtertype_[parameterIndex - 1] =
                    Types.CLOB;

                if (x == null) {
                    setNull(parameterIndex, Types.LONGVARCHAR);
                    return;
                }
                setInput(parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                         new ClientClob(agent_, x, Cursor.ISO_8859_1));
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @throws SQLException if a database access error occurs or this method is
     *      called on a closed <code>PreparedStatement</code>
     */
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setBinaryStream",
                        parameterIndex, x);
            }
            try {
                checkTypeForSetBinaryStream(parameterIndex);
                setBinaryStreamX(parameterIndex, x, -1);
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 
     *      2, ...
     * @param reader an object that contains the data to set the parameter
     *      value to. 
     * @throws SQLException if parameterIndex does not correspond to a 
     *      parameter marker in the SQL statement; if a database access error
     *      occurs; this method is called on a closed PreparedStatementor if
     *      parameterIndex does not correspond to a parameter marker in the SQL
     *      statement
     */
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setClob",
                        parameterIndex, reader);
            }
            
            try {
                checkTypeForSetClob(parameterIndex);
                checkForClosedStatement();
            } catch (SqlException se) {
                throw se.getSQLException();
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            setInput(parameterIndex, new ClientClob(agent_, reader));
        }
    }

   /**
     * Sets the designated parameter to a Reader object.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterIndex does not correspond to a parameter
     * marker in the SQL statement, or if the length specified is less than zero.
     *
     */
    
    public void setClob(int parameterIndex, Reader reader, long length)
    throws SQLException{
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setClob",
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        parameterIndex, reader, Long.valueOf(length));
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-1234
            try {
                checkForClosedStatement();
            } catch (SqlException se) {
                throw se.getSQLException();
            }
            if(length > Integer.MAX_VALUE)
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.BLOB_TOO_LARGE_FOR_CLIENT),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                    length, Integer.MAX_VALUE).getSQLException();
            else
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                setInput(parameterIndex,
                         new ClientClob(agent_, reader, (int)length));
        }
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream(int, InputStream)
     * </code>  method because it informs the driver that the parameter value
     * should be sent to the server as a <code>BLOB</code>. When the
     * <code>setBinaryStream</code> method is used, the driver may have to do
     * extra work to determine whether the parameter data should be sent to the
     * server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is
     *      2, ...
     * @param inputStream an object that contains the data to set the parameter
     *      value to.
     * @throws SQLException if a database access error occurs, this method is
     *      called on a closed <code>PreparedStatement</code> or if
     *      <code>parameterIndex</code> does not correspond to a parameter
     *      marker in the SQL statement
     */
    public void setBlob(int parameterIndex, InputStream inputStream)
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
            throws SQLException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setBlob", parameterIndex,
                        inputStream);
            }

            try {
                checkTypeForSetBlob(parameterIndex);
                setBinaryStreamX(parameterIndex, inputStream, -1);
            } catch (SqlException se) {
                throw se.getSQLException();
            }
        }
    }

    /**
     * Sets the designated parameter to a InputStream object.
     *
     * @param parameterIndex index of the first parameter is 1,
     * the second is 2, ...
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @throws SQLException if parameterIndex does not correspond
     * to a parameter marker in the SQL statement,  if the length specified
     * is less than zero or if the number of bytes in the inputstream does not match
     * the specfied length.
     *
     */
    
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
    throws SQLException{
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "setBlob", parameterIndex,
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        inputStream, Long.valueOf(length));
            }
            if(length > Integer.MAX_VALUE)
                throw new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.BLOB_TOO_LARGE_FOR_CLIENT),
                    length, Integer.MAX_VALUE).getSQLException();
            else {
                try {
                    checkTypeForSetBlob(parameterIndex);
                    setBinaryStreamX(parameterIndex, inputStream, (int)length);
                } catch(SqlException se){
                    throw se.getSQLException();
                }
            }
        }
    }    
 
    public void setNString(int index, String value) throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNString(int, String)");
    }

    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setNCharacterStream(int, Reader)");
    }

    public void setNCharacterStream(int index, Reader value, long length)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setNCharacterStream(int, Reader, long)");
    }

    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob(int, Reader)");
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob(int, Reader, long)");
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        throw SQLExceptionFactory.notImplemented("setRowId (int, RowId)");
    }

    public void setNClob(int index, NClob value) throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob (int, NClob)");
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setSQLXML (int, SQLXML)");
    }

    // End of JDBC 4.0 methods

    // Beginning of JDBC 4.2 methods

    public long executeLargeUpdate() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "executeLargeUpdate");
                }
                long updateValue = executeUpdateX();
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "executeLargeUpdate", updateValue);
                }
                return updateValue;
            }
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-941
//IC see: https://issues.apache.org/jira/browse/DERBY-941
//IC see: https://issues.apache.org/jira/browse/DERBY-941
        catch ( SqlException se ) {
            checkStatementValidity(se);
            throw se.getSQLException();
        }
    }

    // End of JDBC 4.2 methods

        /*
         * Method calls onStatementError occurred on the 
         * BrokeredConnectionControl class after checking the 
         * SQLState of the SQLException thrown.
         * @param sqle SqlException
         * @throws java.sql.SQLException
         */
        
        private void checkStatementValidity(SqlException sqle)  
//IC see: https://issues.apache.org/jira/browse/DERBY-941
                                            throws SQLException {
            //check if the statement is already closed 
            //This might be caused because the connection associated
            //with this prepared statement has been closed marking 
            //its associated prepared statements also as
            //closed
            
            if(pooledConnection_!=null && isClosed()){
                pooledConnection_.onStatementErrorOccurred(this,
                    sqle.getSQLException());
            }
        }
    
    /**
     * PossibleTypes is information which is set of types.
     * A given type is evaluated as *possible* at checkType method if same type was found in the set.
     */
    private static class PossibleTypes{
        
        final private int[] possibleTypes;
        
        private PossibleTypes(int[] types){
            possibleTypes = types;
            Arrays.sort(possibleTypes);
        }
        
        /**
         * This is possibleTypes of variable which can be set by set method for generic scalar.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR = 
            new PossibleTypes( new int[] { 
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                Types.BIGINT,
                Types.LONGVARCHAR ,
                Types.CHAR,
                Types.DECIMAL,
                Types.INTEGER,
                Types.SMALLINT,
                Types.REAL,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.BOOLEAN } );
        
        /**
         * This is possibleTypes of variable which can be set by setDate method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_DATE = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.DATE,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of variable which can be set by setTime method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_TIME = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.TIME } );
        
        /**
         * This is possibleTypes of variable which can be set by setTimestamp method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_TIMESTAMP = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of variable which can be set by setString method.
         */
        final private static PossibleTypes POSSIBLE_TYPES_IN_SET_STRING = 
            new PossibleTypes( new int[] { 
                Types.BIGINT,
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.DECIMAL,
                Types.INTEGER,
                Types.SMALLINT,
                Types.REAL,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.BOOLEAN,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP,
                Types.CLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setBytes method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_BYTES = 
            new PossibleTypes( new int[] { 
                Types.LONGVARBINARY,
                Types.VARBINARY,
                Types.BINARY,
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.BLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setBinaryStream method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_BINARYSTREAM = 
            new PossibleTypes( new int[] { 
                Types.LONGVARBINARY,
                Types.VARBINARY,
                Types.BINARY,
                Types.BLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setAsciiStream method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_ASCIISTREAM = 
            new PossibleTypes( new int[]{ 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.CLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setCharacterStream method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_CHARACTERSTREAM = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.CLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setBlob method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_BLOB = 
            new PossibleTypes( new int[] { 
                Types.BLOB } );
        
        /**
         * This is possibleTypes of variable which can be set by setClob method.
         */
        final public static PossibleTypes POSSIBLE_TYPES_IN_SET_CLOB = 
            new PossibleTypes( new int[] { 
                Types.CLOB } );
        
        /**
         * This is possibleTypes of null value which can be assigned to generic scalar typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL = 
            new PossibleTypes( new int[] { 
                Types.BIT,
                Types.TINYINT,
                Types.BIGINT,
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.NUMERIC,
                Types.DECIMAL,
                Types.INTEGER,
                Types.SMALLINT,
                Types.FLOAT,
                Types.REAL,
                Types.DOUBLE,
                Types.VARCHAR } );
        
        /**
         * This is possibleTypes of null value which can be assigned to generic character typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL = 
            new PossibleTypes( new int[] {
                Types.BIT,
                Types.TINYINT,
                Types.BIGINT,
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.NUMERIC,
                Types.DECIMAL,
                Types.INTEGER,
                Types.SMALLINT,
                Types.FLOAT,
                Types.REAL,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of null value which can be assigned to VARBINARY typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_VARBINARY_NULL = 
            new PossibleTypes( new int[] { 
                Types.VARBINARY,
                Types.BINARY,
                Types.LONGVARBINARY } );
        
        /**
         * This is possibleTypes of null value which can be assigned to BINARY typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_BINARY_NULL = 
            new PossibleTypes( new int[] { 
                Types.VARBINARY,
                Types.BINARY,
                Types.LONGVARBINARY } );
        
        /**
         * This is possibleTypes of null value which can be assigned to LONGVARBINARY typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_LONGVARBINARY_NULL = 
            new PossibleTypes( new int[] {
                Types.VARBINARY,
                Types.BINARY,
                Types.LONGVARBINARY } );
        
        /**
         * This is possibleTypes of null value which can be assigned to DATE typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_DATE_NULL = 
            new PossibleTypes( new int[] {
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.DATE,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of null value which can be assigned to TIME typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_TIME_NULL = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.TIME,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of null value which can be assigned to TIMESTAMP typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_TIMESTAMP_NULL = 
            new PossibleTypes( new int[] {
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.DATE,
                Types.TIMESTAMP } );
        
        /**
         * This is possibleTypes of null value which can be assigned to CLOB typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_CLOB_NULL = 
            new PossibleTypes( new int[] { 
                Types.LONGVARCHAR,
                Types.CHAR,
                Types.VARCHAR,
                Types.CLOB } );
        
        /**
         * This is possibleTypes of null value which can be assigned to BLOB typed variable.
         */
        final public static PossibleTypes POSSIBLE_TYPES_FOR_BLOB_NULL = 
            new PossibleTypes( new int[] {
                Types.BLOB } );
        
        /**
         * This is possibleTypes of null value which can be assigned to other typed variable.
         */
        final public static PossibleTypes DEFAULT_POSSIBLE_TYPES_FOR_NULL = 
            new PossibleTypes( new int[] {
                Types.BIT,
                Types.TINYINT,
                Types.BIGINT,
                Types.LONGVARBINARY,
                Types.VARBINARY,
                Types.BINARY,
                Types.LONGVARCHAR,
                Types.NULL,
                Types.CHAR,
                Types.NUMERIC,
                Types.DECIMAL,
                Types.INTEGER,
                Types.SMALLINT,
                Types.FLOAT,
                Types.REAL,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.BOOLEAN,
                Types.DATALINK,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP,
                Types.OTHER,
                Types.JAVA_OBJECT,
                Types.DISTINCT,
                Types.STRUCT,
                Types.ARRAY,
                Types.BLOB,
                Types.CLOB,
                Types.REF } );
        
        /**
         * This method return true if the type is possible.
         */
        boolean checkType(int type){
            
            if(SanityManager.DEBUG){
                
                for(int i = 0;
                    i < possibleTypes.length - 1;
                    i ++){
                    
                    SanityManager.ASSERT(possibleTypes[i] < possibleTypes[i + 1]);
                    
                }
            }
            
            return Arrays.binarySearch( possibleTypes,
                                        type ) >= 0;
            
        }
        
        static SqlException throw22005Exception( LogWriter logWriter, 
                                                 int valType,
                                                 int paramType)
            
            throws SqlException{
            
            throw new SqlException( logWriter,
                                    new ClientMessageId(SQLState.LANG_DATA_TYPE_GET_MISMATCH) ,
                                    new Object[]{ 
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                        ClientTypes.getTypeString(valType),
                                        ClientTypes.getTypeString(paramType)
                                    },
                                    (Throwable) null);
        }
        
        
        /**
         * This method return possibleTypes of null value in variable typed as typeOfVariable.
         */
        static PossibleTypes getPossibleTypesForNull(int typeOfVariable){
            
            switch(typeOfVariable){
                
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            case Types.SMALLINT:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.INTEGER:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.BIGINT:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.REAL:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.FLOAT:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.DOUBLE:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.DECIMAL:
                return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
                
            case Types.CHAR:
                return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
                
            case Types.VARCHAR:
                return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
                
            case Types.LONGVARCHAR:
                return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
                
            case Types.VARBINARY:
                return POSSIBLE_TYPES_FOR_VARBINARY_NULL;
                
            case Types.BINARY:
                return POSSIBLE_TYPES_FOR_BINARY_NULL;
                
            case Types.LONGVARBINARY:
                return POSSIBLE_TYPES_FOR_LONGVARBINARY_NULL;
                
            case Types.DATE:
                return POSSIBLE_TYPES_FOR_DATE_NULL;
                
            case Types.TIME:
                return POSSIBLE_TYPES_FOR_TIME_NULL;
                
            case Types.TIMESTAMP:
                return POSSIBLE_TYPES_FOR_TIMESTAMP_NULL;
                
            case Types.CLOB:
                return POSSIBLE_TYPES_FOR_CLOB_NULL;
                
            case Types.BLOB:
                return POSSIBLE_TYPES_FOR_BLOB_NULL;
                
            }
        
            // as default, accept all type...
            return DEFAULT_POSSIBLE_TYPES_FOR_NULL;
        }
        
    }
    
    /**
     * <p>
     * Check for closed statement and extract the SQLException if it is raised.
     * </p>
     *
     * @throws java.sql.SQLException on error
     */
    protected void    checkStatus() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        try {
            checkForClosedStatement();
        }
        catch (SqlException se) { throw se.getSQLException(); }
    }

}

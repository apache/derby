/*

   Derby - Class org.apache.derby.client.am.CallableStatement

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



public class CallableStatement extends PreparedStatement
        implements java.sql.PreparedStatement,
        java.sql.CallableStatement,
        PreparedStatementCallbackInterface {
    //---------------------navigational members-----------------------------------

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    public MaterialPreparedStatement materialCallableStatement_ = null;

    //-----------------------------state------------------------------------------

    // last retrieved result was a sql NULL, NOT_NULL, or UNSET.
    private int wasNull_ = WAS_NULL_UNSET;
    static final private int WAS_NULL = 1;
    static final private int WAS_NOT_NULL = 2;
    static final private int WAS_NULL_UNSET = 0;

    //---------------------constructors/finalizer---------------------------------

    private void initCallableStatement() {
        materialCallableStatement_ = null;
        wasNull_ = WAS_NULL_UNSET;
    }

    public void reset(boolean fullReset) throws SqlException {
        if (fullReset) {
            connection_.resetPrepareCall(this);
        } else {
            super.reset(fullReset);
        }
        wasNull_ = WAS_NULL_UNSET;
    }

    // Common constructor for jdbc 2 callable statements with scroll attributes.
    // Called by material statement constructor.
    public CallableStatement(Agent agent,
                             Connection connection,
                             String sql,
                             int type, int concurrency, int holdability) throws SqlException {
        super(agent, connection, sql, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, null);
        initCallableStatement();
    }

    public void resetCallableStatement(Agent agent,
                                       Connection connection,
                                       String sql,
                                       int type, int concurrency, int holdability) throws SqlException {
        super.resetPreparedStatement(agent, connection, sql, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, null);
        initCallableStatement();
    }

    public void resetCallableStatement(Agent agent,
                                       Connection connection,
                                       String sql,
                                       Section section) throws SqlException {
        super.resetPreparedStatement(agent, connection, sql, section);
        initCallableStatement();
    }


    public void resetCallableStatement(Agent agent,
                                       Connection connection,
                                       String sql,
                                       Section section,
                                       ColumnMetaData parameterMetaData,
                                       ColumnMetaData resultSetMetaData) throws SqlException {
        super.resetPreparedStatement(agent, connection, sql, section, parameterMetaData, resultSetMetaData);
        initCallableStatement();
    }

    protected void finalize() throws java.lang.Throwable {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "finalize");
        }
        super.finalize();
    }

    //---------------------------entry points-------------------------------------

    public boolean execute() throws SqlException {
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

    // also used by SQLCA
    boolean executeX() throws SqlException {
        super.flowExecute(executeMethod__);
        return resultSet_ != null;
    }

    public java.sql.ResultSet executeQuery() throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "executeQuery");
            }
            ResultSet resultSet = executeQueryX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "executeQuery", resultSet);
            }
            return resultSet;
        }
    }

    // also used by DBMD methods
    ResultSet executeQueryX() throws SqlException {
        super.flowExecute(executeQueryMethod__);
        super.checkExecuteQueryPostConditions("java.sql.CallableStatement");
        return resultSet_;
    }

    public int executeUpdate() throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "executeUpdate");
            }
            int updateValue = executeUpdateX();
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "executeUpdate", updateValue);
            }
            return updateValue;
        }
    }

    int executeUpdateX() throws SqlException {
        super.flowExecute(executeUpdateMethod__);

        super.checkExecuteUpdatePostConditions("java.sql.CallableStatement");
        // make sure update count >= 0 even if derby don't support update count for call
        //return (updateCount_ < 0) ? 0 : updateCount_;
        return updateCount_;
    }


    public void clearParameters() throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "clearParameters");
            }
            super.clearParameters();
            outputRegistered_ = false; // this variable is only used by Batch
        }
    }

    public void registerOutParameter(int parameterIndex, int jdbcType) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterIndex, jdbcType);
            }
            registerOutParameterX(parameterIndex, jdbcType);
        }
    }

    // also used by Sqlca
    void registerOutParameterX(int parameterIndex, int jdbcType) throws SqlException {
        super.checkForClosedStatement();
        int scale = 0; // default scale to 0 for non numeric and non decimal type
        registerOutParameterX(parameterIndex, jdbcType, scale);
    }

    private int guessScaleForDecimalOrNumeric(int parameterIndex) throws SqlException {
        parameterIndex = checkForEscapedCallWithResult(parameterIndex);
        // Types.DECIMAL with no supplied scale will use the scale supplied by the setter method if input BigDecimal is not null
        if (parameterMetaData_.types_[parameterIndex - 1] == Types.DECIMAL &&
                parameters_[parameterIndex - 1] != null) {
            return parameterMetaData_.sqlScale_[parameterIndex - 1];
        }
        return 8; // default to scale of 8 if not specified
    }

    public void registerOutParameter(int parameterIndex, int jdbcType, int scale) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterIndex, jdbcType, scale);
            }
            super.checkForClosedStatement();
            registerOutParameterX(parameterIndex, jdbcType, scale);
        }
    }

    private void registerOutParameterX(int parameterIndex, int jdbcType, int scale) throws SqlException {
        parameterIndex = checkForEscapedCallWithResult(parameterIndex, jdbcType);
        // if the parameter is the return clause of the call statement
        if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
            return;
        }
        super.checkForValidParameterIndex(parameterIndex);
        checkForValidScale(scale);
        outputRegistered_ = true; // this variable is only used by Batch
        //parameterSetOrRegistered_[parameterIndex - 1] = true;
        parameterRegistered_[parameterIndex - 1] = true;
    }

    public void registerOutParameter(int parameterIndex, int jdbcType, String typeName) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterIndex, jdbcType, typeName);
            }
            super.checkForClosedStatement();
        }
    }

    public boolean wasNull() throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "wasNull");
        }
        boolean result = wasNullX();
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceExit(this, "wasNull", result);
        }
        return result;
    }

    private boolean wasNullX() throws SqlException {
        super.checkForClosedStatement();
        if (wasNull_ == WAS_NULL_UNSET) {
            throw new SqlException(agent_.logWriter_, "Invalid operation: wasNull() called with no data retrieved.");
        }
        return wasNull_ == WAS_NULL;
    }

    //--------------------------------getter methods------------------------------

    public boolean getBoolean(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBoolean", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            boolean result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = agent_.crossConverters_.getBooleanFromInt(returnValueFromProcedure_);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getBoolean", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? false : singletonRowData_.getBoolean(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBoolean", result);
            }
            return result;
        }
    }

    public byte getByte(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getByte", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            byte result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = agent_.crossConverters_.getByteFromInt(returnValueFromProcedure_);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getByte", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? 0 : singletonRowData_.getByte(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getByte", result);
            }
            return result;
        }
    }

    public short getShort(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getShort", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            short result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = agent_.crossConverters_.getShortFromInt(returnValueFromProcedure_);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getShort", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? 0 : singletonRowData_.getShort(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getShort", result);
            }
            return result;
        }
    }

    public int getInt(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getInt", parameterIndex);
            }
            int result = getIntX(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getInt", result);
            }
            return result;
        }
    }

    // also used by SQLCA
    int getIntX(int parameterIndex) throws SqlException {
        super.checkForClosedStatement();
        parameterIndex = checkForEscapedCallWithResult(parameterIndex);
        if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
            this.wasNull_ = this.WAS_NOT_NULL;
            return returnValueFromProcedure_;
        }
        checkGetterPreconditions(parameterIndex);
        setWasNull(parameterIndex);
        return wasNullX() ? 0 : singletonRowData_.getInt(parameterIndex);
    }

    public long getLong(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getLong", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            long result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = (long) returnValueFromProcedure_;
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getLong", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? 0 : singletonRowData_.getLong(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getLong", result);
            }
            return result;
        }
    }

    public float getFloat(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getFloat", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            float result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = (float) returnValueFromProcedure_;
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getFloat", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? 0 : singletonRowData_.getFloat(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getFloat", result);
            }
            return result;
        }
    }

    public double getDouble(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDouble", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            double result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = (double) returnValueFromProcedure_;
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getDouble", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? 0 : singletonRowData_.getDouble(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDouble", result);
            }
            return result;
        }
    }

    public java.math.BigDecimal getBigDecimal(int parameterIndex, int scale) throws SqlException, ArithmeticException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedEntry(this, "getBigDecimal", parameterIndex, scale);
            }
            super.checkForClosedStatement();
            checkForValidScale(scale);
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            java.math.BigDecimal result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = java.math.BigDecimal.valueOf(returnValueFromProcedure_).setScale(scale);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceDeprecatedExit(this, "getBigDecimal", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? null : singletonRowData_.getBigDecimal(parameterIndex);
            if (result != null) {
                result = result.setScale(scale, java.math.BigDecimal.ROUND_DOWN);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceDeprecatedExit(this, "getBigDecimal", result);
            }
            return result;
        }
    }

    public java.math.BigDecimal getBigDecimal(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBigDecimal", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            java.math.BigDecimal result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = java.math.BigDecimal.valueOf(returnValueFromProcedure_);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getBigDecimal", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? null : singletonRowData_.getBigDecimal(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBigDecimal", result);
            }
            return result;
        }
    }

    public java.sql.Date getDate(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Date result = wasNullX() ? null : singletonRowData_.getDate(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDate", result);
            }
            return result;
        }
    }

    public java.sql.Date getDate(int parameterIndex, java.util.Calendar cal) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getDate", parameterIndex, cal);
            }
            if (cal == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Date result = getDate(parameterIndex);
            if (result != null) {
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(cal.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(result);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(result);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                result.setTime(result.getTime() - timeZoneOffset);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getDate", result);
            }
            return result;
        }
    }

    public java.sql.Time getTime(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Time result = wasNullX() ? null : singletonRowData_.getTime(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTime", result);
            }
            return result;
        }
    }

    public java.sql.Time getTime(int parameterIndex, java.util.Calendar cal) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTime", parameterIndex, cal);
            }
            if (cal == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Time result = getTime(parameterIndex);
            if (result != null) {
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(cal.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(result);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(result);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                result.setTime(result.getTime() - timeZoneOffset);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTime", result);
            }
            return result;
        }
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Timestamp result = wasNullX() ? null : singletonRowData_.getTimestamp(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTimestamp", result);
            }
            return result;
        }
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex, java.util.Calendar cal) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getTimestamp", parameterIndex, cal);
            }
            if (cal == null) {
                throw new SqlException(agent_.logWriter_, "Invalid parameter: calendar is null");
            }
            java.sql.Timestamp result = getTimestamp(parameterIndex);
            if (result != null) {
                int nano = result.getNanos();
                java.util.Calendar targetCalendar = java.util.Calendar.getInstance(cal.getTimeZone());
                targetCalendar.clear();
                targetCalendar.setTime(result);
                java.util.Calendar defaultCalendar = java.util.Calendar.getInstance();
                defaultCalendar.clear();
                defaultCalendar.setTime(result);
                long timeZoneOffset =
                        targetCalendar.get(java.util.Calendar.ZONE_OFFSET) - defaultCalendar.get(java.util.Calendar.ZONE_OFFSET) +
                        targetCalendar.get(java.util.Calendar.DST_OFFSET) - defaultCalendar.get(java.util.Calendar.DST_OFFSET);
                result.setTime(result.getTime() - timeZoneOffset);
                result.setNanos(nano);
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getTimestamp", result);
            }
            return result;
        }
    }

    public String getString(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getString", parameterIndex);
            }
            String result = getStringX(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getString", result);
            }
            return result;
        }
    }

    // also used by SQLCA
    String getStringX(int parameterIndex) throws SqlException {
        super.checkForClosedStatement();
        parameterIndex = checkForEscapedCallWithResult(parameterIndex);
        if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
            this.wasNull_ = this.WAS_NOT_NULL;
            return Integer.toString(returnValueFromProcedure_);
        }
        checkGetterPreconditions(parameterIndex);
        setWasNull(parameterIndex);
        return wasNullX() ? null : singletonRowData_.getString(parameterIndex);
    }

    public byte[] getBytes(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBytes", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            byte[] result = wasNullX() ? null : singletonRowData_.getBytes(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBytes", result);
            }
            return result;
        }
    }

    public java.sql.Blob getBlob(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getBlob", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Blob result = wasNullX() ? null : singletonRowData_.getBlob(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getBlob", result);
            }
            return result;
        }
    }

    public java.sql.Clob getClob(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getClob", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Clob result = wasNullX() ? null : singletonRowData_.getClob(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getClob", result);
            }
            return result;
        }
    }

    public java.sql.Array getArray(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getArray", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Array result = wasNullX() ? null : singletonRowData_.getArray(parameterIndex);
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method is not yet implemented");
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getArray", result);
            }
            return result;
        }
    }

    public java.sql.Ref getRef(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getRef", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                throw new SqlException(agent_.logWriter_, "Invalid method call: parameter 1 is an integer OUT parameter returned by the stored procedure, use getInt call.");
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            java.sql.Ref result = wasNullX() ? null : singletonRowData_.getRef(parameterIndex);
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method is not yet implemented");
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getRef", result);
            }
            return result;
        }
    }

    public Object getObject(int parameterIndex) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", parameterIndex);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            Object result;
            if (parameterIndex == 0 && escapedProcedureCallWithResult_) {
                result = new Integer(returnValueFromProcedure_);
                this.wasNull_ = this.WAS_NOT_NULL;
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceExit(this, "getObject", result);
                }
                return result;
            }
            checkGetterPreconditions(parameterIndex);
            setWasNull(parameterIndex);
            result = wasNullX() ? null : singletonRowData_.getObject(parameterIndex);
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getObject", result);
            }
            return result;
        }
    }

    public Object getObject(int parameterIndex, java.util.Map map) throws SqlException {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry(this, "getObject", parameterIndex, map);
            }
            super.checkForClosedStatement();
            parameterIndex = checkForEscapedCallWithResult(parameterIndex);
            Object result;
            checkGetterPreconditions(parameterIndex);
            if (true) {
                throw new SqlException(agent_.logWriter_, "jdbc 2 method is not yet implemented.");
            }
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceExit(this, "getObject", result);
            }
            return result;
        }
    }

    //--------------------------JDBC 3.0------------------------------------------

    public void registerOutParameter(String parameterName, int sqlType) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterName, sqlType);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterName, sqlType, scale);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "registerOutParameter", parameterName, sqlType, typeName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.net.URL getURL(int parameterIndex) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getURL", parameterIndex);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setURL(String parameterName, java.net.URL x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setURL", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setNull(String parameterName, int sqlType) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setNull", parameterName, sqlType);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setBoolean(String parameterName, boolean x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setBoolean", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setByte(String parameterName, byte x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setByte", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setShort(String parameterName, short x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setShort", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setInt(String parameterName, int x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setInt", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setLong(String parameterName, long x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setLong", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setFloat(String parameterName, float x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setFloat", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setDouble(String parameterName, double x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setDouble", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setBigDecimal(String parameterName, java.math.BigDecimal x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setBigDecimal", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setString(String parameterName, String x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setString", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setBytes(String parameterName, byte x[]) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setBytes", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setDate(String parameterName, java.sql.Date x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setDate", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setTime(String parameterName, java.sql.Time x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTime", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setTimestamp(String parameterName, java.sql.Timestamp x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTimestamp", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setAsciiStream(String parameterName, java.io.InputStream x, int length) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setAsciiStream", parameterName, x, length);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setBinaryStream(String parameterName, java.io.InputStream x, int length) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setBinaryStream", parameterName, x, length);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setObject", parameterName, x, targetSqlType, scale);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setObject", parameterName, x, targetSqlType);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setObject(String parameterName, Object x) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setObject", parameterName, x);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setCharacterStream(String parameterName, java.io.Reader reader, int length) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setCharacterStream", parameterName, reader, length);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setDate(String parameterName, java.sql.Date x, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setDate", parameterName, x, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setTime(String parameterName, java.sql.Time x, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTime", parameterName, x, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setTimestamp(String parameterName, java.sql.Timestamp x, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setTimestamp", parameterName, x, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "setNull", parameterName, sqlType, typeName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public String getString(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getString", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public boolean getBoolean(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getBoolean", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public byte getByte(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getByte", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public short getShort(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getShort", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public int getInt(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getInt", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public long getLong(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getLong", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public float getFloat(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getFloat", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public double getDouble(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getDouble", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public byte[] getBytes(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getBytes", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Date getDate(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getDate", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Time getTime(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getTime", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Timestamp getTimestamp(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getTimestamp", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public Object getObject(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getObject", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.math.BigDecimal getBigDecimal(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getBigDecimal", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public Object getObject(String parameterName, java.util.Map map) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getObject", parameterName, map);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Ref getRef(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getRef", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Blob getBlob(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getBlob", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Clob getClob(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getClob", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Array getArray(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getArray", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Date getDate(String parameterName, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getDate", parameterName, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Time getTime(String parameterName, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getTime", parameterName, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.sql.Timestamp getTimestamp(String parameterName, java.util.Calendar calendar) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getTimestamp", parameterName, calendar);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    public java.net.URL getURL(String parameterName) throws SqlException {
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceEntry(this, "getURL", parameterName);
        }
        super.checkForClosedStatement();
        throw new SqlException(agent_.logWriter_, "JDBC 3 method called - not yet supported");
    }

    //----------------------------helper methods----------------------------------

    private int checkForEscapedCallWithResult(int parameterIndex) throws SqlException {
        if (escapedProcedureCallWithResult_) {
            parameterIndex--;
        }
        return parameterIndex;
    }

    private int checkForEscapedCallWithResult(int parameterIndex, int jdbcType) throws SqlException {
        if (escapedProcedureCallWithResult_) {
            if (parameterIndex == 1 && jdbcType != java.sql.Types.INTEGER) {
                throw new SqlException(agent_.logWriter_, "Parameter 1 is the return caluse of the stored procedure call, it can only be registered as an integer type");
            } else {
                parameterIndex--;
            }
        }
        return parameterIndex;
    }

    private void checkGetterPreconditions(int parameterIndex) throws SqlException {
        super.checkForValidParameterIndex(parameterIndex);
        checkForValidOutParameter(parameterIndex);
    }

    private void checkForValidOutParameter(int parameterIndex) throws SqlException {
        if (parameterMetaData_ == null || parameterMetaData_.sqlxParmmode_[parameterIndex - 1] < java.sql.ParameterMetaData.parameterModeInOut) {
            throw new SqlException(agent_.logWriter_, "Invalid argument: parameter index " + parameterIndex +
                    " is not an OUT or INOUT parameter.");
        }
    }

    private void setWasNull(int parameterIndex) {
        if (singletonRowData_ == null) {
            wasNull_ = WAS_NULL_UNSET;
        } else {
            wasNull_ = singletonRowData_.isNull_[parameterIndex - 1] ? WAS_NULL : WAS_NOT_NULL;
        }
    }
}


/*

   Derby - Class org.apache.derby.client.am.LogicalPreparedStatement

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
import java.net.URL;
import java.util.Calendar;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.derby.client.am.stmtcache.StatementKey;

/**
 * A wrapper class for a physical Derby prepared statement.
 * <p>
 * The idea behind the logical prepared statement is to allow reuse of the
 * physical prepared statement. In general the logical entity will forward all
 * calls to the physical entity. A few methods have special implementations, the
 * most important one being {@link #close}. Each method will check that the
 * logical statement is still open before the call is forwarded to the
 * underlying physical statement.
 *
 * @see LogicalStatementEntity
 */
public class LogicalPreparedStatement
    extends LogicalStatementEntity
    implements java.sql.PreparedStatement {

    /**
     * Creates a new logical prepared statement.
     *
     * @param physicalPs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalPreparedStatement(java.sql.PreparedStatement physicalPs,
                                    StatementKey stmtKey,
                                    StatementCacheInteractor cacheInteractor) {
        super(physicalPs, stmtKey, cacheInteractor);
    }

    public int executeUpdate() throws SQLException {
        return getPhysPs().executeUpdate();
    }

    public void addBatch() throws SQLException {
         getPhysPs().addBatch();
    }

    public void clearParameters() throws SQLException {
         getPhysPs().clearParameters();
    }

    public boolean execute() throws SQLException {
        return getPhysPs().execute();
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
         getPhysPs().setByte(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
         getPhysPs().setDouble(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
         getPhysPs().setFloat(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
         getPhysPs().setInt(parameterIndex, x);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
         getPhysPs().setNull(parameterIndex, sqlType);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
         getPhysPs().setLong(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
         getPhysPs().setShort(parameterIndex, x);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
         getPhysPs().setBoolean(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
         getPhysPs().setBytes(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
         getPhysPs().setAsciiStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
         getPhysPs().setBinaryStream(parameterIndex, x, length);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
         getPhysPs().setUnicodeStream(parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
         getPhysPs().setCharacterStream(parameterIndex, reader, length);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
         getPhysPs().setObject(parameterIndex, x);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
                throws SQLException {
         getPhysPs().setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x,
                          int targetSqlType, int scale)
            throws SQLException {
         getPhysPs().setObject(parameterIndex, x, targetSqlType, scale);
    }

    public void setNull(int paramIndex, int sqlType, String typeName)
            throws SQLException {
         getPhysPs().setNull(paramIndex, sqlType, typeName);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
         getPhysPs().setString(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
         getPhysPs().setBigDecimal(parameterIndex, x);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
         getPhysPs().setURL(parameterIndex, x);
    }

    public void setArray(int i, Array x) throws SQLException {
         getPhysPs().setArray(i, x);
    }

    public void setBlob(int i, Blob x) throws SQLException {
         getPhysPs().setBlob(i, x);
    }

    public void setClob(int i, Clob x) throws SQLException {
         getPhysPs().setClob(i, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
         getPhysPs().setDate(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return getPhysPs().getParameterMetaData();
    }

    public void setRef(int i, Ref x) throws SQLException {
         getPhysPs().setRef(i, x);
    }

    public ResultSet executeQuery() throws SQLException {
        return getPhysPs().executeQuery();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return getPhysPs().getMetaData();
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
         getPhysPs().setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
         getPhysPs().setTimestamp(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
         getPhysPs().setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException {
         getPhysPs().setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException {
         getPhysPs().setTimestamp(parameterIndex, x, cal);
    }

    public int getFetchDirection() throws SQLException {
        return getPhysPs().getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        return getPhysPs().getFetchSize();
    }

    public int getMaxFieldSize() throws SQLException {
        return getPhysPs().getMaxFieldSize();
    }

    public int getMaxRows() throws SQLException {
        return getPhysPs().getMaxRows();
    }

    public int getQueryTimeout() throws SQLException {
        return getPhysPs().getQueryTimeout();
    }

    public int getResultSetConcurrency() throws SQLException {
        return getPhysPs().getResultSetConcurrency();
    }

    public int getResultSetHoldability() throws SQLException {
        return getPhysPs().getResultSetHoldability();
    }

    public int getResultSetType() throws SQLException {
        return getPhysPs().getResultSetType();
    }

    public int getUpdateCount() throws SQLException {
        return getPhysPs().getUpdateCount();
    }

    public void cancel() throws SQLException {
         getPhysPs().cancel();
    }

    public void clearBatch() throws SQLException {
         getPhysPs().clearBatch();
    }

    public void clearWarnings() throws SQLException {
         getPhysPs().clearWarnings();
    }


    public boolean getMoreResults() throws SQLException {
        return getPhysPs().getMoreResults();
    }

    public int[] executeBatch() throws SQLException {
        return getPhysPs().executeBatch();
    }

    public void setFetchDirection(int direction) throws SQLException {
         getPhysPs().setFetchDirection(direction);
    }

    public void setFetchSize(int rows) throws SQLException {
         getPhysPs().setFetchSize(rows);
    }

    public void setMaxFieldSize(int max) throws SQLException {
         getPhysPs().setMaxFieldSize(max);
    }

    public void setMaxRows(int max) throws SQLException {
         getPhysPs().setMaxRows(max);
    }

    public void setQueryTimeout(int seconds) throws SQLException {
         getPhysPs().setQueryTimeout(seconds);
    }

    public boolean getMoreResults(int current) throws SQLException {
        return getPhysPs().getMoreResults(current);
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
         getPhysPs().setEscapeProcessing(enable);
    }

    public int executeUpdate(String sql) throws SQLException {
        return getPhysPs().executeUpdate(sql);
    }

    public void addBatch(String sql) throws SQLException {
         getPhysPs().addBatch(sql);
    }

    public void setCursorName(String name) throws SQLException {
         getPhysPs().setCursorName(name);
    }

    public boolean execute(String sql) throws SQLException {
        return getPhysPs().execute(sql);
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        return getPhysPs().executeUpdate(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        return getPhysPs().execute(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        return getPhysPs().executeUpdate(sql, columnIndexes);
    }

    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException {
        return getPhysPs().execute(sql, columnIndexes);
    }

    public Connection getConnection() throws SQLException {
        return getPhysPs().getConnection();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return getPhysPs().getGeneratedKeys();
    }

    public ResultSet getResultSet() throws SQLException {
        return getPhysPs().getResultSet();
    }

    public SQLWarning getWarnings() throws SQLException {
        return getPhysPs().getWarnings();
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        return getPhysPs().executeUpdate(sql, columnNames);
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        return getPhysPs().execute(sql, columnNames);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return getPhysPs().executeQuery(sql);
    }
}

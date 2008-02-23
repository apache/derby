/*

   Derby - Class org.apache.derby.client.am.LogicalCallableStatement

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
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.derby.client.am.stmtcache.StatementKey;

/**
 * A wrapper class for a physical Derby callable statement.
 * <p>
 * The idea behind the logical prepared statement is to allow reuse of the
 * physical callable statement. In general the logical entity will forward all
 * calls to the physical entity. A few methods have special implementations, the
 * most important one being {@link #close}. Each method will check that the
 * logical statement is still open before the call is forwarded to the
 * underlying physical statement.
 *
 * @see LogicalStatementEntity
 */
public class LogicalCallableStatement
    extends LogicalPreparedStatement
    implements java.sql.CallableStatement {

    /**
     * Creates a new logical callable statement.
     *
     * @param physicalCs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalCallableStatement(java.sql.CallableStatement physicalCs,
                                    StatementKey stmtKey,
                                    StatementCacheInteractor cacheInteractor) {
        super(physicalCs, stmtKey, cacheInteractor);
    }

    public boolean wasNull() throws SQLException {
        return getPhysCs().wasNull();
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return getPhysCs().getByte(parameterIndex);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return getPhysCs().getDouble(parameterIndex);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return getPhysCs().getFloat(parameterIndex);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return getPhysCs().getInt(parameterIndex);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return getPhysCs().getLong(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return getPhysCs().getShort(parameterIndex);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return getPhysCs().getBoolean(parameterIndex);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return getPhysCs().getBytes(parameterIndex);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
         getPhysCs().registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
         getPhysCs().registerOutParameter(parameterIndex, sqlType, scale);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return getPhysCs().getObject(parameterIndex);
    }

    public String getString(int parameterIndex) throws SQLException {
        return getPhysCs().getString(parameterIndex);
    }

    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
         getPhysCs().registerOutParameter(paramIndex, sqlType, typeName);
    }

    public byte getByte(String parameterName) throws SQLException {
        return getPhysCs().getByte(parameterName);
    }

    public double getDouble(String parameterName) throws SQLException {
        return getPhysCs().getDouble(parameterName);
    }

    public float getFloat(String parameterName) throws SQLException {
        return getPhysCs().getFloat(parameterName);
    }

    public int getInt(String parameterName) throws SQLException {
        return getPhysCs().getInt(parameterName);
    }

    public long getLong(String parameterName) throws SQLException {
        return getPhysCs().getLong(parameterName);
    }

    public short getShort(String parameterName) throws SQLException {
        return getPhysCs().getShort(parameterName);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return getPhysCs().getBoolean(parameterName);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return getPhysCs().getBytes(parameterName);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
         getPhysCs().setByte(parameterName, x);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
         getPhysCs().setDouble(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
         getPhysCs().setFloat(parameterName, x);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
         getPhysCs().registerOutParameter(parameterName, sqlType);
    }

    public void setInt(String parameterName, int x) throws SQLException {
         getPhysCs().setInt(parameterName, x);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
         getPhysCs().setNull(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
         getPhysCs().registerOutParameter(parameterName, sqlType, scale);
    }

    public void setLong(String parameterName, long x) throws SQLException {
         getPhysCs().setLong(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
         getPhysCs().setShort(parameterName, x);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
         getPhysCs().setBoolean(parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
         getPhysCs().setBytes(parameterName, x);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return getPhysCs().getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return getPhysCs().getBigDecimal(parameterIndex, scale);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return getPhysCs().getURL(parameterIndex);
    }

    public Array getArray(int i) throws SQLException {
        return getPhysCs().getArray(i);
    }

    public Blob getBlob(int i) throws SQLException {
        return getPhysCs().getBlob(i);
    }

    public Clob getClob(int i) throws SQLException {
        return getPhysCs().getClob(i);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return getPhysCs().getDate(parameterIndex);
    }

    public Ref getRef(int i) throws SQLException {
        return getPhysCs().getRef(i);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return getPhysCs().getTime(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return getPhysCs().getTimestamp(parameterIndex);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
         getPhysCs().setAsciiStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
         getPhysCs().setBinaryStream(parameterName, x, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
         getPhysCs().setCharacterStream(parameterName, reader, length);
    }

    public Object getObject(String parameterName) throws SQLException {
        return getPhysCs().getObject(parameterName);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
         getPhysCs().setObject(parameterName, x);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
         getPhysCs().setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
         getPhysCs().setObject(parameterName, x, targetSqlType, scale);
    }

    public Object getObject(int i, Map map) throws SQLException {
        return getPhysCs().getObject(i, map);
    }

    public String getString(String parameterName) throws SQLException {
        return getPhysCs().getString(parameterName);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
         getPhysCs().registerOutParameter(parameterName, sqlType, typeName);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
         getPhysCs().setNull(parameterName, sqlType, typeName);
    }

    public void setString(String parameterName, String x) throws SQLException {
         getPhysCs().setString(parameterName, x);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getPhysCs().getBigDecimal(parameterName);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
         getPhysCs().setBigDecimal(parameterName, x);
    }

    public URL getURL(String parameterName) throws SQLException {
        return getPhysCs().getURL(parameterName);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
         getPhysCs().setURL(parameterName, val);
    }

    public Array getArray(String parameterName) throws SQLException {
        return getPhysCs().getArray(parameterName);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return getPhysCs().getBlob(parameterName);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return getPhysCs().getClob(parameterName);
    }

    public Date getDate(String parameterName) throws SQLException {
        return getPhysCs().getDate(parameterName);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
         getPhysCs().setDate(parameterName, x);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return getPhysCs().getDate(parameterIndex, cal);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return getPhysCs().getRef(parameterName);
    }

    public Time getTime(String parameterName) throws SQLException {
        return getPhysCs().getTime(parameterName);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
         getPhysCs().setTime(parameterName, x);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return getPhysCs().getTime(parameterIndex, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getPhysCs().getTimestamp(parameterName);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
         getPhysCs().setTimestamp(parameterName, x);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return getPhysCs().getTimestamp(parameterIndex, cal);
    }

    public Object getObject(String parameterName, Map map) throws SQLException {
        return getPhysCs().getObject(parameterName, map);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getPhysCs().getDate(parameterName, cal);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getPhysCs().getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getPhysCs().getTimestamp(parameterName, cal);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
         getPhysCs().setDate(parameterName, x, cal);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
         getPhysCs().setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
         getPhysCs().setTimestamp(parameterName, x, cal);
    }
}

/*

   Derby - Class org.apache.derby.client.am.LogicalCallableStatement40

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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;

import org.apache.derby.client.am.stmtcache.StatementKey;

import org.apache.derby.shared.common.reference.SQLState;

/**
 * JDBC 4 specific wrapper class for a Derby physical callable statement.
 *
 * @see LogicalCallableStatement
 * @see #isClosed
 */
public class LogicalCallableStatement40
    extends LogicalCallableStatement {

    /**
     * Creates a new logical callable statement.
     *
     * @param physicalCs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalCallableStatement40(java.sql.CallableStatement physicalCs,
                                      StatementKey stmtKey,
                                      StatementCacheInteractor cacheInteractor){
        super(physicalCs, stmtKey, cacheInteractor);
    }

    public void setRowId(int arg0, RowId arg1)
            throws SQLException {
         getPhysCs().setRowId(arg0, arg1);
    }

    public void setNString(int arg0, String arg1)
            throws SQLException {
         getPhysCs().setNString(arg0, arg1);
    }

    public void setNCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setNCharacterStream(arg0, arg1, arg2);
    }

    public void setNClob(int arg0, NClob arg1)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1);
    }

    public void setClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setClob(arg0, arg1, arg2);
    }

    public void setBlob(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setBlob(arg0, arg1, arg2);
    }

    public void setNClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1, arg2);
    }

    public void setSQLXML(int arg0, SQLXML arg1)
            throws SQLException {
         getPhysCs().setSQLXML(arg0, arg1);
    }

    public void setAsciiStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setAsciiStream(arg0, arg1, arg2);
    }

    public void setBinaryStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setBinaryStream(arg0, arg1, arg2);
    }

    public void setCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setCharacterStream(arg0, arg1, arg2);
    }

    public void setAsciiStream(int arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setAsciiStream(arg0, arg1);
    }

    public void setBinaryStream(int arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setBinaryStream(arg0, arg1);
    }

    public void setCharacterStream(int arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setCharacterStream(arg0, arg1);
    }

    public void setNCharacterStream(int arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setNCharacterStream(arg0, arg1);
    }

    public void setClob(int arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setClob(arg0, arg1);
    }

    public void setBlob(int arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setBlob(arg0, arg1);
    }

    public void setNClob(int arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1);
    }

    public synchronized boolean isClosed()
            throws SQLException {
        // Note the extra synchronization.
        boolean closed = isLogicalEntityClosed();
        if (!closed) {
            // Consult the underlying physical statement.
            closed = getPhysCs().isClosed();
        }
        return closed;
    }

    public void setPoolable(boolean arg0)
            throws SQLException {
         getPhysCs().setPoolable(arg0);
    }

    public boolean isPoolable()
            throws SQLException {
        return getPhysCs().isPoolable();
    }

    public <T> T unwrap(Class<T> arg0)
            throws SQLException {
        try {
            if (getPhysCs().isClosed()) {
                throw (new SqlException(null,
                    new ClientMessageId(SQLState.ALREADY_CLOSED),
                                        "CallableStatement")).getSQLException();
            }
            return arg0.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(
                    null,
                    new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                    arg0).getSQLException();
        }
    }

    public boolean isWrapperFor(Class<?> arg0)
            throws SQLException {
        return getPhysCs().isWrapperFor(arg0);
    }

    public RowId getRowId(int arg0)
            throws SQLException {
        return getPhysCs().getRowId(arg0);
    }

    public RowId getRowId(String arg0)
            throws SQLException {
        return getPhysCs().getRowId(arg0);
    }

    public void setRowId(String arg0, RowId arg1)
            throws SQLException {
         getPhysCs().setRowId(arg0, arg1);
    }

    public void setNString(String arg0, String arg1)
            throws SQLException {
         getPhysCs().setNString(arg0, arg1);
    }

    public void setNCharacterStream(String arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setNCharacterStream(arg0, arg1, arg2);
    }

    public void setNClob(String arg0, NClob arg1)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1);
    }

    public void setClob(String arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setClob(arg0, arg1, arg2);
    }

    public void setBlob(String arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setBlob(arg0, arg1, arg2);
    }

    public void setNClob(String arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1, arg2);
    }

    public NClob getNClob(int arg0)
            throws SQLException {
        return getPhysCs().getNClob(arg0);
    }

    public NClob getNClob(String arg0)
            throws SQLException {
        return getPhysCs().getNClob(arg0);
    }

    public void setSQLXML(String arg0, SQLXML arg1)
            throws SQLException {
         getPhysCs().setSQLXML(arg0, arg1);
    }

    public SQLXML getSQLXML(int arg0)
            throws SQLException {
        return getPhysCs().getSQLXML(arg0);
    }

    public SQLXML getSQLXML(String arg0)
            throws SQLException {
        return getPhysCs().getSQLXML(arg0);
    }

    public String getNString(int arg0)
            throws SQLException {
        return getPhysCs().getNString(arg0);
    }

    public String getNString(String arg0)
            throws SQLException {
        return getPhysCs().getNString(arg0);
    }

    public Reader getNCharacterStream(int arg0)
            throws SQLException {
        return getPhysCs().getNCharacterStream(arg0);
    }

    public Reader getNCharacterStream(String arg0)
            throws SQLException {
        return getPhysCs().getNCharacterStream(arg0);
    }

    public Reader getCharacterStream(int arg0)
            throws SQLException {
        return getPhysCs().getCharacterStream(arg0);
    }

    public Reader getCharacterStream(String arg0)
            throws SQLException {
        return getPhysCs().getCharacterStream(arg0);
    }

    public void setBlob(String arg0, Blob arg1)
            throws SQLException {
         getPhysCs().setBlob(arg0, arg1);
    }

    public void setClob(String arg0, Clob arg1)
            throws SQLException {
         getPhysCs().setClob(arg0, arg1);
    }

    public void setAsciiStream(String arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setAsciiStream(arg0, arg1, arg2);
    }

    public void setBinaryStream(String arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysCs().setBinaryStream(arg0, arg1, arg2);
    }

    public void setCharacterStream(String arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysCs().setCharacterStream(arg0, arg1, arg2);
    }

    public void setAsciiStream(String arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setAsciiStream(arg0, arg1);
    }

    public void setBinaryStream(String arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setBinaryStream(arg0, arg1);
    }

    public void setCharacterStream(String arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setCharacterStream(arg0, arg1);
    }

    public void setNCharacterStream(String arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setNCharacterStream(arg0, arg1);
    }

    public void setClob(String arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setClob(arg0, arg1);
    }

    public void setBlob(String arg0, InputStream arg1)
            throws SQLException {
         getPhysCs().setBlob(arg0, arg1);
    }

    public void setNClob(String arg0, Reader arg1)
            throws SQLException {
         getPhysCs().setNClob(arg0, arg1);
    }
}

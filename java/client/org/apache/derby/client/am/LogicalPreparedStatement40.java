/*

   Derby - Class org.apache.derby.client.am.LogicalPreparedStatement40

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

import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;

import org.apache.derby.client.am.stmtcache.StatementKey;

import org.apache.derby.shared.common.reference.SQLState;

/**
 * JDBC 4 specific wrapper class for a Derby physical prepared statement.
 *
 * @see LogicalPreparedStatement
 * @see #isClosed
 */
public class LogicalPreparedStatement40
    extends LogicalPreparedStatement {

    /**
     * Creates a new logical prepared statement.
     *
     * @param physicalPs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalPreparedStatement40(java.sql.PreparedStatement physicalPs,
                                      StatementKey stmtKey,
                                      StatementCacheInteractor cacheInteractor){
        super(physicalPs, stmtKey, cacheInteractor);
    }

    public void setRowId(int arg0, RowId arg1)
            throws SQLException {
         getPhysPs().setRowId(arg0, arg1);
    }

    public void setNString(int arg0, String arg1)
            throws SQLException {
         getPhysPs().setNString(arg0, arg1);
    }

    public void setNCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysPs().setNCharacterStream(arg0, arg1, arg2);
    }

    public void setNClob(int arg0, NClob arg1)
            throws SQLException {
         getPhysPs().setNClob(arg0, arg1);
    }

    public void setClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysPs().setClob(arg0, arg1, arg2);
    }

    public void setBlob(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysPs().setBlob(arg0, arg1, arg2);
    }

    public void setNClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysPs().setNClob(arg0, arg1, arg2);
    }

    public void setSQLXML(int arg0, SQLXML arg1)
            throws SQLException {
         getPhysPs().setSQLXML(arg0, arg1);
    }

    public void setAsciiStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysPs().setAsciiStream(arg0, arg1, arg2);
    }

    public void setBinaryStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
         getPhysPs().setBinaryStream(arg0, arg1, arg2);
    }

    public void setCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
         getPhysPs().setCharacterStream(arg0, arg1, arg2);
    }

    public void setAsciiStream(int arg0, InputStream arg1)
            throws SQLException {
         getPhysPs().setAsciiStream(arg0, arg1);
    }

    public void setBinaryStream(int arg0, InputStream arg1)
            throws SQLException {
         getPhysPs().setBinaryStream(arg0, arg1);
    }

    public void setCharacterStream(int arg0, Reader arg1)
            throws SQLException {
         getPhysPs().setCharacterStream(arg0, arg1);
    }

    public void setNCharacterStream(int arg0, Reader arg1)
            throws SQLException {
         getPhysPs().setNCharacterStream(arg0, arg1);
    }

    public void setClob(int arg0, Reader arg1)
            throws SQLException {
         getPhysPs().setClob(arg0, arg1);
    }

    public void setBlob(int arg0, InputStream arg1)
            throws SQLException {
         getPhysPs().setBlob(arg0, arg1);
    }

    public void setNClob(int arg0, Reader arg1)
            throws SQLException {
         getPhysPs().setNClob(arg0, arg1);
    }

    public synchronized boolean isClosed()
            throws SQLException {
        // Note the extra synchronization.
        boolean closed = isLogicalEntityClosed();
        if (!closed) {
            // Consult the underlying physical statement.
            closed = getPhysPs().isClosed();
        }
        return closed;
    }

    public void setPoolable(boolean arg0)
            throws SQLException {
         getPhysPs().setPoolable(arg0);
    }

    public boolean isPoolable()
            throws SQLException {
        return getPhysPs().isPoolable();
    }

    public <T> T unwrap(Class<T> arg0)
            throws SQLException {
        try {
            if (getPhysPs().isClosed()) {
                throw (new SqlException(null,
                    new ClientMessageId(SQLState.ALREADY_CLOSED),
                                        "PreparedStatement")).getSQLException();
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
        return getPhysPs().isWrapperFor(arg0);
    }
}

/*
 
   Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet40
 
   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.impl.jdbc;

import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import org.apache.derby.iapi.sql.ResultSet;
import java.sql.Statement;

/**
 * Implementation of JDBC 4 specific ResultSet methods.
 */
public class EmbedResultSet40 extends org.apache.derby.impl.jdbc.EmbedResultSet20{
    
    /** Creates a new instance of EmbedResultSet40 */
    public EmbedResultSet40(org.apache.derby.impl.jdbc.EmbedConnection conn,
        ResultSet resultsToWrap,
        boolean forMetaData,
        org.apache.derby.impl.jdbc.EmbedStatement stmt,
        boolean isAtomic)
        throws SQLException {
        
        super(conn, resultsToWrap, forMetaData, stmt, isAtomic);
    }
    
    public RowId getRowId(int columnIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    public RowId getRowId(String columnName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw Util.notImplemented();
    }
    
    /**
     * Retrieves the holdability for this <code>ResultSet</code>
     * object.
     *
     * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     * or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @exception SQLException if a database error occurs
     */
    public final int getHoldability() throws SQLException {
        checkIfClosed("getHoldability");
        Statement statement = getStatement();
        if (statement == null) {
            // If statement is null, the result set is an internal
            // result set created by getNewRowSet() or getOldRowSet()
            // in InternalTriggerExecutionContext. These result sets
            // are not exposed to the JDBC applications. Returning
            // CLOSE_CURSORS_AT_COMMIT since the result set will be
            // closed when the trigger has finished.
            return CLOSE_CURSORS_AT_COMMIT;
        }
        return statement.getResultSetHoldability();
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
        return isClosed;
    }
    
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNString(String columnName, String nString) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob getNClob(int i) throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob getNClob(String colName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML getSQLXML(String colName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();
    }
    
}

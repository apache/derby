/*

   Derby - Class org.apache.derby.client.am.LogicalConnection40

   Copyright (c) 2006 The Apache Software Foundation or its licensors, where applicable.

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

import java.sql.Array;
import java.sql.BaseQuery;
import java.sql.Blob;
import java.sql.ClientInfoException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Wrapper;
import java.util.Properties;

import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * A simple delegation wrapper handle for a physical connection.
 * This class only contains JDBC 4.0 specific methods.
 *
 * NOTE: All non-implemented JDBC 4.0 methods are located here, but when they
 *       are implemented, they should be moved to the superclass if possible.
 */
public class LogicalConnection40
    extends LogicalConnection {

    public LogicalConnection40(Connection physicalConnection,
                               ClientPooledConnection pooledConnection) 
        throws SqlException {
        super(physicalConnection, pooledConnection);
    }

    public Array createArray(String typeName, Object[] elements)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createArray(String,Object[])");
    }
    
    public Blob createBlob()
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createBlob()");
    }

    public Clob createClob()
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createClob()");
    }
    
    public NClob createNClob()
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createNClob()");
    }

    public <T extends BaseQuery>T createQueryObject(Class<T> ifc)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createQueryObject(Class<T>)");
    }
    
    public SQLXML createSQLXML()
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createSQLXML()");
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("createStruct(String,Object[])");
    }

    public Properties getClientInfo()
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getClientInfo()");
    }
    
    public String getClientInfo(String name)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getClientInfo(String)");
    }


    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by running a simple query against the 
     * database.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the 
     * timeout period expires before the operation completes, this 
     * method returns false. A value of 0 indicates a timeout is not 
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the call on the physical connection throws an
     * exception.
     */
    synchronized public boolean isValid(int timeout) throws SQLException {
        // Check if we have a underlying physical connection
        if (physicalConnection_ == null) {
            return false;
        }
        return physicalConnection_.isValid(timeout);
    }
   

    public boolean isWrapperFor(Class<?> interfaces)
        throws SQLException {
        checkForNullPhysicalConnection();
        return interfaces.isInstance(this);
    }

    public void setClientInfo(Properties properties)
        throws ClientInfoException {
        SQLException sqle = 
            SQLExceptionFactory.notImplemented("setClientInfo(Properties)");
        throw new ClientInfoException(sqle.getMessage(), 
                                      sqle.getSQLState(), 
                                      (Properties)properties.clone());
    }
    
    public void setClientInfo(String name, String value)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("setClientInfo(String,String)");
    }
    
    public <T>T unwrap(Class<T> interfaces)
        throws SQLException {
        checkForNullPhysicalConnection();
        // Derby does not implement non-standard methods on JDBC objects
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                                   new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                                   interfaces).getSQLException();
        }
    }
    
} // End class LogicalConnection40

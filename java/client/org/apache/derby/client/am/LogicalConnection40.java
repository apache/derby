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
import java.sql.SQLClientInfoException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Wrapper;
import java.util.Properties;

import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.shared.common.reference.SQLState;
import java.util.Map;

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

    public Array createArrayOf(String typeName, Object[] elements)
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createArrayOf( typeName, elements );
    }
    
    public Blob createBlob()
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createBlob();
    }

    public Clob createClob()
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createClob();
    }
    
    public NClob createNClob()
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createNClob();
    }

    public <T extends BaseQuery>T createQueryObject(Class<T> ifc)
        throws SQLException {
		checkForNullPhysicalConnection();
		return physicalConnection_.createQueryObject( ifc );
    }
    
    public SQLXML createSQLXML()
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createSQLXML();
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
		checkForNullPhysicalConnection();
        return physicalConnection_.createStruct( typeName, attributes );
    }

    /**
     * <code>getClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     * <code>getClientInfo</code> always returns an empty
     * <code>Properties</code> object since Derby doesn't support
     * ClientInfoProperties.
     *
     * @return an empty <code>Properties</code> object
     * @exception SQLException if an error occurs
     */
    public Properties getClientInfo()
        throws SQLException {
	checkForNullPhysicalConnection();
	return physicalConnection_.getClientInfo();
    }
    
    /**
     * <code>getClientInfo</code> forwards to
     * <code>physicalConnection_</code>. Always returns a <code>null
     * String</code> since Derby does not support
     * ClientInfoProperties.
     *
     * @param name a property key to get <code>String</code>
     * @return a property value <code>String</code>
     * @exception SQLException if an error occurs
     */
    public String getClientInfo(String name)
        throws SQLException {
	checkForNullPhysicalConnection();
	return physicalConnection_.getClientInfo(name);
    }

    /**
     * Returns the type map for this connection.
     *
     * @return type map for this connection
     * @exception SQLException if a database access error occurs
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkForNullPhysicalConnection();
        return ((java.sql.Connection) physicalConnection_).getTypeMap();
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

    /**
     * <code>setClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     *
     * @param properties a <code>Properties</code> object with the
     * properties to set
     * @exception SQLClientInfoException if an error occurs
     */
    public void setClientInfo(Properties properties)
        throws SQLClientInfoException {
	try { checkForNullPhysicalConnection(); }
	catch (SQLException se) { 
	    throw new SQLClientInfoException
		(se.getMessage(), se.getSQLState(), 
		 (new FailedProperties40(properties)).getProperties());
	}
	physicalConnection_.setClientInfo(properties);
    }
    
    /**
     * <code>setClientInfo</code> forwards to
     * <code>physicalConnection_</code>.
     *
     * @param name a property key <code>String</code>
     * @param value a property value <code>String</code>
     * @exception SQLException if an error occurs
     */
    public void setClientInfo(String name, String value)
        throws SQLClientInfoException {
	try { checkForNullPhysicalConnection(); }
        catch (SQLException se) {
            throw new SQLClientInfoException
                (se.getMessage(), se.getSQLState(),
                 new FailedProperties40
                 (FailedProperties40.makeProperties
                  (name,value)).getProperties());
        }
	physicalConnection_.setClientInfo(name, value);
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

/*

   Derby - Class org.apache.derby.client.am.LogicalConnection40

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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.net.NetConnection40;
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

    public Array createArrayOf(String typeName, Object[] elements)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createArrayOf( typeName, elements );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    public Blob createBlob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createBlob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public Clob createClob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    public NClob createNClob()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createNClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public SQLXML createSQLXML()
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createSQLXML();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.createStruct( typeName, attributes );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
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
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.getClientInfo();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
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
        try
        {
            checkForNullPhysicalConnection();
            return physicalConnection_.getClientInfo(name);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Returns a newly created logical database metadata object.
     *
     * @return A logical database metadata object for JDBC 4 environments.
     */
    protected LogicalDatabaseMetaData newLogicalDatabaseMetaData()
            throws SQLException {
        return new LogicalDatabaseMetaData40(
                                this, physicalConnection_.agent_.logWriter_);
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
        try
        {
            // Check if we have a underlying physical connection
            if (physicalConnection_ == null) {
                return false;
            }
            return physicalConnection_.isValid(timeout);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
   

    public boolean isWrapperFor(Class<?> interfaces)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            return interfaces.isInstance(this);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
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
        try
        {
            checkForNullPhysicalConnection(); 
            physicalConnection_.setClientInfo(properties);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw new SQLClientInfoException
            (sqle.getMessage(), sqle.getSQLState(), 
                    sqle.getErrorCode(),
                    (new FailedProperties40(properties)).getProperties());
        }
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
        try
        {
            checkForNullPhysicalConnection(); 
            physicalConnection_.setClientInfo(name, value);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw new SQLClientInfoException
            (sqle.getMessage(), sqle.getSQLState(),
                    sqle.getErrorCode(),
             new FailedProperties40
             (FailedProperties40.makeProperties
              (name,value)).getProperties());
        }
    }
    
    public <T>T unwrap(Class<T> interfaces)
        throws SQLException {
        try
        {
            checkForNullPhysicalConnection();
            // Derby does not implement non-standard methods on JDBC objects
            try {
                return interfaces.cast(this);
            } catch (ClassCastException cce) {
                throw new SqlException(null,
                                       new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                                       interfaces).getSQLException();
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////
    
    public  void    abort( Executor executor )  throws SQLException
    {
        try
        {
            if ( physicalConnection_ != null )
            {
                ((NetConnection40) physicalConnection_).abort( executor );
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public int getNetworkTimeout() throws SQLException
    {
        try
        {
            checkForNullPhysicalConnection();
            return ((NetConnection40) physicalConnection_).getNetworkTimeout();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        try
        {
            checkForNullPhysicalConnection();
            ((NetConnection40) physicalConnection_).setNetworkTimeout( executor, milliseconds );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
} // End class LogicalConnection40

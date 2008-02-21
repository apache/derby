/*
 
   Derby - Class org.apache.derby.client.net.NetDatabaseMetaData40
 
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

package org.apache.derby.client.net;

import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;

public class NetDatabaseMetaData40 extends org.apache.derby.client.net.NetDatabaseMetaData {
    
    
    public NetDatabaseMetaData40(NetAgent netAgent, NetConnection netConnection) {
        super(netAgent,netConnection);
    }

    /**
     * Retrieves the major JDBC version number for this driver.
     * @return JDBC version major number
     * @exception SQLException if the connection is closed
     */
    public int getJDBCMajorVersion() throws SQLException {
        checkForClosedConnection();
        return 4;
    }

    /**
     * Retrieves the minor JDBC version number for this driver.
     * @return JDBC version minor number
     * @exception SQLException if the connection is closed
     */
    public int getJDBCMinorVersion() throws SQLException {
        checkForClosedConnection();
        return 0;
    }

    /**
     * Indicates whether or not this data source supports the SQL
     * <code>ROWID</code> type. Since Derby does not support the
     * <code>ROWID</code> type, return <code>ROWID_UNSUPPORTED</code>.
     *
     * @return <code>ROWID_UNSUPPORTED</code>
     * @exception SQLException if a database access error occurs
     */
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        checkForClosedConnection();
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
    
    /**
     * Returns false unless <code>interfaces</code> is implemented 
     * 
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or 
     *                                directly or indirectly wraps an object 
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining 
     *                                whether this is a wrapper for an object 
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        return interfaces.isInstance(this);
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption if no object if found that implements the 
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces)
                                   throws SQLException {
        try { 
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                interfaces).getSQLException();
        }
    }
}

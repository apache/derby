/*

   Derby - Class org.apache.derby.iapi.jdbc.EngineConnection

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;


/**
 * Additional methods the embedded engine exposes on its Connection object
 * implementations. An internal api only, mainly for the network
 * server. Allows consistent interaction between EmbedConnections
 * and BrokeredConnections.
 * 
 */
public interface EngineConnection extends Connection {

    /**
     * Set the DRDA identifier for this connection.
     */
    public void setDrdaID(String drdaID);

    /**
     * Is this a global transaction
     * @return true if this is a global XA transaction
     */
    public boolean isInGlobalTransaction();
    
    /** 
     * Set the transaction isolation level that will be used for the 
     * next prepare.  Used by network server to implement DB2 style 
     * isolation levels.
     * Note the passed in level using the Derby constants from
     * ExecutionContext and not the JDBC constants from java.sql.Connection.
     * @param level Isolation level to change to.  level is the DB2 level
     *               specified in the package names which happen to correspond
     *               to our internal levels. If 
     *               level == ExecutionContext.UNSPECIFIED_ISOLATION,
     *               the statement won't be prepared with an isolation level.
     * 
     * 
     */
    public void setPrepareIsolation(int level) throws SQLException;

    /**
     * Return prepare isolation 
     */
    public int getPrepareIsolation()
        throws SQLException;

    /**
     * Get the holdability of the connection. 
     * Identical to JDBC 3.0 method, to allow holdabilty
     * to be supported in JDK 1.3 by the network server,
     * e.g. when the client is jdk 1.4 or above.
     * Can be removed once JDK 1.3 is no longer supported.
     */
    public int getHoldability() throws SQLException;
    
    /**
     * Add a SQLWarning to this Connection object.
     * @param newWarning Warning to be added, will be chained to any
     * existing warnings.
     */
    public void addWarning(SQLWarning newWarning)
        throws SQLException;

    /**
    * Clear the HashTable of all entries.
    * Called when a commit or rollback of the transaction
    * happens.
    */
    public void clearLOBMapping() throws SQLException;

    /**
    * Get the LOB reference corresponding to the locator.
    * @param key the integer that represents the LOB locator value.
    * @return the LOB Object corresponding to this locator.
    */
    public Object getLOBMapping(int key) throws SQLException;

    /**
     * Obtain the name of the current schema, so that the NetworkServer can
     * use it for piggy-backing
     * @return the current schema name
     * @throws java.sql.SQLException
     */
    public String getCurrentSchemaName() throws SQLException;

    /**
     * Resets the connection before it is returned from a PooledConnection
     * to a new application request (wrapped by a BrokeredConnection).
     * <p>
     * Note that resetting the transaction isolation level is not performed as
     * part of this method. Temporary tables, IDENTITY_VAL_LOCAL and current
     * schema are reset.
     */
    public void resetFromPool() throws SQLException;
}

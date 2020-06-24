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
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.Executor;


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
     *
     * @param drdaID The DRDA identifier
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
     * @throws SQLException on error
     */
    public void setPrepareIsolation(int level) throws SQLException;

    /**
     * Return prepare isolation
     *
     * @return the isolation level
     * 
     * @throws SQLException on error
     */
    public int getPrepareIsolation()
        throws SQLException;

    /**
     * Add a SQLWarning to this Connection object.
     * @param newWarning Warning to be added, will be chained to any
     * existing warnings.
     * 
     * @throws SQLException on error
     */
    public void addWarning(SQLWarning newWarning)
        throws SQLException;

    /**
    * Get the LOB reference corresponding to the locator.
    * @param key the integer that represents the LOB locator value.
    * @return the LOB Object corresponding to this locator.
    * 
    * @throws SQLException on error
    */
    public Object getLOBMapping(int key) throws SQLException;

    /**
     * Obtain the name of the current schema, so that the NetworkServer can
     * use it for piggy-backing
     * @return the current schema name
     * @throws java.sql.SQLException on error
     */
    public String getCurrentSchemaName() throws SQLException;

    /**
     * Resets the connection before it is returned from a PooledConnection
     * to a new application request (wrapped by a BrokeredConnection).
     * <p>
     * Note that resetting the transaction isolation level is not performed as
     * part of this method. Temporary tables, IDENTITY_VAL_LOCAL and current
     * schema are reset.
     * @throws java.sql.SQLException on error
     */
    public void resetFromPool() throws SQLException;

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Get the name of the current schema.
     *
     * @return the current schema name
     * @throws java.sql.SQLException on error
     */
    public String   getSchema() throws SQLException;

    /**
     * Set the default schema for the Connection.
     * @param schemaName The new default schema
     * @throws java.sql.SQLException on error
     */
    public void   setSchema(  String schemaName ) throws SQLException;

    void abort(Executor executor) throws SQLException;
//IC see: https://issues.apache.org/jira/browse/DERBY-1984

    void setNetworkTimeout(Executor executor, int millis) throws SQLException;

    int getNetworkTimeout() throws SQLException;
}

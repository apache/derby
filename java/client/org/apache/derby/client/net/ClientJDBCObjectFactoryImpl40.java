/*
 
   Derby - Class org.apache.derby.client.net.ClientJDBCObjectFactoryImpl40
 
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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.ClientPooledConnection40;
import org.apache.derby.client.ClientXAConnection;
import org.apache.derby.client.ClientXAConnection40;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.CachingLogicalConnection40;
import org.apache.derby.client.am.ClientCallableStatement;
import org.apache.derby.client.am.ClientCallableStatement40;
import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.LogicalCallableStatement;
import org.apache.derby.client.am.LogicalCallableStatement40;
import org.apache.derby.client.am.LogicalConnection;
import org.apache.derby.client.am.LogicalConnection40;
import org.apache.derby.client.am.LogicalPreparedStatement;
import org.apache.derby.client.am.LogicalPreparedStatement40;
import org.apache.derby.client.am.ClientPreparedStatement;
import org.apache.derby.client.am.ClientPreparedStatement40;
import org.apache.derby.client.am.ClientConnection;
import org.apache.derby.client.am.ClientDatabaseMetaData;
import org.apache.derby.client.am.MaterialStatement;
import org.apache.derby.client.am.ClientResultSet;
import org.apache.derby.client.am.SQLExceptionFactory40;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.StatementCacheInteractor;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.jdbc.ClientBaseDataSourceRoot;

/**
 * Implements the ClientJDBCObjectFactory interface and returns the JDBC 4.0
 * specific classes. If specific classes are not needed for JDBC 4.0, the calls
 * are delegated to ClientJDBCObjectFactoryImpl by inheritance.
 */
public class ClientJDBCObjectFactoryImpl40 extends ClientJDBCObjectFactoryImpl {
    
    /**
     * Sets SQLExceptionFactory40 to SqlException to make sure JDBC 4.0
     * exception and sub classes are thrown when running with JDBC 4.0 support.
     */
    public ClientJDBCObjectFactoryImpl40 () {
        SqlException.setExceptionFactory (new SQLExceptionFactory40 ());
    }
    /**
     * @return an instance of
     * {@link org.apache.derby.client.ClientPooledConnection40}
     */
    public ClientPooledConnection newClientPooledConnection(
            ClientBaseDataSourceRoot ds, LogWriter logWriter,String user,
            String password) throws SQLException {
        return new ClientPooledConnection40(ds,logWriter,user,password);
    }
    /**
     * @return an instance of
     * {@link org.apache.derby.client.ClientXAConnection40}
     */
    public ClientXAConnection newClientXAConnection(
        ClientBaseDataSourceRoot ds, LogWriter logWriter,String user,
        String password) throws SQLException
    {
        return new ClientXAConnection40(ds,
            (NetLogWriter)logWriter,user,password);
    }
    /**
     * Returns an instance of ClientCallableStatement.
     *
     * @param agent       The instance of NetAgent associated with this
     *                    CallableStatement object.
     * @param connection  The connection object associated with this
     *                    PreparedStatement Object.
     * @param sql         A String object that is the SQL statement to be sent 
     *                    to the database.
     * @param type        One of the ResultSet type constants
     * @param concurrency One of the ResultSet concurrency constants
     * @param holdability One of the ResultSet holdability constants
     * @param cpc         The PooledConnection object that will be used to 
     *                    notify the PooledConnection reference of the Error 
     *                    Occurred and the Close events.
     * @return a CallableStatement object
     * @throws SqlException
     */
    public ClientCallableStatement newCallableStatement(Agent agent,
            ClientConnection connection,
            String sql,int type,int concurrency,
            int holdability,ClientPooledConnection cpc) throws SqlException {

        return new ClientCallableStatement40(
            agent,
            connection,
            sql,
            type,
            concurrency,
            holdability,
            cpc);
    }

    /**
     * Returns an instance of LogicalConnection.
     * This method returns an instance of LogicalConnection
     * (or LogicalConnection40) which implements {@link java.sql.Connection}.
     */
    public LogicalConnection newLogicalConnection(
                    ClientConnection physicalConnection,
                    ClientPooledConnection pooledConnection)
        throws SqlException {
        return new LogicalConnection40(physicalConnection, pooledConnection);
    }
    
   /**
    * Returns an instance of a {@code CachingLogicalConnection}, which
    * provides caching of prepared statements.
    *
    * @param physicalConnection the underlying physical connection
    * @param pooledConnection the pooled connection
    * @param stmtCache statement cache
    * @return A logical connection with statement caching capabilities.
    *
    * @throws SqlException if creation of the logical connection fails
    */
    public LogicalConnection newCachingLogicalConnection(
            ClientConnection physicalConnection,
            ClientPooledConnection pooledConnection,
            JDBCStatementCache stmtCache) throws SqlException {
        return new CachingLogicalConnection40(physicalConnection,
                                              pooledConnection,
                                              stmtCache);
    }

    /**
     * Returns an instance of ClientCallableStatement40
     */
    public ClientPreparedStatement newPreparedStatement(Agent agent,
            ClientConnection connection,
            String sql,Section section,ClientPooledConnection cpc) 
            throws SqlException {
        return new ClientPreparedStatement40(agent,connection,sql,section,cpc);
    }
    
    /**
     *
     * This method returns an instance of ClientPreparedStatement
     * which implements {@code java.sql.PreparedStatement}.
     * It has the ClientPooledConnection as one of its parameters
     * this is used to raise the Statement Events when the prepared
     * statement is closed.
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection  The connection object associated with this
     *                    PreparedStatement Object.
     * @param sql         A String object that is the SQL statement
     *                    to be sent to the database.
     * @param type        One of the ResultSet type constants.
     * @param concurrency One of the ResultSet concurrency constants.
     * @param holdability One of the ResultSet holdability constants.
     * @param autoGeneratedKeys a flag indicating whether auto-generated
     *                          keys should be returned.
     * @param columnNames an array of column names indicating the columns that
     *                    should be returned from the inserted row or rows.
     * @param columnIndexes an array of column indexes indicating the columns
     *                  that should be returned from the inserted row.                   
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement
     *            it is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedStatement object
     * @throws SqlException
     *
     */
    public ClientPreparedStatement newPreparedStatement(Agent agent,
            ClientConnection connection,
            String sql,int type,int concurrency,
            int holdability,int autoGeneratedKeys,
            String [] columnNames,
            int[] columnIndexes, ClientPooledConnection cpc) 
            throws SqlException {

        return new ClientPreparedStatement40(
            agent,
            connection,
            sql,
            type,
            concurrency,
            holdability,
            autoGeneratedKeys,
            columnNames,
            columnIndexes,
            cpc);
    }

    /**
     * Returns a new logical prepared statement object.
     *
     * @param ps underlying physical prepared statement
     * @param stmtKey key for the underlying physical prepared statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical prepared statement.
     */
    public LogicalPreparedStatement newLogicalPreparedStatement(
            PreparedStatement ps,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor) {
        return new LogicalPreparedStatement40(ps, stmtKey, cacheInteractor);
    }

    /**
     * Returns a new logical callable statement object.
     *
     * @param cs underlying physical callable statement
     * @param stmtKey key for the underlying physical callable statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical callable statement.
     */
    public LogicalCallableStatement newLogicalCallableStatement(
            CallableStatement cs,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor) {
        return new LogicalCallableStatement40(cs, stmtKey, cacheInteractor);
    }

    /**
     * @return  an instance of
     * {@link org.apache.derby.client.net.NetConnection40}
     */
    public ClientConnection newNetConnection(
            LogWriter netLogWriter,
            int driverManagerLoginTimeout,
            String serverName,
            int portNumber,
            String databaseName,
            Properties properties) throws SqlException {

        return  new NetConnection40(
                (NetLogWriter)netLogWriter,
                driverManagerLoginTimeout,
                serverName,
                portNumber,
                databaseName,
                properties);
    }
    /**
     * @return an instance of
     * {@link org.apache.derby.client.net.NetConnection40}
     */
    public ClientConnection newNetConnection(
            LogWriter netLogWriter,
            String user,
            String password,
            ClientBaseDataSourceRoot dataSource,
            int rmId,
            boolean isXAConn) throws SqlException {

        return new NetConnection40(
                (NetLogWriter)netLogWriter,
                user,
                password,
                dataSource,
                rmId,
                isXAConn);
    }

    /**
     * Returns an instance of {@link NetConnection}.
     * @param netLogWriter placeholder for NetLogWriter object associated with this connection
     * @param user         user id for this connection
     * @param password     password for this connection
     * @param dataSource   The DataSource object passed from the PooledConnection 
     *                     object from which this constructor was called
     * @param rmId         The Resource Manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this 
     *                     NetConnection constructor was called. This is used
     *                     to pass StatementEvents back to the pooledConnection
     *                     object
     * @return a Connection object
     * @throws             SqlException
     */
    public ClientConnection newNetConnection(
            LogWriter netLogWriter,String user,
            String password,
            ClientBaseDataSourceRoot dataSource,
            int rmId,
            boolean isXAConn,
            ClientPooledConnection cpc) throws SqlException {

        return new NetConnection40(
                (NetLogWriter)netLogWriter,
                user,
                password,
                dataSource,
                rmId,
                isXAConn,
                cpc);
        
    }
    /**
     * @return an instance of {@link NetResultSet}
     */
    public ClientResultSet newNetResultSet(Agent netAgent,
            MaterialStatement netStatement,
            Cursor cursor,
            int qryprctyp,
            int sqlcsrhld,
            int qryattscr,
            int qryattsns,
            int qryattset,
            long qryinsid,
            int actualResultSetType,
            int actualResultSetConcurrency,
            int actualResultSetHoldability) throws SqlException {

        return new NetResultSet40((NetAgent)netAgent,(NetStatement)netStatement,
                cursor,
                qryprctyp, sqlcsrhld, qryattscr, qryattsns, qryattset, qryinsid,
                actualResultSetType,actualResultSetConcurrency,
                actualResultSetHoldability);
    }
    /**
     * @return an instance of {@link NetDatabaseMetaData}
     */
    public ClientDatabaseMetaData newNetDatabaseMetaData(Agent netAgent,
            ClientConnection netConnection) {
        return new NetDatabaseMetaData40((NetAgent)netAgent,
                (NetConnection)netConnection);
    }
}

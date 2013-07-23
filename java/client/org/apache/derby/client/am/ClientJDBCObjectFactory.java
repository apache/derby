/*
 
   Derby - Class org.apache.derby.client.am.ClientJDBCObjectFactory
 
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

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.ClientXAConnection;
import java.util.Properties;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.jdbc.ClientBaseDataSourceRoot;

/**
 *
 * The methods of this interface are used to return JDBC interface
 * implementations to the user depending on the JDBC version supported
 * by the JDK.
 *
 */

public interface ClientJDBCObjectFactory {
    
    /**
     * This method is used to return an instance of the {@link
     * org.apache.derby.client.ClientPooledConnection} class which
     * implements {@code javax.sql.PooledConnection}.
     */
    ClientPooledConnection newClientPooledConnection(
            ClientBaseDataSourceRoot ds,
            LogWriter logWriter,
            String user,
            String password) throws SQLException;
    
        /**
     * This method is used to return an instance of
     * ClientXAConnection (or ClientXAConnection40) class which
     * implements {@code javax.sql.XAConnection}.
     */
    ClientXAConnection newClientXAConnection(ClientBaseDataSourceRoot ds,
            LogWriter logWriter,String user,String password)
            throws SQLException;
    
    /**
     * Returns an instance of ClientCallableStatement,
     * ClientCallableStatement40 or ClientCallableStatement42 which
     * all implement java.sql.CallableStatement.
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
    ClientCallableStatement newCallableStatement(Agent agent,
            ClientConnection connection, String sql,
            int type,int concurrency,int holdability,
            ClientPooledConnection cpc) throws SqlException;
    
    /**
     * Returns an instance of LogicalConnection.
     * This method returns an instance of LogicalConnection
     * (or LogicalConnection40) which implements {@code java.sql.Connection}.
     */
    LogicalConnection newLogicalConnection(
                    ClientConnection physicalConnection,
                    ClientPooledConnection pooledConnection)
        throws SqlException;
    
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
            JDBCStatementCache stmtCache) throws SqlException;

    /**
     * This method returns an instance of PreparedStatement
     * (or PreparedStatement40) which implements
     * {@code java.sql.PreparedStatement}.
     * It has the {@link ClientPooledConnection} as one of its parameters
     * this is used to raise the Statement Events when the prepared
     * statement is closed
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection The connection object associated with this
     *                   PreparedStatement Object.
     * @param sql        A String object that is the SQL statement to be sent
     *                   to the database.
     * @param section    Section
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement.
     *            It is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedStatement object
     * @throws SqlException
     */
    ClientPreparedStatement newPreparedStatement(Agent agent,
            ClientConnection connection,
            String sql,Section section,ClientPooledConnection cpc) 
            throws SqlException;
    
    /**
     * Returns an instance of PreparedStatement (or
     * PreparedStatement40) which implements {@code
     * java.sql.PreparedStatement}.

     * It has the ClientPooledConnection as one of its parameters 
     * this is used to raise the Statement Events when the prepared
     * statement is closed
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
     *                    that should be returned form the inserted row.                   
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement
     *            it is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedSatement object
     * @throws SqlException
     */
    ClientPreparedStatement newPreparedStatement(Agent agent,
            ClientConnection connection,String sql,
            int type,int concurrency,int holdability,int autoGeneratedKeys,
            String [] columnNames, int[] columnIndexes, ClientPooledConnection cpc) 
            throws SqlException;
    
    
    /**
     * Returns a new logical prepared statement object.
     *
     * @param ps underlying physical prepared statement
     * @param stmtKey key for the underlying physical prepared statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical prepared statement.
     */
    LogicalPreparedStatement newLogicalPreparedStatement(
            PreparedStatement ps,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor);

    /**
     * Returns a new logical callable statement object.
     *
     * @param cs underlying physical callable statement
     * @param stmtKey key for the underlying physical callable statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical callable statement.
     */
    LogicalCallableStatement newLogicalCallableStatement(
            CallableStatement cs,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor);

    /**
     * This method returns an instance of NetConnection (or NetConnection40)
     * class which extends from ClientConnection
     * this implements the java.sql.Connection interface
     */
    ClientConnection newNetConnection(
            LogWriter logWriter,
            int driverManagerLoginTimeout,String serverName,
            int portNumber,String databaseName, Properties properties)
            throws SqlException;
    
    /**
     * This method returns an instance of NetConnection (or
     * NetConnection40) class which extends from ClientConnection.  This
     * implements the {@code java.sql.Connection} interface.
     */
    ClientConnection newNetConnection(
            LogWriter logWriter,
            String user,
            String password,
            ClientBaseDataSourceRoot dataSource,
            int rmId,
            boolean isXAConn) throws SqlException;
    
    /**
     * This method returns an instance of NetConnection (or NetConnection40)
     * class which extends Connection.
     * This implements the {@code java.sql.Connection} interface.
     * This method is used to pass the ClientPooledConnection
     * object to the NetConnection object which can then be used to pass the 
     * statement events back to the user
     *
     * @param logWriter    LogWriter object associated with this connection
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
     * @throws             SqlException
     */
    ClientConnection newNetConnection(
            LogWriter logWriter,
            String user,String password,
            ClientBaseDataSourceRoot dataSource,int rmId,
            boolean isXAConn,ClientPooledConnection cpc) throws SqlException;
    
    /**
     * This method returns an instance of NetResultSet(or
     * NetResultSet40) which extends from ClientResultSet which implements
     * {@code java.sql.ResultSet}.
     */
    ClientResultSet newNetResultSet(
            Agent netAgent,
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
            int actualResultSetHoldability) throws SqlException;
    
    /**
     * This method provides an instance of NetDatabaseMetaData (or
     * NetDatabaseMetaData40) which extends from ClientDatabaseMetaData
     * which implements {@code java.sql.DatabaseMetaData}.
     */
    ClientDatabaseMetaData newNetDatabaseMetaData(Agent netAgent,
            ClientConnection netConnection);
    
     /**
     * This method provides an instance of Statement or Statement40 
     * depending on the jdk version under use
     * @param  agent            Agent
     * @param  connection       Connection
     * @param  type             int
     * @param  concurrency      int
     * @param  holdability      int
     * @param autoGeneratedKeys int
     * @param columnNames       String[]
     * @param columnIndexes     int[]
     * @return a {@code java.sql.Statement} implementation
     * @throws SqlException
     *
     */
     ClientStatement newStatement(Agent agent,
                     ClientConnection connection, int type,
                     int concurrency, int holdability,
                     int autoGeneratedKeys, String[] columnNames,
                     int[] columnIndexes) 
                     throws SqlException;
     
    /**
     * Returns an instanceof ColumnMetaData or ColumnMetaData40 depending 
     * on the jdk version under use
     *
     * @param logWriter LogWriter
     * @return a ColumnMetaData implementation
     *
     */
    ColumnMetaData newColumnMetaData(LogWriter logWriter); 

    /**
     * Returns an instanceof ColumnMetaData or ColumnMetaData40 depending 
     * on the jdk version under use
     *
     * @param logWriter  LogWriter
     * @param upperBound int
     * @return a ColumnMetaData implementation
     *
     */
    ColumnMetaData newColumnMetaData(LogWriter logWriter, int upperBound);
    
    /**
     * 
     * returns an instance of ParameterMetaData or ParameterMetaData40 depending 
     * on the jdk version under use
     *
     * @param columnMetaData ColumnMetaData
     * @return a ParameterMetaData implementation
     *
     */
    ClientParameterMetaData newParameterMetaData(ColumnMetaData columnMetaData);
    
    /**
     * Creates a BatchUpdateException depending on the JVM level.
     */
    public BatchUpdateException newBatchUpdateException
        ( LogWriter logWriter, ClientMessageId msgid, Object[] args, long[] updateCounts, SqlException cause );
}

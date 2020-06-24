/*
 
   Derby - Class org.apache.derby.client.net.ClientJDBCObjectFactoryImpl
 
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

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.ClientXAConnection;
import org.apache.derby.client.am.CachingLogicalConnection;
import org.apache.derby.client.am.ClientCallableStatement;
import org.apache.derby.client.am.ClientJDBCObjectFactory;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.LogicalConnection;
import org.apache.derby.client.am.ClientParameterMetaData;
import org.apache.derby.client.am.ClientPreparedStatement;
import org.apache.derby.client.am.LogicalCallableStatement;
import org.apache.derby.client.am.LogicalPreparedStatement;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.ClientStatement;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.am.stmtcache.JDBCStatementCache;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.client.am.ColumnMetaData;
import org.apache.derby.client.am.ClientConnection;
import org.apache.derby.client.am.ClientDatabaseMetaData;
import org.apache.derby.client.am.MaterialStatement;
import org.apache.derby.client.am.ClientResultSet;
import org.apache.derby.client.am.StatementCacheInteractor;
import org.apache.derby.client.am.Utils;
import org.apache.derby.client.BasicClientDataSource;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.error.ExceptionUtil;

/**
 * Implements the the ClientJDBCObjectFactory interface and returns the classes
 * that implement the JDBC3.0/2.0 interfaces
 * For example, newCallableStatement would return ClientCallableStatement
 */

public class ClientJDBCObjectFactoryImpl implements ClientJDBCObjectFactory{
    /** 
     *  The message utility instance we use to find messages
     *  It's primed with the name of the client message bundle so that
     *  it knows to look there if the message isn't found in the
     *  shared message bundle.
     */
    private static final MessageUtil msgutil_ =
        SqlException.getMessageUtil();
//IC see: https://issues.apache.org/jira/browse/DERBY-6000

    /**
     * @return an instance of {@link
     * org.apache.derby.client.ClientPooledConnection}
     */
    public ClientPooledConnection newClientPooledConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            BasicClientDataSource ds,
            LogWriter logWriter,
            String user,
            String password) throws SQLException {

        return new ClientPooledConnection(ds,logWriter,user,password);
    }
    /**
     * @return an instance of {@link org.apache.derby.client.ClientXAConnection}
     */
    public ClientXAConnection newClientXAConnection(BasicClientDataSource ds,
        LogWriter logWriter,String user, String password) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1028
        return new ClientXAConnection(ds, logWriter, user, password);
    }
    /**
     * Returns an instance of ClientCallableStatement.
     *
     * @param agent       The instance of NetAgent associated with this
     *                    {@link org.apache.derby.client.am.ClientCallableStatement}
     *                    object.
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
     * @return a {@link org.apache.derby.client.am.ClientCallableStatement}
     *         object
     * @throws SqlException on error
     */
    public ClientCallableStatement newCallableStatement(Agent agent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection connection,
            String sql,int type,int concurrency,
            int holdability,ClientPooledConnection cpc) throws SqlException {
        return new ClientCallableStatement(agent,connection,sql,type,
                concurrency,holdability,cpc);
    }
   
    /**
     * @return an instance of {@link
     * org.apache.derby.client.am.LogicalConnection}
     */
    public LogicalConnection newLogicalConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                    ClientConnection physicalConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-1180
                    ClientPooledConnection pooledConnection)
        throws SqlException {
        return new LogicalConnection(physicalConnection, pooledConnection);
    }
    
   /**
    * Returns an instance of a {@link
    * org.apache.derby.client.am.CachingLogicalConnection}, which provides
    * caching of prepared statements.
    *
    * @param physicalConnection the underlying physical connection
    * @param pooledConnection the pooled connection
    * @param stmtCache statement cache
    * @return A logical connection with statement caching capabilities.
    *
    * @throws SqlException if creation of the logical connection fails
    */
    public LogicalConnection newCachingLogicalConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection physicalConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
            ClientPooledConnection pooledConnection,
            JDBCStatementCache stmtCache) throws SqlException {
        return new CachingLogicalConnection(physicalConnection,
                                            pooledConnection,
                                            stmtCache);
    }

    /**
     * This method returns an instance of ClientPreparedStatement
     * which implements java.sql.PreparedStatement. It has the
     * ClientPooledConnection as one of its parameters
     * this is used to raise the Statement Events when the prepared
     * statement is closed.
     *
     * @param agent The instance of NetAgent associated with this
     *              {@link org.apache.derby.client.am.ClientCallableStatement}
     *              object.
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
     * @throws SqlException on error
     */
    public ClientPreparedStatement newPreparedStatement(Agent agent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection connection,
            String sql,Section section,ClientPooledConnection cpc) 
            throws SqlException {
        return new ClientPreparedStatement(agent,connection,sql,section,cpc);
    }
    
    /**
     *
     * This method returns an instance of ClientPreparedStatement
     * which implements {@code java.sql.PreparedStatement}.
     * It has the {@link org.apache.derby.client.ClientPooledConnection} as one
     * of its parameters this is used to raise the Statement Events when the
     * prepared statement is closed.
     *
     * @param agent The instance of NetAgent associated with this
     *              {@link org.apache.derby.client.am.ClientCallableStatement}
     *              object.
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
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement
     *            it is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedStatement object
     * @throws SqlException on error
     *
     */
    public ClientPreparedStatement newPreparedStatement(Agent agent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection connection,
            String sql,int type,int concurrency,int holdability,
            int autoGeneratedKeys,String [] columnNames,
            int[] columnIndexes,
            ClientPooledConnection cpc)
            throws SqlException {

        return new ClientPreparedStatement(
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            PreparedStatement ps,
            StatementKey stmtKey,
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
            StatementCacheInteractor cacheInteractor) {
        return new LogicalPreparedStatement(ps, stmtKey, cacheInteractor);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            CallableStatement cs,
            StatementKey stmtKey,
//IC see: https://issues.apache.org/jira/browse/DERBY-3328
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
            StatementCacheInteractor cacheInteractor) {
        return new LogicalCallableStatement(cs, stmtKey, cacheInteractor);
    }

    /**
     * @return an instance of {@link org.apache.derby.client.net.NetConnection}
     */
    public ClientConnection newNetConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-1028
            LogWriter logWriter,
            int driverManagerLoginTimeout,
            String serverName,
            int portNumber,
            String databaseName,
            Properties properties) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

        return new NetConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-1028
                logWriter,
                driverManagerLoginTimeout,
                serverName,
                portNumber,
                databaseName,
                properties);
    }
    /**
     * @return an instance of {@link org.apache.derby.client.net.NetConnection}
     */
    public ClientConnection newNetConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-1028
            LogWriter logWriter, String user, String password,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            BasicClientDataSource dataSource,
            int rmId,
            boolean isXAConn) throws SqlException {

        return new NetConnection(
                logWriter,
                user,
                password,
                dataSource,
                rmId,
                isXAConn);
    }

    /**
     * Returns an instance of NetConnection.
     * @param logWriter    LogWriter object associated with this connection.
     * @param user         user id for this connection.
     * @param password     password for this connection.
     * @param dataSource   The DataSource object passed from the PooledConnection
     *                     object from which this constructor was called.
     * @param rmId         The Resource Manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this
     *                     NetConnection constructor was called. This is used to
     *                     pass StatementEvents back to the pooledConnection
     *                     object.
     * @return a {@link ClientConnection} object
     * @throws             SqlException on error
     */
    public ClientConnection newNetConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-1028
            LogWriter logWriter,String user,
            String password,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            BasicClientDataSource dataSource,
            int rmId,boolean isXAConn,
            ClientPooledConnection cpc) throws SqlException {

        return new NetConnection(
            logWriter,
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
            int qryprctyp,int sqlcsrhld,int qryattscr,int qryattsns,
            int qryattset,long qryinsid,int actualResultSetType,
            int actualResultSetConcurrency,
            int actualResultSetHoldability) throws SqlException {
        return new NetResultSet((NetAgent)netAgent,
                (NetStatement)netStatement,cursor,qryprctyp,sqlcsrhld,qryattscr,
                qryattsns,qryattset,qryinsid,actualResultSetType,
                actualResultSetConcurrency,actualResultSetHoldability);
    }
    /**
     * @return an instance of {@link NetDatabaseMetaData}
     */
    public ClientDatabaseMetaData newNetDatabaseMetaData(Agent netAgent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection netConnection) {
        return new NetDatabaseMetaData((NetAgent)netAgent,
                (NetConnection)netConnection);
    }
    
     /**
     * This method provides an instance of Statement 
     * @param  agent            Agent
     * @param  connection       Connection
     * @param  type             int
     * @param  concurrency      int
     * @param  holdability      int
     * @param autoGeneratedKeys int
     * @param columnNames       String[]
     * @param columnIndexes     int[]
     * @return a ClientStatement implementation
     * @throws SqlException on error
     *
     */
     public ClientStatement newStatement(Agent agent,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                     ClientConnection connection, int type,
                     int concurrency, int holdability,
                     int autoGeneratedKeys, String[] columnNames,
                     int[] columnIndexes) 
                     throws SqlException {
         return new ClientStatement(
             agent,
             connection,
             type,
             concurrency,
             holdability,
             autoGeneratedKeys,
             columnNames,
             columnIndexes);
     }
     
     /**
     * Returns an instance of ColumnMetaData
     *
     * @param logWriter LogWriter
     * @return a ColumnMetaData implementation
     *
     */
    public ColumnMetaData newColumnMetaData(LogWriter logWriter) {
        return new ColumnMetaData(logWriter);
    }

    /**
     * Returns an instance of ColumnMetaData or ColumnMetaData40 depending
     * on the JDK version under use
     *
     * @param logWriter  LogWriter
     * @param upperBound int
     * @return a ColumnMetaData implementation
     *
     */
    public ColumnMetaData newColumnMetaData(LogWriter logWriter, int upperBound) {
        return new ColumnMetaData(logWriter,upperBound);
    }
    
    /**
     * 
     * returns an instance of ParameterMetaData 
     *
     * @param columnMetaData ColumnMetaData
     * @return a ParameterMetaData implementation
     *
     */
    public ClientParameterMetaData newParameterMetaData(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ColumnMetaData columnMetaData) {
        return new ClientParameterMetaData(columnMetaData);
    }

    /**
     * Creates a BatchUpdateException depending on the JVM level.
     */
    public  BatchUpdateException    newBatchUpdateException
        ( LogWriter logWriter, ClientMessageId msgid, Object[] args, long[] updateCounts, SqlException cause )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        BatchUpdateException bue = newBatchUpdateException
            (
             msgutil_.getCompleteMessage( msgid.msgid, args),
             ExceptionUtil.getSQLStateFromIdentifier(msgid.msgid),
             ExceptionUtil.getSeverityFromIdentifier(msgid.msgid),
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
             updateCounts,
             cause
             );
    
        if (logWriter != null) {
            logWriter.traceDiagnosable( bue );
        }
    
        if (cause != null) {
            bue.setNextException(cause.getSQLException());
        }

        return bue;
    }
    /**
     * This method is overridden on JVM 8
     *
     * @param message The message to put in the exception
     * @param sqlState The SQLState to put in the exception
     * @param errorCode The errorCode to put in the exception
     * @param updateCounts The array of update counts
     * @param cause The original exception
     *
     * @return a batch update exception
     */
    protected   BatchUpdateException   newBatchUpdateException
        ( String message, String sqlState, int errorCode, long[] updateCounts, SqlException cause  )
    {
        BatchUpdateException bue = new BatchUpdateException
            ( message, sqlState, errorCode, Utils.squashLongs( updateCounts ) );

        if (cause != null) {
            bue.initCause(cause);
        }

        return bue;
    }
}

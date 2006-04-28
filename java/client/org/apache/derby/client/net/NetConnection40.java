/*
 
   Derby - Class org.apache.derby.client.net.NetConnection40
 
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

package org.apache.derby.client.net;

import java.sql.BaseQuery;
import java.sql.QueryObjectFactory;
import org.apache.derby.client.am.SQLExceptionFactory;
import org.apache.derby.client.am.SqlException;
import java.sql.Blob;
import java.sql.ClientInfoException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.client.am.MessageId;
import org.apache.derby.shared.common.reference.SQLState;

public class  NetConnection40 extends org.apache.derby.client.net.NetConnection {
    /**
     * Prepared statement that is used each time isValid() is called on this
     * connection. The statement is created the first time isValid is called
     * and closed when the connection is closed (by the close call).
     */
    private PreparedStatement isValidStmt = null;

    /*
     *-------------------------------------------------------
     * JDBC 4.0 
     *-------------------------------------------------------
    */

    public NetConnection40(NetLogWriter netLogWriter,
                         String databaseName,
                         java.util.Properties properties) throws SqlException {
	super(netLogWriter,databaseName,properties);
    }
    public NetConnection40(NetLogWriter netLogWriter,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         String user,
                         String password) throws SqlException {
	super(netLogWriter,dataSource,user,password);
    }
     public NetConnection40(NetLogWriter netLogWriter,
                         int driverManagerLoginTimeout,
                         String serverName,
                         int portNumber,
                         String databaseName,
                         java.util.Properties properties) throws SqlException{
	super(netLogWriter,driverManagerLoginTimeout,serverName,portNumber,databaseName,properties);
     }
     public NetConnection40(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         int rmId,
                         boolean isXAConn) throws SqlException{
	super(netLogWriter,user,password,dataSource,rmId,isXAConn);
    }
    public NetConnection40(NetLogWriter netLogWriter,
                         String ipaddr,
                         int portNumber,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         boolean isXAConn) throws SqlException{
        super(netLogWriter,ipaddr,portNumber,dataSource,isXAConn);
    }
    

    
    /**
     * Constructs an object that implements the Clob interface. The object
     * returned initially contains no data.
     * @return An object that implements the Clob interface
     * @throws SQLException if an object that implements the
     * Clob interface can not be constructed.
     *
     */
    
    public Clob createClob() throws SQLException {
        org.apache.derby.client.am.Clob clob = new org.apache.derby.client.am.Clob(this.agent_,"");
        return clob;
    }

    /**
     * Constructs an object that implements the Clob interface. The object
     * returned initially contains no data.
     * @return An object that implements the Clob interface
     * @throws SQLException if an object that implements the
     * Clob interface can not be constructed.
     *
     */
    
    public Blob createBlob() throws SQLException {
        org.apache.derby.client.am.Blob blob = new org.apache.derby.client.am.Blob(new byte[0],this.agent_, 0);
        return blob;
    }
    
    public NClob createNClob() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createNClob ()");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createSQLXML ()");
    }

    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by running a simple query against the 
     * database.
     *
     * The timeout specified by the caller is implemented as follows:
     * On the server: uses the queryTimeout functionality to make the
     * query time out on the server in case the server has problems or
     * is highly loaded.
     * On the client: uses a timeout on the socket to make sure that 
     * the client is not blocked forever in the cases where the server
     * is "hanging" or not sending the reply.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the 
     * timeout period expires before the operation completes, this 
     * method returns false. A value of 0 indicates a timeout is not 
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the parameter value is illegal or if a
     * database error has occured
     */
    public boolean isValid(int timeout) throws SQLException {
        // Validate that the timeout has a legal value
        if (timeout < 0) {
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,
                                              new Integer(timeout), "timeout",
                                              "java.sql.Connection.isValid");
        }

        // Check if the connection is closed
        if (isClosed()) {
            return false;
        }

        // Do a simple query against the database
        synchronized(this) {
            try {
                // Save the current network timeout value
                int oldTimeout = netAgent_.getTimeout();

                // Set the required timeout value on the network connection
                netAgent_.setTimeout(timeout);

                // If this is the first time this method is called on this 
                // connection we prepare the query 
                if (isValidStmt == null) {
                    isValidStmt = prepareStatement("VALUES (1)");
                }

                // Set the query timeout
                isValidStmt.setQueryTimeout(timeout);

                // Run the query against the database
                ResultSet rs = isValidStmt.executeQuery();
                rs.close();

                // Restore the previous timeout value
                netAgent_.setTimeout(oldTimeout);
            } catch(SQLException e) {
                // If an SQL exception is thrown the connection is not valid,
                // we ignore the exception and return false.
                return false;
            }
	 }

        return true;  // The connection is valid
    }

    /**
     * Close the connection and release its resources. 
     * @exception SQLException if a database-access error occurs.
     */
    synchronized public void close() throws SQLException {
        // Release resources owned by the prepared statement used by isValid
        if (isValidStmt != null) {
            isValidStmt.close();
            isValidStmt = null;
        }
        super.close();
    }
   
    public void setClientInfo(String name, String value)
		throws SQLException{
	throw SQLExceptionFactory.notImplemented ("setClientInfo (String, String)");
    }
	
    public void setClientInfo(Properties properties)
		throws ClientInfoException {
	SQLException temp= SQLExceptionFactory.notImplemented ("setClientInfo ()");
	ClientInfoException clientInfoException = new ClientInfoException
	(temp.getMessage(),temp.getSQLState(),(Properties) null);
	throw clientInfoException; 
    }
	
    public String getClientInfo(String name)
		throws SQLException{
	throw SQLExceptionFactory.notImplemented ("getClientInfo (String)");
    }
	
    public Properties getClientInfo()
		throws SQLException{
	throw SQLExceptionFactory.notImplemented ("getClientInfo (Properties)");
    }
    
    /**
     * This method forwards all the calls to default query object provided by 
     * the jdk.
     * @param ifc interface to generated concreate class
     * @return concreat class generated by default qury object generator
     */
    public <T extends BaseQuery> T createQueryObject(Class<T> ifc) 
                                                    throws SQLException {
        return QueryObjectFactory.createDefaultQueryObject (ifc, this);
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
            throw new SqlException(null,new MessageId(SQLState.UNABLE_TO_UNWRAP),
                    interfaces).getSQLException();
        }
    }
    
}

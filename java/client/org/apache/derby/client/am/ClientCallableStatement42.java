/*
 
   Derby - Class org.apache.derby.client.am.CallableStatement42
 
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

import java.sql.SQLException;
import java.sql.SQLType;
import org.apache.derby.client.ClientPooledConnection;


public class ClientCallableStatement42 extends ClientCallableStatement
{    
    /**
     * Calls the superclass constructor and passes the parameters
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
     * @throws SqlException
     */
    public ClientCallableStatement42(Agent agent,
        ClientConnection connection,
        String sql,
        int type, int concurrency, int holdability,
        ClientPooledConnection cpc) throws SqlException {
        super(agent, connection, sql, type, concurrency, holdability,cpc);        
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType );
            }
            
            checkStatus();
            registerOutParameter( parameterIndex, Utils42.getTypeAsInt( agent_, sqlType ) );
        }
    }

    public  void registerOutParameter( int parameterIndex, SQLType sqlType, int scale )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType, scale );
            }
            
            checkStatus();
            registerOutParameter( parameterIndex, Utils42.getTypeAsInt( agent_, sqlType ), scale );
        }
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, String typeName )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType, typeName );
            }
            
            checkStatus();
            registerOutParameter( parameterIndex, Utils42.getTypeAsInt(agent_, sqlType ), typeName );
        }
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterName, sqlType );
            }
            
            checkStatus();
            registerOutParameter( parameterName, Utils42.getTypeAsInt(agent_, sqlType ) );
        }
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType, int scale )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterName, sqlType, scale );
            }
            
            checkStatus();
            registerOutParameter( parameterName, Utils42.getTypeAsInt(agent_, sqlType ), scale );
        }
    }
    
    public  void registerOutParameter( String parameterName,  SQLType sqlType, String typeName )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterName, sqlType, typeName );
            }
            
            checkStatus();
            registerOutParameter( parameterName, Utils42.getTypeAsInt(agent_, sqlType ), typeName );
        }
    }

    public  void setObject
        ( int parameterIndex, Object x, SQLType sqlType )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "setObject", parameterIndex, x, sqlType );
            }
            
            checkStatus();
            setObject( parameterIndex, x, Utils42.getTypeAsInt(agent_, sqlType ) );
        }
    }
    
    public void setObject
        ( int parameterIndex, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "setObject", new Integer( parameterIndex ), x, sqlType, new Integer( scaleOrLength ) );
            }
            
            checkStatus();
            setObject( parameterIndex, x, Utils42.getTypeAsInt(agent_, sqlType ), scaleOrLength );
        }
    }

    public  void setObject( String parameterName, Object x, SQLType sqlType )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "setObject", parameterName, x, sqlType );
            }
            
            checkStatus();
            setObject( parameterName, x, Utils42.getTypeAsInt(agent_, sqlType ) );
        }
    }
    
    public  void setObject( String parameterName, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        synchronized (connection_) {
            if (agent_.loggingEnabled()) {
                agent_.logWriter_.traceEntry( this, "setObject", parameterName, x, sqlType, scaleOrLength );
            }
            
            checkStatus();
            setObject( parameterName, x, Utils42.getTypeAsInt(agent_, sqlType ), scaleOrLength );
        }
    }

}

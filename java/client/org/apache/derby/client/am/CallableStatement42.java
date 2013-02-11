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
import org.apache.derby.shared.common.reference.SQLState;


public class CallableStatement42 extends CallableStatement40
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
    public CallableStatement42(Agent agent,
        Connection connection,
        String sql,
        int type, int concurrency, int holdability,
        ClientPooledConnection cpc) throws SqlException {
        super(agent, connection, sql, type, concurrency, holdability,cpc);        
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType )
        throws SQLException
    {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType );
                }
                checkForClosedStatement();
                registerOutParameter( parameterIndex, Utils42.getTypeAsInt( agent_, sqlType ) );
            }
        }
        catch ( SqlException se )  { throw se.getSQLException(); }
    }

    public  void registerOutParameter( int parameterIndex, SQLType sqlType, int scale )
        throws SQLException
    {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType, scale );
                }
                checkForClosedStatement();
                registerOutParameter( parameterIndex, Utils42.getTypeAsInt( agent_, sqlType ), scale );
            }
        }
        catch ( SqlException se )  { throw se.getSQLException(); }
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, String typeName )
        throws SQLException
    {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry( this, "registerOutParameter", parameterIndex, sqlType, typeName );
                }
                checkForClosedStatement();
                registerOutParameter( parameterIndex, Utils42.getTypeAsInt(agent_, sqlType ), typeName );
            }
        }
        catch ( SqlException se )  { throw se.getSQLException(); }
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType )
        throws SQLException
    {
        throw jdbcMethodNotImplemented();
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType, int scale )
        throws SQLException
    {
        throw jdbcMethodNotImplemented();
    }
    
    public  void registerOutParameter( String parameterName,  SQLType sqlType, String typeName )
        throws SQLException
    {
        throw jdbcMethodNotImplemented();
    }

}

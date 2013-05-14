/*

   Derby - Class org.apache.derby.client.am.LogicalCallableStatement42

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

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import org.apache.derby.client.am.stmtcache.StatementKey;

/**
 * JDBC 4 specific wrapper class for a Derby physical callable statement.
 *
 * @see LogicalCallableStatement
 * @see #isClosed
 */
public class LogicalCallableStatement42 extends LogicalCallableStatement
{
    /**
     * Creates a new logical callable statement.
     *
     * @param physicalCs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalCallableStatement42(CallableStatement physicalCs,
                                      StatementKey stmtKey,
                                      StatementCacheInteractor cacheInteractor){
        super(physicalCs, stmtKey, cacheInteractor);
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.2 IN JAVA 8
    //
    ////////////////////////////////////////////////////////////////////
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterIndex, sqlType );
    }

    public  void registerOutParameter( int parameterIndex, SQLType sqlType, int scale )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterIndex, sqlType, scale );
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, String typeName )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterIndex, sqlType, typeName );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterName, sqlType );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType, int scale )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterName, sqlType, scale );
    }
    
    public  void registerOutParameter( String parameterName,  SQLType sqlType, String typeName )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            registerOutParameter( parameterName, sqlType, typeName );
    }

    public  void setObject
        ( int parameterIndex, Object x, SQLType sqlType )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            setObject( parameterIndex, x, sqlType );
    }
    
    public void setObject
        ( int parameterIndex, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            setObject( parameterIndex, x, sqlType, scaleOrLength );
    }

    public  void setObject( String parameterName, Object x, SQLType sqlType )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            setObject( parameterName, x, sqlType );
    }
    
    public  void setObject( String parameterName, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        ((ClientCallableStatement42)getPhysCs()).
            setObject( parameterName, x, sqlType, scaleOrLength );
    }
}

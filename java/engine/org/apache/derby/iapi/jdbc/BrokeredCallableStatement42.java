/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredCallableStatement42
 
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

import java.sql.SQLException;
import java.sql.SQLType;

public class BrokeredCallableStatement42 extends BrokeredCallableStatement
{
    public BrokeredCallableStatement42( BrokeredStatementControl control, String sql )
        throws SQLException
    {
        super(control, sql);
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType );
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, int scale )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType, scale );
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, String typeName )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType, typeName );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterName, sqlType );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType, int scale )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterName, sqlType, scale );
    }
    
    public  void registerOutParameter( String parameterName,  SQLType sqlType, String typeName )
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterName, sqlType, typeName );
    }
    
    public  void setObject
        ( int parameterIndex, java.lang.Object x, SQLType sqlType )
        throws SQLException
    {
        getCallableStatement().setObject( parameterIndex, x, sqlType );
    }
    
    public void setObject
        ( int parameterIndex, java.lang.Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        getCallableStatement().setObject( parameterIndex, x, sqlType, scaleOrLength );
    }

    public  void setObject( String parameterName, Object x, SQLType sqlType )
        throws SQLException
    {
        getCallableStatement().setObject( parameterName, x, sqlType );
    }
    
    public  void setObject( String parameterName, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        getCallableStatement().setObject( parameterName, x, sqlType, scaleOrLength );
    }

}

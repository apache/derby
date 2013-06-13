/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedCallableStatement42

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

package org.apache.derby.impl.jdbc;

import java.sql.SQLException;
import java.sql.SQLType;

public class EmbedCallableStatement42 extends EmbedCallableStatement
{
        
    /** Creates a new instance of EmbedCallableStatement42 */
    public EmbedCallableStatement42(EmbedConnection conn, String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException{
        super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);     
    }

    public  void registerOutParameter( int parameterIndex, SQLType sqlType )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterIndex, Util42.getTypeAsInt( sqlType ) );
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, int scale )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterIndex, Util42.getTypeAsInt( sqlType ), scale );
    }
    
    public  void registerOutParameter( int parameterIndex, SQLType sqlType, String typeName )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterIndex, Util42.getTypeAsInt( sqlType ), typeName );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterName, Util42.getTypeAsInt( sqlType ) );
    }
    
    public  void registerOutParameter( String parameterName, SQLType sqlType, int scale )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterName, Util42.getTypeAsInt( sqlType ), scale );
    }
    
    public  void registerOutParameter( String parameterName,  SQLType sqlType, String typeName )
        throws SQLException
    {
        checkStatus();
        registerOutParameter( parameterName, Util42.getTypeAsInt( sqlType ), typeName );
    }

    public  void setObject
        ( int parameterIndex, java.lang.Object x, SQLType targetSqlType )
        throws SQLException
    {
        checkStatus();
        setObject( parameterIndex, x, Util42.getTypeAsInt( targetSqlType ) );
    }
    
    public void setObject
        (
         int parameterIndex, java.lang.Object x,
         SQLType targetSqlType, int scaleOrLength
         )  throws SQLException
    {
        checkStatus();
        setObject( parameterIndex, x, Util42.getTypeAsInt( targetSqlType ), scaleOrLength );
    }

    public  void setObject( String parameterName, Object x, SQLType sqlType )
        throws SQLException
    {
        checkStatus();
        setObject( parameterName, x, Util42.getTypeAsInt( sqlType ) );
    }
    
    public  void setObject( String parameterName, Object x, SQLType sqlType, int scaleOrLength )
        throws SQLException
    {
        checkStatus();
        setObject( parameterName, x, Util42.getTypeAsInt( sqlType ), scaleOrLength );
    }
    
}

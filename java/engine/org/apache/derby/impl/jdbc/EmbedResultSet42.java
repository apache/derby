/*
 
   Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet42
 
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

import org.apache.derby.iapi.sql.ResultSet;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;

import org.apache.derby.iapi.reference.SQLState;

/**
 * JDBC 4.2 specific methods that cannot be implemented in superclasses.
 */
public class EmbedResultSet42 extends org.apache.derby.impl.jdbc.EmbedResultSet40
{
    
    /** Creates a new instance of EmbedResultSet42 */
    public EmbedResultSet42(org.apache.derby.impl.jdbc.EmbedConnection conn,
        ResultSet resultsToWrap,
        boolean forMetaData,
        org.apache.derby.impl.jdbc.EmbedStatement stmt,
        boolean isAtomic)
        throws SQLException {
        
        super(conn, resultsToWrap, forMetaData, stmt, isAtomic);
    }
    
    public void updateObject
        ( int columnIndex, Object x, SQLType targetSqlType )
        throws SQLException
    {
        checkIfClosed("updateObject");
        updateObject( columnIndex, x, Util42.getTypeAsInt( this, targetSqlType ) );
    }

    public void updateObject
        ( int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength )
        throws SQLException
    {
        checkIfClosed("updateObject");
        updateObject( columnIndex, x, Util42.getTypeAsInt( this, targetSqlType ) );
        adjustScale( columnIndex, scaleOrLength );
    }

    public void updateObject
        ( String columnName, Object x, SQLType targetSqlType )
        throws SQLException
    {
        checkIfClosed("updateObject");
        updateObject( columnName, x, Util42.getTypeAsInt( this, targetSqlType ) );
    }

    public void updateObject
        ( String columnName, Object x, SQLType targetSqlType, int scaleOrLength )
        throws SQLException
    {
        checkIfClosed("updateObject");
        updateObject( findColumnName( columnName ), x, targetSqlType, scaleOrLength );
    }
    
}

/*

   Derby - Class org.apache.derby.client.am.LogicalPreparedStatement42

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;

import org.apache.derby.client.am.stmtcache.StatementKey;

/**
 * JDBC 4.2 specific wrapper class for a Derby physical prepared statement.
 */
public class LogicalPreparedStatement42 extends LogicalPreparedStatement
{
    /**
     * Creates a new logical prepared statement.
     *
     * @param physicalPs underlying physical statement
     * @param stmtKey key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    public LogicalPreparedStatement42 (
            PreparedStatement physicalPs,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor ) {

        super(physicalPs, stmtKey, cacheInteractor);
    }

    public  void setObject
        ( int parameterIndex, Object x, SQLType targetSqlType )
        throws SQLException
    {
        getPhysPs().setObject( parameterIndex, x, targetSqlType );
    }
    
    public void setObject
        (
         int parameterIndex, Object x,
         SQLType targetSqlType, int scaleOrLength
         )  throws SQLException
    {
        getPhysPs().setObject( parameterIndex, x, targetSqlType, scaleOrLength );
    }

}

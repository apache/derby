/*

   Derby - Class org.apache.derby.client.net.NetResultSet42

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

import java.sql.SQLException;
import java.sql.SQLType;
import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Utils42;


//IC see: https://issues.apache.org/jira/browse/DERBY-6213
class NetResultSet42 extends NetResultSet
{
    
    NetResultSet42(NetAgent netAgent,
        NetStatement netStatement,
        Cursor cursor,
        int qryprctyp,  //protocolType, CodePoint.FIXROWPRC | 
                        //              CodePoint.LMTBLKPRC
        int sqlcsrhld, // holdOption, 0xF0 for false (default) | 0xF1 for true.
        int qryattscr, // scrollOption, 0xF0 for false (default) | 0xF1 for true.
        int qryattsns, // sensitivity, CodePoint.QRYUNK | 
                       //              CodePoint.QRYINS | 
                       //              CodePoint.QRYSNSSTC
        int qryattset, // rowsetCursor, 0xF0 for false (default) | 0xF1 for true.
        long qryinsid, // instanceIdentifier, 0 (if not returned, check default) or number
        int actualResultSetType,
        int actualResultSetConcurrency,
        int actualResultSetHoldability) //throws DisconnectException
    {
        super(netAgent, netStatement, cursor, qryprctyp, sqlcsrhld, qryattscr,
              qryattsns, qryattset, qryinsid, actualResultSetType,
              actualResultSetConcurrency, actualResultSetHoldability);
    }
    
    public void updateObject
        ( int columnIndex, Object x, SQLType targetSqlType )
        throws SQLException
    {
        checkClosed( "updateObject" );
        updateObject( columnIndex, x, Utils42.getTypeAsInt( this.agent_, targetSqlType ) );
    }

    public void updateObject
        ( int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength )
        throws SQLException
    {
        checkClosed( "updateObject" );
        updateObject( columnIndex, x, Utils42.getTypeAsInt( this.agent_, targetSqlType ) );
        // the client driver doesn't seem to adjust the scale, unlike the embedded driver
    }

    public void updateObject
        ( String columnName, Object x, SQLType targetSqlType )
        throws SQLException
    {
        checkClosed( "updateObject" );
        updateObject( columnName, x, Utils42.getTypeAsInt( this.agent_, targetSqlType ) );
    }

    public void updateObject
        ( String columnName, Object x, SQLType targetSqlType, int scaleOrLength )
        throws SQLException
    {
        checkClosed( "updateObject" );
        try {
            updateObject( findColumnX( columnName, "updateObject" ), x, targetSqlType, scaleOrLength );
        } catch (SqlException se) { throw se.getSQLException(); }
    }

    private void    checkClosed( String methodName )
        throws SQLException
    {
        try { checkForClosedResultSet( methodName ); } catch (SqlException se)
        { throw se.getSQLException(); }
    }
    
}

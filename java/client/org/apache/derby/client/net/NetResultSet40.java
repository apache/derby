/*

   Derby - Class org.apache.derby.client.net.NetResultSet40

   Copyright (c) 2005 The Apache Software Foundation or its licensors, where applicable.

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

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.client.am.Cursor;

public class NetResultSet40 extends NetResultSet{
    
    public NetResultSet40(NetAgent netAgent,
        NetStatement netStatement,
        Cursor cursor,
        //int qryprctyp,  //protocolType, CodePoint.FIXROWPRC | CodePoint.LMTBLKPRC
        int sqlcsrhld, // holdOption, 0xF0 for false (default) | 0xF1 for true.
        int qryattscr, // scrollOption, 0xF0 for false (default) | 0xF1 for true.
        int qryattsns, // sensitivity, CodePoint.QRYUNK | CodePoint.QRYINS
        int qryattset, // rowsetCursor, 0xF0 for false (default) | 0xF1 for true.
        long qryinsid, // instanceIdentifier, 0 (if not returned, check default) or number
        int actualResultSetType,
        int actualResultSetConcurrency,
        int actualResultSetHoldability) //throws DisconnectException
    {
        super(netAgent,netStatement,cursor,sqlcsrhld,qryattscr,qryattsns,qryattset,qryinsid,actualResultSetType,actualResultSetConcurrency,actualResultSetHoldability);
    }
    
    public RowId getRowId(int columnIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    public RowId getRowId(String columnName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw Util.notImplemented();
    }
    
    public int getHoldability() throws SQLException {
        throw Util.notImplemented();
    }
    
    public boolean isClosed() throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNString(String columnName, String nString) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob getNClob(int i) throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob getNClob(String colName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML getSQLXML(String colName) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();
    }
    
    public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();
    }
    
}

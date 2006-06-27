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
import org.apache.derby.client.am.SQLExceptionFactory;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;


public class NetResultSet40 extends NetResultSet{
    
    NetResultSet40(NetAgent netAgent,
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
    
    public Reader getNCharacterStream(int columnIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNCharacterStream(int)");
    }

    public Reader getNCharacterStream(String columnName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNCharacterStream(String)");
    }

    public String getNString(int columnIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(int)");
    }

    public String getNString(String columnName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(String)");
    }
    
    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (int)");
    }
    
    
    public RowId getRowId(String columnName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (String)");
    }
    
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateRowId (int, RowId)");
    }
    
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateRowId (String, RowId)");
    }
    
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNString (int, String)");
    }
    
    public void updateNString(String columnName, String nString) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNString (String, String)");
    }
    
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "updateNCharacterStream(int,Reader,long)");
    }
    
    public void updateNCharacterStream(String columnName, Reader x, long length)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "updateNCharacterStream(String,Reader,long)");
    }
    
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNClob (int, NClob)");
    }
    
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateNClob (String, NClob)");
    }
    
    public NClob getNClob(int i) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getNClob (int)");
    }
    
    public NClob getNClob(String colName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getNClob (String)");
    }
    
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (int)");
    }
    
    public SQLXML getSQLXML(String colName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (String)");
    }
    
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateSQLXML (int, SQLXML)");
    }
    
    public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("updateSQLXML (String, SQLXML)");
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
        try {
            checkForClosedResultSet();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
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
            checkForClosedResultSet();
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                interfaces).getSQLException();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }
    
    /**
     * Updates the designated column with a java.sql.Blob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnIndex -
     *            the first column is 1, the second is 2
     * @param x -
     *            the new column value
     * @param length -
     *            the length of the Blob datatype
     * @exception SQLException
     *
     */
    public void updateBlob(int columnIndex, InputStream x, long length)
                            throws SQLException {
        throw SQLExceptionFactory.notImplemented("updateBlob(int,InputStream,long)");
    }

    /**
     * Updates the designated column with a java.sql.Blob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the column to be updated
     * @param x -
     *            the new column value
     * @param length -
     *            the length of the Blob datatype
     * @exception SQLException
     *
     */

    public void updateBlob(String columnName, InputStream x, long length)
                           throws SQLException {
        throw SQLExceptionFactory.notImplemented("updateBlob(String,inputStream,long)");
    }

    /**
     * Updates the designated column with a java.sql.Clob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnIndex -
     *            the first column is 1, the second is 2
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */
    public void updateClob(int columnIndex, Reader x, long length)
                throws SQLException {
        throw SQLExceptionFactory.notImplemented("updateClob(int,Reader,long)");
    }

    /**
     * Updates the designated column with a java.sql.Clob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateClob(String columnName, InputStream x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateClob(String,InputStream,long)");
     }

     /**
     * Updates the designated column with a java.sql.Clob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateClob(String columnName, Reader x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateClob(String,Reader,long)");
     }

     /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnIndex -
     *            the first column is 1, the second is 2
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */
    public void updateNClob(int columnIndex, Reader x, long length)
                throws SQLException {
        throw SQLExceptionFactory.notImplemented("updateNClob(int,Reader,long)");
    }

    /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateNClob(String columnName, InputStream x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateNClob(String,InputStream,long)");
     }

     /**
     * Updates the designated column with a java.sql.NClob value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the updateRow or insertRow methods are called to update the database.
     *
     * @param columnName -
     *            the name of the Clob column
     * @param x -
     *            the new column value
     * @exception SQLException
     *                Feature not implemented for now.
     */

     public void updateNClob(String columnName, Reader x, long length)
                           throws SQLException {
         throw SQLExceptionFactory.notImplemented("updateNClob(String,Reader,long)");
     }
}

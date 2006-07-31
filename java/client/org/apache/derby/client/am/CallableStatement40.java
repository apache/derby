/*
 
   Derby - Class org.apache.derby.client.am.CallableStatement40
 
   Copyright 2005, 2006 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.client.am;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.SQLState;


public class CallableStatement40 extends org.apache.derby.client.am.CallableStatement {       
    
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
    public CallableStatement40(Agent agent,
        Connection connection,
        String sql,
        int type, int concurrency, int holdability,
        ClientPooledConnection cpc) throws SqlException {
        super(agent, connection, sql, type, concurrency, holdability,cpc);        
    }
    
    public Reader getCharacterStream(String parameterName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getCharacterStream(String)");
    }

    public Reader getNCharacterStream(int parameterIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNCharacterStream(int)");
    }
    
    public Reader getNCharacterStream(String parameterName)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "getNCharacterStream(String)");
    }

    public String getNString(int parameterIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(int)");
    }

    public String getNString(String parameterIndex)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("getNString(String)");
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (int)");
    }
    
    public RowId getRowId(String parameterName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getRowId (String)");
    }
    
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setRowId (String, RowId)");
    }
    
    public void setBlob(String parameterName, Blob x)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("setBlob(String, Blob)");
    }
    
    public void setClob(String parameterName, Clob x)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented("setClob(String, Clob)");
    }
    
    public void setNString(String parameterName, String value)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNString (String, String)");
    }
    
    public void setNCharacterStream(String parameterName, Reader value, long length)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented (
                "setNString (String, Reader, long)");
    }
    
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (String, NClob)");
    }
    
    public void setClob(String parameterName, Reader reader, long length)
    throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setClob (String, Reader, long)");
        
    }
    
    public void setBlob(String parameterName, InputStream inputStream, long length)
    throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setBlob (String, InputStream, long)");
    }
    
    public void setNClob(String parameterName, Reader reader, long length)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (String, Reader, long)");
    }
    
    public NClob getNClob(int i) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (int)");
    }
    
    
    public NClob getNClob(String parameterName) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (String)");
    }
    
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setSQLXML (String, SQLXML)");
        
    }
    
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (int)");
    }
    
    public SQLXML getSQLXML(String parametername) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("getSQLXML (String)");
    }
    
    public void setRowId(int parameterIndex, RowId x) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setRowId (int, RowId)");
    }
    
    /**************************************************************************
     * The methods from PreparedStatement for JDBC 4.0.                       *
     * These are added here because we can't inherit                          *
     * PreparedStatement40.java. Instead of moving the non-implemented        *
     * classes to PreparedStatement.java, we duplicate them here.             *
     **************************************************************************/
    public void setNString(int index, String value) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNString (int, String)");
    }
    
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNCharacterStream" +
                "(int,Reader)");
    }

    public void setNCharacterStream(int index, Reader value, long length) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNCharacterStream " +
                "(int,Reader,long)");
    }
    
    public void setNClob(int index, NClob value) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (int, NClob)");
    }
    
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob(int,Reader)");
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setNClob (int, " +
                "Reader, long)");
    }
    
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setSQLXML (int, SQLXML)");
    }

    /**************************************************************************
     * End of methods from PreparedStatement for JDBC 4.0.                    *
     **************************************************************************/

    public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setAsciiStream(String,InputStream)");
    }

    public void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setBinaryStream(String,InputStream)");
    }

    public void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setBlob(String,InputStream)");
    }

    public void setCharacterStream(String parameterName, Reader reader)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setCharacterStream(String,Reader)");
    }

    public void setClob(String parameterName, Reader reader)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setClob(String,Reader)");
    }

    public void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented(
                "setNCharacterStream(String,Reader)");
    }

    public void setNClob(String parameterName, Reader reader)
            throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob(String,Reader)");
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
            checkForClosedStatement();
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null, new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                    interfaces).getSQLException();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }


    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setAsciiStream(String parameterName, InputStream x, long length)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setAsciiStream(String,InputStream,long)");
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setBinaryStream(String parameterName, InputStream x, long length)
    throws SQLException {
        throw SQLExceptionFactory.notImplemented ("setBinaryStream(String,InputStream,long)");
    }

    /**
     * Sets the designated parameter to the given Reader, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */

    public final void setCharacterStream(String parameterName, Reader x, long length)
    throws SQLException {
       throw SQLExceptionFactory.notImplemented ("setCharacterStream(String,Reader,long)");
    }
}

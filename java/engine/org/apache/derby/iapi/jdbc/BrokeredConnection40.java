/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection40
 
   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.iapi.jdbc;

import java.sql.Array;
import java.sql.BaseQuery;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ClientInfoException;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Properties;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;


public class BrokeredConnection40 extends BrokeredConnection30 {
    
    /** Creates a new instance of BrokeredConnection40 */
    public BrokeredConnection40(BrokeredConnectionControl control) {
        super(control);
    }
    
    public Array createArray(String typeName, Object[] elements)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    /**
     *
     * Constructs an object that implements the <code>Blob</code> interface. The object
     * returned initially contains no data.  The <code>setBinaryStream</code> and
     * <code>setBytes</code> methods of the <code>Blob</code> interface may be used to add data to
     * the <code>Blob</code>.
     *
     * @return  An object that implements the <code>Blob</code> interface
     * @throws SQLException if an object that implements the
     * <code>Blob</code> interface can not be constructed, this method is 
     * called on a closed connection or a database access error occurs.
     *
     */
    public Blob createBlob() throws SQLException {
        if (isClosed()) {
            throw Util.noCurrentConnection();
        }
        // Forward the createBlob call to the physical connection
        try {
            return getRealConnection().createBlob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     *
     * Constructs an object that implements the <code>Clob</code> interface. The object
     * returned initially contains no data.  The <code>setAsciiStream</code>,
     * <code>setCharacterStream</code> and <code>setString</code> methods of 
     * the <code>Clob</code> interface may be used to add data to the <code>Clob</code>.
     *
     * @return An object that implements the <code>Clob</code> interface
     * @throws SQLException if an object that implements the
     * <code>Clob</code> interface can not be constructed, this method is 
     * called on a closed connection or a database access error occurs.
     *
     */
    public Clob createClob() throws SQLException{
        if (isClosed()) {
            throw Util.noCurrentConnection();
        }
        // Forward the createClob call to the physical connection
        try {
            return getRealConnection().createClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    
    public NClob createNClob() throws SQLException{
        throw Util.notImplemented();
    }
    
    public SQLXML createSQLXML() throws SQLException{
        throw Util.notImplemented();
    }
    
    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw Util.notImplemented();
    }


    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by running a simple query against the 
     * database.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the 
     * timeout period expires before the operation completes, this 
     * method returns false. A value of 0 indicates a timeout is not 
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the call on the physical connection throws an
     * exception.
     */
    public final boolean isValid(int timeout) throws SQLException{
        // Check first if the Brokered connection is closed
        if (isClosed()) {
            return false;
        }

        // Forward the isValid call to the physical connection
        try {
            return getRealConnection().isValid(timeout);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    
    public void setClientInfo(String name, String value)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public void setClientInfo(Properties properties)
    throws ClientInfoException{
        SQLException temp= Util.notImplemented();
        ClientInfoException clientInfoException = new ClientInfoException
            (temp.getMessage(),temp.getSQLState(),(Properties) null);
        throw clientInfoException;
    }
    
    public String getClientInfo(String name)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public Properties getClientInfo()
    throws SQLException{
        throw Util.notImplemented();
    }
    
    /**
     *
     * This method forwards all the calls to default query object provided by 
     * the jdk.
     * @param ifc interface to generated concreate class
     * @return concrete class generated by default qury object generator
     *
     */
    public <T extends BaseQuery>T createQueryObject(Class<T> ifc) 
        throws SQLException {
        if (isClosed()) {
            throw Util.noCurrentConnection();
        }
        try {
            return getRealConnection().createQueryObject(ifc);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    /**
     * returns an instance of JDBC4.0 speccific class BrokeredStatement40
     * @param  statementControl BrokeredStatementControl
     * @return an instance of BrokeredStatement40 
     * throws java.sql.SQLException
     */
    public BrokeredStatement newBrokeredStatement
            (BrokeredStatementControl statementControl) throws SQLException {
		return new BrokeredStatement40(statementControl, getJDBCLevel());
    }
    public BrokeredPreparedStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql, Object generatedKeys) throws SQLException {
        return new BrokeredPreparedStatement40(statementControl, getJDBCLevel(), sql, generatedKeys);
    }
    public BrokeredCallableStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql) throws SQLException {
        return new BrokeredCallableStatement40(statementControl, getJDBCLevel(), sql);
    }
    
    /**
     * Returns the type map for this connection.
     *
     * @return type map for this connection
     * @exception SQLException if a database access error occurs
     */
    public java.util.Map<String,Class<?>> getTypeMap() throws SQLException {
        try {
            return getRealConnection().getTypeMap();
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }
    
    int getJDBCLevel() { return 4;}
    
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
        checkIfClosed();
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
                            throws SQLException{
        checkIfClosed();
        //Derby does not implement non-standard methods on 
        //JDBC objects
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    interfaces);
        }
    }
    
}

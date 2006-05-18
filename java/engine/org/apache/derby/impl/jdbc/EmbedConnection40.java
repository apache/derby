/*
 
   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection40
 
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

package org.apache.derby.impl.jdbc;

import java.sql.Array;
import java.sql.BaseQuery;
import java.sql.Blob;
import java.sql.ClientInfoException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.QueryObjectFactory;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.iapi.reference.SQLState;

public class EmbedConnection40 extends EmbedConnection30 {
    
    /** Creates a new instance of EmbedConnection40 */
    public EmbedConnection40(EmbedConnection inputConnection) {
        super(inputConnection);
    }
    
    public EmbedConnection40(
        InternalDriver driver,
        String url,
        Properties info)
        throws SQLException {
        super(driver, url, info);
    }
    
    /*
     *-------------------------------------------------------
     * JDBC 4.0
     *-------------------------------------------------------
     */
    
    public Array createArray(String typeName, Object[] elements)
        throws SQLException {
        throw Util.notImplemented();
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
    public Clob createClob() throws SQLException {
        checkIfClosed();
        return new EmbedClob("",this);
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
        checkIfClosed();
        return new EmbedBlob(new byte[0],this);
    }
    
    public NClob createNClob() throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML createSQLXML() throws SQLException {
        throw Util.notImplemented();
    }
    
    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by checking that the connection is not closed.
     *
     * @param timeout This should be the time in seconds to wait for the 
     * database operation used to validate the connection to complete 
     * (according to the JDBC4 JavaDoc). This is currently not supported/used.
     *
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the parameter value is illegal or if a
     * database error has occured
     */
    public boolean isValid(int timeout) throws SQLException {
        // Validate that the timeout has a legal value
        if (timeout < 0) {
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,
                                              new Integer(timeout), "timeout",
                                              "java.sql.Connection.isValid");
        }

        // Use the closed status for the connection to determine if the
        // connection is valid or not
        return !isClosed();
    }

    public void setClientInfo(String name, String value)
    throws SQLException{
        throw Util.notImplemented();
    }
    
    public void setClientInfo(Properties properties)
    throws ClientInfoException {
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
     * Returns the type map for this connection.
     *
     * @return type map for this connection
     * @exception SQLException if a database access error occurs
     */
    public final Map<String, Class<?>> getTypeMap() throws SQLException {
        // This method is already implemented with a non-generic
        // signature in EmbedConnection. We could just use that method
        // directly, but then we get a compiler warning (unchecked
        // cast/conversion). Copy the map to avoid the compiler
        // warning.
        Map typeMap = super.getTypeMap();
        if (typeMap == null) return null;
        Map<String, Class<?>> genericTypeMap = new HashMap<String, Class<?>>();
        for (Object key : typeMap.keySet()) {
            genericTypeMap.put((String) key, (Class) typeMap.get(key));
        }
        return genericTypeMap;
    }
    
    /**
     * This method forwards all the calls to default query object provided by 
     * the jdk.
     * @param ifc interface to generated concreate class
     * @return concreat class generated by default qury object generator
     */
    public <T extends BaseQuery> T createQueryObject(Class<T> ifc) 
                                                    throws SQLException {
        checkIfClosed();
        return QueryObjectFactory.createDefaultQueryObject (ifc, this);
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
        //hence return this if this class implements the interface 
        //or throw an SQLException
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP,interfaces);
        }
    }
}

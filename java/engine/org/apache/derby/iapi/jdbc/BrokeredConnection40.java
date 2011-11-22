/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection40
 
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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLClientInfoException;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;


public class BrokeredConnection40
        extends BrokeredConnection30 implements EngineConnection40 {
    
    /** Creates a new instance of BrokeredConnection40 */
    public BrokeredConnection40(BrokeredConnectionControl control) {
        super(control);
    }
    
    public Array createArrayOf(String typeName, Object[] elements)
          throws SQLException {    
         try {
             return getRealConnection().createArrayOf (typeName, elements);
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
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
        // Forward the createClob call to the physical connection
        try {
            return getRealConnection().createClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    
    public NClob createNClob() throws SQLException{
         try {
             return getRealConnection().createNClob();
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
    }
    
    public SQLXML createSQLXML() throws SQLException{
         try {
             return getRealConnection().createSQLXML ();
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
    }
    
    public Struct createStruct(String typeName, Object[] attributes)
          throws SQLException {
         try {
             return getRealConnection().createStruct (typeName, attributes);
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
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
    
    
    /**
     * <code>setClientInfo</code> forwards to the real connection.
     *
     * @param name the property key <code>String</code>
     * @param value the property value <code>String</code>
     * @exception SQLClientInfoException if the property is not
     * supported or the real connection could not be obtained.
     */
    public void setClientInfo(String name, String value)
    throws SQLClientInfoException{        
        try {
            getRealConnection().setClientInfo(name, value);
        } catch (SQLClientInfoException se) {
            notifyException(se);
            throw se;
        }
        catch (SQLException se) {
            notifyException(se);
            throw new SQLClientInfoException
                (se.getMessage(), se.getSQLState(), 
                		se.getErrorCode(),
  		 (new FailedProperties40
		  (FailedProperties40.makeProperties(name,value))).
		 getProperties());
        }
    }

    /**
     * <code>setClientInfo</code> forwards to the real connection.  If
     * the call to <code>getRealConnection</code> fails the resulting
     * <code>SQLException</code> is wrapped in a
     * <code>SQLClientInfoException</code> to satisfy the specified
     * signature.
     * @param properties a <code>Properties</code> object with the
     * properties to set.
     * @exception SQLClientInfoException if the properties are not
     * supported or the real connection could not be obtained.
     */    
    public void setClientInfo(Properties properties)
    throws SQLClientInfoException{
        try {
            getRealConnection().setClientInfo(properties);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        }
        catch (SQLException se) {
            notifyException(se);
            throw new SQLClientInfoException
                (se.getMessage(), se.getSQLState(),
                		se.getErrorCode(),
  		 (new FailedProperties40(properties)).getProperties());
        }
    }
    
    /**
     * <code>getClientInfo</code> forwards to the real connection.
     *
     * @param name a <code>String</code> that is the property key to get.
     * @return a <code>String</code> that is returned from the real connection.
     * @exception SQLException if a database access error occurs.
     */
    public String getClientInfo(String name)
    throws SQLException{
        try {
            return getRealConnection().getClientInfo(name);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }
    
    /**
     * <code>getClientInfo</code> forwards to the real connection.
     *
     * @return a <code>Properties</code> object
     * from the real connection.
     * @exception SQLException if a database access error occurs.
     */
    public Properties getClientInfo()
    throws SQLException{
        try {
            return getRealConnection().getClientInfo();
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }
    
    /**
     * returns an instance of JDBC4.0 speccific class BrokeredStatement40
     * @param  statementControl BrokeredStatementControl
     * @return an instance of BrokeredStatement40 
     * throws java.sql.SQLException
     */
    public final BrokeredStatement newBrokeredStatement
            (BrokeredStatementControl statementControl) throws SQLException {
        try {
            return new BrokeredStatement40(statementControl);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    public final BrokeredPreparedStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql, Object generatedKeys) throws SQLException {
        try {
            return new BrokeredPreparedStatement40(statementControl, sql, generatedKeys);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    public final BrokeredCallableStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql) throws SQLException {
        try {
            return new BrokeredCallableStatement40(statementControl, sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    /**
     * Returns the type map for this connection.
     *
     * @return type map for this connection
     * @exception SQLException if a database access error occurs
     */
    public final java.util.Map<String,Class<?>> getTypeMap() throws SQLException {
        try {
            return getRealConnection().getTypeMap();
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
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
    public final boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        try {
            if (getRealConnection().isClosed())
                throw Util.noCurrentConnection();
            return interfaces.isInstance(this);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption if no object if found that implements the 
     * interface
     */
    public final <T> T unwrap(java.lang.Class<T> interfaces) 
                            throws SQLException{
        try {
            if (getRealConnection().isClosed())
                throw Util.noCurrentConnection();
            //Derby does not implement non-standard methods on 
            //JDBC objects
            try {
                return interfaces.cast(this);
            } catch (ClassCastException cce) {
                throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                        interfaces);
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////
    
    public  void    abort( Executor executor )  throws SQLException
    {
        if (!isClosed)
        {
            ((EngineConnection40) getRealConnection()).abort(executor);
        }
    }
    
    public int getNetworkTimeout() throws SQLException
    {
         try {
             return
                 ((EngineConnection40) getRealConnection()).getNetworkTimeout();
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
    }
    
    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
         try {
             ((EngineConnection40) getRealConnection())
                     .setNetworkTimeout(executor, milliseconds);
         } catch (SQLException sqle) {
             notifyException(sqle);
             throw sqle;
         }
    }

}

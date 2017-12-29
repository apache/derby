/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection

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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.derby.shared.common.error.SQLWarningFactory;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.ExceptionFactory;

/**
 * This is a rudimentary connection that delegates
 * EVERYTHING to Connection.
 */
public class BrokeredConnection implements EngineConnection
{
	
	// default for Derby
	int stateHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;

	final BrokeredConnectionControl control;
	protected boolean isClosed;
        private String connString;

	/**
		Maintain state as seen by this Connection handle, not the state
		of the underlying Connection it is attached to.
	*/
	private int stateIsolationLevel;
	private boolean stateReadOnly;
	private boolean stateAutoCommit;

	/////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////

	public	BrokeredConnection(BrokeredConnectionControl control)
            throws SQLException
	{
		this.control = control;
	}

    // JDBC 2.0 methods

	public final void setAutoCommit(boolean autoCommit) throws SQLException 
	{
		try {
			control.checkAutoCommit(autoCommit);

			getRealConnection().setAutoCommit(autoCommit);

			stateAutoCommit = autoCommit;
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}
	public final boolean getAutoCommit() throws SQLException 
	{
		try {
			return getRealConnection().getAutoCommit();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}
	public final Statement createStatement() throws SQLException 
	{
		try {
			return control.wrapStatement(getRealConnection().createStatement());
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final PreparedStatement prepareStatement(String sql)
	    throws SQLException 
	{
		try {
			return control.wrapStatement(getRealConnection().prepareStatement(sql), sql, null);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final CallableStatement prepareCall(String sql) throws SQLException 
	{
		try {
			return control.wrapStatement(getRealConnection().prepareCall(sql), sql);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final String nativeSQL(String sql) throws SQLException
	{
		try {
			return getRealConnection().nativeSQL(sql);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void commit() throws SQLException 
	{
		try {
			control.checkCommit();
			getRealConnection().commit();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void rollback() throws SQLException 
	{
		try {
			control.checkRollback();
			getRealConnection().rollback();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void close() throws SQLException 
	{ 
		if (isClosed)
			return;

		try {
            control.checkClose();

			if (!control.closingConnection()) {
				isClosed = true;
				return;
			}

			isClosed = true;


			getRealConnection().close();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final boolean isClosed() throws SQLException 
	{
		if (isClosed)
			return true;
		try {
			boolean realIsClosed = getRealConnection().isClosed();
			if (realIsClosed) {
				control.closingConnection();
				isClosed = true;
			}
			return realIsClosed;
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final SQLWarning getWarnings() throws SQLException 
	{
		try {
			return getRealConnection().getWarnings();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void clearWarnings() throws SQLException 
	{
		try {
			getRealConnection().clearWarnings();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final DatabaseMetaData getMetaData() throws SQLException 
	{
		try {
			return getRealConnection().getMetaData();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void setReadOnly(boolean readOnly) throws SQLException 
	{
		try {
			getRealConnection().setReadOnly(readOnly);
			stateReadOnly = readOnly;
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final boolean isReadOnly() throws SQLException 
	{
		try {
			return getRealConnection().isReadOnly();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void setCatalog(String catalog) throws SQLException 
	{
		try {
			getRealConnection().setCatalog(catalog);
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final String getCatalog() throws SQLException 
	{
		try {
			return getRealConnection().getCatalog();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final void setTransactionIsolation(int level) throws SQLException 
	{
		try {
			getRealConnection().setTransactionIsolation(level);
			stateIsolationLevel = level;
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

	public final int getTransactionIsolation() throws SQLException
	{
		try {
			return getRealConnection().getTransactionIsolation();
		} catch (SQLException sqle) {
			notifyException(sqle);
			throw sqle;
		}
	}

    public final Statement createStatement(int resultSetType, int resultSetConcurrency) 
      throws SQLException
	{
		try
		{
			return control.wrapStatement(getRealConnection().
				createStatement(resultSetType, resultSetConcurrency));
		}
		catch (SQLException se)
		{
			notifyException(se);
			throw se;
		}
	}


	public final PreparedStatement prepareStatement(String sql, int resultSetType, 
					int resultSetConcurrency)
       throws SQLException
	{
		try
		{
			return control.wrapStatement(getRealConnection().
				prepareStatement(sql, resultSetType, resultSetConcurrency), sql, null);
		}
		catch (SQLException se)
		{
			notifyException(se);
			throw se;
		}
	}

    public final CallableStatement prepareCall(String sql, int resultSetType, 
				 int resultSetConcurrency) throws SQLException
	{
		try
		{
			return control.wrapStatement(getRealConnection().
				prepareCall(sql, resultSetType, resultSetConcurrency), sql);
		}
		catch (SQLException se)
		{
			notifyException(se);
			throw se;
		}
	}

    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException
	{
		try
		{
			return getRealConnection().getTypeMap();
		}
		catch (SQLException se)
		{
			notifyException(se);
			throw se;
		}
	}

    public final void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException
	{
		try
		{
			getRealConnection().setTypeMap(map);
		}
		catch (SQLException se)
		{
			notifyException(se);
			throw se;
		}
	}

    // JDBC 3.0 methods

    public final Statement createStatement(int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {
        try {
            resultSetHoldability =
                    statementHoldabilityCheck(resultSetHoldability);
            return control.wrapStatement(
                    getRealConnection().createStatement(resultSetType,
                    resultSetConcurrency, resultSetHoldability));
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final CallableStatement prepareCall(String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {
        try {
            resultSetHoldability =
                    statementHoldabilityCheck(resultSetHoldability);
            return control.wrapStatement(
                    getRealConnection().prepareCall(sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability), sql);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final Savepoint setSavepoint()
            throws SQLException {
        try {
            control.checkSavepoint();
            return getRealConnection().setSavepoint();
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final Savepoint setSavepoint(String name)
            throws SQLException {
        try {
            control.checkSavepoint();
            return getRealConnection().setSavepoint(name);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final void rollback(Savepoint savepoint)
            throws SQLException {
        try {
            control.checkRollback();
            getRealConnection().rollback(savepoint);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        try {
            getRealConnection().releaseSavepoint(savepoint);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final void setHoldability(int holdability)
            throws SQLException {
        try {
            holdability = control.checkHoldCursors(holdability, false);
            getRealConnection().setHoldability(holdability);
            stateHoldability = holdability;
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final PreparedStatement prepareStatement(
            String sql,
            int autoGeneratedKeys)
            throws SQLException {
        try {
            return control.wrapStatement(getRealConnection().prepareStatement(
                sql, autoGeneratedKeys), sql, autoGeneratedKeys);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final PreparedStatement prepareStatement(
            String sql,
            int[] columnIndexes)
            throws SQLException {
        try {
            return control.wrapStatement(getRealConnection().prepareStatement(
                    sql, columnIndexes), sql, columnIndexes);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    public final PreparedStatement prepareStatement(
            String sql,
            String[] columnNames)
            throws SQLException {
        try {
            return control.wrapStatement(getRealConnection().prepareStatement(
                    sql, columnNames), sql, columnNames);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }


	/////////////////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     * Generate an exception reporting that there is no current connection.
     * @return a no-current-connection exception
     */
    final SQLException noCurrentConnection() {
        return ExceptionFactory.getInstance().getSQLException(
                SQLState.NO_CURRENT_CONNECTION, null, null, null);
    }

	/**
	  *	A little indirection for getting the real connection. 
	  *
	  *	@return	the current connection
	  */
	final EngineConnection getRealConnection() throws SQLException {
		if (isClosed)
			throw noCurrentConnection();

		return control.getRealConnection();
	}

	final void notifyException(SQLException sqle) {
		if (!isClosed)
			control.notifyException(sqle);
	}

	/**
		Sync up the state of the underlying connection
		with the state of this new handle.
	*/
	public void syncState() throws SQLException {
		EngineConnection conn = getRealConnection();

		stateIsolationLevel = conn.getTransactionIsolation();
		stateReadOnly = conn.isReadOnly();
		stateAutoCommit = conn.getAutoCommit();
        stateHoldability = conn.getHoldability(); 
	}

	/**
		Isolation level state in BrokeredConnection can get out of sync
		if the isolation is set using SQL rather than JDBC. In order to
		ensure correct state level information, this method is called
		at the start and end of a global transaction.
	*/
	public void getIsolationUptoDate() throws SQLException {
		if (control.isIsolationLevelSetUsingSQLorJDBC()) {
			stateIsolationLevel = getRealConnection().getTransactionIsolation();
			control.resetIsolationLevelFlag();
		}
	}
	/**
		Set the state of the underlying connection according to the
		state of this connection's view of state.

		@param complete If true set the complete state of the underlying
		Connection, otherwise set only the Connection related state (ie.
		the non-transaction specific state).


	*/
	public void setState(boolean complete) throws SQLException {

		if (complete) {
		    Connection conn = getRealConnection();
			conn.setTransactionIsolation(stateIsolationLevel);
			conn.setReadOnly(stateReadOnly);
			conn.setAutoCommit(stateAutoCommit);
			// make the underlying connection pick my holdability state
			// since holdability is a state of the connection handle
			// not the underlying transaction.
            conn.setHoldability(stateHoldability);
		}
	}

    public final BrokeredStatement newBrokeredStatement(
            BrokeredStatementControl statementControl) throws SQLException {
        try {
            return new BrokeredStatement(statementControl);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public BrokeredPreparedStatement newBrokeredStatement(
            BrokeredStatementControl statementControl,
            String sql, Object generatedKeys) throws SQLException {
        try {
            return new BrokeredPreparedStatement(statementControl, sql, generatedKeys);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public BrokeredCallableStatement newBrokeredStatement(
            BrokeredStatementControl statementControl, String sql)
            throws SQLException {
        try {
            return new BrokeredCallableStatement(statementControl, sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

	/**
	 *  set the DrdaId for this connection. The drdaID prints with the 
	 *  statement text to the errror log
	 *  @param drdaID  drdaID to be used for this connection
	 *
	 */
	public final void setDrdaID(String drdaID)
	{
        try {
		    getRealConnection().setDrdaID(drdaID);
        } catch (SQLException sqle)
        {
            // connection is closed, just ignore drdaId
            // since connection cannot be used.
        }
	}

    /** @see EngineConnection#isInGlobalTransaction() */
    public boolean isInGlobalTransaction() {
    	return control.isInGlobalTransaction();
    }

	/**
	 *  Set the internal isolation level to use for preparing statements.
	 *  Subsequent prepares will use this isoalation level
	 * @param level - internal isolation level 
	 * @throws SQLException
	 * See EmbedConnection#setPrepareIsolation
	 * 
	 */
	public final void setPrepareIsolation(int level) throws SQLException
	{
        getRealConnection().setPrepareIsolation(level);
	}

	/**
	 * get the isolation level that is currently being used to prepare 
	 * statements (used for network server)
	 * 
	 * @throws SQLException
	 * @return current prepare isolation level 
	 * See EmbedConnection#getPrepareIsolation
	 */
	public final int getPrepareIsolation() throws SQLException
	{
		return getRealConnection().getPrepareIsolation();
	}
    
    /**
     * Add a SQLWarning to this Connection object.
     * @throws SQLException 
     */
    public final void addWarning(SQLWarning w) throws SQLException
    {
        getRealConnection().addWarning(w);
    }

    /**
     * Get the string representation for this connection.  Return
     * the class name/hash code and various debug information.
     * 
     * @return unique string representation for this connection
     */
    public String toString() 
    {
        if ( connString == null )
        {
            String wrappedString;
            try
            {
                wrappedString = getRealConnection().toString();
            }
            catch ( SQLException e )
            {
                wrappedString = "<none>";
            }
            
            connString = this.getClass().getName() + "@" + this.hashCode() +
                ", Wrapped Connection = " + wrappedString;
        }
        
        return connString;
    }

    /*
     * JDBC 3.0 methods.
     */
    
    /**
     * Prepare statement with explicit holdability.
     */
    public final PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
    	try {
            resultSetHoldability = statementHoldabilityCheck(resultSetHoldability);
    		
    		return control.wrapStatement(
    			getRealConnection().prepareStatement(sql, resultSetType,
                        resultSetConcurrency, resultSetHoldability), sql, null);
    	}
    	catch (SQLException se)
    	{
    		notifyException(se);
    		throw se;
    	}
    }

    /**
     * Get the holdability for statements created by this connection
     * when holdability is not passed in.
     */
    public final int getHoldability() throws SQLException {
    	try {
    		return getRealConnection().getHoldability();
    	}
    	catch (SQLException se)
    	{
    		notifyException(se);
    		throw se;
    	}
    }

    // JDBC 4.0 methods

    public final Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        try {
            return getRealConnection().createArrayOf(typeName, elements);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     *
     * Constructs an object that implements the {@code Blob} interface. The
     * object returned initially contains no data. The {@code setBinaryStream}
     * and {@code setBytes} methods of the {@code Blob} interface may be used to
     * add data to the {@code Blob}.
     *
     * @return An object that implements the {@code Blob} interface
     * @throws SQLException if an object that implements the {@code Blob}
     * interface can not be constructed, this method is called on a closed
     * connection or a database access error occurs.
     *
     */
    public final Blob createBlob() throws SQLException {
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
     * Constructs an object that implements the {@code Clob} interface. The
     * object returned initially contains no data. The {@code setAsciiStream},
     * {@code setCharacterStream} and {@code setString} methods of the
     * {@code Clob} interface may be used to add data to the {@code Clob}.
     *
     * @return An object that implements the {@code Clob} interface
     * @throws SQLException if an object that implements the {@code Clob}
     * interface can not be constructed, this method is called on a closed
     * connection or a database access error occurs.
     *
     */
    public final Clob createClob() throws SQLException {
        // Forward the createClob call to the physical connection
        try {
            return getRealConnection().createClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public final NClob createNClob() throws SQLException {
        try {
            return getRealConnection().createNClob();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public final SQLXML createSQLXML() throws SQLException {
        try {
            return getRealConnection().createSQLXML();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public final Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        try {
            return getRealConnection().createStruct(typeName, attributes);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Checks if the connection has not been closed and is still valid. The
     * validity is checked by running a simple query against the database.
     *
     * @param timeout The time in seconds to wait for the database operation
     * used to validate the connection to complete. If the timeout period
     * expires before the operation completes, this method returns false. A
     * value of 0 indicates a timeout is not applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the call on the physical connection throws an
     * exception.
     */
    public final boolean isValid(int timeout) throws SQLException {
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
     * {@code setClientInfo} forwards to the real connection.
     *
     * @param name the property key {@code String}
     * @param value the property value {@code String}
     * @exception SQLClientInfoException if the property is not supported or the
     * real connection could not be obtained.
     */
    public final void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        try {
            getRealConnection().setClientInfo(name, value);
        } catch (SQLClientInfoException se) {
            notifyException(se);
            throw se;
        } catch (SQLException se) {
            notifyException(se);
            throw new SQLClientInfoException(se.getMessage(), se.getSQLState(),
                    se.getErrorCode(),
                    new FailedProperties40(
                        FailedProperties40.makeProperties(name, value))
                            .getProperties());
        }
    }

    /**
     * {@code setClientInfo} forwards to the real connection. If the call to
     * {@code getRealConnection} fails the resulting {@code SQLException} is
     * wrapped in a {@code SQLClientInfoException} to satisfy the specified
     * signature.
     *
     * @param properties a {@code Properties} object with the properties to set.
     * @exception SQLClientInfoException if the properties are not supported or
     * the real connection could not be obtained.
     */
    public final void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        try {
            getRealConnection().setClientInfo(properties);
        } catch (SQLClientInfoException cie) {
            notifyException(cie);
            throw cie;
        } catch (SQLException se) {
            notifyException(se);
            throw new SQLClientInfoException(se.getMessage(), se.getSQLState(),
                    se.getErrorCode(),
                    (new FailedProperties40(properties)).getProperties());
        }
    }

    /**
     * {@code getClientInfo} forwards to the real connection.
     *
     * @param name a {@code String} that is the property key to get.
     * @return a {@code String} that is returned from the real connection.
     * @exception SQLException if a database access error occurs.
     */
    public final String getClientInfo(String name)
            throws SQLException {
        try {
            return getRealConnection().getClientInfo(name);
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    /**
     * {@code getClientInfo} forwards to the real connection.
     *
     * @return a {@code Properties} object from the real connection.
     * @exception SQLException if a database access error occurs.
     */
    public final Properties getClientInfo()
            throws SQLException {
        try {
            return getRealConnection().getClientInfo();
        } catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    /**
     * Returns false unless {@code iface} is implemented.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly
     * wraps an object that does.
     * @throws SQLException if an error occurs while determining
     * whether this is a wrapper for an object with the given interface.
     */
    public final boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            if (getRealConnection().isClosed()) {
                throw noCurrentConnection();
            }
            return iface.isInstance(this);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /**
     * Returns {@code this} if this class implements the interface.
     *
     * @param iface a Class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object if found that implements the
     * interface
     */
    public final <T> T unwrap(Class<T> iface)
            throws SQLException {
        try {
            if (getRealConnection().isClosed()) {
                throw noCurrentConnection();
            }
            // Derby does not implement non-standard methods on
            // JDBC objects.
            try {
                return iface.cast(this);
            } catch (ClassCastException cce) {
                throw ExceptionFactory.getInstance().getSQLException(
                        SQLState.UNABLE_TO_UNWRAP,
                        (SQLException) null, (Throwable) null, iface);
            }
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    /*
    ** Methods private to the class.
    */
    
    /**
     * Check the result set holdability when creating a statement
     * object. Section 16.1.3.1 of JDBC 4.0 (proposed final draft)
     * says the driver may change the holdabilty and add a SQLWarning
     * to the Connection object.
     * 
     * This work-in-progress implementation throws an exception
     * to match the old behaviour just as part of incremental development.
     */
    final int statementHoldabilityCheck(int resultSetHoldability)
        throws SQLException
    {
        int holdability = control.checkHoldCursors(resultSetHoldability, true);
        if (holdability != resultSetHoldability) {
            SQLWarning w =
                 SQLWarningFactory.newSQLWarning(SQLState.HOLDABLE_RESULT_SET_NOT_AVAILABLE);
            
            addWarning(w);
        }
        
        return holdability;
        
    }

	/**
	* Get the LOB reference corresponding to the locator.
	* @param key the integer that represents the LOB locator value.
	* @return the LOB Object corresponding to this locator.
	*/
	public Object getLOBMapping(int key) throws SQLException {
            //Forward the methods implementation to the implementation in the
            //underlying EmbedConnection object. 
            return getRealConnection().getLOBMapping(key);
	}

    /**
     * Obtain the name of the current schema. Not part of the
     * java.sql.Connection interface, but is accessible through the
     * EngineConnection interface, so that the NetworkServer can get at the
     * current schema for piggy-backing
     * @return the current schema name
     * @throws java.sql.SQLException
     */
    public String getCurrentSchemaName() throws SQLException {
        try {
            return getRealConnection().getCurrentSchemaName();
        }
        catch (SQLException se) {
            notifyException(se);
            throw se;
        }
    }

    /**
     * @see org.apache.derby.iapi.jdbc.EngineConnection
     */
    public void resetFromPool()
            throws SQLException {
        getRealConnection().resetFromPool();
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Get the name of the current schema.
     */
    public String   getSchema() throws SQLException
    {
    	try {
            return getRealConnection().getSchema();
    	}
    	catch (SQLException se)
    	{
    		notifyException(se);
    		throw se;
    	}
    }

    /**
     * Set the default schema for the Connection.
     */
    public void   setSchema(  String schemaName ) throws SQLException
	{
    	try {
            getRealConnection().setSchema( schemaName );
    	}
    	catch (SQLException se)
    	{
    		notifyException(se);
    		throw se;
    	}
    }

    public void abort(Executor executor) throws SQLException {
        if (!isClosed) {
            ((EngineConnection) getRealConnection()).abort(executor);
        }
    }

    public int getNetworkTimeout() throws SQLException {
        try {
            return ((EngineConnection) getRealConnection()).getNetworkTimeout();
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        try {
            ((EngineConnection) getRealConnection())
                    .setNetworkTimeout(executor, milliseconds);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }

}

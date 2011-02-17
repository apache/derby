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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.impl.jdbc.Util;

import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This is a rudimentary connection that delegates
 * EVERYTHING to Connection.
 */
public abstract class BrokeredConnection implements EngineConnection
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
	{
		this.control = control;
	}

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

    public java.util.Map getTypeMap() throws SQLException
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

    public final void setTypeMap(java.util.Map map) throws SQLException
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

	/////////////////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	  *	A little indirection for getting the real connection. 
	  *
	  *	@return	the current connection
	  */
	final EngineConnection getRealConnection() throws SQLException {
		if (isClosed)
			throw Util.noCurrentConnection();

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

	public BrokeredStatement newBrokeredStatement(BrokeredStatementControl statementControl) throws SQLException {
		return new BrokeredStatement(statementControl);
	}
	public abstract BrokeredPreparedStatement
        newBrokeredStatement(BrokeredStatementControl statementControl,
                String sql, Object generatedKeys) throws SQLException;
	public abstract BrokeredCallableStatement
        newBrokeredStatement(BrokeredStatementControl statementControl,
                String sql) throws SQLException;

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
     * Checks if the connection is closed and throws an exception if
     * it is.
     *
     * @exception SQLException if the connection is closed
     */
    protected final void checkIfClosed() throws SQLException {
        if (isClosed()) {
            throw Util.noCurrentConnection();
        }
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
     * JDBC 3.0 methods that are exposed through EngineConnection.
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
	* Clear the HashMap of all entries.
	* Called when a commit or rollback of the transaction
	* happens.
	*/
	public void clearLOBMapping() throws SQLException {
            //Forward the methods implementation to the implementation in the
            //underlying EmbedConnection object. 
            getRealConnection().clearLOBMapping();
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
    
}

/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.impl.jdbc.Util;

import java.io.ObjectOutput;
import java.io.ObjectInput;

/**
 * This is a rudimentary connection that delegates
 * EVERYTHING to Connection.
 */
public class BrokeredConnection implements Connection
{

	protected final BrokeredConnectionControl control;
	private boolean isClosed;

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

    public final java.util.Map getTypeMap() throws SQLException
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
	protected final Connection getRealConnection() throws SQLException {
		if (isClosed)
			throw Util.noCurrentConnection();

		return control.getRealConnection();
	}

	protected final void notifyException(SQLException sqle) {
		if (!isClosed)
			control.notifyException(sqle);
	}

	/**
		Sync up the state of the underlying connection
		with the state of this new handle.
	*/
	public void syncState() throws SQLException {
		Connection conn = getRealConnection();

		stateIsolationLevel = conn.getTransactionIsolation();
		stateReadOnly = conn.isReadOnly();
		stateAutoCommit = conn.getAutoCommit();
	}

	/**
		Set the state of the underlying connection according to the
		state of this connection's view of state.

		@param complete If true set the complete state of the underlying
		Connection, otherwise set only the Connection related state (ie.
		the non-transaction specific state).

		
	*/
	public void setState(boolean complete) throws SQLException {

		Connection conn = getRealConnection();

		if (complete) {
			conn.setTransactionIsolation(stateIsolationLevel);
			conn.setReadOnly(stateReadOnly);
			conn.setAutoCommit(stateAutoCommit);
		}
	}

	public BrokeredStatement newBrokeredStatement(BrokeredStatementControl statementControl) throws SQLException {
		return new BrokeredStatement(statementControl, getJDBCLevel());
	}
	public BrokeredPreparedStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql, Object generatedKeys) throws SQLException {
		return new BrokeredPreparedStatement(statementControl, getJDBCLevel(), sql);
	}
	public BrokeredCallableStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql) throws SQLException {
		return new BrokeredCallableStatement(statementControl, getJDBCLevel(), sql);
	}

	protected int getJDBCLevel() { return 2;}
}

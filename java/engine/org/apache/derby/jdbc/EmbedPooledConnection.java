/*

   Derby - Class org.apache.derby.jdbc.EmbedPooledConnection

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.jdbc;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.JDBC30Translation;

/* import impl class */
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedConnection20;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredConnectionControl;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

import java.util.Vector;
import java.util.Enumeration;

/* -- New jdbc 20 extension types --- */
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionEvent;

/** 
	A PooledConnection object is a connection object that provides hooks for
	connection pool management.

	<P>This is Cloudscape's implementation of a PooledConnection.  

 */
class EmbedPooledConnection implements javax.sql.PooledConnection, BrokeredConnectionControl
{

	private Vector eventListener; // who wants to know I am closed or error

	protected EmbedConnection20 realConnection;
	protected int defaultIsolationLevel;
	private boolean defaultReadOnly;
	protected BrokeredConnection currentConnectionHandle;

	// set up once by the data source
	protected final ReferenceableDataSource dataSource;
	private final String username;
	private final String password;
	/**
		True if the password was passed in on the connection request, false if it came from the data source property.
	*/
	private final boolean requestPassword;

	private boolean isActive;

	EmbedPooledConnection(ReferenceableDataSource ds, String u, String p, boolean requestPassword) throws SQLException
	{
		dataSource = ds;
		username = u;
		password = p;
		this.requestPassword = requestPassword;
		isActive = true;

		// open the connection up front in order to do authentication
		openRealConnection();

	}

	String getUsername()
	{
		if (username == null || username.equals(""))
			return Property.DEFAULT_USER_NAME;
		else
			return username;
	}

	String getPassword()
	{
		if (password == null)
			return "";
		else
			return password;
	}


	/** 
		Create an object handle for a database connection.

		@return a Connection object

		@exception SQLException - if a database-access error occurs.
	*/
	public synchronized Connection getConnection() throws SQLException
	{
		checkActive();

		// need to do this in case the connection is forcibly removed without
		// first being closed.
		closeCurrentConnectionHandle();


		// RealConnection is not null if the app server yanks a local
		// connection from one client and give it to another.  In this case,
		// the real connection is ready to be used.  Otherwise, set it up
		if (realConnection == null)
		{
			// first time we establish a connection
			openRealConnection();
		}
		else
		{
			resetRealConnection();
		}

		// now make a brokered connection wrapper and give this to the user
		// we reuse the EmbedConnection(ie realConnection).
		Connection c = getNewCurrentConnectionHandle();		
		return c;
	}

	protected final void openRealConnection() throws SQLException {
		// first time we establish a connection
		Connection rc = dataSource.getConnection(username, password, requestPassword);

		this.realConnection = (EmbedConnection20) rc;
		defaultIsolationLevel = rc.getTransactionIsolation();
		defaultReadOnly = rc.isReadOnly();
		if (currentConnectionHandle != null)
			realConnection.setApplicationConnection(currentConnectionHandle);
	}

	protected final Connection getNewCurrentConnectionHandle() {
		Connection applicationConnection = currentConnectionHandle =
			((org.apache.derby.jdbc.Driver20) (realConnection.getLocalDriver())).newBrokeredConnection(this);
		realConnection.setApplicationConnection(applicationConnection);
		return applicationConnection;

	}

	/**
		In this case the Listeners are *not* notified. JDBC 3.0 spec section 11.4
	*/
	protected void closeCurrentConnectionHandle() throws SQLException {
		if (currentConnectionHandle != null)
		{
			Vector tmpEventListener = eventListener;
			eventListener = null;

			try {
				currentConnectionHandle.close();
			} finally {
				eventListener = tmpEventListener;
			}

			currentConnectionHandle = null;
		}
	}

	protected void resetRealConnection() throws SQLException {

		// ensure any outstanding changes from the previous
		// user are rolledback.
		realConnection.rollback();

		// clear any warnings that are left over
		realConnection.clearWarnings();

		// need to reset transaction isolation, autocommit, readonly, holdability states
		if (realConnection.getTransactionIsolation() != defaultIsolationLevel) {

			realConnection.setTransactionIsolation(defaultIsolationLevel);
		}

		if (!realConnection.getAutoCommit())
			realConnection.setAutoCommit(true);

		if (realConnection.isReadOnly() != defaultReadOnly)
			realConnection.setReadOnly(defaultReadOnly);

		if (realConnection.getHoldability() != JDBC30Translation.HOLD_CURSORS_OVER_COMMIT)
			realConnection.setHoldability(JDBC30Translation.HOLD_CURSORS_OVER_COMMIT);

		// drop any temporary tables that may have been declared by the previous user
		realConnection.dropAllDeclaredGlobalTempTables();
	}

	/**
		Close the Pooled connection.

		@exception SQLException - if a database-access error occurs.
	 */
	public synchronized void close() throws SQLException
	{
		if (!isActive)
			return;

		closeCurrentConnectionHandle();
		try {
			if (realConnection != null) {
				if (!realConnection.isClosed())
					realConnection.close();
			}

		} finally {

			realConnection = null;	// make sure I am not accessed again.
			isActive = false;
			eventListener = null;
		}
	}

	/**
		Add an event listener.
	 */
	public final synchronized void addConnectionEventListener(ConnectionEventListener listener) 
	{
		if (!isActive)
			return;
		if (listener == null)
			return;
		if (eventListener == null)
			eventListener = new Vector();
		eventListener.addElement(listener);
	}

	/**
		Remove an event listener.
	 */
	public final synchronized void removeConnectionEventListener(ConnectionEventListener listener)
	{
		if (listener == null)
			return;
		if (eventListener != null)
			eventListener.removeElement(listener);
	}

	/*
	 * class specific method
	 */

	// called by ConnectionHandle when it needs to forward things to the
	// underlying connection
	public synchronized Connection getRealConnection() throws SQLException
	{
		checkActive();

		return realConnection;
	}


	// my conneciton handle has caught an error (actually, the real connection
	// has already handled the error, we just need to nofity the listener an
	// error is about to be thrown to the app).
	public synchronized void notifyError(SQLException exception)
	{
		// only report fatal error to the connection pool manager 
		if (exception.getErrorCode() < ExceptionSeverity.SESSION_SEVERITY)
			return;

		// tell my listeners an exception is about to be thrown
		if (eventListener != null && eventListener.size() > 0)
		{
			ConnectionEvent errorEvent = new ConnectionEvent(this, exception);

			for (Enumeration enum = eventListener.elements();
				 enum.hasMoreElements(); )
			{
				ConnectionEventListener l =
					(ConnectionEventListener)enum.nextElement(); 
				l.connectionErrorOccurred(errorEvent);
			}
		}
	}

	// my conneciton handle is being closed
	public synchronized void notifyClose()
	{
		// tell my listeners I am closed 
		if (eventListener != null && eventListener.size() > 0)
		{
			ConnectionEvent closeEvent = new ConnectionEvent(this);

			for (Enumeration enum = eventListener.elements();
				 enum.hasMoreElements(); )
			{
				ConnectionEventListener l =
					(ConnectionEventListener)enum.nextElement(); 
				l.connectionClosed(closeEvent);
			}
		}
	}

	protected final void checkActive() throws SQLException {
		if (!isActive)
			throw Util.noCurrentConnection();
	}


	/*
	** BrokeredConnectionControl api
	*/

	/**
		Notify the control class that a SQLException was thrown
		during a call on one of the brokered connection's methods.
	*/
	public void notifyException(SQLException sqle) {
		this.notifyError(sqle);
	}


	/**
		Allow control over setting auto commit mode.
	*/
	public void checkAutoCommit(boolean autoCommit) throws SQLException {
	}

	/**
		Are held cursors allowed.
	*/
	public void checkHoldCursors(int holdability) throws SQLException {
	}

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	public void checkSavepoint() throws SQLException {
	}

	/**
		Allow control over calling rollback.
	*/
	public void checkRollback() throws SQLException {
	}

	/**
		Allow control over calling commit.
	*/
	public void checkCommit() throws SQLException {
	}

	/**
		Close called on BrokeredConnection. If this call
		returns true then getRealConnection().close() will be called.

		Don't close the underlying real connection as
		it is pooled.
	*/
	public boolean closingConnection() throws SQLException {
		notifyClose();
		currentConnectionHandle = null;
		return false;
	}

	/**
		No need to wrap statements for PooledConnections.
	*/
	public Statement wrapStatement(Statement s) throws SQLException {
		return s;
	}
	/**
		No need to wrap statements for PooledConnections.
	*/
	public PreparedStatement wrapStatement(PreparedStatement ps, String sql, Object generatedKeys) throws SQLException {
		return ps;
	}
	/**
		No need to wrap statements for PooledConnections.
	*/
	public CallableStatement wrapStatement(CallableStatement cs, String sql) throws SQLException {
		return cs;
	}
}

/*

   Derby - Class org.apache.derby.impl.jdbc.ConnectionChild

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.jdbc.Driver169;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.db.Database;

import java.sql.SQLException;

/**
	Any class in the local JDBC driver (ie this package) that needs to
	refer back to the EmbedConnection object extends this class.
*/

public abstract class ConnectionChild {

	// parameters to handleException
	protected static final boolean CLOSE = true;
	protected static final boolean NOCLOSE = false;

	/*
	** Local connection is the current EmbedConnection
	** object that we use for all our work.
	*/
	protected EmbedConnection localConn;

	/**	
		Factory for JDBC objects to be created.
	*/
	protected final Driver169 factory;

	/**
		Calendar for data operations.
	*/
	private java.util.Calendar cal;


	protected ConnectionChild(EmbedConnection conn) {
		super();
		localConn = conn;
		factory = conn.getLocalDriver();
	}

	/**
		Return a reference to the EmbedConnection
	*/
	protected final EmbedConnection getEmbedConnection() {
		return localConn;
	}

	/**
	 * Return an object to be used for connection
	 * synchronization.
	 */
	protected final Object getConnectionSynchronization()
	{
		return localConn.getConnectionSynchronization();
	}

	/**
		Handle any exception.
		@see EmbedConnection#handleException
		@exception SQLException thrown if can't handle
	*/
	protected final SQLException handleException(Throwable t)
			throws SQLException {
		return localConn.handleException(t);
	}

	/**
		Handle any exception.
		@see EmbedConnection#handleException
		@exception SQLException thrown if can't handle
	*/
	protected final SQLException handleException(Throwable t, boolean close)
			throws SQLException {
		return localConn.handleException(t, close);
	}
	/**
		If Autocommit is on, note that a commit is needed.
		@see EmbedConnection#needCommit
	 */
	protected final void needCommit() {
		localConn.needCommit();
	}

	/**
		Perform a commit if one is needed.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	protected final void commitIfNeeded() throws SQLException {
		//System.out.println(this + " <> " + localConn.getClass());
		//new Throwable("cin").printStackTrace(System.out);
		localConn.commitIfNeeded();
	}

	/**
		Perform a commit if autocommit is enabled.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	protected final void commitIfAutoCommit() throws SQLException {
		//System.out.println(this + " <> " + localConn.getClass());
		//new Throwable("cin").printStackTrace(System.out);
		localConn.commitIfAutoCommit();
	}

	/**
		Setup the context stack (a.k.a. context manager)
		for this connection.
		@see EmbedConnection#setupContextStack
		@exception SQLException thrown on failure
	 */
	protected final void setupContextStack() throws SQLException {
		localConn.setupContextStack();
	}

	/**
		Setup the context stack (a.k.a. context manager)
		for this connection.
		@see EmbedConnection#restoreContextStack
		@exception SQLException thrown on failure
	 */
	protected final void restoreContextStack() throws SQLException {
		localConn.restoreContextStack();
	}

    public ContextManager getContextManager()
    {
        return localConn.getContextManager();
    }

    public Database getDatabase()
    {
        return localConn.getDatabase();
    }

	/**
		Get and save a unique calendar object for this JDBC object.
		No need to synchronize because multiple threads should not
		be using a single JDBC object. Even if they do there is only
		a small window where each would get its own Calendar for a
		single call.
	*/
	protected java.util.Calendar getCal() {
		if (cal == null)
			cal = new java.util.GregorianCalendar();
		return cal;
	}

	protected SQLException newSQLException(String messageId) {
		return localConn.newSQLException(messageId);
	}
	protected SQLException newSQLException(String messageId, Object arg1) {
		return localConn.newSQLException(messageId, arg1);
	}
	protected SQLException newSQLException(String messageId, Object arg1, Object arg2) {
		return localConn.newSQLException(messageId, arg1, arg2);
	}
}



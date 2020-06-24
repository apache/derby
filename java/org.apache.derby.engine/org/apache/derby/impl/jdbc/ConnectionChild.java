/*

   Derby - Class org.apache.derby.impl.jdbc.ConnectionChild

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.jdbc.InternalDriver;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.InterruptStatus;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;

/**
	Any class in the embedded JDBC driver (ie this package) that needs to
	refer back to the EmbedConnection object extends this class.
*/

abstract class ConnectionChild {

	/*
	** Local connection is the current EmbedConnection
	** object that we use for all our work.
	*/
	EmbedConnection localConn;

    /** Cached LanguageConnectionContext */
    private LanguageConnectionContext   lcc;

	/**	
		Factory for JDBC objects to be created.
	*/
	final InternalDriver factory;

	/**
		Calendar for data operations.
	*/
	private java.util.Calendar cal;


	ConnectionChild(EmbedConnection conn) {
		super();
		localConn = conn;
		factory = conn.getLocalDriver();
	}

	/**
		Return a reference to the EmbedConnection
	*/
	final EmbedConnection getEmbedConnection() {
		return localConn;
	}

	/**
	 * Return an object to be used for connection
	 * synchronization.
	 */
	final Object getConnectionSynchronization()
	{
		return localConn.getConnectionSynchronization();
	}

	/**
		Handle any exception.
		@see EmbedConnection#handleException
		@exception SQLException thrown if can't handle
	*/
	final SQLException handleException(Throwable t)
			throws SQLException {
		return localConn.handleException(t);
	}

	/**
		If Autocommit is on, note that a commit is needed.
		@see EmbedConnection#needCommit
	 */
	final void needCommit() {
		localConn.needCommit();
	}

	/**
		Perform a commit if one is needed.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	final void commitIfNeeded() throws SQLException {
		//System.out.println(this + " <> " + localConn.getClass());
		//new Throwable("cin").printStackTrace(System.out);
		localConn.commitIfNeeded();
	}

	/**
		Perform a commit if autocommit is enabled.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	final void commitIfAutoCommit() throws SQLException {
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
	final void setupContextStack() throws SQLException {
		localConn.setupContextStack();
	}

	/**
		Setup the context stack (a.k.a. context manager)
		for this connection.
		@see EmbedConnection#restoreContextStack
		@exception SQLException thrown on failure
	 */
	final void restoreContextStack() throws SQLException {
		localConn.restoreContextStack();
	}

	/**
		Get and save a unique calendar object for this JDBC object.
		No need to synchronize because multiple threads should not
		be using a single JDBC object. Even if they do there is only
		a small window where each would get its own Calendar for a
		single call.
	*/
	java.util.Calendar getCal() {
		if (cal == null)
			cal = new java.util.GregorianCalendar();
		return cal;
	}

    static SQLException newSQLException(String messageId, Object... args) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6262
        return EmbedConnection.newSQLException(messageId, args);
    }

    protected static void restoreIntrFlagIfSeen(
        boolean pushStack, EmbedConnection ec) {

//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        if (pushStack) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
            InterruptStatus.restoreIntrFlagIfSeen( getLCC( ec ) );
        } else {
            // no lcc if connection is closed:
            InterruptStatus.restoreIntrFlagIfSeen();
        }
    }
    
	/**
	  *	Get and cache the LanguageConnectionContext for this connection.
	  */
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
	LanguageConnectionContext	getLanguageConnectionContext( final EmbedConnection conn )
	{
        if ( lcc == null ) { lcc = getLCC( conn ); }

        return lcc;
	}
	/**
	  *	Gets the LanguageConnectionContext for this connection.
	  */
	static LanguageConnectionContext	getLCC( final EmbedConnection conn )
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
        return AccessController.doPrivileged
            (
             new PrivilegedAction<LanguageConnectionContext>()
             {
                 public LanguageConnectionContext run()
                 {
                     return conn.getLanguageConnection();
                 }
             }
             );
	}

}



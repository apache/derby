/*

   Derby - Class org.apache.derby.impl.jdbc.TransactionResourceImpl

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.error.ExceptionSeverity;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;
import java.sql.SQLException;

/** 
 *	An instance of a TransactionResourceImpl is a bundle of things that
 *	connects a connection to the database - it is the transaction "context" in
 *	a generic sense.  It is also the object of synchronization used by the
 *	connection object to make sure only one thread is accessing the underlying
 *	transaction and context.
 *
 *  <P>TransactionResourceImpl not only serves as a transaction "context", it
 *	also takes care of: <OL>
 *	<LI>context management: the pushing and popping of the context manager in
 *		and out of the global context service</LI>
 *	<LI>transaction demarcation: all calls to commit/abort/prepare/close a
 *		transaction must route thru the transaction resource.
 *	<LI>error handling</LI>
 *	</OL>
 *
 *  <P>The only connection that have access to the TransactionResource is the
 *  root connection, all other nested connections (called proxyConnection)
 *  accesses the TransactionResource via the root connection.  The root
 *  connection may be a plain EmbedConnection, or a DetachableConnection (in
 *  case of a XATransaction).  A nested connection must be a ProxyConnection.
 *  A proxyConnection is not detachable and can itself be a XA connection -
 *  although an XATransaction may start nested local (proxy) connections.
 *
 *	<P> this is an example of how all the objects in this package relate to each
 *		other.  In this example, the connection is nested 3 deep.  
 *		DetachableConnection.  
 *	<P><PRE>
 *
 *      lcc  cm   database  jdbcDriver
 *       ^    ^    ^         ^ 
 *       |    |    |         |
 *      |======================|
 *      | TransactionResource  |
 *      |======================|
 *             ^  |
 *             |  |
 *             |  |      |---------------rootConnection----------|
 *             |  |      |                                       |
 *             |  |      |- rootConnection-|                     |
 *             |  |      |                 |                     |
 *             |  V      V                 |                     |
 *|========================|      |=================|      |=================|
 *|    EmbedConnection     |      | EmbedConnection |      | EmbedConnection |
 *|                        |<-----|                 |<-----|                 |
 *| (DetachableConnection) |      | ProxyConnection |      | ProxyConnection |
 *|========================|      |=================|      |=================|
 *   ^                 | ^             ^                        ^
 *   |                 | |             |                        |
 *   ---rootConnection-- |             |                        |
 *                       |             |                        |
 *                       |             |                        |
 * |======================|  |======================|  |======================|
 * | ConnectionChild |  | ConnectionChild |  | ConnectionChild |
 * |                      |  |                      |  |                      |
 * |  (EmbedStatement)    |  |  (EmbedResultSet)    |  |  (...)               |
 * |======================|  |======================|  |======================|
 *
 * <P>A plain local connection <B>must</B> be attached (doubly linked with) to a
 * TransactionResource at all times.  A detachable connection can be without a
 * TransactionResource, and a TransactionResource for an XATransaction
 * (called  XATransactionResource) can be without a connection.
 *
 *
 */
public final class TransactionResourceImpl
{
	/*
	** instance variables set up in the constructor.
	*/
	// conn is only present if TR is attached to a connection
	protected ContextManager cm;
	protected ContextService csf;
	protected String username;

	private String dbname;
	private Driver169 driver;
	private String url;
	private String drdaID;

	// set these up after constructor, called by EmbedConnection
	protected Database database;
	protected LanguageConnectionContext lcc;

	/*
	 * create a brand new connection for a brand new transaction
	 */
	TransactionResourceImpl(
							Driver169 driver, 
							String url,
							Properties info) throws SQLException 
	{
		this.driver = driver;
		csf = driver.getContextServiceFactory();
		dbname = Driver169.getDatabaseName(url, info);
		this.url = url;

		// the driver manager will push a user name
		// into the properties if its getConnection(url, string, string)
		// interface is used.  Thus, we look there first.
		// Default to APP.
		username = info.getProperty(Attribute.USERNAME_ATTR,
									Property.DEFAULT_USER_NAME);
		if (username.equals(""))
			username = Property.DEFAULT_USER_NAME;

		drdaID = info.getProperty(Attribute.DRDAID_ATTR, null);

		// make a new context manager for this TransactionResource

		// note that the Database API requires that the 
		// getCurrentContextManager call return the context manager
		// associated with this session.  The JDBC driver assumes
		// responsibility (for now) for tracking and installing
		// this context manager for the thread, each time a database
		// call is made.
		cm = csf.newContextManager();
	}

	/*
	 * Called only in EmbedConnection construtor.
	 * The Local Connection sets up the database in its constructor and sets it
	 * here.
	 */
	void setDatabase(Database db)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(database == null, 
				"setting database when it is not null"); 

		database = db;
	}

	/*
	 * Called only in EmbedConnection constructor.  Create a new transaction
	 * by creating a lcc.
	 *
	 * The arguments are not used by this object, it is used by
	 * XATransactionResoruceImpl.  Put them here so that there is only one
	 * routine to start a local connection.
	 */
	void startTransaction() throws StandardException, SQLException
	{
		// setting up local connection
		lcc = database.setupConnection(cm, username, drdaID, dbname);
	}

	/*
	 * Return instance variables to EmbedConnection.  RESOLVE: given time, we
	 * should perhaps stop giving out reference to these things but instead use
	 * the transaction resource itself.
	 */
	Driver169 getDriver() {
		return driver;
	}
	ContextService getCsf() {
		return  csf;
	}

	/*
	 * need to be public because it is in the XATransactionResource interface
	 */
	ContextManager getContextManager() {
		return  cm;
	}

	LanguageConnectionContext getLcc() {
		return  lcc;
	}
	String getDBName() {
		return  dbname;
	}
	String getUrl() {
		return  url;
	}
	Database getDatabase() {
		return  database;
	}

	StandardException shutdownDatabaseException() {
		StandardException se = StandardException.newException(SQLState.SHUTDOWN_DATABASE, getDBName());
		se.setReport(StandardException.REPORT_NEVER);
		return se;
	}

	/*
	 * local transaction demarcation - note that global or xa transaction
	 * cannot commit thru the connection, they can only commit thru the
	 * XAResource, which uses the xa_commit or xa_rollback interface as a 
	 * safeguard. 
	 */
	protected void commit() throws StandardException
	{
		lcc.userCommit();
	}		

	protected void rollback() throws StandardException
	{
		// lcc may be null if this is called in close.
		if (lcc != null)
			lcc.userRollback();
	}

	/*
	 * context management
	 */

	/**
	 * An error happens in the constructor, pop the context.
	 */
	void clearContextInError()
	{
		csf.resetCurrentContextManager(cm);
		cm = null;
	}

	/**
	 * Resolve: probably superfluous
	 */
	protected void clearLcc()
	{
		lcc = null;
	}

	protected final void setupContextStack()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(cm != null, "setting up null context manager stack");
		}

			csf.setCurrentContextManager(cm);
	}

	protected final void restoreContextStack() {

		if ((csf == null) || (cm == null))
			return;
		csf.resetCurrentContextManager(cm);
	}

	/*
	 * exception handling
	 */

	/*
	 * clean up the error and wrap the real exception in some SQLException.
	 */
	protected final SQLException handleException(Throwable thrownException,
									   boolean autoCommit,	
									   boolean rollbackOnAutoCommit)
			throws SQLException 
	{
		try {
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(thrownException != null);

			/*
				just pass SQL exceptions right back. We assume that JDBC driver
				code has cleaned up sufficiently. Not passing them through would mean
				that all cleanupOnError methods would require knowledge of Utils.
			 */
			if (thrownException instanceof SQLException) {

				return (SQLException) thrownException;

			} 

			boolean checkForShutdown = false;
			if (thrownException instanceof StandardException)
			{
				StandardException se = (StandardException) thrownException;
				int severity = se.getSeverity();
				if (severity <= ExceptionSeverity.STATEMENT_SEVERITY)
				{
					/*
					** If autocommit is on, then do a rollback
					** to release locks if requested.  We did a stmt 
					** rollback in the cleanupOnError above, but we still
					** may hold locks from the stmt.
					*/
					if (autoCommit && rollbackOnAutoCommit)
					{
						se.setSeverity(ExceptionSeverity.TRANSACTION_SEVERITY);
					}
				} else if (SQLState.CONN_INTERRUPT.equals(se.getMessageId())) {
					// an interrupt closed the connection.
					checkForShutdown = true;
				}
			}
			// if cm is null, we don't have a connection context left,
			// it was already removed.  all that's left to cleanup is
			// JDBC objects.
			if (cm!=null) {
				boolean isShutdown = cleanupOnError(thrownException);
				if (checkForShutdown && isShutdown) {
					// Change the error message to be a known shutdown.
					thrownException = shutdownDatabaseException();
				}
			}



			return wrapInSQLException((SQLException) null, thrownException);

		} catch (Throwable t) {

			if (cm!=null) { // something to let us cleanup?
				cm.cleanupOnError(t);
			}
			/*
			   We'd rather throw the Throwable,
			   but then javac complains...
			   We assume if we are in this degenerate
			   case that it is actually a java exception
			 */
			throw wrapInSQLException((SQLException) null, t);
			//throw t;
		}

	}
		 
	public static final SQLException wrapInSQLException(SQLException sqlException, Throwable thrownException) {

		if (thrownException == null)
			return sqlException;

		SQLException nextSQLException;

		if (thrownException instanceof SQLException) {

			// server side JDBC can end up with a SQLException in the nested stack
			nextSQLException = (SQLException) thrownException;
		}
		else if (thrownException instanceof StandardException) {

			StandardException se = (StandardException) thrownException;

			nextSQLException = Util.generateCsSQLException(se);

			wrapInSQLException(nextSQLException, se.getNestedException());

		} else {

			nextSQLException = Util.javaException(thrownException);

			// special case some java exceptions that have nested exceptions themselves
			Throwable nestedByJVM = null;
			if (thrownException instanceof ExceptionInInitializerError) {
				nestedByJVM = ((ExceptionInInitializerError) thrownException).getException();
			} else if (thrownException instanceof java.lang.reflect.InvocationTargetException) {
				nestedByJVM = ((java.lang.reflect.InvocationTargetException) thrownException).getTargetException();
			}

			if (nestedByJVM != null) {
				wrapInSQLException(nextSQLException, nestedByJVM);
			}
			
		}

		if (sqlException != null) {
			sqlException.setNextException(nextSQLException);
		}

		return nextSQLException;
	}

	/*
	 * TransactionResource methods
	 */

	String getUserName() {
		return  username;
	}

	public boolean cleanupOnError(Throwable e)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(cm != null, "cannot cleanup on error with null context manager");

		return cm.cleanupOnError(e);
	}

	public boolean isIdle()
	{
		// If no lcc, there is no transaction.
		return (lcc == null || lcc.getTransactionExecute().isIdle());
	}


	/*
	 * class specific methods
	 */


	/* 
	 * is the underlaying database still active?
	 */
	boolean isActive()
	{
		// database is null at connection open time
		return (driver.isActive() && ((database == null) || database.isActive()));
	}

}



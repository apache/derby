/*

   Derby - Class org.apache.derby.impl.jdbc.TransactionResourceImpl

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

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.db.Database;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.shared.common.error.ExceptionSeverity;

import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.InterruptStatus;

import java.util.Properties;
import java.sql.SQLException;

/**
 *  <P>
 *	An instance of a TransactionResourceImpl is a bundle of things that
 *	connects a connection to the database - it is the transaction "context" in
 *	a generic sense.  It is also the object of synchronization used by the
 *	connection object to make sure only one thread is accessing the underlying
 *	transaction and context.
 *
 *  <P>
 *  TransactionResourceImpl not only serves as a transaction "context", it
 *	also takes care of:
 *  </P>
 *  <OL>
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
 *  </P>
 *
 *	<P> this is an example of how all the objects in this package relate to each
 *		other.  In this example, the connection is nested 3 deep.  
 *		DetachableConnection.  
 *	</P>
 * <PRE>
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
 *|                        |&lt;-----|                 |&lt;-----|                 |
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
 * </PRE>
 * <P>A plain local connection <B>must</B> be attached (doubly linked with) to a
 * TransactionResource at all times.  A detachable connection can be without a
 * TransactionResource, and a TransactionResource for an XATransaction
 * (called  XATransactionResource) can be without a connection.
 *  </P>
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
	private InternalDriver driver;
	private String url;
	private String drdaID;

	// set these up after constructor, called by EmbedConnection
	protected Database database;
	protected LanguageConnectionContext lcc;

	/**
	 * create a brand new connection for a brand new transaction
	 */
	TransactionResourceImpl(
							InternalDriver driver, 
							String url,
							Properties info) throws SQLException 
	{
		this.driver = driver;
		csf = driver.getContextServiceFactory();
		dbname = InternalDriver.getDatabaseName(url, info);
		this.url = url;

		// the driver manager will push a user name
		// into the properties if its getConnection(url, string, string)
		// interface is used.  Thus, we look there first.
		// Default to APP.
		username = IdUtil.getUserNameFromURLProps(info);
//IC see: https://issues.apache.org/jira/browse/DERBY-464

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

	/**
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

	/**
	 * Return instance variables to EmbedConnection.  RESOLVE: given time, we
	 * should perhaps stop giving out reference to these things but instead use
	 * the transaction resource itself.
	 */
	InternalDriver getDriver() {
		return driver;
	}
	ContextService getCsf() {
		return  csf;
	}

	/**
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

	/**
	 * local transaction demarcation - note that global or xa transaction
	 * cannot commit thru the connection, they can only commit thru the
	 * XAResource, which uses the xa_commit or xa_rollback interface as a 
	 * safeguard. 
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
	void commit() throws StandardException
	{
		lcc.userCommit();
	}		

	void rollback() throws StandardException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-467
	void clearLcc()
	{
		lcc = null;
	}

	final void setupContextStack()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(cm != null, "setting up null context manager stack");
		}

			csf.setCurrentContextManager(cm);
	}

	final void restoreContextStack() {
//IC see: https://issues.apache.org/jira/browse/DERBY-467

		if ((csf == null) || (cm == null))
			return;
		csf.resetCurrentContextManager(cm);
	}

	/*
	 * exception handling
	 */

	/**
	 * clean up the error and wrap the real exception in some SQLException.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
	final SQLException handleException(Throwable thrownException,
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

                InterruptStatus.restoreIntrFlagIfSeen();
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
			    //diagActive should be passed to cleanupOnError
			    //only if a session is active, Login errors are a special case where
			    // the database is active but the session is not.
				boolean sessionActive = (database != null) && database.isActive() && 
					!isLoginException(thrownException);
				boolean isShutdown = cleanupOnError(thrownException, sessionActive);
				if (checkForShutdown && isShutdown) {
					// Change the error message to be a known shutdown.
					thrownException = shutdownDatabaseException();
				}
			}

            InterruptStatus.restoreIntrFlagIfSeen();
//IC see: https://issues.apache.org/jira/browse/DERBY-4741

			return wrapInSQLException(thrownException);
//IC see: https://issues.apache.org/jira/browse/DERBY-1440
//IC see: https://issues.apache.org/jira/browse/DERBY-2472

		} catch (Throwable t) {

            if (cm != null) { // something to let us cleanup?
                cm.cleanupOnError(t, database != null ? isActive() : false);
			}

            InterruptStatus.restoreIntrFlagIfSeen();
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
//IC see: https://issues.apache.org/jira/browse/DERBY-4741

			/*
			   We'd rather throw the Throwable,
			   but then javac complains...
			   We assume if we are in this degenerate
			   case that it is actually a java exception
			 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1440
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
			throw wrapInSQLException(t);
			//throw t;
		}

	}

    /**
     * Determine if the exception thrown is a login exception.
     * Needed for DERBY-5427 fix to prevent inappropriate thread dumps
     * and javacores. This exception is special because it is 
     * SESSION_SEVERITY and database.isActive() is true, but the 
     * session hasn't started yet,so it is not an actual crash and 
     * should not report extended diagnostics.
     * 
     * @param thrownException
     * @return true if this is a login failure exception
     */
    private boolean isLoginException(Throwable thrownException) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6426
       if ((thrownException instanceof StandardException) &&
           ((StandardException) thrownException).getSQLState().equals(SQLState.LOGIN_FAILED)) {
           return true;
       }
       return false;
    }
    
    /**
     * Wrap a <code>Throwable</code> in an <code>SQLException</code>.
     *
     * @param thrownException a <code>Throwable</code>
     * @return <code>thrownException</code>, if it is an
     * <code>SQLException</code>; otherwise, an <code>SQLException</code> which
     * wraps <code>thrownException</code>
     */
	public static SQLException wrapInSQLException(Throwable thrownException) {

		if (thrownException == null)
			return null;

		if (thrownException instanceof SQLException) {
            // Server side JDBC can end up with a SQLException in the nested
            // stack. Return the exception with no wrapper.
//IC see: https://issues.apache.org/jira/browse/DERBY-1440
            return (SQLException) thrownException;
		}

        if (thrownException instanceof StandardException) {

			StandardException se = (StandardException) thrownException;

//IC see: https://issues.apache.org/jira/browse/DERBY-5243
            if (SQLState.CONN_INTERRUPT.equals(se.getSQLState())) {
                Thread.currentThread().interrupt();
            }

            if (se.getCause() == null) {
                // se is a single, unchained exception. Just convert it to an
                // SQLException.
                return Util.generateCsSQLException(se);
            }

            // se contains a non-empty exception chain. We want to put all of
            // the exceptions (including Java exceptions) in the next-exception
            // chain. Therefore, call wrapInSQLException() recursively to
            // convert the cause chain into a chain of SQLExceptions.
            return Util.seeNextException(se.getMessageId(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
                        wrapInSQLException(se.getCause()), se.getCause(),
                        se.getArguments());
        }

        // thrownException is a Java exception
        return Util.javaException(thrownException);
	}

	/*
	 * TransactionResource methods
	 */

	String getUserName() {
		return  username;
	}

    /**
     * clean up error and print it to derby.log if diagActive is true
     * @param e the error we want to clean up
     * @param diagActive
     *        true if extended diagnostics should be considered, 
     *        false not interested of extended diagnostic information
     * @return true if the context manager is shutdown, false otherwise.
     */
    boolean cleanupOnError(Throwable e, boolean diagActive)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(cm != null, "cannot cleanup on error with null context manager");

        //DERBY-4856 thread dump
        return cm.cleanupOnError(e, diagActive);
	}

	boolean isIdle()
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



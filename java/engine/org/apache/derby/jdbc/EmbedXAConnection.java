/*

   Derby - Class org.apache.derby.jdbc.EmbedXAConnection

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.iapi.store.access.xa.XAResourceManager;
import org.apache.derby.iapi.store.access.XATransactionController;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedConnection20;
import org.apache.derby.impl.jdbc.TransactionResourceImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.iapi.jdbc.BrokeredConnection;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.JDBC30Translation;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

/* import jta packages */
import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;

/** -- jdbc 2.0. extension -- */
import javax.sql.XAConnection;

/** 
 */
final class EmbedXAConnection extends EmbedPooledConnection
		implements XAConnection, XAResource

{


	final ResourceAdapter ra;

	XAXactId	currentXid;


	EmbedXAConnection(EmbeddedDataSource ds, ResourceAdapter ra, String u, String p, boolean requestPassword) throws SQLException
	{
		super(ds, u, p, requestPassword);
		this.ra = ra;

	}

	/*
	** XAConnection methods
	*/

	public final synchronized XAResource getXAResource() throws SQLException {
		checkActive();
		return this;
	}

	/*
	** XAResource methods
	*/

	/**
		Start work on behalf of a transaction branch specified in xid If TMJOIN
		is specified, the start is for joining a transaction previously seen by
		the resource manager. If TMRESUME is specified, the start is to resume
		a suspended transaction specified in the parameter xid. If neither
		TMJOIN nor TMRESUME is specified and the transaction specified by xid
		has previously been seen by the resource manager, the resource manager
		throws the XAException exception with XAER_DUPID error code.

		@param xid A global transaction identifier to be associated with the
				resource 
		@param flags One of TMNOFLAGS, TMJOIN, or TMRESUME 

		@exception XAException An error has occurred. Possible exceptions are
		XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_DUPID, XAER_OUTSIDE, XAER_NOTA,
		XAER_INVAL, or XAER_PROTO.
	 */
	public final synchronized void start(Xid xid, int flags) throws XAException
	{
		checkXAActive();

		// JDBC 3.0 section 12.3 - One transaction associated with a XAConnection
		if (currentXid != null)
			throw new XAException(XAException.XAER_PROTO);

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		XATransactionState tranState = getTransactionState(xid_im);

		switch (flags) {
		case XAResource.TMNOFLAGS:
			if (tranState != null)
				throw new XAException(XAException.XAER_DUPID);

			try {

				if (realConnection == null) {
					openRealConnection();

					if (currentConnectionHandle != null) {

						// since this is a new connection, set its complete
						// state according to the application's Connection
						// handle view of the world.
						currentConnectionHandle.setState(true);
						realConnection.setApplicationConnection(currentConnectionHandle);
					}

				} else {

					// XAResource.start() auto commits in DB2 when in auto commit mode.
					if (currentConnectionHandle != null) {
						if (currentConnectionHandle.getAutoCommit())
							currentConnectionHandle.rollback();
					}
					if (!realConnection.transactionIsIdle())
						throw new XAException(XAException.XAER_OUTSIDE);

					if (currentConnectionHandle != null) {

						// we have a current handle so we need to keep
						// the connection state of the current connection.
						currentConnectionHandle.setState(true);

						// At the local to global transition we need to discard
						// and close any open held result sets, a rollback will do this.
						realConnection.rollback();
					} else {
						resetRealConnection();
					}

				}

				// Global connections are always in auto commit false mode.
				realConnection.setAutoCommit(false);

				// and holdability false (cannot hold cursors across XA transactions.
				realConnection.setHoldability(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT);

				realConnection.getLanguageConnection().
					getTransactionExecute().createXATransactionFromLocalTransaction(
						 xid_im.getFormatId(), 
						 xid_im.getGlobalTransactionId(),
						 xid_im.getBranchQualifier());


			} catch (StandardException se) {
				throw wrapInXAException(se);
			
			} catch (SQLException sqle) {
				throw wrapInXAException(sqle);
			}


			if (!ra.addConnection(xid_im, new XATransactionState(realConnection.getContextManager(), realConnection, this, xid_im)))
				throw new XAException(XAException.XAER_DUPID);

			break;

		case XAResource.TMRESUME:
		case XAResource.TMJOIN:
			if (tranState == null)
				throw new XAException(XAException.XAER_NOTA);

			tranState.start(this, flags);

			if (tranState.conn != realConnection) {

				if (realConnection != null) {

					if (!realConnection.transactionIsIdle())
						throw new XAException(XAException.XAER_OUTSIDE);

					closeUnusedConnection(realConnection);
				}
				realConnection = tranState.conn;

				if (currentConnectionHandle != null) {

					try {
						// only reset the non-transaction specific Connection state.
						currentConnectionHandle.setState(false);
						realConnection.setApplicationConnection(currentConnectionHandle);
					} catch (SQLException sqle) {
						throw wrapInXAException(sqle);
					}
				}

			}


			break;

		default:
			throw new XAException(XAException.XAER_INVAL);
		}

		currentXid = xid_im;
	}


	/**
		Ends the work performed on behalf of a transaction branch. The resource
		manager disassociates the XA resource from the transaction branch
		specified and let the transaction be completed.  

		<p> If TMSUSPEND is specified in flags, the transaction branch is
		temporarily suspended in incomplete state. The transaction context 
		is in suspened state and must be resumed via start with TMRESUME
		specified. 

		<p> If TMFAIL is specified, the portion of work has failed. The
		resource manager may mark the transaction as rollback-only 

		<p> If TMSUCCESS is specified, the portion of work has completed
		successfully. 

		@param xid A global transaction identifier that is the same as what was
				used previously in the start method. 
		@param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND

		@exception XAException An error has occurred. 
		Possible XAException values are XAER_RMERR, XAER_RMFAILED, XAER_NOTA,
		XAER_INVAL, XAER_PROTO, or XA_RB*. 
	 */
	public final synchronized void end(Xid xid, int flags) throws XAException
	{
		checkXAActive();

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		boolean endingCurrentXid = false;

		// must match the Xid from start()
		if (currentXid != null) {
			if (!currentXid.equals(xid_im))
				throw new XAException(XAException.XAER_PROTO);
			endingCurrentXid = true;
		}

		XATransactionState tranState = getTransactionState(xid_im);
		if (tranState == null)
			throw new XAException(XAException.XAER_NOTA);

		boolean rollbackOnly = tranState.end(this, flags, endingCurrentXid);

		// RESOLVE - what happens to the connection on a fail
		// where we are not ending the current XID.
		if (endingCurrentXid) {
			currentXid = null;

			realConnection = null;
		}

		if (rollbackOnly)
			throw new XAException(tranState.rollbackOnlyCode); 

	}

	/**
		Ask the resource manager to prepare for a transaction commit of the
		transaction specified in xid. 

		@param xid A global transaction identifier

		@return A value indicating the resource manager's vote on the outcome
		of the transaction. The possible values are: XA_RDONLY or XA_OK. If the
		resource manager wants to roll back the transaction, it should do so by
		raising an appropriate XAException in the prepare method.

		@exception XAException An error has occurred. Possible exception values
		are: XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or
		XAER_PROTO.

	 */
	public final synchronized int prepare(Xid xid) throws XAException
	{
		checkXAActive();

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		XATransactionState tranState = getTransactionState(xid_im);

		if (tranState == null) {
			XAResourceManager rm = ra.getXAResourceManager();

			ContextManager inDoubtCM = rm.find(xid);

			// RM also does not know about this xid.
			if (inDoubtCM == null)
				throw new XAException(XAException.XAER_NOTA);

			// cannot prepare in doubt transactions
			throw new XAException(XAException.XAER_PROTO);

		}

		synchronized (tranState) {

			checkUserCredentials(tranState.creatingResource);

			// Check the transaction is no associated with
			// any XAResource.
			switch (tranState.associationState) {
			case XATransactionState.T0_NOT_ASSOCIATED:
				break;

			case XATransactionState.TRO_FAIL:
				throw new XAException(tranState.rollbackOnlyCode);

			default:
				throw new XAException(XAException.XAER_PROTO);
			}

			if (tranState.suspendedList != null && tranState.suspendedList.size() != 0)
				throw new XAException(XAException.XAER_PROTO);

			if (tranState.isPrepared)
				throw new XAException(XAException.XAER_PROTO);

			EmbedConnection20 conn = tranState.conn; 

			try {

				int ret = conn.xa_prepare();

				if (ret == XATransactionController.XA_OK) {
					tranState.isPrepared = true;

					return XAResource.XA_OK;
				} else {

					returnConnectionToResource(tranState, xid_im);
					return XAResource.XA_RDONLY;
				}
			} catch (SQLException sqle) {
				throw wrapInXAException(sqle);
			}
		}
	
	}

	/**
		Commit the global transaction specified by xid.
		@param xid A global transaction identifier
		@param onePhase If true, the resource manager should use a one-phase
				commit protocol to commit the work done on behalf of xid.

		@exception XAException An error has occurred. Possible XAExceptions are
				XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
				XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.  
				<P>If the resource manager did not commit the transaction and
				the paramether onePhase is set to true, the resource manager 
				may throw one of the XA_RB* exceptions. Upon return, the
				resource manager has rolled back the branch's work and has 
				released all held resources.
	*/
	public final synchronized void commit(Xid xid, boolean onePhase) throws XAException
	{
		checkXAActive();

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		XATransactionState tranState = getTransactionState(xid_im);

		if (tranState == null) {

			XAResourceManager rm = ra.getXAResourceManager();

			ContextManager inDoubtCM = rm.find(xid);

			// RM also does not know about this xid.
			if (inDoubtCM == null)
				throw new XAException(XAException.XAER_NOTA);

			ContextService csf = ContextService.getFactory();

			csf.setCurrentContextManager(inDoubtCM);
			try
			{
				rm.commit(inDoubtCM, xid_im, onePhase);

				// close the connection/transaction since it can never be used again.
				inDoubtCM.cleanupOnError(StandardException.closeException());
				return;
			}
			catch (StandardException se)
			{
				// The rm threw an exception, clean it up in the approprate
				// context.  There is no transactionResource to handle the
				// exception for us.
				inDoubtCM.cleanupOnError(se);
				throw wrapInXAException(se);
			}
			finally
			{
				csf.resetCurrentContextManager(inDoubtCM);
			}

		}

		synchronized (tranState) {
			checkUserCredentials(tranState.creatingResource);
		
			// Check the transaction is no associated with
			// any XAResource.
			switch (tranState.associationState) {
			case XATransactionState.T0_NOT_ASSOCIATED:
				break;

			case XATransactionState.TRO_FAIL:
				throw new XAException(tranState.rollbackOnlyCode);

			default:
				throw new XAException(XAException.XAER_PROTO);
			}

			if (tranState.suspendedList != null && tranState.suspendedList.size() != 0)
				throw new XAException(XAException.XAER_PROTO);

			if (tranState.isPrepared == onePhase)
				throw new XAException(XAException.XAER_PROTO);

			EmbedConnection20 conn = tranState.conn; 

			try {

				conn.xa_commit(onePhase);

			} catch (SQLException sqle) {
				throw wrapInXAException(sqle);
			} finally {
				returnConnectionToResource(tranState, xid_im);
			}
		}
	}

	/** 
		Inform the resource manager to roll back work done on behalf of a
		transaction branch

		@param xid A global transaction identifier
		@exception XAException - An error has occurred 
	*/
	public final synchronized void rollback(Xid xid) throws XAException
	{
		checkXAActive();

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		XATransactionState tranState = getTransactionState(xid_im);

		if (tranState == null) {
			XAResourceManager rm = ra.getXAResourceManager();

			ContextManager inDoubtCM = rm.find(xid);

			// RM also does not know about this xid.
			if (inDoubtCM == null)
				throw new XAException(XAException.XAER_NOTA);

			ContextService csf = ContextService.getFactory();

			csf.setCurrentContextManager(inDoubtCM);
			try
			{
				rm.rollback(inDoubtCM, xid_im);

				// close the connection/transaction since it can never be used again.
				inDoubtCM.cleanupOnError(StandardException.closeException());
				return;
			}
			catch (StandardException se)
			{
				// The rm threw an exception, clean it up in the approprate
				// context.  There is no transactionResource to handle the
				// exception for us.
				inDoubtCM.cleanupOnError(se);
				throw wrapInXAException(se);
			}
			finally
			{
				csf.resetCurrentContextManager(inDoubtCM);
			}

		}

		synchronized (tranState) {
		
			// Check the transaction is no associated with
			// any XAResource.
			switch (tranState.associationState) {
			case XATransactionState.T0_NOT_ASSOCIATED:
			case XATransactionState.TRO_FAIL:
				break;
				
			default:
				throw new XAException(XAException.XAER_PROTO);
			}

			if (tranState.suspendedList != null && tranState.suspendedList.size() != 0)
				throw new XAException(XAException.XAER_PROTO);

			checkUserCredentials(tranState.creatingResource);

			try {

				tranState.conn.xa_rollback();
			} catch (SQLException sqle) {
				throw wrapInXAException(sqle);
			} finally {
				returnConnectionToResource(tranState, xid_im);
			}
		}
	}

	/**
		Obtain a list of prepared transaction branches from a resource
		manager. The transaction manager calls this method during recovery to
		obtain the list of transaction branches that are currently in prepared
		or heuristically completed states. 

		@param flag One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must
		be used when no other flags are set in flags.

		@return The resource manager returns zero or more XIDs for the
		transaction branches that are currently in a prepared or heuristically
		completed state. If an error occurs during the operation, the resource
		manager should throw the appropriate XAException.

		@exception XAException An error has occurred. Possible values are
		XAER_RMERR, XAER_RMFAIL, XAER_INVAL, and XAER_PROTO.

	*/
	public final synchronized Xid[] recover(int flag) throws XAException
	{
		checkXAActive();

		try
		{
			return ra.getXAResourceManager().recover(flag);
		}
		catch (StandardException se)
		{
			throw wrapInXAException(se);
		}
	}

	/**
		Tell the resource manager to forget about a heuristically completed
		transaction branch. 

		@param xid A global transaction identifier
		@exception XAException An error has occurred. Possible exception values
		are XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
	 */
	public final synchronized void forget(Xid xid) throws XAException {

		checkXAActive();

		// ensure immtable and correct equals method.
		XAXactId xid_im = new XAXactId(xid);

		XATransactionState tranState = getTransactionState(xid_im);
		if (tranState == null) {
			XAResourceManager rm = ra.getXAResourceManager();

			ContextManager inDoubtCM = rm.find(xid);

			// RM also does not know about this xid.
			if (inDoubtCM == null)
				throw new XAException(XAException.XAER_NOTA);

			ContextService csf = ContextService.getFactory();

			csf.setCurrentContextManager(inDoubtCM);
			try
			{
				rm.forget(inDoubtCM, xid_im);

				// close the connection/transaction since it can never be used again.
				inDoubtCM.cleanupOnError(StandardException.closeException());
				return;
			}
			catch (StandardException se)
			{
				// The rm threw an exception, clean it up in the approprate
				// context.  There is no transactionResource to handle the
				// exception for us.
				inDoubtCM.cleanupOnError(se);
				throw wrapInXAException(se);
			}
			finally
			{
				csf.resetCurrentContextManager(inDoubtCM);
			}

		}

		throw new XAException(tranState.isPrepared ? XAException.XAER_NOTA : XAException.XAER_PROTO);

	}
	/** 
		This method is called to determine if the resource manager instance
		represented by the target object is the same as the resouce manager
		instance represented by the parameter xares.

		@param xares An XAResource object whose resource manager instance is to
		be compared with the resource manager instance of the target object.

		@return true if it's the same RM instance; otherwise false.
 		@exception XAException An error has occurred. Possible exception values
		are XAER_RMERR, XAER_RMFAIL. 
	 */
	public final synchronized boolean isSameRM(XAResource xares) throws XAException {
		checkXAActive();

		if (xares instanceof EmbedXAConnection) {

			return ra == ((EmbedXAConnection) xares).ra;
		}

		return false;
	}
	/**
		Obtain the current transaction timeout value set for this XAResource
		instance. If XAResource.setTransactionTimeout was not use prior to
		invoking this method, the return value is the default timeout set for
		the resource manager; otherwise, the value used in the previous
		setTransactionTimeout call is returned. 

		@return the transaction timeout value in seconds.
	 */
	public int getTransactionTimeout()
	{
		return 0;
	}

	/**
		Set the current transaction timeout value for this XAResource
		instance. Once set, this timeout value is effective until
		setTransactionTimeout is invoked again with a different value. To reset
		the timeout value to the default value used by the resource manager,
		set the value to zero. If the timeout operation is performed
		successfully, the method returns true; otherwise false. If a resource
		manager does not support transaction timeout value to be set
		explicitly, this method returns false.

		@param seconds the transaction timeout value in seconds.
		@return true if transaction timeout value is set successfully;
		otherwise false. 

		@exception XAException - An error has occurred. Possible exception
		values are XAER_RMERR, XAER_RMFAIL, or XAER_INVAL.
	 */
	public boolean setTransactionTimeout(int seconds)
	{
		return false;
	}

	/*
	** BrokeredConnectionControl api
	*/
	/**
		Allow control over setting auto commit mode.
	*/
	public void checkAutoCommit(boolean autoCommit) throws SQLException {
		if (autoCommit && (currentXid != null))
			throw Util.generateCsSQLException(SQLState.CANNOT_AUTOCOMMIT_XA);

		super.checkAutoCommit(autoCommit);
	}
	/**
		Are held cursors allowed.
	*/
	public void checkHoldCursors(int holdability) throws SQLException {

		if (holdability == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {		
			if (currentXid != null)
				throw Util.generateCsSQLException(SQLState.CANNOT_HOLD_CURSOR_XA);
		}

		super.checkHoldCursors(holdability);
	}

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	public void checkSavepoint() throws SQLException {

		if (currentXid != null)
			throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);

		super.checkSavepoint();
	}

	/**
		Allow control over calling rollback.
	*/
	public void checkRollback() throws SQLException {

		if (currentXid != null)
			throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);

		super.checkRollback();
	}
	/**
		Allow control over calling commit.
	*/
	public void checkCommit() throws SQLException {

		if (currentXid != null)
			throw Util.generateCsSQLException(SQLState.CANNOT_COMMIT_XA);

		super.checkCommit();
	}

	public Connection getConnection() throws SQLException
	{
		Connection handle;

		// Is this just a local transaction?
		if (currentXid == null) {
			handle = super.getConnection();
		} else {

			if (currentConnectionHandle != null) {
				// this can only happen if someone called start(Xid),
				// getConnection, getConnection (and we are now the 2nd
				// getConnection call).
				// Cannot yank a global connection away like, I don't think... 
				throw Util.generateCsSQLException(
							   SQLState.CANNOT_CLOSE_ACTIVE_XA_CONNECTION);
			}

			handle = getNewCurrentConnectionHandle();
		}

		currentConnectionHandle.syncState();

		return handle;
	}

	/**
		Wrap and control a Statement
	*/
	public Statement wrapStatement(Statement s) throws SQLException {
		XAStatementControl sc = new XAStatementControl(this, s);
		return sc.applicationStatement;
	}
	/**
		Wrap and control a PreparedStatement
	*/
	public PreparedStatement wrapStatement(PreparedStatement ps, String sql, Object generatedKeys) throws SQLException {
		XAStatementControl sc = new XAStatementControl(this, ps, sql, generatedKeys);
		return (PreparedStatement) sc.applicationStatement;
	}
	/**
		Wrap and control a PreparedStatement
	*/
	public CallableStatement wrapStatement(CallableStatement cs, String sql) throws SQLException {
		XAStatementControl sc = new XAStatementControl(this, cs, sql);
		return (CallableStatement) sc.applicationStatement;
	}

	/**
		Override getRealConnection to create a a local connection
		when we are not associated with an XA transaction.

		This can occur if the application has a Connection object (conn)
		and the following sequence occurs.

		conn = xac.getConnection();
		xac.start(xid, ...)
		
		// do work with conn

		xac.end(xid, ...);

		// do local work with conn
		// need to create new connection here.
	*/
	public Connection getRealConnection() throws SQLException
	{
		Connection rc = super.getRealConnection();
		if (rc != null)
			return rc;

		openRealConnection();

		// a new Connection, set its state according to the application's Connection handle
		currentConnectionHandle.setState(true);

		return realConnection;
	}


	/*
	** Class specific methods
	*/

	private XATransactionState getTransactionState(XAXactId xid_im) {

		return (XATransactionState) ra.findConnection(xid_im);
	}


	/**
		Map a SQL exception to appropriate XAException.
		Return the mapped XAException.
	 */
	private static XAException wrapInXAException(SQLException se)
    {
		// Map interesting exceptions to XAException
		String sqlstate = se.getSQLState();
		String message = se.getMessage();

		XAException xae;

		if (sqlstate == null)
		{
			// no idea what was wrong, throw non-descript error. 
			if (message != null)
				xae = new XAException(message);
			else
				xae = new XAException(XAException.XAER_RMERR);
		}
		else if (sqlstate.equals(StandardException.getSQLStateFromIdentifier(SQLState.STORE_XA_XAER_DUPID)))
			xae = new XAException(XAException.XAER_DUPID);
		else if (sqlstate.equals(StandardException.getSQLStateFromIdentifier(SQLState.STORE_XA_PROTOCOL_VIOLATION)))
			xae = new XAException(XAException.XA_RBPROTO);
		else if (sqlstate.equals(SQLState.DEADLOCK))
			xae = new XAException(XAException.XA_RBDEADLOCK);
		else if (sqlstate.equals(SQLState.LOCK_TIMEOUT))
			xae = new XAException(XAException.XA_RBTIMEOUT);
		else if (message != null)
			xae = new XAException(message);
		else
			xae = new XAException(XAException.XAER_RMERR);

		if (org.apache.derby.iapi.services.info.JVMInfo.JDK_ID >= 4)
			xae.initCause(se);
		return xae;
	}
	/**
		Map a Standard exception to appropriate XAException.
		Return the mapped XAException.
	 */
	private static XAException wrapInXAException(StandardException se)
	{
		return wrapInXAException(TransactionResourceImpl.wrapInSQLException((SQLException) null, se));
	}

	void removeXATransaction(XAXactId xid_im) {
		XATransactionState tranState = (XATransactionState) ra.removeConnection(xid_im);
		if (tranState != null)
			tranState.popMe();
	}


	/**
		Return an underlying connection object back to its XAResource
		if possible. If not close the connection.
	*/
	private void returnConnectionToResource(XATransactionState tranState, XAXactId xid_im) {

		removeXATransaction(xid_im);

		synchronized (tranState) {
			// global transaction is over.
			tranState.associationState = XATransactionState.TC_COMPLETED;
			tranState.notifyAll();

			EmbedConnection20 conn = tranState.conn;

			// already set in its own resource
			// or can it be returned to its original resource?
			if ((tranState.creatingResource.realConnection == conn) ||
				(tranState.creatingResource.realConnection == null)) {

				tranState.creatingResource.realConnection = conn;

				BrokeredConnection handle = tranState.creatingResource.currentConnectionHandle;

				conn.setApplicationConnection(handle);

				if (handle != null) {
					try {
						handle.setState(true);
					} catch (SQLException sqle) {

						// couldn't reset the connection
						closeUnusedConnection(tranState.conn);
						tranState.creatingResource.realConnection = null;
					}
				}
				return;
			}
		}

		// nowhere to place it, close it.
		closeUnusedConnection(tranState.conn);
	}

	private void checkXAActive() throws XAException {

		try {
			checkActive();
		} catch (SQLException sqle) {
			throw wrapInXAException(sqle);
		}
	}

	private void checkUserCredentials(EmbedXAConnection original)
		throws XAException {

		if (original == this)
			return;

		if (original.getPassword().equals(getPassword()) && (original.getUsername().equals(getUsername())))
			return;


		throw new XAException(XAException.XA_RBINTEGRITY);
	}

	/**
		Close  an underlying connection object when there is
		no active XAResource to hand it to.
	*/
	private static void closeUnusedConnection(EmbedConnection20 conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException sqle) {
			}
		}
	}
}

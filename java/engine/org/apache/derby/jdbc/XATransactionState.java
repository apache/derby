/*

   Derby - Class org.apache.derby.jdbc.XATransactionState

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


import org.apache.derby.impl.jdbc.EmbedConnection20;
import javax.transaction.xa.XAResource;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.iapi.reference.SQLState;
import java.util.HashMap;
import javax.transaction.xa.XAException;

/** 
*/
public final class XATransactionState extends ContextImpl {

	/** Rollback-only due to deadlock */
	final static int TRO_DEADLOCK				= -2;
	/** Rollback-only due to end(TMFAIL) */
	final static int TRO_FAIL					= -1;
	final static int T0_NOT_ASSOCIATED			= 0;
	final static int T1_ASSOCIATED				= 1;
	// final static int T2_ASSOCIATION_SUSPENDED	= 2;
	final static int TC_COMPLETED				= 3; // rollback/commit called

	public final EmbedConnection20	conn;
	final EmbedXAConnection creatingResource;
	private EmbedXAConnection  associatedResource;	// owning XAResource
	final XAXactId			xid;	
	/**
		When an XAResource suspends a transaction (end(TMSUSPEND)) it must be resumed
		using the same XAConnection. This has been the traditional Cloudscape behaviour,
		though there does not seem to be a specific reference to this behaviour in
		the JTA spec. Note that while the transaction is suspended by this XAResource,
		another XAResource may join the transaction and suspend it after the join.
	*/
	HashMap suspendedList;


	/**
		Association state of the transaction.
	*/
	int associationState;

	int rollbackOnlyCode;


	/**
		has this transaction been prepared.
	*/
	boolean isPrepared;

	XATransactionState(ContextManager cm, EmbedConnection20 conn, EmbedXAConnection resource, XAXactId xid) {

		super(cm, "XATransactionState");
		this.conn = conn;
		this.associatedResource = resource;
		this.creatingResource = resource;
		this.associationState = XATransactionState.T1_ASSOCIATED;
		this.xid = xid;

	}

	public void cleanupOnError(Throwable t) {

		if (t instanceof StandardException) {

			StandardException se = (StandardException) t;

			if (se.getSeverity() == ExceptionSeverity.TRANSACTION_SEVERITY) {

				synchronized (this) {
					// disable use of the connection until it is cleaned up.
					conn.setApplicationConnection(null);
					notifyAll();
					associationState = TRO_FAIL;
					if (SQLState.DEADLOCK.equals(se.getMessageId()))
						rollbackOnlyCode = XAException.XA_RBDEADLOCK;
					else if (SQLState.LOCK_TIMEOUT.equals(se.getMessageId()))
						rollbackOnlyCode = XAException.XA_RBTIMEOUT;					
					else
						rollbackOnlyCode = XAException.XA_RBOTHER;
				}
			}
		}
	}

	void start(EmbedXAConnection resource, int flags) throws XAException {

		synchronized (this) {
			if (associationState == XATransactionState.TRO_FAIL)
				throw new XAException(rollbackOnlyCode);

			boolean isSuspendedByResource = (suspendedList != null) && (suspendedList.get(resource) != null);

			if (flags == XAResource.TMRESUME) {
				if (!isSuspendedByResource)
					throw new XAException(XAException.XAER_PROTO);

			} else {
				// cannot join a transaction we have suspended.
				if (isSuspendedByResource)
					throw new XAException(XAException.XAER_PROTO);
			}

			while (associationState == XATransactionState.T1_ASSOCIATED) {
				
				try {
					wait();
				} catch (InterruptedException ie) {
					throw new XAException(XAException.XA_RETRY);
				}
			}


			switch (associationState) {
			case XATransactionState.T0_NOT_ASSOCIATED:
				break;

			case XATransactionState.TRO_FAIL:
				throw new XAException(rollbackOnlyCode);

			default:
				throw new XAException(XAException.XAER_NOTA);
			}

			if (isPrepared)
				throw new XAException(XAException.XAER_PROTO);

			if (isSuspendedByResource) {
				suspendedList.remove(resource);
			}

			associationState = XATransactionState.T1_ASSOCIATED;
			associatedResource = resource;
		}
	}

	boolean end(EmbedXAConnection resource, int flags, boolean endingCurrentXid) throws XAException {

		boolean rollbackOnly = false;
		synchronized (this) {


			boolean isSuspendedByResource = (suspendedList != null) && (suspendedList.get(resource) != null);

			if (!endingCurrentXid) {
				while (associationState == XATransactionState.T1_ASSOCIATED) {
					
					try {
						wait();
					} catch (InterruptedException ie) {
						throw new XAException(XAException.XA_RETRY);
					}
				}
			}

			switch (associationState) {
			case XATransactionState.TC_COMPLETED:
				throw new XAException(XAException.XAER_NOTA);
			case XATransactionState.TRO_FAIL:
				if (endingCurrentXid)
					flags = XAResource.TMFAIL;
				else
					throw new XAException(rollbackOnlyCode);
			}

			boolean notify = false;
			switch (flags) {
			case XAResource.TMSUCCESS:
				if (isSuspendedByResource) {
					suspendedList.remove(resource);
				}
				else {
					if (resource != associatedResource)
						throw new XAException(XAException.XAER_PROTO);

					associationState = XATransactionState.T0_NOT_ASSOCIATED;
					associatedResource = null;
					notify = true;
				}

				conn.setApplicationConnection(null);
				break;

			case XAResource.TMFAIL:

				if (isSuspendedByResource) {
					suspendedList.remove(resource);
				} else {
					if (resource != associatedResource)
						throw new XAException(XAException.XAER_PROTO);
					associatedResource = null;
				}
				
				if (associationState != XATransactionState.TRO_FAIL) {
					associationState = XATransactionState.TRO_FAIL;
					rollbackOnlyCode = XAException.XA_RBROLLBACK;
				}
				conn.setApplicationConnection(null);
				notify = true;
				rollbackOnly = true;
				break;

			case XAResource.TMSUSPEND:
				if (isSuspendedByResource)
					throw new XAException(XAException.XAER_PROTO);
				
				if (resource != associatedResource)
					throw new XAException(XAException.XAER_PROTO);

				if (suspendedList == null)
					suspendedList = new HashMap();
				suspendedList.put(resource, this);

				associationState = XATransactionState.T0_NOT_ASSOCIATED;
				associatedResource = null;
				conn.setApplicationConnection(null);
				notify = true;

				break;

			default:
				throw new XAException(XAException.XAER_INVAL);
			}

			if (notify)
				notifyAll();

			return rollbackOnly;
		}
	}

}

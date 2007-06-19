/*

   Derby - Class org.apache.derby.jdbc.XATransactionState

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.jdbc;


import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.timer.TimerFactory;
import org.apache.derby.impl.jdbc.EmbedConnection;
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
final class XATransactionState extends ContextImpl {

    /** Rollback-only due to timeout */
    final static int TRO_TIMEOUT                = -3;
	/** Rollback-only due to deadlock */
	final static int TRO_DEADLOCK				= -2;
	/** Rollback-only due to end(TMFAIL) */
	final static int TRO_FAIL					= -1;
	final static int T0_NOT_ASSOCIATED			= 0;
	final static int T1_ASSOCIATED				= 1;
	// final static int T2_ASSOCIATION_SUSPENDED	= 2;
	final static int TC_COMPLETED				= 3; // rollback/commit called

	final EmbedConnection	conn;
	final EmbedXAResource creatingResource;
        // owning XAResource
	private EmbedXAResource  associatedResource;	
	final XAXactId			xid;	
	/**
		When an XAResource suspends a transaction (end(TMSUSPEND)) it must be resumed
		using the same XAConnection. This has been the traditional Cloudscape/Derby behaviour,
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

    /** Has this transaction been finished (committed
      * or rolled back)? */
    boolean isFinished;

    /** A timer task scheduled for the time when the transaction will timeout. */
    CancelXATransactionTask timeoutTask = null;


    /** The implementation of TimerTask to cancel a global transaction. */
    private class CancelXATransactionTask extends TimerTask {

        /** Creates the cancelation object to be passed to a timer. */
        public CancelXATransactionTask() {
            XATransactionState.this.timeoutTask = this;
        }

        /** Runs the cancel task of the global transaction */
        public void run() {
            try {
                XATransactionState.this.cancel();
            } catch (XAException ex) {
                Monitor.logThrowable(ex);
            }
        }
    }



	XATransactionState(ContextManager cm, EmbedConnection conn, 
                EmbedXAResource resource, XAXactId xid) {

		super(cm, "XATransactionState");
		this.conn = conn;
		this.associatedResource = resource;
		this.creatingResource = resource;
		this.associationState = XATransactionState.T1_ASSOCIATED;
		this.xid = xid;
        this.isFinished = false;

	}

	public void cleanupOnError(Throwable t) {

		if (t instanceof StandardException) {

			StandardException se = (StandardException) t;
            
            if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY) {
                popMe();
                return;
            }

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

	void start(EmbedXAResource resource, int flags) throws XAException {

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

            case XATransactionState.TRO_DEADLOCK:
            case XATransactionState.TRO_TIMEOUT:
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

	boolean end(EmbedXAResource resource, int flags, 
                boolean endingCurrentXid) throws XAException {

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

   /**
    * Schedule a timeout task wich will rollback the global transaction
    * after the specified time will elapse.
    *
    * @param timeoutMillis The number of milliseconds to be elapsed before
    *                      the transaction will be rolled back.
    */
    synchronized void scheduleTimeoutTask(long timeoutMillis) {
        // schedule a time out task if the timeout was specified
        if (timeoutMillis > 0) {
            // take care of the transaction timeout
            TimerTask cancelTask = new CancelXATransactionTask();
            TimerFactory timerFactory = Monitor.getMonitor().getTimerFactory();
            Timer timer = timerFactory.getCancellationTimer();
            timer.schedule(cancelTask, timeoutMillis);
        } else {
            timeoutTask = null;
        }
    }

   /**
     * Rollback the global transaction and cancel the timeout task.
     */
    synchronized void xa_rollback() throws SQLException {
        conn.xa_rollback();
        xa_finalize();
    }

   /**
     * Commit the global transaction and cancel the timeout task.
     * @param onePhase Indicates whether to use one phase commit protocol.
     *                Otherwise two phase commit protocol will be used.
     */
    synchronized void xa_commit(boolean onePhase) throws SQLException {
        conn.xa_commit(onePhase);
        xa_finalize();
    }

   /**
     * Prepare the global transaction for commit.
     */
    synchronized int xa_prepare() throws SQLException {
        int retVal = conn.xa_prepare();
        return retVal;
    }

    /** This method cancels timeoutTask and marks the transaction
      * as finished by assigning 'isFinished = true'.
      */
    synchronized void xa_finalize() {
        if (timeoutTask != null) {
            timeoutTask.cancel();
        }
        isFinished = true;
    }

    /**
     * This function is called from the timer task when the transaction
     * times out.
     *
     * @see CancelXATransactionTask
     */
    private synchronized void cancel() throws XAException {
        // Check isFinished just to be sure that
        // the cancellation task was not started
        // just before the xa_commit/rollback
        // obtained this object's monitor.
        if (!isFinished) {
            // Check whether the transaction is associated
            // with any EmbedXAResource instance.
            if (associationState == XATransactionState.T1_ASSOCIATED) {
                conn.cancelRunningStatement();
                EmbedXAResource assocRes = associatedResource;
                end(assocRes, XAResource.TMFAIL, true);
            }

            // Rollback the global transaction
            try {
                conn.xa_rollback();
            } catch (SQLException sqle) {
                XAException ex = new XAException(XAException.XAER_RMERR);
                ex.initCause(sqle);
                throw ex;
            }

            // Do the cleanup on the resource
            creatingResource.returnConnectionToResource(this, xid);
        }
    }
}

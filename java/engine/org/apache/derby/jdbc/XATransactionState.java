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


import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.TimerTask;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.timer.TimerFactory;
import org.apache.derby.impl.jdbc.EmbedConnection;
import javax.transaction.xa.XAResource;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.XATransactionController;
import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.shared.common.reference.SQLState;
import java.util.HashMap;
import javax.transaction.xa.XAException;
import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.shared.common.reference.MessageId;

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
     * A class used to monitor if the transaction is in the middle of
     * cancelling or cleaning up on an error.  
     * 
     * See DERBY-6879
     * 
     */
    private static class CleanupOrCancelMonitor {
        
        private Long cancelThreadId;
        private Long cleanupThreadId;
        
        /**
         * See if it is ok to cancel.  It is okay to cancel if the transaction
         * is not cleaning up from an error.  The assumption is that if the 
         * cleanUpOnError is/has been invoked, then there is no reason for
         * the cancel to be processed as the transaction is going to end
         * (ab)normally.
         * 
         * @return <code>true</code> if it is okay to cancel.
         */
        public synchronized boolean okToCancel() {
            boolean res = false;
            if (null == cancelThreadId && null == cleanupThreadId) {
                cancelThreadId = Thread.currentThread().getId();
                res = true;
            }
            return res;
        }
        
        /**
         * See if it is ok to cleanup.  It is okay to cleanup if the transaction
         * is not cancelling.  The assumption is that if the 
         * cancel is/has been invoked, then there is no reason to try to
         * mark the transaction as being in error.  The transaction will 
         * be cancelled in any case.
         * 
         * @return <code>true</code> if it is okay to cleanup.
         */
        private synchronized boolean okToCleanup() {
            boolean res = false;
            if (null == cleanupThreadId && null == cancelThreadId) {
                cleanupThreadId = Thread.currentThread().getId();
                res = true;
            }
            return res;
        }
    };
    
    CleanupOrCancelMonitor cleanupOrCancelMonitor = new CleanupOrCancelMonitor();
    
	/**
		When an XAResource suspends a transaction (end(TMSUSPEND)) it must be resumed
		using the same XAConnection. This has been the traditional Cloudscape/Derby behaviour,
		though there does not seem to be a specific reference to this behaviour in
		the JTA spec. Note that while the transaction is suspended by this XAResource,
		another XAResource may join the transaction and suspend it after the join.
	*/
    HashMap<EmbedXAResource, XATransactionState> suspendedList;


	/**
		Association state of the transaction.
	*/
	int associationState;

	int rollbackOnlyCode;


	/**
		has this transaction been prepared.
	*/
	boolean isPrepared;

    /** Indicates whether this transaction is supposed to be rolled back by timeout. */
    boolean performTimeoutRollback;

    /** A timer task scheduled for the time when the transaction will timeout. */
    CancelXATransactionTask timeoutTask = null;


    /** The implementation of TimerTask to cancel a global transaction. */
    private static class CancelXATransactionTask extends TimerTask {

        private XATransactionState xaState; 

        /**
         * Creates the cancellation task to be passed to a timer.
         *
         * @param xaState the XA state object for the transaction to cancel
         */
        public CancelXATransactionTask(XATransactionState xaState) {
            this.xaState = xaState;
        }
        
        public synchronized boolean cancel() {
            // nullify reference to reduce memory footprint of canceled tasks
            xaState = null;
            return super.cancel();
        }

        /** Runs the cancel task of the global transaction */
        public synchronized void run() {
            try {
                if (null != xaState) {
                    xaState.cancel(MessageId.CONN_XA_TRANSACTION_TIMED_OUT);
                }
            } catch (Throwable th) {
                Monitor.logThrowable(th);
            }
        }
    }

    private static TimerFactory getTimerFactory() {
        return getMonitor().getTimerFactory();
    }

	XATransactionState(ContextManager cm, EmbedConnection conn, 
                EmbedXAResource resource, XAXactId xid) {

		super(cm, "XATransactionState");
		this.conn = conn;
		this.associatedResource = resource;
		this.creatingResource = resource;
		this.associationState = XATransactionState.T1_ASSOCIATED;
		this.xid = xid;
		this.performTimeoutRollback = false; // there is no transaction yet
	}

	public void cleanupOnError(Throwable t) {
        // See if it is okay to cleanup.  This is for DERBY-6879
        if (cleanupOrCancelMonitor.okToCleanup()) {
            if (t instanceof StandardException) {

                StandardException se = (StandardException) t;

                if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY) {
                    popMe();
                    return;
                }

                if (se.getSeverity() == ExceptionSeverity.TRANSACTION_SEVERITY) {

                    synchronized (this) {
                        // prior to the DERBY-5552 fix, we would disable the connection
                        // here with conn.setApplicationConnection(null);
                        // which could cause a NPE
                        notifyAll();
                        associationState = TRO_FAIL;
                        if (SQLState.DEADLOCK.equals(se.getMessageId()))
                            rollbackOnlyCode = XAException.XA_RBDEADLOCK;
                        else if (se.isLockTimeout())
                            rollbackOnlyCode = XAException.XA_RBTIMEOUT;
                        else
                            rollbackOnlyCode = XAException.XA_RBOTHER;
                    }
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

                if (suspendedList == null) {
                    suspendedList =
                        new HashMap<EmbedXAResource, XATransactionState>();
                }
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
    * Schedule a timeout task which will rollback the global transaction
    * after the specified time will elapse.
    *
    * @param timeoutMillis The number of milliseconds to be elapsed before
    *                      the transaction will be rolled back.
    */
    synchronized void scheduleTimeoutTask(long timeoutMillis) {
        // Mark the transaction to be rolled back bby timeout
        performTimeoutRollback = true;
        // schedule a time out task if the timeout was specified
        if (timeoutMillis > 0) {
            // take care of the transaction timeout
            timeoutTask = new CancelXATransactionTask(this);
            getTimerFactory().schedule(timeoutTask, timeoutMillis);
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
        int retVal;

        try {
            retVal = conn.xa_prepare();
        } catch (SQLException e) {
            if (ExceptionUtil.isDeferredConstraintViolation(e.getSQLState())) {
                // we are rolling back
                xa_finalize();
            }

            throw e;
        }

        if (retVal == XATransactionController.XA_RDONLY) {
            // Read-only transactions are implicitly committed when they are
            // prepared. Since the transaction has completed, the timeout task
            // should be cancelled now. DERBY-5562.
            xa_finalize();
        }

        return retVal;
    }

    /** This method cancels timeoutTask and assigns
      * 'performTimeoutRollback = false'.
      */
    private void xa_finalize() {
        if (timeoutTask != null) {
            getTimerFactory().cancel(timeoutTask);
            timeoutTask = null;
        }
        performTimeoutRollback = false;
    }

    /**
     * This function is called from the timer task when the transaction
     * times out.
     *
     * @see CancelXATransactionTask
     */
    void cancel(String messageId) throws XAException {
        // See if it is ok to cancel.  This is for DERBY-6879
        if (cleanupOrCancelMonitor.okToCancel()) {
            synchronized (this) {
                // We remove the XID so that the client code if any
                //  will get an XAER_NOTA when it accesses the XA transaction
                //  once it starts being canceled.  This makes since in that
                //  once the cancel starts, the client will not be able
                //  to access the XA transaction.  See DERBY-6879
                creatingResource.removeXATransaction(xid);
                // Check performTimeoutRollback just to be sure that
                // the cancellation task was not started
                // just before the xa_commit/rollback
                // obtained this object's monitor.
                if (performTimeoutRollback) {

                    // Log the message about the transaction cancelled
                    if (messageId != null)
                        Monitor.logTextMessage(messageId, xid.toString());

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
    }
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}

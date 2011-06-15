/*

   Derby - Class org.apache.derby.jdbc.EmbedXAResource

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

import java.sql.ResultSet;
import java.sql.SQLException;


import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.transaction.xa.XAException;

import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.XATransactionController;
import org.apache.derby.iapi.store.access.xa.XAResourceManager;
import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.TransactionResourceImpl;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.Property;

/**
 * Implements XAResource
 */
class EmbedXAResource implements XAResource {

    private EmbedPooledConnection con;
    private ResourceAdapter ra;
    private XAXactId currentXid;    
    /** The value of the transaction timeout on this resource. */
    private int timeoutSeconds;
    
    EmbedXAResource (EmbedPooledConnection con, ResourceAdapter ra) {
        this.con = con;
        this.ra = ra;
        // Setup the default value for the transaction timeout.
        this.timeoutSeconds = 0;
    }
    
    /**
     * Commit the global transaction specified by xid.
     * @param xid A global transaction identifier
     * @param onePhase If true, the resource manager should use a one-phase
     * commit protocol to commit the work done on behalf of xid.
     *
     * @exception XAException An error has occurred. Possible XAExceptions are
     * XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
     * XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.  
     * <P>If the resource manager did not commit the transaction and
     * the paramether onePhase is set to true, the resource manager 
     * may throw one of the XA_RB* exceptions. Upon return, the
     * resource manager has rolled back the branch's work and has 
     * released all held resources.
     */    
    public final synchronized void commit(Xid xid, boolean onePhase) 
                                            throws XAException {
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
            try {
                rm.commit(inDoubtCM, xid_im, onePhase);
                
                // close the connection/transaction since it can never
                // be used again. DERBY-4856 No extended diagnostic information needed.
                inDoubtCM.cleanupOnError(StandardException.closeException(),
                        false);
                return;
            } catch (StandardException se) {
                // The rm threw an exception, clean it up in the approprate
                // context.  There is no transactionResource to handle the
                // exception for us.
                inDoubtCM.cleanupOnError(se, con.isActive());
                throw wrapInXAException(se);
            } finally {
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
            
            try {
                tranState.xa_commit(onePhase);
            } catch (SQLException sqle) {
                throw wrapInXAException(sqle);
            } finally {
                returnConnectionToResource(tranState, xid_im);
            }
        }
    }

    /**
     * Ends the work performed on behalf of a transaction branch. The resource
     * manager disassociates the XA resource from the transaction branch
     * specified and let the transaction be completed.
     *
     * <p> If TMSUSPEND is specified in flags, the transaction branch is
     * temporarily suspended in incomplete state. The transaction context
     * is in suspened state and must be resumed via start with TMRESUME
     * specified.
     *
     * <p> If TMFAIL is specified, the portion of work has failed. The
     * resource manager may mark the transaction as rollback-only
     *
     * <p> If TMSUCCESS is specified, the portion of work has completed
     * successfully.
     *
     * @param xid A global transaction identifier that is the same as what was
     * used previously in the start method.
     * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND
     *
     * @exception XAException An error has occurred.
     * Possible XAException values are XAER_RMERR, XAER_RMFAILED, XAER_NOTA,
     * XAER_INVAL, XAER_PROTO, or XA_RB*.
     */
    public final synchronized void end(Xid xid, int flags) throws XAException {
        checkXAActive();
        
        try {
            // It is possible that the isolation level state in connection
            // handle has gotten out of sync with the real isolation level.
            // This can happen if SLQ instead of JDBC api has been used to
            // set the isolation level. The code below will check if isolation
            // was set using JDBC or SQL and if yes, then it will update the
            // isolation state in BrokeredConnection with EmbedConnection's
            // isolation level.
            if (con.currentConnectionHandle != null)
                con.currentConnectionHandle.getIsolationUptoDate();
        } catch (SQLException sqle) {
            throw wrapInXAException(sqle);
        }
        
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
            con.realConnection = null;
        }
        
        if (rollbackOnly)
            throw new XAException(tranState.rollbackOnlyCode);        
    }

    /**
     * Ask the resource manager to prepare for a transaction commit of the
     * transaction specified in xid.
     *
     * @param xid A global transaction identifier
     *
     * @return A value indicating the resource manager's vote on the outcome
     * of the transaction. The possible values are: XA_RDONLY or XA_OK. If the
     * resource manager wants to roll back the transaction, it should do so by
     * raising an appropriate XAException in the prepare method.
     *
     * @exception XAException An error has occurred. Possible exception values
     * are: XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or
     * XAER_PROTO.
     *
     */
    public final synchronized int prepare(Xid xid) throws XAException {
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
            
            if (tranState.suspendedList != null 
                    && tranState.suspendedList.size() != 0)
                throw new XAException(XAException.XAER_PROTO);
            
            if (tranState.isPrepared)
                throw new XAException(XAException.XAER_PROTO);
            
            try {
                
                int ret = tranState.xa_prepare();
                
                if (ret == XATransactionController.XA_OK) {
                    tranState.isPrepared = true;
                    
                    return XAResource.XA_OK;
                } else {
                    
                    returnConnectionToResource(tranState, xid_im);

					if (SanityManager.DEBUG) {
						if (con.realConnection != null) {
							SanityManager.ASSERT(
                                con.realConnection.transactionIsIdle(),
                                "real connection should have been idle." +
                                "tranState = " + tranState +
                                "ret = " + ret +
                                "con.realConnection = " + con.realConnection);
                        }
					}
                    return XAResource.XA_RDONLY;
                }
            } catch (SQLException sqle) {
                throw wrapInXAException(sqle);
            }
        }
        
    }

    /**
     * Obtain the current transaction timeout value set for this XAResource
     * instance. If XAResource.setTransactionTimeout was not use prior to
     * invoking this method, the return value is 0; otherwise, the value
     * used in the previous setTransactionTimeout call is returned.
     *
     * @return the transaction timeout value in seconds. If the returned value
     * is equal to Integer.MAX_VALUE it means no timeout.
     */
    public synchronized int getTransactionTimeout() {
        return timeoutSeconds;
    }

    /**
     * This method is called to determine if the resource manager instance
     * represented by the target object is the same as the resouce manager
     * instance represented by the parameter xares.
     *
     * @param xares An XAResource object whose resource manager instance is to
     * be compared with the resource manager instance of the target object.
     *
     * @return true if it's the same RM instance; otherwise false.
     * @exception XAException An error has occurred. Possible exception values
     * are XAER_RMERR, XAER_RMFAIL.
     */
    public final synchronized boolean isSameRM(XAResource xares) 
                                                        throws XAException {
        checkXAActive();        
        if (xares instanceof EmbedXAResource) {            
            return ra == ((EmbedXAResource) xares).ra;
        }        
        return false;
    }
    
    /**
     * Obtain a list of prepared transaction branches from a resource
     * manager. The transaction manager calls this method during recovery to
     * obtain the list of transaction branches that are currently in prepared
     * or heuristically completed states.
     *
     * @param flag One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must
     * be used when no other flags are set in flags.
     *
     * @return The resource manager returns zero or more XIDs for the
     * transaction branches that are currently in a prepared or heuristically
     * completed state. If an error occurs during the operation, the resource
     * manager should throw the appropriate XAException.
     *
     * @exception XAException An error has occurred. Possible values are
     * XAER_RMERR, XAER_RMFAIL, XAER_INVAL, and XAER_PROTO.
     *
     */
    public final synchronized Xid[] recover(int flag) throws XAException {
        checkXAActive();
        
        try {
            return ra.getXAResourceManager().recover(flag);
        } catch (StandardException se) {
            throw wrapInXAException(se);
        }
    }
    
    /**
     * Tell the resource manager to forget about a heuristically completed
     * transaction branch.
     *
     * @param xid A global transaction identifier
     * @exception XAException An error has occurred. Possible exception values
     * are XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
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
            try {
                rm.forget(inDoubtCM, xid_im);
                
                // close the connection/transaction since it can never be used again.
                inDoubtCM.cleanupOnError(StandardException.closeException(),
                        false);
                return;
            } catch (StandardException se) {
                // The rm threw an exception, clean it up in the approprate
                // context.  There is no transactionResource to handle the
                // exception for us.
                inDoubtCM.cleanupOnError(se, con.isActive());
                throw wrapInXAException(se);
            } finally {
                csf.resetCurrentContextManager(inDoubtCM);
            }
            
        }
        
        throw new XAException(tranState.isPrepared
            ? XAException.XAER_NOTA 
            : XAException.XAER_PROTO);
    }    

    /**
     * Inform the resource manager to roll back work done on behalf of a
     * transaction branch
     *
     * @param xid A global transaction identifier
     * @exception XAException - An error has occurred
     */
    public final synchronized void rollback(Xid xid) throws XAException {
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
            try {
                rm.rollback(inDoubtCM, xid_im);
                
                // close the connection/transaction since it can never be used again.
                inDoubtCM.cleanupOnError(StandardException.closeException(),
                        false);
                return;
            } catch (StandardException se) {
                // The rm threw an exception, clean it up in the approprate
                // context.  There is no transactionResource to handle the
                // exception for us.
                inDoubtCM.cleanupOnError(se, con.isActive());
                throw wrapInXAException(se);
            } finally {
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
            
            if (tranState.suspendedList != null 
                    && tranState.suspendedList.size() != 0)
                throw new XAException(XAException.XAER_PROTO);
            
            checkUserCredentials(tranState.creatingResource);
            
            try {
                
                tranState.xa_rollback();
            } catch (SQLException sqle) {
                throw wrapInXAException(sqle);
            } finally {
                returnConnectionToResource(tranState, xid_im);
            }
        }
    }


    /**
     * Set the current transaction timeout value for this XAResource
     * instance. Once set, this timeout value is effective until
     * setTransactionTimeout is invoked again with a different value. To reset
     * the timeout value to the default value used by the resource manager,
     * set the value to zero. If the timeout operation is performed
     * successfully, the method returns true; otherwise false. If a resource
     * manager does not support transaction timeout value to be set
     * explicitly, this method returns false.
     *
     * @param seconds the transaction timeout value in seconds.
     *                Value of 0 means the reasource manager's default value.
     *                Value of Integer.MAX_VALUE means no timeout.
     * @return true if transaction timeout value is set successfully;
     * otherwise false.
     *
     * @exception XAException - An error has occurred. Possible exception
     * values are XAER_RMERR, XAER_RMFAIL, or XAER_INVAL.
     */
    public synchronized boolean setTransactionTimeout(int seconds)
    throws XAException {
        if (seconds < 0) {
            // throw an exception if invalid value was specified
            throw new XAException(XAException.XAER_INVAL);
        }
        timeoutSeconds = seconds;
        return true;
    }

    /** Returns the default value for the transaction timeout in milliseconds
     *  setted up by the system properties.
     */
    private long getDefaultXATransactionTimeout() throws XAException {
        try {
            LanguageConnectionContext lcc = con.getLanguageConnection();
            TransactionController tc = lcc.getTransactionExecute();

            long timeoutMillis = 1000 * (long) PropertyUtil.getServiceInt(
                tc,
                Property.PROP_XA_TRANSACTION_TIMEOUT,
                0,
                Integer.MAX_VALUE,
                Property.DEFAULT_XA_TRANSACTION_TIMEOUT
                );

            return timeoutMillis;
        } catch (SQLException sqle) {
            throw wrapInXAException(sqle);
        } catch (StandardException se) {
            throw wrapInXAException(se);
        }
    }

    /**
     * Start work on behalf of a transaction branch specified in xid If TMJOIN
     * is specified, the start is for joining a transaction previously seen by
     * the resource manager. If TMRESUME is specified, the start is to resume
     * a suspended transaction specified in the parameter xid. If neither
     * TMJOIN nor TMRESUME is specified and the transaction specified by xid
     * has previously been seen by the resource manager, the resource manager
     * throws the XAException exception with XAER_DUPID error code.
     *
     * @param xid A global transaction identifier to be associated with the
     * resource
     * @param flags One of TMNOFLAGS, TMJOIN, or TMRESUME
     *
     * @exception XAException An error has occurred. Possible exceptions are
     * XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_DUPID, XAER_OUTSIDE, XAER_NOTA,
     * XAER_INVAL, or XAER_PROTO.
     */
    public final synchronized void start(Xid xid, 
                                        int flags) throws XAException {
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
                    
                    if (con.realConnection == null) {
                        con.openRealConnection();
                        
                        if (con.currentConnectionHandle != null) {
                            
                            // since this is a new connection, set its complete
                            // state according to the application's Connection
                            // handle view of the world.
                            con.currentConnectionHandle.setState(true);
                            con.realConnection.setApplicationConnection
                                    (con.currentConnectionHandle);
                        }
                        
                    } else {
                        
                        // XAResource.start() auto commits in DB2 when in 
                        // auto commit mode.
                        if (con.currentConnectionHandle != null) {
                            if (con.currentConnectionHandle.getAutoCommit())
                                con.currentConnectionHandle.rollback();
                        }
                        if (!con.realConnection.transactionIsIdle())
                            throw new XAException(XAException.XAER_OUTSIDE);
                        
                        if (con.currentConnectionHandle != null) {
                            // It is possible that the isolation level state 
                            // in connection handle has gotten out of sync 
                            // with the real isolation level. This can happen 
                            // if SLQ instead of JDBC api has been used to set 
                            // the isolation level. The code below will check 
                            // if isolation was set using JDBC or SQL and if 
                            // yes, then it will update the isolation state 
                            // in BrokeredConnection with EmbedConnection's
                            // isolation level.
                            con.currentConnectionHandle.getIsolationUptoDate();
                            // we have a current handle so we need to keep
                            // the connection state of the current connection.
                            con.currentConnectionHandle.setState(true);
                            
                            // At the local to global transition we need 
                            // to discard and close any open held result 
                            // sets, a rollback will do this.
                            con.realConnection.rollback();
                        } else {
                            con.resetRealConnection();
                        }
                        
                    }
                    
                    // Global connections are always in auto commit false mode.
                    con.realConnection.setAutoCommit(false);
                    
                    // and holdability false (cannot hold cursors across 
                    // XA transactions.
                    con.realConnection.setHoldability(
                            ResultSet.CLOSE_CURSORS_AT_COMMIT);
                    
                    con.realConnection.getLanguageConnection().
                            getTransactionExecute().
                            createXATransactionFromLocalTransaction(
                                                xid_im.getFormatId(),
                                                xid_im.getGlobalTransactionId(),
                                                xid_im.getBranchQualifier());
                    
                    
                } catch (StandardException se) {
                    throw wrapInXAException(se);
                } catch (SQLException sqle) {
                    throw wrapInXAException(sqle);
                }
                
                tranState = new XATransactionState(
                    con.realConnection.getContextManager(),
                    con.realConnection, this, xid_im);
                if (!ra.addConnection(xid_im, tranState))
                    throw new XAException(XAException.XAER_DUPID);
                
                currentXid = xid_im;

                // If the the timeout specified is equal to Integer.MAX_VALUE
                // it means that transaction timeout is disabled.
                if (timeoutSeconds != Integer.MAX_VALUE) {
                    // Find out the value of the transaction timeout
                    long timeoutMillis;
                    if (timeoutSeconds > 0) {
                        timeoutMillis = 1000*timeoutSeconds;
                    } else {
                        timeoutMillis = getDefaultXATransactionTimeout();
                    }
                    // If we have non-zero transaction timeout schedule a timeout task.
                    // The only way how timeoutMillis might be equeal to 0 is that
                    // it was specified as a default transaction timeout
                    if (timeoutMillis > 0) {
                        tranState.scheduleTimeoutTask(timeoutMillis);
                    }
                }

                break;
                
            case XAResource.TMRESUME:
            case XAResource.TMJOIN:
                if (tranState == null)
                    throw new XAException(XAException.XAER_NOTA);
                
                tranState.start(this, flags);
                
                if (tranState.conn != con.realConnection) {
                    
                    if (con.realConnection != null) {
                        
                        if (!con.realConnection.transactionIsIdle())
                            throw new XAException(XAException.XAER_OUTSIDE);
                        
                        // We need to get the isolation level up to date same 
                        // way as it is done at start of a transaction. Before
                        // joining the transaction, it is possible that the 
                        // isolation level was updated using SQL. We need to 
                        // get this state and store in the connection handle so 
                        // that we can restore the isolation when we are in the 
                        // local mode.
                        try {
	                    	if (con.currentConnectionHandle != null) {
	                    		con.currentConnectionHandle.getIsolationUptoDate();
	                    	}
                    	} catch (SQLException sqle) {
                            throw wrapInXAException(sqle);
                        }
                        
                        closeUnusedConnection(con.realConnection);
                    }
                    con.realConnection = tranState.conn;
                    
                    if (con.currentConnectionHandle != null) {
                        
                        try {
                            // only reset the non-transaction specific 
                            // Connection state.
                            con.currentConnectionHandle.setState(false);
                            con.realConnection.setApplicationConnection(
                                    con.currentConnectionHandle);
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
     * Resturns currently active xid
     * @return Xid
     */
    Xid getCurrentXid () {
        return currentXid;
    }

    /**
     * Returns the XATransactionState of the the transaction
     * @param xid_im 
     * @return XATransactionState
     */
    private XATransactionState getTransactionState(XAXactId xid_im) {
        return (XATransactionState) ra.findConnection(xid_im);
    }

    /**
     * Compares the user name and password of the XAResource with
     * user name and password of this and throws XAException if there is 
     * a mismatch
     * @param original EmbedXAResource
     */
    private void checkUserCredentials(EmbedXAResource original)
                                                        throws XAException {        
        if (original == this)
            return;        
        if (original.con.getPassword().equals(con.getPassword()) && 
                (original.con.getUsername().equals(con.getUsername())))
            return;                
        throw new XAException(XAException.XA_RBINTEGRITY);
    }
    
    /**
     * Checks if currently associated connection is active
     * throws exception if not
     */
    private void checkXAActive() throws XAException {    
        try {
            con.checkActive();
        } catch (SQLException sqle) {
            throw wrapInXAException(sqle);
        }
    }
    
    /**
     * Map a SQL exception to appropriate XAException.
     * Return the mapped XAException.
     */
    private static XAException wrapInXAException(SQLException se) {
        // Map interesting exceptions to XAException
        String sqlstate = se.getSQLState();
        String message = se.getMessage();
        int seErrorCode = se.getErrorCode();      
        int xaErrorCode;
        
        XAException xae;
        
        // Determine the XAException.errorCode.  This is known for 
        // some specific exceptions. For other exceptions, we will
        // return XAER_RMFAIL for SESSION_SEVERITY or greater and
        // XAER_RMERR for less severe errors. DERBY-4141.
        if (sqlstate.equals(StandardException.getSQLStateFromIdentifier(
                            SQLState.STORE_XA_XAER_DUPID)))
            xaErrorCode = XAException.XAER_DUPID;
        else if (sqlstate.equals(StandardException.getSQLStateFromIdentifier(
                                SQLState.STORE_XA_PROTOCOL_VIOLATION)))
            xaErrorCode = XAException.XA_RBPROTO;
        else if (sqlstate.equals(SQLState.DEADLOCK))
            xaErrorCode = XAException.XA_RBDEADLOCK;
        else if (sqlstate.equals(SQLState.LOCK_TIMEOUT))
            xaErrorCode = XAException.XA_RBTIMEOUT;
        else if (seErrorCode >=  ExceptionSeverity.SESSION_SEVERITY)
            xaErrorCode = XAException.XAER_RMFAIL;            
        else
            xaErrorCode = XAException.XAER_RMERR;
        
        xae = new XAException(message);
        xae.errorCode = xaErrorCode;
        if (JVMInfo.JDK_ID >= JVMInfo.J2SE_14)
            xae.initCause(se);
        return xae;
    }
	
    /**
     * Map a Standard exception to appropriate XAException.
     * Return the mapped XAException.
     */
    private static XAException wrapInXAException(StandardException se) {
        return wrapInXAException(
                TransactionResourceImpl.wrapInSQLException(se));
    }
    
    /**
     * Return an underlying connection object back to its XAResource
     * if possible. If not close the connection.
     * @param tranState 
     * @param xid_im 
     */
    void returnConnectionToResource(XATransactionState tranState,
                                                            XAXactId xid_im) {
        
        removeXATransaction(xid_im);    
        synchronized (tranState) {
            // global transaction is over.
            tranState.associationState = XATransactionState.TC_COMPLETED;
            tranState.notifyAll();
            
            EmbedConnection conn = tranState.conn;
            
            // already set in its own resource
            // or can it be returned to its original resource?
            if ((tranState.creatingResource.con.realConnection == conn) ||
                    (tranState.creatingResource.con.realConnection == null)) {
                
                tranState.creatingResource.con.realConnection = conn;
                
                BrokeredConnection handle = 
                        tranState.creatingResource.con.currentConnectionHandle;
                
                conn.setApplicationConnection(handle);
                
                if (handle != null) {
                    try {
                        handle.setState(true);
                    } catch (SQLException sqle) {
                        
                        // couldn't reset the connection
                        closeUnusedConnection(tranState.conn);
                        tranState.creatingResource.con.realConnection = null;
                    }
                }
                return;
            }
        }
        
        // nowhere to place it, close it.
        closeUnusedConnection(tranState.conn);
    }


    /**
     * Close  an underlying connection object when there is
     * no active XAResource to hand it to.
     * @param conn 
     */
    private static void closeUnusedConnection(EmbedConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqle) {
                
            }
        }
    }

    /**
     * Removes the xid from currently active transactions
     * @param xid_im 
     */
    void removeXATransaction(XAXactId xid_im) {
        XATransactionState tranState = 
                (XATransactionState) ra.removeConnection(xid_im);
        if (tranState != null)
            tranState.popMe();
    }
    
    void setCurrentXid(XAXactId aCurrentXid) {
        currentXid = aCurrentXid;
    }

}

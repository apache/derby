/*

   Derby - Class org.apache.derby.client.net.NetXAResource

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
/**********************************************************************
 *
 *
 *  Component Name =
 *
 *      Package Name = org.apache.derby.client.net
 *
 *  Descriptive Name = class implements XAResource
 *
 *  Status = New code
 *
 *  Function = Handle XA methods
 *
 *  List of Classes
 *              - NetXAResource
 *
 *  Restrictions : None
 *
 **********************************************************************/
package org.apache.derby.client.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.derby.client.ClientXid;
import org.apache.derby.client.am.ClientConnection;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlCode;
import org.apache.derby.client.am.Utils;
import org.apache.derby.client.am.XaException;
import org.apache.derby.shared.common.reference.SQLState;

public class NetXAResource implements XAResource {
    private static final int INITIAL_CALLINFO_ELEMENTS = 1;
    static final ClientXid nullXid = new ClientXid();

    // xaFunction defines, shows which queued XA function is being performed
    static final int XAFUNC_COMMIT = 1;
    private static final int XAFUNC_END = 2;
    private static final int XAFUNC_FORGET = 3;
    private static final int XAFUNC_PREPARE = 4;
    private static final int XAFUNC_RECOVER = 5;
    static final int XAFUNC_ROLLBACK = 6;
    private static final int XAFUNC_START = 7;

    private static final String XAFUNCSTR_NONE = "No XA Function";
    private static final String XAFUNCSTR_COMMIT = "XAResource.commit()";
    private static final String XAFUNCSTR_END = "XAResource.end()";
    private static final String XAFUNCSTR_FORGET = "XAResource.forget()";
    private static final String XAFUNCSTR_PREPARE = "XAResource.prepare()";
    private static final String XAFUNCSTR_RECOVER = "XAResource.recover()";
    private static final String XAFUNCSTR_ROLLBACK = "XAResource.rollback()";
    private static final String XAFUNCSTR_START = "XAResource.start()";

    SqlException exceptionsOnXA = null;

    NetXAConnection netXAConn_;
    NetConnection conn_;
    private boolean keepIsolationLevel;

    // TODO: change to a single callInfo field (not an array)
    NetXACallInfo callInfoArray_[] =
            new NetXACallInfo[INITIAL_CALLINFO_ELEMENTS];

    /** The value of the transaction timeout in seconds. */
    private int timeoutSeconds = 0;

    public NetXAResource(XAConnection xaconn,
                         NetXAConnection conn) {
        conn_ = conn.getNetConnection();
        netXAConn_ = conn;
        conn.setNetXAResource(this);

        // link the primary connection to the first XACallInfo element
        conn_.currXACallInfoOffset_ = 0;

        // construct the NetXACallInfo object for the array.
        for (int i = 0; i < INITIAL_CALLINFO_ELEMENTS; ++i) {
            callInfoArray_[i] = new NetXACallInfo(null, XAResource.TMNOFLAGS,
                    null);
        }

        // initialize the first XACallInfo element with the information from the
        //  primary connection
        callInfoArray_[0].actualConn_ = conn;
        // ~~~ save conn_ connection variables in callInfoArray_[0]
        callInfoArray_[0].saveConnectionVariables();
    }

    public void commit(Xid xid, boolean onePhase) throws XAException {
        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        
        exceptionsOnXA = null;
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "commit", xid, onePhase);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        // update the XACallInfo
        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xaFlags_ = (onePhase ? XAResource.TMONEPHASE :
                XAResource.TMNOFLAGS);
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            netAgent.beginWriteChainOutsideUOW();
            netAgent.netConnectionRequest_.writeXaCommit(conn_, xid);
            netAgent.flowOutsideUOW();
            netAgent.netConnectionReply_.readXaCommit(conn_);
            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_COMMIT;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
            netAgent.endReadChain();
        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }
        if (rc != XAResource.XA_OK) {
            throwXAException(rc);
        }
    }

    /**
     * Get XAException.errorCode from SqlException
     * For disconnect exception, return XAER_RMFAIL
     * For other exceptions return XAER_RMERR
     * 
     * For server side SQLExceptions during 
     * XA operations the errorCode has already been determined
     * and wrapped in an XAException for return to the client.
     * see EmbedXAResource.wrapInXAException
     * 
     * @param sqle  SqlException to evaluate.
     * @return XAException.XAER_RMFAIL for disconnect exception,
     *         XAException.XAER_RMERR for other exceptions.
     */
    private int getSqlExceptionXAErrorCode(SqlException sqle) {      
       int seErrorCode = sqle.getErrorCode();
       return (seErrorCode == 40000 ? XAException.XAER_RMFAIL : XAException.XAER_RMERR);
    }

    /**
     * <p>
     * Ends the work performed on behalf of a transaction branch. The resource manager dissociates the XA resource from
     * the transaction branch specified and let the transaction be completed.
     * </p>
     * <p>
     * If TMSUSPEND is specified in flags, the transaction branch is temporarily suspended in incomplete state. The
     * transaction context is in suspened state and must be resumed via start with TMRESUME specified.
     * </p>
     * <p>
     * If TMFAIL is specified, the portion of work has failed. The resource manager may mark the transaction as
     * rollback-only
     * </p>
     * <p>
     * If TMSUCCESS is specified, the portion of work has completed successfully.
     * </p>
     *
     * @param xid   A global transaction identifier that is the same as what was used previously in the start method.
     * @param flags One of TMSUCCESS, TMFAIL, or TMSUSPEND
     *
     * @throws XAException An error has occurred. Possible XAException values are XAER_RMERR, XAER_RMFAILED, XAER_NOTA,
     *                     XAER_INVAL, XAER_PROTO, or XA_RB*.
     */

    public void end(Xid xid, int flags) throws XAException {

        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        exceptionsOnXA = null;
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "end", xid, flags);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xaFlags_ = flags;
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            netAgent.beginWriteChainOutsideUOW();
            netAgent.netConnectionRequest_.writeXaEndUnitOfWork(conn_);
            netAgent.flowOutsideUOW();
            rc = netAgent.netConnectionReply_.readXaEndUnitOfWork(conn_);

            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_END;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
            netAgent.endReadChain();
        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }

        if (rc != XAResource.XA_OK) {
            // The corresponding XA connection association state
            // is changed by setXaStateForXAException inside the call
            // to throwXAException according the error code of the XAException
            // to be thrown.
            throwXAException(rc);
        }else {
            conn_.setXAState(ClientConnection.XA_T0_NOT_ASSOCIATED);
        } 
    }

    /**
     * Tell the resource manager to forget about a heuristically (MANUALLY) completed transaction branch.
     *
     * @param xid A global transaction identifier
     *
     * @throws XAException An error has occurred. Possible exception values are XAER_RMERR, XAER_RMFAIL, XAER_NOTA,
     *                     XAER_INVAL, or XAER_PROTO.
     */

    public void forget(Xid xid) throws XAException {
        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        exceptionsOnXA = null;

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "forget", xid);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }
        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            // flow the required PROTOCOL to the server
            netAgent.beginWriteChainOutsideUOW();

            // sent the commit PROTOCOL
            netAgent.netConnectionRequest_.writeXaForget(netAgent.netConnection_, xid);

            netAgent.flowOutsideUOW();

            // read the reply to the commit
            netAgent.netConnectionReply_.readXaForget(netAgent.netConnection_);

            netAgent.endReadChain();
            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_FORGET;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
        } catch (SqlException sqle) {
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
            throwXAException(getSqlExceptionXAErrorCode(sqle));
        }

        if (rc != XAResource.XA_OK) {
            throwXAException(rc);
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
    public int getTransactionTimeout() throws XAException {
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "getTransactionTimeout");
        }
        exceptionsOnXA = null;
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceExit(this, "getTransactionTimeout", timeoutSeconds);
        }
        return timeoutSeconds;
    }

    /**
     * Ask the resource manager to prepare for a transaction commit of the transaction specified in xid.
     *
     * @param xid A global transaction identifier
     *
     * @return A value indicating the resource manager's vote on the outcome of the transaction. The possible values
     *         are: XA_RDONLY or XA_OK. If the resource manager wants to roll back the transaction, it should do so by
     *         raising an appropriate XAException in the prepare method.
     *
     * @throws XAException An error has occurred. Possible exception values are: XA_RB*, XAER_RMERR, XAER_RMFAIL,
     *                     XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     */
    public int prepare(Xid xid) throws XAException { // public interface for prepare
        // just call prepareX with the recursion flag set to true
        exceptionsOnXA = null;

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "prepare", xid);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        /// update the XACallInfo
        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            netAgent.beginWriteChainOutsideUOW();
            // sent the prepare PROTOCOL
            netAgent.netConnectionRequest_.writeXaPrepare(conn_);
            netAgent.flowOutsideUOW();

            // read the reply to the prepare
            rc = netAgent.netConnectionReply_.readXaPrepare(conn_);
            if ((callInfo.xaRetVal_ != XAResource.XA_OK) &&
                    (callInfo.xaRetVal_ != XAException.XA_RDONLY)) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_PREPARE;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }

            netAgent.endReadChain();
        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }

        if ((rc != XAResource.XA_OK ) && (rc != XAResource.XA_RDONLY)) {
            throwXAException(rc);
        }
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceExit(this, "prepare", rc);
        }
        return rc;
    }

    /**
     * Obtain a list of prepared transaction branches from a resource manager. The transaction manager calls this method
     * during recovery to obtain the list of transaction branches that are currently in prepared or heuristically
     * completed states.
     *
     * @param flag One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must be used when no other flags are set in
     *             flags.
     *
     * @return The resource manager returns zero or more XIDs for the transaction branches that are currently in a
     *         prepared or heuristically completed state. If an error occurs during the operation, the resource manager
     *         should raise the appropriate XAException.
     *
     * @throws XAException An error has occurred. Possible values are XAER_RMERR, XAER_RMFAIL, XAER_INVAL, and
     *                     XAER_PROTO.
     */
    public Xid[] recover(int flag) throws XAException {
        int rc = XAResource.XA_OK;
        NetAgent netAgent = conn_.netAgent_;

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "recover", flag);
        }
        exceptionsOnXA = null;
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        Xid[] xidList = null;

        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xaFlags_ = flag;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            netAgent.beginWriteChainOutsideUOW();
            // sent the recover PROTOCOL
            netAgent.netConnectionRequest_.writeXaRecover(conn_, flag);
            netAgent.flowOutsideUOW();
            netAgent.netConnectionReply_.readXaRecover(conn_);
            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_RECOVER;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
            netAgent.endReadChain();
            xidList = conn_.getIndoubtTransactionIds();

        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }

        if (rc != XAResource.XA_OK) {
            throwXAException(rc);
        }

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceExit(this, "recover", xidList);
        }
        return xidList;
    }

    /**
     * Inform the resource manager to roll back work done on behalf of a transaction branch
     *
     * @param xid A global transaction identifier
     *
     * @throws XAException An error has occurred
     */
    public void rollback(Xid xid) throws XAException {
        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        exceptionsOnXA = null;

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "rollback", xid);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        // update the XACallInfo
        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        try {
            netAgent.beginWriteChainOutsideUOW();
            netAgent.netConnectionRequest_.writeXaRollback(conn_, xid);
            netAgent.flowOutsideUOW();
            // read the reply to the rollback
            rc = netAgent.netConnectionReply_.readXaRollback(conn_);
            netAgent.endReadChain();
            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_END;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }

        if (rc != XAResource.XA_OK) {
            throwXAException(rc);
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
    public boolean setTransactionTimeout(int seconds) throws XAException {
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "setTransactionTimeout");
        }
        if (seconds < 0) {
            // throw an exception if invalid value was specified
            throw new XAException(XAException.XAER_INVAL);
        }
        exceptionsOnXA = null;
        timeoutSeconds = seconds;
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceExit(this, "setTransactionTimeout", true);
        }
        return true;
    }

    public void setKeepCurrentIsolationLevel(boolean flag) {
        keepIsolationLevel = flag;
    }

    public boolean keepCurrentIsolationLevel() {
        return keepIsolationLevel;
    }

    /**
     * Start work on behalf of a transaction branch specified in xid
     *
     * @param xid   A global transaction identifier to be associated with the resource
     * @param flags One of TMNOFLAGS, TMJOIN, or TMRESUME
     *
     * @throws XAException An error has occurred. Possible exceptions   * are XA_RB*, XAER_RMERR, XAER_RMFAIL,
     *                     XAER_DUPID, XAER_OUTSIDE, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     */
    public synchronized void start(Xid xid, int flags) throws XAException {

        NetAgent netAgent = conn_.netAgent_;
        int rc = XAResource.XA_OK;
        exceptionsOnXA = null;
        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "start", xid, flags);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }
        
        // DERBY-1025 - Flow an auto-commit if in auto-commit mode before 
        // entering a global transaction
        try {
            if(conn_.autoCommit_)
                conn_.flowAutoCommit();
        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        } 

        // update the XACallInfo
        NetXACallInfo callInfo = callInfoArray_[conn_.currXACallInfoOffset_];
        callInfo.xaFlags_ = flags;
        callInfo.xid_ = xid;
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL

        // check and setup the transaction timeout settings
        if (flags == TMNOFLAGS) {
            if (timeoutSeconds == Integer.MAX_VALUE) {
                // Disable the transaction timeout.
                callInfo.xaTimeoutMillis_ = 0;
            } else if (timeoutSeconds > 0) {
                // Use the timeout value specified.
                callInfo.xaTimeoutMillis_ = 1000*timeoutSeconds;
            } else if (timeoutSeconds == 0) {
                // The -1 value means that the timeout codepoint
                // will not be sent in the request and thus the server
                // will use the default value.
                callInfo.xaTimeoutMillis_ = -1;
            } else {
                // This should not ever happen due that setTransactionTimeout
                // does not allow a negative value
                throwXAException(XAException.XAER_RMERR);
            }
        }
        try {
            netAgent.beginWriteChainOutsideUOW();
            netAgent.netConnectionRequest_.writeXaStartUnitOfWork(conn_);
            netAgent.flowOutsideUOW();
            netAgent.netConnectionReply_.readXaStartUnitOfWork(conn_);
            if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = XAFUNC_START;
                rc = xaRetValErrorAccumSQL(callInfo, rc);
                callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            }
            // Setting this is currently required to avoid client from sending
            // commit for autocommit.
            if (rc == XAResource.XA_OK) {
                conn_.setXAState(ClientConnection.XA_T1_ASSOCIATED);
            }

        } catch (SqlException sqle) {
            rc = getSqlExceptionXAErrorCode(sqle);
            exceptionsOnXA = Utils.accumulateSQLException(sqle, exceptionsOnXA);
        }

        if (rc != XAResource.XA_OK) {
            throwXAException(rc);
        }
    }

    private String getXAExceptionText(int rc) {
        String xaExceptionText;
        switch (rc) {
        case XAException.XA_RBROLLBACK:
            xaExceptionText = "XA_RBROLLBACK";
            break;
        case XAException.XA_RBCOMMFAIL:
            xaExceptionText = "XA_RBCOMMFAIL";
            break;
        case XAException.XA_RBDEADLOCK:
            xaExceptionText = "XA_RBDEADLOCK";
            break;
        case XAException.XA_RBINTEGRITY:
            xaExceptionText = "XA_RBINTEGRITY";
            break;
        case XAException.XA_RBOTHER:
            xaExceptionText = "XA_RBOTHER";
            break;
        case XAException.XA_RBPROTO:
            xaExceptionText = "XA_RBPROTO";
            break;
        case XAException.XA_RBTIMEOUT:
            xaExceptionText = "XA_RBTIMEOUT";
            break;
        case XAException.XA_RBTRANSIENT:
            xaExceptionText = "XA_RBTRANSIENT";
            break;
        case XAException.XA_NOMIGRATE:
            xaExceptionText = "XA_NOMIGRATE";
            break;
        case XAException.XA_HEURHAZ:
            xaExceptionText = "XA_HEURHAZ";
            break;
        case XAException.XA_HEURCOM:
            xaExceptionText = "XA_HEURCOM";
            break;
        case XAException.XA_HEURRB:
            xaExceptionText = "XA_HEURRB";
            break;
        case XAException.XA_HEURMIX:
            xaExceptionText = "XA_HEURMIX";
            break;
        case XAException.XA_RETRY:
            xaExceptionText = "XA_RETRY";
            break;
        case XAException.XA_RDONLY:
            xaExceptionText = "XA_RDONLY";
            break;
        case XAException.XAER_ASYNC:
            xaExceptionText = "XAER_ASYNC";
            break;
        case XAException.XAER_RMERR:
            xaExceptionText = "XAER_RMERR";
            break;
        case XAException.XAER_NOTA:
            xaExceptionText = "XAER_NOTA";
            break;
        case XAException.XAER_INVAL:
            xaExceptionText = "XAER_INVAL";
            break;
        case XAException.XAER_PROTO:
            xaExceptionText = "XAER_PROTO";
            break;
        case XAException.XAER_RMFAIL:
            xaExceptionText = "XAER_RMFAIL";
            break;
        case XAException.XAER_DUPID:
            xaExceptionText = "XAER_DUPID";
            break;
        case XAException.XAER_OUTSIDE:
            xaExceptionText = "XAER_OUTSIDE";
            break;
        case XAResource.XA_OK:
            xaExceptionText = "XA_OK";
            break;
        default:
            xaExceptionText = "Unknown Error";
            break;
        }
        return xaExceptionText;
    }

    private void throwXAException(int rc)
            throws XAException {

        StringBuilder xaExceptionText = new StringBuilder(64);

        xaExceptionText.append(getXAExceptionText(rc));
        // save the SqlException chain to add it to the XAException
        SqlException sqlExceptions = exceptionsOnXA;

        while (exceptionsOnXA != null) { // one or more SqlExceptions received, format them
            xaExceptionText.append(" : ").append(exceptionsOnXA.getMessage());
            exceptionsOnXA = (SqlException)
                    exceptionsOnXA.getNextException();
        }

        XaException xaException = new XaException(
                conn_.agent_.logWriter_,
                sqlExceptions,
                xaExceptionText.toString());

        xaException.errorCode = rc;
        setXaStateForXAException(rc); 
        throw xaException;
    }


    /**
     * Reset the transaction branch association state  to XA_T0_NOT_ASSOCIATED
     * for XAER_RM* and XA_RB* Exceptions. All other exceptions leave the state
     * unchanged
     * 
     * @param rc  // return code from XAException
     * @throws XAException
     */
    private void setXaStateForXAException(int rc) {
        switch (rc)
        {
            // Reset to T0, not  associated for XA_RB*, RM*
           // XAER_RMFAIL and XAER_RMERR will be fatal to the connection
           // but that is not dealt with here
           case XAException.XAER_RMFAIL:
           case XAException.XAER_RMERR:
           case XAException.XA_RBROLLBACK:
           case XAException.XA_RBCOMMFAIL:
           case XAException.XA_RBDEADLOCK:
           case XAException.XA_RBINTEGRITY:
           case XAException.XA_RBOTHER:
           case XAException.XA_RBPROTO:
           case XAException.XA_RBTIMEOUT:
           case XAException.XA_RBTRANSIENT:
               conn_.setXAState(ClientConnection.XA_T0_NOT_ASSOCIATED);
           break;
            // No change for other XAExceptions
            // XAException.XA_NOMIGRATE
           //XAException.XA_HEURHAZ
           // XAException.XA_HEURCOM
           // XAException.XA_HEURRB
           // XAException.XA_HEURMIX
           // XAException.XA_RETRY
           // XAException.XA_RDONLY
           // XAException.XAER_ASYNC
           // XAException.XAER_NOTA
           // XAException.XAER_INVAL
           // XAException.XAER_PROTO
           // XAException.XAER_DUPID
           // XAException.XAER_OUTSIDE
            default:
                return;
        }    
    }

    public boolean isSameRM(XAResource xares) throws XAException {
        boolean isSame = false; // preset that the RMs are NOT the same
        exceptionsOnXA = null;

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceEntry(this, "isSameRM", xares);
        }
        if (conn_.isPhysicalConnClosed()) {
            connectionClosedFailure();
        }

        if (xares instanceof NetXAResource) {
            // Both are NetXAResource so check to see if this is the same RM.
            // Remember, isSame is initialized to false
            NetXAResource derbyxares = (NetXAResource) xares;
            while (true) {
                if (!conn_.databaseName_.equalsIgnoreCase(derbyxares.conn_.databaseName_)) {
                    break; // database names are not equal, not same RM
                }
                if (!conn_.netAgent_.server_.equalsIgnoreCase
                        (derbyxares.conn_.netAgent_.server_)) { // server name strings not equal, compare IP addresses
                    try {
                        // 1st convert "localhost" to actual server name
                        String server1 = this.processLocalHost(conn_.netAgent_.server_);
                        String server2 =
                                this.processLocalHost(derbyxares.conn_.netAgent_.server_);
                        // now convert the server name to ip address
                        InetAddress serverIP1 = InetAddress.getByName(server1);
                        InetAddress serverIP2 = InetAddress.getByName(server2);
                        if (!serverIP1.equals(serverIP2)) {
                            break; // server IPs are not equal, not same RM
                        }
                    } catch (UnknownHostException ue) {
                        break;
                    }
                }
                if (conn_.netAgent_.port_ != derbyxares.conn_.netAgent_.port_) {
                    break; // ports are not equal, not same RM
                }
                isSame = true; // everything the same, set RMs are the same
                break;
            }
        }

        if (conn_.agent_.loggingEnabled()) {
            conn_.agent_.logWriter_.traceExit
                    (this, "isSameRM", isSame);
        }
        return isSame;
    }
    
    public static boolean xidsEqual(Xid xid1, Xid xid2) { // determine if the 2 xids contain the same values even if not same object
        // comapre the format ids
        if (xid1.getFormatId() != xid2.getFormatId()) {
            return false; // format ids are not the same
        }

        // compare the global transaction ids
        int xid1Length = xid1.getGlobalTransactionId().length;
        if (xid1Length != xid2.getGlobalTransactionId().length) {
            return false; // length of the global trans ids are not the same
        }
        byte[] xid1Bytes = xid1.getGlobalTransactionId();
        byte[] xid2Bytes = xid2.getGlobalTransactionId();
        int i;
        for (i = 0; i < xid1Length; ++i) { // check all bytes are the same
            if (xid1Bytes[i] != xid2Bytes[i]) {
                return false; // bytes in the global trans ids are not the same
            }
        }

        // compare the branch qualifiers
        xid1Length = xid1.getBranchQualifier().length;
        if (xid1Length != xid2.getBranchQualifier().length) {
            return false; // length of the global trans ids are not the same
        }
        xid1Bytes = xid1.getBranchQualifier();
        xid2Bytes = xid2.getBranchQualifier();
        for (i = 0; i < xid1Length; ++i) { // check all bytes are the same
            if (xid1Bytes[i] != xid2Bytes[i]) {
                return false; // bytes in the global trans ids are not the same
            }
        }

        return true; // all of the fields are the same, xid1 == xid2
    }


    private void connectionClosedFailure() throws XAException { // throw an XAException XAER_RMFAIL, with a chained SqlException - closed
        exceptionsOnXA = Utils.accumulateSQLException
                (new SqlException(null, 
                        new ClientMessageId(SQLState.NO_CURRENT_CONNECTION)),
                        exceptionsOnXA);
        throwXAException(XAException.XAER_RMFAIL);
    }

    private String getXAFuncStr(int xaFunc) {
        switch (xaFunc) {
        case XAFUNC_COMMIT:
            return XAFUNCSTR_COMMIT;
        case XAFUNC_END:
            return XAFUNCSTR_END;
        case XAFUNC_FORGET:
            return XAFUNCSTR_FORGET;
        case XAFUNC_PREPARE:
            return XAFUNCSTR_PREPARE;
        case XAFUNC_RECOVER:
            return XAFUNCSTR_RECOVER;
        case XAFUNC_ROLLBACK:
            return XAFUNCSTR_ROLLBACK;
        case XAFUNC_START:
            return XAFUNCSTR_START;
        }
        return XAFUNCSTR_NONE;
    }

    protected int xaRetValErrorAccumSQL(NetXACallInfo callInfo, int currentRC) {

        // xaRetVal_ is set by the server to be one of the
        // standard constants from XAException.
        int rc = callInfo.xaRetVal_;

        if (rc != XAResource.XA_OK) { // error was detected
            // create an SqlException to report this error within
            SqlException accumSql = new SqlException(conn_.netAgent_.logWriter_,
                new ClientMessageId(SQLState.NET_XARETVAL_ERROR),
                SqlCode.queuedXAError,
                getXAFuncStr(callInfo.xaFunction_),
                getXAExceptionText(rc));
            exceptionsOnXA = Utils.accumulateSQLException
                    (accumSql, exceptionsOnXA);

            if (currentRC != XAResource.XA_OK) { // the rc passed into this function had an error also, prioritize error
                if (currentRC < 0) { // rc passed in was a major error use it instead of current error
                    return currentRC;
                }
            }
        }
        return rc;
    }

    private String processLocalHost(String serverName) {
        if (serverName.equalsIgnoreCase("localhost")) { // this is a localhost, find hostname
            try {
                InetAddress localhostNameIA = InetAddress.getLocalHost();
                String localhostName = localhostNameIA.getHostName();
                return localhostName;
            } catch (SecurityException se) {
                return serverName;
            } catch (UnknownHostException ue) {
                return serverName;
            }
        }
        // not "localhost", return original server name
        return serverName;
    }
}

/*

   Derby - Class org.apache.derby.client.net.NetXAConnection

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.net;

import java.sql.SQLException;

import javax.transaction.xa.Xid;

import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Statement;

public class NetXAConnection extends org.apache.derby.client.net.NetConnection {
    //---------------------constructors/finalizer---------------------------------
    // For XA Connections
    public NetXAConnection(NetLogWriter netLogWriter,
                           String user,
                           String password,
                           org.apache.derby.jdbc.ClientDataSource dataSource,
                           int rmId,
                           boolean isXAConn) throws SqlException {
        super(netLogWriter, user, password, dataSource, rmId, isXAConn);
        checkPlatformVersion();
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    public void setCorrelatorToken(byte[] crttoken) {
        crrtkn_ = crttoken;
    }

    public byte[] getCorrelatorToken() {
        return crrtkn_;
    }

    void setNetXAResource(NetXAResource xares) {
        xares_ = xares;
    }

    public void writeLocalXAStart_() throws SqlException {
        netAgent_.netConnectionRequest_.writeLocalXAStart(this);
    }

    public void readLocalXAStart_() throws SqlException {
        netAgent_.netConnectionReply_.readLocalXAStart(this);
    }

    public void writeLocalXACommit_() throws SqlException {
        netAgent_.netConnectionRequest_.writeLocalXACommit(this);
    }

    public void readLocalXACommit_() throws SqlException {
        netAgent_.netConnectionReply_.readLocalXACommit(this);
    }

    public void writeLocalXARollback_() throws SqlException {
        netAgent_.netConnectionRequest_.writeLocalXARollback(this);
    }

    public void readLocalXARollback_() throws SqlException {
        netAgent_.netConnectionReply_.readLocalXARollback(this);
    }

    public void writeTransactionStart(Statement statement) throws SqlException {
        //KATHEY  remove below after checking that we don't need it.
        if (!isXAConnection_) {
            return; // not a XA connection
        }

        // this is a XA connection
        int xaState = getXAState();
        xares_.exceptionsOnXA = null;
        //TODO: Looks like this can go and also the whole client indoubtTransaction code.
        /*
        if (xaState == XA_RECOVER) { // in recover, clean up and go to open-idle
            if (indoubtTransactions_ != null) {
                indoubtTransactions_.clear();
                indoubtTransactions_ = null;
                setXAState(XA_OPEN_IDLE);
                xaState = XA_OPEN_IDLE;
            }
            
        }*/
        // For derby we don't need to write transaction start for a local
        //transaction.  If autocommit is off we are good to go.
        return;
    }

    public void setIndoubtTransactions(java.util.Hashtable indoubtTransactions) {
        if (indoubtTransactions_ != null) {
            indoubtTransactions_.clear();
        }
        indoubtTransactions_ = indoubtTransactions;
    }

    public byte[] getUOWID(Xid xid) {
        NetIndoubtTransaction indoubtTxn = (NetIndoubtTransaction) indoubtTransactions_.get(xid);
        if (indoubtTxn == null) {
            return null;
        }
        byte[] uowid = indoubtTxn.getUOWID();
        return uowid;
    }


    public int getPort(Xid xid) {
        NetIndoubtTransaction indoubtTxn = (NetIndoubtTransaction) indoubtTransactions_.get(xid);
        if (indoubtTxn == null) {
            return -1;
        }
        return indoubtTxn.getPort();
    }

    public void writeCommit() throws SqlException {
        // this logic must be in sync with willAutoCommitGenerateFlow() logic
        if (isXAConnection_) { // XA Connection
            int xaState = getXAState();
            if (xaState == XA_T0_NOT_ASSOCIATED){
                xares_.callInfoArray_[xares_.conn_.currXACallInfoOffset_].xid_ =
                        NetXAResource.nullXid;
                writeLocalXACommit_();
            }
        } else { // not XA connection
            writeLocalCommit_();
        }
    }

    public void readCommit() throws SqlException {
        if (isXAConnection_) { // XA Connection
            int xaState = getXAState();
            NetXACallInfo callInfo = xares_.callInfoArray_[currXACallInfoOffset_];
            callInfo.xaRetVal_ = NetXAResource.XARETVAL_XAOK; // initialize XARETVAL
            if (xaState == XA_T0_NOT_ASSOCIATED) {
                readLocalXACommit_();
                //TODO: Remove
                //setXAState(XA_LOCAL);
            }
            if (callInfo.xaRetVal_ != NetXAResource.XARETVAL_XAOK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = NetXAResource.XAFUNC_COMMIT;
                xares_.xaRetValErrorAccumSQL(callInfo, 0);
                callInfo.xaRetVal_ = NetXAResource.XARETVAL_XAOK; // re-initialize XARETVAL
                throw xares_.exceptionsOnXA;
            }
        } else
        // non-XA connections
        {
            readLocalCommit_();
        }
    }

    public void writeRollback() throws SqlException {
        if (isXAConnection_) {
            xares_.callInfoArray_[xares_.conn_.currXACallInfoOffset_].xid_ =
                    xares_.nullXid;
            writeLocalXARollback_();
        } else {
            writeLocalRollback_(); // non-XA
        }
    }

    public void readRollback() throws SqlException {
        if (isXAConnection_) { // XA connections
            NetXACallInfo callInfo = xares_.callInfoArray_[currXACallInfoOffset_];
            callInfo.xaRetVal_ = NetXAResource.XARETVAL_XAOK; // initialize XARETVAL
            readLocalXARollback_();

            if (callInfo.xaRetVal_ != NetXAResource.XARETVAL_XAOK) { // xaRetVal has possible error, format it
                callInfo.xaFunction_ = NetXAResource.XAFUNC_ROLLBACK;
                xares_.xaRetValErrorAccumSQL(callInfo, 0);
                callInfo.xaRetVal_ = NetXAResource.XARETVAL_XAOK; // re-initialize XARETVAL
                throw xares_.exceptionsOnXA;
            }


            // for all XA connectiions
            // TODO:KATHEY - Do we need this?
            setXAState(XA_T0_NOT_ASSOCIATED);
        } else {
            readLocalRollback_(); // non-XA connections
        }
    }

    synchronized public void close() throws SQLException {
        // call super.close*() to do the close*
        super.close();
        if (open_) {
            return; // still open, return
        }
        if (xares_ != null) {
            xares_.removeXaresFromSameRMchain();
        }
    }

    synchronized public void closeX() throws SQLException {
        // call super.close*() to do the close*
        super.closeX();
        if (open_) {
            return; // still open, return
        }
        if (xares_ != null) {
            xares_.removeXaresFromSameRMchain();
        }
    }

    synchronized public void closeForReuse() throws SqlException {
        // call super.close*() to do the close*
        super.closeForReuse();
        if (open_) {
            return; // still open, return
        }
        if (xares_ != null) {
            xares_.removeXaresFromSameRMchain();
        }
    }

    synchronized public void closeResources() throws SQLException {
        // call super.close*() to do the close*
        super.closeResources();
        if (open_) {
            return; // still open, return
        }
        if (xares_ != null) {
            xares_.removeXaresFromSameRMchain();
        }
    }

    private void checkPlatformVersion() throws SqlException {
        int supportedVersion;

        supportedVersion = 8;

        if (xaHostVersion_ >= supportedVersion) { // supported version, return
            return;
        }

        // unsupported version for platform
        String platform = null;
        platform = "Linux, Unix, Windows";
        String versionMsg = "On " + platform + " XA supports version " +
                supportedVersion + " and above, this is version " +
                xaHostVersion_;
        throw new SqlException(agent_.logWriter_, versionMsg);
    }
}

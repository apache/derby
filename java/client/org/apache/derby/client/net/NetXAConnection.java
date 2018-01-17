/*

   Derby - Class org.apache.derby.client.net.NetXAConnection

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

package org.apache.derby.client.net;

import javax.transaction.xa.XAResource;

import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientStatement;

import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.BasicClientDataSource;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.jdbc.ClientDriver;

public class NetXAConnection {    
    private NetConnection netCon;
    //---------------------constructors/finalizer---------------------------------
    // For XA Connections    
    /**
     *
     * The construcor for the NetXAConnection. The parameter 
     * is set to <code>this</code> from ClientXAConnection when
     * it creates an instance of NetXAConnection. This is then
     * passed on the underlying NetConnection constructor and is 
     * used to raise StatementEvents from any PreparedStatement that
     * would be created from that NetConnection.
     *
     * @param logWriter    LogWriter object associated with this connection
     * @param user         user id for this connection
     * @param password     password for this connection
     * @param dataSource   The DataSource object passed from the ClientXAConnection 
     *                     object from which this constructor was called
     * @param rmId         The Resource manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this 
     *                     NetConnection constructor was called. This is used
     *                     to pass StatementEvents back to the pooledConnection
     *                     object
     * @throws SqlException
     * 
     */
    public NetXAConnection(
            LogWriter logWriter,
            String user,
            String password,
            BasicClientDataSource dataSource,
            int rmId,
            boolean isXAConn,
            ClientPooledConnection cpc) throws SqlException {

        netCon = createNetConnection(logWriter, user, password,
                dataSource, rmId, isXAConn,cpc);
        checkPlatformVersion();
    }

    public void setCorrelatorToken(byte[] crttoken) {
        netCon.crrtkn_ = crttoken;
    }

    void setNetXAResource(NetXAResource xares) {
        netCon.xares_ = xares;
    }

    private void writeLocalXACommit_() throws SqlException {
        netCon.netAgent_.netConnectionRequest_.writeLocalXACommit(netCon);
    }

    private void readLocalXACommit_() throws SqlException {
        netCon.netAgent_.netConnectionReply_.readLocalXACommit(netCon);
    }

    private void writeLocalXARollback_() throws SqlException {
        netCon.netAgent_.netConnectionRequest_.writeLocalXARollback(netCon);
    }

    private void readLocalXARollback_() throws SqlException {
        netCon.netAgent_.netConnectionReply_.readLocalXARollback(netCon);
    }

    void writeTransactionStart(ClientStatement statement)
            throws SqlException {
        //KATHEY  remove below after checking that we don't need it.
        if (!netCon.isXAConnection()) {
            return; // not a XA connection
        }

        // this is a XA connection
        int xaState = netCon.getXAState();
        netCon.xares_.exceptionsOnXA = null;
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

    void writeCommit() throws SqlException {
        // this logic must be in sync with willAutoCommitGenerateFlow() logic
        int xaState = netCon.getXAState();
        if (xaState == netCon.XA_T0_NOT_ASSOCIATED){
            netCon.xares_.callInfoArray_[
                    netCon.xares_.conn_.currXACallInfoOffset_
                    ].xid_ = NetXAResource.nullXid;
            writeLocalXACommit_();
        }
    }

    void readCommit() throws SqlException {
        int xaState = netCon.getXAState();
        NetXACallInfo callInfo = netCon.xares_.callInfoArray_
                [netCon.currXACallInfoOffset_];
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        if (xaState == netCon.XA_T0_NOT_ASSOCIATED) {
            readLocalXACommit_();
            //TODO: Remove
            //setXAState(XA_LOCAL);
        }
        if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
            callInfo.xaFunction_ = NetXAResource.XAFUNC_COMMIT;
            netCon.xares_.xaRetValErrorAccumSQL(callInfo, 0);
            callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            throw netCon.xares_.exceptionsOnXA;
        }        
    }

    void writeRollback() throws SqlException {
      netCon.xares_.callInfoArray_[
                netCon.xares_.conn_.currXACallInfoOffset_
                ].xid_ = netCon.xares_.nullXid;
       writeLocalXARollback_(); 
    }

    void readRollback() throws SqlException {
        NetXACallInfo callInfo = netCon.xares_.callInfoArray_
                [netCon.currXACallInfoOffset_];
        callInfo.xaRetVal_ = XAResource.XA_OK; // initialize XARETVAL
        readLocalXARollback_();

        if (callInfo.xaRetVal_ != XAResource.XA_OK) { // xaRetVal has possible error, format it
            callInfo.xaFunction_ = NetXAResource.XAFUNC_ROLLBACK;
            netCon.xares_.xaRetValErrorAccumSQL(callInfo, 0);
            callInfo.xaRetVal_ = XAResource.XA_OK; // re-initialize XARETVAL
            throw netCon.xares_.exceptionsOnXA;
        }


        // for all XA connectiions
        // TODO:KATHEY - Do we need this?
        netCon.setXAState(netCon.XA_T0_NOT_ASSOCIATED);
    }

    /**
     * Returns underlying net connection
     * @return NetConnection
     */
    public NetConnection getNetConnection () {
        return netCon;
    }

    private void checkPlatformVersion() throws SqlException {
        int supportedVersion;

        supportedVersion = 8;

        if (netCon.xaHostVersion_ >= supportedVersion) { 
            // supported version, return
            return;
        }

        // unsupported version for platform
        String platform = null;
        platform = "Linux, Unix, Windows";
        throw new SqlException(netCon.agent_.logWriter_, 
            new ClientMessageId(SQLState.NET_WRONG_XA_VERSION),
            platform, supportedVersion, netCon.xaHostVersion_);
    }
    
    /**
     *
     * Creates NetConnection for the supported version of jdbc.
     * This method can be overwritten to return NetConnection
     * of the supported jdbc version.
     * @param logWriter    LogWriter object associated with this connection
     * @param user         user id for this connection
     * @param password     password for this connection
     * @param dataSource   The DataSource object passed from the ClientXAConnection 
     *                     object from which this constructor was called
     * @param rmId         The Resource manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this 
     *                     NetConnection constructor was called. This is used
     *                     to pass StatementEvents back to the pooledConnection
     *                     object
     * @return NetConnection
     *
     */
    private NetConnection createNetConnection (
            LogWriter logWriter,
            String user,
            String password,
            BasicClientDataSource dataSource,
            int rmId,
            boolean isXAConn,
            ClientPooledConnection cpc) throws SqlException {

        return (NetConnection)ClientDriver.getFactory().newNetConnection
            (logWriter, user, password,dataSource, rmId, isXAConn,cpc);
    }
}

/*

   Derby - Class org.apache.derby.client.net.NetAgent

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

import java.net.SocketException;

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.Utils;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.i18n.MessageUtil;

public class NetAgent extends Agent {
    //---------------------navigational members-----------------------------------

    // All these request objects point to the same physical request object.
    public ConnectionRequestInterface connectionRequest_;
    public NetConnectionRequest packageRequest_;
    public StatementRequestInterface statementRequest_;
    public ResultSetRequestInterface resultSetRequest_;

    // All these reply objects point to the same physical reply object.
    public ConnectionReply connectionReply_;
    public ConnectionReply packageReply_;
    public StatementReply statementReply_;
    public ResultSetReply resultSetReply_;

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    // Alias for (NetConnection) super.connection
    NetConnection netConnection_;

    // Alias for (Request) super.*Request, all in one
    // In the case of the NET implementation, these all point to the same physical request object.
    protected Request request_;
    public NetConnectionRequest netConnectionRequest_;
    public NetPackageRequest netPackageRequest_;
    public NetStatementRequest netStatementRequest_;
    public NetResultSetRequest netResultSetRequest_;

    // Alias for (Reply) super.*Reply, all in one.
    // In the case of the NET implementation, these all point to the same physical reply object.
    protected Reply reply_;
    public NetConnectionReply netConnectionReply_;
    public NetPackageReply netPackageReply_;
    public NetStatementReply netStatementReply_;
    public NetResultSetReply netResultSetReply_;

    //-----------------------------state------------------------------------------

    java.net.Socket socket_;
    java.io.InputStream rawSocketInputStream_;
    java.io.OutputStream rawSocketOutputStream_;

    String server_;
    int port_;
    int clientSSLMode_;

    private EbcdicCcsidManager ebcdicCcsidManager_;
    private Utf8CcsidManager utf8CcsidManager_;
    private CcsidManager currentCcsidManager_;
    
    // TODO: Remove target? Keep just one CcsidManager?
    //public CcsidManager targetCcsidManager_;
    public Typdef typdef_;
    public Typdef targetTypdef_;
    public Typdef originalTargetTypdef_; // added to support typdef overrides

    protected int svrcod_;

    public int orignalTargetSqlam_ = NetConfiguration.MGRLVL_7;
    public int targetSqlam_ = orignalTargetSqlam_;

    public SqlException exceptionOpeningSocket_ = null;
    public SqlException exceptionConvertingRdbnam = null;
    
    /**
     * Flag which indicates that a writeChain has been started and data sent to
     * the server.
     * If true, starting a new write chain will throw a DisconnectException. 
     * It is cleared when the write chain is ended.
     */
    private boolean writeChainIsDirty_ = false;
    //---------------------constructors/finalizer---------------------------------
    public NetAgent(NetConnection netConnection,
                    org.apache.derby.client.am.LogWriter logWriter) throws SqlException {
        super(netConnection, logWriter);
        this.netConnection_ = netConnection;
    }

    NetAgent(NetConnection netConnection,
             org.apache.derby.client.am.LogWriter netLogWriter,
             int loginTimeout,
             String server,
             int port,
             int clientSSLMode) throws SqlException {
        super(netConnection, netLogWriter);

        server_ = server;
        port_ = port;
        netConnection_ = netConnection;
        clientSSLMode_ = clientSSLMode;

        if (server_ == null) {
            throw new DisconnectException(this, 
                new ClientMessageId(SQLState.CONNECT_REQUIRED_PROPERTY_NOT_SET),
                "serverName");
        }

        try {
            socket_ = (java.net.Socket) java.security.AccessController.doPrivileged(new OpenSocketAction(server, port, clientSSLMode_));
        } catch (java.security.PrivilegedActionException e) {
            throw new DisconnectException(this,
                new ClientMessageId(SQLState.CONNECT_UNABLE_TO_CONNECT_TO_SERVER),
                new Object[] { e.getException().getClass().getName(), server, 
                    Integer.toString(port), e.getException().getMessage() },
                e.getException());
        }

        // Set TCP/IP Socket Properties
        try {
            if (exceptionOpeningSocket_ == null) {
                socket_.setTcpNoDelay(true); // disables nagles algorithm
                socket_.setKeepAlive(true); // PROTOCOL Manual: TCP/IP connection allocation rule #2
                socket_.setSoTimeout(loginTimeout * 1000);
            }
        } catch (java.net.SocketException e) {
            try {
                socket_.close();
            } catch (java.io.IOException doNothing) {
            }
            exceptionOpeningSocket_ = new DisconnectException(this,
                new ClientMessageId(SQLState.CONNECT_SOCKET_EXCEPTION),
                e.getMessage(), e);
        }

        try {
            if (exceptionOpeningSocket_ == null) {
                rawSocketOutputStream_ = socket_.getOutputStream();
                rawSocketInputStream_ = socket_.getInputStream();
            }
        } catch (java.io.IOException e) {
            try {
                socket_.close();
            } catch (java.io.IOException doNothing) {
            }
            exceptionOpeningSocket_ = new DisconnectException(this, 
                new ClientMessageId(SQLState.CONNECT_UNABLE_TO_OPEN_SOCKET_STREAM),
                e.getMessage(), e);
        }

        ebcdicCcsidManager_ = new EbcdicCcsidManager();
        utf8CcsidManager_ = new Utf8CcsidManager();
        
        currentCcsidManager_ = ebcdicCcsidManager_;

        if (netConnection_.isXAConnection()) {
            NetXAConnectionReply netXAConnectionReply_ = new NetXAConnectionReply(this, netConnection_.commBufferSize_);
            netResultSetReply_ = (NetResultSetReply) netXAConnectionReply_;
            netStatementReply_ = (NetStatementReply) netResultSetReply_;
            netPackageReply_ = (NetPackageReply) netStatementReply_;
            netConnectionReply_ = (NetConnectionReply) netPackageReply_;
            reply_ = (Reply) netConnectionReply_;

            resultSetReply_ = new ResultSetReply(this,
                    netResultSetReply_,
                    netStatementReply_,
                    netConnectionReply_);
            statementReply_ = (StatementReply) resultSetReply_;
            packageReply_ = (ConnectionReply) statementReply_;
            connectionReply_ = (ConnectionReply) packageReply_;
            NetXAConnectionRequest netXAConnectionRequest_ = new NetXAConnectionRequest(this, netConnection_.commBufferSize_);
            netResultSetRequest_ = (NetResultSetRequest) netXAConnectionRequest_;
            netStatementRequest_ = (NetStatementRequest) netResultSetRequest_;
            netPackageRequest_ = (NetPackageRequest) netStatementRequest_;
            netConnectionRequest_ = (NetConnectionRequest) netPackageRequest_;
            request_ = (Request) netConnectionRequest_;

            resultSetRequest_ = (ResultSetRequestInterface) netResultSetRequest_;
            statementRequest_ = (StatementRequestInterface) netStatementRequest_;
            packageRequest_ = (NetConnectionRequest) netPackageRequest_;
            connectionRequest_ = (ConnectionRequestInterface) netConnectionRequest_;
        } else {
            netResultSetReply_ = new NetResultSetReply(this, netConnection_.commBufferSize_);
            netStatementReply_ = (NetStatementReply) netResultSetReply_;
            netPackageReply_ = (NetPackageReply) netStatementReply_;
            netConnectionReply_ = (NetConnectionReply) netPackageReply_;
            reply_ = (Reply) netConnectionReply_;

            resultSetReply_ = new ResultSetReply(this,
                    netResultSetReply_,
                    netStatementReply_,
                    netConnectionReply_);
            statementReply_ = (StatementReply) resultSetReply_;
            packageReply_ = (ConnectionReply) statementReply_;
            connectionReply_ = (ConnectionReply) packageReply_;
            netResultSetRequest_ = new NetResultSetRequest(this, netConnection_.commBufferSize_);
            netStatementRequest_ = (NetStatementRequest) netResultSetRequest_;
            netPackageRequest_ = (NetPackageRequest) netStatementRequest_;
            netConnectionRequest_ = (NetConnectionRequest) netPackageRequest_;
            request_ = (Request) netConnectionRequest_;

            resultSetRequest_ = (ResultSetRequestInterface) netResultSetRequest_;
            statementRequest_ = (StatementRequestInterface) netStatementRequest_;
            packageRequest_ = (NetConnectionRequest) netPackageRequest_;
            connectionRequest_ = (ConnectionRequestInterface) netConnectionRequest_;
        }
    }

    protected void resetAgent_(org.apache.derby.client.am.LogWriter netLogWriter,
                               //CcsidManager sourceCcsidManager,
                               //CcsidManager targetCcsidManager,
                               int loginTimeout,
                               String server,
                               int port) throws SqlException {
        
        exceptionConvertingRdbnam = null;
        // most properties will remain unchanged on connect reset.
        targetTypdef_ = originalTargetTypdef_;
        svrcod_ = 0;

        // Set TCP/IP Socket Properties
        try {
            socket_.setSoTimeout(loginTimeout * 1000);
        } catch (java.net.SocketException e) {
            try {
                socket_.close();
            } catch (java.io.IOException doNothing) {
            }
            throw new SqlException(logWriter_, 
                new ClientMessageId(SQLState.SOCKET_EXCEPTION),
                e.getMessage(), e);
        }
    }


    void setSvrcod(int svrcod) {
        if (svrcod > svrcod_) {
            svrcod_ = svrcod;
        }
    }

    void clearSvrcod() {
        svrcod_ = CodePoint.SVRCOD_INFO;
    }

    int getSvrcod() {
        return svrcod_;
    }

    public void flush_() throws DisconnectException {
        sendRequest();
        reply_.initialize();
    }

    // Close socket and its streams.
    public void close_() throws SqlException {
        // can we just close the socket here, do we need to close streams individually
        SqlException accumulatedExceptions = null;
        if (rawSocketInputStream_ != null) {
            try {
                rawSocketInputStream_.close();
            } catch (java.io.IOException e) {
                // note when {6} = 0 it indicates the socket was closed.
                // this should be ok since we are going to go an close the socket
                // immediately following this call.
                // changing {4} to e.getMessage() may require pub changes
                accumulatedExceptions = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                    e.getMessage(), e);
            } finally {
                rawSocketInputStream_ = null;
            }
        }

        if (rawSocketOutputStream_ != null) {
            try {
                rawSocketOutputStream_.close();
            } catch (java.io.IOException e) {
                // note when {6} = 0 it indicates the socket was closed.
                // this should be ok since we are going to go an close the socket
                // immediately following this call.
                // changing {4} to e.getMessage() may require pub changes
                SqlException latestException = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                    e.getMessage(), e);
                accumulatedExceptions = Utils.accumulateSQLException(latestException, accumulatedExceptions);
            } finally {
                rawSocketOutputStream_ = null;
            }
        }

        if (socket_ != null) {
            try {
                socket_.close();
            } catch (java.io.IOException e) {
                // again {6} = 0, indicates the socket was closed.
                // maybe set {4} to e.getMessage().
                // do this for now and but may need to modify or
                // add this to the message pubs.
                SqlException latestException = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                        e.getMessage(), e);
                accumulatedExceptions = Utils.accumulateSQLException(latestException, accumulatedExceptions);
            } finally {
                socket_ = null;
            }
        }

        if (accumulatedExceptions != null) {
            throw accumulatedExceptions;
        }
    }

    /**
     * Specifies the maximum blocking time that should be used when sending
     * and receiving messages. The timeout is implemented by using the the 
     * underlying socket implementation's timeout support. 
     * 
     * Note that the support for timeout on sockets is dependent on the OS 
     * implementation. For the same reason we ignore any exceptions thrown
     * by the call to the socket layer.
     * 
     * @param timeout The timeout value in seconds. A value of 0 corresponds to 
     * infinite timeout.
     */
    protected void setTimeout(int timeout) {
        try {
            // Sets a timeout on the socket
            socket_.setSoTimeout(timeout * 1000); // convert to milliseconds
        } catch (SocketException se) {
            // Silently ignore any exceptions from the socket layer
            if (SanityManager.DEBUG) {
                System.out.println("NetAgent.setTimeout: ignoring exception: " + 
                                   se);
            }
        }
    }

    /**
     * Returns the current timeout value that is set on the socket.
     * 
     * Note that the support for timeout on sockets is dependent on the OS 
     * implementation. For the same reason we ignore any exceptions thrown
     * by the call to the socket layer.
     * 
     * @return The timeout value in seconds. A value of 0 corresponds to
     * that no timeout is specified on the socket.
     */
    protected int getTimeout() {
        int timeout = 0; // 0 is default timeout for sockets

        // Read the timeout currently set on the socket
        try {
            timeout = socket_.getSoTimeout();
        } catch (SocketException se) {
            // Silently ignore any exceptions from the socket layer
            if (SanityManager.DEBUG) {
                System.out.println("NetAgent.getTimeout: ignoring exception: " + 
                                   se);
            }
        }

        // Convert from milliseconds to seconds (note that this truncates
        // the results towards zero but that should not be a problem).
        timeout = timeout / 1000;
        return timeout;
    }

    protected void sendRequest() throws DisconnectException {
        try {
            request_.flush(rawSocketOutputStream_);
        } catch (java.io.IOException e) {
            throwCommunicationsFailure(e);
        }
    }

    public java.io.InputStream getInputStream() {
        return rawSocketInputStream_;
    }

    public CcsidManager getCurrentCcsidManager() {
        return currentCcsidManager_;
    }
    
    public java.io.OutputStream getOutputStream() {
        return rawSocketOutputStream_;
    }

    void setInputStream(java.io.InputStream inputStream) {
        rawSocketInputStream_ = inputStream;
    }

    void setOutputStream(java.io.OutputStream outputStream) {
        rawSocketOutputStream_ = outputStream;
    }

    public void throwCommunicationsFailure(Throwable cause) 
        throws org.apache.derby.client.am.DisconnectException {
        //org.apache.derby.client.am.DisconnectException
        //accumulateReadExceptionAndDisconnect
        // note when {6} = 0 it indicates the socket was closed.
        // need to still validate any token values against message publications.
        accumulateChainBreakingReadExceptionAndThrow(
            new org.apache.derby.client.am.DisconnectException(this,
                new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                cause.getMessage(), cause));
    }
        
    // ----------------------- call-down methods ---------------------------------

    public org.apache.derby.client.am.LogWriter newLogWriter_(java.io.PrintWriter printWriter,
                                                              int traceLevel) {
        return new NetLogWriter(printWriter, traceLevel);
    }

    protected void markChainBreakingException_() {
        setSvrcod(CodePoint.SVRCOD_ERROR);
    }

    public void checkForChainBreakingException_() throws SqlException {
        int svrcod = getSvrcod();
        clearSvrcod();
        if (svrcod > CodePoint.SVRCOD_WARNING) // Not for SQL warning, if svrcod > WARNING, then its a chain breaker
        {
            super.checkForExceptions(); // throws the accumulated exceptions, we'll always have at least one.
        }
    }

    private void writeDeferredResetConnection() throws SqlException {
        if (!netConnection_.resetConnectionAtFirstSql_) {
            return;
        }
        try {
            netConnection_.writeDeferredReset();
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(this, 
                new ClientMessageId(SQLState.CONNECTION_FAILED_ON_DEFERRED_RESET));
            de.setNextException(sqle);
            throw de;
        }
    }
    /**
     * Marks the agent's write chain as dirty. A write chain is dirty when data
     * from it has been sent to the server. A dirty write chain cannot be reset 
     * and reused for another request until the remaining data has been sent to
     * the server and the write chain properly ended. 
     * 
     * Resetting a dirty chain will cause the new request to be appended to the 
     * unfinished request already at the server, which will likely lead to 
     * cryptic syntax errors.
     */
    void markWriteChainAsDirty() {    
        writeChainIsDirty_ = true;
    }
    
    private void verifyWriteChainIsClean() throws DisconnectException {
        if (writeChainIsDirty_) { 
            throw new DisconnectException(this, 
                new ClientMessageId(SQLState.NET_WRITE_CHAIN_IS_DIRTY));
        }
    }
    public void beginWriteChainOutsideUOW() throws SqlException {
        verifyWriteChainIsClean();
        request_.initialize();
        writeDeferredResetConnection();
    }

    public void beginWriteChain(org.apache.derby.client.am.Statement statement) throws SqlException {
        verifyWriteChainIsClean();
        request_.initialize();
        writeDeferredResetConnection();
        super.beginWriteChain(statement);
    }

    protected void endWriteChain() {}
    
    private void readDeferredResetConnection() throws SqlException {
        if (!netConnection_.resetConnectionAtFirstSql_) {
            return;
        }
        try {
            netConnection_.readDeferredReset();
            checkForExceptions();
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(this, 
                new ClientMessageId(SQLState.CONNECTION_FAILED_ON_DEFERRED_RESET));
            de.setNextException(sqle);
            throw de;
        }
    }

    protected void beginReadChain(org.apache.derby.client.am.Statement statement) throws SqlException {
        // Clear here as endWriteChain may not always be called
        writeChainIsDirty_ = false;
        readDeferredResetConnection();
        super.beginReadChain(statement);
    }

    protected void beginReadChainOutsideUOW() throws SqlException {
        // Clear here as endWriteChain may not always be called
        writeChainIsDirty_ = false;
        readDeferredResetConnection();
        super.beginReadChainOutsideUOW();
    }

    /**
     * Switches the current CCSID manager to UTF-8
     */
    public void switchToUtf8CcsidMgr() {
        currentCcsidManager_ = utf8CcsidManager_;
    }
    
    /**
     * Switches the current CCSID manager to EBCDIC
     */
    public void switchToEbcdicMgr() {
        currentCcsidManager_ = ebcdicCcsidManager_;
    }

    public String convertToStringTcpIpAddress(int tcpIpAddress) {
        StringBuffer ipAddrBytes = new StringBuffer();
        ipAddrBytes.append((tcpIpAddress >> 24) & 0xff);
        ipAddrBytes.append(".");
        ipAddrBytes.append((tcpIpAddress >> 16) & 0xff);
        ipAddrBytes.append(".");
        ipAddrBytes.append((tcpIpAddress >> 8) & 0xff);
        ipAddrBytes.append(".");
        ipAddrBytes.append((tcpIpAddress) & 0xff);

        return ipAddrBytes.toString();
    }

    protected int getPort() {
        return port_;
    }

}



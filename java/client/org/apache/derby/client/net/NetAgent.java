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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.security.AccessController;
import java.security.PrivilegedActionException;

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.ClientStatement;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.Utils;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.reference.SQLState;

public class NetAgent extends Agent {
    //---------------------navigational members-----------------------------------

    // All these request objects point to the same physical request object.
    ConnectionRequestInterface connectionRequest_;
    StatementRequestInterface statementRequest_;
    ResultSetRequestInterface resultSetRequest_;

    // All these reply objects point to the same physical reply object.
    ConnectionReply connectionReply_;
    private ConnectionReply packageReply_;
    StatementReply statementReply_;
    ResultSetReply resultSetReply_;

    //---------------------navigational cheat-links-------------------------------
    // Cheat-links are for convenience only, and are not part of the conceptual model.
    // Warning:
    //   Cheat-links should only be defined for invariant state data.
    //   That is, the state data is set by the constructor and never changes.

    // Alias for (NetConnection) super.connection
    NetConnection netConnection_;

    // Alias for (Request) super.*Request, all in one
    // In the case of the NET implementation, these all point to the same physical request object.
    private Request request_;
    NetConnectionRequest netConnectionRequest_;
    private NetPackageRequest netPackageRequest_;
    private NetStatementRequest netStatementRequest_;
    private NetResultSetRequest netResultSetRequest_;

    // Alias for (Reply) super.*Reply, all in one.
    // In the case of the NET implementation, these all point to the same physical reply object.
    private Reply reply_;
    NetConnectionReply netConnectionReply_;
    private NetPackageReply netPackageReply_;
    private NetStatementReply netStatementReply_;
    private NetResultSetReply netResultSetReply_;

    //-----------------------------state------------------------------------------

    Socket socket_;
    private InputStream rawSocketInputStream_;
    private OutputStream rawSocketOutputStream_;

    String server_;
    int port_;
    private int clientSSLMode_;

    private EbcdicCcsidManager ebcdicCcsidManager_;
    private Utf8CcsidManager utf8CcsidManager_;
    private CcsidManager currentCcsidManager_;
    
    // TODO: Remove target? Keep just one CcsidManager?
    //public CcsidManager targetCcsidManager_;
    Typdef typdef_;
    Typdef targetTypdef_;
    Typdef originalTargetTypdef_; // added to support typdef overrides

    private int svrcod_;

    int orignalTargetSqlam_ = NetConfiguration.MGRLVL_7;
    int targetSqlam_ = orignalTargetSqlam_;

    SqlException exceptionOpeningSocket_ = null;
    SqlException exceptionConvertingRdbnam = null;
    
    /**
     * Flag which indicates that a writeChain has been started and data sent to
     * the server.
     * If true, starting a new write chain will throw a DisconnectException. 
     * It is cleared when the write chain is ended.
     */
    private boolean writeChainIsDirty_ = false;
    //---------------------constructors/finalizer---------------------------------

    // Only used for testing
    public NetAgent(NetConnection netConnection,
                    LogWriter logWriter) throws SqlException {
        super(netConnection, logWriter);
        this.netConnection_ = netConnection;
    }

    NetAgent(NetConnection netConnection,
             LogWriter netLogWriter,
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
            socket_ = (Socket)AccessController.doPrivileged(
                new OpenSocketAction(server, port, clientSSLMode_));
        } catch (PrivilegedActionException e) {
            throw new DisconnectException(this,
                new ClientMessageId(SQLState.CONNECT_UNABLE_TO_CONNECT_TO_SERVER),
                e.getException(),
                e.getException().getClass().getName(), server, port,
                e.getException().getMessage());
        }

        // Set TCP/IP Socket Properties
        try {
            if (exceptionOpeningSocket_ == null) {
                socket_.setTcpNoDelay(true); // disables nagles algorithm
                socket_.setKeepAlive(true); // PROTOCOL Manual: TCP/IP connection allocation rule #2
                socket_.setSoTimeout(loginTimeout * 1000);
            }
        } catch (SocketException e) {
            try {
                socket_.close();
            } catch (IOException doNothing) {
            }
            exceptionOpeningSocket_ = new DisconnectException(this,
                new ClientMessageId(SQLState.CONNECT_SOCKET_EXCEPTION),
                e, e.getMessage());
        }

        try {
            if (exceptionOpeningSocket_ == null) {
                rawSocketOutputStream_ = socket_.getOutputStream();
                rawSocketInputStream_ = socket_.getInputStream();
            }
        } catch (IOException e) {
            try {
                socket_.close();
            } catch (IOException doNothing) {
            }
            exceptionOpeningSocket_ = new DisconnectException(this, 
                new ClientMessageId(SQLState.CONNECT_UNABLE_TO_OPEN_SOCKET_STREAM),
                e, e.getMessage());
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
            connectionRequest_ = (ConnectionRequestInterface) netConnectionRequest_;
        }
    }

    protected void resetAgent_(LogWriter netLogWriter,
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
        } catch (SocketException e) {
            try {
                socket_.close();
            } catch (IOException doNothing) {
            }
            throw new SqlException(logWriter_, 
                new ClientMessageId(SQLState.SOCKET_EXCEPTION),
                e, e.getMessage());
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

    private int getSvrcod() {
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
            } catch (IOException e) {
                // note when {6} = 0 it indicates the socket was closed.
                // this should be ok since we are going to go an close the socket
                // immediately following this call.
                // changing {4} to e.getMessage() may require pub changes
                accumulatedExceptions = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                    e, e.getMessage());
            } finally {
                rawSocketInputStream_ = null;
            }
        }

        if (rawSocketOutputStream_ != null) {
            try {
                rawSocketOutputStream_.close();
            } catch (IOException e) {
                // note when {6} = 0 it indicates the socket was closed.
                // this should be ok since we are going to go an close the socket
                // immediately following this call.
                // changing {4} to e.getMessage() may require pub changes
                SqlException latestException = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                    e, e.getMessage());
                accumulatedExceptions = Utils.accumulateSQLException(latestException, accumulatedExceptions);
            } finally {
                rawSocketOutputStream_ = null;
            }
        }

        if (socket_ != null) {
            try {
                socket_.close();
            } catch (IOException e) {
                // again {6} = 0, indicates the socket was closed.
                // maybe set {4} to e.getMessage().
                // do this for now and but may need to modify or
                // add this to the message pubs.
                SqlException latestException = new SqlException(logWriter_,
                    new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                        e, e.getMessage());
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

    private void sendRequest() throws DisconnectException {
        try {
            request_.flush(rawSocketOutputStream_);
        } catch (IOException e) {
            throwCommunicationsFailure(e);
        }
    }

    public InputStream getInputStream() {
        return rawSocketInputStream_;
    }

    public CcsidManager getCurrentCcsidManager() {
        return currentCcsidManager_;
    }
    
    public OutputStream getOutputStream() {
        return rawSocketOutputStream_;
    }

    void setInputStream(InputStream inputStream) {
        rawSocketInputStream_ = inputStream;
    }

    void setOutputStream(OutputStream outputStream) {
        rawSocketOutputStream_ = outputStream;
    }

    void throwCommunicationsFailure(Throwable cause)
        throws DisconnectException {
        //DisconnectException
        //accumulateReadExceptionAndDisconnect
        // note when {6} = 0 it indicates the socket was closed.
        // need to still validate any token values against message publications.
        accumulateChainBreakingReadExceptionAndThrow(
            new DisconnectException(this,
                new ClientMessageId(SQLState.COMMUNICATION_ERROR),
                cause, cause.getMessage()));
    }
        
    // ----------------------- call-down methods ---------------------------------

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

    public void beginWriteChain(ClientStatement statement) throws SqlException {
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

    protected void beginReadChain(ClientStatement statement)
            throws SqlException {
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
    void switchToUtf8CcsidMgr() {
        currentCcsidManager_ = utf8CcsidManager_;
    }
    
    /**
     * Switches the current CCSID manager to EBCDIC
     */
    void switchToEbcdicMgr() {
        currentCcsidManager_ = ebcdicCcsidManager_;
    }
}



/*

   Derby - Class org.apache.derby.client.net.NetAgent

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

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Utils;

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
    public CcsidManager sourceCcsidManager_;
    public CcsidManager targetCcsidManager_;
    public Typdef typdef_;
    public Typdef targetTypdef_;
    public Typdef originalTargetTypdef_; // added to support typdef overrides

    protected int svrcod_;

    public int orignalTargetSqlam_ = NetConfiguration.MGRLVL_7;
    public int targetSqlam_ = orignalTargetSqlam_;

    public SqlException exceptionOpeningSocket_ = null;

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
             int port) throws SqlException {
        super(netConnection, netLogWriter);

        server_ = server;
        port_ = port;
        netConnection_ = netConnection;
        if (server_ == null) {
            throw new DisconnectException(this, "Required property \"serverName\" not set");
        }

        try {
            socket_ = (java.net.Socket) java.security.AccessController.doPrivileged(new OpenSocketAction(server, port));
        } catch (java.security.PrivilegedActionException e) {
            throw new DisconnectException(this,
                    e.getClass().getName() + " : Error opening socket to server " + server + " on port " + port + " with message : " + e.getMessage());
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
                    "SocketException '" + e.getMessage() + "'");
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
            exceptionOpeningSocket_ = new DisconnectException(this, "unable to open stream on socket '"+e.getMessage() + "'");
        }

        sourceCcsidManager_ = new EbcdicCcsidManager(); // delete these
        targetCcsidManager_ = sourceCcsidManager_; // delete these

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
            NetXAConnectionRequest netXAConnectionRequest_ = new NetXAConnectionRequest(this, sourceCcsidManager_, netConnection_.commBufferSize_);
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
            netResultSetRequest_ = new NetResultSetRequest(this, sourceCcsidManager_, netConnection_.commBufferSize_);
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
            throw new SqlException(logWriter_, e, "SocketException '" + e.getMessage() + "'");
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
                accumulatedExceptions =
                        new SqlException(logWriter_, e, "A communication error has been detected. " +
                        "Communication protocol being used: {0}. " +
                        "Communication API being used: {1}. " +
                        "Location where the error was detected: {2}. " +
                        "Communication function detecting the error: {3}. " +
                        "Protocol specific error codes(s) {4}, {5}, {6}. " +
                        "TCP/IP " + "SOCKETS " + "Agent.close() " +
                        "InputStream.close() " + e.getMessage() + " " + "* " + "0");
                //"08001",
                //-30081);
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
                        e,
                        "A communication error has been detected. " +
                        "Communication protocol being used: {0}. " +
                        "Communication API being used: {1}. " +
                        "Location where the error was detected: {2}. " +
                        "Communication function detecting the error: {3}. " +
                        "Protocol specific error codes(s) {4}, {5}, {6}. " +
                        "TCP/IP " + "SOCKETS " + "Agent.close() " +
                        "OutputStream.close() " + e.getMessage() + " " + "* " + "0");
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
                        e,
                        "A communication error has been detected. " +
                        "Communication protocol being used: {0}. " +
                        "Communication API being used: {1}. " +
                        "Location where the error was detected: {2}. " +
                        "Communication function detecting the error: {3}. " +
                        "Protocol specific error codes(s) {4}, {5}, {6}. " +
                        "TCP/IP " + "SOCKETS " + "Agent.close() " +
                        "Socket.close() " + e.getMessage() + " " + "* " + "0");
                accumulatedExceptions = Utils.accumulateSQLException(latestException, accumulatedExceptions);
            } finally {
                socket_ = null;
            }
        }

        if (accumulatedExceptions != null) {
            throw accumulatedExceptions;
        }
    }


    protected void sendRequest() throws DisconnectException {
        try {
            request_.flush(rawSocketOutputStream_);
        } catch (java.io.IOException e) {
            throwCommunicationsFailure("NetAgent.sendRequest()",
                    "OutputStream.flush()",
                    e.getMessage(),
                    "*");
        }
    }

    public java.io.InputStream getInputStream() {
        return rawSocketInputStream_;
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

    public void throwCommunicationsFailure(String location,
                                           String function,
                                           String rc1,
                                           String rc2) throws org.apache.derby.client.am.DisconnectException {
        //org.apache.derby.client.am.DisconnectException
        //accumulateReadExceptionAndDisconnect
        // note when {6} = 0 it indicates the socket was closed.
        // need to still validate any token values against message publications.
        accumulateChainBreakingReadExceptionAndThrow(new org.apache.derby.client.am.DisconnectException(this,
                "A communication error has been detected. " +
                "Communication protocol being used: " + location + ". " +
                "Communication API being used: " + function + ". " +
                "Location where the error was detected: " + rc1 + ". " +
                "Communication function detecting the error: " + rc2 + ". " +
                "Protocol specific error codes(s) " +
                "TCP/IP SOCKETS "));  // hardcode tokens 0 and 1
        //"08001"));  //derby code -30081, don't send 08001 now either
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
            DisconnectException de = new DisconnectException(this, "An error occurred during a deferred connect reset and the connection has been terminated.  See chained exceptions for details.");
            de.setNextException(sqle);
            throw de;
        }
    }

    public void beginWriteChainOutsideUOW() throws SqlException {
        request_.initialize();
        writeDeferredResetConnection();
        super.beginWriteChainOutsideUOW();
    }

    public void beginWriteChain(org.apache.derby.client.am.Statement statement) throws SqlException {
        request_.initialize();
        writeDeferredResetConnection();
        super.beginWriteChain(statement);
    }

    protected void endWriteChain() {
        super.endWriteChain();
    }

    private void readDeferredResetConnection() throws SqlException {
        if (!netConnection_.resetConnectionAtFirstSql_) {
            return;
        }
        try {
            netConnection_.readDeferredReset();
            checkForExceptions();
        } catch (SqlException sqle) {
            DisconnectException de = new DisconnectException(this, "An error occurred during a deferred connect reset and the connection has been terminated.  See chained exceptions for details.");
            de.setNextException(sqle);
            throw de;
        }
    }

    protected void beginReadChain(org.apache.derby.client.am.Statement statement) throws SqlException {
        readDeferredResetConnection();
        super.beginReadChain(statement);
    }

    protected void beginReadChainOutsideUOW() throws SqlException {
        readDeferredResetConnection();
        super.beginReadChainOutsideUOW();
    }

    public void endReadChain() throws SqlException {
        super.endReadChain();
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



/*
 
   Derby - Class org.apache.derby.impl.store.replication.net.ReplicationMessageTransmit
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */
package org.apache.derby.impl.store.replication.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.net.SocketFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;

/**
 * Used to send replication messages to the slave. Called by the
 * Master controller to transmit replication messages wrapped in
 * a <code>ReplicationMessage</code> object to a receiver. The
 * receiver is implemented by the <code>ReplicationMessageReceive</code>
 * class.
 */
public class ReplicationMessageTransmit {
    
    /** Number of millis to wait for a response message before timing out
     */
    private final int DEFAULT_MESSAGE_RESPONSE_TIMEOUT = 30000;

    /** Used to synchronize when waiting for a response message from the slave
     */
    private final Object receiveSemaphore = new Object();

    /** The message received from the slave as a response to sending a
     * message. */
    private ReplicationMessage receivedMsg = null;

    /** Whether or not to keep the message receiver thread alive. Set to true
     * to terminate the thread */
    private volatile boolean stopMessageReceiver = false;

    /**
     * Contains the address (hostname and port number) of the slave
     * to replicate to.
     */
    private final SlaveAddress slaveAddress;
    
    /**
     * Used to write/read message objects to/from a connection.
     */
    private SocketConnection socketConn;

    /**
     * The name of the replicated database
     */
    private String dbname;
    
    /**
     * Constructor initializes the slave address used in replication.
     *
     * @param slaveAddress contains the address (host name and port number)
     *                     of the slave to connect to.
     */
    public ReplicationMessageTransmit(SlaveAddress slaveAddress) {
        this.slaveAddress = slaveAddress;
    }
    
    /**
     * Used to create a <code>Socket</code> connection to the slave and
     * establish compatibility with the database version of the slave by
     * comparing the UID's of the <code>ReplicationMessage</code> classes
     * of the master and the slave.
     *
     * @param timeout the amount of time for which the connection should
     *                block before being established.
     *
     * @param synchOnInstant the master log instant, used to check
     * that the master and slave log files are in synch. If no chunks
     * of log records have been shipped to the slave yet, this is the
     * end position in the current log file. If a chunk of log has
     * been shipped, this is the instant of the log record shipped
     * last. Note that there is a difference!
     * @throws PrivilegedActionException if an exception occurs while trying
     *                                   to open a connection.
     *
     * @throws IOException if an exception occurs while trying to create the
     *         <code>SocketConnection</code> class.
     *
     * @throws StandardException If an error message is received from the
     *         server indicating incompatible software versions of master
     *         and slave.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *         be found.
     */
    public void initConnection(int timeout, long synchOnInstant) throws
        PrivilegedActionException,
        IOException,
        StandardException,
        ClassNotFoundException {
        
        Socket s = null;
        
        final int timeout_ = timeout;
        
        //create a connection to the slave.
        s = (Socket)
        AccessController.doPrivileged(new PrivilegedExceptionAction() {
            public Object run() throws IOException {
                SocketFactory sf = SocketFactory.getDefault();
                InetSocketAddress sockAddr = new InetSocketAddress(
                        slaveAddress.getHostAddress(), 
                        slaveAddress.getPortNumber());
                Socket s_temp = sf.createSocket();
                s_temp.connect(sockAddr, timeout_);
                return s_temp;
            }
        });
        
        // keep socket alive even if no log is shipped for a long time
        s.setKeepAlive(true);
        
        socketConn = new SocketConnection(s);

        // Start the thread that will listen for incoming messages.
        startMessageReceiverThread(dbname);
        
        // Verify that the master and slave have the same software version
        // and exactly equal log files.
        brokerConnection(synchOnInstant);
    }
    
    /**
     * Tear down the network connection established with the
     * other replication peer
     *
     * @throws IOException if an exception occurs while trying to tear
     *                     down the network connection
     */
    public void tearDown() throws IOException {
        stopMessageReceiver = true;
        if(socketConn != null) {
            socketConn.tearDown();
            socketConn = null;
        }
    }

    /**
     * Used to send a replication message to the slave.
     *
     * @param message a <code>ReplicationMessage</code> object that contains
     *                the message to be transmitted.
     *
     * @throws IOException 1) if an exception occurs while transmitting
     *                        the message.
     *                     2) if the connection handle is invalid.
     */
    public void sendMessage(ReplicationMessage message) throws IOException {
        checkSocketConnection();
        socketConn.writeMessage(message);
    }
    
    /**
     * Send a replication message to the slave and return the
     * message received as a response. Will only wait
     * DEFAULT_MESSAGE_RESPONSE_TIMEOUT millis for the response
     * message. If not received when the wait times out, no message is
     * returned. The method is synchronized to guarantee that only one
     * thread will be waiting for a response message at any time.
     *
     * @param message a ReplicationMessage object that contains the message to
     * be transmitted.
     *
     * @return the response message
     * @throws IOException 1) if an exception occurs while sending or receiving
     *                        a message.
     *                     2) if the connection handle is invalid.
     * @throws StandardException if the response message has not been received
     * after DEFAULT_MESSAGE_RESPONSE_TIMEOUT millis
     */
    public synchronized ReplicationMessage
        sendMessageWaitForReply(ReplicationMessage message)
        throws IOException, StandardException {
        receivedMsg = null;
        checkSocketConnection();
        socketConn.writeMessage(message);
        synchronized (receiveSemaphore) {
            try {
                receiveSemaphore.wait(DEFAULT_MESSAGE_RESPONSE_TIMEOUT);
            } catch (InterruptedException ie) {
            }
        }
        if (receivedMsg == null) {
            throw StandardException.
                newException(SQLState.REPLICATION_CONNECTION_LOST, dbname);

        }
        return receivedMsg;
    }
    
    /**
     * Used to send initiator messages to the slave and receive
     * information about the compatibility of the slave with the
     * master. One message is used to check that the slave and master
     * have the same software versions. A second message is used to
     * check that the master and slave log files are in synch.
     *
     * @param synchOnInstant the master log instant, used to check
     * that the master and slave log files are in synch. If no chunks
     * of log records have been shipped to the slave yet, this is the
     * end position in the current log file. If a chunk of log has
     * been shipped, this is the instant of the log record shipped
     * last. Note that there is a difference!
     *
     * @throws IOException if an exception occurs during the sending or
     *                     reading of the message.
     *
     * @throws StandardException If an error message is received from the
     *                           server indicating a mis-match in
     *                           serialVersionUID or log files out of synch.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     */
    private void brokerConnection(long synchOnInstant)
        throws IOException, StandardException, ClassNotFoundException {
        // Check that master and slave have the same serialVersionUID
        ReplicationMessage initiatorMsg = 
            new ReplicationMessage(ReplicationMessage.TYPE_INITIATE_VERSION, 
                                   new Long(ReplicationMessage.
                                            serialVersionUID));
        verifyMessageType(sendMessageWaitForReply(initiatorMsg),
                          ReplicationMessage.TYPE_ACK);

        // Check that master and slave log files are in synch
        initiatorMsg =
            new ReplicationMessage(ReplicationMessage.TYPE_INITIATE_INSTANT,
                                   new Long(synchOnInstant));
        verifyMessageType(sendMessageWaitForReply(initiatorMsg),
                          ReplicationMessage.TYPE_ACK);
    }

    /**
     * Used to parse a message received from the slave. If the message
     * is an ack of the last shipped message, this method terminates
     * quietly. Otherwise, it throws the exception received in the
     * message from the slave describing why the last message could
     * not be acked.
     *
     * @throws StandardException If an error message is received from
     *                           the server
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     */
    private boolean verifyMessageType(ReplicationMessage message,
                                      int expectedType)
        throws StandardException {
        //If the message is a TYPE_ACK the slave is capable
        //of handling the messages and is at a compatible database version.
        if (message.getType() == expectedType) {
            return true;
        } else if (message.getType() == ReplicationMessage.TYPE_ERROR) {
            // See ReplicationMessage#TYPE_ERROR
            String exception[] = (String[])message.getMessage();
            throw StandardException.
                newException(exception[exception.length - 1], exception);
        } else {
            //The message format was not recognized. Hence throw
            //an unexpected exception.
            throw StandardException.newException
                (SQLState.REPLICATION_UNEXPECTED_EXCEPTION);
        }
    }
    
    /**
     * Verifies if the <code>SocketConnection</code> is valid.
     *
     * @throws IOException If the socket connection object is not
     *                     valid (is null).
     */
    private void checkSocketConnection() throws IOException {
        if (socketConn == null) {
            throw new IOException
                    (MessageId.REPLICATION_INVALID_CONNECTION_HANDLE);
        }
    }

    private void startMessageReceiverThread(String dbname) {
        MasterReceiverThread msgReceiver = new MasterReceiverThread(dbname);
        msgReceiver.setDaemon(true);
        msgReceiver.start();
    }

    /////////////////
    // Inner Class //
    /////////////////

    /**
     * Thread that listens for messages from the slave. A separate thread
     * listening for messages from the slave is needed because the slave
     * may send messages to the master at any time, and these messages require
     * immediate action.
     */
    private class MasterReceiverThread extends Thread {

        private final ReplicationMessage pongMsg =
            new ReplicationMessage(ReplicationMessage.TYPE_PONG, null);

        MasterReceiverThread(String dbname) {
            super("derby.master.receiver-" + dbname);
        }

        public void run() {
            ReplicationMessage message;
            while (!stopMessageReceiver) {
                try {
                    message = readMessage();

                    switch (message.getType()) {
                    case ReplicationMessage.TYPE_PING:
                        sendMessage(pongMsg);
                        break;
                    case ReplicationMessage.TYPE_ACK:
                    case ReplicationMessage.TYPE_ERROR:
                        synchronized (receiveSemaphore) {
                            receivedMsg = message;
                            receiveSemaphore.notify();
                        }
                        break;
                    default:
                        // Handling of other messages (i.e., stop and failover)
                        // not implemented yet
                        break;
                    }
                } catch (SocketTimeoutException ste) {
                    // ignore socket timeout on reads
                } catch (ClassNotFoundException cnfe) {
                    // TODO: print problem to log
                } catch (IOException ex) {
                    // TODO: print problem to log
                    // If we get an exception for this socket, the log shipper
                    // will clean up. Stop this thread.
                    stopMessageReceiver = true;
                }
            }
        }

        /**
         * Used to read a replication message sent by the slave. Hangs until a
         * message is received from the slave
         *
         * @return the reply message.
         *
         * @throws ClassNotFoundException Class of a serialized object cannot
         *                                be found.
         *
         * @throws IOException 1) if an exception occurs while reading from the
         *                        stream.
         *                     2) if the connection handle is invalid.
         */
        private ReplicationMessage readMessage() throws
            ClassNotFoundException, IOException {
            checkSocketConnection();
            return (ReplicationMessage)socketConn.readMessage();
        }
    }
}


/*
 
   Derby - Class org.apache.derby.impl.store.replication.net.ReplicationMessageReceive
 
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
import java.net.ServerSocket;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.net.ServerSocketFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.iapi.util.InterruptStatus;

/**
 * This class is the Receiver (viz. Socket server or listener) part of the
 * network communication. It receives the message from the master and
 * performs appropriate action depending on the type of the message.
 */
public class ReplicationMessageReceive {
    /**
     * Contains the address (hostname and port number) of the slave
     * to replicate to.
     */
    private final SlaveAddress slaveAddress;
    
    /**
     * Contains the <code>ServerSocket</code> used to listen for
     * connections from the replication master. */
    private ServerSocket serverSocket;

    /**
     * Contains the methods used to read and write to the Object streams
     * obtained from a <code>Socket</code> connection.
     */
    private SocketConnection socketConn;
    
    /* -- Ping-thread related fields start -- */

    /** The maximum number of millis to wait before giving up waiting for
     * a ping response*/
    private static final int DEFAULT_PING_TIMEOUT = 5000; // 5 seconds

    /** Thread used to send ping messages to master to check if the connection
     * is working. The ping message must be sent from a separate thread
     * because failed message shipping over TCP does not timeout for two
     * minutes (not configurable). */
    private Thread pingThread = null;

    /** Used to terminate the ping thread. */
    private boolean killPingThread = false;

    /** Whether or not the connection with the master is confirmed to be
     * working. Set to false by isConnectedToMaster, set to true when
     * a pong (i.e., a response to a ping) is received. Field protected by
     * receivePongSemephore */
    private boolean connectionConfirmed = false;

    /** Used for synchronization of the ping thread */
    private final Object sendPingSemaphore = new Object();

    /**
     * Whether or not the ping thread has been notified to check connection.
     * Protected by sendPingSemaphore.
     */
    private boolean doSendPing = false;

    /** Used for synchronization when waiting for a ping reply message */
    private final Object receivePongSemaphore = new Object();

    /* -- Ping-thread related fields stop -- */

    /**
     * Constructor initializes the slave address used in replication. Accepts
     * the host name and port number that constitute the slave address as
     * parameters.
     *
     * @param slaveAddress the address (host name and port number) of the slave
     *                     to connect to.
     * @param dbname the name of the database.
     */
    public ReplicationMessageReceive(SlaveAddress slaveAddress, 
                                     String dbname) {
        this.slaveAddress = slaveAddress;
        Monitor.logTextMessage(MessageId.REPLICATION_SLAVE_NETWORK_LISTEN,
                               dbname, 
                               slaveAddress.getHostAddress().getHostName(),
                               String.valueOf(slaveAddress.getPortNumber()));
    }
    
    /**
     * Used to create the server socket, listen on the socket
     * for connections from the master and verify compatibility
     * with the database version of the master.
     *
     * @param timeout The amount of time, in milliseconds, this method
     * will wait for a connection to be established. If no connection
     * has been established before the timeout, a
     * IOException is raised with cause
     * java.net.SocketTimeoutException
     * @param synchOnInstant the slave log instant, used to check that
     * the master and slave log files are in synch. If no chunks of log
     * records have been received from the master yet, this is the
     * end position in the current log file. If a chunk of log has been
     * received, this is the instant of the log record received last.
     * Note that there is a difference!
     * @param dbname the name of the replicated database
     *
     *
     *
     * @throws IOException if an exception occurs while trying to create the
     *                     <code>SocketConnection</code> class or while
     *                     trying to open a connection.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     * @throws StandardException if an incompatible database version is found.
     *
     */
    public void initConnection(int timeout, long synchOnInstant, String dbname)
        throws
        IOException,
        StandardException,
        ClassNotFoundException {
        
        // Create the ServerSocket object if this is the first
        // initConnection attempt. Otherwise, we reuse the existing
        // server socket
        if (serverSocket == null) {
            serverSocket = createServerSocket();
        }
        serverSocket.setSoTimeout(timeout);
        Socket client = null;
        try {
            //Start listening on the socket and accepting the connection
            client =
                AccessController.doPrivileged(new PrivilegedExceptionAction<Socket>() {
                    public Socket run() throws IOException {
                        return serverSocket.accept();
                    }
                });
        } catch(PrivilegedActionException pea) {
            throw (IOException) pea.getException();
        }

        //create the SocketConnection object using the client connection.
        socketConn = new SocketConnection(client);
        
        // exchange initiator messages to check that master and slave are at 
        // the same version...
        parseAndAckVersion(readMessage(), dbname);
        // ...and have equal log files
        parseAndAckInstant(readMessage(), synchOnInstant, dbname);

        killPingThread = false;
        pingThread = new SlavePingThread(dbname);
        pingThread.setDaemon(true);
        pingThread.start();

    }
    
    /**
     * Used to create a <code>ServerSocket</code> for listening to connections
     * from the master.
     *
     * @return an instance of the <code>ServerSocket</code> class.
     *
     * @throws IOException if an exception occurs while trying
     *                                   to open a connection.
     */
    private ServerSocket createServerSocket() throws IOException {
        //create a ServerSocket at the specified host name and the
        //port number.
        ServerSocket ss = null;
        try { 
            ss = AccessController.doPrivileged
            (new PrivilegedExceptionAction<ServerSocket>() {
                public ServerSocket run() throws IOException  {
                    ServerSocketFactory sf = ServerSocketFactory.getDefault();
                    return sf.createServerSocket(slaveAddress.getPortNumber(),
                            0, slaveAddress.getHostAddress());
                }
            });
            return ss;
        } catch(PrivilegedActionException pea) {
            throw (IOException) pea.getException();
        }
    }
    
    /**
     * Used to close the <code>ServerSocket</code> and the resources
     * associated with it.
     *
     * @throws IOException If an exception occurs while trying to
     *                     close the socket or the associated resources.
     */
    public void tearDown() throws IOException {
        synchronized (sendPingSemaphore) {
            killPingThread = true;
            sendPingSemaphore.notify();
        }

        // socketConn.tearDown() may fail if the master has crashed. We still
        // want to close the server socket if an exception is thrown, so that
        // we don't prevent starting a new slave listening to the same port.
        // Therefore, use try/finally. DERBY-3878
        try {
            if (socketConn != null) {
                socketConn.tearDown();
            }
        } finally {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }
    
    /**
     * Used to parse the initiator message from the master and check if the
     * slave is compatible with the master by comparing the UID of the 
     * <code>ReplicationMessage</code> class of the master, that is wrapped
     * in the initiator message, with the UID of the same class in the slave.
     *
     * @param initiatorMessage the object containing the UID.
     * @param dbname the name of the replicated database
     *
     * @throws IOException If an exception occurs while sending the
     *                     acknowledgment.
     *
     * @throws StandardException If the UID's do not match.
     */
    private void parseAndAckVersion(ReplicationMessage initiatorMessage, 
                                    String dbname)
        throws IOException, StandardException {
        //Holds the replication message that will be sent
        //to the master.
        ReplicationMessage ack = null;

        //Check if this message is an initiate version message, if not
        //throw an exception
        if (initiatorMessage.getType() != 
                ReplicationMessage.TYPE_INITIATE_VERSION) {
            // The message format was not recognized. Notify master and throw
            // an exception
            String expectedMsgId = String.
                valueOf(ReplicationMessage.TYPE_INITIATE_VERSION);
            String receivedMsgId = String.valueOf(initiatorMessage.getType());
            handleUnexpectedMessage(dbname, expectedMsgId, receivedMsgId);
        }

        //Get the UID of the master
        long masterVersion = ((Long)initiatorMessage.getMessage()).longValue();
        //If the UID's are equal send the acknowledgment message
        if (masterVersion == ReplicationMessage.serialVersionUID) {
            ack = new ReplicationMessage
                (ReplicationMessage.TYPE_ACK, "UID OK");
            sendMessage(ack);
        } else {
            //If the UID's are not equal send an error message. The
            //object of a TYPE_ERROR message must be a String[]
            ack = new ReplicationMessage
                (ReplicationMessage.TYPE_ERROR,
                 new String[]{SQLState.
                              REPLICATION_MASTER_SLAVE_VERSION_MISMATCH});
            sendMessage(ack);

            //The UID's do not match.
            throw StandardException.newException
                (SQLState.REPLICATION_MASTER_SLAVE_VERSION_MISMATCH);
        }
    }

    /**
     * Used to parse the log instant initiator message from the master and 
     * check that the master and slave log files are in synch.
     *
     * @param initiatorMessage the object containing the UID.
     * @param synchOnInstant the slave log instant, used to check that
     * the master and slave log files are in synch. If no chunks of log
     * records have been received from the master yet, this is the
     * end position in the current log file. If a chunk of log has been
     * received, this is the instant of the log record received last.
     * Note that there is a difference!
     * @param dbname the name of the replicated database
     *
     * @throws IOException If an exception occurs while sending the
     *                     acknowledgment.
     *
     * @throws StandardException If the log files are not in synch
     */
    private void parseAndAckInstant(ReplicationMessage initiatorMessage,
                                    long synchOnInstant, String dbname)
        throws IOException, StandardException {
        ReplicationMessage ack = null;

        //Check if this message is a log synch message, if not throw
        //an exception
        if (initiatorMessage.getType() !=
            ReplicationMessage.TYPE_INITIATE_INSTANT) {
            // The message format was not recognized. Notify master and throw 
            // an exception
            String expectedMsgId = String.
                valueOf(ReplicationMessage.TYPE_INITIATE_INSTANT);
            String receivedMsgId = String.valueOf(initiatorMessage.getType());
            handleUnexpectedMessage(dbname, expectedMsgId, receivedMsgId);
        }

        // Get the log instant of the master
        long masterInstant = ((Long)initiatorMessage.getMessage()).longValue();

        if (masterInstant == synchOnInstant) {
            // Notify the master that the logs are in synch
            ack = new ReplicationMessage
                (ReplicationMessage.TYPE_ACK, "Instant OK");
            sendMessage(ack);
        } else {
            // Notify master that the logs are out of synch
            // See ReplicationMessage#TYPE_ERROR
            String[] exception = new String[6];
            exception[0] = dbname;
            exception[1] = String.valueOf(LogCounter.
                                          getLogFileNumber(masterInstant));
            exception[2] = String.valueOf(LogCounter.
                                          getLogFilePosition(masterInstant));
            exception[3] = String.valueOf(LogCounter.
                                          getLogFileNumber(synchOnInstant));
            exception[4] = String.valueOf(LogCounter.
                                          getLogFilePosition(synchOnInstant));
            exception[5] = SQLState.REPLICATION_LOG_OUT_OF_SYNCH;
            ack = new ReplicationMessage(ReplicationMessage.TYPE_ERROR, 
                                         exception);
            sendMessage(ack);

            throw StandardException.
                newException(SQLState.REPLICATION_LOG_OUT_OF_SYNCH, exception);
        }
    }
    
    /**
     * Notify other replication peer that the message type was unexpected and 
     * throw a StandardException
     *
     * @param dbname the name of the replicated database
     * @param expextedMsgId the expected message type
     * @param receivedMsgId the received message type
     *
     * @throws StandardException exception describing that an unexpected
     * message was received is always thrown 
     * @throws java.io.IOException thrown if an exception occurs while sending
     * the error message 
     */
    private void handleUnexpectedMessage(String dbname, 
                                         String expextedMsgId,
                                         String receivedMsgId)
        throws StandardException, IOException {
        String[] exception = new String[4];
        exception[0] = dbname;
        exception[1] = expextedMsgId;
        exception[2] = receivedMsgId;
        exception[3] = SQLState.REPLICATION_UNEXPECTED_MESSAGEID;

        ReplicationMessage ack = 
            new ReplicationMessage(ReplicationMessage.TYPE_ERROR, exception);

        sendMessage(ack);

        throw StandardException.
            newException(SQLState.REPLICATION_UNEXPECTED_MESSAGEID, exception);

    }

    /**
     * Used to send a replication message to the master.
     *
     * @param message a <code>ReplicationMessage</code> object that contains
     *                the message to be transmitted.
     *
     * @throws IOException 1) if an exception occurs while transmitting
     *                        the message,
     *                     2) if the connection handle is invalid.
     */
    public void sendMessage(ReplicationMessage message) throws IOException {
        checkSocketConnection();
        socketConn.writeMessage(message);
    }
    
    /**
     * Used to read a replication message sent by the master. This method
     * would wait on the connection from the master until a message is received
     * or a connection failure occurs. Replication network layer specific
     * messages (i.e. ping/pong messages) are handled internally and are not
     * returned.
     *
     * @return a <code>ReplicationMessage</code> object that contains
     *         the reply that is sent.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     *
     * @throws IOException 1) if an exception occurs while reading from the
     *                        stream,
     *                     2) if the connection handle is invalid.
     */
    public ReplicationMessage readMessage() throws
        ClassNotFoundException, IOException {
        checkSocketConnection();
        ReplicationMessage msg = (ReplicationMessage)socketConn.readMessage();

        if (msg.getType() == ReplicationMessage.TYPE_PONG) {
            // If a pong is received, connection is confirmed to be working.
            synchronized (receivePongSemaphore) {
                connectionConfirmed = true;
                receivePongSemaphore.notify();
            }
            // Pong messages are network layer specific. Do not return these
            return readMessage();
        } else {
            return msg;
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

    /**
     * Check if the repliation network is working. Tries to send a ping
     * message to the master and returns the network status based on the
     * success or failure of sending this message and receiving a pong reply.
     * MT: Currently, only one thread is allowed to check the network status at
     * any time to keep the code complexity down.
     * @return true if the pong message was received before timing out after
     * DEFAULT_PING_TIMEOUT millis, false otherwise
     * @see #DEFAULT_PING_TIMEOUT
     */
    public synchronized boolean isConnectedToMaster() {
        // synchronize on receivePongSemaphore so that this thread is
        // guaraneed to get to receivePongSemaphore.wait before the pong
        // message is processed in readMessage

        synchronized (receivePongSemaphore) {
            connectionConfirmed = false;

            long startWaitingatTime;
            long giveupWaitingAtTime;
            long nextWait = DEFAULT_PING_TIMEOUT;

            synchronized (sendPingSemaphore) {
                // Make ping thread send a ping message to the master
                doSendPing = true;
                sendPingSemaphore.notify();

                // want result within DEFAULT_PING_TIMEOUT millis.
                startWaitingatTime = System.currentTimeMillis();
                giveupWaitingAtTime = startWaitingatTime + DEFAULT_PING_TIMEOUT;
            }

            while (true) {
                try {
                    // Wait for the pong response message
                    receivePongSemaphore.wait(nextWait);
                } catch (InterruptedException ex) {
                    InterruptStatus.setInterrupted();
                }

                nextWait = giveupWaitingAtTime - System.currentTimeMillis();

                if (!connectionConfirmed && nextWait > 0) {
                    // we could have been interrupted or seen a spurious
                    // wakeup, so wait a bit longer
                    continue;
                }
                break;
            }
        }
        return connectionConfirmed;
    }

    /////////////////
    // Inner Class //
    /////////////////
    /**
     * Thread that sends ping messages to the master on request to check if the
     * replication network is working
     */
    private class SlavePingThread extends Thread {

        private final ReplicationMessage pingMsg =
            new ReplicationMessage(ReplicationMessage.TYPE_PING, null);

        SlavePingThread(String dbname) {
            super("derby.slave.ping-" + dbname);
        }

        public void run() {
            try {
                while (!killPingThread) {
                    synchronized (sendPingSemaphore) {
                        while (!doSendPing) {
                            try {
                                sendPingSemaphore.wait();
                            } catch (InterruptedException e) {
                                InterruptStatus.setInterrupted();
                            }
                        }

                        doSendPing = false;
                    }

                    if (killPingThread) {
                        // The thread was notified to terminate
                        break;
                    }

                    sendMessage(pingMsg);
                }
            } catch (IOException ioe) {
            // For both exceptions: Do nothing. isConnectedToMaster will return
            // 'false' and appropriate action will be taken.
            }
        }
    }
}

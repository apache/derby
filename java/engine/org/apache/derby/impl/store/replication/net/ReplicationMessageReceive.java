
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
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.net.ServerSocketFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.impl.store.raw.log.LogCounter;

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
    
    /**
     * Constructor initializes the slave address used in replication. Accepts
     * the host name and port number that constitute the slave address as
     * parameters.
     *
     * @param hostName a <code>String</code> that contains the host name of
     *                 the slave to replicate to.
     * @param portNumber an integer that contains the port number of the
     *                   slave to replicate to.
     * @param dbname the name of the database
     *
     * @throws StandardException If an exception occurs while trying to
     *                           resolve the host name.
     */
    public ReplicationMessageReceive(String hostName, int portNumber, 
                                     String dbname)
        throws StandardException {
        try {
            slaveAddress = new SlaveAddress(hostName, portNumber);
            Monitor.logTextMessage(MessageId.REPLICATION_SLAVE_NETWORK_LISTEN, 
                                   dbname, getHostName(), 
                                   String.valueOf(getPort()));
        } catch (UnknownHostException uhe) {
            // cannot use getPort because SlaveAddress creator threw
            // exception and has therefore not been initialized
            String port;
            if (portNumber > 0) {
                port = String.valueOf(portNumber);
            } else {
                port = String.valueOf(SlaveAddress.DEFAULT_PORT_NO);
            }
            throw StandardException.newException
                (SQLState.REPLICATION_CONNECTION_EXCEPTION, uhe, 
                 dbname, hostName, port);
        }
    }
    
    /**
     * Used to create the server socket, listen on the socket
     * for connections from the master and verify compatibility
     * with the database version of the master.
     *
     * @param timeout The amount of time, in milliseconds, this method
     * will wait for a connection to be established. If no connection
     * has been established before the timeout, a
     * PrivilegedExceptionAction is raised with cause
     * java.net.SocketTimeoutException
     * @param synchOnInstant the slave log instant, used to check that
     * the master and slave log files are in synch. If no chunks of log
     * records have been received from the master yet, this is the
     * end position in the current log file. If a chunk of log has been
     * received, this is the instant of the log record received last.
     * Note that there is a difference!
     * @param dbname the name of the replicated database
     *
     * @throws PrivilegedActionException if an exception occurs while trying
     *                                   to open a connection.
     *
     * @throws IOException if an exception occurs while trying to create the
     *                     <code>SocketConnection</code> class.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot
     *                                be found.
     * @throws StandardException if an incompatible database version is found.
     *
     */
    public void initConnection(int timeout, long synchOnInstant, String dbname)
        throws
        PrivilegedActionException,
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
        
        //Start listening on the socket and accepting the connection
        Socket client =
            (Socket)
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
            public Object run() throws IOException {
                return serverSocket.accept();
            }
        });
        
        //create the SocketConnection object using the client connection.
        socketConn = new SocketConnection(client);
        
        // exchange initiator messages to check that master and slave are at 
        // the same version...
        parseAndAckVersion(readMessage(), dbname);
        // ...and have equal log files
        parseAndAckInstant(readMessage(), synchOnInstant, dbname);
    }
    
    /**
     * Used to create a <code>ServerSocket</code> for listening to connections
     * from the master.
     *
     * @return an instance of the <code>ServerSocket</code> class.
     *
     * @throws PrivilegedActionException if an exception occurs while trying
     *                                   to open a connection.
     */
    private ServerSocket createServerSocket() throws PrivilegedActionException {
        //create a ServerSocket at the specified host name and the
        //port number.
        return   (ServerSocket) AccessController.doPrivileged
            (new PrivilegedExceptionAction() {
            public Object run() throws IOException, StandardException {
                ServerSocketFactory sf = ServerSocketFactory.getDefault();
                return sf.createServerSocket(slaveAddress.getPortNumber(),
                    0, slaveAddress.getHostAddress());
            }
        });
    }
    
    /**
     * Used to close the <code>ServerSocket</code> and the resources
     * associated with it.
     *
     * @throws IOException If an exception occurs while trying to
     *                     close the socket or the associated resources.
     */
    public void tearDown() throws IOException {
        if (socketConn != null) {
            socketConn.tearDown();
        }
        if (serverSocket != null) {
            serverSocket.close();
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
     * or a connection failure occurs.
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
        return (ReplicationMessage)socketConn.readMessage();
    }

    /**
     * Used to get the host name the slave listens for master
     * connections on
     *
     * @return the host name 
     */
    public String getHostName() {
        return slaveAddress.getHostAddress().getHostName();
     }

    /**
     * Used to get the port number the slave listens for master
     * connections on
     *
     * @return the port number
     */
    public int getPort() {
        return slaveAddress.getPortNumber();
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
}

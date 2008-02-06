
/*
 
   Derby - Class org.apache.derby.impl.services.replication.net.ReplicationMessageReceive
 
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

package org.apache.derby.impl.services.replication.net;

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
    public void initConnection(int timeout) throws
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
        
        //wait for the initiator message on the SocketConnection
        ReplicationMessage initMesg = readMessage();
        
        //Check if this message is an initiator message, if not
        //throw an exception
        if (initMesg.getType() != ReplicationMessage.TYPE_INITIATE) {
            //The message format was not recognized. Hence throw
            //an unexpected exception.
            throw StandardException.newException
                (SQLState.REPLICATION_UNEXPECTED_EXCEPTION);
        }
        
        //parse the initiator message and perform appropriate action
        parseInitiatorMessage(initMesg);
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
     *
     * @throws IOException If an exception occurs while sending the
     *                     acknowledgment.
     *
     * @throws StandardException If the UID's do not match.
     */
    private void parseInitiatorMessage(ReplicationMessage initiatorMessage)
        throws IOException, StandardException {
        //Holds the replication message that will be sent
        //to the master.
        ReplicationMessage ack = null;
        //Get the UID of the master
        long masterVersion = ((Long)initiatorMessage.getMessage()).longValue();
        //If the UID's are equal send the acknowledgment message
        if (masterVersion == ReplicationMessage.serialVersionUID) {
            ack = new ReplicationMessage
                (ReplicationMessage.TYPE_ACK, "UID OK");
            socketConn.writeMessage(ack);
        } else {
            //If the UID's are not equal send an error message
            ack = new ReplicationMessage
                (ReplicationMessage.TYPE_ERROR,
                SQLState.REPLICATION_MASTER_SLAVE_VERSION_MISMATCH);
            
            //The UID's do not match.
            throw StandardException.newException
                (SQLState.REPLICATION_MASTER_SLAVE_VERSION_MISMATCH);
        }
    }
    
    /**
     * Used to send a replication message to the master.
     *
     * @param message a <code>ReplicationMessage</code> object that contains
     *                the message to be transmitted.
     *
     * @throws IOException if an exception occurs while transmitting
     *                     the message.
     */
    public void sendMessage(ReplicationMessage message) throws IOException {
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
     * @throws IOException if an exception occurs while reading from the
     *                     stream.
     */
    public ReplicationMessage readMessage() throws
        ClassNotFoundException, IOException {
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
}

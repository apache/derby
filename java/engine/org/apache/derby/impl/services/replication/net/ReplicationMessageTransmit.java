/*
 
   Derby - Class org.apache.derby.impl.services.replication.net.ReplicationMessageTransmit
 
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.net.SocketFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Used to send replication messages to the slave. Called by the
 * Master controller to transmit replication messages wrapped in
 * a <code>ReplicationMessage</code> object to a receiver. The
 * receiver is implemented by the <code>ReplicationMessageReceive</code>
 * class.
 */
public class ReplicationMessageTransmit {
    
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
     * Constructor initializes the slave address used in replication.
     *
     * @param hostName a <code>String</code> that contains the host name of
     *                 the slave to replicate to.
     * @param portNumber an integer that contains the port number of the
     *                   slave to replicate to.
     *
     * @throws UnknownHostException If an exception occurs while trying to
     *                              resolve the host name.
     */
    public ReplicationMessageTransmit(String hostName, int portNumber) 
    throws UnknownHostException {
        slaveAddress = new SlaveAddress(hostName, portNumber);
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
        
        //The reads on the InputStreams obtained from the socket on the
        //transmitter should not hang indefinitely. Use the timeout
        //used for the connection establishment here to ensure that the
        //reads timeout after the timeout period mentioned for the
        //connection.
        s.setSoTimeout(timeout_);
        
        socketConn = new SocketConnection(s);
        
        //send the initiate message and receive acknowledgment
        sendInitiatorAndReceiveAck(synchOnInstant);
    }
    
    /**
     * Tear down the network connection established with the
     * other replication peer
     *
     * @throws IOException if an exception occurs while trying to tear
     *                     down the network connection
     */
    public void tearDown() throws IOException {
        socketConn.tearDown();
    }

    /**
     * Used to send a replication message to the slave.
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
     * Used to read a replication message sent by the slave. This method
     * would wait on the connection from the slave until a message is received
     * or a connection failure occurs.
     *
     * @return the reply message.
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
    private void sendInitiatorAndReceiveAck(long synchOnInstant)
        throws IOException, StandardException, ClassNotFoundException {
        // Check that master and slave have the same serialVersionUID
        ReplicationMessage initiatorMsg = 
            new ReplicationMessage(ReplicationMessage.TYPE_INITIATE_VERSION, 
                                   new Long(ReplicationMessage.
                                            serialVersionUID));
        sendMessage(initiatorMsg);
        verifyMessageAck(readMessage());

        // Check that master and slave log files are in synch
        initiatorMsg =
            new ReplicationMessage(ReplicationMessage.TYPE_INITIATE_INSTANT,
                                   new Long(synchOnInstant));
        sendMessage(initiatorMsg);
        verifyMessageAck(readMessage());
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
    private void verifyMessageAck(ReplicationMessage ack) 
        throws StandardException {
        //If the message is a TYPE_ACK the slave is capable
        //of handling the messages and is at a compatible database version.
        if (ack.getType() == ReplicationMessage.TYPE_ACK) {
            return;
        } else if (ack.getType() == ReplicationMessage.TYPE_ERROR) {
            // See ReplicationMessage#TYPE_ERROR
            String exception[] = (String[])ack.getMessage();
            throw StandardException.
                newException(exception[exception.length - 1], exception);
        } else {
            //The message format was not recognized. Hence throw
            //an unexpected exception.
            throw StandardException.newException
                (SQLState.REPLICATION_UNEXPECTED_EXCEPTION);
        }
    }
}

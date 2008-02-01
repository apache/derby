/*
 
   Derby - Class org.apache.derby.impl.services.replication.net.SocketConnection
 
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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * This class encapsulates a <code>Socket</code> connection and has
 * methods that allow to read and write into the Object streams
 * created from this connection.
 */
public class SocketConnection {
    /**
     * Contains the Socket connection between the Replication master and the
     * slave.
     */
    private final Socket socket;
    
    /**
     * used to write message objects into the socket connection.
     */
    private final ObjectOutputStream objOutputStream;
    
    /**
     * used to read message objects sent in the socket connection.
     */
    private final ObjectInputStream objInputStream;
    
    /**
     * Constructor creates the streams from the socket object passed as
     * parameter.
     *
     * @param socket the <code>Socket</code> object that this class
     *               encapsulates.
     *
     * @throws IOException If an exception occurs while creating the
     *                     streams from the socket object.
     */
    public SocketConnection(Socket socket) throws IOException {
        this.socket = socket;
        
        //Get the OutputStream from the socket
        objOutputStream = new ObjectOutputStream(socket.getOutputStream());
        //Get the InputStream from the socket
        objInputStream = new ObjectInputStream(socket.getInputStream());
    }
    
    /**
     * Used to read the object messages that are sent.
     * waits on the input stream until a data is present that
     * can be read and returns this data.
     *
     * @return the data read from the connection.
     *
     * @throws ClassNotFoundException Class of a serialized object cannot 
     *                                be found.
     * @throws IOException if an exception occurs while reading from the
     *                     stream.
     */
    public Object readMessage()
    throws ClassNotFoundException, IOException {
        return objInputStream.readObject();
    }
    
    /**
     * Used to send the object messages across the socket conection. 
     *
     * @param message the data to be written into the connection.
     *
     * @throws IOException if an exception occurs while writing into the
     *                     stream.
     */
    public void writeMessage(Object message) throws IOException {
        objOutputStream.writeObject(message);
        //flush the stream to ensure that all the data that is part
        //of the message object is written and no data remains
        //in this stream.
        objOutputStream.flush();
    }
    
    /**
     * Closes the <code>Socket</code> and the object streams obtained
     * from it.
     *
     * @throws IOException if an exception occurs while trying to close
     *                     the socket or the streams.
     */
    public void tearDown() throws IOException {
        objInputStream.close();
        objOutputStream.close();
        socket.close();
    }
}

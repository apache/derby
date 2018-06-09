/*
 
   Derby - Class org.apache.derby.impl.store.replication.net.SlaveAddress
 
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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Encapsulates the host name and the port number of the slave machine.
 */
public class SlaveAddress {
    /**
     * Contains the IP Address of the slave to replicate to.
     */
    private final InetAddress hostAddress;
    
    /**
     * Contains the port number at which the slave is listening for
     * connections from the master.
     */
    private final int portNumber;
    
    /**
     * Used as the default port number if the port number
     * is not mentioned. Port number 4851 is registered for Derby Replication
     * at IANA (See: http://www.iana.org/assignments/port-numbers)
     */
    public static final int DEFAULT_PORT_NO = 4851;
    
    /**
     *
     * Constructor initializes the host name and the port number with
     * valid values. If a valid host name or port number is not provided 
     * then these are initialized to default values.
     *
     * @param hostName a <code>String</code> that contains the host name of
     *                 the slave.
     * @param portNumber an <code>int</code> that contains the port number
     *                   of the slave.
     *
     * @throws UnknownHostException If an exception occurs while trying to
     *                              resolve the host name.
     */
    public SlaveAddress(String hostName, int portNumber) 
    throws UnknownHostException {
        //InetAddress#getByName will return the default (localhost) 
        //if no host name is given.  Hence, no explicit handling of 
        //default is necessary for the host address.
        hostAddress = InetAddress.getByName(hostName);
        //Check if a valid port number has been supplied
        if (portNumber > 0) { //If yes assign the value to port number
            this.portNumber = portNumber;
        } else { //If no assign the default value of the port number
            this.portNumber = DEFAULT_PORT_NO;
        }
    }
    
    /**
     * Used to get the IP Address corresponding to the host name of the
     * slave.
     *
     * @return an IP Address corresponding to the slave host name.
     */
    public InetAddress getHostAddress() {
        return hostAddress;
    }
    
    /**
     * Used to get the port number of the slave.
     *
     * @return an int representing the value of the port number of the slave.
     */
    public int getPortNumber() {
        return portNumber;
    }
}

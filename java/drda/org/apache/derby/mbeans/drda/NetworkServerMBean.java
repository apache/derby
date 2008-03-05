/*
    
   Derby - Class org.apache.derby.mbeans.drda.NetworkServerMBean

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

package org.apache.derby.mbeans.drda;

/**
 * This is an MBean defining a JMX management and monitoring interface of 
 * Derby's Network Server.
 * This interface consists of getter and setter methods for attributes that may
 * be read and/or modified, and methods representing operations that can be 
 * invoked.
 * 
 * For more information on Managed Beans, refer to the JMX specification.
 *
 * @see org.apache.derby.drda.NetworkServerControl
 *
 */
public interface NetworkServerMBean {
    
    // ---
    // ----------------- MBean attributes ------------------------------------
    // ---
    
    // Commented setters because:
    //   No attribute setting yet due to security concerns, see DERBY-1387.
    
    /**
     * Gets the value of the <code>derby.drda.host</code> network server
     * setting. In this context, the host defines the network interface on which
     * the Network Server is listening for connections. "<code>0.0.0.0</code>" 
     * means that the server allows connections from any host on the network.
     * 
     * @return the value of <code>derby.drda.host</code>
     */
    public String getDrdaHost();
    
    /**
     * Gets the value of the <code>derby.drda.keepAlive</code> network server
     * setting. 
     * 
     * @see <a href="http://db.apache.org/derby/docs/dev/adminguide/radmindrdakeepalive.html"><code>derby.drda.keepAlive</code> documentation</a>
     * @return the value of <code>derby.drda.keepAlive</code>
     */
    public boolean getDrdaKeepAlive();
    
    /**
     * Gets the value of the <code>derby.drda.maxThreads</code> network server 
     * setting.
     * @return the value of the <code>derby.drda.maxThreads</code> network 
     *         server setting
     */
    public int getDrdaMaxThreads();
    //public void setDrdaMaxThreads(int max) throws Exception;
    
    /**
     * Gets the value of the <code>derby.drda.portNumber</code> network server
     * setting. This is the port number on which the Network Server is listening
     * for client connections.
     * 
     * @return the port number on which the Network Server is listening
     *         for client connections.
     */
    public int getDrdaPortNumber();
    
    /**
     * Gets the value of the <code>derby.drda.securityMechanism</code> network 
     * server setting. 
     * 
     * @return the value of the <code>derby.drda.securityMechanism</code> 
     *         network server setting.
     */
    public String getDrdaSecurityMechanism();
    
    /**
     * Gets the value of the <code>derby.drda.sslMode</code> network server 
     * setting. 
     * 
     * @return the value of the <code>derby.drda.sslMode</code> network server 
     *         setting.
     */
    public String getDrdaSslMode();
    
    /**
     * Gets the value of the <code>derby.drda.streamOutBufferSize</code> network
     * server setting.
     * This setting is used to configure the size of the buffer used for 
     * streaming blob/clob from server to client.
     * 
     * @return the size of the buffer used for streaming blob/clob from server 
     *         to client
     */
    public String getDrdaStreamOutBufferSize();
    
    /**
     * Gets the value of the <code>derby.drda.timeSlice</code> network server 
     * setting.
     * @return the value of the <code>derby.drda.timeSlice</code> network 
     *         server setting
     */
    public int getDrdaTimeSlice();
    //public void setDrdaTimeSlice(int timeSlice) throws Exception;
    
    /**
     * Gets the value of the <code>derby.drda.traceAll</code> network server 
     * setting.
     * @return the value of the <code>derby.drda.traceAll</code> network 
     *         server setting
     */
    public boolean getDrdaTraceAll();
    //public void setDrdaTraceAll(boolean on) throws Exception;
    
    /**
     * Gets the value of the <code>derby.drda.traceDirectory</code> network 
     * server setting. If this setting has not been explicitly set by the
     * network server administrator, the default value is returned.
     * @return the value of the <code>derby.drda.timeSlice</code> network 
     *         server setting
     */
    public String getDrdaTraceDirectory();
    //public void setDrdaTraceDirectory(String dir) throws Exception;

    /**
     * Get the number of active sessions
     * @return number of active sessions
     */
    public int getActiveConnections();
    
    /**
     * get the number of waiting sessions. Always 0 if drdaMaxThreads is 0.
     * @return number of waiting sessions
     */
    public int getWaitingConnections();
    
    /**
     * Get the size of the thread pool.
     * @return size of thread pool
     */
    public int getConnectionThreadPoolSize();
    
    /**
     * Get the accumulated number of connections.
     * @return number of connections.
     */
    public int getConnectionCounter();
    
    /**
     * Get the total number of bytes read
     * @return number of bytes
     */
    public long getBytesReceived();
    
    /** 
     * Get the total number of bytes written.
     * @return number of bytes
     */
    public long getBytesSent();
    
    /**
     * Get the number of bytes received pr second. 
     * Shortest interval measured is 1 second.
     * @return bytes per millisecond
     */
    
    public int getBytesReceivedPerSecond();
    
     /**
     * Get the number of bytes sent pr second. 
     * Shortest interval measured is 1 second.
     * @return bytes per millisecond
     */
    
    public int getBytesSentPerSecond();
    
    /**
     * Return the start time of the network server.
     * @return Time in milli-seconds since the epoch that the network server started.
     * @see System#currentTimeMillis()
     */
    public long getStartTime();
    
    /**
     * Return the time the network server has been running.
     * @return Time in milli-seconds the server has been running.
     */
    public long getUptime(); 
    
    
    
    // ---
    // ----------------- MBean operations ------------------------------------
    // ---

    /**
     * Executes the network server's <code>ping</code> command.
     * Returns without errors if the server was successfully pinged.
     * @throws java.lang.Exception if the ping attempt fails (an indication that
     *         the network server is not running properly)
     */
    public void ping() throws Exception;
    
    // No other management operations yet due to security concerns, see 
    // DERBY-1387 for details.
    
}

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
 * <p>
 * This is an MBean defining a JMX management and monitoring interface of 
 * Derby's Network Server.</p>
 * <p>
 * This MBean is created and registered automatically at Network Server startup
 * if all requirements are met (J2SE 5.0 or better).</p>
 * <p>
 * Key properties for the registered MBean:</p>
 * <ul>
 * <li><code>type=NetworkServer</code></li>
 * <li><code>system=</code><em>runtime system identifier</em> (see 
 *     <a href="../package-summary.html#package_description">description of 
 * package org.apache.derby.mbeans</a>)</li>
 * </ul>
 * <p>
 * If a security manager is installed, accessing attributes and operations of
 * this MBean may require a <code>SystemPermission</code>; see individual method
 * documentation for details.</p>
 * <p>
 * For more information on Managed Beans, refer to the JMX specification.</p>
 *
 * @see org.apache.derby.drda.NetworkServerControl
 * @see org.apache.derby.security.SystemPermission
 * @see <a href="../package-summary.html"><code>org.apache.derby.mbeans</code></a>
 */
public interface NetworkServerMBean {
    
    // ---
    // ----------------- MBean attributes ------------------------------------
    // ---
    
    // Commented setters because:
    //   No attribute setting yet due to security concerns, see DERBY-1387.
    
    /**
     * <p>
     * Gets the network interface address on which the Network Server is 
     * listening. This corresponds to the value of the 
     * <code>derby.drda.host</code> property.</p>
     * <p>
     * For example, the value "<code>localhost</code>" means that the 
     * Network Server is listening on the local loopback interface only.
     * <p>
     * The special value "<code>0.0.0.0</code>" (IPv4 environments only)
     * represents the "unspecified address" - also known as the anylocal or 
     * wildcard address.  In this context this means that the server is 
     * listening on all network interfaces (and may thus be able to see 
     * connections from both the local host as well as remote hosts, depending
     * on which network interfaces are available).</p>
     * <p>
     * Requires <code>SystemPermission("server", "control")</code> if a security
     * manager is installed.</p>
     * 
     * @return the the network interface address on which the Network Server is 
     *         listening (<code>derby.drda.host</code>)
     */
    public String getDrdaHost();
    
    /**
     * <p>
     * Reports whether or not the Derby Network Server will send keep-alive 
     * probes and attempt to clean up connections for disconnected clients (the 
     * value of the {@code derby.drda.keepAlive} property).</p>
     * <p>
     * If {@code true}, a keep-alive probe is sent to the client if a "long 
     * time" (by default, more than two hours) passes with no other data being 
     * sent or received. This will detect and clean up connections for clients 
     * on powered-off machines or clients that have disconnected unexpectedly.
     * </p>
     * <p>
     * If {@code false}, Derby will not attempt to clean up connections from
     * disconnected clients, and will not send keep-alive probes.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * <p>
     * See also the documentation for the property {@code derby.drda.keepAlive}
     * in the <em>Derby Server and Administration Guide</em>, section
     * <em>Managing the Derby Network Server</em>, subsection <em>Setting
     * Network Server Properties</em>, subsubsection <em>derby.drda.keepAlive
     * property</em>.
     * </p>
     * @return {@code true} if Derby Network Server will send keep-alive 
     *         probes and attempt to clean up connections for disconnected 
     *         clients ({@code derby.drda.keepAlive})
     */
    public boolean getDrdaKeepAlive();
    
    /**
     * <p>
     * Reports the maximum number of client connection threads the Network 
     * Server will allocate at any given time. This corresponds to the 
     * <code>derby.drda.maxThreads</code> property.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     *
     * @return the maximum number of client connection threads the Network 
     *         Server will allocate at any given time 
     *         (<code>derby.drda.maxThreads</code>)
     */
    public int getDrdaMaxThreads();
    //public void setDrdaMaxThreads(int max) throws Exception;
    
    /**
     * <p>
     * Gets the port number on which the Network Server is listening for client 
     * connections. This corresponds to the value of the 
     * <code>derby.drda.portNumber</code> Network Server setting.</p>
     * <p>
     * Requires <code>SystemPermission("server", "control")</code> if a security
     * manager is installed.</p>
     * 
     * @return the port number on which the Network Server is listening
     *         for client connections.
     */
    public int getDrdaPortNumber();
    
    /**
     * <p>
     * The Derby security mechanism required by the Network Server for all 
     * client connections. This corresponds to the value of the 
     * <code>derby.drda.securityMechanism</code> property on the server.</p>
     * <p>
     * If not set, the empty String will be returned, which means that the 
     * Network Server accepts any connection which uses a valid security 
     * mechanism.</p>
     * <p>
     * For a list of valid security mechanisms, refer to the 
     * documentation for the <code>derby.drda.securityMechanism</code> property
     * in the <i>Derby Server and Administration Guide</i>.</p>
     * <p>
     * Requires <code>SystemPermission("server", "control")</code> if a security
     * manager is installed.</p>
     * 
     * @return the security mechanism required by the Network Server for all 
     *         client connections (<code>derby.drda.securityMechanism</code>)
     */
    public String getDrdaSecurityMechanism();
    
    /**
     * <p>
     * Reports whether client connections must be encrypted using Secure 
     * Sockets Layer (SSL), and whether certificate based peer authentication 
     * is enabled. Refers to the <code>derby.drda.sslMode</code> property.</p>
     * <p>
     * Peer authentication means that the other side of the SSL connection is 
     * authenticated based on a trusted certificate installed locally.</p>
     * <p>
     * The value returned is one of "<code>off</code>" (no SSL encryption), 
     * "<code>basic</code>" (SSL encryption, no peer authentication) and 
     * "<code>peerAuthentication</code>" (SSL encryption and peer
     * authentication). Refer to the <i>Derby Server and Administration 
     * Guide</i> for more details.</p>
     * <p>
     * Requires <code>SystemPermission("server", "control")</code> if a security
     * manager is installed.</p>
     * 
     * @return whether client connections must be encrypted using Secure 
     *         Sockets Layer (SSL), and whether certificate based peer 
     *         authentication is enabled (<code>derby.drda.sslMode</code>)
     */
    public String getDrdaSslMode();
    
    /**
     * <p>
     * The size of the buffer used for streaming BLOB and CLOB from server to 
     * client. Refers to the <code>derby.drda.streamOutBufferSize</code> 
     * property.</p>
     * <p>
     * This setting may improve streaming performance when the default sizes of 
     * packets being sent are significantly smaller than the maximum allowed 
     * packet size in the network.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the size of the buffer used for streaming blob/clob from server 
     *         to client (<code>derby.drda.streamOutBufferSize</code>)
     */
    public int getDrdaStreamOutBufferSize();
    
    /**
     * <p>
     * If the server property <code>derby.drda.maxThreads</code> is set to a 
     * non-zero value, this is the number of milliseconds that each client 
     * connection will actively use in the Network Server before yielding to 
     * another connection. If this value is 0, a waiting connection will become
     * active once a currently active connection is closed.</p>
     * <p>
     * Refers to the <code>derby.drda.timeSlice</code> server property.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     *
     * @return the number of milliseconds that each client connection will 
     *         actively use in the Network Server before yielding to 
     *         another connection (<code>derby.drda.timeSlice</code>)
     * @see #getDrdaMaxThreads()
     */
    public int getDrdaTimeSlice();
    //public void setDrdaTimeSlice(int timeSlice) throws Exception;
    
    /**
     * <p>
     * Whether server-side tracing is enabled for all client connections 
     * (sessions). Refers to the <code>derby.drda.traceAll</code> server 
     * property.</p>
     * <p>
     * Tracing may for example be useful when providing technical support 
     * information. The Network Server also supports tracing for individual
     * connections (sessions), see the <i>Derby Server and Administration 
     * Guide</i> ("Controlling tracing by using the trace facility") for 
     * details.</p>
     * <p>
     * When tracing is enabled, tracing information from each client 
     * connection will be written to a separate trace file.
     * </p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     *
     * @return whether tracing for all client connections is enabled
     *         (<code>derby.drda.traceAll</code>)
     * @see #getDrdaTraceDirectory()
     */
    public boolean getDrdaTraceAll();
    //public void setDrdaTraceAll(boolean on) throws Exception;
    
    /**
     * <p>
     * Indicates the location of tracing files on the server host, if server
     * tracing has been enabled.</p>
     * <p>
     * If the server setting <code>derby.drda.traceDirectory</code> is set,
     * its value will be returned. Otherwise, the Network Server's default 
     * values will be taken into account when producing the result.</p>
     * <p>
     * Requires <code>SystemPermission("server", "control")</code> if a security
     * manager is installed.</p>
     *
     * @return the potential location of tracing files on the server host
     * @see #getDrdaTraceAll()
     */
    public String getDrdaTraceDirectory();
    //public void setDrdaTraceDirectory(String dir) throws Exception;
    
    /**
     * <p>
     * Gets the total number of current connections (waiting or active) to the
     * Network Server.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the number of current connections
     * @see #getActiveConnectionCount()
     * @see #getWaitingConnectionCount()
     */
    public int getConnectionCount();

    /**
     * <p>
     * Gets the number of currently active connections. All connections are 
     * active if the DrdaMaxThreads attribute (<code>derby.drda.maxThreads</code> 
     * property) is 0.</p>
     * <p>
     * If DrdaMaxThreads is &gt; 0 and DrdaTimeSlice is 0, connections remain 
     * active until they are closed. If there are more than DrdaMaxThreads 
     * connections, inactive connections will be waiting for some active 
     * connection to close. The connection request will return when the 
     * connection becomes active.</p>
     * <p>
     * If DrdaMaxThreads is &gt; 0 and DrdaTimeSlice &gt; 0, connections will be 
     * alternating beetween active and waiting according to Derby's time 
     * slicing algorithm.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the number of active connections
     * @see #getDrdaMaxThreads()
     * @see #getDrdaTimeSlice()
     * @see #getWaitingConnectionCount()
     */
    public int getActiveConnectionCount();
    
    /**
     * <p>
     * Gets the number of currently waiting connections. This number will always
     * be 0 if DrdaMaxThreads is 0. Otherwise, if the total number of 
     * connections is less than or equal to DrdaMaxThreads, then no connections
     * are waiting.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the number of waiting connections
     * @see #getActiveConnectionCount()
     * @see #getDrdaMaxThreads()
     * @see #getDrdaTimeSlice()
     */
    public int getWaitingConnectionCount();
    
    /**
     * <p>
     * Get the size of the connection thread pool. If DrdaMaxThreads 
     * (<code>derby.drda.maxThreads</code>) is set to a non-zero value, the size
     * of the thread pool will not exceed this value.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the size of the Network Server's connection thread pool
     * @see #getDrdaMaxThreads()
     */
    public int getConnectionThreadPoolSize();
    
    /**
     * <p>
     * Gets the accumulated number of connections. This includes all active and
     * waiting connections since the Network Server was started. This number
     * will not decrease as long as the Network Server is running.</p>
     * <p>
     * Require <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the accumulated number of connections made since server startup
     */
    public int getAccumulatedConnectionCount();
    
    /**
     * <p>
     * Gets the total number of bytes read by the server since it was started.
     * </p>
     * <p>
     * Require <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the total number of bytes received by the server
     */
    public long getBytesReceived();
    
    /**
     * <p> 
     * Gets the total number of bytes written by the server since it was 
     * started.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the total number of bytes sent by the server
     */
    public long getBytesSent();
    
    /**
     * <p>
     * Gets the number of bytes received per second by the Network 
     * Server. This number is calculated by taking into account the number of
     * bytes received since the last calculation (or since MBean startup if
     * it is the first time this attibute is being read).</p>
     * <p>
     * The shortest interval measured is 1 second. This means that a new value
     * will not be calculated unless there has been at least 1 second since the
     * last calculation.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the number of bytes received per second
     */
    public int getBytesReceivedPerSecond();
    
     /**
     * <p>
     * Gets the number of bytes sent per second by the Network Server. 
     * This number is calculated by taking into account the number of
     * bytes sent since the last calculation (or since MBean startup if
     * it is the first time this attibute is being read).</p>
     * <p> 
     * The shortest interval measured is 1 second. This means that a new value
     * will not be calculated unless there has been at least 1 second since the
     * last calculation.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the number of bytes sent per millisecond
     */
    public int getBytesSentPerSecond();
    
    /**
     * <p>
     * Gets the start time of the network server. The time is reported as
     * the number of milliseconds (ms) since Unix epoch (1970-01-01 00:00:00 
     * UTC), and corresponds to the value of 
     * <code>java.lang.System#currentTimeMillis()</code> at the time the
     * Network Server was started.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the difference, measured in milliseconds, between the time the
     *         Network Server was started and Unix epoch (1970-01-01T00:00:00Z)
     * @see java.lang.System#currentTimeMillis()
     */
    public long getStartTime();
    
    /**
     * <p>
     * Gets the time (in milliseconds) the Network Server has been running. In
     * other words, the time passed since the server was started.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @return the difference, measured in milliseconds, between the current 
     *         time and the time the Network Server was started
     * @see #getStartTime()
     */
    public long getUptime(); 
    
    
    
    // ---
    // ----------------- MBean operations ------------------------------------
    // ---

    /**
     * <p>
     * Executes the network server's <code>ping</code> command.
     * Returns without errors if the server was successfully pinged.</p>
     * <p>
     * Note that the <code>ping</code> command itself will be executed from the 
     * network server instance that is actually running the server, and that the 
     * result will be transferred via JMX to the JMX client invoking this
     * operation. 
     * This means that this operation will test network server connectivity 
     * from the same host (machine) as the network server, as opposed to when 
     * the <code>ping</code> command (or method) of 
     * <code>NetworkServerControl</code> is executed from a remote machine.</p>
     * <p>
     * This operation requires the following permission to be granted to
     * the network server code base if a Java security manager is installed
     * in the server JVM:</p>
     * <codeblock>
     *   <code>
     *     permission java.net.SocketPermission "*", "connect,resolve";
     *   </code>
     * </codeblock>
     * <p>The value <code>"*"</code> will allow connections from the network 
     * server to any host and any port, and may be replaced with a more specific
     * value if so desired. The required value will depend on the value of the
     * <code>-h</code> (or <code>derby.drda.host</code>) (host) and 
     * <code>-p</code> (or <code>derby.drda.portNumber</code>) (port) settings
     * of the Network Server.</p>
     * <p>
     * Requires <code>SystemPermission("server", "monitor")</code> if a security
     * manager is installed.</p>
     * 
     * @throws java.lang.Exception if the ping attempt fails (an indication that
     *         the network server is not running properly)
     * @see org.apache.derby.drda.NetworkServerControl#ping()
     * @see java.net.SocketPermission
     */
    public void ping() throws Exception;
    
    // No other management operations yet due to security concerns, see 
    // DERBY-1387 for details.
    
}

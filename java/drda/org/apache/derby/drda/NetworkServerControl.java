/*

   Derby - Class org.apache.derby.drda.NetworkServerControl

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.drda;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Properties;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.impl.drda.DB2jServerImpl;

/** 
	NetworkServerControl provides the ability to start a Network Server or 
	connect to a running Network Server to shutdown, configure or retreive 
	diagnostic information.  With the exception of ping, these commands 
	can  only be performed from the  machine on which the server is running.  
	Commands can be performed from  the command line with the following 
	arguments:

	<P>
	<UL>
	<LI>start [-h &lt;host>] [-p &lt;portnumber>]:  This starts the network
	server on the port/host specified or on localhost, port 1527 if no
	host/port is specified and no properties are set to override the 
	defaults. By default Network Server will only listen for 
	connections from the machine on which it is running. 
	Use -h 0.0.0.0 to listen on all interfaces or -h &lt;hostname> to listen 
	on a specific interface on a  multiple IP machine. </LI>

	<LI>shutdown [-h &lt;host>][-p &lt;portnumber>]: This shutdowns the network 	server on the host and port specified or on the local host and port 
	1527(default) if no host or port is specified.  </LI> 

	<LI>ping [-h &lt;host>] [-p &lt;portnumber>] 
	This will test whether the Network Server is up.
	</LI>

	<LI>sysinfo [-h &lt;host>] [-p &lt;portnumber>]:  This prints 
	classpath and version  information about the Network Server, 
	the JVM and the Cloudscape server. 

	<LI>runtimeinfo [-h &lt;host] [-p &lt;portnumber]: This prints
	extensive debbugging information about sessions, threads, 
	prepared statements, and memory usage for the running Network Server.
	</LI>

	<LI>logconnections {on | off} [-h &lt;host>] [-p &lt;portnumber>]:  
	This turns logging of connections and disconnections on and off.  
	Connections and disconnections are logged to derby.log. 
	Default is off.</LI>

	<LI>maxthreads &lt;max> [-h &lt;host>][-p &lt;portnumber>]:  
	This sets the maximum number of threads that can be used for connections. 
	Default 0 (unlimitted).
	</LI>

	<LI>timeslice &lt;milliseconds> [-h &lt;host>][-p &lt;portnumber>]: 
	This sets the time each session can have using a connection thread 
	before yielding to a waiting session. Default is 0 (no yeild).
	
	</LI>

	<LI>trace {on | off} [-s &lt;session id>] [-h &lt;host>] [-p &lt;portnumber>]: 
	This turns drda tracing on or off for the specified session or if no 
	session is  specified for all sessions. Default is off</LI>


	<LI>tracedirectory &lt;tracedirectory> [-h &lt;host>] [-p &lt;portnumber>]: 
	This changes where new trace files will be placed. 
	For sessions with tracing already turned on,  
	trace files remain in the previous location. 
	Default is clousdcape.system.home</LI>

	</UL>
	<P>Properties can be set in the derby.properties file or on the command line.
	Properties on the command line take precedence over properties in the 
	derby.properties file.  Arguments on the command line take precedence
	over properties. 
	The following is a list of properties that can be set for 
	NetworkServerControl:

	<UL><LI>derby.drda.portNumber=&lt;port number>: This property 
	indicates which port should be used for the Network Server. </LI>

	<LI>derby.drda.host=&lt;host name  or ip address >: This property 
	indicates the ip address to which NetworkServerControl should connect 

	<LI>derby.drda.traceDirectory=&lt;trace directory>: This property 
	indicates where to put trace files. </LI>

	<LI>derby.drda.traceAll=true:  This property turns on tracing for
	all sessions. Default is tracing is off.</LI>

	<LI>derby.drda.logConnections=true:  This property turns on logging
	of connections and disconnections. Default is connections are not logged.</LI>

	<LI>derby.drda.minThreads=&lt;value>: If this property
	is set, the &lt;value> number of threads will be created when the Network Server is
	booted. </LI>

	<LI>derby.drda.maxThreads=&lt;value>: If this property
	is set, the &lt;value> is the maximum number of connection threads that will be 
	created.  If a session starts when there are no connection threads available
	and the maximum number of threads has been reached, it will wait until a 
	conection thread becomes available. </LI>

	<LI>derby.drda.timeSlice=&lt;milliseconds>: If this property
	is set, the connection threads will not check for waiting sessions until the
	current session has been working for &lt;milliseconds>.  
	A value of 0 causes the thread to work on the current session until the 
	session exits. If this property is not set, the default value is 0. </LI>
	
</LI>

<P><B>Examples.</B>

	<P>This is an example of shutting down the server on port 1621.
	<PRE> 
	java org.apache.derby.drda.NetworkServerControl shutdown -p 1621
	</PRE>

	<P>This is an example of turning tracing on for session 3
	<PRE>
	java org.apache.derby.drda.NetworkServerControl  trace on -s 3 
	</PRE>

	<P>This is an example of starting and then shutting down the network 
	   server on port 1621 on machine myhost   
	<PRE>
	java org.apache.derby.drda.NetworkServerControl  start -h myhost -p 1621
	java org.apache.derby.drda.NetworkServerControl  shutdown -h myhost -p 1621
	</PRE>

	<P> This is an example of starting and shutting down the Network Server in the example
	above with the API.
	<PRE>
	
	NetworkServerControl serverControl = new NetworkServerControl(InetAddress.getByName("myhost"),1621)

	serverControl.shutdown();
	</PRE>

	
*/

public class NetworkServerControl{


	
	public final static int DEFAULT_PORTNUMBER = 1527;
	private DB2jServerImpl serverImpl;

	// constructor

	/**
	 * 
	 * Creates a NetworkServerControl object that is configured to control
	 * a Network Server on a  specified port and InetAddress.
	 *<P>
	 * <B> Examples: </B>
	 * <P>
	 * To configure for port 1621 and listen on the loopback address:
	 *<PRE>
	 *  NetworkServerControl  util = new
	 * NetworkServerControl(InetAddress.getByName("localhost"), 1621);
	 * </PRE>
	 *
	 * @param address     The IP address of the Network Server host.
	 *                     address cannot be null.

	 * @param portNumber  port number server is to used. If <= 0,
	 *                    default port number is used
	 *                       
	 * @throws             Exception on error
	 */
	public NetworkServerControl(InetAddress address,int portNumber) throws Exception
	{
		
		serverImpl = new DB2jServerImpl(address, 
										portNumber);

	}
	

	/**
	 * 
	 * Creates a NetworkServerControl object that is configured to control
	 * a Network Server on the default host(localhost)
	 * and the default port(1527) unless derby.drda.portNumber and 
	 * derby.drda.host are set.
	 * <P><PRE>
	 * new NetworkServerControl() 
	 *
	 * is equivalent to calling
	 *
	 * new NetworkServerControl(InetAddress.getByName("localhost"),1527);
	 * </PRE>
	 *
	 * @throws             Exception on error
	 */
	public NetworkServerControl() throws Exception
	{
		
		serverImpl = new DB2jServerImpl();

	}
	
    
	/**
	 * main routine for NetworkServerControl
	 *
	 * @param args	array of arguments indicating command to be executed.
	 * See class comments for more information
	 */
    public static void main(String args[]) {
		DB2jServerImpl.execute(args);
		
	}

	/**********************************************************************
	 * Public NetworkServerControl  commands
	 * The server commands throw exceptions for errors, so that users can handle
	 * them themselves.
	 ************************************************************************
	 **/

	/** Start a Network Server
	 *  This method will launch a separate thread and start Network Server.
	 *  This method  may return before the server is ready to accept connections.
	 *  Use the ping method to verify that the server has started.
	 *
	 * <P>
	 *  Note: an alternate method to starting the Network Server with the API,
	 *  is to use the derby.drda.startNetworkServer property in 
	 *  cloudscape.properties.
	 *  
	 * 
	 * @param consoleWriter   PrintWriter to which server console will be 
	 *                        output. Null will disable console output. 
	 *
	 * @exception Exception if there is an error starting the server.
	 *
	 * @see #shutdown
	 */
	public void start(PrintWriter consoleWriter) throws Exception
	{
		serverImpl.start(consoleWriter);
	}

	

	/**
	 * Shutdown a Network Server.
	 * Shuts down the Network Server listening on the port and InetAddress
	 * specified in the constructor for this NetworkServerControl object.
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void shutdown()
		throws Exception
	{
		serverImpl.shutdown();
	}

	/**
	 * Check if Network Server is started
	 * Excecutes and returns without error if the server has started
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void  ping() throws Exception
	{
		 serverImpl.ping();
	}

	/**
	 * Turn tracing on or off for the specified connection 
	 * on the Network Server.
	 *
	 * @param on true to turn tracing on, false to turn tracing off.
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void trace(boolean on)
		throws Exception
	{
		serverImpl.trace(on);
	}


	/**
	 * Turn tracing on or off for all connections on the Network Server.
	 *
	 * @param connNum connection number. Note: Connection numbers will print
	 *                in the Cloudscape error log if logConnections is on
	 * @param on true to turn tracing on, false to turn tracing off.
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void trace(int connNum, boolean on)
		throws Exception
	{
		serverImpl.trace(connNum, on);
	}

	/**
	 * Turn logging connections on or off. When logging is turned on a message is
	 * written to the Cloudscape error log each time a connection 
	 * connects or disconnects.
	 *
	 * @param on			true to turn on, false to turn  off
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void logConnections(boolean on)
		throws Exception
	{
		serverImpl.logConnections(on);
	}

	/**
	 * Set directory for trace files. The directory must be on the machine
	 * where the server is running.
	 *
	 * @param traceDirectory	directory for trace files on machine 
	 *                          where server is running
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void setTraceDirectory(String traceDirectory)
		throws Exception
	{
		serverImpl.sendSetTraceDirectory(traceDirectory);
	}

	/**
	 * Return classpath and version information about the running 
	 * Network Server. 
	 *
	 * @return sysinfo output
	 * @exception Exception	throws an exception if an error occurs
	 */
	public String getSysinfo()
		throws Exception
	{
		
		return serverImpl.sysinfo();
	}

	/**
	 * Return detailed session runtime information about sessions,
	 * prepared statements, and memory usage for the running Network Server. 
	 *
	 * @return run time information
	 * @exception Exception	throws an exception if an error occurs
	 */
	public String getRuntimeInfo()
		throws Exception
	{
		return serverImpl.runtimeInfo();
	}


	/**
	 * Set Network Server maxthread parameter.  This is the maximum number 
	 * of threads that will be used for JDBC client connections.   setTimeSlice
	 * should also be set so that clients will yield appropriately.
	 *
	 * @param max		maximum number of connection threads.
	 *                  If <= 0, connection threads will be created when 
	 *                  there are no free connection threads.
	 *
	 * @exception Exception	throws an exception if an error occurs
	 * @see #setTimeSlice
	 */
	public void setMaxThreads(int max) throws Exception
	{
		serverImpl.netSetMaxThreads(max);
	}


	/** Returns the current maxThreads setting for the running Network Server
	 * 
	 * @return maxThreads setting 
	 * @exception Exception	throws an exception if an error occurs
	 * @see #setMaxThreads
	 */
	public int getMaxThreads() throws Exception
	{
	    String val =serverImpl.getCurrentProperties().getProperty(Property.DRDA_PROP_MAXTHREADS);

		
		return Integer.parseInt(val);
	}

	/**
	 * Set Network Server connection time slice parameter.  
	 * This should be set and is only relevant if setMaxThreads > 0.
	 *
	 * @param timeslice	number of milliseconds given to each session before yielding to 
	 *						another session, if <=0, never yield. 
	 *
	 * @exception Exception	throws an exception if an error occurs
	 * @see #setMaxThreads
	 */
	public void setTimeSlice(int timeslice) throws Exception
	{
		serverImpl.netSetTimeSlice(timeslice);
	}

	/** Return the current timeSlice setting for the running Network Server
	 * 
	 * @return timeSlice  setting
	 * @exception Exception throws an exception if an error occurs
	 * @see #setTimeSlice
	 */
	public int getTimeSlice() throws Exception
	{
		String val  =
			serverImpl.getCurrentProperties().getProperty(Property.DRDA_PROP_TIMESLICE);
		return Integer.parseInt(val);
	}



	/**
	 * Get current Network server properties
	 *
	 * @return Properties object containing Network server properties
	 * @exception Exception	throws an exception if an error occurs
	 */
	public Properties getCurrentProperties() throws Exception
	{
		return serverImpl.getCurrentProperties();
	}

	/** Protected methods ***/

	/***
	 * set the client locale. Used by servlet for localization
	 * @param locale  Locale to use
	 *
	 */
		  
	protected void setClientLocale(String locale)
	{
		serverImpl.clientLocale = locale;
	}
}






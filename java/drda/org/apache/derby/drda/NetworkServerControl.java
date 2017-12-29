/*

   Derby - Class org.apache.derby.drda.NetworkServerControl

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

package org.apache.derby.drda;

import java.io.PrintWriter;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URL;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Properties;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.impl.drda.NetworkServerControlImpl;

/** 
    NetworkServerControl provides the ability to start a Network Server or 
    connect to a running Network Server to shutdown, configure or retrieve 
    diagnostic information.  With the exception of ping, these commands 
    can  only be performed from the  machine on which the server is running.  
    Commands can be performed from  the command line with the following 
    arguments:

    <P>
    <UL>
    <LI>start [-h &lt;host&gt;] [-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]:  This starts the Network
    Server on the port/host specified or on localhost, port 1527 if no
    host/port is specified and no properties are set to override the 
    defaults. By default a security manager with a default security policy will 
    be installed. The default security policy file is called server.policy. 
    By default the Network Server will only listen for 
    connections from the machine on which it is running. 
    Use -h 0.0.0.0 to listen on all interfaces or -h &lt;hostname&gt; to listen 
    on a specific interface on a  multiple IP machine. 
    For documentation on &lt;sslmode&gt;, consult the Server and Administration Guide.</LI>

    <LI>shutdown [-h &lt;host&gt;][-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;] [-user &lt;username&gt;] [-password &lt;password&gt;]: This shutdowns the Network Server with given user credentials on the host and port specified or on the local host and port 1527(default) if no host or port is specified.  </LI> 

    <LI>ping [-h &lt;host&gt;] [-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]
    This will test whether the Network Server is up.
    </LI>

    <LI>sysinfo [-h &lt;host&gt;] [-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]:  This prints 
    classpath and version information about the Network Server, 
    the JVM and the Derby engine. 
    </LI>

    <LI>runtimeinfo [-h &lt;host] [-p &lt;portnumber] [-ssl &lt;sslmode&gt;]: This prints
    extensive debbugging information about sessions, threads, 
    prepared statements, and memory usage for the running Network Server.
    </LI>

    <LI>logconnections {on | off} [-h &lt;host&gt;] [-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]:  
    This turns logging of connections on or off.  
    Connections are logged to derby.log. 
    Default is off.</LI>

    <LI>maxthreads &lt;max&gt; [-h &lt;host&gt;][-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]:  
    This sets the maximum number of threads that can be used for connections. 
    Default 0 (unlimitted).
    </LI>

    <LI>timeslice &lt;milliseconds&gt; [-h &lt;host&gt;][-p &lt;portnumber&gt;] [-ssl &lt;sslmode&gt;]: 
    This sets the time each session can have using a connection thread 
    before yielding to a waiting session. Default is 0 (no yeild).
    
    </LI>

    <LI>trace {on | off} [-s &lt;session id&gt;] [-h &lt;host&gt;] [-p &lt;portnumber&gt;]  [-ssl &lt;sslmode&gt;]: 
    This turns drda tracing on or off for the specified session or if no 
    session is  specified for all sessions. Default is off</LI>


    <LI>tracedirectory &lt;tracedirectory&gt; [-h &lt;host&gt;] [-p &lt;portnumber&gt;]  [-ssl &lt;sslmode&gt;]: 
    This changes where new trace files will be placed. 
    For sessions with tracing already turned on,  
    trace files remain in the previous location. 
    Default is derby.system.home, if it is set. 
    Otherwise the default is the current directory.</LI>

    </UL>
    </P>
    <P>Properties can be set in the derby.properties file or on the command line.
    Properties on the command line take precedence over properties in the 
    derby.properties file.  Arguments on the command line take precedence
    over properties. 
    The following is a list of properties that can be set for 
    NetworkServerControl:

    <UL><LI>derby.drda.portNumber=&lt;port number&gt;: This property 
    indicates which port should be used for the Network Server. </LI>

    <LI>derby.drda.host=&lt;host name  or ip address &gt;: This property 
    indicates the ip address to which NetworkServerControl should connect. </LI>

    <LI>derby.drda.traceDirectory=&lt;trace directory&gt;: This property 
    indicates where to put trace files. </LI>

    <LI>derby.drda.traceAll=true:  This property turns on tracing for
    all sessions. Default is tracing is off.</LI>

    <LI>derby.drda.logConnections=true:  This property turns on logging
    of connections. Default is connections are not logged.</LI>

    <LI>derby.drda.minThreads=&lt;value&gt;: If this property
    is set, the &lt;value&gt; number of threads will be created when the Network Server is
    booted. </LI>

    <LI>derby.drda.maxThreads=&lt;value&gt;: If this property
    is set, the &lt;value&gt; is the maximum number of connection threads that will be 
    created.  If a session starts when there are no connection threads available
    and the maximum number of threads has been reached, it will wait until a 
    conection thread becomes available. </LI>

    <LI>derby.drda.timeSlice=&lt;milliseconds&gt;: If this property
    is set, the connection threads will not check for waiting sessions until the
    current session has been working for &lt;milliseconds&gt;.  
    A value of 0 causes the thread to work on the current session until the 
    session exits. If this property is not set, the default value is 0. </LI>

    <LI>derby.drda.sslMode=&lt;sslmode&gt; This property sets the SSL
    mode of the server.
    
</LI>
</UL>
</P>

<P><B>Examples.</B></P>

    <P>This is an example of shutting down the server on port 1621.
    <PRE> 
    java org.apache.derby.drda.NetworkServerControl shutdown -p 1621
    </PRE>
    </P>

    <P>This is an example of turning tracing on for session 3
    <PRE>
    java org.apache.derby.drda.NetworkServerControl  trace on -s 3 
    </PRE>
    </P>

    <P>This is an example of starting and then shutting down the Network 
       Server on port 1621 on machine myhost   
    <PRE>
    java org.apache.derby.drda.NetworkServerControl  start -h myhost -p 1621
    java org.apache.derby.drda.NetworkServerControl  shutdown -h myhost -p 1621
    </PRE>
    </P>

    <P> This is an example of starting and shutting down the Network Server in the example
    above with the API.
    <PRE>
    
    NetworkServerControl serverControl = new NetworkServerControl(InetAddress.getByName("myhost"),1621)

    serverControl.shutdown();
    </PRE>
    </P>

    
*/

public class NetworkServerControl{


    
    public final static int DEFAULT_PORTNUMBER = 1527;

    private final static String DERBYNET_JAR = "derbynet.jar";
    private final static String POLICY_FILENAME = "server.policy";
    private final static String POLICY_FILE_PROPERTY = "java.security.policy";
    private final static String DERBY_HOSTNAME_WILDCARD = "0.0.0.0";
    private final static String IPV6_HOSTNAME_WILDCARD = "::";
    private final static String SOCKET_PERMISSION_HOSTNAME_WILDCARD = "*";

    private NetworkServerControlImpl serverImpl;

    // constructor

    /**
     * Creates a NetworkServerControl object that is configured to control
     * a Network Server on a specified port and InetAddress with given
     * user credentials.
     *
     * @param address     The IP address of the Network Server host.
     *                     address cannot be null.
     *
     * @param portNumber  port number server is to used. If &lt;= 0,
     *                    default port number is used
     *
     * @param userName    The user name for actions requiring authorization.
     *
     * @param password    The password for actions requiring authorization.
     *
     * @throws             Exception on error
     */
    public NetworkServerControl(InetAddress address, int portNumber,
                                String userName, String password)
            throws Exception
    {
        serverImpl = new NetworkServerControlImpl(address, portNumber,
                                                  userName, password);
    }

    /**
     * Creates a NetworkServerControl object that is configured to control
     * a Network Server on the default host and the default port with given
     * user credentials.
     *
     * @param userName    The user name for actions requiring authorization.
     *
     * @param password    The password for actions requiring authorization.
     *
     * @throws             Exception on error
     */
    public NetworkServerControl(String userName, String password)
            throws Exception
    {
        serverImpl = new NetworkServerControlImpl(userName, password);
    }

    /**
     * 
     * Creates a NetworkServerControl object that is configured to control
     * a Network Server on a  specified port and InetAddress.
     *<P>
     * <B> Examples: </B>
     * </P>
     * <P>
     * To configure for port 1621 and listen on the loopback address:
     *<PRE>
     *  NetworkServerControl  util = new
     * NetworkServerControl(InetAddress.getByName("localhost"), 1621);
     * </PRE>
     * </P>
     *
     * @param address     The IP address of the Network Server host.
     *                     address cannot be null.

     * @param portNumber  port number server is to used. If &lt;= 0,
     *                    default port number is used
     *                       
     * @throws             Exception on error
     */
    public NetworkServerControl(InetAddress address,int portNumber) throws Exception
    {
        serverImpl = new NetworkServerControlImpl(address, portNumber);
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
     * </P>
     *
     * @throws             Exception on error
     */
    public NetworkServerControl() throws Exception
    {
        serverImpl = new NetworkServerControlImpl();
    }
    
    
    /**
     * main routine for NetworkServerControl
     *
     * @param args  array of arguments indicating command to be executed.
     * See class comments for more information
     */
    public static void main(String args[]) {
        NetworkServerControlImpl server = null;

        //
        // The following variable lets us preserve the error printing behavior
        // seen before we started installing a security manager. Errors can be
        // raised as we figure out whether we need to install a security manager
        // and during the actual installation of the security manager. We need
        // to print out these errors. The old error printing behavior assumed
        // that all errors were generated inside NetworkServerControlImpl and
        // were reported there.
        //
        boolean                                 printErrors = true;
        
        try
        {
            server = new NetworkServerControlImpl();
            
            int     command = server.parseArgs( args );

            // Java 7 and above: file permission restriction
            if (command == NetworkServerControlImpl.COMMAND_START) {
                try {
                    AccessController.doPrivileged((PrivilegedAction<Void>) () ->
                    {
                        System.setProperty(
                                Property.SERVER_STARTED_FROM_CMD_LINE,
                                "true");
                        return null;
                    });
                } catch (Exception e) {
                    server.consoleExceptionPrintTrace(e);
                    System.exit(1);
                }
            }

            //
            // In order to run secure-by-default, we install a security manager
            // if one isn't already installed. This feature is described by DERBY-2196.
            //
            if ( needsSecurityManager( server, command ) )
            {
                verifySecurityState( server );
                installSecurityManager( server );
            }

            //
            // From this point on, NetworkServerControlImpl is responsible for
            // printing errors.
            //
            printErrors = false;
            server.executeWork( command );
        }
        catch (Exception e)
        {
            //if there was an error, exit(1)
            if ((e.getMessage() == null) ||
                !e.getMessage().equals(NetworkServerControlImpl.UNEXPECTED_ERR) ||
                printErrors
            )
            {
                if (server != null)
                    server.consoleExceptionPrint(e);
                else
                    e.printStackTrace();  // default output stream is System.out
            }
            // else, we've already printed a trace, so just exit.
            System.exit(1);
        }
        System.exit(0);
        
    }

    /**********************************************************************
     * Public NetworkServerControl  commands
     * The server commands throw exceptions for errors, so that users can handle
     * them themselves.
     ************************************************************************
     **/

    /** Start a Network Server.
     *  This method will launch a separate thread and start a Network Server.
     *  This method  may return before the server is ready to accept connections.
     *  This will also install a security manager with a default security policy.
     *  Use the ping method to verify that the server has started.
     *
     * <P>
     *  Note: an alternate method to starting the Network Server with the API,
     *  is to use the derby.drda.startNetworkServer property in 
     *  derby.properties.
     * </P>
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
     * @exception Exception throws an exception if an error occurs
     */
    public void shutdown()
        throws Exception
    {
        serverImpl.shutdown();
    }

    /**
     * Check if the Network Server is started.
     * Excecutes and returns without error if the server has started
     *
     * @exception Exception throws an exception if an error occurs
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
     * @exception Exception throws an exception if an error occurs
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
     *                in the Derby error log if logConnections is on
     * @param on true to turn tracing on, false to turn tracing off.
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void trace(int connNum, boolean on)
        throws Exception
    {
        serverImpl.trace(connNum, on);
    }

    /**
     * Turn logging connections on or off. When logging is turned on a message is
     * written to the Derby error log each time a connection 
     * is made.
     *
     * @param on            true to turn on, false to turn  off
     *
     * @exception Exception throws an exception if an error occurs
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
     * @param traceDirectory    directory for trace files on machine 
     *                          where server is running
     *
     * @exception Exception throws an exception if an error occurs
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
     * @exception Exception throws an exception if an error occurs
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
     * @exception Exception throws an exception if an error occurs
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
     * @param max       maximum number of connection threads.
     *                  If &lt;= 0, connection threads will be created when 
     *                  there are no free connection threads.
     *
     * @exception Exception throws an exception if an error occurs
     * @see #setTimeSlice
     */
    public void setMaxThreads(int max) throws Exception
    {
        serverImpl.netSetMaxThreads(max);
    }


    /** Returns the current maxThreads setting for the running Network Server
     * 
     * @return maxThreads setting 
     * @exception Exception throws an exception if an error occurs
     * @see #setMaxThreads
     */
    public int getMaxThreads() throws Exception
    {
        String val =serverImpl.getCurrentProperties().getProperty(Property.DRDA_PROP_MAXTHREADS);

        
        return Integer.parseInt(val);
    }

    /**
     * Set Network Server connection time slice parameter.  
     * This should be set and is only relevant if setMaxThreads &gt; 0.
     *
     * @param timeslice number of milliseconds given to each session before yielding to
     *                      another session, if &lt;=0, never yield.
     *
     * @exception Exception throws an exception if an error occurs
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
     * @exception Exception throws an exception if an error occurs
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
        serverImpl.setClientLocale( locale );
    }

    /**
     * Return true if we need to install a Security Manager. All of the
     * following must apply. See DERBY-2196.
     *
     * <ul>
     * <li>The VM was booted with NetworkServerContro.main() as the
     * entry point. This is handled by the fact that this method is only called
     * by main().</li>
     * <li>The VM isn't already running a SecurityManager.</li>
     * <li>The command must be "start".</li>
     * <li>The customer didn't specify the -noSecurityManager flag on the startup command
     * line.</li>
     * </ul>
     */
    private static  boolean needsSecurityManager( NetworkServerControlImpl server, int command )
        throws Exception
    {
        return
            (
             (System.getSecurityManager() == null) &&
             (command == NetworkServerControlImpl.COMMAND_START) &&
             (!server.runningUnsecure())
             );
   }
    
    /**
     * Verify that all prerequisites are met before bringing up a security
     * manager. See DERBY-2196. If prerequisites aren't met, raise an
     * exception which explains how to get up and running. At one point, we were going to require
     * that authentication be enabled before bringing up a security manager.
     * This, however, gave rise to incompatibilities. See DERBY-2757.
     *
     * Currently, this method is a nop.
     */
    private static  void verifySecurityState( NetworkServerControlImpl server )
        throws Exception
    {
    }

    
    /**
     * Install a SecurityManager governed by the Basic startup policy. See DERBY-2196.
     */
    private static  void installSecurityManager( NetworkServerControlImpl server )
        throws Exception
    {
        //
        // The Basic policy refers to some properties. Make sure they are set.
        //
        if ( PropertyUtil.getSystemProperty( Property.SYSTEM_HOME_PROPERTY ) == null )
        { System.setProperty( Property.SYSTEM_HOME_PROPERTY, PropertyUtil.getSystemProperty( "user.dir" ) ); }

        //
        // Make sure the following property is set so that it can be substituted into the
        // policy file. That will let us grant write permission on the server's
        // trace file.
        //
        if ( PropertyUtil.getSystemProperty( Property.DRDA_PROP_TRACEDIRECTORY ) == null )
        { System.setProperty( Property.DRDA_PROP_TRACEDIRECTORY, PropertyUtil.getSystemProperty( Property.SYSTEM_HOME_PROPERTY ) ); }

        //
        // Forcibly set the following property so that it will be correctly
        // substituted into the default policy file. This is the hostname for
        // SocketPermissions. This is an internal property which customers
        // may not override.
        //
        System.setProperty( Property.DERBY_SECURITY_HOST, getHostNameForSocketPermission( server ) );

        //
        // Forcibly set the following property so that it will be correctly
        // substituted into the default policy file. This is the hostname for
        // SocketPermissions. This is an internal property which customers
        // may not override.
        //
        System.setProperty(Property.DERBY_SECURITY_PORT,
                           String.valueOf(server.getPort()));

        //
        // Forcibly set the following property. This is the parameter in
        // the Basic policy which points at the directory where the embedded and
        // network codesources. Do not let the customer
        // override this
        //
        String      derbyInstallURL = getCodeSourcePrefix( server );

        System.setProperty( Property.DERBY_INSTALL_URL, derbyInstallURL );
        
        //
        // Now install a SecurityManager, using the Basic policy file.
        //
        String      policyFileURL = getPolicyFileURL();

        System.setProperty( POLICY_FILE_PROPERTY, policyFileURL );
        
        SecurityManager     securityManager = new SecurityManager();

        System.setSecurityManager( securityManager );

        // Report success.
        if (securityManager.equals(System.getSecurityManager())) {
            String successMessage = server.localizeMessage(
                    "DRDA_SecurityInstalled.I", null);
            server.consoleMessage(successMessage, true);
        }
    }

    /**
     * Get the hostname as a value suitable for substituting into the
     * default server policy file. The special
     * wildcard valuse "0.0.0.0" and "::" are forced to be "*" since that is the wildcard
     * hostname understood by SocketPermission. SocketPermission does
     * not understand the "0.0.0.0" and "::" wildcards. IPV6 addresses are
     * enclosed in square brackets. This logic arose from two JIRAs:
     * DERBY-2811 and DERBY-2874.
     */
    private static String  getHostNameForSocketPermission( NetworkServerControlImpl server )
        throws Exception
    {
        //
        // By now, server.getPropertyInfo() has been called, followed by
        // server.parseArgs(). So the server knows its hostname.
        //
        String  hostname = server.getHost();
        
        if (
            hostnamesEqual( DERBY_HOSTNAME_WILDCARD, hostname ) ||
            IPV6_HOSTNAME_WILDCARD.equals( hostname ) 
            )
        { hostname = SOCKET_PERMISSION_HOSTNAME_WILDCARD; }
        else if ( isIPV6Address( hostname ) )
        { hostname = '[' + hostname + "]:0-"; }

        return hostname;
    }

    // return true if the two hostnames are equivalent
    private static  boolean hostnamesEqual( String left, String right )
    {
        try {
            InetAddress leftAddress = InetAddress.getByName( left );
            InetAddress rightAddress = InetAddress.getByName( right );

            return leftAddress.equals( rightAddress );
            
        } catch (Exception e) { return false; }
    }
    
    // return true if the host address is an IPV6 address
    private static  boolean isIPV6Address( String hostname )
    {
        if ( hostname == null ) { return false; }

        //
        // First make sure that the address is composed entirely
        // of hex digits and colons.
        //
        int         count = hostname.length();

        for ( int i = 0; i < count; i++ )
        {
            char    currentChar = hostname.charAt( i );

            if ( currentChar == ':' ) { continue; }
            if ( Character.digit( currentChar, 16 ) >= 0 ) { continue; }

            return false;
        }

        //
        // OK, now see whether the address is parsed as an IPV6 address.
        //
        
        try {
            InetAddress address = InetAddress.getByName( hostname );

            return (address instanceof Inet6Address);
            
        } catch (Exception e) { return false; }
    }

    /**
     *<p>
     * Find the url of the library directory which holds derby.jar and
     * derbynet.jar. The Basic policy assumes that both jar files live in the
     * same directory.
     * </p>
     */
    private static  String  getCodeSourcePrefix( NetworkServerControlImpl server )
        throws Exception
    {
        // Note: This method is expected to run only when no security manager
        //       has been installed, hence no use of privileged blocks.
        ProtectionDomain pd = NetworkServerControl.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        if (cs == null) {
            return null;
        }
        URL url = cs.getLocation();
        if (url == null) {
            return null;
        }
        // Replace in "file://some", but not in "file:///some".
        String extForm = url.toExternalForm().replaceFirst(
                "^file://([^/].*)", "file:////$1");
        int idx = extForm.indexOf(DERBYNET_JAR);

        //
        // If the customer isn't running against jar files, our Basic policy
        // won't work.
        //
        if ( idx < 0 )
        {
            String  errorMessage = server.localizeMessage( "DRDA_MissingNetworkJar.S", null );

            // this throws an exception and exits this method
            server.consoleError( errorMessage );
        }

        //
        // Otherwise, we have the directory prefix for our url.
        //
        String directoryPrefix = extForm.substring(0, idx);

        return directoryPrefix;
    }

    /**
     *<p>
     * Get the URL of the policy file. Typically, this will be some pointer into
     * derbynet.jar.
     * </p>
     */
    private static  String getPolicyFileURL()
        throws Exception
    {
        String      resourceName =
            NetworkServerControl.class.getPackage().getName().replace( '.', '/' ) +
            '/' +
            POLICY_FILENAME;
        URL         resourceURL = NetworkServerControl.class.getClassLoader().getResource( resourceName );
        String      stringForm = resourceURL.toExternalForm();

        return stringForm;
    }

}

/*
 *
 * Derby - Class org.apache.derbyTesting.junit.NetworkServerTestSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.iapi.error.ExceptionUtil;
import org.apache.derby.iapi.services.info.JVMInfo;

/**
 * Test decorator that starts the network server on startup
 * and stops it on teardown.
 * 
 */
final public class NetworkServerTestSetup extends BaseTestSetup {

    /**
     * <p>
     * Setting maximum wait time to 4 minutes by default. On some platforms
     * it may take this long to start the server. See for example
     * <a href="http://bugs.sun.com/view_bug.do?bug_id=6483406">this JVM
     * bug</a> that sometimes makes server startup take more than 3 minutes.
     * </p>
     *
     * <p>
     * Increasing the wait time should not adversely affect those
     *  systems with fast port turnaround as the actual code loops for 
     *  SLEEP_TIME intervals, so should never see WAIT_TIME.
     *  For even slower systems (or for faster systems) the default value can
     *  be overwritten using the property derby.tests.networkServerStartTimeout
     *  (which is in seconds, rather than milliseconds)
     * </p>
     */
    private static final long DEFAULT_WAIT_TIME = 240000;
    private static final long WAIT_TIME = getWaitTime();
    
    /** Sleep for 100 ms before pinging the network server (again) */
    private static final int SLEEP_TIME = 100;

    public static final String HOST_OPTION = "-h";

    private static  long    waitTime = WAIT_TIME;
    
    private FileOutputStream serverOutput;
    private final boolean asCommand;

    private final boolean startServerAtSetup;
    private final boolean useSeparateProcess;
    private final boolean serverShouldComeUp;
    
    /**
     * System properties to set on the command line (using -D)
     * only when starting the server in a separate virtual machine.
     */
    private final String[]    systemProperties;
    
    /**
     * Startup arguments for the command line
     * only when starting the server in a separate virtual machine.
     */
    private String[]    startupArgs;
    /**
     * The server as a process if started in a different vm.
     */
    private SpawnedProcess spawnedServer;
    
    /**
     * Decorates a test with the NetworkServerTestSetup.
     * 
     * Runs the server using the current configuration (at the time
     * of setup).
     * 
     * @param asCommand True to start using NetworkServerControl.main()
     * within the same virtual machine, false to use NetworkServerControl.start
     * (also within the same JVM).
     * 
     * @see NetworkServerControl#main(String[])
     * @see NetworkServerControl#start(PrintWriter)
     */
    public NetworkServerTestSetup(Test test, boolean asCommand) {
        super(test);
        this.asCommand = asCommand;

        this.systemProperties = null;
        this.startupArgs = null;
        this.useSeparateProcess = false;
        this.serverShouldComeUp = true;
        this.startServerAtSetup = true;
}

    /**
     * Decorates a test with the NetworkServerTestSetup.
     * 
     * Sets up the server using the current configuration. Whether or not the
     * server is actually started at setup time is determined by the value of 
     * the passed parameters.
     * 
     * @param test the Test for which this setup is used
     * @param asCommand True to start using NetworkServerControl.main()
     * within the same virtual machine, false to use NetworkServerControl.start()
     * (also within the same virtual machine).
     * @param startServerAtSetup True to start the Network Server at setup time,
     *        False otherwise.
     * 
     * @see NetworkServerControl#main(String[])
     * @see NetworkServerControl#start(PrintWriter)
     */
    public NetworkServerTestSetup(  Test test, 
                                    boolean asCommand, 
                                    boolean startServerAtSetup) {
        super(test);
        this.asCommand = asCommand;

        this.systemProperties = null;
        this.startupArgs = null;
        this.useSeparateProcess = false;
        this.serverShouldComeUp = true;

        this.startServerAtSetup = startServerAtSetup;
    }
    
     /**
     * Decorator for starting up with specific command args
     * and system properties. Server is always started up
     * in a separate process with a separate virtual machine.
     * <P>
     * If the classes are being loaded from the classes
     * folder instead of jar files then this will start
     * the server up with no security manager using -noSecurityManager,
     * unless the systemProperties or startupArgs set up any security
     * manager.
     * This is because the default policy
     * installed by the network server only works from jar files.
     * If this not desired then the test should skip the
     * fixtures when loading from classes or
     * install its own security manager.
     */
    public NetworkServerTestSetup
        (
         Test test,
         String[] systemProperties,
         String[] startupArgs,
         boolean serverShouldComeUp
        )
    {
        super(test);
        
        this.asCommand = true;

        this.systemProperties = systemProperties;
        this.startupArgs = startupArgs;
        this.useSeparateProcess = true;
        this.serverShouldComeUp = serverShouldComeUp;
        this.startServerAtSetup = true;
    }

    /**
     * Start the network server.
     */
    protected void setUp() throws Exception {
        BaseTestCase.println("Starting network server:");
        
        networkServerController = getNetworkServerControl();

        if (startServerAtSetup)
        {
            // DERBY-4201: A network server instance used in an earlier test
            // case might not have completely shut down and released the server
            // port yet. Wait here until the port has been released.
            waitForAvailablePort();

            if (useSeparateProcess)
            { spawnedServer = startSeparateProcess(); }
            else if (asCommand)
            { startWithCommand(); }
            else
            { startWithAPI(); }

            if (serverShouldComeUp)
            {
                if (!pingForServerStart(networkServerController)) {
                    String msg = "Timed out waiting for network server to start";
                    // Dump the output from the spawned process
                    // and destroy it.
                    if (spawnedServer != null) {
                        spawnedServer.complete(2000);
                        msg = spawnedServer.getFailMessage(msg);
                        spawnedServer = null;
                    }
                    fail(msg);
                    //DERBY-6012 print thread dump and java core
                    fail(ExceptionUtil.dumpThreads());
                    JVMInfo.javaDump();
                }
            }
        }
    }

    /**
     * Wait until the server port has been released by server instances used
     * by earlier test cases, or until the timeout specified by
     * {@link #getWaitTime()} has elapsed.
     *
     * @throws AssertionFailedError if the port didn't become available before
     * the timeout
     * @throws InterruptedException if the thread was interrupted while waiting
     * for the port to become available
     * @throws UnknownHostException if the host name couldn't be resolved
     */
    public static void waitForAvailablePort()
            throws InterruptedException, UnknownHostException {
        TestConfiguration conf = TestConfiguration.getCurrent();
        InetAddress serverAddress = InetAddress.getByName(conf.getHostName());
        int port = conf.getPort();
        long giveUp = System.currentTimeMillis() + getWaitTime();

        while (true) {
            try {
                probeServerPort(port, serverAddress);
                break;
            } catch (IOException ioe) {
                if (System.currentTimeMillis() < giveUp) {
                    Thread.sleep(SLEEP_TIME);
                } else {
                    BaseTestCase.fail(
                        "Timed out waiting for server port to become available",
                        ioe);
                }
            }
        }
    }

    /**
     * Check if a server socket can be opened on the specified port.
     *
     * @param port the port to check
     * @param addr the address of the network interface
     * @throws IOException if a server socket couldn't be opened
     */
    private static void probeServerPort(final int port, final InetAddress addr)
            throws IOException {
        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    new ServerSocket(port, 0, addr).close();
                    return null;
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

    private void startWithAPI() throws Exception
    {
            
            serverOutput = (FileOutputStream)
            AccessController.doPrivileged(new PrivilegedAction() {
                public Object run() {
                    File logs = new File("logs");
                    logs.mkdir();
                    File console = new File(logs, "serverConsoleOutput.log");
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(console.getPath(), true);
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }
                    return fos;
                }
            });
            
            networkServerController.start(new PrintWriter(serverOutput));
    }
    
    private void startWithCommand() throws Exception
    {
        // start the server through the command line
        // arguments using a new thread to do so.
        new Thread(
        new Runnable() {
            public void run() {

                String[]    args = getDefaultStartupArgs( false );
                
                org.apache.derby.drda.NetworkServerControl.main( args );
            }
            
        }, "NetworkServerTestSetup command").start();
    }

    private SpawnedProcess startSeparateProcess() throws Exception
    {
        ArrayList       al = new ArrayList();
        boolean         skipHostName = false;

        // Loading from classes need to work-around the limitation of the
        // default policy file doesn't work with classes.  Similarly, if we are
        // running with Emma we don't run with the security manager, as the
        // default server policy doesn't contain needed permissions and,
        // additionally, Emma sources do not use doPrivileged blocks anyway.
        if (!TestConfiguration.loadingFromJars() || BaseTestCase.runsWithEmma())
        {
            boolean setNoSecurityManager = true;
            for (int i = 0; i < systemProperties.length; i++)
            {
                if (systemProperties[i].startsWith("java.security."))
                {
                    setNoSecurityManager = false;
                    break;
                }
            }
            for (int i = 0; i < startupArgs.length; i++)
            {
                if (startupArgs[i].equals("-noSecurityManager"))
                {
                    setNoSecurityManager = false;
                    break;
                }
            }
            if (setNoSecurityManager)
            {
                String[] newArgs = new String[startupArgs.length + 1];
                System.arraycopy(startupArgs, 0, newArgs, 0, startupArgs.length);
                newArgs[newArgs.length - 1] = "-noSecurityManager";
                startupArgs = newArgs;
            }
        }

        int         count = systemProperties.length;
        for ( int i = 0; i < count; i++ )
        {
            al.add( "-D" + systemProperties[ i ] );
        }

        al.add( "org.apache.derby.drda.NetworkServerControl" );

        count = startupArgs.length;
        for ( int i = 0; i < count; i++ )
        {
            // if the special startup args override the hostname, then don't
            // specify it twice
            if ( HOST_OPTION.equals( startupArgs[ i ] ) ) { skipHostName = true; }
        }

        al.addAll(Arrays.asList(getDefaultStartupArgs(skipHostName)));
        al.addAll(Arrays.asList(startupArgs));

        final   String[]  command = new String[ al.size() ];
        al.toArray(command);

        Process serverProcess = BaseTestCase.execJavaCmd(command);

        return new SpawnedProcess(serverProcess, "SpawnedNetworkServer");
    }

    /**
     * Returns the <code>Process</code> object for the server process or <code>null</code> if the
     * network server does not run in a separate process
     */
    public SpawnedProcess getServerProcess() {
        return spawnedServer;
    }

    /**
     * Stop the network server if it still
     * appears to be running.
     */
    protected void tearDown() throws Exception {

        if (networkServerController != null) {
            boolean running = false;
            try {
                networkServerController.ping();
                running = true;
            } catch (Exception e) {
            }
      
            Throwable failedShutdown = null;
            if (running)
            {
                try {
                    networkServerController.shutdown();
                } catch (Throwable t)
                {
                    failedShutdown = t;
                }
            }
 
            if ( serverOutput != null ) { serverOutput.close(); }
            networkServerController = null;
            serverOutput = null;

            if (spawnedServer != null) {
                // Destroy the process if a failed shutdown
                // to avoid hangs running tests as the complete()
                // waits for the process to complete.
                spawnedServer.complete(getWaitTime());
                spawnedServer = null;
            }

            // Throw an error to record the fact that the
            // shutdown failed.
            if (failedShutdown != null)
            {
                if (failedShutdown instanceof Exception)
                {
                    // authentication failure is ok.
                    if (
                        !(failedShutdown instanceof SQLException)
                        )
                    {
                        throw (Exception) failedShutdown;
                    }
                }
                else
                {
                    throw (Error) failedShutdown;
                }
            }
                
        }
    }
    
    /**
     * Get the default command arguments for booting the network server.
     */
    public  static String[] getDefaultStartupArgs( boolean skipHostName )
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        ArrayList               argsList = new ArrayList();

        argsList.add( "start" );

        if ( !skipHostName )
        {
            argsList.add( HOST_OPTION );
            argsList.add( config.getHostName() );
        }
        argsList.add( "-p" );
        argsList.add( Integer.toString(config.getPort() ) );

        if (config.getSsl() != null) {
            argsList.add( "-ssl" );
            argsList.add( config.getSsl( ) );
        }

        String[]    retval = new String[ argsList.size() ];

        argsList.toArray( retval );

        return retval;
    }
    
    /* Network Server Control */
    private NetworkServerControl networkServerController;
        
    /*
     * Utility methods related to controlling network server.
     */
    
    /**
     * Return a new NetworkServerControl for the current configuration.
     */
    public static NetworkServerControl getNetworkServerControl()
        throws Exception
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        final InetAddress host = InetAddress.getByName(config.getHostName());
        final int port = config.getPort();
        final String user = config.getUserName();
        final String password = config.getUserPassword();
        if (config.getSsl() == null) {
            return new NetworkServerControl(host, port, user, password);
        } else {
            // This is a hack. A NetworkServerControl constructor with
            // the needed interface to control sslMode (and possibly
            // more) would be better.
            String oldValue = BaseTestCase.getSystemProperty("derby.drda.sslMode");
            BaseTestCase.setSystemProperty("derby.drda.sslMode", config.getSsl());
            NetworkServerControl control
                = new NetworkServerControl(host, port, user, password);
               
            if (oldValue == null) {

                BaseTestCase.removeSystemProperty("derby.drda.sslMode");
            } else {
                BaseTestCase.setSystemProperty("derby.drda.sslMode", oldValue);
            }
            return control;
        }
    }
    
    /**
     * Return a new NetworkServerControl for the current configuration.
     * Use the port number specified.
     * This method is not for general use - in most cases, the port
     * should not be specified in the test, instead, the test framework
     * will decide what is the best port number to use.
     */
    public static NetworkServerControl getNetworkServerControl(int port)
        throws Exception
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        final InetAddress host = InetAddress.getByName(config.getHostName());
        final String user = config.getUserName();
        final String password = config.getUserPassword();
        return new NetworkServerControl(host, port, user, password);
    }
    
    /**
     * Return a new NetworkServerControl for the current configuration.
     * Use default values, i.e. port number and host are dependent on 
     * whatever settings are set in the environment (properties)
     */
    public static NetworkServerControl getNetworkServerControlDefault()
        throws Exception
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        final String user = config.getUserName();
        final String password = config.getUserPassword();
        return new NetworkServerControl(user, password);
    }
    
    /**
     * Ping the server until it has started. Asserts a failure
     * if the server has not started within sixty seconds.
     */
    public static void waitForServerStart(NetworkServerControl networkServerController)
       throws InterruptedException 
    {
        if (!pingForServerStart(networkServerController)) {
             fail("Timed out waiting for network server to start");
        }
    }
    
     /**
     * Set the number of milliseconds to wait before declaring server startup
     * a failure.
     * 
     */
    public static void setWaitTime( long newWaitTime )
   {
        waitTime = newWaitTime;
    }
    
    /**
     * Set the number of milliseconds to wait before declaring server startup
     * a failure back to the default value specified in this class.
     * 
     */
    public static void setDefaultWaitTime()
    {
        waitTime = WAIT_TIME;
    }
    
    /**
     * Ping server for upto sixty seconds. If the server responds
     * in that time then return true, otherwise return false.
     * 
     * @param networkServerController controller object for network server
     * @param serverProcess the external process in which the server runs
     * (could be <code>null</code>)
     * @return true if server responds in time, false otherwise
     */
    public static boolean pingForServerUp(
        NetworkServerControl networkServerController, Process serverProcess,
        boolean expectServerUp)
        throws InterruptedException
    {
        // If we expect the server to be or come up, then
        // it makes sense to sleep (if ping unsuccessful), then ping 
        // and repeat this for the duration of wait-time, but stop
        // when the ping is successful.
        // But if we are pinging to see if the server is - or
        // has come - down, we should do the opposite, stop if ping 
        // is unsuccessful, and repeat until wait-time if it is
        final long startTime = System.currentTimeMillis();
        while (true) {
            try {
                networkServerController.ping();
                long elapsed = System.currentTimeMillis() - startTime;
                if (expectServerUp) {
                    if (elapsed > 60000L) {
                        BaseTestCase.alarm(
                            "Very slow server startup: " + elapsed + " ms");
                    }
                    return true;
                } else if (elapsed > waitTime) {
                    return true;
                }
            } catch (Throwable e) {
                if ( !vetPing( e ) )
                {
                    e.printStackTrace( System.out );

                    // at this point, we don't have the usual "server not up
                    // yet" error. get out. at this point, you may have to
                    // manually kill the server.

                    return false;
                }
                if (expectServerUp){
                    if (System.currentTimeMillis() - startTime > waitTime) 
                        return false;
                }
                // else, we got what we expected, done.
                else
                    return false;
            }
            if (serverProcess != null) {
                // if the server runs in a separate process, check whether the
                // process is still alive
                try {
                    int exitVal = serverProcess.exitValue();
                    // When exitValue() returns successfully, the server
                    // process must have terminated. No point in pinging the
                    // server anymore.
                    return false;
                } catch (IllegalThreadStateException e) {
                    // This exception is thrown by Process.exitValue() if the
                    // process has not terminated. Keep on pinging the server.
                } catch (Throwable t) {
                    // something unfortunate happened
                    t.printStackTrace( System.out );
                    return false;
                }
            }
            Thread.sleep(SLEEP_TIME);
        }
    }

    // return false if ping returns an error other than "server not up yet"
    private static  boolean vetPing( Throwable t )
    {
        if ( !t.getClass().getName().equals( "java.lang.Exception" ) ) { return false; }
        
        return ( t.getMessage().startsWith( "DRDA_NoIO.S:" ) );
    }
    
    // return true if this is a drda error
    private static  boolean isDRDAerror( Throwable t )
    {
        if ( !t.getClass().getName().equals( "java.lang.Exception" ) ) { return false; }
        
        return ( t.getMessage().startsWith( "DRDA" ) );
    }
    
    public static boolean pingForServerStart(NetworkServerControl control)
        throws InterruptedException
    {
        return pingForServerUp(control, null, true);
    }
    
    /**
     * Set the period before network server times out on start up based on the
     * value passed in with property derby.tests.networkServerStartTimeout
     * in seconds, or use the default.
     * For example: with DEFAULT_WAIT_TIME set to 240000, i.e. 4 minutes,
     * setting the property like so: 
     * <pre>
     *          -Dderby.tests.networkServerStartTimeout=600
     * </pre>
     * would extend the timeout to 10 minutes.
     * If an invalid value is passed in (eg. 'abc') the calling test will fail
     */
    public static long getWaitTime() {
        long waitTime = DEFAULT_WAIT_TIME;
        String waitString = BaseTestCase.getSystemProperty(
                "derby.tests.networkServerStartTimeout");
        if (waitString != null && waitString.length() != 0)
        {
            try {
                waitTime = (Long.parseLong(waitString)*1000);
            } catch (Exception e) {
                BaseTestCase.fail(
                        "trouble setting WAIT_TIME from passed in property " +
                        "derby.tests.networkServerStartTimeout", e);
            }
        }
        return waitTime;
    }

}

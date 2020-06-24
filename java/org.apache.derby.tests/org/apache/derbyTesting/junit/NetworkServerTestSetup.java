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
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.shared.common.error.ExceptionUtil;

/**
 * Test decorator that starts the network server on startup
 * and stops it on teardown.
 * 
 */
final public class NetworkServerTestSetup extends BaseTestSetup {
//IC see: https://issues.apache.org/jira/browse/DERBY-2000

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
     * Classpath or modulepath to use.
     */
    private final String      moduleOrClassPath;

    /**
     * If true, then use the modulepath. Otherwise, use the classpath.
     */
    private final boolean     useModules;

    /**
     * If true, then expected server diagnostics will be ignored
     * and will not be printed to the console.
     */
    private final boolean     suppressServerDiagnostics;
  
  
    
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

        this.moduleOrClassPath = null;
        this.useModules = false;
        this.suppressServerDiagnostics = false;
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

//IC see: https://issues.apache.org/jira/browse/DERBY-2196
        this.systemProperties = null;
        this.startupArgs = null;
        this.useSeparateProcess = false;
        this.serverShouldComeUp = true;

        this.startServerAtSetup = startServerAtSetup;

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        this.moduleOrClassPath = null;
        this.useModules = false;
        this.suppressServerDiagnostics = false;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        this(test, systemProperties, startupArgs, serverShouldComeUp, null, JVMInfo.isModuleAware(), false);
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
       boolean serverShouldComeUp,
       String moduleOrClassPath,
       boolean useModules,
       boolean suppressServerDiagnostics
       )
    {
        super(test);
        
        this.asCommand = true;

        this.systemProperties = systemProperties;
        this.startupArgs = startupArgs;
        this.useSeparateProcess = true;
        this.serverShouldComeUp = serverShouldComeUp;
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
        this.startServerAtSetup = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        this.moduleOrClassPath = moduleOrClassPath;
        this.useModules = useModules;
        this.suppressServerDiagnostics = suppressServerDiagnostics;
    }

    /**
     * Start the network server.
     */
    protected void setUp() throws Exception {
        BaseTestCase.println("Starting network server:");
        
        networkServerController = getNetworkServerControl();

//IC see: https://issues.apache.org/jira/browse/DERBY-3088
        if (startServerAtSetup)
        {
            // DERBY-4201: A network server instance used in an earlier test
            // case might not have completely shut down and released the server
            // port yet. Wait here until the port has been released.
            waitForAvailablePort();

            if (useSeparateProcess)
//IC see: https://issues.apache.org/jira/browse/DERBY-3504
            { spawnedServer = startSeparateProcess(); }
            else if (asCommand)
            { startWithCommand(); }
            else
            { startWithAPI(); }

            if (serverShouldComeUp)
            {
                if (!pingForServerStart(networkServerController)) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6012
                    String msg = getTimeoutErrorMsg("network server to start");
                    // Dump the output from the spawned process
                    // and destroy it.
                    if (spawnedServer != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
                        spawnedServer.complete(2000);
                        msg = spawnedServer.getFailMessage(msg);
                        spawnedServer = null;
                    }
                    //DERBY-6012 print thread dump and java core
                    JVMInfo.javaDump();
//IC see: https://issues.apache.org/jira/browse/DERBY-6012
                    fail(msg + Utilities.NL + ExceptionUtil.dumpThreads());
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6179
        waitForAvailablePort(TestConfiguration.getCurrent().getPort());
    }


    /**
     * Wait until the specified port has been released by
     * by earlier test cases, or until the timeout specified by
     * {@link #getWaitTime()} has elapsed.
     *
     * @param port value.
     * @throws AssertionFailedError if the port didn't become available before
     * the timeout
     * @throws InterruptedException if the thread was interrupted while waiting
     * for the port to become available
     * @throws UnknownHostException if the host name couldn't be resolved
     */
    public static void waitForAvailablePort(int port)
            throws InterruptedException, UnknownHostException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5547

//IC see: https://issues.apache.org/jira/browse/DERBY-6179
        InetAddress serverAddress = InetAddress.getByName(
                TestConfiguration.getCurrent().getHostName());

        long giveUp = System.currentTimeMillis() + getWaitTime();
        BaseTestCase.println(
                "probing port for availability: " + serverAddress + ":" + port);

        while (true) {
            try {
                probeServerPort(port, serverAddress);
                break;
            } catch (IOException ioe) {
                if (System.currentTimeMillis() < giveUp) {
                    Thread.sleep(SLEEP_TIME);
                } else {
                    BaseTestCase.fail(
//IC see: https://issues.apache.org/jira/browse/DERBY-6179
                        getTimeoutErrorMsg("server port to become available",
                            port),
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        BaseTestCase.println("Starting network server with NetworkServerControl api:");
            
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            serverOutput = AccessController.doPrivileged(
                    new PrivilegedAction<FileOutputStream>() {
                public FileOutputStream run() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
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
                
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                BaseTestCase.println("Starting network server with this command: " + Arrays.asList(args));
                
                org.apache.derby.drda.NetworkServerControl.main( args );
            }
            
        }, "NetworkServerTestSetup command").start();
    }

    private SpawnedProcess startSeparateProcess() throws Exception
    {
        BaseTestCase.println("Starting network server as a separate process:");
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> al = new ArrayList<String>();
        boolean         skipHostName = false;

        // Loading from classes need to work-around the limitation of the
        // default policy file doesn't work with classes.  Similarly, if we are
        // running with Emma we don't run with the security manager, as the
        // default server policy doesn't contain needed permissions and,
        // additionally, Emma sources do not use doPrivileged blocks anyway.
//IC see: https://issues.apache.org/jira/browse/DERBY-6067
        if (!TestConfiguration.loadingFromJars() ||
                BaseTestCase.runsWithEmma() || BaseTestCase.runsWithJaCoCo())
        {
            boolean setNoSecurityManager = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-3504
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

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        String serverName = NetworkServerControl.class.getName();
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (useModules)
        {
            al.add("-m");
            al.add(ModuleUtil.SERVER_MODULE_NAME + "/" + serverName);
        }
        else
        {
            al.add(serverName);
        }

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

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        Process serverProcess = BaseTestCase.execJavaCmd
          (null, moduleOrClassPath, command, null, true, useModules);

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
      
//IC see: https://issues.apache.org/jira/browse/DERBY-3544
            Throwable failedShutdown = null;
            if (running)
            {
                try {
                    networkServerController.shutdown();
                } catch (Throwable t)
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                    String errorMessage = t.getMessage();
                    //
                    // The following error message is expected if we
                    // are shutting down a server from an old Derby
                    // codeline. That can happen during the upgrade tests.
                    //
                    if ((errorMessage == null) || !errorMessage.contains("DRDA_InvalidReplyHead"))
                    {
                        failedShutdown = t;
                    }
                }
            }
 
            if ( serverOutput != null ) { serverOutput.close(); }
//IC see: https://issues.apache.org/jira/browse/DERBY-1966
            networkServerController = null;
            serverOutput = null;

            if (spawnedServer != null) {
                // Destroy the process if a failed shutdown
                // to avoid hangs running tests as the complete()
                // waits for the process to complete.
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                if (suppressServerDiagnostics) { spawnedServer.suppressOutputOnComplete(); }
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
                spawnedServer.complete(getWaitTime());
                spawnedServer = null;
            }

            // Throw an error to record the fact that the
            // shutdown failed.
//IC see: https://issues.apache.org/jira/browse/DERBY-3544
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
        ArrayList<String> argsList = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

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
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6012
             fail(getTimeoutErrorMsg("network server to start"));
        }
    }
    
     /**
     * Set the number of milliseconds to wait before declaring server startup
     * a failure.
     * 
     */
    public static void setWaitTime( long newWaitTime )
   {
//IC see: https://issues.apache.org/jira/browse/DERBY-2196
        waitTime = newWaitTime;
    }
    
    /**
     * Set the number of milliseconds to wait before declaring server startup
     * a failure back to the default value specified in this class.
     * 
     */
    public static void setDefaultWaitTime()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
        NetworkServerControl networkServerController, Process serverProcess,
        boolean expectServerUp)
        throws InterruptedException
    {
        //DERBY-6337(derbynet.ServerPropertiesTest.ttestSetPortPriority prints 
        // exception java.lang.Exception: 
        // DRDA_InvalidReplyTooShort.S:Invalidreply from network 
        // server:Insufficent data. but test passes)
        //Sometimes, when server is coming down and a ping is sent to it, ping
        // may get DRDA_InvalidReplyTooShort.S:Invalidreply rather than server
        // is down depending on the timing of the server shutdown. If we do run
        // into DRDA_InvalidReplyTooShort.S:Invalidreply, we will now send 
        // another ping after a little wait, and this time around we should 
        // get expected server down exception.
        //Following boolean will be set to true if we get reply too short
        // during the ping and it will try to ping again. But if we get
        // the reply too short on that ping attempt as well, we will just
        // print the exception on the console and conclude that server is
        // down.
        boolean alreadyGotReplyTooShort=false;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5643
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
                    if ( !alreadyGotReplyTooShort && 
                            (e.getMessage().startsWith( "DRDA_InvalidReplyTooShort.S:" ) ) ){
                        alreadyGotReplyTooShort = true;
                        Thread.sleep(SLEEP_TIME);
                        continue;
                    }
                    e.printStackTrace( System.out );
                    // at this point, we don't have the usual "server not up
                    // yet" error. get out. at this point, you may have to
                    // manually kill the server.

                    return false;
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
                if (expectServerUp){
                    if (System.currentTimeMillis() - startTime > waitTime) 
                        return false;
                }
                // else, we got what we expected, done.
                else
                    return false;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-2714
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
            Thread.sleep(SLEEP_TIME);
        }
    }

    // return false if ping returns an error other than "server not up yet"
    private static  boolean vetPing( Throwable t )
    {
        if ( !t.getClass().getName().equals( "java.lang.Exception" ) ) { return false; }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3834
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5643
                BaseTestCase.fail(
                        "trouble setting WAIT_TIME from passed in property " +
                        "derby.tests.networkServerStartTimeout", e);
            }
        }
        return waitTime;
    }

    /** Returns an error message for timeouts including the port and host. */
    private static String getTimeoutErrorMsg(String failedAction, int port) {
        TestConfiguration conf = TestConfiguration.getCurrent();
        String host = conf.getHostName();
        return "Timed out waiting for " +
                failedAction + " (" + host + ":" + port + ")";
    }

    private static String getTimeoutErrorMsg(String failedAction) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6179
        TestConfiguration conf = TestConfiguration.getCurrent();
        int port = conf.getPort();
        return getTimeoutErrorMsg(failedAction, port);
    }
}

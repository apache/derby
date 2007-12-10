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
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;

/**
 * Test decorator that starts the network server on startup
 * and stops it on teardown.
 * 
 */
final public class NetworkServerTestSetup extends BaseTestSetup {

    /** Setting maximum wait time to 300 seconds.   For some systems it looks
     *  like restarting a server to listen on the same port is blocked waiting
     *  for a system specific interval.  This number looks to be something
     *  like 240 seconds on XP.  Waiting shorter than this time causes
     *  intermittent failures on a laptop running XP with a software firewall
     *  and a VPN.  Increasing the wait time should not adversely affect those
     *  systems with fast port turnaround as the actual code loops for 
     *  SLEEP_TIME intervals, so should never see WAIT_TIME.
     */
    private static final long WAIT_TIME = 300000;
    
    /** Sleep for 500 ms before pinging the network server (again) */
    private static final int SLEEP_TIME = 500;

    public static final String HOST_OPTION = "-h";

    private static  long    waitTime = WAIT_TIME;
    
    private FileOutputStream serverOutput;
    private final boolean asCommand;

    private final boolean useSeparateProcess;
    private final boolean serverShouldComeUp;
    private final InputStream[] inputStreamHolder;
    
    /**
     * System properties to set on the command line (using -D)
     * only when starting the server in a separate virtual machine.
     */
    private final String[]    systemProperties;
    
    /**
     * Startup arguments for the command line
     * only when starting the server in a separate virtual machine.
     */
    private final String[]    startupArgs;
    private Process serverProcess;
    
    /**
     * Decorator this test with the NetworkServerTestSetup.
     * 
     * Runs the server using the current configuration (at the time
     * of setup).
     * 
     * @param asCommand True to start using NetworkServerControl.main()
     * within the same virtual machine, false to use NetworkServerControl.start.
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
        this.inputStreamHolder = null;
}

     /**
     * Decorator for starting up with specific command args
     * and system properties. Server is always started up
     * in a separate process with a separate virtual machine.
     */
    public NetworkServerTestSetup
        (
         Test test,
         String[] systemProperties,
         String[] startupArgs,
         boolean serverShouldComeUp,
         InputStream[] inputStreamHolder
        )
    {
        super(test);
        
        this.asCommand = true;

        this.systemProperties = systemProperties;
        this.startupArgs = startupArgs;
        this.useSeparateProcess = true;
        this.serverShouldComeUp = serverShouldComeUp;
        this.inputStreamHolder = inputStreamHolder;
    }

    /**
     * Start the network server.
     */
    protected void setUp() throws Exception {
        BaseTestCase.println("Starting network server:");
        
        networkServerController = getNetworkServerControl();

        if (useSeparateProcess)
        { serverProcess = startSeparateProcess(); }
        else if (asCommand)
        { startWithCommand(); }
        else
        { startWithAPI(); }
        
        if ( serverShouldComeUp ) { waitForServerStart(networkServerController); }
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
        final TestConfiguration config = TestConfiguration.getCurrent();
        
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

    private Process startSeparateProcess() throws Exception
    {
        StringBuffer    buffer = new StringBuffer();
        String              classpath = BaseTestCase.getSystemProperty( "java.class.path" );
        boolean         skipHostName = false;

        buffer.append( "java -classpath " );
        buffer.append( classpath );
        buffer.append( " " );

        int         count = systemProperties.length;
        for ( int i = 0; i < count; i++ )
        {
            buffer.append( " -D" );
            buffer.append( systemProperties[ i ] );
        }

        buffer.append( " org.apache.derby.drda.NetworkServerControl " );

        count = startupArgs.length;
        for ( int i = 0; i < count; i++ )
        {
            // if the special startup args override the hostname, then don't
            // specify it twice
            if ( HOST_OPTION.equals( startupArgs[ i ] ) ) { skipHostName = true; }
        }

        String[]    defaultArgs = getDefaultStartupArgs( skipHostName );

        count = defaultArgs.length;
        for ( int i = 0; i < count; i++ )
        {
            buffer.append( " " );
            buffer.append( defaultArgs[ i ] );
        }

        count = startupArgs.length;
        for ( int i = 0; i < count; i++ )
        {
            buffer.append( " " );
            buffer.append( startupArgs[ i ] );
        }

        final   String  command = buffer.toString();

        //System.out.println( "XXX server startup command = " + command );

        Process     serverProcess = (Process) AccessController.doPrivileged
            (
             new PrivilegedAction()
             {
                 public Object run()
                 {
                     Process    result = null;
                     try {
                        result = Runtime.getRuntime().exec( command );
                     } catch (Exception ex) {
                         ex.printStackTrace();
                     }
                     
                     return result;
                 }
             }
            );

        inputStreamHolder[ 0 ] = serverProcess.getInputStream();
        return serverProcess;
    }

    /**
     * Returns the <code>Process</code> object for the server process or <code>null</code> if the
     * network server does not run in a separate process
     */
    public Process getServerProcess() {
        return serverProcess;
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
      
            if (running)
            {
                try {
                    networkServerController.shutdown();
                } catch (Throwable t)
                {
                    t.printStackTrace( System.out );
                }
            }
 
            if ( serverOutput != null ) { serverOutput.close(); }
            networkServerController = null;
            serverOutput = null;

            if (serverProcess != null) {
                serverProcess.waitFor();
                serverProcess = null;
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
        if (config.getSsl() == null) {
            return new NetworkServerControl
                (InetAddress.getByName(config.getHostName()), 
                 config.getPort());
        } else {
            // This is a hack. A NetworkServerControl constructor with
            // the needed interface to control sslMode (and possibly
            // more) would be better.
            String oldValue = BaseTestCase.getSystemProperty("derby.drda.sslMode");
            BaseTestCase.setSystemProperty("derby.drda.sslMode", config.getSsl());
            NetworkServerControl control = new NetworkServerControl
                (InetAddress.getByName(config.getHostName()), 
                 config.getPort());
               
            if (oldValue == null) {

                BaseTestCase.removeSystemProperty("derby.drda.sslMode");
            } else {
                BaseTestCase.setSystemProperty("derby.drda.sslMode", oldValue);
            }
            return control;
        }
    }
    
    /**
     * Ping the server until it has started. Asserts a failure
     * if the server has not started within sixty seconds.
     */
    public static void waitForServerStart(NetworkServerControl networkServerController)
       throws InterruptedException 
    {
        if (!pingForServerStart(networkServerController))
            fail("Timed out waiting for network server to start");
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
     * Ping server for upto sixty seconds. If the server responds
     * in that time then return true, otherwise return false.
     * 
     * @param networkServerController controller object for network server
     * @param serverProcess the external process in which the server runs
     * (could be <code>null</code>)
     * @return true if server responds in time, false otherwise
     */
    public static boolean pingForServerStart(
        NetworkServerControl networkServerController, Process serverProcess)
        throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();
        while (true) {
            Thread.sleep(SLEEP_TIME);
            try {
                networkServerController.ping();
                return true;
            } catch (Throwable e) {
                if ( !vetPing( e ) )
                {
                    e.printStackTrace( System.out );

                    // at this point, we don't have the usual "server not up
                    // yet" error. get out. at this point, you may have to
                    // manually kill the server.

                    return false;
                }
                if (System.currentTimeMillis() - startTime > waitTime) {
                    return false;
                }
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
        }
    }

    // return false if ping returns an error other than "server not up yet"
    private static  boolean vetPing( Throwable t )
    {
        if ( !t.getClass().getName().equals( "java.lang.Exception" ) ) { return false; }
        
        return ( t.getMessage().startsWith( "DRDA_NoIO.S:Could not connect to Derby Network Server" ) );
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
        return pingForServerStart(control, null);
    }
}

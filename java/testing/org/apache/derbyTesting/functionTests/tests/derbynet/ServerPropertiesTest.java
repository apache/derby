/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.ServerPropertiesTest

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/** 
 * This test tests the derby.properties, system properties and command line
 * parameters to make sure the pick up settings in the correct order. 
 * Search order is:
 *     command line parameters
 *     System properties
 *     derby.properties
 *     default     
 * The command line should take precedence
 * 
 * The test also tests start server by specifying system properties without
 * values; in this case the server will use default values.
 */

public class ServerPropertiesTest  extends BaseJDBCTestCase {
    
    // helper state for intercepting server error messages;
    // needed by fixture testToggleTrace
    private InputStream[]  _inputStreamHolder;
    
    //create own policy file
    private static String POLICY_FILE_NAME = 
        "functionTests/tests/derbynet/ServerPropertiesTest.policy";
    private static String TARGET_POLICY_FILE_NAME = "server.policy";
    
    private static String[] serverProperties = {
                "derby.drda.logConnections",
                "derby.drda.traceAll",
                "derby.drda.traceDirectory",
                "derby.drda.keepAlive",
                "derby.drda.timeSlice",
                "derby.drda.host",
                "derby.drda.portNumber",
                "derby.drda.minThreads",
                "derby.drda.maxThreads",
                "derby.drda.startNetworkServer",
                "derby.drda.debug"
                };
    
    public ServerPropertiesTest(String name) {
        super(name);
        _inputStreamHolder = new InputStream[1];
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ServerPropertiesTest");
          
        if (!Derby.hasServer()) return suite;
        // don't run with JSR169 for 1. this is network server and
        // 2. the java executable may be named differently
        if (JDBC.vmSupportsJSR169()) return suite;
        
        // this fixture doesn't use a client/server setup, instead does the 
        // relevant starting/stopping inside the test
        // Add security manager policy that allows executing java commands
        Test setPortPriority = new ServerPropertiesTest("ttestSetPortPriority");
        setPortPriority = decorateWithPolicy(setPortPriority);
        suite.addTest(setPortPriority);
        
        // test unfinished properties settings. 
        // decorateTest adds policy file and sets up properties
        // fixture hits error DRDA_MissingNetworkJar (Cannot find derbynet.jar) so,
        // only run with jars
        if (TestConfiguration.loadingFromJars())
            suite.addTest(decorateTest("ttestDefaultProperties", 
                getStartupProperties(), new String[] {}));
        
        // The other fixtures, testToggleTrace (trace on/off), 
        // testToggleLogConnections (logconnections on/off) , and
        // testWrongCommands can all use the default setup with adjusted policy
        
        // need english locale so we can compare command output for those tests 
        if (!Locale.getDefault().getLanguage().equals("en"))
            return suite;
        
        Test test = TestConfiguration
            .clientServerSuite(ServerPropertiesTest.class);
        
        // Install a security manager using the special policy file.
        test = decorateWithPolicy(test);
        suite.addTest(test);
        return suite;
    }
    
    public void tearDown() throws Exception {
        super.tearDown();
        serverProperties = null;
        POLICY_FILE_NAME = null;
        TARGET_POLICY_FILE_NAME = null;
        _inputStreamHolder = null;
    }
    
    /**
     * <p>
     * Compose the required decorators to bring up the server in the correct
     * configuration.
     * </p>
     */
    private static Test decorateTest(String testName, 
            String[] startupProperties, String[] startupArgs)
    {
        ServerPropertiesTest spt = new ServerPropertiesTest(testName);
        String [] startupProps;
        if (startupProperties == null)
            startupProps = new String[] {};
        else
            startupProps = startupProperties;
        if (startupArgs == null)
            startupArgs = new String[]{};
        // start networkServer as a process
        NetworkServerTestSetup networkServerTestSetup =
            new NetworkServerTestSetup(spt,
                startupProps, startupArgs, true, 
                spt._inputStreamHolder);
        Test test = decorateWithPolicy(networkServerTestSetup);
        test = TestConfiguration.defaultServerDecorator(test);
        return test;
    }   
    
    /**
     * <p>
     * Return a set of startup properties for testing
     * </p>
     */
    private static  String[]  getStartupProperties()
    {

        ArrayList list = new ArrayList();
        for (int i = 0 ; i<serverProperties.length ; i++)
        {
            //System.out.println(serverProperties[i]);
            list.add(serverProperties[i] + "=");
        }
        String[] result = new String[ list.size()];
        list.toArray(result);
        return result;
    }
    
    /**
     * Construct the name of the server policy file.
     */
    private String makeServerPolicyName()
    {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            String  fileName = userDir + File.separator + SupportFilesSetup.EXTINOUT + File.separator + TARGET_POLICY_FILE_NAME;
            File      file = new File( fileName );
            String  urlString = file.toURL().toExternalForm();

            return urlString;
        }
        catch (Exception e)
        {
            System.out.println( "Unexpected exception caught by makeServerPolicyName(): " + e );

            return null;
        }
    }
    
    // grant ALL FILES execute, and getPolicy permissions,
    // as well as write for the trace files.
    private static Test decorateWithPolicy(Test test) {
        String serverPolicyName = new ServerPropertiesTest("test").makeServerPolicyName();
        //
        // Install a security manager using the initial policy file.
        //
        test = new SecurityManagerSetup(test,serverPolicyName );
        // Copy over the policy file we want to use.
        //
        test = new SupportFilesSetup(
            test, null, new String[] {POLICY_FILE_NAME},
            null, new String[] {TARGET_POLICY_FILE_NAME}
        );
        return test;
    }

    private static void verifyProperties(String[] expectedValues) { 
        Properties p;
        try {
        NetworkServerControl derbyServer = NetworkServerTestSetup.getNetworkServerControl(); 
            p = derbyServer.getCurrentProperties();
        } catch (Exception e) {
            p = null; // should be ok to set to null (to satisfy compiler)
            // as fail will exit without further checks.
            e.printStackTrace();
            fail("unexpected exception getting properties from server");
        }
        
        Enumeration e = p.propertyNames();
        // for debugging:
        for (int i=0 ; i<expectedValues.length; i++){
            println("expV: " + expectedValues[i]);
        }
        assertEquals(expectedValues.length , p.size());
        for ( int i = 0 ; i < p.size() ; i++)
        {
            String propName = (String)e.nextElement();
            // next line for debugging
            println("propName: " + propName);
            String propval = (String)p.get(propName);
            assertEquals(expectedValues[i], propval);
        }
        p = null;
    }
    
    public int getAlternativePort() throws SQLException {

        Exception failException = null;
        // start with the default port + 1
        // there may be a smarter way to get the starting point...
        int possiblePort = TestConfiguration.getCurrent().getPort();
        if (!(possiblePort > 0))
            possiblePort = 1528;
        else
            possiblePort = possiblePort + 1;
        try {
            boolean portOK = false;
            while (!portOK) {
                // check for first one in use
                NetworkServerControl networkServer =
                    new NetworkServerControl(InetAddress.getByName("localhost"), possiblePort);
                // Ping and wait for the network server to reply
                boolean started = false;

                try {
                    networkServer.ping();
                    // If ping throws no exception the server is running
                    started = true;
                } catch(Exception e) {         
                    failException = e;
                }
                // Check if we got a reply on ping
                if (!started) {
                    // we'll assume we can use this port. 
                    // If there was some other problem with the pinging, it'll
                    // become clear when someone attempts to use the port
                    portOK = true;
                }
                else { // this port's in use.
                    possiblePort = possiblePort + 1;
                }
            }
        } catch (Exception e) {
            SQLException se = new SQLException("Error pinging network server");
            se.initCause(failException);
            throw se;
        }        
        return possiblePort;
    }
    
    /**
     *  Ping for the server started on the specified port
     */
    public boolean canPingServer(int port, int SLEEP_TIME, int retries) 
    throws SQLException {
    
        // Wait for the network server to respond
        boolean started = false;
        if (retries > 10)
            retries = 10;         // Max retries = max seconds to wait

        while (!started && retries > 0) {
            try {
                NetworkServerControl nsctrl = new NetworkServerControl(
                        InetAddress.getByName(
                                TestConfiguration.getCurrent().getHostName()),
                                port);
                // Sleep x second and then ping the network server
                Thread.sleep(SLEEP_TIME);
                nsctrl.ping();

                // If ping does not throw an exception the server has started
                started = true;
            } catch(Exception e) {         
                retries--;
            }
        }
        return (started);
    }
        
    private Process runProcess(String[] command) {
        final String[] finalCommand = command;
        Process serverProcess = (Process) AccessController.doPrivileged
        (
         new PrivilegedAction()
         {
             public Object run()
             {
                 Process result = null;
                 try {
                    result = Runtime.getRuntime().exec(finalCommand);
                 } catch (Exception ex) {
                     ex.printStackTrace();
                     println("failure starting process");
                 }
                 return result;
             }
         }
        );
        return serverProcess;
    }
    
    // obtain & shutdown the network server;
    // port needs to be passed in to verify it's down;
    private String shutdownServer(int port, boolean specifyPort) 
    throws SQLException {
        try {
            if (specifyPort)
            {
                NetworkServerControl nsctrl = new NetworkServerControl(
                    InetAddress.getByName(
                        TestConfiguration.getCurrent().getHostName()), port);
                nsctrl.shutdown();
            }
            else
            {
                NetworkServerControl nsctrl = new NetworkServerControl();
                nsctrl.shutdown();
            }
        } catch (Exception e) {
            return "failed to shutdown server with API parameter";
        }
        if (canPingServer(port,0,1)) {
            return "Can still ping server";
        }
        return null;
    }


    // obtain & start the network server without specifying port;
    // port needs to be passed in to verify it's up.
    public String startServer(int port, boolean specifyPort) 
    throws SQLException {
        try {
            if (specifyPort)
            {
                NetworkServerControl nsctrl = new NetworkServerControl(
                        InetAddress.getByName(
                            TestConfiguration.getCurrent().getHostName()),
                            port);
                // For debugging, to make output come to console uncomment:
                //nsctrl.start(new PrintWriter(System.out, true));
                // and comment out:
                nsctrl.start(null);
                NetworkServerTestSetup.waitForServerStart(nsctrl);
            }
            else
            {
            NetworkServerControl nsctrl = new NetworkServerControl();
                // For debugging, to make output come to console uncomment:
                //nsctrl.start(new PrintWriter(System.out, true));
                // and comment out:
                nsctrl.start(null);
                NetworkServerTestSetup.waitForServerStart(nsctrl);
            }
        } catch (Exception e) {
            return "failed to start server with port " + port;
        }
        // check that we have this server up now
        if (!canPingServer(port, 1, 10)) {
            return "Cannot ping server started with port set to " + port;
        }
        return null;
    }
    
    /**
     *  Shutdown the server on the specified port - for cleanup
     */
    public void shutdownServer(int port) throws SQLException {
        try {
        NetworkServerControl nsctrl = new NetworkServerControl(
            InetAddress.getByName(
                TestConfiguration.getCurrent().getHostName()), port);
        nsctrl.shutdown();
        } catch (Exception e) {
            // ignore errors for this one.
        }
    }
    
   public void checkWhetherNeedToShutdown(int[] portsSoFar, String failReason) {
       
       if (!(failReason == null))
       {
           if (portsSoFar != null && portsSoFar[0] != 0);
           for (int i = 0 ; i < portsSoFar.length ; i++)
           {
               try {
                   shutdownServer(portsSoFar[i]);
               } catch (SQLException e) {
                   fail("could not shutdown server at port " + portsSoFar[i]);
               }
           }
       fail(failReason);
       }
   }
   
   /**
    * Execute command and verify that it completes successfully
    * @param Cmd array of java arguments for command
    * @throws InterruptedException
    * @throws IOException
    */
   private void  assertSuccessfulCmd(String expectedString, String[] Cmd) throws InterruptedException, IOException {
       assertExecJavaCmdAsExpected(new String[] {expectedString}, Cmd, 0);
   }

    /**
     *  Test port setting priority
     */
    public void ttestSetPortPriority() throws SQLException {
        // default is 1527. The test harness configuration would
        // use the API and add the port number. We want to test all
        // 4 mechanisms for specifying the port.
        // To ensure getting a unique port number, this test leaves open
        // each server for a bit.
        // as we need to test the default as well as with setting various
        // properties, this test can't rely on the testsetup.

        // So, first, bring default server down if up
        // Note: if the harness gets modified to accomodate splitting
        //    over different networkservers, there maybe something more
        //    appropriate than shutting down the default server.
        if (canPingServer(1527, 0, 1)) {
            // for now, shutdown
            shutdownServer(1527, false);
        }
        // start the default, which at this point should be localhost and 1527
        String actionResult = startServer(1527, false);
        checkWhetherNeedToShutdown(new int[] {1527}, actionResult);
        
        // set derby.drda.portNumber to an alternate number in derby.properties
        int firstAlternatePort = getAlternativePort();
        final Properties derbyProperties = new Properties();
        derbyProperties.put("derby.drda.portNumber", 
                new Integer(firstAlternatePort).toString());

        String tmpDerbyHome = "";
        try {
            final String derbyHome = (String)
            AccessController.doPrivileged
            (new java.security.PrivilegedAction(){
                public Object run(){
                    String x = System.getProperty(
                        "derby.system.home");
                    println("derbyhome: " + x);
                        return x;
                }
            });
            tmpDerbyHome = derbyHome;
        } catch (Exception e) {
            checkWhetherNeedToShutdown(new int[] {1527}, "failed to get derby.system.home for test");
        }
        
        final String derbyHome = tmpDerbyHome;
        Boolean b = (Boolean)AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            public Object run(){
                boolean fail = false;
                try {
                    FileOutputStream propFile = 
                        new FileOutputStream(derbyHome + File.separator + "derby.properties");
                    derbyProperties.store(propFile,"testing derby.properties");
                    propFile.close();
                } catch (IOException ioe) {
                    fail = true;
                }
                return new Boolean(fail);
            }
        });
        if (b.booleanValue())
       {
            checkWhetherNeedToShutdown(new int[] {1527}, "failed to write derby.properties");
        }
        // have to shutdown engine to force read of derby.properties
        TestConfiguration.getCurrent().shutdownEngine();
        actionResult = startServer(firstAlternatePort, false);
        checkWhetherNeedToShutdown(new int[] {1527, firstAlternatePort}, actionResult);

        final int secondAlternatePort = getAlternativePort();
        // Now set system properties.
        setSystemProperty("derby.drda.portNumber", 
            new Integer(secondAlternatePort).toString());
        actionResult = startServer(secondAlternatePort, false);
        checkWhetherNeedToShutdown( new int[] {1527, firstAlternatePort, secondAlternatePort},
            actionResult);
        
        // now try with specifying port
        // Note that we didn't unset the system property yet, nor did
        // we get rid of derby.properties...
        // command line parameter should take hold
        int thirdAlternatePort = getAlternativePort();
        actionResult = startServer(thirdAlternatePort, true);
        checkWhetherNeedToShutdown(new int[] {1527, firstAlternatePort, secondAlternatePort,
            thirdAlternatePort}, actionResult);

        // now with -p. 
        int fourthAlternatePort = getAlternativePort();
        String classpath = getSystemProperty("java.class.path");
        String[] commandArray = {"java", "-classpath", classpath, 
            "-Dderby.system.home=" + derbyHome,
            "org.apache.derby.drda.NetworkServerControl", "-p",
            String.valueOf(fourthAlternatePort).toString(), 
            "-noSecurityManager", "start"};
        Process p = runProcess(commandArray);
        
        if (!canPingServer(fourthAlternatePort,1,10)) {
            actionResult = "Can not ping server specified with -p";
        }
        checkWhetherNeedToShutdown(new int[] {1527, firstAlternatePort, secondAlternatePort,
            thirdAlternatePort, fourthAlternatePort}, actionResult);
            
        // shutdown with -p
        commandArray = new String[] {"java", "-classpath", classpath, 
                "-Dderby.system.home=" + derbyHome,
                "org.apache.derby.drda.NetworkServerControl", "-p",
                String.valueOf(fourthAlternatePort).toString(), 
                "-noSecurityManager", "shutdown"};
        Process p2 = runProcess(commandArray);

        if (canPingServer(fourthAlternatePort,1000,10)) {
            actionResult = "Can still ping server specified with -p";
        }
        checkWhetherNeedToShutdown(new int[] {1527, firstAlternatePort, secondAlternatePort,
            thirdAlternatePort, fourthAlternatePort}, actionResult);
            
        // clean up
        InputStream istr = p.getInputStream();
        InputStream istr2 = p2.getInputStream();
        try {
            istr.close();
            istr2.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("cannot close spawned process' inputstream");
        }
        istr=null;
        istr2=null;
        p.destroy();
        p=null;
        p2.destroy();
        p2=null;
        
        // shutdown with port specified in constructor
        actionResult = shutdownServer(thirdAlternatePort, true);
        checkWhetherNeedToShutdown( new int[] {1527, firstAlternatePort, secondAlternatePort,
            thirdAlternatePort}, actionResult);
        
        // shutdown using System property
        actionResult = shutdownServer(secondAlternatePort, false);
        checkWhetherNeedToShutdown ( new int[] {1527, firstAlternatePort, secondAlternatePort},
            actionResult);
        // remove system property
        removeSystemProperty("derby.drda.portNumber");

        // shutdown server with port set in derby.properties
        actionResult = shutdownServer(firstAlternatePort, false);
        checkWhetherNeedToShutdown ( new int[] {1527, firstAlternatePort},
            actionResult);
        // remove derby.properties
        Boolean ret = (Boolean) AccessController.doPrivileged
        (new java.security.PrivilegedAction() {
            public Object run() {
                return Boolean.valueOf((new File(
                    derbyHome+File.separator + "derby.properties")).delete());
            }
        }
        );
        if (ret.booleanValue() == false) {
            checkWhetherNeedToShutdown ( new int[] {1527, firstAlternatePort},
                "unable to remove derby.properties");
        }
        // have to shutdown engine to force re-evaluation of derby.properties
        TestConfiguration.getCurrent().shutdownEngine();
        
        // shutdown the default server
        actionResult = shutdownServer(1527, false);
        checkWhetherNeedToShutdown ( new int[] {1527}, actionResult);
    }
    
    /**
     *   Test start server specifying system properties without values
     */
    public void ttestDefaultProperties() throws SQLException
    {
        //check that default properties are used
        verifyProperties(new String[] {
                // getProperties returns properties in sequence:
                // maxThreads; sslMode; keepAlive; minThreads; portNumber;
                // logConnections; timeSlice; startNetworkServer; host; traceAll 
                "0", "off", "true", "0", 
                String.valueOf(TestConfiguration.getCurrent().getPort()),
                "false", "0", "false", 
                String.valueOf(TestConfiguration.getCurrent().getHostName()), 
                "false"});     
    }
       
    /**
     *   Test trace command on - property traceAll should get set
     */
    public void testToggleTrace() 
    throws SQLException, IOException, InterruptedException
    {        
        String[] expectedTraceOff = new String[] {
                // getProperties returns properties in sequence:
                // traceDirectory; maxThreads; sslMode; keepAlive; minThreads; 
                // portNumber; logConnections; timeSlice; startNetworkServer;
                // host; traceAll
                getSystemProperty("derby.system.home"),
                "0", "off", "true", "0", 
                String.valueOf(TestConfiguration.getCurrent().getPort()),
                "false", "0", "false", 
                //String.valueOf(TestConfiguration.getCurrent().getHostName()),
                "127.0.0.1", 
                "false"};     
        String[] expectedTraceOn = new String[] {
                // getProperties returns properties in sequence:
                // traceDirectory; maxThreads; sslMode; keepAlive; minThreads; 
                // portNumber; logConnections; timeSlice; startNetworkServer;
                // host; traceAll
                getSystemProperty("derby.system.home"),
                "0", "off", "true", "0", 
                String.valueOf(TestConfiguration.getCurrent().getPort()),
                "false", "0", "false", 
                //String.valueOf(TestConfiguration.getCurrent().getHostName()),
                "127.0.0.1", 
                "true"};     
        
        verifyProperties(expectedTraceOff);     

        String[] traceCmd = new String[] {
            "org.apache.derby.drda.NetworkServerControl", "trace", "on" };
        assertSuccessfulCmd("Trace turned on for all sessions.", traceCmd);
        verifyProperties(expectedTraceOn);     

        traceCmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "trace", "off" };
        assertSuccessfulCmd("Trace turned off for all sessions", traceCmd);
        // traceAll should be back to false
        verifyProperties(expectedTraceOff);     
    }

    /**
     *   Test logconnections on
     */
    public void testToggleLogConnections() 
    throws SQLException, IOException, InterruptedException
    {
        String[] expectedLogConnectionsOff = new String[] {
                // getProperties returns properties in sequence:
                // traceDirectory; maxThreads; sslMode; keepAlive; minThreads; 
                // portNumber; logConnections; timeSlice; startNetworkServer;
                // host; traceAll
                getSystemProperty("derby.system.home"),
                "0", "off", "true", "0", 
                String.valueOf(TestConfiguration.getCurrent().getPort()),
                "false", "0", "false", 
                //String.valueOf(TestConfiguration.getCurrent().getHostName()),
                "127.0.0.1", 
                "false"};     
        String[] expectedLogConnectionsOn = new String[] {
                // getProperties returns properties in sequence:
                // traceDirectory; maxThreads; sslMode; keepAlive; minThreads; 
                // portNumber; logConnections; timeSlice; startNetworkServer;
                // host; traceAll
                getSystemProperty("derby.system.home"),
                "0", "off", "true", "0", 
                String.valueOf(TestConfiguration.getCurrent().getPort()),
                "true", "0", "false", 
                //String.valueOf(TestConfiguration.getCurrent().getHostName()),
                "127.0.0.1", 
                "false"};     
        
        verifyProperties(expectedLogConnectionsOff);     

        String[] cmd = new String[] {
            "org.apache.derby.drda.NetworkServerControl", "logconnections", "on" };
        assertSuccessfulCmd("Log Connections changed to on.", cmd);
        verifyProperties(expectedLogConnectionsOn);     

        cmd = new String[] {
                "org.apache.derby.drda.NetworkServerControl", "logconnections", "off" };
        assertSuccessfulCmd("Log Connections changed to off.", cmd);
        // traceAll should be back to false
        verifyProperties(expectedLogConnectionsOff);    
    }

    
    /**
     *   Test other commands. These should all give a helpful error and the
     *   usage message
     *   Note: maybe these test cases should be moved to another Test,
     *   as they don't actually test any properties. 
     */
    public void testWrongCommands() 
    throws SQLException, IOException, InterruptedException
    {
        String nsc = "org.apache.derby.drda.NetworkServerControl";
        // no arguments
        String[] cmd = new String[] {nsc};
        // we'll assume that we get the full message if we get 'Usage'
        // because sometimes, the message gets returned with carriage return,
        // and sometimes it doesn't, checking for two different parts...
        assertExecJavaCmdAsExpected(new String[] 
            {"No arguments given.", "Usage: "}, cmd, 1);
        //Unknown command
        cmd = new String[] {nsc, "unknowncmd"};
        assertExecJavaCmdAsExpected(new String[] 
            {"Command unknowncmd is unknown.", "Usage: "}, cmd, 0);
        // wrong number of arguments
        cmd = new String[] {nsc, "ping", "arg1"};
        assertExecJavaCmdAsExpected(new String[] 
            {"Invalid number of arguments for command ping.",
             "Usage: "}, cmd, 1);
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SecureServerTest

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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.ClassLoaderTestSetup;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SpawnedProcess;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This Junit test class tests whether the server comes up under a security
 * manager as expected.
 */

public class SecureServerTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // basic properties file which tests that properties are picked up from derby.properties
    private static  final   String  BASIC = "functionTests/tests/derbynet/SecureServerTest.derby.properties";

    private static  final   String  SST_USER_NAME="MARY";
    private static  final   String  SST_PASSWORD = "marypwd";
    
    private static  final   String  HOSTW = "0.0.0.0";
    private static  final   String  ALTW = "0.00.000.0";
    private static  final   String  IPV6W = "::";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Possible outcomes for the experiment of bringing up the server.
     * </p>
     */
    public  static  final   class   Outcome
    {
        private boolean _serverShouldComeUp;
        private String      _expectedServerOutput;

        public Outcome
            (
             boolean serverShouldComeUp,
             String expectedServerOutput
             )
        {
            _serverShouldComeUp =  serverShouldComeUp;
            _expectedServerOutput = expectedServerOutput;
        }

        public  boolean serverShouldComeUp() { return _serverShouldComeUp; }
        public  String    expectedServerOutput() { return _expectedServerOutput; }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final Outcome RUNNING_SECURITY_NOT_BOOTED = new Outcome( true, "" );
    private static final Outcome RUNNING_SECURITY_BOOTED = new Outcome( true,  serverBootedOK() );

    /** Reference to the enclosing NetworkServerTestSetup. */
    private NetworkServerTestSetup nsTestSetup;
        
    // startup state
    private boolean _unsecureSet;
    private boolean _authenticationRequired;
    private String   _customDerbyProperties;
    private String _wildCardHost;

    // expected outcomes
    private Outcome _outcome;

    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public SecureServerTest
        (
         boolean unsecureSet,
         boolean authenticationRequired,
         String     customDerbyProperties,
         String     wildCardHost,

         Outcome    outcome
        )
    {
         super( "testServerStartup" );

         _unsecureSet =  unsecureSet;
         _authenticationRequired =  authenticationRequired;
         _customDerbyProperties = customDerbyProperties;
         _wildCardHost = wildCardHost;

         _outcome = outcome;
    }

    public SecureServerTest(String fixture) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745
        super(fixture);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Tests to run.
     */
    public static Test suite()
    {
        //NetworkServerTestSetup.setWaitTime( 10000L );
        
        BaseTestSuite      suite = new BaseTestSuite("SecureServerTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Server booting requires that we run from the jar files
        if ( !TestConfiguration.loadingFromJars() ) { return suite; }
        
        // Need derbynet.jar in the classpath!
//IC see: https://issues.apache.org/jira/browse/DERBY-2366
        if (!Derby.hasServer())
            return suite;

        // O = Overriden
        // A = Authenticated
        // C = Custom properties
        // W = Wildcard host
        //
        //      .addTest( decorateTest( O,        A,       C,    W,    Outcome ) );
        //

        suite.addTest( decorateTest( false,  false, null, null, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  false, BASIC, null, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  true, null, null, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  true, null, HOSTW, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  true, null, ALTW, RUNNING_SECURITY_BOOTED ) );

        // this wildcard port is rejected by the server right now
        //suite.addTest( decorateTest( false,  true, null, IPV6W, RUNNING_SECURITY_BOOTED ) );
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745
        suite.addTest( makeDerby6619Test() );
        return suite;
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TEST DECORATION
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * <p>
     * Compose the required decorators to bring up the server in the correct
     * configuration.
     * </p>
     */
    private static  Test    decorateTest
        (
         boolean unsecureSet,
         boolean authenticationRequired,
         String customDerbyProperties,
         String wildCardHost,
         
         Outcome outcome
        )
    {
        SecureServerTest            secureServerTest = new SecureServerTest
            (
             unsecureSet,
             authenticationRequired,
             customDerbyProperties,
             wildCardHost,

             outcome
            );

        String[]        startupProperties = getStartupProperties( authenticationRequired, customDerbyProperties );
        String[]        startupArgs = getStartupArgs( unsecureSet, wildCardHost );

//IC see: https://issues.apache.org/jira/browse/DERBY-2714
        NetworkServerTestSetup networkServerTestSetup =
                new NetworkServerTestSetup
            (
             secureServerTest,
             startupProperties,
             startupArgs,
//IC see: https://issues.apache.org/jira/browse/DERBY-3504
             secureServerTest._outcome.serverShouldComeUp()
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-2714

        secureServerTest.nsTestSetup = networkServerTestSetup;

        Test testSetup =
            SecurityManagerSetup.noSecurityManager(networkServerTestSetup);

        // if using the custom derby.properties, copy the custom properties to a visible place
        if ( customDerbyProperties != null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2435
            testSetup = new SupportFilesSetup
                (
                 testSetup,
                 null,
                 new String[] { "functionTests/tests/derbynet/SecureServerTest.derby.properties" },
                 null,
                 new String[] { "derby.properties" }
                 );
        }

        Test        test = TestConfiguration.defaultServerDecorator( testSetup );
        // DERBY-2109: add support for user credentials
        test = TestConfiguration.changeUserDecorator( test,
                                                      SST_USER_NAME,
                                                      SST_PASSWORD );

        return test;
    }

    /**
     * <p>
     * Return an array of startup args suitable for booting a server.
     * </p>
     */
    private static  String[]    getStartupArgs( boolean setUnsecureOption, String wildCardHost )
    {
        ArrayList<String> list = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        if ( setUnsecureOption )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2378
            list.add( "-noSecurityManager" );
        }
        
        if ( wildCardHost != null )
        {
            list.add( NetworkServerTestSetup.HOST_OPTION );
            list.add( wildCardHost );
        }

        return list.toArray(new String[list.size()]);
    }
    
    /**
     * <p>
     * Return a set of startup properties suitable for SystemPropertyTestSetup.
     * </p>
     */
    private static  String[]  getStartupProperties( boolean authenticationRequired, String customDerbyProperties )
    {
        ArrayList<String> list = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        if ( authenticationRequired )
        {
            list.add( "derby.connection.requireAuthentication=true" );
            list.add( "derby.authentication.provider=BUILTIN" );
            list.add( "derby.user." + SST_USER_NAME + "=" + SST_PASSWORD );
        }

        if ( customDerbyProperties != null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2435
            list.add( "derby.system.home=extinout" );
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return list.toArray(new String[list.size()]);
    }
    
    // Policy which lacks the permission to set the context class loader.
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745
    final static String POLICY6619 =
            "org/apache/derbyTesting/functionTests/" +
            "tests/derbynet/SecureServerTest.policy";

    private static Test makeDerby6619Test() {
        Test t = new SecureServerTest("test6619");
        t = TestConfiguration.clientServerDecorator(t);
        t = new SecurityManagerSetup(t, POLICY6619);
        t = new ClassLoaderTestSetup(t);
        return t;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    public void test6619() throws Exception {
        NetworkServerControl nsc =
                NetworkServerTestSetup.getNetworkServerControl();
        NetworkServerTestSetup.waitForServerStart(nsc);
         // non standard class loader, so expect to see the warning on derby.log
        assertWarningDerby6619("derby.system.home", true);
    }

    /**
     * Verify if the server came up and if so, was a security manager installed.
     */
    public void testServerStartup()
        throws Exception
    {	
        String      myName = toString();
        boolean     serverCameUp = serverCameUp();
//IC see: https://issues.apache.org/jira/browse/DERBY-6225
        String      serverOutput = getServerOutput();
        boolean     outputOK = ( serverOutput.indexOf( _outcome.expectedServerOutput() ) >= 0 );

        assertEquals( myName + ": serverCameUp = " + serverCameUp, _outcome.serverShouldComeUp(), serverCameUp );

        assertWarningDerby6619("user.dir", false); // standard class loader
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745

//IC see: https://issues.apache.org/jira/browse/DERBY-6067
        if (!(runsWithEmma() || runsWithJaCoCo())) {
            // With Emma we run without the security manager, so we can't
            // assert on seeing it.
//IC see: https://issues.apache.org/jira/browse/DERBY-5514
            assertTrue( myName + "\nExpected: " +
                        _outcome.expectedServerOutput() +
                        "\nBut saw: " + serverOutput , outputOK );
        }

        //
        // make sure that the default policy lets us connect to the server if the hostname was
        // wildcarded (DERBY-2811)
        //
        if ( _authenticationRequired && ( _wildCardHost != null ) ) { connectToServer(); }

        //
        // make sure that we can run sysinfo and turn on tracing (DERBY-3086)
        //
        runsysinfo();
        enableTracing();
//IC see: https://issues.apache.org/jira/browse/DERBY-3708
        setTraceDirectory();
        disableTracing();
        
        
    }

    private void disableTracing() throws Exception {

//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        String traceOffOutput = runServerCommand(
                new String[] { "trace", "off" });

        println( "Output for trace off command:\n\n" + traceOffOutput );

        if ( traceOffOutput.indexOf( "Trace turned off for all sessions." ) < 0 )
        { fail( "Failed to turn trace off:\n\n:" + traceOffOutput ); }
    }

    private void setTraceDirectory() throws Exception {

//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        String  traceDirectoryOutput = runServerCommand(
                new String[] { "tracedirectory", "trace" });
        println( "Output for tracedirectory trace command:\n\n" + traceDirectoryOutput );

        if ( traceDirectoryOutput.indexOf( "Trace directory changed to trace." ) < 0 )
        { fail( "Unexpected output in setting trace directory:" + traceDirectoryOutput ); }

        String pingOutput = runServerCommand( new String[] { "ping" } );

        if (pingOutput.indexOf("Connection obtained for host:") < 0)
        { fail ("Failed ping after changing trace directory: " + pingOutput);}
        assertTrue("directory trace does not exist",
                PrivilegedFileOpsForTests.exists(new File("trace")));
    }

    private void    connectToServer()
        throws Exception
    {
        final TestConfiguration config = getTestConfiguration();
        String  url
            = ( "jdbc:derby://localhost:" + config.getPort()
                + "/" + "wombat;create=true"
                + ";user=" + config.getUserName()
                + ";password=" + config.getUserPassword() );

        println( "XXX in connectToServer(). url = " + url );

        // just try to get a connection
        Class.forName( "org.apache.derby.jdbc.ClientDriver" );
        
        Connection  conn = DriverManager.getConnection(  url );

        assertNotNull( "Connection should not be null...", conn );

        conn.close();
    }

    private void    runsysinfo()
        throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        String          sysinfoOutput = runServerCommand(
                new String[] { "sysinfo" } );

        if ( sysinfoOutput.indexOf( "Security Exception:" ) > -1 )
        { fail( "Security exceptions in sysinfo output:\n\n:" + sysinfoOutput ); }
    }

    private void    enableTracing()
        throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        String          traceOnOutput = runServerCommand(
                new String[] { "trace",  "on" } );

        println( "Output for trace on command:\n\n" + traceOnOutput );

        if ( traceOnOutput.indexOf( "Trace turned on for all sessions." ) < 0 )
        { fail( "Security exceptions in output of trace enabling command:\n\n:" + traceOnOutput ); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Object OVERLOADS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        StringBuilder    buffer = new StringBuilder();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        buffer.append( "SecureServerTest( " );
        buffer.append( "Opened = " ); buffer.append( _unsecureSet);
        buffer.append( ", Authenticated= " ); buffer.append( _authenticationRequired );
        buffer.append( ", CustomDerbyProperties= " ); buffer.append( _customDerbyProperties );
        buffer.append( ", WildCardHost= " ); buffer.append( _wildCardHost );
        buffer.append( " )" );

        return buffer.toString();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Run a NetworkServerControl command.
     * </p>
     */
    private String    runServerCommand( String[] commandSpecifics )
        throws Exception
    {
        String          portNumber = Integer.toString( getTestConfiguration().getPort() );

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> cmdList = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        cmdList.add("-Demma.verbosity.level=silent");
        cmdList.add("org.apache.derby.drda.NetworkServerControl");
        cmdList.add("-p");
        cmdList.add(portNumber);
        cmdList.addAll(Arrays.asList(commandSpecifics));

        String[] cmd = (String[]) cmdList.toArray(commandSpecifics);

        Process serverProcess = execJavaCmd(cmd);
        
        SpawnedProcess spawned = new SpawnedProcess(serverProcess,
                cmdList.toString());
        
        // Ensure it completes without failures.
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
        assertEquals(0, spawned.complete());
        
        return spawned.getFullServerOutput();
    }

    private String  getServerOutput()
        throws Exception
    {
        return nsTestSetup.getServerProcess().getNextServerOutput();
    }

    private static  String  serverBootedOK()
    {
        return "Security manager installed using the Basic server security policy.";
    }

    private boolean serverCameUp()
        throws Exception
    {
        return NetworkServerTestSetup.pingForServerUp(
            NetworkServerTestSetup.getNetworkServerControl(),
//IC see: https://issues.apache.org/jira/browse/DERBY-3504
            nsTestSetup.getServerProcess().getProcess(), true);
    }

    final String[] expected6619 =
         new String[]{
             "WARNING: cannot set the context class loader due to a " +
                 "security exception:",
             "This may lead to class loader leak"};


    private void assertWarningDerby6619(String logLocation, boolean expected)
            throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
//IC see: https://issues.apache.org/jira/browse/DERBY-3745

        final String logFileName =
                getSystemProperty(logLocation) + File.separator + "derby.log";
        if (DerbyNetAutoStartTest.checkLog(logFileName, expected6619)) {
            if (!expected) {
                fail("Expected no warning on derby.log cf DERBY-6619");
            }
        } else {
            if (expected) {
                fail("Expected warning on derby.log cf DERBY-6619");
            }
        }
    }
}


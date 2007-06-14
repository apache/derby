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

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.ServerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derby.drda.NetworkServerControl;

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
    
    private static  final   String  DERBY_HOSTNAME_WILDCARD = "0.0.0.0";

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
    private boolean _useWildCardHost;

    // expected outcomes
    private Outcome _outcome;

    // helper state for intercepting server error messages
    private InputStream[]  _inputStreamHolder;

    
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
         boolean    useWildCardHost,

         Outcome    outcome
        )
    {
         super( "testServerStartup" );

         _unsecureSet =  unsecureSet;
         _authenticationRequired =  authenticationRequired;
         _customDerbyProperties = customDerbyProperties;
         _useWildCardHost = useWildCardHost;

         _outcome = outcome;

         _inputStreamHolder = new InputStream[ 1 ];
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
        
        TestSuite       suite = new TestSuite("SecureServerTest");

        // Server booting requires that we run from the jar files
        if ( !TestConfiguration.loadingFromJars() ) { return suite; }
        
        // Need derbynet.jar in the classpath!
        if (!Derby.hasServer())
            return suite;

        // O = Overriden
        // A = Authenticated
        // C = Custom properties
        // W = Use wildcard host
        //
        //      .addTest( decorateTest( O,        A,       C,    W,    Outcome ) );
        //

        suite.addTest( decorateTest( false,  false, null, false, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  false, BASIC, false, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  true, null, false, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( false,  true, null, true, RUNNING_SECURITY_BOOTED ) );
        suite.addTest( decorateTest( true,  false, null, false, RUNNING_SECURITY_NOT_BOOTED ) );
        suite.addTest( decorateTest( true,  true, null, false, RUNNING_SECURITY_NOT_BOOTED ) );
        
        return suite;
    }
    
    /**
     * Release resources.
     */
    protected void tearDown() throws Exception
    {
        _inputStreamHolder = null;
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
         boolean    useWildCardHost,
         
         Outcome outcome
        )
    {
        SecureServerTest            secureServerTest = new SecureServerTest
            (
             unsecureSet,
             authenticationRequired,
             customDerbyProperties,
             useWildCardHost,

             outcome
            );

        String[]        startupProperties = getStartupProperties( authenticationRequired, customDerbyProperties );
        String[]        startupArgs = getStartupArgs( unsecureSet, useWildCardHost );

        NetworkServerTestSetup networkServerTestSetup =
                new NetworkServerTestSetup
            (
             secureServerTest,
             startupProperties,
             startupArgs,
             true,
             secureServerTest._outcome.serverShouldComeUp(),
             secureServerTest._inputStreamHolder
             );

        secureServerTest.nsTestSetup = networkServerTestSetup;

        Test testSetup =
            SecurityManagerSetup.noSecurityManager(networkServerTestSetup);

        // if using the custom derby.properties, copy the custom properties to a visible place
        if ( customDerbyProperties != null )
        {
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

        return test;
    }

    /**
     * <p>
     * Return an array of startup args suitable for booting a server.
     * </p>
     */
    private static  String[]    getStartupArgs( boolean setUnsecureOption, boolean useWildCardHost )
    {
        ArrayList       list = new ArrayList();

        if ( setUnsecureOption )
        {
            list.add( "-noSecurityManager" );
        }
        
        if ( useWildCardHost )
        {
            list.add( NetworkServerTestSetup.HOST_OPTION );
            list.add( DERBY_HOSTNAME_WILDCARD );
        }
        
        String[]    result = new String[ list.size() ];

        list.toArray( result );

        return result;
    }
    
    /**
     * <p>
     * Return a set of startup properties suitable for SystemPropertyTestSetup.
     * </p>
     */
    private static  String[]  getStartupProperties( boolean authenticationRequired, String customDerbyProperties )
    {
        ArrayList       list = new ArrayList();

        if ( authenticationRequired )
        {
            list.add( "derby.connection.requireAuthentication=true" );
            list.add( "derby.authentication.provider=BUILTIN" );
            list.add( "derby.user." + SST_USER_NAME + "=" + SST_PASSWORD );
        }

        if ( customDerbyProperties != null )
        {
            list.add( "derby.system.home=extinout" );
        }

        String[]    result = new String[ list.size() ];

        list.toArray( result );

        return result;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Verify if the server came up and if so, was a security manager installed.
     */
    public void testServerStartup()
        throws Exception
    {	
        String      myName = toString();
        String      serverOutput = getServerOutput();
        boolean     serverCameUp = serverCameUp();
        boolean     outputOK = ( serverOutput.indexOf( _outcome.expectedServerOutput() ) >= 0 );

        assertEquals( myName + ": serverCameUp = " + serverCameUp, _outcome.serverShouldComeUp(), serverCameUp );
        
        assertTrue( myName + "\nExpected: " + _outcome.expectedServerOutput() + "\nBut saw: " + serverOutput , outputOK );

        //
        // make sure that the default policy lets us connect to the server if the hostname was
        // wildcarded (DERBY-2811)
        //
        if ( _authenticationRequired && _useWildCardHost ) { connectToServer(); }
    }

    private void    connectToServer()
        throws Exception
    {
        String  url =
            "jdbc:derby://localhost:" + getTestConfiguration().getPort() + "/" + "wombat;create=true" +
            ";user=" + SST_USER_NAME + ";password=" + SST_PASSWORD;

        println( "XXX in connectToServer(). url = " + url );

        // just try to get a connection
        Class.forName( "org.apache.derby.jdbc.ClientDriver" );
        
        Connection  conn = DriverManager.getConnection(  url );

        assertNotNull( "Connection should not be null...", conn );

        conn.close();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Object OVERLOADS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( "SecureServerTest( " );
        buffer.append( "Opened = " ); buffer.append( _unsecureSet);
        buffer.append( ", Authenticated= " ); buffer.append( _authenticationRequired );
        buffer.append( ", CustomDerbyProperties= " ); buffer.append( _customDerbyProperties );
        buffer.append( ", UsingWildCardHost= " ); buffer.append( _useWildCardHost );
        buffer.append( " )" );

        return buffer.toString();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private String  getServerOutput()
        throws Exception
    {
        byte[]          inputBuffer = new byte[ 1000 ];

        InputStream is = _inputStreamHolder[ 0 ];

        int             bytesRead = is.read( inputBuffer );

        return new String( inputBuffer, 0, bytesRead );
    }

    private static  String  serverBootedOK()
    {
        return "Security manager installed using the Basic server security policy.";
    }

    private boolean serverCameUp()
        throws Exception
    {
        return NetworkServerTestSetup.pingForServerStart(
            NetworkServerTestSetup.getNetworkServerControl(),
            nsTestSetup.getServerProcess());
    }

}


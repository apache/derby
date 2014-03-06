/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.LoginTimeoutTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Properties;
import javax.sql.DataSource;
import javax.sql.CommonDataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

import junit.framework.*;

import org.apache.derby.authentication.UserAuthenticator;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.JDBCClientSetup;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Test login timeouts.
 */

public class LoginTimeoutTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String[][]    SYSTEM_PROPERTIES =
    {
        { "derby.connection.requireAuthentication", "true" },
        { "derby.authentication.provider", LoginTimeoutTest.class.getName() + "$SluggishAuthenticator" },
    };

    private static  final   boolean SUCCEED = true;
    private static  final   boolean FAIL = false;

    private static  final   int LONG_TIMEOUT = 10;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      RUTH = "RUTH";
    private static  final   String      RUTH_PASSWORD = "RUTHPASSWORD";

    private static  final   String      LOGIN_TIMEOUT = "XBDA0";
    private static  final   String      LOGIN_FAILED = "08004";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** User authenticator which sleeps for a while */
    public  static  final   class   SluggishAuthenticator   implements  UserAuthenticator
    {
        public  static  boolean debugPrinting = false;
        
        private static  final   long    MILLIS_PER_SECOND = 1000L;
        
        public  static  long    secondsToSleep = 2;
        public  static  boolean returnValue = true;
        
        public  SluggishAuthenticator() {}
    
        public boolean authenticateUser
            (
             String userName,
             String userPassword,
             String databaseName,
             Properties info
             )
            throws SQLException
        {
            // sleepy...
            try {
                long    sleepTime = secondsToSleep * MILLIS_PER_SECOND;
                printText( "SluggishAuthenticator going to sleep for " + sleepTime + " milliseconds." );
                Thread.sleep( secondsToSleep * MILLIS_PER_SECOND );
                printText( "...SluggishAuthenticator waking up after " + sleepTime + " milliseconds." );
            } catch (Exception e) { throw new SQLException( e.getMessage(), e ); }

            // ...and vacuous.
            return returnValue;
        }
        
        private static  void    printText( String text )
        {
            if ( debugPrinting )
            {
                BaseTestCase.println( text );
            }
        }
    }

    /** Behavior shared by DataSource and DriverManager */
    public  static  interface   Connector
    {
        public  Connection  getConnection( String user, String password ) throws SQLException;

        public  void    setLoginTimeout( int seconds ) throws SQLException;
    }

    public  static  final   class   DriverManagerConnector  implements Connector
    {
        private BaseJDBCTestCase    _test;

        public  DriverManagerConnector( BaseJDBCTestCase test ) { _test = test; }

        public  Connection  getConnection( String user, String password ) throws SQLException
        {
            return _test.openDefaultConnection( user, password );
        }

        public  void    setLoginTimeout( int seconds ) { DriverManager.setLoginTimeout( seconds ); }

        public  String  toString()  { return "DriverManagerConnector"; }
    }
    
    public  static  final   class   DataSourceConnector  implements Connector
    {
        private CommonDataSource  _dataSource;

        public  DataSourceConnector( CommonDataSource dataSource )
        {
            _dataSource = dataSource;
        }

        public  Connection  getConnection( String user, String password ) throws SQLException
        {
            if ( _dataSource instanceof DataSource )
            {
                return ((DataSource) _dataSource).getConnection( user, password );
            }
            else if ( _dataSource instanceof ConnectionPoolDataSource )
            {
                return ((ConnectionPoolDataSource) _dataSource).getPooledConnection( user, password ).getConnection();
            }
            else if ( _dataSource instanceof XADataSource )
            {
                return ((XADataSource) _dataSource).getXAConnection( user, password ).getConnection();
            }
            else { throw new SQLException( "Unknown data source type: " + _dataSource.getClass().getName() ); }
        }

        public  void    setLoginTimeout( int seconds ) throws SQLException
        { _dataSource.setLoginTimeout( seconds ); }

        public  String  toString()
        {
            return "DataSourceConnector( " + _dataSource.getClass().getName() + " )";
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     *
     * Create a test with the given name.
     */
    public LoginTimeoutTest(String name) { super(name); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
        TestSuite   suite = new TestSuite();

        Test    embedded = new TestSuite( LoginTimeoutTest.class, "embedded LoginTimeoutTest" );
        embedded = TestConfiguration.singleUseDatabaseDecorator( embedded );
        embedded = new SystemPropertyTestSetup( embedded, systemProperties() );
        suite.addTest( embedded );

        if (Derby.hasServer() && Derby.hasClient()) {
            Test clientServer = new TestSuite(
                    LoginTimeoutTest.class, "client/server LoginTimeoutTest");
            clientServer =
                    TestConfiguration.singleUseDatabaseDecorator(clientServer);
            clientServer = new JDBCClientSetup(
                    clientServer, JDBCClient.DERBYNETCLIENT);
            clientServer = new NetworkServerTestSetup(clientServer,
                    systemPropertiesArray(), new String[]{}, true);
            suite.addTest(clientServer);
        }

        return suite;
    }
    private static  Properties  systemProperties()
    {
        Properties  props = new Properties();

        for ( int i = 0; i < SYSTEM_PROPERTIES.length; i++ )
        {
            String[]    raw = SYSTEM_PROPERTIES[ i ];

            props.put( raw[ 0 ], raw[ 1 ] );
        }

        return props;
    }
    private static  String[]    systemPropertiesArray()
    {
        String[]    result = new String[ SYSTEM_PROPERTIES.length ];

        for ( int i = 0; i < SYSTEM_PROPERTIES.length; i++ )
        {
            String[]    raw = SYSTEM_PROPERTIES[ i ];

            result[ i ] = raw[ 0 ] + "=" + raw[ 1 ];
        }

        return result;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Basic test of login timeouts.
     */
    public  void    testBasic() throws Exception
    {
        SluggishAuthenticator.debugPrinting = TestConfiguration.getCurrent().isVerbose();

        // make sure the database is created in order to eliminate asymmetries
        // in running the tests
        Connection  conn = openDefaultConnection( RUTH, RUTH_PASSWORD );
        conn.close();
        
        vetConnector( new DriverManagerConnector( this ), true );
        vetConnector( new DataSourceConnector( JDBCDataSource.getDataSource() ), true );
        vetConnector( new DataSourceConnector( J2EEDataSource.getConnectionPoolDataSource() ), true );
        vetConnector( new DataSourceConnector( J2EEDataSource.getXADataSource() ), true );

        if ( usingEmbedded() ) { vetExceptionPassthrough(); }
        if ( usingDerbyNetClient() ) { vetServerTimeouts(); }
    }
    private void    vetConnector( Connector connector, boolean shouldSucceed ) throws Exception
    {
        try {
            // sometimes this succeeds when we expect not, see DERBY-6250,
            // give more time to the slug sleep
            if (usingEmbedded())
                SluggishAuthenticator.secondsToSleep = 4;
            tryTimeout( connector, 1, FAIL && shouldSucceed );
            // set back.
            if (usingEmbedded())
                SluggishAuthenticator.secondsToSleep = 2;
            tryTimeout( connector, LONG_TIMEOUT, SUCCEED && shouldSucceed );
            tryTimeout( connector, 0, SUCCEED && shouldSucceed );
        }
        finally
        {
            // revert to default state
            connector.setLoginTimeout( 0 );
            // set sluggishauthenticator sleep back
            SluggishAuthenticator.secondsToSleep = 2;
        }
    }
    private void    tryTimeout( Connector connector, int timeout, boolean shouldSucceed ) throws Exception
    {
        println( "Setting timeout " + timeout + " on " + connector );
        connector.setLoginTimeout( timeout );

        tryTimeout( connector, shouldSucceed );
    }
    private void    tryTimeout( Connector connector, boolean shouldSucceed ) throws Exception
    {
        long    startTime = System.currentTimeMillis();
        
        try {
            Connection  conn = connector.getConnection( RUTH, RUTH_PASSWORD );
            println( "    Got a " + conn.getClass().getName() );
            conn.close();
            if ( !shouldSucceed )   
            {
                // sometimes the connect succeeds, see DERBY-6250. 
                // adding more details to fail message.
                long    duration = System.currentTimeMillis() - startTime;
                String message ="Should not have been able to connect! \n " +
                "        connector: " + connector +
                "        Experiment took " + duration + " milliseconds. \n " +
                "        seconds sleep time was: " + SluggishAuthenticator.secondsToSleep;
                fail( message ); 
            }
        }
        catch (SQLException se)
        {
            if ( shouldSucceed ) { fail( "Should have been able to connect!", se ); }

            assertTrue( "Didn't expect to see a " + se.getClass().getName(), (se instanceof SQLTimeoutException) );
            assertSQLState( LOGIN_TIMEOUT, se );
        }

        long    duration = System.currentTimeMillis() - startTime;

        println( "        Experiment took " + duration + " milliseconds." );
    }
    private void    vetExceptionPassthrough() throws Exception
    {
        try {
            println( "Verifying that exceptions are not swallowed by the embedded login timer." );
            // set a long timeout which we won't exceed
            DriverManager.setLoginTimeout( LONG_TIMEOUT );

            // tell the authenticator to always fail
            SluggishAuthenticator.returnValue = false;

            try {
                Connection conn = openDefaultConnection( RUTH, RUTH_PASSWORD );
                conn.close();
                fail( "Didn't expect to get a connection!" );
            }
            catch (SQLException se) { assertSQLState( LOGIN_FAILED, se ); }
        }
        finally
        {
            // return to default position
            DriverManager.setLoginTimeout( 0 );
            SluggishAuthenticator.returnValue = true;
        }
    }
    private void    vetServerTimeouts() throws Exception
    {
        println( "Verifying behavior when timeouts are also set on the server." );

        Connection  controlConnection = openDefaultConnection( RUTH, RUTH_PASSWORD );

        // create a procedure for changing the login timeout on the server
        String  createProc = 
            "create procedure setLoginTimeout( timeout int ) language java parameter style java no sql\n" +
            "external name '" + getClass().getName() + ".setLoginTimeout'";
        println( createProc );
        controlConnection.prepareStatement( createProc ).execute();
        createProc = 
                "create procedure setAuthenticatorSleep( seconds int ) language java parameter style java no sql\n" +
                "external name '" + getClass().getName() + ".setAuthenticatorSleep'";
        controlConnection.prepareStatement( createProc ).execute();
        println( createProc );

        Connector   connector = new DriverManagerConnector( this );

        vetServerTimeout( controlConnection, connector, 1, FAIL );
        vetServerTimeout( controlConnection, connector, LONG_TIMEOUT, SUCCEED );
        vetServerTimeout( controlConnection, connector, 0, SUCCEED );

        // reset server timeout to default
        setServerTimeout( controlConnection, 0 );
        controlConnection.close();
    }
    private void    vetServerTimeout
        ( Connection controlConnection, Connector connector, int serverTimeout, boolean shouldSucceed )
        throws Exception
    {
        setServerTimeout( controlConnection, serverTimeout );
        // Sometimes we get an unexpected connection when we expect
        // the timeout to work, see DERBY-6250.
        // Setting the sleep Authenticator sleep time longer on the server.
        // for those cases to make the chance of this occurring smaller.
        if (!shouldSucceed)
            setServerAuthenticatorSleep(controlConnection, 4);
        else 
            setServerAuthenticatorSleep(controlConnection, 2);
        vetConnector( connector, shouldSucceed );
    }
    private void    setServerTimeout( Connection conn, int seconds ) throws Exception
    {
        CallableStatement   cs = conn.prepareCall( "call setLoginTimeout( ? )" );
        cs.setInt( 1, seconds );
        cs.execute();
        cs.close();
    }
    
    private void    setServerAuthenticatorSleep( Connection conn, int seconds )
            throws Exception
    {
        CallableStatement   cs = conn.prepareCall( "call setAuthenticatorSleep( ? )" );
        cs.setInt( 1, seconds );
        cs.execute();
        cs.close();
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Routine to set the DriverManager login timeout on the server */
    public  static  void    setLoginTimeout( int seconds ) throws Exception
    {
        DriverManager.setLoginTimeout( seconds );
    }
    
    /** Routine to set the SluggishAuthenticator Sleep 
     *  time on the server */
    public  static  void    setAuthenticatorSleep( int seconds ) throws Exception
    {
        SluggishAuthenticator.secondsToSleep = seconds ;
    }
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NativeAuthenticationServiceTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabaseChangeSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

/**
 * <p>
 * Tests for the NATIVE authentication service introduced by DERBY-866.
 * </p>
 */
public class NativeAuthenticationServiceTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // fruits are legal users. nuts are not
    private static  final   String  DBO = "KIWI";   
    private static  final   String  APPLE_USER = "APPLE";   
    private static  final   String  PEAR_USER = "PEAR";   

    private static  final   String  WALNUT_USER = "WALNUT";

    private static  final   String  CREDENTIALS_DB = "credDB";
    private static  final   String  SECOND_DB = "secondDB";
    private static  final   String  THIRD_DB = "thirdDB";

    private static  final   String  PROVIDER_PROPERTY = "derby.authentication.provider";

    private static  final   String  CREDENTIALS_DB_DOES_NOT_EXIST = "4251I";
    private static  final   String  INVALID_AUTHENTICATION = "08004";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private final   boolean _nativeAuthentication;
    private final   boolean _localAuthentication;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  NativeAuthenticationServiceTest
        (
         boolean    nativeAuthentication,
         boolean    localAuthentication
         )
    {
        super( "testAll" );

        _nativeAuthentication = nativeAuthentication;
        _localAuthentication = localAuthentication;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SETUP BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Return the system properties to be used in a particular test run.
     * </p>
     */
    private Properties  systemProperties( String physicalDatabaseName )
    {
        if ( !_nativeAuthentication ) { return null; }

        String  authenticationProvider = "NATIVE:" + physicalDatabaseName;
        if ( _localAuthentication ) { authenticationProvider = authenticationProvider + ":LOCAL"; }

        Properties  result = new Properties();
        result.put( PROVIDER_PROPERTY, authenticationProvider );

        return result;
    }

    /**
     * <p>
     * Construct the name of this test (useful for error messages).
     * </p>
     */
    private String  nameOfTest()
    {
        String  authType = _nativeAuthentication ?
            "NATIVE authentication on, " :
            "Authentication off, ";
        String  local = _localAuthentication ?
            "LOCAL authentication ON" :
            "LOCAL authentication OFF";

        return "[ " + authType + local + " ]";
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite();

        suite.addTest( allConfigurations( false ) );
        if ( !JDBC.vmSupportsJSR169() ) { suite.addTest( allConfigurations( true ) ); }

        return suite;
    }

    /**
     * <p>
     * Create a suite of all test configurations.
     * </p>
     */
    private static  Test   allConfigurations( boolean clientServer )
    {
        TestSuite suite = new TestSuite();

        suite.addTest( decorate( new NativeAuthenticationServiceTest( false, false ), clientServer ) );
        suite.addTest( decorate( new NativeAuthenticationServiceTest( true, true ), clientServer ) );
        suite.addTest( decorate( new NativeAuthenticationServiceTest( true, false ), clientServer ) );

        return suite;
    }

    /**
     * <p>
     * Wrap base test with standard decorators in order to setup system
     * properties and allow for the creation of multiple databases with
     * stored properties that can't be removed at tearDown time.
     * </p>
     */
    private static  Test    decorate( NativeAuthenticationServiceTest nast, boolean clientServer )
    {
        String      credentialsDBPhysicalName = TestConfiguration.generateUniqueDatabaseName();
        
        Test        result = nast;

        //
        // Putting the clientServer decorator on the inside allows the server-side
        // embedded driver to be re-registered after engine shutdown. If you put
        // this decorator outside the SystemProperty decorator, then engine shutdown
        // unregisters the server-side embedded driver and it can't be found by
        // the next test.
        //
        if ( clientServer ) { result = TestConfiguration.clientServerDecorator( result ); }
        
        //
        // Turn on the property which enables NATIVE authentication. This will trigger
        // an engine shutdown at the end of the test. We want to shutdown the engine
        // before deleting the physical databases. This is because we need one of the
        // databases (the credentials db) in order to authenticate engine shutdown.
        //
        Properties  systemProperties = nast.systemProperties( credentialsDBPhysicalName );
        if ( systemProperties != null )
        {
            result = new SystemPropertyTestSetup( result, systemProperties, true );
        }
        
        //
        // Register temporary databases, where the test will do its work.
        // We can't use the default, re-usable database because NATIVE authentication stores
        // persistent properties which cannot be turned off.
        //
        result = TestConfiguration.additionalDatabaseDecoratorNoShutdown
            ( result, CREDENTIALS_DB, credentialsDBPhysicalName );
        result = TestConfiguration.additionalDatabaseDecoratorNoShutdown( result, SECOND_DB );
        result = TestConfiguration.additionalDatabaseDecoratorNoShutdown( result, THIRD_DB );

        result = TestConfiguration.changeUserDecorator( result, DBO, getPassword( DBO ) );
        
        return result;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Entry point for tests.
     * </p>
     */
    public  void    testAll()   throws Exception
    {
        println( nameOfTest() );

        // can't create any database until the credentials db has been created
        Connection  secondDBConn = createDB
            ( _nativeAuthentication, SECOND_DB, APPLE_USER, CREDENTIALS_DB_DOES_NOT_EXIST );

        // create the credentials database
        Connection  sysadminConn = openConnection( CREDENTIALS_DB, DBO );

        // add another legal user
        addUser( sysadminConn, APPLE_USER );

        //
        // Creating the credentials db should have stored the following information in it:
        //
        // 1) The DBO's credentials should have been stored in SYSUSERS.
        // 2) The authentication provider should have been set to NATIVE::LOCAL
        // 3) SQL authorization should have been turned on.
        //
        String[][]  legalUsers = _nativeAuthentication ? new String[][] { { APPLE_USER }, { DBO } } : new String[][] {  { APPLE_USER } };
        assertResults
            (
             sysadminConn,
             "select username from sys.sysusers order by username",
             legalUsers,
             false
             );
        String[][]  authenticationProvider = _nativeAuthentication ? new String[][] { { "NATIVE::LOCAL" } } : new String[][] { { null } };
        assertResults
            (
             sysadminConn,
             "values ( syscs_util.syscs_get_database_property( 'derby.authentication.provider' ) )",
             authenticationProvider,
             false
             );
        String[][]  sqlAuthorization = _nativeAuthentication ? new String[][] { { "true" } } : new String[][] { { null } };
        assertResults
            (
             sysadminConn,
             "values ( syscs_util.syscs_get_database_property( 'derby.database.sqlAuthorization' ) )",
             sqlAuthorization,
             false
             );

        // Sanity-check that the creator of the credentials db is the DBO
        String[][]   dboName = new String[][] { { DBO } };
        assertResults
            (
             sysadminConn,
             "select authorizationID from sys.sysschemas where schemaName = 'SYS'",
             dboName,
             false
             );

        // Databases can't be created by users who don't have credentials stored in the credentials database
        Connection  thirdDBConn = createDB
            ( _nativeAuthentication, THIRD_DB, WALNUT_USER, INVALID_AUTHENTICATION );

        // Now let the other valid user create a database
        if ( secondDBConn == null )
        {
            secondDBConn = createDB( false, SECOND_DB, APPLE_USER, null );
        }

        // verify that the other valid user is the dbo in the database he just created
        assertResults
            (
             secondDBConn,
             "select authorizationID from sys.sysschemas where schemaName = 'SYS'",
             new String[][] { { APPLE_USER } },
             false
             );

        // NATIVE authentication turns on SQL authorization in the second database
        assertResults
            (
             secondDBConn,
             "values ( syscs_util.syscs_get_database_property( 'derby.database.sqlAuthorization' ) )",
             sqlAuthorization,
             false
             );

        //
        // If LOCAL authentication was specified...
        //
        // 1) It will be turned on in the second database too.
        // 2) The other legal user's credentials (as the database dbo) will be stored.
        //
        authenticationProvider = _localAuthentication ? new String[][] { { "NATIVE::LOCAL" } } : new String[][] { { null } };
        assertResults
            (
             secondDBConn,
             "values ( syscs_util.syscs_get_database_property( 'derby.authentication.provider' ) )",
             authenticationProvider,
             false
             );
        legalUsers = _localAuthentication ? new String[][] { { APPLE_USER } } : new String[][] {};
        assertResults
            (
             secondDBConn,
             "select username from sys.sysusers order by username",
             legalUsers,
             false
             );
        
    }

    private Connection  createDB( boolean shouldFail, String dbName, String user, String expectedSQLState )
        throws Exception
    {
        Connection  conn = null;

        println( user + " attempting to create database " + dbName );

        try {
            conn = openConnection( dbName, user );

            if ( shouldFail )   { fail( tagError( "Connection to " + dbName + " should have failed." ) ); }
        }
        catch (SQLException se)
        {
            if ( shouldFail )   { assertSQLState( expectedSQLState, se ); }
            else    { fail( tagError( "Connection to " + dbName + " unexpectedly succeeded." ) );}
        }

        return conn;
    }

    private void    addUser( Connection conn, String user ) throws Exception
    {
        String  password = getPassword( user );
        String  statement = "call syscs_util.syscs_create_user( '" + user + "', '" + password + "' )";
        
        goodStatement( conn, statement );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Open a connection to a database using the supplied credentials */
    private Connection  openConnection( String logicalDBName, String user )
        throws SQLException
    {
        return getTestConfiguration().openConnection( logicalDBName, user, getPassword( user ) );
    }
    
    /** Get the password for a user */
    private static  String  getPassword( String user ) { return user + "_password"; }

    /** Tag an error with the name of the test configuration */
    private String  tagError( String text ) { return nameOfTest() + ": " + text; }

}

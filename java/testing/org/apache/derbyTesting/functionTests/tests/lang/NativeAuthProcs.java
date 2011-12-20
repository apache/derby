/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NativeAuthProcs

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

import java.io.CharArrayReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Tests the behavior of the system procedures which support NATIVE authentication.
 */
public class NativeAuthProcs extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      JANET = "JANET";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, JANET };

    private static  final   String      NO_EXECUTE_PERMISSION = "42504";
    private static  final   String      DUPLICATE_USER = "X0Y68";
    private static  final   String      CANT_DROP_DBO = "4251F";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Create a new instance.
     */

    public NativeAuthProcs( String name )
    {
        super( name );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OVERRIDABLE BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  boolean authorizationIsOn() { return true; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   NoAuthorization extends NativeAuthProcs
    {
        public NoAuthorization( String name )
        {
            super( name );
        }

        public  boolean authorizationIsOn() { return false; }
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

        suite.addTest( makeCollations() );
        suite.addTest( TestConfiguration.clientServerDecorator( makeCollations() ) );

        return suite;
    }
    private static  Test    makeCollations()
    {
        TestSuite   suite = new TestSuite();
        suite.addTest( makeUncollated() );
        suite.addTest( makeCollated() );

        return suite;
    }
    private static  Test    makeUncollated()
    {
        TestSuite   suite = new TestSuite();
        suite.addTest( makeAuthorized() );
        suite.addTest( makeUnauthorized() );

        return suite;
    }
    private static  Test    makeCollated()
    {
        TestSuite   suite = new TestSuite();
        suite.addTest( Decorator.territoryCollatedDatabase( makeAuthorized(), "en" ) );
        suite.addTest( Decorator.territoryCollatedDatabase( makeUnauthorized(), "en" ) );

        return suite;
    }
    private static  Test    makeAuthorized()
    {
        Test    authorizedRun = new NativeAuthProcs( "testAll" );
        authorizedRun = new CleanDatabaseTestSetup( authorizedRun );
        authorizedRun = DatabasePropertyTestSetup.builtinAuthentication
            ( authorizedRun, LEGAL_USERS, "authorizationOnDBOAccessTest" );
        authorizedRun = TestConfiguration.sqlAuthorizationDecorator( authorizedRun );

        return authorizedRun;
    }
    private static  Test    makeUnauthorized()
    {
        Test    unauthorizedRun = new NoAuthorization( "testAll" );

        return new CleanDatabaseTestSetup( unauthorizedRun );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Master test entry point.
     * </p>
     */
    public  void    testAll() throws Exception
    {
        println( "authorizationIsOn() = " + authorizationIsOn() );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        if ( !dboExists( dboConnection ) )
        {
            goodStatement( dboConnection, "call syscs_util.syscs_create_user( 'TEST_DBO', 'test_dbopassword' )" );
        }
        goodStatement( dboConnection, "call syscs_util.syscs_create_user( 'JANET', 'janetpassword' )" );

        createUserTests( dboConnection, janetConnection );
        resetPasswordTests( dboConnection, janetConnection );
        modifyPasswordTests( dboConnection, janetConnection );
        if ( authorizationIsOn() ) { grantRevokeTests( dboConnection, janetConnection ); }
    }
    private boolean dboExists( Connection conn )
        throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement( "select username from sys.sysusers" );
        ResultSet   rs = ps.executeQuery();

        try {
            while ( rs.next() )
            {
                if ( rs.getString( 1 ).equals( "TEST_DBO" ) ) { return true; }
            }
        }
        finally
        {
            rs.close();
            ps.close();
        }

        return false;
    }


    //
    // Create/Drop User
    //
    
    private void    createUserTests
        ( Connection dboConnection, Connection janetConnection )
        throws Exception
    {
        vetCreateDropUser( dboConnection, true );
        vetCreateDropUser( janetConnection, !authorizationIsOn() );

        // Make sure that we can create a user in the approved fashion.
        char[]  password = new char[] { 'r','u','t','h','p','a','s','s','w','o','r','d' };
        CharArrayReader reader = new CharArrayReader( password );

        CallableStatement cs = dboConnection.prepareCall( "call syscs_util.syscs_create_user( 'ruth', ? )" );
        cs.setCharacterStream( 1, reader, password.length );
        cs.execute();
        cs.close();
        Arrays.fill( password, (char) 0 );
        
        vetQuery
            (
             dboConnection, true,
             "select username from sys.sysusers order by  username",
             new String[][]
             {
                 new String[] { "JANET" },
                 new String[] { "TEST_DBO" },
                 new String[] { "ruth" },
             },
             true, null
             );

        // ok, now drop the new user
        goodStatement( dboConnection, "call syscs_util.syscs_drop_user( 'ruth' )" );
    }
    private void    vetCreateDropUser( Connection conn, boolean shouldSucceed )
        throws Exception
    {
        vetExecution( conn, shouldSucceed, "call syscs_util.syscs_create_user( 'fred', 'fredpassword' )", NO_EXECUTE_PERMISSION );
        vetQuery
            (
             conn, shouldSucceed,
             "select username from sys.sysusers order by  username",
             new String[][]
             {
                 new String[] { "JANET" },
                 new String[] { "TEST_DBO" },
                 new String[] { "fred" },
             },
             true, "4251D"
             );
        vetExecution
            (
             conn, false, "call syscs_util.syscs_create_user( 'fred', 'fredpassword' )",
             shouldSucceed ? DUPLICATE_USER : NO_EXECUTE_PERMISSION
             );
        vetExecution( conn, shouldSucceed, "call syscs_util.syscs_drop_user( 'fred' )", NO_EXECUTE_PERMISSION );
        vetQuery
            (
             conn, shouldSucceed,
             "select username from sys.sysusers order by  username",
             new String[][]
             {
                 new String[] { "JANET" },
                 new String[] { "TEST_DBO" },
             },
             true, "4251D"
             );

        // no-one can drop the credentials of the DBO
        String  dbo = authorizationIsOn() ? "TEST_DBO" : "APP";
        expectExecutionError
            (
             conn, shouldSucceed ? CANT_DROP_DBO : NO_EXECUTE_PERMISSION,
             "call syscs_util.syscs_drop_user( '" + dbo + "' )"
             );
    }
    private void    vetExecution
        ( Connection conn, boolean shouldSucceed, String query, String expectedSQLState )
        throws Exception
    {
        if ( shouldSucceed ) { goodStatement( conn, query ); }
        else    { expectExecutionError( conn, expectedSQLState, query ); }
    }
    private void    vetQuery
        (
         Connection conn,
         boolean shouldSucceed,
         String query,
         String[][] expectedResults,
         boolean compileTimeError,
         String expectedSQLState
         )
        throws Exception
    {
        if ( shouldSucceed )    { assertResults( conn, query, expectedResults, true ); }
        else
        {
            if ( compileTimeError ) { expectCompilationError( conn, expectedSQLState, query ); }
            else { expectExecutionError( conn, expectedSQLState, query ); }
        }
    }

    //
    // Reset Password
    //
    
    private void    resetPasswordTests
        ( Connection dboConnection, Connection janetConnection )
        throws Exception
    {
        goodStatement( dboConnection, "call syscs_util.syscs_create_user( 'resetuser', 'resetuserpassword_rev1' )" );

        long lastModified = getLastModified( dboConnection );

        lastModified = vetResetPassword( dboConnection, dboConnection, lastModified, true );
        lastModified = vetResetPassword( dboConnection, janetConnection, lastModified, !authorizationIsOn() );
                 
        // Make sure that we can reset a password in the approved fashion.
        char[]  password = new char[] { 'r','u','t','h','p','a','s','s','w','o','r','d' };
        CharArrayReader reader = new CharArrayReader( password );

        CallableStatement cs = dboConnection.prepareCall( "call syscs_util.syscs_reset_password( 'resetuser', ? )" );
        cs.setCharacterStream( 1, reader, password.length );
        cs.execute();
        cs.close();
        Arrays.fill( password, (char) 0 );
        
        long    newLastModified = getLastModified( dboConnection );
        assertTrue( newLastModified > lastModified );

        goodStatement( dboConnection, "call syscs_util.syscs_drop_user( 'resetuser' )" );
    }
    private long    vetResetPassword( Connection dboConnection, Connection conn, long oldLastModified, boolean shouldSucceed )
        throws Exception
    {
        // pause so that when we check timestamps, we will see a change
        Thread.sleep( 1L );
        
        vetExecution
            (
             conn, shouldSucceed,
             "call syscs_util.syscs_reset_password( 'resetuser', 'resetuserpassword_rev2' )",
             NO_EXECUTE_PERMISSION
             );

        long    newLastModified = getLastModified( dboConnection );
        if ( shouldSucceed ) { assertTrue( newLastModified > oldLastModified ); }
        else { assertTrue( newLastModified == oldLastModified ); }

        return newLastModified;
    }
    private long    getLastModified( Connection conn )
        throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement( "select max( lastmodified ) from sys.sysusers" );
        ResultSet rs = ps.executeQuery();

        rs.next();

        long    result = rs.getTimestamp( 1 ).getTime();

        rs.close();
        ps.close();

        return result;
    }
    
    //
    // Modify Password
    //
    
    private void    modifyPasswordTests
        ( Connection dboConnection, Connection janetConnection )
        throws Exception
    {
        long    lastModified = getLastModified( dboConnection );

        lastModified = vetModifyPassword( dboConnection, dboConnection, lastModified );
        lastModified = vetModifyPassword( dboConnection, janetConnection, lastModified );
    }
    private long    vetModifyPassword( Connection dboConnection, Connection conn, long oldLastModified )
        throws Exception
    {
        // pause so that when we check timestamps, we will see a change
        Thread.sleep( 1L );
        
        goodStatement( conn, "call syscs_util.syscs_modify_password( 'newpassword' )" );
                       
        long    newLastModified = getLastModified( dboConnection );
        assertTrue( newLastModified > oldLastModified );

        // Make sure that we can modify a password in the approved fashion.
        char[]  password = new char[] { 'r','u','t','h','p','a','s','s','w','o','r','d' };
        CharArrayReader reader = new CharArrayReader( password );

        CallableStatement cs = conn.prepareCall( "call syscs_util.syscs_modify_password( ? )" );
        cs.setCharacterStream( 1, reader, password.length );
        cs.execute();
        cs.close();
        Arrays.fill( password, (char) 0 );
        
        long    newerLastModified = getLastModified( dboConnection );
        assertTrue( newerLastModified > newLastModified );

        return newerLastModified;
    }
    
    //
    // Grant/Revoke
    //
    private void    grantRevokeTests
        ( Connection dboConnection, Connection janetConnection )
        throws Exception
    {
        goodStatement( dboConnection, "grant execute on procedure syscs_util.syscs_create_user to JANET" );
        goodStatement( dboConnection, "grant execute on procedure syscs_util.syscs_reset_password to JANET" );
        goodStatement( dboConnection, "grant execute on procedure syscs_util.syscs_drop_user to JANET" );

        goodStatement( janetConnection, "call syscs_util.syscs_create_user( 'JOE', 'joepassword' )" );
        goodStatement( janetConnection, "call syscs_util.syscs_reset_password( 'JOE', 'joepassword_rev3' )" );
        goodStatement( janetConnection, "call syscs_util.syscs_drop_user( 'JOE' )" );

        goodStatement( dboConnection, "revoke execute on procedure syscs_util.syscs_create_user from JANET restrict" );
        goodStatement( dboConnection, "revoke execute on procedure syscs_util.syscs_reset_password from JANET restrict" );
        goodStatement( dboConnection, "revoke execute on procedure syscs_util.syscs_drop_user from JANET restrict" );
        
        expectExecutionError
            ( janetConnection, NO_EXECUTE_PERMISSION, "call syscs_util.syscs_create_user( 'JOE', 'joepassword' )" );
        expectExecutionError
            ( janetConnection, NO_EXECUTE_PERMISSION, "call syscs_util.syscs_reset_password( 'JOE', 'joepassword_rev3' )" );
        expectExecutionError
            ( janetConnection, NO_EXECUTE_PERMISSION, "call syscs_util.syscs_drop_user( 'JOE' )" );
    }
    
}

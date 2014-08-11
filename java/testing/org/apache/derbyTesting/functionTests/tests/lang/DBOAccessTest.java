/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DBOAccessTest

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derby.catalog.SystemProcedures;

/**
 * Tests that certain operations can only be performed by the DBO.
 */
public class DBOAccessTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      JANET = "JANET";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, JANET };

    // Name of the log file to use when testing VTIs that expect one.
    private static final String TEST_LOG_FILE = "sys_vti_test_derby.tstlog";

    private static  final   String      ONLY_DBO = "4251D";
    private static  final   String      HIDDEN_COLUMN = "4251E";
    private static  final   String      NULL_BACKUP_DIRECTORY = "XSRS6";
    private static  final   String      FIRST_CREDENTIALS = "4251K";
    private static  final   String      MISSING_OBJECT = "X0X13";
    private static  final   String      MISSING_TABLE = "42X05";
    private static  final   String      NO_SUCH_TABLE = "XIE0M";
    private static  final   String      UNKNOWN_USER = "XK001";
    private static  final   String      SQLJ_INVALID_JAR = "46001";

    private static  final   String      SYSCS_SET_DATABASE_PROPERTY = "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY";
    private static  final   String      SYSCS_GET_DATABASE_PROPERTY = "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY";
    private static  final   String      SYSCS_FREEZE_DATABASE = "SYSCS_UTIL.SYSCS_FREEZE_DATABASE";
    private static  final   String      SYSCS_UNFREEZE_DATABASE = "SYSCS_UTIL.SYSCS_UNFREEZE_DATABASE";
    private static  final   String      SYSCS_CHECKPOINT_DATABASE = "SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE";
    private static  final   String      SYSCS_BACKUP_DATABASE = "SYSCS_UTIL.SYSCS_BACKUP_DATABASE";
    private static  final   String      SYSCS_BACKUP_DATABASE_NOWAIT = "SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT";
    private static  final   String      SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE = "SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE";
    private static  final   String      SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT = "SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT";
    private static  final   String      SYSCS_DISABLE_LOG_ARCHIVE_MODE = "SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE";
    private static  final   String      SYSCS_CHECK_TABLE = "SYSCS_UTIL.SYSCS_CHECK_TABLE";
    private static  final   String      INSTALL_JAR = "SQLJ.INSTALL_JAR";
    private static  final   String      REPLACE_JAR = "SQLJ.REPLACE_JAR";
    private static  final   String      REMOVE_JAR = "SQLJ.REMOVE_JAR";
    private static  final   String      SYSCS_EXPORT_TABLE = "SYSCS_UTIL.SYSCS_EXPORT_TABLE";
    private static  final   String      SYSCS_IMPORT_TABLE = "SYSCS_UTIL.SYSCS_IMPORT_TABLE";
    private static  final   String      SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE = "SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE";
    private static  final   String      SYSCS_IMPORT_DATA = "SYSCS_UTIL.SYSCS_IMPORT_DATA";
    private static  final   String      SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE = "SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE";
    private static  final   String      SYSCS_BULK_INSERT = "SYSCS_UTIL.SYSCS_BULK_INSERT";
    private static  final   String      SYSCS_RELOAD_SECURITY_POLICY = "SYSCS_UTIL.SYSCS_RELOAD_SECURITY_POLICY";
    private static  final   String      SYSCS_SET_USER_ACCESS = "SYSCS_UTIL.SYSCS_SET_USER_ACCESS";
    private static  final   String      SYSCS_GET_USER_ACCESS = "SYSCS_UTIL.SYSCS_GET_USER_ACCESS";
    private static  final   String      SYSCS_INVALIDATE_STORED_STATEMENTS = "SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS";
    private static  final   String      SYSCS_EMPTY_STATEMENT_CACHE = "SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE";
    private static  final   String      SYSCS_SET_XPLAIN_MODE = "SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE";
    private static  final   String      SYSCS_GET_XPLAIN_MODE = "SYSCS_UTIL.SYSCS_GET_XPLAIN_MODE";
    private static  final   String      SYSCS_SET_XPLAIN_SCHEMA = "SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA";
    private static  final   String      SYSCS_GET_XPLAIN_SCHEMA = "SYSCS_UTIL.SYSCS_GET_XPLAIN_SCHEMA";
    private static  final   String      SYSCS_CREATE_USER = "SYSCS_UTIL.SYSCS_CREATE_USER";
    private static  final   String      SYSCS_RESET_PASSWORD = "SYSCS_UTIL.SYSCS_RESET_PASSWORD";
    private static  final   String      SYSCS_DROP_USER = "SYSCS_UTIL.SYSCS_DROP_USER";

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

    public DBOAccessTest( String name )
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

    public  static  final   class   NoAuthorization extends DBOAccessTest
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
        String[]    testFiles = new String[] { "functionTests/tests/lang/" + TEST_LOG_FILE };
        
        Test    authorizedRun = TestConfiguration.embeddedSuite( DBOAccessTest.class );
        authorizedRun = DatabasePropertyTestSetup.builtinAuthentication
            ( authorizedRun, LEGAL_USERS, "authorizationOnDBOAccessTest" );
        authorizedRun = new SupportFilesSetup( authorizedRun, testFiles );
        authorizedRun = TestConfiguration.sqlAuthorizationDecorator( authorizedRun );

        Test    unauthorizedRun = TestConfiguration.embeddedSuite( NoAuthorization.class );
        unauthorizedRun = new SupportFilesSetup( unauthorizedRun, testFiles );

        BaseTestSuite suite = new BaseTestSuite();
        suite.addTest( authorizedRun );
        suite.addTest( unauthorizedRun );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Tests that only the DBO can run diagnostic VTIs which return sensitive information.
     * See DERBY-5395.
     * </p>
     */
    public  void    test_5395() throws Exception
    {
        println( "authorizationIsOn() = " + authorizationIsOn() );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        minion_5395( dboConnection, true );
        minion_5395( janetConnection, !authorizationIsOn() );
    }
    private void    minion_5395( Connection conn, boolean shouldSucceed ) throws Exception
    {
        vet_5395( conn, shouldSucceed, "select * from syscs_diag.statement_cache" );
        vet_5395( conn, shouldSucceed, "select * from syscs_diag.transaction_table" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.error_log_reader( ) ) s" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.statement_duration() ) s" );
        
        java.net.URL logURL = SupportFilesSetup.getReadOnlyURL( TEST_LOG_FILE );
        String vtiArg = "'" + logURL.getFile() + "'";

        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.error_log_reader( " + vtiArg + " ) ) s" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.statement_duration( " + vtiArg + " ) ) s" );
    }
    private void    vet_5395( Connection conn, boolean shouldSucceed, String query ) throws Exception
    {
        if ( shouldSucceed ) { goodStatement( conn, query ); }
        else
        {
            expectCompilationError( conn, ONLY_DBO, query );
        }
    }

    /**
     * <p>
     * Tests that only the DBO can select from SYSUSERS and no-one can SELECT
     * SYSUSERS.PASSWORD.
     * </p>
     */
    public  void    testSYSUSERS() throws Exception
    {
        println( "testSYSUSERS authorizationIsOn() = " + authorizationIsOn() );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        goodStatement( dboConnection, "create view v2 as select username, hashingscheme, lastmodified from sys.sysusers" );
        if ( authorizationIsOn() ) { goodStatement( dboConnection, "grant select on v2 to public" ); }

        goodStatement( dboConnection, "create view v3 as select username, hashingscheme, lastmodified from sys.sysusers where password is null" );
        if ( authorizationIsOn() ) { goodStatement( dboConnection, "grant select on v3 to public" ); }

        vetDBO_OKProbes( dboConnection, true );
        vetDBO_OKProbes( janetConnection, !authorizationIsOn() );

        vetUnauthorizedProbes( dboConnection, !authorizationIsOn(), HIDDEN_COLUMN );
        vetUnauthorizedProbes( janetConnection, !authorizationIsOn(), ONLY_DBO );
    }
    // these statements should always succeed if the dbo is running
    // them or if authorization is not enabled
    private void    vetDBO_OKProbes( Connection conn, boolean shouldSucceed )
        throws Exception
    {
        vetUserProbes( conn, shouldSucceed, "select count(*) from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, hashingscheme, lastmodified from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, lastmodified from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, lastmodified from sys.sysusers where username = 'FRED'", ONLY_DBO );

        // can't use views to subvert authorization checks
        vetUserProbes( conn, shouldSucceed, "select count(*) from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select * from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, hashingscheme, lastmodified from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username from test_dbo.v2", ONLY_DBO );
    }
    // these statements should always fail if authorization is enabled
    private void    vetUnauthorizedProbes( Connection conn, boolean shouldSucceed, String expectedSQLState )
        throws Exception
    {
        vetUserProbes( conn, shouldSucceed, "select * from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select * from sys.sysusers where username='foo'", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select password from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select username, password from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select username from sys.sysusers where password = 'foo'", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select username, lastmodified from sys.sysusers where password is not null", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select * from test_dbo.v3", expectedSQLState );
    }
    private void    vetUserProbes
        ( Connection conn, boolean shouldSucceed, String query, String expectedSQLState )
        throws Exception
    {
        if ( shouldSucceed ) { goodStatement( conn, query ); }
        else
        {
            expectCompilationError( conn, expectedSQLState, query );
        }
    }

    /**
     * <p>
     * Tests that you can't subvert sql authorization by directly calling
     * the entry points in SystemProcedures.
     * </p>
     */
    public  void    test_6616() throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        goodStatement
            (
             dboConnection,
             "create procedure runSystemRoutine( routineName varchar( 32672 ) )\n" +
             "language java parameter style java modifies sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.DBOAccessTest.runSystemRoutine'\n"
             );
        if ( authorizationIsOn() )
        {
            goodStatement
                (
                 dboConnection,
                 "grant execute on procedure runSystemRoutine to public"
                 );
        }

        vet6616( dboConnection, janetConnection, SYSCS_SET_DATABASE_PROPERTY, false );
        vet6616( dboConnection, janetConnection, SYSCS_GET_DATABASE_PROPERTY, true );
        vet6616( dboConnection, janetConnection, SYSCS_FREEZE_DATABASE, false );
        vet6616( dboConnection, janetConnection, SYSCS_UNFREEZE_DATABASE, false );
        vet6616( dboConnection, janetConnection, SYSCS_CHECKPOINT_DATABASE, false );
        vet6616( dboConnection, janetConnection, SYSCS_BACKUP_DATABASE, false );
        vet6616( dboConnection, janetConnection, SYSCS_BACKUP_DATABASE_NOWAIT, false );
        vet6616( dboConnection, janetConnection, SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE, false );
        vet6616( dboConnection, janetConnection, SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT, false );
        vet6616( dboConnection, janetConnection, SYSCS_DISABLE_LOG_ARCHIVE_MODE, false );
        vet6616( dboConnection, janetConnection, SYSCS_CHECK_TABLE, true );
        vet6616( dboConnection, janetConnection, INSTALL_JAR, false );
        vet6616( dboConnection, janetConnection, REPLACE_JAR, false );
        vet6616( dboConnection, janetConnection, REMOVE_JAR, false );
        vet6616( dboConnection, janetConnection, SYSCS_EXPORT_TABLE, false );
        vet6616( dboConnection, janetConnection, SYSCS_IMPORT_TABLE, false );
        vet6616( dboConnection, janetConnection, SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE, false );
        vet6616( dboConnection, janetConnection, SYSCS_IMPORT_DATA, false );
        vet6616( dboConnection, janetConnection, SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE, false );
        vet6616( dboConnection, janetConnection, SYSCS_BULK_INSERT, false );
        vet6616( dboConnection, janetConnection, SYSCS_RELOAD_SECURITY_POLICY, false );
        vet6616( dboConnection, janetConnection, SYSCS_SET_USER_ACCESS, false );
        vet6616( dboConnection, janetConnection, SYSCS_GET_USER_ACCESS, true );
        vet6616( dboConnection, janetConnection, SYSCS_INVALIDATE_STORED_STATEMENTS, false );
        vet6616( dboConnection, janetConnection, SYSCS_EMPTY_STATEMENT_CACHE, false );
        vet6616( dboConnection, janetConnection, SYSCS_SET_XPLAIN_MODE, false );
        vet6616( dboConnection, janetConnection, SYSCS_GET_XPLAIN_MODE, true );
        vet6616( dboConnection, janetConnection, SYSCS_SET_XPLAIN_SCHEMA, false );
        vet6616( dboConnection, janetConnection, SYSCS_GET_XPLAIN_SCHEMA, true );
        vet6616( dboConnection, janetConnection, SYSCS_CREATE_USER, false );
        vet6616( dboConnection, janetConnection, SYSCS_RESET_PASSWORD, false );
        vet6616( dboConnection, janetConnection, SYSCS_DROP_USER, false );
    }
    private void    vet6616
        ( Connection dboConnection, Connection janetConnection, String routineName, boolean isFunction )
        throws Exception
    {
        vet6616( dboConnection, true, routineName );
        vet6616( janetConnection, !authorizationIsOn(), routineName );

        if ( authorizationIsOn() )
        {
            boolean isFreeze = SYSCS_FREEZE_DATABASE.equals( routineName );
            String  routineType = isFunction ? "function" : "procedure";
            goodStatement( dboConnection, "grant execute on " + routineType + " " + routineName + " to public" );

            if ( isFreeze )
            {
                goodStatement( dboConnection, "grant execute on " + routineType + " " + SYSCS_UNFREEZE_DATABASE + " to public" );
            }
        
            vet6616( janetConnection, true, routineName );
            
            goodStatement( dboConnection, "revoke execute on " + routineType + " " + routineName + " from public restrict" );

            if ( isFreeze )
            {
                goodStatement
                    ( dboConnection, "revoke execute on " + routineType + " " + SYSCS_UNFREEZE_DATABASE + " from public restrict" );
            }
        }
    }
    private void    vet6616( Connection conn, boolean shouldSucceed, String routineName )
        throws Exception
    {
        CallableStatement   cs = conn.prepareCall( "call test_dbo.runSystemRoutine( ? )" );
        cs.setString( 1, routineName );

        try {
            cs.execute();
            if ( !shouldSucceed ) { fail( routineName + " should have failed!" ); }
        }
        catch (SQLException se)
        {
            if ( shouldSucceed )
            {
                fail( routineName + " should have succeeded", se );
            }
            else
            {
                assertSQLState( LACK_EXECUTE_PRIV, se );
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Run various system procedures and functions by calling them directly */
    public  static  void    runSystemRoutine( String routineName )
        throws Exception
    {
        if ( SYSCS_SET_DATABASE_PROPERTY.equals( routineName ) )
        {
            SystemProcedures.SYSCS_SET_DATABASE_PROPERTY( "foo.bar.wibble", "wibble.bar.foo" );
        }
        else if ( SYSCS_GET_DATABASE_PROPERTY.equals( routineName ) )
        {
            SystemProcedures.SYSCS_GET_DATABASE_PROPERTY( "la.dee.dah" );
        }
        else if ( SYSCS_FREEZE_DATABASE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_FREEZE_DATABASE();

            // if that succeeded, then unfreeze the database immediately
            SystemProcedures.SYSCS_UNFREEZE_DATABASE();
        }
        else if ( SYSCS_UNFREEZE_DATABASE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_UNFREEZE_DATABASE();
        }
        else if ( SYSCS_CHECKPOINT_DATABASE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_CHECKPOINT_DATABASE();
        }
        else if ( SYSCS_BACKUP_DATABASE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_BACKUP_DATABASE( null );
                fail();
            }
            catch (SQLException se) { vetError( NULL_BACKUP_DIRECTORY, se ); }
        }
        else if ( SYSCS_BACKUP_DATABASE_NOWAIT.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_BACKUP_DATABASE_NOWAIT( null );
                fail();
            }
            catch (SQLException se) { vetError( NULL_BACKUP_DIRECTORY, se ); }
        }
        else if (SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE( null, (short) 0 );
                fail();
            }
            catch (SQLException se) { vetError( NULL_BACKUP_DIRECTORY, se ); }
        }
        else if (SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT( null, (short) 0 );
                fail();
            }
            catch (SQLException se) { vetError( NULL_BACKUP_DIRECTORY, se ); }
        }
        else if ( SYSCS_DISABLE_LOG_ARCHIVE_MODE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_DISABLE_LOG_ARCHIVE_MODE( (short) 0 );
        }
        else if ( SYSCS_CHECK_TABLE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_CHECK_TABLE( "SYS", "SYSTABLES" );
        }
        else if (INSTALL_JAR.equals( routineName ) )
        {
            try {
                SystemProcedures.INSTALL_JAR( SupportFilesSetup.getReadOnlyFileName("foo"), "bar", 1 );
                fail();
            }
            catch (SQLException se) {
                vetError(SQLJ_INVALID_JAR, se);
            }
        }
        else if (REPLACE_JAR.equals( routineName ) )
        {
            try {
                SystemProcedures.REPLACE_JAR( SupportFilesSetup.getReadOnlyFileName("foo"), "bar" );
                fail();
            }
            catch (SQLException se) {
                vetError(SQLJ_INVALID_JAR, se);
            }
        }
        else if (REMOVE_JAR.equals( routineName ) )
        {
            try {
                SystemProcedures.REMOVE_JAR( "test_dbo.foo", 0 );
                fail();
            }
            catch (SQLException se) { vetError( MISSING_OBJECT, se ); }
        }
        else if ( SYSCS_EXPORT_TABLE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_EXPORT_TABLE(
                        "TEST_DBO", "BAR",
                        SupportFilesSetup.getReadWriteFileName("WIBBLE"),
                        null, null, null);
                fail();
            }
            catch (SQLException se) { vetError( MISSING_TABLE, se ); }
        }
        else if ( SYSCS_IMPORT_TABLE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_IMPORT_TABLE(
                        "TEST_DBO", "BAR",
                        SupportFilesSetup.getReadWriteFileName("WIBBLE"),
                        null, null, null, (short) 1);
                fail();
            }
            catch (SQLException se) { vetError( NO_SUCH_TABLE, se ); }
        }
        else if ( SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE( "TEST_DBO", "BAR", "WIBBLE", null, null, null, (short) 1 );
                fail();
            }
            catch (SQLException se) { vetError( NO_SUCH_TABLE, se ); }
        }
        else if ( SYSCS_IMPORT_DATA.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_IMPORT_DATA
                    ( "TEST_DBO", "BAR", null, "1,3,4", "WIBBLE", null, null, null, (short) 1 );
                fail();
            }
            catch (SQLException se) { vetError( NO_SUCH_TABLE, se ); }
        }
        else if ( SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE
                    ( "TEST_DBO", "BAR", null, "1,3,4", "WIBBLE", null, null, null, (short) 1 );
                fail();
            }
            catch (SQLException se) { vetError( NO_SUCH_TABLE, se ); }
        }
        else if ( SYSCS_BULK_INSERT.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_BULK_INSERT
                    ( "TEST_DBO", "BAR", "WIBBLE", "wombat" );
                fail();
            }
            catch (SQLException se) { vetError( SYNTAX_ERROR, se ); }
        }
        else if ( SYSCS_RELOAD_SECURITY_POLICY.equals( routineName ) )
        {
            SystemProcedures.SYSCS_RELOAD_SECURITY_POLICY();
        }
        else if ( SYSCS_SET_USER_ACCESS.equals( routineName ) )
        {
            SystemProcedures.SYSCS_SET_USER_ACCESS( "FOO", "FULLACCESS" );
        }
        else if ( SYSCS_GET_USER_ACCESS.equals( routineName ) )
        {
            SystemProcedures.SYSCS_GET_USER_ACCESS( "FOO" );
        }
        else if ( SYSCS_INVALIDATE_STORED_STATEMENTS.equals( routineName ) )
        {
            SystemProcedures.SYSCS_INVALIDATE_STORED_STATEMENTS();
        }
        else if ( SYSCS_EMPTY_STATEMENT_CACHE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_EMPTY_STATEMENT_CACHE();
        }
        else if ( SYSCS_SET_XPLAIN_MODE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_SET_XPLAIN_MODE( 0 );
        }
        else if ( SYSCS_GET_XPLAIN_MODE.equals( routineName ) )
        {
            SystemProcedures.SYSCS_GET_XPLAIN_MODE();
        }
        else if ( SYSCS_SET_XPLAIN_SCHEMA.equals( routineName ) )
        {
            SystemProcedures.SYSCS_SET_XPLAIN_SCHEMA( "" );
        }
        else if ( SYSCS_GET_XPLAIN_SCHEMA.equals( routineName ) )
        {
            SystemProcedures.SYSCS_GET_XPLAIN_SCHEMA();
        }
        else if (SYSCS_CREATE_USER.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_CREATE_USER( "foo", "bar" );
                fail();
            }
            catch (SQLException se) { vetError( FIRST_CREDENTIALS, se ); }
        }
        else if (SYSCS_RESET_PASSWORD.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_RESET_PASSWORD( "foo", "bar" );
                fail();
            }
            catch (SQLException se) { vetError( UNKNOWN_USER, se ); }
        }
        else if (SYSCS_DROP_USER.equals( routineName ) )
        {
            try {
                SystemProcedures.SYSCS_DROP_USER( "foo" );
                fail();
            }
            catch (SQLException se) { vetError( UNKNOWN_USER, se ); }
        }
        else
        {
            throw new Exception( "Unknown routine name: " + routineName );
        }
    }
    private static  void    vetError( String sqlState, SQLException se )
        throws SQLException
    {
        if ( sqlState.equals( se.getSQLState() ) )
        {
            println( "Caught expected error: " + sqlState );
        }
        else { throw se; }
    }

}

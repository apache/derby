/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneBackupTest

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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test backup and restore of databases with Lucene indexes.
 * </p>
 */
public class LuceneBackupTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      DB_NAME = "lucenebackupdb";
    private static  final   String      BACKUP_DIR = "extinout/backupdir";
    private static  final   String      BACKUP_DIR2 = "extinout/backupdir2";

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK  };

    private static  final   String      POLICY_FILE = "org/apache/derbyTesting/functionTests/tests/lang/luceneSupport.policy";

    private static  final   String      ENGLISH_ANALYZER =
        "org.apache.derbyTesting.functionTests.tests.lang.LuceneCoarseAuthorizationTest.getEnglishAnalyzer";

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      INDEX_POEMS =
        "call LuceneSupport.createIndex( 'ruth', 'poems', 'poemText', '" + ENGLISH_ANALYZER + "' )";
    private static  final   String      DROP_POEMS_INDEX = "call LuceneSupport.dropIndex( 'ruth', 'poems', 'poemText' )";

    private static  final   String      READ_POEMS_INDEX =
        "select p.originalAuthor, i.score\n" +
        "from ruth.poems p, table ( ruth.poems__poemText( 'star', 1000, null ) ) i\n" +
        "where p.poemID = i.poemID and p.versionStamp = i.versionStamp\n" +
        "order by i.score desc\n";
    private static  final   String[][]  DEFAULT_POEMS_RESULT =
        new String[][]
        {
            { "Walt Whitman", "0.26756266" },
            { "Lord Byron", "0.22933942" },
            { "John Milton", "0.22933942" },
        };

    private static  final   String  GOOD_SHUTDOWN = "08006";

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

    public LuceneBackupTest( String name )
    {
        super( name );
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
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            LuceneBackupTest.class );

        Test        secureTest = new SecurityManagerSetup( suite, POLICY_FILE );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( secureTest, LEGAL_USERS, "LuceneBackupPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecoratorSingleUse( authenticatedTest, DB_NAME, true );
        Test        supportFilesTest = new SupportFilesSetup( authorizedTest );

        return supportFilesTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test basic functionality of backup/restore.
     * </p>
     */
    public  void    test_001_basic()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        LuceneCoarseAuthorizationTest.createSchema( ruthConnection );

        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, INDEX_POEMS );

        // verify that everything looks good
        assertResults
            (
             ruthConnection,
             READ_POEMS_INDEX,
             DEFAULT_POEMS_RESULT,
             false
             );

        // now backup the database from disk to disk
        goodStatement( dboConnection, "call syscs_util.syscs_backup_database( '" + BACKUP_DIR + "' )" );

        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        goodStatement( dboConnection, UNLOAD_TOOL );

        LuceneCoarseAuthorizationTest.dropSchema( ruthConnection );

        //
        // Now restore the database and verify it.
        //
        String  source = BACKUP_DIR + File.separator + DB_NAME;
        String  dboPassword = getTestConfiguration().getPassword( TEST_DBO );
        String  ruthPassword = getTestConfiguration().getPassword( RUTH );
        dboConnection = DriverManager.getConnection
            ( "jdbc:derby:memory:lbt1;user=" + TEST_DBO + ";password=" + dboPassword + ";restoreFrom=" + source );
        ruthConnection = DriverManager.getConnection
            ( "jdbc:derby:memory:lbt1;user=" + RUTH + ";password=" + ruthPassword );
        assertResults
            (
             ruthConnection,
             READ_POEMS_INDEX,
             DEFAULT_POEMS_RESULT,
             false
             );

        // backup the in-memory database
        goodStatement( dboConnection, "call syscs_util.syscs_backup_database( '" + BACKUP_DIR2 + "' )" );

        // free up the memory consumed by the in-memory database
        try {
            dboConnection = DriverManager.getConnection
                ( "jdbc:derby:memory:lbt1;user=" + TEST_DBO + ";password=" + dboPassword + ";drop=true" );
            fail( "Expected to get an exception!" );
        }
        catch (SQLException se)
        {
            assertEquals( GOOD_SHUTDOWN, se.getSQLState() );
        }

        //
        // Now restore the second backup and verify it
        //
        source = BACKUP_DIR2 + File.separator + "lbt1";
        dboConnection = DriverManager.getConnection
            ( "jdbc:derby:memory:lbt2;user=" + TEST_DBO + ";password=" + dboPassword + ";restoreFrom=" + source );
        ruthConnection = DriverManager.getConnection
            ( "jdbc:derby:memory:lbt2;user=" + RUTH + ";password=" + ruthPassword );
        assertResults
            (
             ruthConnection,
             READ_POEMS_INDEX,
             DEFAULT_POEMS_RESULT,
             false
             );
        
        // free up the memory consumed by the in-memory database
        try {
            dboConnection = DriverManager.getConnection
                ( "jdbc:derby:memory:lbt2;user=" + TEST_DBO + ";password=" + dboPassword + ";drop=true" );
            fail( "Expected to get an exception!" );
        }
        catch (SQLException se)
        {
            assertEquals( GOOD_SHUTDOWN, se.getSQLState() );
        }
    }

}

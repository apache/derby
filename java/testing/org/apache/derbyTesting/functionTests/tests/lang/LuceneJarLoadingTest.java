/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneJarLoadingTest

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
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * <p>
 * Test backup and restore of databases with Lucene indexes.
 * </p>
 */
public class LuceneJarLoadingTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      DB_NAME = "lucenejarloadingdb";

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK  };

    private static  final   String      POLICY_FILE = "org/apache/derbyTesting/functionTests/tests/lang/luceneSupport.policy";

    /** the jar file which contains the custom Analyzer and QueryParser */
    private static  final   String      EXTERNAL_JAR_NAME = "myLuceneClasses.jar";
    private static  final   String      INTERNAL_JAR_NAME = "TEST_DBO.myLuceneClasses";
    private static  final   String[]    SUPPORT_FILES = { "functionTests/tests/lang/" + EXTERNAL_JAR_NAME };

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      MY_ANALYZER = "MyAnalyzer.makeMyAnalyzer";
    private static  final   String      INDEX_TEXT_TABLE =
        "call LuceneSupport.createIndex( 'ruth', 'textTable', 'textCol', '" + MY_ANALYZER + "' )";
    private static  final   String      DROP_TEXT_INDEX = "call LuceneSupport.dropIndex( 'ruth', 'textTable', 'textCol' )";

    private static  final   String      READ_TEXT_INDEX =
        "select * from table\n" +
        "(\n" +
        "  ruth.textTable__textCol\n" +
        "  (\n" +
        "    'one two three four five six seven eight nine ten',\n" +
        "    'MyQueryParser.makeMyQueryParser',\n" +
        "    100, null\n" +
        "  )\n" +
        ") t\n";
    private static  final   String[][]  READ_TEXT_RESULT =
        new String[][]
        {
            { "10", "9", "2.2791052" },   
            { "9", "8", "1.6305782" },
            { "8", "7", "1.1616905" },   
            { "7", "6", "0.97469425" }, 
            { "6", "5", "0.6597747" },  
            { "5", "4", "0.49575216" },
            { "4", "3", "0.33803377" }, 
            { "3", "2", "0.17799875" },     
            { "2", "1", "0.09289266" },
            { "1", "0", "0.035006654" },   
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

    public LuceneJarLoadingTest( String name )
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite( LuceneJarLoadingTest.class );

        Test        secureTest = new SecurityManagerSetup( suite, POLICY_FILE );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( secureTest, LEGAL_USERS, "LuceneJarLoadingPermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecoratorSingleUse( authenticatedTest, DB_NAME, true );
        Test        supportFilesTest = new SupportFilesSetup( authorizedTest, SUPPORT_FILES );

        return supportFilesTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that you can use Analyzers and QueryParsers which live inside jar files
     * stored in the database.
     * </p>
     */
    public  void    test_001_basic()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        goodStatement( dboConnection, "create table dummyJustToCreateSchema( a int )" );

        // load the jar file
        URL jar = SupportFilesSetup.getReadOnlyURL( EXTERNAL_JAR_NAME );
        goodStatement( dboConnection, "call sqlj.install_jar( '" + jar.toExternalForm() + "', '" + INTERNAL_JAR_NAME + "', 0 )" );
        goodStatement
            (
             dboConnection,
             "call syscs_util.syscs_set_database_property( 'derby.database.classpath', '" + INTERNAL_JAR_NAME + "' )"
             );

        LuceneSupportPermsTest.loadTestTable( ruthConnection );

        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, INDEX_TEXT_TABLE );

        // verify that everything looks good
        assertResults
            (
             ruthConnection,
             READ_TEXT_INDEX,
             READ_TEXT_RESULT,
             false
             );

        // cleanup
        goodStatement( ruthConnection, DROP_TEXT_INDEX );
        goodStatement( dboConnection, UNLOAD_TOOL );
        LuceneSupportPermsTest.unloadTestTable( ruthConnection );
    }

    /**
     * <p>
     * Test that you can declare a function on methods in the Lucene api package.
     * </p>
     */
    public  void    test_002_apiPackage()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement( dboConnection, "create type LuceneVersion external name 'org.apache.lucene.util.Version' language java" );
        goodStatement
            (
             dboConnection,
             "create function getLuceneVersion() returns LuceneVersion\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derby.optional.api.LuceneUtils.currentVersion'\n"
             );

        assertResults
            (
             dboConnection,
             "values getLuceneVersion()",
             new String[][]
             {
                 { LuceneUtils.currentVersion().toString() }
             },
             false
             );

        goodStatement( dboConnection, "drop function getLuceneVersion" );
        goodStatement( dboConnection, "drop type LuceneVersion restrict" );
    }
    
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest

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
import java.io.IOException;
import java.math.BigDecimal;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.optional.api.LuceneIndexDescriptor;
import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * <p>
 * Test permissions on objects created by the optional Lucene support tool.
 * </p>
 */
public class LuceneSupportPermsTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      LUCENE_VERSION_PROPERTY = "derby.tests.lucene.version";
    private static  String              LUCENE_VERSION = "LUCENE_47";

    private static  final   String      DB_NAME = "lucenesupportpermsdb";

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK  };

	private static  final   String      AUTH_NO_ACCESS_NOT_OWNER    = "42507";
	private static  final   String      DBO_ONLY                                = "4251D";
    private static  final   String      FUNCTION_EXISTS                 = "X0Y68";

    private static  final   String      NOT_INDEXABLE                   = "42XBA";
    private static  final   String      NO_PRIMARY_KEY              = "42XBB";
    // LUCENE_UNSUPPORTED_TYPE = "42XBC": only raised if key type is unsupported. but all indexable types are supported.
    // LUCENE_INVALID_CHARACTER = "42XBD" is tested by LuceneSupportTest.
    private static  final   String      NONEXISTENT_INDEX           = "42XBE";
	private static  final   String      NO_DDL_PRIV                    = "42XBF";
	private static  final   String      DOUBLE_LOAD_ILLEGAL         = "42XBG";
	private static  final   String      DOUBLE_UNLOAD_ILLEGAL       = "42XBH";
	private static  final   String      BAD_DIRECTORY                      = "42XBI";
	private static  final   String      BAD_COLUMN_NAME                 = "42XBJ";
    private static  final   String      NONEXISTENT_TABLE_FUNCTION  ="42ZB4";
    private static  final   String      INCOMPATIBLE_ENCRYPTION = "42XBL";
    private static  final   String      ILLEGAL_NULL_ARG = "42XBM";
    private static  final   String      BAD_FIELD_NAME = "42XBN";
    private static  final   String      DUPLICATE_FIELD_NAME = "42XBO";
    private static  final   String      NULL_PRIMITIVE_ARG = "39004";

    private static  final   String      POLICY_FILE = "org/apache/derbyTesting/functionTests/tests/lang/luceneSupport.policy";

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      INDEX_POEMS = "call LuceneSupport.createIndex( 'ruth', 'poems', 'poemText', null )";
    private static  final   String      UPDATE_POEMS_INDEX = "call LuceneSupport.updateIndex( 'ruth', 'poems', 'poemText', null )";
    private static  final   String      DROP_POEMS_INDEX = "call LuceneSupport.dropIndex( 'ruth', 'poems', 'poemText' )";
    private static  final   String      DROP_PRIMARY_KEY = "alter table poems drop constraint poemsKey";

    private static  final   long        MILLIS_IN_HOUR = 1000L * 60L * 60L;
    private static  final   long        MILLIS_IN_DAY = MILLIS_IN_HOUR * 24L;

    private static  final   String      LANGUAGE = "en";
    private static  final   String      COUNTRY = "US";

    private static  final   String      DEFAULT_INDEX_DESCRIPTOR = "org.apache.derby.optional.api.LuceneUtils.defaultIndexDescriptor";
    private static  final   String      CONSTANT_QUERY_PARSER = "org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest.constantStringIndexDescriptor";
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    static  String[]    _fieldNames;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public LuceneSupportPermsTest(String name)
    {
        super(name);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        String      luceneVersion = getSystemProperty( LUCENE_VERSION_PROPERTY );
        if ( luceneVersion != null ) { LUCENE_VERSION = luceneVersion; }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            LuceneSupportPermsTest.class);

//IC see: https://issues.apache.org/jira/browse/DERBY-590
        Test        secureTest = new SecurityManagerSetup( suite, POLICY_FILE );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( secureTest, LEGAL_USERS, "LuceneSupportPermissions" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6546
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecoratorSingleUse( authenticatedTest, DB_NAME, true );
        Test        localizedTest = new LocaleTestSetup( authorizedTest, new Locale( LANGUAGE, COUNTRY ) );

        return localizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test baseline permissions where no grants are made.
     * </p>
     */
    public  void    test_001_basicNoGrant()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        createSchema( ruthConnection, Types.INTEGER );

        // ruth does not have permission to load the tool
        expectExecutionError( ruthConnection, LACK_EXECUTE_PRIV, LOAD_TOOL );

        // but the dbo has permission
        goodStatement( dboConnection, LOAD_TOOL );

        // can't update a non-existent index
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        expectExecutionError
            ( ruthConnection, NONEXISTENT_INDEX, "call LuceneSupport.updateIndex( 'ruth', 'poems', 'poemText', null )" );

        // alice does not have permission to index a table owned by ruth
        expectExecutionError( aliceConnection, LACK_COLUMN_PRIV, INDEX_POEMS );

        // but ruth can
        goodStatement( ruthConnection, INDEX_POEMS );

        // redundant index creation fails
        expectExecutionError( ruthConnection, FUNCTION_EXISTS, INDEX_POEMS );

        // can't update a non-existent index
        expectExecutionError( ruthConnection, NONEXISTENT_INDEX, "call LuceneSupport.updateIndex( 'ruth', 'poems', 'foo', null )" );
        expectExecutionError
            ( ruthConnection, NONEXISTENT_INDEX, "call LuceneSupport.updateIndex( 'ruth', 'poems', 'originalAuthor', null )" );

        // alice can't view an index created by ruth
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        String  viewPoemsIndex = "select * from table ( ruth.poems__poemText( 'star', 1000, null ) ) luceneResults order by poemID";
        expectExecutionError( aliceConnection, LACK_EXECUTE_PRIV, viewPoemsIndex );

        // but ruth can
        assertResults
            (
             ruthConnection,
             viewPoemsIndex,
             new String[][]
             {
                 { "3", "3", "2", "0.22933942" },
                 { "4", "4", "3", "0.22933942" },
                 { "5", "5", "4", "0.26756266" },
             },
             false
             );

        // alice can list indexes even on tables owned by ruth
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        String  listIndexes = "select schemaName, tableName, columnName from table ( LuceneSupport.listIndexes() ) listindexes";
        assertResults
            (
             aliceConnection,
             listIndexes,
             new String[][]
             {
                 { "RUTH", "POEMS", "POEMTEXT" },
             },
             false
             );

        // alice cannot update an index owned by ruth
        expectExecutionError( aliceConnection, NO_DDL_PRIV, UPDATE_POEMS_INDEX );
        
        // alice cannot drop an index owned by ruth
        expectExecutionError( aliceConnection, AUTH_NO_ACCESS_NOT_OWNER, DROP_POEMS_INDEX );

        // ruth can update the index
        goodStatement( ruthConnection, UPDATE_POEMS_INDEX );

        // dropping the key does NOT prevent you from re-indexing
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        goodStatement( ruthConnection, DROP_PRIMARY_KEY );
        goodStatement( ruthConnection, UPDATE_POEMS_INDEX );

        // but dropping a key column DOES prevent you from re-indexing and from selecting
        goodStatement( ruthConnection, "alter table poems drop column versionStamp" );
        expectExecutionError( ruthConnection, COLUMN_OUT_OF_SCOPE, UPDATE_POEMS_INDEX );
        expectExecutionError( ruthConnection, COLUMN_OUT_OF_SCOPE, viewPoemsIndex );
        
        // ruth can drop the index
        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        assertResults
            (
             ruthConnection,
             listIndexes,
             new String[][] {},
             false
             );

        // redundant drop fails, however
        expectExecutionError( ruthConnection, NONEXISTENT_OBJECT, DROP_POEMS_INDEX );

        // verify that a primary key is necessary in order to index a table
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        dropSchema( ruthConnection );
        createSchema( ruthConnection, Types.INTEGER );
        goodStatement( ruthConnection, DROP_PRIMARY_KEY );
        expectExecutionError( ruthConnection, NO_PRIMARY_KEY, INDEX_POEMS );

        // ruth cannot unload the tool
        expectExecutionError( ruthConnection, LACK_EXECUTE_PRIV, UNLOAD_TOOL );

        // but the dbo can
        goodStatement( dboConnection, UNLOAD_TOOL );

        dropSchema( ruthConnection );
    }

    /**
     * <p>
     * Test that a user can grant access to her indexes.
     * </p>
     */
    public  void    test_002_userGrant()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        createSchema( ruthConnection, Types.INTEGER );

        // load the Lucene plugin
        goodStatement( dboConnection, LOAD_TOOL );

        // ruth indexes her table and grants alice privilege to run the index reading function
        goodStatement( ruthConnection, INDEX_POEMS );

        Permission[]    permissions = new Permission[]
        {
            new Permission( "execute on function poems__poemText", NO_GENERIC_PERMISSION ),
            new Permission( "select ( poemID ) on poems", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( versionStamp ) on poems", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( poemText ) on poems", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grantPermission( ruthConnection, permission.text, ALICE );
        }

        // but alice still needs select privilege on the base table columns
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        String  viewPoemsIndex = "select * from table ( ruth.poems__poemText( 'star', 1000, null ) ) luceneResults order by poemid";
        String[][]  viewPoemsIndexResults = new String[][]
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                { "3", "3", "2", "0.22933942" },
                { "4", "4", "3", "0.22933942" },
                { "5", "5", "4", "0.26756266" },
            };

        // now alice can view the index
        assertResults( aliceConnection, viewPoemsIndex, viewPoemsIndexResults, false );

        // now revoke each permission and verify that it is needed
        for ( Permission permission : permissions )
        {
            vetPermission_002( permission, ruthConnection, aliceConnection, viewPoemsIndex, viewPoemsIndexResults );
        }

        // but alice still can't drop an index owned by ruth
        expectExecutionError( aliceConnection, AUTH_NO_ACCESS_NOT_OWNER, DROP_POEMS_INDEX );

        // unload the plugin
        goodStatement( dboConnection, UNLOAD_TOOL );

        dropSchema( ruthConnection );
    }
    private void    vetPermission_002
        (
         Permission permission,
         Connection ruthConnection,
         Connection aliceConnection,
         String statement,
         String[][] expectedResults
         )
        throws Exception
    {
        revokePermission( ruthConnection, permission.text, ALICE );
        expectExecutionError( aliceConnection, permission.sqlStateWhenMissing, statement );
        grantPermission( ruthConnection, permission.text, ALICE );
        assertResults( aliceConnection, statement, expectedResults, false );
    }
    private void    grantPermission( Connection conn, String permission, String grantee )
        throws Exception
    {
        String  command = "grant " + permission + " to " + grantee;

        goodStatement( conn, command );
    }
    private void    revokePermission( Connection conn, String permission, String grantee )
        throws Exception
    {
        String  command = "revoke " + permission + " from " + grantee;
        if ( permission.startsWith( "execute" ) || permission.startsWith( "usage" ) )   { command += " restrict"; }

        goodStatement( conn, command );
    }

    /**
     * <p>
     * Test that only the DBO can (un)load the tool and the tool
     * can't be (un)loaded twice.
     * </p>
     */
    public  void    test_003_loading()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema( ruthConnection, Types.INTEGER );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        goodStatement( dboConnection, "grant execute on procedure syscs_util.syscs_register_tool to public" );

        // only the DBO can load the tool
        expectExecutionError( ruthConnection, DBO_ONLY, LOAD_TOOL );

        goodStatement( dboConnection, LOAD_TOOL );
        expectExecutionError( dboConnection, DOUBLE_LOAD_ILLEGAL, LOAD_TOOL );

        // cannot index non-existent table or column
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        expectExecutionError( ruthConnection, NOT_INDEXABLE, "call LuceneSupport.createIndex( 'ruth', 'foo', 'poemText', null )" );
        expectExecutionError( ruthConnection, NOT_INDEXABLE, "call LuceneSupport.createIndex( 'ruth', 'poems', 'fooText', null )" );
        expectExecutionError( ruthConnection, NOT_INDEXABLE, "call LuceneSupport.createIndex( 'ruth', 'poems', 'versionStamp', null )" );

        // cannot drop non-existent index
        expectExecutionError( ruthConnection, NONEXISTENT_OBJECT, "call LuceneSupport.dropIndex( 'ruth', 'foo', 'poemText' )" );
        expectExecutionError( ruthConnection, NONEXISTENT_OBJECT, "call LuceneSupport.dropIndex( 'ruth', 'poems', 'versionStamp' )" );
        
        // only the DBO can unload the tool
        expectExecutionError( ruthConnection, DBO_ONLY, UNLOAD_TOOL );

        goodStatement( dboConnection, "revoke execute on procedure syscs_util.syscs_register_tool from public restrict" );

        goodStatement( dboConnection, UNLOAD_TOOL );
        expectExecutionError( dboConnection, DOUBLE_UNLOAD_ILLEGAL, UNLOAD_TOOL );

        // try loading and unloading again for good measure
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( dboConnection, UNLOAD_TOOL );
        dropSchema( ruthConnection );
    }
    
    /**
     * <p>
     * Test all datatypes as key types.
     * </p>
     */
    public  void    test_004_datatypes()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        goodStatement( dboConnection, LOAD_TOOL );

        vet_004( ruthConnection, Types.BIGINT );
        vet_004( ruthConnection, Types.BOOLEAN );
        vet_004( ruthConnection, Types.CHAR );
        vet_004( ruthConnection, Types.BINARY );
        vet_004( ruthConnection, Types.DATE );
        vet_004( ruthConnection, Types.DECIMAL );
        vet_004( ruthConnection, Types.DOUBLE );
        vet_004( ruthConnection, Types.FLOAT );
        vet_004( ruthConnection, Types.INTEGER );
        vet_004( ruthConnection, Types.NUMERIC );
        vet_004( ruthConnection, Types.REAL );
        vet_004( ruthConnection, Types.SMALLINT );
        vet_004( ruthConnection, Types.TIME );
        vet_004( ruthConnection, Types.TIMESTAMP );
        vet_004( ruthConnection, Types.VARCHAR );
        vet_004( ruthConnection, Types.VARBINARY );

        goodStatement( dboConnection, UNLOAD_TOOL );
    }
    private void    vet_004( Connection ruthConnection, int jdbcType )
        throws Exception
    {
        createSchema( ruthConnection, jdbcType );
        goodStatement( ruthConnection, INDEX_POEMS );

        // make sure that we can de-serialize the key
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select p.originalAuthor, i.score\n" +
             "from ruth.poems p, table ( ruth.poems__poemText( 'star', 1000, null ) ) i\n" +
             "where p.poemID = i.poemID and p.versionStamp = i.versionStamp\n" +
             "order by i.score desc\n",
             new String[][]
             {
                 { "Walt Whitman", "0.26756266" },
                 { "Lord Byron", "0.22933942" },
                 { "John Milton", "0.22933942" },
             },
             false
             );

        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        dropSchema( ruthConnection );
    }

    /**
     * <p>
     * Test error messages when a lucene directory has been deleted.
     * </p>
     */
    public  void    test_005_deleteDirectory()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema( ruthConnection, Types.INTEGER );
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, INDEX_POEMS );

        TestConfiguration   config = getTestConfiguration();

        String  dbName = config.getDefaultDatabaseName();
        String  physicalDBName = config.getPhysicalDatabaseName( dbName );
        String  dbPath = config.getDatabasePath( physicalDBName );
        File    dbDirectory = new File( dbPath );
//IC see: https://issues.apache.org/jira/browse/DERBY-6562
        File    luceneDirectory = new File( dbDirectory, "LUCENE" );
        File    ruthDirectory = new File( luceneDirectory, "RUTH" );
        File    poemsDirectory = new File( ruthDirectory, "POEMS" );
        File    poemTextIndexDirectory = new File( poemsDirectory, "POEMTEXT" );

        assertTrue( deleteFile( poemTextIndexDirectory ) );

        // but that doesn't stop you from deleting the index
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        expectCompilationError
            (
             ruthConnection, NONEXISTENT_TABLE_FUNCTION,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 100, null ) ) t"
             );

        goodStatement( dboConnection, UNLOAD_TOOL );
        dropSchema( ruthConnection );
    }
    
    /**
     * <p>
     * Test that you can change the Analyzer.
     * </p>
     */
    public  void    test_006_changeAnalyzer()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema( ruthConnection, Types.INTEGER );
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, INDEX_POEMS );

        // verify that we are the correct locale
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "values ( substr( getDatabaseLocale(), 1, 2 ) )",
             new String[][]
             {
                 { LANGUAGE },
             },
             false
             );
        

        String  query =
            "select p.originalAuthor, i.score\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "from ruth.poems p, table ( ruth.poems__poemText( 'star', 1000, null ) ) i\n" +
            "where p.poemID = i.poemID and p.versionStamp = i.versionStamp\n" +
            "order by i.score desc\n";

        assertResults
            (
             ruthConnection,
             query,
             new String[][]
             {
                 { "Walt Whitman", "0.26756266" },
                 { "Lord Byron", "0.22933942" },
                 { "John Milton", "0.22933942" },
             },
             false
             );

        // now switch the Analyzer and re-run the query
        goodStatement
            ( ruthConnection,
              "call LuceneSupport.updateIndex( 'ruth', 'poems', 'poemText', '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        assertResults
            (
             ruthConnection,
             query,
             new String[][]
             {
                 { "Walt Whitman", "0.3304931" },
                 { "John Milton", "0.2832798" },
             },
             false
             );

        //
        // Add another index and inspect the values of listIndexes()
        //
        goodStatement( ruthConnection, "call LuceneSupport.createIndex( 'ruth', 'poems', 'originalAuthor', null )" );
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select schemaName, tableName, columnName, luceneVersion, analyzer, indexDescriptorMaker\n" +
             "from table( LuceneSupport.listIndexes() ) l\n" +
             "order by schemaName, tableName, columnName\n",
             new String[][]
             {
                 {
                     "RUTH", "POEMS", "ORIGINALAUTHOR", LUCENE_VERSION,
                     "org.apache.lucene.analysis.en.EnglishAnalyzer",
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                     DEFAULT_INDEX_DESCRIPTOR
                 },
                 {
                     "RUTH", "POEMS", "POEMTEXT", LUCENE_VERSION,
                     "org.apache.lucene.analysis.standard.StandardAnalyzer",
                     LuceneCoarseAuthorizationTest.STANDARD_ANALYZER
                 },
             },
             false
             );

        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'poems', 'originalAuthor' )" );
        goodStatement( dboConnection, UNLOAD_TOOL );
        dropSchema( ruthConnection );
    }

    /**
     * <p>
     * Test that you can index views and index tables with alternative column lists.
     * </p>
     */
    public  void    test_007_indexViews()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema( ruthConnection, Types.INTEGER );
        createPoemView( ruthConnection );
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, INDEX_POEMS );

        // must supply some key columns if you're going to index a view
        expectExecutionError
            (
             ruthConnection,
             NO_PRIMARY_KEY,
             "call LuceneSupport.createIndex( 'ruth', 'poemView', 'poemText', null )"
             );

        // now index the view
        goodStatement
            (
             ruthConnection,
             "call LuceneSupport.createIndex( 'ruth', 'poemView', 'poemText', null, 'poemID', 'versionStamp' )"
             );

        // can't create a second index by the same name
        expectExecutionError
            (
             ruthConnection,
             FUNCTION_EXISTS,
             "call LuceneSupport.createIndex( 'ruth', 'poemView', 'poemText', null, 'poemID' )"
             );

        // vet index contents
        String  selectFromViewIndex =
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "select p.originalAuthor, i.score\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "from ruth.poems p, table ( ruth.poemView__poemText( 'star', 1000, null ) ) i\n" +
            "where p.poemID = i.poemID and p.versionStamp = i.versionStamp\n" +
            "order by i.score desc\n";
        assertResults
            (
             ruthConnection,
             selectFromViewIndex,
             new String[][]
             {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                 { "Walt Whitman", "0.26756266" },
                 { "Lord Byron", "0.22933942" },
                 { "John Milton", "0.22933942" },
             },
             false
             );

        // vet index list
        String  selectIndexes =
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "select schemaName, tableName, columnName, indexDescriptorMaker\n" +
            "from table( LuceneSupport.listIndexes() ) l\n" +
            "order by schemaName, tableName, columnName\n";
        assertResults
            (
             ruthConnection,
             selectIndexes,
             new String[][]
             {
                 {
                     "RUTH", "POEMS", "POEMTEXT",
                     DEFAULT_INDEX_DESCRIPTOR
                 },
                 {
                     "RUTH", "POEMVIEW", "POEMTEXT",
                     DEFAULT_INDEX_DESCRIPTOR
                 },
             },
             false
             );

        // update the view index, changing its analyzer
        goodStatement
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "call LuceneSupport.updateIndex( 'ruth', 'poemView', 'poemText', '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )"
             );
        assertResults
            (
             ruthConnection,
             selectFromViewIndex,
             new String[][]
             {
                 { "Walt Whitman", "0.3304931" },
                 { "John Milton", "0.2832798" },
             },
             false
             );
        assertResults
            (
             ruthConnection,
             selectIndexes,
             new String[][]
             {
                 {
                     "RUTH", "POEMS", "POEMTEXT",
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                     DEFAULT_INDEX_DESCRIPTOR
                 },
                 {
                     "RUTH", "POEMVIEW", "POEMTEXT",
                     LuceneCoarseAuthorizationTest.STANDARD_ANALYZER
                 },
             },
             false
             );

        // drop the index on the view
        goodStatement
            (
             ruthConnection,
             "call LuceneSupport.dropIndex( 'ruth', 'poemView', 'poemText' )"
             );
        assertResults
            (
             ruthConnection,
             selectIndexes,
             new String[][]
             {
                 {
                     "RUTH", "POEMS", "POEMTEXT",
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                     DEFAULT_INDEX_DESCRIPTOR
                 },
             },
             false
             );

        // now drop the index on the table and create one with just one key column
        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        goodStatement
            (
             ruthConnection,
             "call LuceneSupport.createIndex( 'ruth', 'poems', 'poemText', null, 'poemID' )"
             );
        assertResults
            (
             ruthConnection,
             "select *\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "from table ( ruth.poems__poemText( 'star', 1000, null ) ) i\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "order by i.score desc\n",
             new String[][]
             {
                 { "5", "4", "0.26756266" },
                 { "4", "3", "0.22933942" },
                 { "3", "2", "0.22933942" },
             },
             false
             );
        
        goodStatement( ruthConnection, DROP_POEMS_INDEX );
        goodStatement( dboConnection, UNLOAD_TOOL );
        goodStatement( ruthConnection, "drop view poemView" );
        dropSchema( ruthConnection );
    }

    /**
     * <p>
     * Test that you can't create an index involving a column with the same name
     * as one of the system-supplied column names (documentID and score).
     * </p>
     */
    public  void    test_008_columnNames()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        goodStatement( dboConnection, LOAD_TOOL );

        goodStatement( ruthConnection, "create table badTable1( keyCol int primary key, score clob )" );
        goodStatement( ruthConnection, "create table badTable2( keyCol int primary key, documentID clob )" );
        goodStatement( ruthConnection, "create table badTable3( score int primary key, textCol clob )" );
        goodStatement( ruthConnection, "create table badTable4( documentID int primary key, textCol clob )" );

        expectExecutionError
            (
             ruthConnection, BAD_COLUMN_NAME,
             "call LuceneSupport.createIndex( 'ruth', 'badTable1', 'score', null )"
             );
        expectExecutionError
            (
             ruthConnection, BAD_COLUMN_NAME,
             "call LuceneSupport.createIndex( 'ruth', 'badTable2', 'documentID', null )"
             );
        expectExecutionError
            (
             ruthConnection, BAD_COLUMN_NAME,
             "call LuceneSupport.createIndex( 'ruth', 'badTable3', 'textCol', null )"
             );
        expectExecutionError
            (
             ruthConnection, BAD_COLUMN_NAME,
             "call LuceneSupport.createIndex( 'ruth', 'badTable4', 'textCol', null )"
             );

        goodStatement( dboConnection, UNLOAD_TOOL );
        goodStatement( ruthConnection, "drop table badTable1" );
        goodStatement( ruthConnection, "drop table badTable2" );
        goodStatement( ruthConnection, "drop table badTable3" );
        goodStatement( ruthConnection, "drop table badTable4" );
    }
    
   /**
     * <p>
     * Test changes to the arguments to the searching table function.
     * </p>
     */
    public  void    test_009_searchArgs()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        loadTestTable( ruthConnection );
        
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( ruthConnection, "call LuceneSupport.createIndex( 'ruth', 'textTable', 'textCol', null )" );

        // get all the matches
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 100, null ) ) t",
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
             },
             false
             );
        
        // get an initial 3-row window of the top results
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 3, null ) ) t",
             new String[][]
             {
                 { "10", "9", "2.2791052" },
                 { "9", "8", "1.6305782" },
                 { "8", "7", "1.1616905" },
             },
             false
             );
        
        // get the next 4-row window of results
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 4, 1.0 ) ) t",
             new String[][]
             {
                 { "7", "6", "0.97469425" },
                 { "6", "5", "0.6597747" },
                 { "5", "4", "0.49575216" },
                 { "4", "3", "0.33803377" },
             },
             false
             );

        // get the final window of results
        assertResults
            (
             ruthConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 100, 0.2 ) ) t",
             new String[][]
             {
                 { "3", "2", "0.17799875" },
                 { "2", "1", "0.09289266" },
                 { "1", "0", "0.035006654" },
             },
             false
             );
        
        // try a different query parser
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'textTable', 'textCol' )" );
        goodStatement
            (
             ruthConnection,
             "call LuceneSupport.createIndex( 'ruth', 'textTable', 'textCol', '" + CONSTANT_QUERY_PARSER + "' )"
             );
        assertResults
            (
             ruthConnection,
             "select * from table( ruth.textTable__textCol( 'one two three four five six seven eight nine ten', 100, null ) ) t",
             new String[][]
             {
                 { "1", "0", "1.597837" },
                 { "2", "1", "0.9986481" },
                 { "3", "2", "0.7989185" },
                 { "4", "3", "0.7989185" },
                 { "5", "4", "0.69905365" },
                 { "6", "5", "0.59918886" },
                 { "7", "6", "0.59918886" },
                 { "8", "7", "0.49932405" },
                 { "9", "8", "0.49932405" },
                 { "10", "9", "0.49932405" },
             },
             false
             );
        
        goodStatement( dboConnection, UNLOAD_TOOL );
        unloadTestTable( ruthConnection );
    }
    static  void    loadTestTable( Connection conn ) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6600
        conn.prepareStatement
            (
             "create table textTable( keyCol int primary key, textCol clob )"
             ).execute();
        conn.prepareStatement
            (
             "insert into textTable values\n" +
             "( 1, 'one' ),\n" +
             "( 2, 'one two' ),\n" +
             "( 3, 'one two three' ),\n" +
             "( 4, 'one two three four' ),\n" +
             "( 5, 'one two three four five' ),\n" +
             "( 6, 'one two three four five six' ),\n" +
             "( 7, 'one two three four five six seven' ),\n" +
             "( 8, 'one two three four five six seven eight' ),\n" +
             "( 9, 'one two three four five six seven eight nine' ),\n" +
             "( 10, 'one two three four five six seven eight nine ten' ),\n" +
             "( 101, 'bricks' ),\n" +
             "( 102, 'bricks and mortar' ),\n" +
             "( 103, 'bricks and mortar, tea' ),\n" +
             "( 104, 'bricks and mortar, tea, tears' ),\n" +
             "( 105, 'bricks and mortar, tea, tears, turtle' ),\n" +
             "( 106, 'bricks and mortar, tea, tears, turtle, soup' ),\n" +
             "( 107, 'bricks and mortar, tea, tears, turtle, soup, when in the course' ),\n" +
             "( 108, 'bricks and mortar, tea, tears, turtle, soup, when in the course of human events' ),\n" +
             "( 109, 'bricks and mortar, tea, tears, turtle, soup, when in the course of human events you want' ),\n" +
             "( 110, 'bricks and mortar, tea, tears, turtle, soup, when in the course of human events you want better cell coverage' )\n"
//IC see: https://issues.apache.org/jira/browse/DERBY-6600
             ).execute();
    }
    static  void    unloadTestTable( Connection conn ) throws Exception
    {
        conn.prepareStatement
            (
             "drop table textTable"
             ).execute();
    }

   /**
     * <p>
     * Test that encryption and the Lucene plugin are incompatible.
     * </p>
     */
    public  void    test_010_encryption()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        String      password = getTestConfiguration().getPassword( TEST_DBO );
        String      encryptDatabaseURL = "jdbc:derby:" + DB_NAME + ";user=" + TEST_DBO + ";password=" + password +
                ";dataEncryption=true;bootPassword=tryToGuessThis";
        String      decryptDatabaseURL = "jdbc:derby:" + DB_NAME + ";user=" + TEST_DBO + ";password=" + password +
                ";decryptDatabase=true;bootPassword=tryToGuessThis";

        goodStatement( dboConnection, LOAD_TOOL );
        getTestConfiguration().shutdownDatabase();

        // verify that you can't encrypt the database now
        try {
            DriverManager.getConnection( encryptDatabaseURL );
            fail( "Should not have been able to get a connection!" );
        }
        catch (SQLException se)
        {
            assertSQLState( INCOMPATIBLE_ENCRYPTION, se );
        }

        // now unload the tool and encrypt the database
        dboConnection = openUserConnection( TEST_DBO );
        goodStatement( dboConnection, UNLOAD_TOOL );
        getTestConfiguration().shutdownDatabase();
        dboConnection = DriverManager.getConnection( encryptDatabaseURL );

        // now you can't load the plugin
        expectExecutionError( dboConnection, INCOMPATIBLE_ENCRYPTION, LOAD_TOOL );

        // turn off encryption
        getTestConfiguration().shutdownDatabase();
        dboConnection = DriverManager.getConnection( decryptDatabaseURL );
    }
    
   /**
     * <p>
     * Test restrictions on field names.
     * </p>
     */
    public  void    test_011_fieldNames()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        loadTestTable( ruthConnection );
        goodStatement( dboConnection, LOAD_TOOL );

        String  indexTestTable =
            "call LuceneSupport.createIndex\n" +
            "(\n" +
            "    'ruth', 'textTable', 'textCol',\n" +
            "    'org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest.makeFieldNameIndexDescriptor'\n" +
            ")\n";
        
        goodStatement
            (
             ruthConnection,
             "create procedure setFieldNames( fieldName varchar( 32672 )... )\n" +
             "language java parameter style derby no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest.setFieldNames'\n"
             );

        // Field and key name conflict.
        goodStatement( ruthConnection, "call setFieldNames( 'KEYCOL' )" );
        expectExecutionError( ruthConnection, BAD_FIELD_NAME, indexTestTable );

        // Duplicate field names.
        goodStatement( ruthConnection, "call setFieldNames( 'FOO', 'FOO' )" );
        expectExecutionError( ruthConnection, DUPLICATE_FIELD_NAME, indexTestTable );

        // Null field name.
        goodStatement( ruthConnection, "call setFieldNames( 'FOO', null )" );
        expectExecutionError( ruthConnection, DUPLICATE_FIELD_NAME, indexTestTable );
        goodStatement( ruthConnection, "call setFieldNames( null, 'FOO' )" );
        expectExecutionError( ruthConnection, DUPLICATE_FIELD_NAME, indexTestTable );

        goodStatement( ruthConnection, "drop procedure setFieldNames" );
        goodStatement( dboConnection, UNLOAD_TOOL );
        unloadTestTable( ruthConnection );
    }

   /**
     * <p>
     * Test that nulls are allowed in keys. See DERBY-6602.
     * </p>
     */
    public  void    test_6602()
//IC see: https://issues.apache.org/jira/browse/DERBY-6602
        throws Exception
    {
        CD[]    columnDescriptors = new CD[]
        {
            new CD( "bigintCol", "bigint" ),
            new CD( "blobCol", "blob" ),
            new CD( "booleanCol", "boolean" ),
            new CD( "charCol", "char( 10 )" ),
            new CD( "clobCol", "clob" ),
            new CD( "dataCol", "date" ),
            new CD( "decimalCol", "decimal" ),
            new CD( "doubleCol", "double" ),
            new CD( "floatCol", "float" ),
            new CD( "intCol", "int" ),
            new CD( "longvarcharCol", "long varchar" ),
            new CD( "longvarcharforbitdataCol", "long varchar for bit data" ),
            new CD( "numericCol", "numeric" ),
            new CD( "realCol", "real" ),
            new CD( "smallintCol", "smallint" ),
            new CD( "timeCol", "time" ),
            new CD( "varcharCol", "varchar( 10 )" ),
            new CD( "varcharforbitdataCol", "varchar( 10 ) for bit data" ),
        };

        StringBuilder   buffer = new StringBuilder();
        buffer.append( "create table t_6602\n(\ttextcol clob" );
        for ( CD cd : columnDescriptors )
        {
            buffer.append( ",\n\t" + cd.name + " " + cd.type );
        }
        buffer.append( "\n)" );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( dboConnection, buffer.toString() );
        goodStatement( dboConnection, "insert into t_6602( textcol ) values ( 'abc'), ( 'def')" );

        for ( CD cd : columnDescriptors )
        {
            vet6602( dboConnection, cd );
        }

        goodStatement( dboConnection, "drop table t_6602" );
        goodStatement( dboConnection, UNLOAD_TOOL );
    }
    private void    vet6602( Connection conn, CD cd )   throws Exception
    {
        PreparedStatement   ps = chattyPrepare
            (
             conn,
             "call lucenesupport.createindex\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "( 'TEST_DBO', 't_6602', 'textcol', '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "', ? )"
             );
        ps.setString( 1, cd.name );
        ps.execute();

        assertResults
            (
             conn,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table( t_6602__textcol( 'abc or def', 3, null ) ) tc order by documentid",
             new String[][]
             {
                 { null, "0", "0.35355338" },
                 { null, "1", "0.35355338" },
             },
             false
             );

        goodStatement( conn, "call lucenesupport.dropIndex( 'TEST_DBO', 't_6602', 'textcol' )" );
    }
    
   /**
     * <p>
     * Test that nulls are not allowed as the values of certain arguments. See DERBY-6596.
     * </p>
     */
    public  void    test_6596_null_args()
//IC see: https://issues.apache.org/jira/browse/DERBY-6596
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        
        goodStatement( dboConnection, LOAD_TOOL );
        goodStatement( dboConnection, "create table t_6596( x int primary key, c clob )" );
        goodStatement( dboConnection, "insert into t_6596 values ( 1, 'abc' ), ( 2, 'def' )" );

        // create index errors
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
              "call lucenesupport.createindex( null, 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.createindex( 'TEST_DBO', null, 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.createindex( 'TEST_DBO', 't_6596', null,  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.createindex( 'TEST_DBO', 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "', null )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.createindex( 'TEST_DBO', 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "', 'x', null )" );

        goodStatement( dboConnection, "call lucenesupport.createindex( 'TEST_DBO', 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        assertResults
            (
             dboConnection,
             "select * from table( t_6596__c( 'abc or def', 3, null ) ) tc order by documentid",
             new String[][]
             {
                 { "1", "0", "0.35355338" },
                 { "2", "1", "0.35355338" },
             },
             false
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        goodStatement( dboConnection, "call lucenesupport.updateindex( 'TEST_DBO', 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        
        // update index errors
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.updateindex( null, 't_6596', 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.updateindex( 'TEST_DBO', null, 'c',  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.updateindex( 'TEST_DBO', 't_6596', null,  '" + LuceneCoarseAuthorizationTest.STANDARD_ANALYZER + "' )" );

        // query errors
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "select * from table( t_6596__c( null, 3, null ) ) tc order by documentid" );
        expectExecutionError
            ( dboConnection, NULL_PRIMITIVE_ARG,
              "select * from table( t_6596__c( 'abc or def', null, null ) ) tc order by documentid" );

        // drop index errors
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.dropindex( null, 't_6596', 'c' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.dropindex( 'TEST_DBO', null, 'c' )" );
        expectExecutionError
            ( dboConnection, ILLEGAL_NULL_ARG,
              "call lucenesupport.dropindex( 'TEST_DBO', 't_6596', null )" );

        goodStatement( dboConnection, "call lucenesupport.dropindex( 'TEST_DBO', 't_6596', 'c' )" );
        goodStatement( dboConnection, "drop table t_6596" );
        goodStatement( dboConnection, UNLOAD_TOOL );
    }
    
    /**
     * <p>
     * Test identifier casing for keys and text columns. See DERBY-6730.
     * </p>
     */
    public  void    test_6730()
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        createSchema( ruthConnection, Types.INTEGER );
        goodStatement
            (
             ruthConnection,
             "create view v_6730_1 ( poemID, poemText ) as select poemID, poemText from ruth.poems"
             );
        goodStatement
            (
             ruthConnection,
             "create view v_6730_2 ( poemID, \"poemText\" ) as select poemID, poemText from ruth.poems"
             );
        goodStatement
            (
             ruthConnection,
             "create view v_6730_3 ( \"poemID\", poemText ) as select poemID, poemText from ruth.poems"
             );
        goodStatement
            (
             ruthConnection,
             "create view v_6730_4 ( \"poemID\", \"poemText\" ) as select poemID, poemText from ruth.poems"
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        goodStatement
            (
             ruthConnection,
             "create view v_6730_5 ( poemID, \"c l o b\" ) as select poemID, poemText from ruth.poems"
             );
        goodStatement
            (
             ruthConnection,
             "create view v_6730_6 ( \"k e y\", poemText ) as select poemID, poemText from ruth.poems"
             );

        goodStatement( dboConnection, LOAD_TOOL );

        // Index the views
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_1',\n" +
             "  'poemText',\n" +
             "  null,\n" +
             "  'poemID'\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_2',\n" +
             "  '\"poemText\"',\n" +
             "  null,\n" +
             "  'poemID'\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_3',\n" +
             "  'poemText',\n" +
             "  null,\n" +
             "  '\"poemID\"'\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_4',\n" +
             "  '\"poemText\"',\n" +
             "  null,\n" +
             "  '\"poemID\"'\n" +
             ")\n"
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_5',\n" +
             "  '\"c l o b\"',\n" +
             "  null,\n" +
             "  'poemID'\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.createIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_6',\n" +
             "  'poemText',\n" +
             "  null,\n" +
             "  '\"k e y\"'\n" +
             ")\n"
             );

        // Verify the expected casing of identifiers
        vet6730( ruthConnection );

        // Update the indexes and re-verify
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_1',\n" +
             "  'poemText',\n" +
             "  null\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_2',\n" +
             "  '\"poemText\"',\n" +
             "  null\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_3',\n" +
             "  'poemText',\n" +
             "  null\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_4',\n" +
             "  '\"poemText\"',\n" +
             "  null\n" +
             ")\n"
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_5',\n" +
             "  '\"c l o b\"',\n" +
             "  null\n" +
             ")\n"
             );
        goodStatement
            (
             ruthConnection,
             "call luceneSupport.updateIndex\n" +
             "(\n" +
             "  'ruth',\n" +
             "  'v_6730_6',\n" +
             "  'poemText',\n" +
             "  null\n" +
             ")\n"
             );
        vet6730( ruthConnection );

        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_1', 'poemText' )" );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_2', '\"poemText\"' )" );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_3', 'poemText' )" );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_4', '\"poemText\"' )" );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_5', '\"c l o b\"' )" );
        goodStatement( ruthConnection, "call LuceneSupport.dropIndex( 'ruth', 'v_6730_6', 'poemText' )" );
        goodStatement( dboConnection, UNLOAD_TOOL );
        goodStatement( ruthConnection, "drop view v_6730_6" );
        goodStatement( ruthConnection, "drop view v_6730_5" );
        goodStatement( ruthConnection, "drop view v_6730_4" );
        goodStatement( ruthConnection, "drop view v_6730_3" );
        goodStatement( ruthConnection, "drop view v_6730_2" );
        goodStatement( ruthConnection, "drop view v_6730_1" );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        dropSchema( ruthConnection );
    }
    private void vet6730( Connection conn ) throws Exception
    {
        // Verify the expected casing of identifiers
        String[][]  expectedResults = new String[][]
        {
            { "5", "0.26756266" },
            { "4", "0.22933942" },
            { "3", "0.22933942" },
        };
        assertResults
            (
             conn,
             "select i.poemID, i.score\n" +
             "from table ( ruth.v_6730_1__poemText( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
             expectedResults,
             false
             );
        assertResults
            (
             conn,
             "select i.poemID, i.score\n" +
             "from table ( ruth.\"V_6730_2__poemText\"( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
             expectedResults,
             false
             );
        assertResults
            (
             conn,
             "select i.\"poemID\", i.score\n" +
             "from table ( ruth.v_6730_3__poemText( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
             expectedResults,
             false
             );
        assertResults
            (
             conn,
             "select i.\"poemID\", i.score\n" +
             "from table ( ruth.\"V_6730_4__poemText\"( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
             expectedResults,
             false
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
        assertResults
            (
             conn,
             "select i.poemID, i.score\n" +
             "from table ( ruth.\"V_6730_5__c l o b\"( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
            expectedResults,
             false
             );
        assertResults
            (
             conn,
             "select i.\"k e y\", i.score\n" +
             "from table ( ruth.v_6730_6__poemText( 'star', 1000, null ) ) i\n" +
             "order by i.score desc\n",
             expectedResults,
             false
             );
        
        String[][]  expectedListResults = new String[][]
        {
            { "RUTH", "V_6730_1", "POEMTEXT", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
            { "RUTH", "V_6730_2", "poemText", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
            { "RUTH", "V_6730_3", "POEMTEXT", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
            { "RUTH", "V_6730_4", "poemText", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
            { "RUTH", "V_6730_5", "c l o b", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
            { "RUTH", "V_6730_6", "POEMTEXT", "org.apache.lucene.analysis.en.EnglishAnalyzer" },
        };
        assertResults
            (
             conn,
             "select schemaName, tableName, columnName, analyzer\n" +
             "from table( lucenesupport.listIndexes() ) t\n" +
             "order by schemaName, tableName, columnName\n",
             expectedListResults,
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void    createSchema( Connection ruthConnection, int jdbcType )  throws Exception
    {
        createPoemsTable( ruthConnection, jdbcType );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        createLocaleFunction( ruthConnection );
    }
    private void    createPoemsTable( Connection conn, int jdbcType )
        throws Exception
    {
        goodStatement
            (
             conn,
             "create table poems\n" +
             "(\n" +
             "    poemID " + getType( jdbcType ) + ",\n" +
             "    versionStamp int not null,\n" +
             "    originalAuthor       varchar( 50 ),\n" +
             "    lastEditor           varchar( 50 ),\n" +
             "    poemText            clob,\n" +
             "    constraint poemsKey primary key( poemID, versionStamp )\n" +
             ")\n"
             );

        PreparedStatement   ps = conn.prepareStatement( "insert into poems values ( ?, ?, ?, ?, ? )" );

        int     poemID = 1;
        int     versionStamp = 1;

        setNextPoemID( ps, jdbcType, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Geoffrey Chaucer" );
        ps.setString( 4, "Geoffrey Chaucer" );
        ps.setString( 5, "Whan that Aprill, with his shoures soote The droghte of March hath perced to the roote And bathed every veyne in swich licour, Of which vertu engendred is the flour;" );
        ps.executeUpdate();

        setNextPoemID( ps, jdbcType, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Andrew Marvell" );
        ps.setString( 4, "Andrew Marvell" );
        ps.setString( 5, "Had we but world enough, and time, This coyness, lady, were no crime." );
        ps.executeUpdate();

        setNextPoemID( ps, jdbcType, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "John Milton" );
        ps.setString( 4, "John Milton" );
        ps.setString( 5, "From morn to noon he fell, from noon to dewy eve, a summers day, and with the setting sun dropped from the ze4ith like a falling star on Lemnos, the Aegean isle" );
        ps.executeUpdate();

        setNextPoemID( ps, jdbcType, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Lord Byron" );
        ps.setString( 4, "Lord Byron" );
        ps.setString( 5, "The Assyrian came down like the wolf on the fold, And his cohorts were gleaming in purple and gold; And the sheen of their spears was like stars on the sea, When the blue wave rolls nightly on deep Galilee." );
        ps.executeUpdate();

        setNextPoemID( ps, jdbcType, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Walt Whitman" );
        ps.setString( 4, "Walt Whitman" );
        ps.setString( 5, "When lilacs last in the dooryard bloomd, And the great star early droopd in the western sky in the night, I mournd, and yet shall mourn with ever-returning spring." );
        ps.executeUpdate();

        ps.close();
    }

    private void    createPoemView( Connection conn )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        goodStatement
            (
             conn,
             "create view poemView as select poemID, versionStamp, poemText from poems"
             );
    }
    
    private void    createLocaleFunction( Connection conn )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws Exception
    {
        goodStatement
            (
             conn,
             "create function getDatabaseLocale() returns varchar( 20 )\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest.getDatabaseLocale()'\n"
             );
    }
    
    private String  getType( int jdbcType ) throws Exception
    {
        switch( jdbcType )
        {
        case Types.BINARY: return "char( 100 ) for bit data";
        case Types.BOOLEAN: return "boolean";
        case Types.DECIMAL: return "decimal";
        case Types.INTEGER: return "int";
        case Types.BIGINT: return "bigint";
        case Types.SMALLINT: return "smallint";
        case Types.DOUBLE: return "double";
        case Types.FLOAT: return "real";
        case Types.NUMERIC: return "numeric";
        case Types.CHAR: return "char( 5 )";
        case Types.REAL: return "real";
        case Types.VARCHAR: return "varchar( 5 )";
        case Types.VARBINARY: return "varchar( 256 ) for bit data";
        case Types.DATE: return "date";
        case Types.TIME: return "time";
        case Types.TIMESTAMP: return "timestamp";

        default:    throw new Exception( "Unsupported datatype: " + jdbcType );
        }
    }

    private void    setNextPoemID( PreparedStatement ps, int jdbcType, int intPoemID )
        throws Exception
    {
        switch( jdbcType )
        {
        case Types.BINARY:
            ps.setBytes( 1, makeBytes( intPoemID ) );
            break;

        case Types.BOOLEAN:
            ps.setBoolean( 1, (intPoemID % 2 == 0) ? true : false );
            break;

        case Types.DECIMAL:
        case Types.NUMERIC:
            ps.setBigDecimal( 1, new BigDecimal( intPoemID ) );
            break;

        case Types.INTEGER:
            ps.setInt( 1, intPoemID );
            break;

        case Types.BIGINT:
            ps.setLong( 1, (long) intPoemID + (long) Integer.MAX_VALUE );
            break;

        case Types.SMALLINT:
            ps.setShort( 1, (short) intPoemID );
            break;

        case Types.DOUBLE:
            ps.setDouble( 1, (double) intPoemID );
            break;

        case Types.FLOAT:
        case Types.REAL:
            ps.setFloat( 1, (float) intPoemID );
            break;

        case Types.CHAR:
        case Types.VARCHAR:
            ps.setString( 1, makeStringKey( intPoemID ) );
            break;

        case Types.VARBINARY:
            ps.setBytes( 1, makeAllBytes( intPoemID ) );
            break;

        case Types.DATE:
            ps.setDate( 1, new Date( MILLIS_IN_DAY * (long)(500 + intPoemID) ) );
            break;

        case Types.TIME:
            ps.setTime( 1, new Time( MILLIS_IN_HOUR * (long)(intPoemID) ) );
            break;

        case Types.TIMESTAMP:
            ps.setTimestamp( 1, new Timestamp( MILLIS_IN_DAY * (long)(500 + intPoemID) ) );
            break;

        default:    throw new Exception( "Unsupported datatype: " + jdbcType );
        }
    }
    private String  makeStringKey( int key )
    {
        String  digit = Integer.toString( key );
        return digit + digit + digit + digit + digit;
    }

    private void    dropSchema( Connection ruthConnection )    throws Exception
    {
        goodStatement( ruthConnection, "drop table poems" );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        goodStatement( ruthConnection, "drop function getDatabaseLocale" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PROCEDURES AND FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the database locale */
    public  static  String  getDatabaseLocale()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        return ConnectionUtil.getCurrentLCC().getDatabase().getLocale().toString();
    }
    
    public  static  String  toString( byte[] value )
    {
        if ( value == null ) { return null; }

        return Arrays.toString( value );
    }

    /** Make a byte array with all possible byte values in it */
    public  static  byte[]  makeAllBytes( int initialValue )
    {
        int     size = 2 * ( -Byte.MIN_VALUE );
        byte[]  result = new byte[ size ];
        int     value = initialValue;

        if ( value < Byte.MIN_VALUE ) { value = Byte.MIN_VALUE; }
        if ( value > Byte.MAX_VALUE ) { value = Byte.MAX_VALUE; }

        for ( int idx = 0; idx < size; idx++ )
        {
            result[ idx ] = (byte) (value++);

            if ( value > Byte.MAX_VALUE ) { value = Byte.MIN_VALUE; }
        }

        return result;
    }
    
    /** Make a byte array starting with the given byte */
    public  static  byte[]  makeBytes( int initialValue )
    {
        byte[]  result = new byte[ initialValue ];

        for ( int idx = 0; idx < initialValue; idx++ )
        {
            result[ idx ] = (byte) initialValue;
        }

        return result;
    }

    /** Alternative QueryParser maker, which forces the text to be a constant string */
    public  static  LuceneIndexDescriptor constantStringIndexDescriptor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        return new ConstantIndexDescriptor();
    }
    
    /**
     * Delete a file. If it's a directory, recursively delete all directories
     * and files underneath it first.
     */
    private boolean deleteFile( File file )
        throws IOException, PrivilegedActionException
    {
        boolean retval = true;
        
        if ( isDirectory( file ) )
        {
            for ( File child : listFiles( file ) ) { retval = retval && deleteFile( child ); }
        }

        return retval && clobberFile( file );
    }

    /** Return true if the file is a directory */
    private boolean isDirectory( final File file )
        throws IOException, PrivilegedActionException
    {
        return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Boolean>()
             {
                public Boolean run() throws IOException
                {
                    if ( file == null ) { return false; }
                    else { return file.isDirectory(); }
                }
             }
             ).booleanValue();
    }

    /** Really delete a file */
    private boolean clobberFile( final File file )
        throws IOException, PrivilegedActionException
    {
        return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Boolean>()
             {
                public Boolean run() throws IOException
                {
                    return file.delete();
                }
             }
             ).booleanValue();
    }

    /** List files */
    private File[]  listFiles( final File file )
        throws IOException, PrivilegedActionException
    {
        return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<File[]>()
             {
                public File[] run() throws IOException
                {
                    return file.listFiles();
                }
             }
             );
    }

    public  static  void    setFieldNames( String... fieldNames ) { _fieldNames = fieldNames; }

    public  static  LuceneIndexDescriptor   makeFieldNameIndexDescriptor() { return new FieldNameIndexDescriptor(); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  class   ConstantQueryParser extends MultiFieldQueryParser
    {
        public  ConstantQueryParser
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             Version version,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             String[] fieldNames,
             Analyzer analyzer
             )
        {
            super( version, fieldNames, analyzer );
        }

        public Query parse( String query )  throws ParseException
        {
            return super.parse( "one" );
        }
    }

    public  static  class   CD
    {
        public  final   String  name;
        public  final   String  type;

        public  CD( String Name, String Type )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6602
            name = Name;
            type = Type;
        }

        public  String  toString() { return "[ " + name + ", " + type + " ]"; }
    }

    public  static  class   ConstantIndexDescriptor extends LuceneUtils.DefaultIndexDescriptor
    {
        public  ConstantIndexDescriptor() { super(); }
        
        public  QueryParser getQueryParser()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            throws SQLException
        {
            return new ConstantQueryParser
                (
                 LuceneUtils.currentVersion(),
                 getFieldNames(),
                 getAnalyzer()
                 );
        }
    }

    public  static  class   FieldNameIndexDescriptor extends LuceneUtils.DefaultIndexDescriptor
    {
        public  FieldNameIndexDescriptor() { super(); }
        public  String[]    getFieldNames() { return LuceneSupportPermsTest._fieldNames; }
    }
    
}

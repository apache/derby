/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneCoarseAuthorizationTest

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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derby.optional.api.LuceneIndexDescriptor;
import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.lucene.analysis.Analyzer;

/**
 * <p>
 * Test permissions on objects created by the optional Lucene support tool.
 * </p>
 */
public class LuceneCoarseAuthorizationTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      RUTH = "RUTH";
    private static  final   String      READ_ONLY_USER = "READONLYUSER";
    private static  final   String      READ_WRITE_USER = "READWRITEUSER";
    private static  final   String[]    LEGAL_USERS = { RUTH, READ_ONLY_USER, READ_WRITE_USER };

    public  static  final   String      ENGLISH_ANALYZER =
//IC see: https://issues.apache.org/jira/browse/DERBY-6544
        "org.apache.derbyTesting.functionTests.tests.lang.LuceneCoarseAuthorizationTest.getEnglishAnalyzer";
    public  static  final   String      STANDARD_ANALYZER =
        "org.apache.derbyTesting.functionTests.tests.lang.LuceneCoarseAuthorizationTest.getStandardAnalyzer";
//IC see: https://issues.apache.org/jira/browse/DERBY-590

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      INDEX_POEMS =
        "call LuceneSupport.createIndex( 'ruth', 'poems', 'poemText', '" + ENGLISH_ANALYZER + "' )";
    private static  final   String      UPDATE_POEMS_INDEX =
        "call LuceneSupport.updateIndex( 'ruth', 'poems', 'poemText', '" + STANDARD_ANALYZER + "' )";
    private static  final   String      DROP_POEMS_INDEX = "call LuceneSupport.dropIndex( 'ruth', 'poems', 'poemText' )";

    private static  final   String      ILLEGAL_FOR_READONLY = "25502";

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

    public LuceneCoarseAuthorizationTest(String name)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            LuceneCoarseAuthorizationTest.class);

        Test        unsecureTest = SecurityManagerSetup.noSecurityManager( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( unsecureTest, LEGAL_USERS, "LuceneCoarsePermissions" );

        Test        coarseTest = new DatabasePropertyTestSetup( authenticatedTest, makeProperties() );
        Test        singleUseTest = TestConfiguration.singleUseDatabaseDecorator( coarseTest );
//IC see: https://issues.apache.org/jira/browse/DERBY-6544

        return singleUseTest;
    }
    private static  Properties  makeProperties()
    {
        Properties  props = new Properties();

        props.setProperty(  "derby.database.fullAccessUsers", RUTH + "," + READ_WRITE_USER );
        props.setProperty(  "derby.database.readOnlyAccessUsers", READ_ONLY_USER );

        return props;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that read-only users can't (un)load the tool or create/update indexes. But
     * they can view data.
     * </p>
     */
    public  void    test_001_basic()
        throws Exception
    {
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  readOnlyConnection = openUserConnection( READ_ONLY_USER );
        Connection  readWriteConnection = openUserConnection( READ_WRITE_USER );

        createSchema( ruthConnection );

        expectExecutionError
            (
             readOnlyConnection,
             ILLEGAL_FOR_READONLY,
             LOAD_TOOL
             );
        goodStatement( readWriteConnection, LOAD_TOOL );

        expectExecutionError
            (
             readOnlyConnection,
             ILLEGAL_FOR_READONLY,
             INDEX_POEMS
             );
        goodStatement( readWriteConnection, INDEX_POEMS );

        String  readPoemsIndex =
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "select p.originalAuthor, i.score\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "from ruth.poems p, table ( ruth.poems__poemText( 'star', 1000, null ) ) i\n" +
            "where p.poemID = i.poemID and p.versionStamp = i.versionStamp\n" +
            "order by i.score desc\n";
        String[][]  defaultPoemResults =
            new String[][]
            {
                { "Walt Whitman", "0.26756266" },
                { "Lord Byron", "0.22933942" },
                { "John Milton", "0.22933942" },
            };

        assertResults
            (
             readOnlyConnection,
             readPoemsIndex,
             defaultPoemResults,
             false
             );
        assertResults
            (
             readWriteConnection,
             readPoemsIndex,
             defaultPoemResults,
             false
             );

        String  listIndexes =
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            "select schemaName, tableName, columnName, indexDescriptorMaker from table( LuceneSupport.listIndexes() ) l";
        String[][]  defaultIndexList =
            new String[][]
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6544
                { "RUTH", "POEMS", "POEMTEXT", ENGLISH_ANALYZER },
            };

        assertResults
            (
             readOnlyConnection,
             listIndexes,
             defaultIndexList,
             false
             );
        assertResults
            (
             readWriteConnection,
             listIndexes,
             defaultIndexList,
             false
             );

        expectExecutionError
            (
             readOnlyConnection,
             ILLEGAL_FOR_READONLY,
             UPDATE_POEMS_INDEX
             );
        goodStatement( readWriteConnection, UPDATE_POEMS_INDEX );

        String[][]  standardPoemResults =
            new String[][]
            {
                { "Walt Whitman", "0.3304931" },
                { "John Milton", "0.2832798" },
            };

        assertResults
            (
             readOnlyConnection,
             readPoemsIndex,
             standardPoemResults,
             false
             );
        assertResults
            (
             readWriteConnection,
             readPoemsIndex,
             standardPoemResults,
             false
             );

        String[][]  standardIndexList =
            new String[][]
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                { "RUTH", "POEMS", "POEMTEXT", STANDARD_ANALYZER },
            };

        assertResults
            (
             readOnlyConnection,
             listIndexes,
             standardIndexList,
             false
             );
        assertResults
            (
             readWriteConnection,
             listIndexes,
             standardIndexList,
             false
             );

        expectExecutionError
            (
             readOnlyConnection,
             ILLEGAL_FOR_READONLY,
             DROP_POEMS_INDEX
             );
        goodStatement( readWriteConnection, DROP_POEMS_INDEX );

        expectExecutionError
            (
             readOnlyConnection,
             ILLEGAL_FOR_READONLY,
             UNLOAD_TOOL
             );
        goodStatement( readWriteConnection, UNLOAD_TOOL );

        dropSchema( ruthConnection );
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // EXTERNAL ENTRY POINTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return an index descriptor with an Analyzer for an English Locale */
    public  static  LuceneIndexDescriptor    getEnglishAnalyzer()
        throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        return new EnglishIndexDescriptor();
    }
    
    /** Return an index descriptor with a StandardAnalyzer */
    public  static  LuceneIndexDescriptor    getStandardAnalyzer()
        throws Exception
    {
        return new StandardIndexDescriptor();
    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static void    createSchema( Connection conn )  throws Exception
    {
        createPoemsTable( conn );
    }
    public  static void    dropSchema( Connection conn )   throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        conn.prepareStatement( "drop table poems" ).execute();
    }
    public static void    createPoemsTable( Connection conn )
        throws Exception
    {
        conn.prepareStatement
            (
             "create table poems\n" +
             "(\n" +
             "    poemID int,\n" +
             "    versionStamp int not null,\n" +
             "    originalAuthor       varchar( 50 ),\n" +
             "    lastEditor           varchar( 50 ),\n" +
             "    poemText            clob,\n" +
             "    constraint poemsKey primary key( poemID, versionStamp )\n" +
             ")\n"
             ).execute();
//IC see: https://issues.apache.org/jira/browse/DERBY-590

        PreparedStatement   ps = conn.prepareStatement( "insert into poems values ( ?, ?, ?, ?, ? )" );

        int     poemID = 1;
        int     versionStamp = 1;

        ps.setInt( 1, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Geoffrey Chaucer" );
        ps.setString( 4, "Geoffrey Chaucer" );
        ps.setString( 5, "Whan that Aprill, with his shoures soote The droghte of March hath perced to the roote And bathed every veyne in swich licour, Of which vertu engendred is the flour;" );
        ps.executeUpdate();

        ps.setInt( 1, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Andrew Marvell" );
        ps.setString( 4, "Andrew Marvell" );
        ps.setString( 5, "Had we but world enough, and time, This coyness, lady, were no crime." );
        ps.executeUpdate();

        ps.setInt( 1, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "John Milton" );
        ps.setString( 4, "John Milton" );
        ps.setString( 5, "From morn to noon he fell, from noon to dewy eve, a summers day, and with the setting sun dropped from the ze4ith like a falling star on Lemnos, the Aegean isle" );
        ps.executeUpdate();

        ps.setInt( 1, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Lord Byron" );
        ps.setString( 4, "Lord Byron" );
        ps.setString( 5, "The Assyrian came down like the wolf on the fold, And his cohorts were gleaming in purple and gold; And the sheen of their spears was like stars on the sea, When the blue wave rolls nightly on deep Galilee." );
        ps.executeUpdate();

        ps.setInt( 1, poemID++ );
        ps.setInt( 2, versionStamp++ );
        ps.setString( 3, "Walt Whitman" );
        ps.setString( 4, "Walt Whitman" );
        ps.setString( 5, "When lilacs last in the dooryard bloomd, And the great star early droopd in the western sky in the night, I mournd, and yet shall mourn with ever-returning spring." );
        ps.executeUpdate();

        ps.close();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  class   EnglishIndexDescriptor extends LuceneUtils.DefaultIndexDescriptor
    {
        public  EnglishIndexDescriptor() { super(); }
        
        public  Analyzer    getAnalyzer()   throws SQLException
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            return LuceneUtils.getAnalyzerForLocale( Locale.US );
        }
    }

    public  static  class   StandardIndexDescriptor extends LuceneUtils.DefaultIndexDescriptor
    {
        public  StandardIndexDescriptor() { super(); }
        
        public  Analyzer    getAnalyzer()   throws SQLException
        {
            return LuceneUtils.standardAnalyzer();
        }
    }


}

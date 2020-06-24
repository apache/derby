/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneCollationTest

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
import junit.framework.Test;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test that the Lucene plugin works on databases with territory based collation enabled.
 * </p>
 */
public class LuceneCollationTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      RUTH = "RUTH";

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      INDEX_POEMS = "call LuceneSupport.createIndex( 'ruth', 'poems', 'poemText', null )";
    private static  final   String      UPDATE_POEMS_INDEX = "call LuceneSupport.updateIndex( 'ruth', 'poems', 'poemText', null )";
    private static  final   String      DROP_POEMS_INDEX = "call LuceneSupport.dropIndex( 'ruth', 'poems', 'poemText' )";

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

    public LuceneCollationTest(String name)
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
        Test        collationTest = Decorator.territoryCollatedDatabase
            (
             TestConfiguration.embeddedSuite( LuceneCollationTest.class ),
             "en"
             );

        //
        // Turn off the security manager. trying to override the default security manager
        // with a lucene-specific policy file fails because the decorators don't compose correctly.
        //
        return SecurityManagerSetup.noSecurityManager( collationTest );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Verify that the collation is what we expect.
     * </p>
     */
    public  void    test_001_collation()
        throws Exception
    {
        Connection  conn = getConnection();

        assertResults
            (
             conn,
             "values syscs_util.syscs_get_database_property( 'derby.database.collation' )",
             new String[][]
             {
                 { "TERRITORY_BASED" },
             },
             true
             );
    }

    /**
     * <p>
     * Verify that basic operations work with collation turned on.
     * </p>
     */
    public  void    test_002_basic()
        throws Exception
    {
        Connection  conn = openUserConnection( RUTH );

        createSchema( conn );

        goodStatement( conn, LOAD_TOOL );
        goodStatement( conn, INDEX_POEMS );
        goodStatement( conn, UPDATE_POEMS_INDEX );

        assertResults
            (
             conn,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select * from table ( ruth.poems__poemText( 'star', 1000, null ) ) luceneResults order by poemID",
             new String[][]
             {
                 { "3", "3", "2", "0.22933942" },
                 { "4", "4", "3", "0.22933942" },
                 { "5", "5", "4", "0.26756266" },
             },
             false
             );

        assertResults
            (
             conn,
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             "select schemaName, tableName, columnName from table ( LuceneSupport.listIndexes() ) listindexes",
             new String[][]
             {
                 { "RUTH", "POEMS", "POEMTEXT" },
             },
             false
             );
        
        goodStatement( conn, DROP_POEMS_INDEX );
        goodStatement( conn, UNLOAD_TOOL );

        dropSchema( conn );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void    createSchema( Connection conn )  throws Exception
    {
        createPoemsTable( conn );
    }
    private void    createPoemsTable( Connection conn )
        throws Exception
    {
        goodStatement
            (
             conn,
             "create table poems\n" +
             "(\n" +
             "    poemID int,\n" +
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

    private void    dropSchema( Connection conn )    throws Exception
    {
        goodStatement( conn, "drop table poems" );
    }
    
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneInMemoryTest

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

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.functionTests.tests.memorydb.MemoryDbManager;

/**
 * <p>
 * Test permissions on objects created by the optional Lucene support tool.
 * </p>
 */
public class LuceneInMemoryTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      ENGLISH_ANALYZER =
        "org.apache.derbyTesting.functionTests.tests.lang.LuceneCoarseAuthorizationTest.getEnglishAnalyzer";

    private static  final   String      LOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', true )";
    private static  final   String      UNLOAD_TOOL = "call syscs_util.syscs_register_tool( 'luceneSupport', false )";
    private static  final   String      INDEX_POEMS =
        "call LuceneSupport.createIndex( 'app', 'poems', 'poemText', '" + ENGLISH_ANALYZER + "' )";
    private static  final   String      DROP_POEMS_INDEX = "call LuceneSupport.dropIndex( 'app', 'poems', 'poemText' )";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Helper for dealing with memory databases. For now we use a single
     * instance for all test classes / cases, as the tests are run single
     * threaded.
     */
    private static final MemoryDbManager dbm = MemoryDbManager.getSharedInstance();

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public LuceneInMemoryTest(String name)
    {
        super( name );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    public static Test suite()
    {
		TestSuite suite = new TestSuite( "LuceneInMemoryTest" );
        
        Test    baseTest = TestConfiguration.embeddedSuite( LuceneInMemoryTest.class );
		suite.addTest( SecurityManagerSetup.noSecurityManager( baseTest) );

        return suite;
    }

    /**
     * Closes all opened statements and connections that are known, and also
     * deletes all known in-memory databases.
     *
     * @throws Exception if something goes wrong
     */
    public void tearDown()  throws Exception
    {
        dbm.cleanUp();
        super.tearDown();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that you can create lucene indexes in an in-memory database.
     * </p>
     */
    public  void    test_001_basic()
        throws Exception
    {
        Connection  conn = dbm.createDatabase( "luceneMemDB" );

        LuceneCoarseAuthorizationTest.createSchema( conn );

        goodStatement( conn, LOAD_TOOL );
        goodStatement( conn, INDEX_POEMS );

        String  readPoemsIndex =
            "select p.originalAuthor, i.score\n" +
            "from poems p, table ( poems__poemText( 'star', null, 1000, null ) ) i\n" +
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
             conn,
             readPoemsIndex,
             defaultPoemResults,
             false
             );

        String  listIndexes =
            "select schemaName, tableName, columnName, analyzerMaker from table( LuceneSupport.listIndexes() ) l";
        String[][]  defaultIndexList =
            new String[][]
            {
                { "APP", "POEMS", "POEMTEXT", ENGLISH_ANALYZER },
            };

        assertResults
            (
             conn,
             listIndexes,
             defaultIndexList,
             false
             );

        goodStatement( conn, DROP_POEMS_INDEX );
        goodStatement( conn, UNLOAD_TOOL );

        LuceneCoarseAuthorizationTest.dropSchema( conn );
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


}

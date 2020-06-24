/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UserLobTest

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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Additional tests for Blob/Clobs created from user-supplied large objects.
 * </p>
 */
public class UserLobTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public UserLobTest(String name)
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
            UserLobTest.class);

        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that user-defined LOBs can be stored in varbinary and varchar columns.
     * </p>
     */
    public  void    test_001_casts()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create some user defined functions which return lobs.
        // 
        goodStatement
            (
             conn,
             "create function f_2201_blob_1\n" +
             "(\n" +
             "	a_0 varchar( 10 )\n" +
             ")\n" +
             "returns blob\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.AnsiSignatures.blob_Blob_String'\n"
             );
        goodStatement
            (
             conn,
             "create function f_2201_clob_1\n" +
             "(\n" +
             "	a_0 varchar( 10 )\n" +
             ")\n" +
             "returns clob\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.AnsiSignatures.clob_Clob_String'\n"
             );

        //
        // Create some tables for storing the results of these functions
        // 
        goodStatement
            (
             conn,
             "create table t_2201_clob_blob_1( a clob, b blob )\n"
             );
        goodStatement
            (
             conn,
             "create table t_2201_chartypes_1( a char( 10 ), b varchar( 10 ), c long varchar )\n"
              );
        
        //
        // Successfully insert into these tables, casting as necessary.
        // 
        goodStatement
            (
             conn,
             "insert into t_2201_clob_blob_1( a, b ) values( f_2201_clob_1( 'abc' ), f_2201_blob_1( 'abc' ) )\n"
              );
        goodStatement
            (
             conn,
             "insert into t_2201_chartypes_1( a, b, c )\n" +
             "values\n" +
             "(\n" +
             "  cast( f_2201_clob_1( 'abc' ) as char( 10)),\n" +
             "  cast( f_2201_clob_1( 'def' ) as varchar( 10)),\n" +
             "  cast( f_2201_clob_1( 'ghi' ) as long varchar )\n" +
             ")\n"
              );
        assertResults
            (
             conn,
             "select * from t_2201_clob_blob_1",
             new String[][]
             {
                 { "abc" ,         "616263" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_2201_chartypes_1",
             new String[][]
             {
                 { "abc       ", "def", "ghi" },
             },
             false
             );
        assertResults
            (
             conn,
             "select length( a ), length( b ), length( c ) from t_2201_chartypes_1",
             new String[][]
             {
                 { "10", "3", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values\n" +
             "(\n" +
             "  length( cast( f_2201_clob_1( 'abc' ) as char( 10)) ),\n" +
             "  length( cast( f_2201_clob_1( 'defg' ) as varchar( 10)) ),\n" +
             "  length( cast( f_2201_clob_1( 'hijkl' ) as long varchar ) ),\n" +
             "  length( f_2201_clob_1( 'mnopqr' ) )\n" +
             ")\n",
             new String[][]
             {
                 { "10", "4", "5", "6" },
             },
             false
             );
        assertResults
            (
             conn,
             "select length( a ), length( b ) from t_2201_clob_blob_1",
             new String[][]
             {
                 { "3", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( varchar( f_2201_clob_1( 'abc' ) ) )",
             new String[][]
             {
                 { "abc" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( substr( f_2201_clob_1( 'abc' ), 2, 2 ), upper( f_2201_clob_1( 'defg' ) ) )",
             new String[][]
             {
                 { "bc", "DEFG" },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


}

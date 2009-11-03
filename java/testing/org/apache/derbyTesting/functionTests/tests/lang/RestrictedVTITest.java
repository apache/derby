/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest

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
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * <p>
 * Test RestrictedVTIs. See DERBY-4357.
 * </p>
 */
public class RestrictedVTITest  extends GeneratedColumnsHelper
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

    public RestrictedVTITest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(RestrictedVTITest.class);
        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    protected void    setUp()
        throws Exception
    {
        super.setUp();

        Connection conn = getConnection();

        if ( !routineExists( conn, "GETLASTPROJECTION" ) )
        {
            goodStatement
                (
                 conn,
                 "create function getLastProjection\n" +
                 "()\n" +
                 "returns varchar( 32672 )\n" +
                 "language java parameter style java no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.IntegerArrayVTI.getLastProjection'\n"
                 );
        }
        if ( !routineExists( conn, "GETLASTRESTRICTION" ) )
        {
            goodStatement
                (
                 conn,
                 "create function getLastRestriction\n" +
                 "()\n" +
                 "returns varchar( 32672 )\n" +
                 "language java parameter style java no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.IntegerArrayVTI.getLastRestriction'\n"
                 );
        }
        if ( !routineExists( conn, "INTEGERLIST" ) )
        {
            goodStatement
                (
                 conn,
                 "create function integerList()\n" +
                 "returns table( s_r int, s_nr int, ns_r int, ns_nr int )\n" +
                 "language java\n" +
                 "parameter style derby_jdbc_result_set\n" +
                 "no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.integerList'\n"
                 );
        }
        if ( !routineExists( conn, "NULLABLEINTEGERLIST" ) )
        {
            goodStatement
                (
                 conn,
                 "create function nullableIntegerList()\n" +
                 "returns table( s_r int, s_nr int, ns_r int, ns_nr int )\n" +
                 "language java\n" +
                 "parameter style derby_jdbc_result_set\n" +
                 "no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.nullableIntegerList'\n"
                 );
        }
        if ( !tableExists( conn, "T_4357_1" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t_4357_1( a int )\n"
                 );
            goodStatement
                (
                 conn,
                 "insert into t_4357_1( a ) values cast( null as int), ( 1 ), ( 100 ), ( 1000 ), ( 10000)\n"
                 );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Heartbeat test to verify that projections and restrictions are being
     * pushed into RestrictedVTIs.
     * </p>
     */
    public void test_01_heartbeat() throws Exception
    {
        Connection conn = getConnection();

        // test basic pushing of projection and restriction
        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where s_r > 1 and ns_r < 3000\n",
             new String[][]
             {
                 { "100" ,         "200"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "( \"NS_R\" < 3000 ) AND ( \"S_R\" > 1 )"
             );
        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where s_r > 1 and ns_r < 3000 order by s_r\n",
             new String[][]
             {
                 { "100" ,         "200"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "( \"NS_R\" < 3000 ) AND ( \"S_R\" > 1 )"
             );

        // order by with no restriction
        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s order by s_r\n",
             new String[][]
             {
                 { "1" ,         "2"  },
                 { "100" ,         "200"  },
                 { "1000" ,         "2000"  },
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             null
             );

        // similar test except with a ? parameter
        PreparedStatement ps = chattyPrepare
            (
             conn,
             "select s_r from table( integerList() ) s where s_r > 1 and ns_r < ?"
             );
        ps.setInt( 1, 30000 );
        ResultSet rs = ps.executeQuery();
        assertResults
            (
             rs,
             new String[][]
             {
                 { "100" },
                 { "1000" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( getLastProjection() )\n",
             new String[][] { { "[S_R, null, NS_R, null]" } },
             false
             );
        assertResults
            (
             conn,
             "values ( getLastRestriction() )\n",
             new String[][] { { "( \"NS_R\" < 30000 ) AND ( \"S_R\" > 1 )" } },
             false
             );
        
        // similar to the first test except NOT the qualification
        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where not( s_r > 1 and ns_r < 3000 )\n",
             new String[][]
             {
                 { "1" ,         "2"  },
                 { "1000" ,         "2000"  },
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "( \"S_R\" <= 1 ) OR ( \"NS_R\" >= 3000 )"
             );
    }

    /**
     * <p>
     * Verify that aliases are correctly mapped to table column names. Also
     * verify that SELECT list expressions cause columns to be included in the
     * column list. Also verify that predicates which aren't qualifiers are not included in the restriction.
     * </p>
     */
    public void test_02_aliasing() throws Exception
    {
        Connection conn = getConnection();

        assertPR
            (
             conn,
             "select 2*w, x from table( integerList() ) as s( w, x, y, z ) where w > 1 and mod( y, 3 ) = 0\n",
             new String[][]
             {
                 { "200" ,         "200"  },
                 { "2000" ,         "2000"  },
                 { "20000" ,         "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "\"S_R\" > 1"
             );
    }
    
    /**
     * <p>
     * Verify that all relational operators are handled.
     * </p>
     */
    public void test_03_allRelationalOperators() throws Exception
    {
        Connection conn = getConnection();

        // IS NULL
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r is null\n",
             new String[][]
             {
                 { null ,         "2"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" IS NULL "
             );

        // IS NOT NULL
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r is not null\n",
             new String[][]
             {
                 { "100",         null  },
                 { "1000",         "2000"  },
                 { "10000",         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" IS NOT NULL "
             );

        // <
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r < 1000\n",
             new String[][]
             {
                 { "100",         null  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" < 1000"
             );

        // <=
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r <= 100\n",
             new String[][]
             {
                 { "100",         null  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" <= 100"
             );

        // =
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r = 100\n",
             new String[][]
             {
                 { "100",         null  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" = 100"
             );

        // >
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r > 100\n",
             new String[][]
             {
                 { "1000",         "2000"  },
                 { "10000",         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" > 100"
             );

        // >=
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r >= 100\n",
             new String[][]
             {
                 { "100",         null  },
                 { "1000",         "2000"  },
                 { "10000",         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" >= 100"
             );
    }
    
    /**
     * <p>
     * Miscellaneous conditions.
     * </p>
     */
    public void test_04_misc() throws Exception
    {
        Connection conn = getConnection();

        // Arithmetic expressions are not qualifiers.
        assertPR
            (
             conn,
             "select s_r, s_nr from table( nullableIntegerList() ) s where s_r < s_nr + ns_r\n",
             new String[][]
             {
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             null
             );

        // Casting a literal to an int is computed by the compiler and so is a qualifier
        assertPR
            (
             conn,
             "select s_r from table( nullableIntegerList() ) s where ns_r = cast( '300' as int)\n",
             new String[][]
             {
                 { "100"  },
             },
             "[S_R, null, NS_R, null]",
             "\"NS_R\" = 300"
             );

    }

    /**
     * <p>
     * Test joins to RestrictedVTIs.
     * </p>
     */
    public void test_05_joins() throws Exception
    {
        Connection conn = getConnection();

        // hashjoin with no restriction
        assertPR
            (
             conn,
             "select a, w, y from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w\n",
             new String[][]
             {
                 { "100" ,    "100",    "300"  },
                 { "1000" ,    "1000",    null  },
                 { "10000" ,    "10000",    "30000"  },
             },
             "[S_R, null, NS_R, null]",
             null
             );
        assertPR
            (
             conn,
             "select a, w, y from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w order by y\n",
             new String[][]
             {
                 { "100" ,    "100",    "300"  },
                 { "10000" ,    "10000",    "30000"  },
                 { "1000" ,    "1000",    null  },
             },
             "[S_R, null, NS_R, null]",
             null
             );

        // hashjoin with a restriction on the table function
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and y is not null\n",
             new String[][]
             {
                 { "100" ,    "100",    null  },
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "\"NS_R\" IS NOT NULL "
             );
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and y is not null order by w\n",
             new String[][]
             {
                 { "100" ,    "100",    null  },
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "\"NS_R\" IS NOT NULL "
             );

        // hashjoin with a restriction on the base table which transitive closure
        // turns into a restriction on the table function
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and a > 100\n",
             new String[][]
             {
                 { "1000" ,    "1000",    "2000" },
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" > 100"
             );
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and a > 100 order by x\n",
             new String[][]
             {
                 { "1000" ,    "1000",    "2000" },
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" > 100"
             );

        // hashjoin with a restriction that can't be pushed into the table function
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and a + x > 100\n",
             new String[][]
             {
                 { "1000" ,    "1000",    "2000" },
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, null, null]",
             null
             );
        assertPR
            (
             conn,
             "select a, w, x from t_4357_1, table( nullableIntegerList() ) as s( w, x, y, z ) where a = w and x + y > 100\n",
             new String[][]
             {
                 { "10000" ,    "10000",    "20000"  },
             },
             "[S_R, S_NR, NS_R, null]",
             null
             );

    }

    /**
     * <p>
     * Test DISTINCT.
     * </p>
     */
    public void test_06_distinct() throws Exception
    {
        Connection conn = getConnection();

        // distinct with restriction
        assertPR
            (
             conn,
             "select distinct s_r, s_nr from table( integerList() ) s where s_r > 1 and ns_r < 3000\n",
             new String[][]
             {
                 { "100" ,         "200"  },
             },
             "[S_R, S_NR, NS_R, null]",
             "( \"NS_R\" < 3000 ) AND ( \"S_R\" > 1 )"
             );

        // distinct without restriction
        assertPR
            (
             conn,
             "select distinct s_r, s_nr from table( integerList() ) s\n",
             new String[][]
             {
                 { "1" ,         "2"  },
                 { "100" ,         "200"  },
                 { "1000" ,         "2000"  },
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             null
             );
    }

    /**
     * <p>
     * Test subqueries.
     * </p>
     */
    public void test_07_subqueries() throws Exception
    {
        Connection conn = getConnection();

        // table function in subquery
        assertPR
            (
             conn,
             "select * from t_4357_1 where exists ( select x from table( nullableIntegerList() ) as s( w, x, y, z ) where a = w )\n",
             new String[][]
             {
                 { "100"  },
                 { "1000"  },
                 { "10000"  },
             },
             "[S_R, S_NR, null, null]",
             null
             );

        // table function in inner and outer query blocks
        assertPR
            (
             conn,
             "select * from table( nullableIntegerList() ) as t( a, b, c, d ) where exists ( select x from table( nullableIntegerList() ) as s( w, x, y, z ) where a = w )\n",
             new String[][]
             {
                 { "100", null, "300", "400"  },
                 { "1000", "2000", null, "4000"  },
                 { "10000", "20000", "30000", null  },
             },
             "[S_R, S_NR, null, null]",
             null
             );

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static IntegerArrayVTI integerList()
    {
        // S => in SELECT list
        // NS => NOT in SELECT LIST
        // R => in restriction
        // NR => NOT in restriction
        return new IntegerArrayVTI
            (
             new String[] { "S_R", "S_NR", "NS_R", "NS_NR" },
             new int[][]
             {
                 new int[] { 1, 2, 3, 4 },
                 new int[] { 100, 200, 300, 400 },
                 new int[] { 1000, 2000, 3000, 4000 },
                 new int[] { 10000, 20000, 30000, 40000 },
             }
             );
    }
    
    public static IntegerArrayVTI nullableIntegerList()
    {
        // S => in SELECT list
        // NS => NOT in SELECT LIST
        // R => in restriction
        // NR => NOT in restriction
        return new IntegerArrayVTI
            (
             new String[] { "S_R", "S_NR", "NS_R", "NS_NR" },
             new Integer[][]
             {
                 new Integer[] { null, i(2), i(3), i(4) },
                 new Integer[] { i(100), null, i(300), i(400) },
                 new Integer[] { i(1000), i(2000), null, i(4000) },
                 new Integer[] { i(10000), i(20000), i(30000), null },
             }
             );
    }
    private static Integer i( int intValue ) { return new Integer( intValue ); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the SQL routine exists */
    private boolean routineExists( Connection conn, String functionName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.sysaliases where alias = ?" );
        ps.setString( 1, functionName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

    /** Return true if the table exists */
    private boolean tableExists( Connection conn, String tableName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.systables where tablename = ?" );
        ps.setString( 1, tableName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

    /**
     * <p>
     * Run a query against a RestrictedVTI and verify that the expected
     * projection and restriction are pushed into the VTI.
     * </p>
     */
    private void assertPR
        (
         Connection conn,
         String query,
         String[][] expectedResults,
         String expectedProjection,
         String expectedRestriction
         ) throws Exception
    {
        assertResults
            (
             conn,
             query,
             expectedResults,
             false
             );
        assertResults
            (
             conn,
             "values ( getLastProjection() )\n",
             new String[][] { { expectedProjection } },
             false
             );
        assertResults
            (
             conn,
             "values ( getLastRestriction() )\n",
             new String[][] { { expectedRestriction } },
             false
             );
    }

}

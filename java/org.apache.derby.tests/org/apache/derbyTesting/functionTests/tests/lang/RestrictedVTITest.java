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
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

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
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            RestrictedVTITest.class);

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
        if ( !routineExists( conn, "GETCOUNT" ) )
        {
            goodStatement
                (
                 conn,
                 "create function getCount\n" +
                 "()\n" +
                 "returns int\n" +
                 "language java parameter style java no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.IntegerArrayVTI.getLastQualifiedRowCount'\n"
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
        if ( !routineExists( conn, "INTEGERLISTSPECIALCOLNAMES" ) )
        {
            goodStatement
                (
                 conn,
                 "create function integerListSpecialColNames()\n" +
                 "returns table( \"CoL \"\"1\"\"\" int,\n" +
                 "\"cOL \"\"2\"\"\" int, col3 int, col4 int )\n" +
                 "language java\n" +
                 "parameter style derby_jdbc_result_set\n" +
                 "no sql\n" +
                 "external name '" + getClass().getName() +
                 ".integerListSpecialColNames'\n"
                 );
        }
        if ( !routineExists( conn, "MAKEBLOB5370" ) )
        {
            goodStatement
                (
                 conn,
                 "create function makeBlob5370( ) returns blob\n" +
                 "language java parameter style java no sql deterministic\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest.makeSimpleBlob'\n"
                 );
        }
        if ( !routineExists( conn, "LASTQUERY5370" ) )
        {
            goodStatement
                (
                 conn,
                 "create function lastQuery5370() returns varchar( 32672 )\n" +
                 "language java parameter style java no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedTableVTI.getLastQuery'\n"
                 );
        }
        if ( !routineExists( conn, "RESTRICTED5370" ) )
        {
            goodStatement
                (
                 conn,
                 "create function restricted5370( schemaName varchar( 32672 ), tableName varchar( 32672 ) )\n" +
                 "returns table\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    boolean_col  BOOLEAN,\n" +
                 "    bigint_col  BIGINT,\n" +
                 "    blob_col  BLOB(2147483647),\n" +
                 "    char_col  CHAR(10),\n" +
                 "    char_for_bit_data_col  CHAR (10) FOR BIT DATA,\n" +
                 "    clob_col  CLOB,\n" +
                 "    date_col  DATE,\n" +
                 "    decimal_col  DECIMAL(5,2),\n" +
                 "    real_col  REAL,\n" +
                 "    double_col  DOUBLE,\n" +
                 "    int_col  INTEGER,\n" +
                 "    long_varchar_col  LONG VARCHAR,\n" +
                 "    long_varchar_for_bit_data_col  LONG VARCHAR FOR BIT DATA,\n" +
                 "    numeric_col  NUMERIC(5,2), \n" +
                 "    smallint_col  SMALLINT,\n" +
                 "    time_col  TIME,\n" +
                 "    timestamp_col  TIMESTAMP,\n" +
                 "    varchar_col  VARCHAR(10),\n" +
                 "    varchar_for_bit_data_col  VARCHAR (10) FOR BIT DATA\n" +
                 ")\n" +
                 "language java parameter style derby_jdbc_result_set reads sql data\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedTableVTI.readTable'\n"
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
        if ( !tableExists( conn, "T_5370" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t_5370\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    boolean_col  BOOLEAN,\n" +
                 "    bigint_col  BIGINT,\n" +
                 "    blob_col  BLOB(2147483647),\n" +
                 "    char_col  CHAR(10),\n" +
                 "    char_for_bit_data_col  CHAR (10) FOR BIT DATA,\n" +
                 "    clob_col  CLOB,\n" +
                 "    date_col  DATE,\n" +
                 "    decimal_col  DECIMAL(5,2),\n" +
                 "    real_col  REAL,\n" +
                 "    double_col  DOUBLE,\n" +
                 "    int_col  INTEGER,\n" +
                 "    long_varchar_col  LONG VARCHAR,\n" +
                 "    long_varchar_for_bit_data_col  LONG VARCHAR FOR BIT DATA,\n" +
                 "    numeric_col  NUMERIC(5,2), \n" +
                 "    smallint_col  SMALLINT,\n" +
                 "    time_col  TIME,\n" +
                 "    timestamp_col  TIMESTAMP,\n" +
                 "    varchar_col  VARCHAR(10),\n" +
                 "    varchar_for_bit_data_col  VARCHAR (10) FOR BIT DATA\n" +
                 ")\n"
                 );
            goodStatement
                (
                 conn,
                 "insert into t_5370\n" +
                 "(\n" +
                 "    key_col,\n" +
                 "    boolean_col,\n" +
                 "    bigint_col,\n" +
                 "    blob_col,\n" +
                 "    char_col,\n" +
                 "    char_for_bit_data_col,\n" +
                 "    clob_col,\n" +
                 "    date_col,\n" +
                 "    decimal_col,\n" +
                 "    real_col,\n" +
                 "    double_col,\n" +
                 "    int_col,\n" +
                 "    long_varchar_col,\n" +
                 "    long_varchar_for_bit_data_col,\n" +
                 "    numeric_col, \n" +
                 "    smallint_col,\n" +
                 "    time_col,\n" +
                 "    timestamp_col,\n" +
                 "    varchar_col,\n" +
                 "    varchar_for_bit_data_col\n" +
                 ")\n" +
                 "values\n" +
                 "(\n" +
                 "    0,\n" +
                 "    false,\n" +
                 "    0,\n" +
                 "    makeBlob5370(),\n" +
                 "    '0',\n" +
                 "    X'DE',\n" +
                 "    '0',\n" +
                 "    date('1994-02-23'),\n" +
                 "    0.00,\n" +
                 "    0.0,\n" +
                 "    0.0,\n" +
                 "    0,\n" +
                 "    '0',\n" +
                 "    X'DE',\n" +
                 "    0.00, \n" +
                 "    0,\n" +
                 "    time('15:09:02'),\n" +
                 "    timestamp('1962-09-23 03:23:34.234'),\n" +
                 "    '0',\n" +
                 "    X'DE'\n" +
                 "),\n" +
                 "(\n" +
                 "    1,\n" +
                 "    true,\n" +
                 "    1,\n" +
                 "    makeBlob5370(),\n" +
                 "    '1',\n" +
                 "    X'DD',\n" +
                 "    '1',\n" +
                 "    date('1994-02-24'),\n" +
                 "    1.00,\n" +
                 "    1.0,\n" +
                 "    1.0,\n" +
                 "    1,\n" +
                 "    '1',\n" +
                 "    X'DE',\n" +
                 "    1.00, \n" +
                 "    1,\n" +
                 "    time('15:09:03'),\n" +
                 "    timestamp('1963-09-23 03:23:34.234'),\n" +
                 "    '1',\n" +
                 "    X'DD'\n" +
                 "),\n" +
                 "(\n" +
                 "    2,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null,\n" +
                 "    null\n" +
                 ")\n"
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

    /**
     * Predicates in HAVING clauses are not (yet) pushed down to the VTI.
     * Tracked as DERBY-4650.
     */
    public void test_08_having() throws Exception
    {
        assertPR(
                getConnection(),
                "select s_r, count(*) from table(integerList()) t " +
                "group by s_r having s_r > 1",
                new String[][] {{"100", "1"}, {"1000", "1"}, {"10000", "1"}},
                "[S_R, null, null, null]",
                null // DERBY-4650: should be "\"S_R\" > 1" if pushed down
                );
    }

    /**
     * Verify that attempts to create a trailing constant qualification do no
     * cause the VTI to return the wrong rows.
     * Tracked as DERBY-4651.
     */
    public void test_09_4651() throws Exception
    {
        Connection conn = getConnection();

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
             null,
             4
             );

        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where s_r > 500 order by s_r\n",
             new String[][]
             {
                 { "1000" ,         "2000"  },
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             "\"S_R\" > 500",
             2
             );

        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where s_r > 500 or 1=1 order by s_r\n",
             new String[][]
             {
                 { "1" ,         "2"  },
                 { "100" ,         "200"  },
                 { "1000" ,         "2000"  },
                 { "10000" ,         "20000"  },
             },
             "[S_R, S_NR, null, null]",
             null,
             4
             );

        assertPR
            (
             conn,
             "select s_r, s_nr from table( integerList() ) s where s_r > 500 and 1 != 1 order by s_r\n",
             new String[][]
             {
             },
             "[S_R, S_NR, null, null]",
             null,
             4
             );
    }

    /**
     * Test that {@code Restriction.toSQL()} returns properly quoted column
     * names. DERBY-4654.
     */
    public void test_10_quotes_in_column_names() throws Exception
    {
        String[][] expectedRows = new String[][] {{"100", "200", "300", "400"}};
        String expectedRestriction =
                "( \"cOL \"\"2\"\"\" < 1000 ) AND ( \"CoL \"\"1\"\"\" > 1 )";

        // Check that we can execute a query against a restricted VTI with
        // double quotes in the column names.
        assertPR(
                getConnection(),
                "select * from table(integerListSpecialColNames()) t " +
                "where \"CoL \"\"1\"\"\" > 1 and \"cOL \"\"2\"\"\" < 1000",
                expectedRows,
                "[CoL \"1\", cOL \"2\", COL3, COL4]",
                expectedRestriction);

        // Get the restriction that was pushed down.
        Statement stmt = createStatement();
        ResultSet rs = executeQuery(stmt, "values getLastRestriction()");
        assertTrue("empty result", rs.next());
        String restriction = rs.getString(1);
        assertEquals(expectedRestriction, restriction);
        rs.close();

        // Verify that the returned restriction has correct syntax so that
        // we can put it directly into the WHERE clause of a select query and
        // get the same rows as we did above.
        rs = executeQuery(
                stmt,
                "select * from table(integerListSpecialColNames()) t where " +
                restriction);
        JDBC.assertUnorderedResultSet(rs, expectedRows);
    }

    /**
     * Verify that Restriction.toSQL() returns usable SQL for all of the
     * comparable types. See DERBY-5369 and DERBY-5370.
     */
    public void test_11_5369_5370() throws Exception
    {
        Connection conn = getConnection();

        //
        // The table function used by this test extends VTITemplate, an
        // implementation of the JDBC 3.0 ResultSet. This table function will
        // not run on JSR169 because the JDBC 3.0 ResultSet pulls in classes
        // which don't exist in the JSR169 java.sql package (e.g., java.sql.Ref).
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }

        // if this fails, then we need to add a new data type to this test
        vetDatatypeCount( conn, 22 );
        
        // comparable types
        vet5370positive( conn, "BOOLEAN_COL", "false", "false", "true" );
        vet5370positive( conn, "BIGINT_COL", "0", "0", "1" );
        vet5370positive( conn, "CHAR_COL", "'0'", "0         ", "1         " );
        vet5370positive( conn, "CHAR_FOR_BIT_DATA_COL", "X'de'", "de202020202020202020", "dd202020202020202020" );
        vet5370positive( conn, "DATE_COL", "DATE('1994-02-23')", "1994-02-23", "1994-02-24" );
        vet5370positive( conn, "DECIMAL_COL", "0.00", "0.00", "1.00" );
        vet5370positive( conn, "REAL_COL", "0.0", "0.0", "1.0" );
        vet5370positive( conn, "DOUBLE_COL", "0.0", "0.0", "1.0" );
        vet5370positive( conn, "INT_COL", "0", "0", "1" );
        vet5370positive( conn, "NUMERIC_COL", "0.00", "0.00", "1.00" );
        vet5370positive( conn, "SMALLINT_COL", "0", "0", "1" );
        vet5370positive( conn, "TIME_COL", "TIME('15:09:02')", "15:09:02", "15:09:03" );
        vet5370positive( conn, "TIMESTAMP_COL", "TIMESTAMP('1962-09-23 03:23:34.234')", "1962-09-23 03:23:34.234", "1963-09-23 03:23:34.234" );
        vet5370positive( conn, "VARCHAR_COL", "'0'", "0", "1" );
        vet5370positive( conn, "VARCHAR_FOR_BIT_DATA_COL", "X'de'", "de", "dd" );

        //
        // The following all fail. If these comparisons start working, then this
        // test should be revisited to make sure that Restriction.toSQL() handles
        // the types which used to not be comparable.
        //
        vet5370negative( "BLOB_COL", "makeBlob5370()" );
        vet5370negative( "CLOB_COL", "'0'" );
        vet5370negative( "LONG_VARCHAR_COL", "'0'" );
        vet5370negative( "LONG_VARCHAR_FOR_BIT_DATA_COL", "X'de'" );
    }
    private void    vet5370positive
        (
         Connection conn,
         String columnName,
         String columnValue,
         String expectedValue,
         String negatedValue
         )
        throws Exception
    {
        assertResults
            (
             conn,
             "select " + columnName + " from table( restricted5370( 'APP', 'T_5370' ) ) s\n" +
             "where " + columnName + " = " + columnValue,
             new String[][] { new String[] { expectedValue } },
             false
             );

        assertResults
            (
             conn,
             "values( lastQuery5370() )",
             new String[][]
             {
                 new String[]
                 {
                     "select " + doubleQuote( columnName ) + "\n" +
                     "from " + doubleQuote( "APP" ) + "." + doubleQuote( "T_5370" ) + "\n" +
                     "where " + doubleQuote( columnName ) + " = " + columnValue
                 }
             },
             false
             );
        
        assertResults
            (
             conn,
             "select " + columnName + " from table( restricted5370( 'APP', 'T_5370' ) ) s\n" +
             "where " + columnName + " != " + columnValue,
             new String[][] { new String[] { negatedValue } },
             false
             );

        assertResults
            (
             conn,
             "values( lastQuery5370() )",
             new String[][]
             {
                 new String[]
                 {
                     "select " + doubleQuote( columnName ) + "\n" +
                     "from " + doubleQuote( "APP" ) + "." + doubleQuote( "T_5370" ) + "\n" +
                     "where " + doubleQuote( columnName ) + " != " + columnValue
                 }
             },
             false
             );
    }
    private static  String  doubleQuote( String text )  { return '"' + text + '"'; }
    private void    vet5370negative
        (
         String columnName,
         String columnValue
         )
        throws Exception
    {
        expectCompilationError
            (
             "42818",
             "select " + columnName + " from table( restricted5370( 'APP', 'T_5370' ) ) s\n" +
             "where " + columnName + " = " + columnValue
             );

    }
    private int vetDatatypeCount( Connection conn, int expectedTypeCount ) throws Exception
    {
        //
        // If this fails, it means that we need to add another datatype to
        //
        
        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

        assertEquals( expectedTypeCount, actualTypeCount );

        return actualTypeCount;
    }
    
    /**
     * Verify that if you wrap a RestrictedVTI in a view, selects
     * from the view pass the restriction on to the RestrictedVTI.
     * However, the projection is not passed through to the view so it
     * is not passed on to the RestrictedVTI, as described on DERBY-6036.
     * When that issue is addressed, we should adjust this test case.
     */
    public void test_12_6036() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "create view v6036 as select * from table( integerList() ) s" );

        // directly selecting from the vti pushes down both the projection and the restriction
        assertResults
            (
             conn,
             "select s_nr from table( integerList() ) s where ns_r = 3000",
             new String[][]
             {
                { "2000" }
             },
             false
             );
        assertResults
            (
             conn,
             "values getLastProjection()",
             new String[][]
             {
                { "[null, S_NR, NS_R, null]" }
             },
             false
             );
        assertResults
            (
             conn,
             "values getLastRestriction()",
             new String[][]
             {
                { "\"NS_R\" = 3000" }
             },
             false
             );

        // directly selecting from the view only pushes down the restriction
        assertResults
            (
             conn,
             "select s_nr from v6036 where ns_r = 3000",
             new String[][]
             {
                { "2000" }
             },
             false
             );
        assertResults
            (
             conn,
             "values getLastProjection()",
             new String[][]
             {
                { "[S_R, S_NR, NS_R, NS_NR]" }
             },
             false
             );
        assertResults
            (
             conn,
             "values getLastRestriction()",
             new String[][]
             {
                { "\"NS_R\" = 3000" }
             },
             false
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
    private static Integer i( int intValue ) { return intValue; }

    public static IntegerArrayVTI integerListSpecialColNames()
    {
        return new IntegerArrayVTI
            (
             new String[] { "CoL \"1\"", "cOL \"2\"", "COL3", "COL4" },
             new int[][]
             {
                 new int[] { 1, 2, 3, 4 },
                 new int[] { 100, 200, 300, 400 },
                 new int[] { 1000, 2000, 3000, 4000 },
                 new int[] { 10000, 20000, 30000, 40000 },
             }
             );
    }

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
     * Run a query against a RestrictedVTI, verify that the expected
     * projection and restriction are pushed into the VTI, and verify
     * that the VTI returns the expected number of rows.
     * </p>
     */
    private void assertPR
        (
         Connection conn,
         String query,
         String[][] expectedResults,
         String expectedProjection,
         String expectedRestriction,
         int expectedQualifiedRowCount
         ) throws Exception
    {
        assertPR( conn, query, expectedResults, expectedProjection, expectedRestriction );
        
        assertResults
            (
             conn,
             "values ( getCount() )\n",
             new String[][] { { Integer.toString( expectedQualifiedRowCount ) } },
             false
             );
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

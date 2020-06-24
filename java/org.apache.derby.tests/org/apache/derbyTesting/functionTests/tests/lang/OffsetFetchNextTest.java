/*

  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OffsetFetchNextTest

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test {@code <result offset clause>} and {@code <fetch first clause>}.
 */
public class OffsetFetchNextTest extends BaseJDBCTestCase {

    private final static String LANG_FORMAT_EXCEPTION = "22018";
    private final static String LANG_INTEGER_LITERAL_EXPECTED = "42X20";
    private final static String LANG_INVALID_ROW_COUNT_FIRST = "2201W";
    private final static String LANG_INVALID_ROW_COUNT_OFFSET = "2201X";
    private final static String LANG_MISSING_PARMS = "07000";
    private final static String LANG_SYNTAX_ERROR = "42X01";
	private final static String LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL = "2201Z";

    private final static String PERCENT_TOKEN = "%";
    
    // flavors of SQL Standard syntax
    private final static String FIRST_ROWS_ONLY = "fetch first % rows only";
    private final static String FIRST_ROW_ONLY = "fetch first % row only";
    private final static String NEXT_ROWS_ONLY = "fetch next % rows only";

    // variants
    private final static int SQL_STANDARD_VARIANT = 0;
    private final static int JDBC_VARIANT = SQL_STANDARD_VARIANT + 1;
    private final static int VARIANT_COUNT = JDBC_VARIANT + 1;

    public OffsetFetchNextTest(String name) {
        super(name);
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("OffsetFetchNextTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(
            baseSuite("OffsetFetchNextTest:embedded"));
        suite.addTest(
            TestConfiguration.clientServerDecorator(
                baseSuite("OffsetFetchNextTest:client")));

        return suite;
    }

    public static Test baseSuite(String suiteName) {
        return new CleanDatabaseTestSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            new BaseTestSuite(OffsetFetchNextTest.class,
                          suiteName)) {
            @Override
            protected void decorateSQL(Statement s)
                    throws SQLException {
                createSchemaObjects(s);
            }
        };
    }


    /**
     * Creates tables used by the tests (never modified, we use rollback after
     * changes).
     */
    private static void createSchemaObjects(Statement st) throws SQLException
    {
        // T1 (no indexes)
        st.executeUpdate("create table t1 (a int, b bigint)");
        st.executeUpdate("insert into t1 (a, b) " +
                         "values (1,1), (1,2), (1,3), (1,4), (1,5)");

        // T2 (primary key)
        st.executeUpdate("create table t2 (a int primary key, b bigint)");
        st.executeUpdate("insert into t2 (a, b) " +
                         "values (1,1), (2,1), (3,1), (4,1), (5,1)");

        // T3 (primary key + secondary key)
        st.executeUpdate("create table t3 (a int primary key, " +
                         "                 b bigint unique)");
        st.executeUpdate("insert into t3 (a, b) " +
                         "values (1,1), (2,2), (3,3), (4,4), (5,5)");
    }

    /**
     * Negative tests. Test various invalid OFFSET and FETCH NEXT clauses.
     *
     * @throws java.sql.SQLException
     */
    public void testErrors() throws SQLException
    {
        Statement st = createStatement();

        String  stub = "select * from t1 %";

        // Wrong range in row count argument
        vetStatement( st, LANG_INVALID_ROW_COUNT_OFFSET, stub, FIRST_ROWS_ONLY, "-1", null, null );

        vetStatement( st, LANG_SYNTAX_ERROR, stub, FIRST_ROWS_ONLY, "-?", null, null );

        assertStatementError(LANG_INVALID_ROW_COUNT_FIRST, st,
                             "select * from t1 fetch first 0 rows only");

        vetStatement( st, LANG_INVALID_ROW_COUNT_FIRST, stub, FIRST_ROWS_ONLY, null, "-1", null );

        // Wrong type in row count argument
        vetStatement( st, LANG_INTEGER_LITERAL_EXPECTED, stub, FIRST_ROWS_ONLY, null, "3.14", null );

        // Wrong order of clauses
        assertStatementError(LANG_SYNTAX_ERROR, st,
                             "select * from t1 " +
                             "fetch first 0 rows only offset 0 rows");
        assertStatementError(LANG_SYNTAX_ERROR, st,
                             "select * from t1 { offset 0 limit 0 }");
    }


    /**
     * Positive tests. Check that the new keyword OFFSET introduced is not
     * reserved so we don't risk breaking existing applications.
     *
     * @throws java.sql.SQLException
     */
    public void testNewKeywordNonReserved() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4562
        setAutoCommit(false);
        prepareStatement("select a,b as offset from t1 offset 0 rows");
        prepareStatement("select a,b as limit from t1 offset 0 rows");

        // Column and table correlation name usage
        prepareStatement("select a,b from t1 as offset");
        prepareStatement("select a,b from t1 as limit");

        prepareStatement("select a,b offset from t1 offset");
        prepareStatement("select a,b limit from t1 limit");

        prepareStatement("select a,b offset from t1 offset +2 rows");

        prepareStatement("select a offset,b from t1 offset ? rows");

        prepareStatement("select offset.a, offset.b offset from t1 as offset offset ? rows");
        prepareStatement("select limit.a, limit.b offset from t1 as limit offset ? rows");

        // DERBY-4562
        Statement s = createStatement();
        s.executeUpdate("create table t4562(i int, offset int)");
        ResultSet rs = s.executeQuery(
            "select * from t4562 where i > 0 and offset + i < 0 offset 2 rows");
        rs.next();

        rs = s.executeQuery(
            "select * from t4562 where i > 0 and offset - i < 0 offset 2 rows");
        rs.next();

        rs = s.executeQuery(
            "select * from t4562 where i > 0 and offset * i < 0 offset 2 rows");
        rs.next();

        rs.close();

        rollback();
    }


    /**
     * Positive tests.
     *
     * @throws java.sql.SQLException
     */
    public void testOffsetFetchFirstReadOnlyForwardOnlyRS() throws SQLException
    {
        Statement stm = createStatement();

        /*
         * offset 0 rows (a no-op)
         */

        vetStatement
            (
             stm, null, "select a, b from t1%", FIRST_ROWS_ONLY, "0", null,
             new String [][] { {"1","1"}, {"1","2"},{"1","3"}, {"1","4"},{"1","5"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROWS_ONLY, "0", null,
             new String [][] { {"1","1"}, {"2","1"},{"3","1"}, {"4","1"},{"5","1"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROWS_ONLY, "0", null,
             new String [][] { {"1","1"}, {"2","2"},{"3","3"}, {"4","4"},{"5","5"} }
             );

        /*
         * offset 1 rows
         */

        vetStatement
            (
             stm, null, "select a,b from t1%", FIRST_ROWS_ONLY, "1", null,
             new String [][] { {"1","2"},{"1","3"}, {"1","4"},{"1","5"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROWS_ONLY, "1", null,
             new String [][] { {"2","1"},{"3","1"}, {"4","1"},{"5","1"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROWS_ONLY, "1", null,
             new String [][] { {"2","2"},{"3","3"}, {"4","4"},{"5","5"} }
             );

        /*
         * offset 4 rows
         */

        vetStatement
            (
             stm, null, "select a,b from t1%", FIRST_ROWS_ONLY, "4", null,
             new String [][] { {"1","5"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROWS_ONLY, "4", null,
             new String [][] { {"5","1"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROWS_ONLY, "4", null,
             new String [][] {  {"5","5"} }
             );

        /*
         * offset 1 rows fetch 1 row. Use "next"/"rows" syntax
         */
        vetStatement
            (
             stm, null, "select a,b from t1%", FIRST_ROWS_ONLY, "1", "1",
             new String [][] { {"1","2"}  }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROWS_ONLY, "1", "1",
             new String [][] { {"2","1"}  }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROWS_ONLY, "1", "1",
             new String [][] { {"2","2"}  }
             );

        /*
         * offset 1 rows fetch so many rows we drain rs row. Use "first"/"row"
         * syntax
         */
        vetStatement
            (
             stm, null, "select a,b from t1%", FIRST_ROW_ONLY, "1", "10",
             new String [][] { {"1","2"},{"1","3"}, {"1","4"},{"1","5"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROW_ONLY, "1", "10",
             new String [][] { {"2","1"},{"3","1"}, {"4","1"},{"5","1"} }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROW_ONLY, "1", "10",
             new String [][] { {"2","2"},{"3","3"}, {"4","4"},{"5","5"} }
             );

        /*
         * offset so many rows that we see empty rs
         */
        vetStatement
            (
             stm, null, "select a,b from t1%", FIRST_ROW_ONLY, "10", null,
             new String [][] { }
             );
        vetStatement
            (
             stm, null, "select a,b from t2%", FIRST_ROW_ONLY, "10", null,
             new String [][] { }
             );
        vetStatement
            (
             stm, null, "select a,b from t3%", FIRST_ROW_ONLY, "10", null,
             new String [][] { }
             );

        /*
         * fetch first/next row (no row count given)
         */
        queryAndCheck(
            stm,
            "select a,b from t1 fetch first row only",
            new String [][] {{"1","1"}});
        queryAndCheck(
            stm,
            "select a,b from t2 fetch next row only",
            new String [][] {{"1","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 fetch next row only",
            new String [][] {{"1","1"}});

        /*
         * Combine with order by asc
         */
        queryAndCheck(
            stm,
            "select a,b from t1 order by b asc fetch first row only",
            new String [][] {{"1","1"}});
        queryAndCheck(
            stm,
            "select a,b from t2 order by a asc fetch next row only",
            new String [][] {{"1","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 order by a asc fetch next row only",
            new String [][] {{"1","1"}});

        /*
         * Combine with order by desc.
         */
        queryAndCheck(
            stm,
            // Note: use column b here since for t1 all column a values are the
            // same and order can change after sorting, want unique row first
            // in rs so we can test it.
            "select a,b from t1 order by b desc fetch first row only",
            new String [][] {{"1","5"}});
        queryAndCheck(
            stm,
            "select a,b from t2 order by a desc fetch next row only",
            new String [][] {{"5","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 order by a desc fetch next row only",
            new String [][] {{"5","5"}});

        /*
         * Combine with group by, order by.
         */
        queryAndCheck(
            stm,
            "select max(a) from t1 group by b fetch first row only",
            new String [][] {{"1"}});
        vetStatement
            (
             stm, null, "select max(a) from t2 group by b %", FIRST_ROW_ONLY, "0", null,
             new String [][] { {"5"} }
             );
        vetStatement
            (
             stm, null, "select max(a) from t3 group by b order by max(a) %", NEXT_ROWS_ONLY, null, "2",
             new String [][] { {"1"},{"2"} }
             );

        /*
         * Combine with union
         */

        vetStatement
            (
             stm, null, "select * from t1 union all select * from t1 %", FIRST_ROW_ONLY, null, "2",
             new String [][] { {"1","1"}, {"1","2"} }
             );

        /*
         * Combine with join
         */
        vetStatement
            (
             stm, null, "select t2.b, t3.b from t2,t3 where t2.a=t3.a %", FIRST_ROW_ONLY, null, "2",
             new String [][] { {"1","1"}, {"1","2"} }
             );

        stm.close();
    }


    /**
     * Positive tests.
     *
     * @throws java.sql.SQLException
     */
    public void testOffsetFetchFirstUpdatableForwardOnlyRS() throws SQLException
    {
        Statement stm = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);
        ResultSet   rs;
        String[]    variants;

        setAutoCommit(false);

        /*
         * offset 0 rows (a no-op), update a row and verify result
         */
        variants = makeVariants( "select * from t1 %", FIRST_ROWS_ONLY, "0", null );
        for (String variant : variants)
        {
            rs = stm.executeQuery( variant );
            rs.next();
            rs.next(); // at row 2
            rs.updateInt(1, -rs.getInt(1));
            rs.updateRow();
            rs.close();

            queryAndCheck(
                          stm,
                          "select a,b from t1",
                          new String [][] {
                              {"1","1"}, {"-1","2"},{"1","3"}, {"1","4"},{"1","5"}});

            rollback();
        }

        /*
         * offset 1 rows, update a row and verify result
         */
        variants = makeVariants( "select * from t1 %", FIRST_ROWS_ONLY, "1", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for ( String variant : variants )
        {
            rs = stm.executeQuery( variant );
            rs.next(); // at row 1, but row 2 of underlying rs

            rs.updateInt(1, -rs.getInt(1));
            rs.updateRow();
            rs.close();

            queryAndCheck(
                          stm,
                          "select a,b from t1",
                          new String [][] {
                              {"1","1"}, {"-1","2"},{"1","3"}, {"1","4"},{"1","5"}});

            rollback();
        }
        
        stm.close();
    }


    /**
     * Positive tests with scrollable read-only.
     *
     * @throws java.sql.SQLException
     */
    public void testOffsetFetchFirstReadOnlyScrollableRS() throws SQLException
    {
        Statement stm = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
        ResultSet   rs;
        String[]    variants;

        /*
         * offset 0 rows (a no-op), update a row and verify result
         */
        variants = makeVariants( "select * from t1 %", FIRST_ROWS_ONLY, "0", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for ( String variant : variants )
        {
            rs = stm.executeQuery( variant );
            rs.next();
            rs.next(); // at row 2
            assertTrue(rs.getInt(2) == 2);
            rs.close();
        }
        
        /*
         * offset 1 rows, fetch 3 row, check that we have the right ones
         */
        variants = makeVariants( "select * from t1 %", FIRST_ROWS_ONLY, "1", "3" );
        for ( String variant : variants )
        {
            rs = stm.executeQuery( variant );
            rs.next();
            rs.next(); // at row 2, but row 3 of underlying rs

            assertTrue(rs.getInt(2) == 3);

            // Go backbards and update
            rs.previous();
            assertTrue(rs.getInt(2) == 2);

            // Try some navigation and border conditions
            rs.previous();
            assertTrue(rs.isBeforeFirst());
            rs.next();
            rs.next();
            rs.next();
            rs.next();
            assertTrue(rs.isAfterLast());
        }
        
        stm.close();
    }


    /**
     * Positive tests with SUR (Scrollable updatable result set).
     *
     * @throws java.sql.SQLException
     */
    public void testOffsetFetchFirstUpdatableScrollableRS() throws SQLException
    {
        Statement stm = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_UPDATABLE);
        ResultSet   rs;
        String[]    variants;

        setAutoCommit(false);

        /*
         * offset 0 rows (a no-op), update a row and verify result
         * also try the "for update" syntax so we see that it still works
         */
        variants = makeVariants( "select * from t1 % for update", FIRST_ROWS_ONLY, "0", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            rs = stm.executeQuery( variant );
            rs.next();
            rs.next(); // at row 2
            rs.updateInt(1, -rs.getInt(1));
            rs.updateRow();
            rs.close();

            queryAndCheck(
                          stm,
                          "select a,b from t1",
                          new String [][] {
                              {"1","1"}, {"-1","2"},{"1","3"}, {"1","4"},{"1","5"}});

            rollback();
        }
        
        /*
         * offset 1 rows, fetch 3 row, update some rows and verify result
         */
        variants = makeVariants( "select * from t1 %", NEXT_ROWS_ONLY, "1", "3" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for ( String variant : variants )
        {
            rs = stm.executeQuery( variant );
            rs.next();
            rs.next(); // at row 2, but row 3 of underlying rs

            rs.updateInt(1, -rs.getInt(1));
            rs.updateRow();

            // Go backbards and update
            rs.previous();
            rs.updateInt(1, -rs.getInt(1));
            rs.updateRow();

            // Try some navigation and border conditions
            rs.previous();
            assertTrue(rs.isBeforeFirst());
            rs.next();
            rs.next();
            rs.next();
            rs.next();
            assertTrue(rs.isAfterLast());

            // Insert a row
            rs.moveToInsertRow();
            rs.updateInt(1,42);
            rs.updateInt(2,42);
            rs.insertRow();

            // Delete a row
            rs.previous();
            rs.deleteRow();

            // .. and see that a hole is left in its place
            rs.previous();
            rs.next();
            assertTrue(rs.rowDeleted());

            rs.close();

            queryAndCheck(
                          stm,
                          "select a,b from t1",
                          new String [][] {
                              {"1","1"}, {"-1","2"},{"-1","3"},{"1","5"},{"42","42"}});
            rollback();
        }
        
        // Test with projection
        variants = makeVariants( "select * from t1 where a + 1 < b%", NEXT_ROWS_ONLY, "1", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            rs = stm.executeQuery( variant );
            // should yield 2 rows
            rs.absolute(2);
            assertTrue(rs.getInt(2) == 5);
            rs.updateInt(2, -5);
            rs.updateRow();
            rs.close();

            queryAndCheck(
                          stm,
                          "select a,b from t1",
                          new String [][] {
                              {"1","1"}, {"1","2"},{"1","3"},{"1","4"},{"1","-5"}});
            rollback();
        }
        
        stm.close();
    }


    public void testValues() throws SQLException
    {
        Statement stm = createStatement();

        vetStatement
            (
             stm, null, "values 4%", FIRST_ROW_ONLY, null, "2",
             new String [][] { {"4"} }
             );

        vetStatement
            (
             stm, null, "values 4%", FIRST_ROW_ONLY, "1", null,
             new String [][] { }
             );

        stm.close();
    }

    /**
     * Positive tests, result set metadata
     *
     * @throws java.sql.SQLException
     */
    public void testMetadata() throws SQLException
    {
        Statement stm = createStatement();
        ResultSet   rs;
        String[]    variants;

        variants = makeVariants( "select * from t1%", NEXT_ROWS_ONLY, "1", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            rs = stm.executeQuery( variant );
            ResultSetMetaData rsmd= rs.getMetaData();
            int cnt = rsmd.getColumnCount();

            String[] cols = new String[]{"A","B"};
            int[] types = {Types.INTEGER, Types.BIGINT};

            for (int i=1; i <= cnt; i++) {
                String name = rsmd.getColumnName(i);
                int type = rsmd.getColumnType(i);

                assertTrue(name.equals(cols[i-1]));
                assertTrue(type == types[i-1]);
            }

            rs.close();
        }
        
        stm.close();
    }


    /**
     * Test that we see correct traces of the filtering in the statistics
     *
     * @throws java.sql.SQLException
     */
    public void testRunTimeStatistics() throws SQLException
    {
        Statement stm = createStatement();
        ResultSet   rs;
        String[]    variants;

        variants = makeVariants( "select a,b from t1%", NEXT_ROWS_ONLY, "2", null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            stm.executeUpdate( "call syscs_util.syscs_set_runtimestatistics(1)" );

            queryAndCheck(
                          stm,
                          variant,
                          new String [][] {
                              {"1","3"}, {"1","4"},{"1","5"}});

            stm.executeUpdate( "call syscs_util.syscs_set_runtimestatistics(0)" );

            rs = stm.executeQuery( "values syscs_util.syscs_get_runtimestatistics()" );
            rs.next();
            String plan = rs.getString(1);

            // Verify that the plan shows the filtering (2 rows of 3 seen):
//IC see: https://issues.apache.org/jira/browse/DERBY-4079
            assertTrue(plan.indexOf("Row Count (1):\n" +
                                    "Number of opens = 1\n" +
                                    "Rows seen = 3\n" +
                                    "Rows filtered = 2") != -1);

            rs.close();
        }
        
        stm.close();
    }


    /**
     * Test against a bigger table
     *
     * @throws java.sql.SQLException
     */
    public void testBigTable() throws SQLException
    {
        Statement stm = createStatement();

        setAutoCommit(false);

        stm.executeUpdate("declare global temporary table session.t (i int) " +
                          "on commit preserve rows not logged");

        PreparedStatement ps =
            prepareStatement("insert into session.t values ?");

        for (int i=1; i <= 100000; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();

            if (i % 10000 == 0) {
                commit();
            }
        }

        queryAndCheck(
            stm,
            "select count(*) from session.t",
            new String [][] {
                {"100000"}});

        vetStatement
            (
             stm, null, "select i from session.t%", FIRST_ROWS_ONLY, "99999", null,
             new String [][] { {"100000"} }
             );

        stm.executeUpdate("drop table session.t");
        stm.close();
    }

    /**
     * Test that the values of offset and fetch first are not forgotten if
     * a {@code PreparedStatement} is executed multiple times (DERBY-4212).
     *
     * @throws java.sql.SQLException
     */
    public void testRepeatedExecution() throws SQLException
    {
        PreparedStatement ps;
        String[]    variants;

        variants = makeVariants( "select * from t1 order by b%", NEXT_ROWS_ONLY, "2", "2" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            String[][] expected = {{"1", "3"}, {"1", "4"}};
            for (int i = 0; i < 10; i++) {
                JDBC.assertFullResultSet(ps.executeQuery(), expected);
            }
        }
    }

    /**
     * Test dynamic arguments
     *
     * @throws java.sql.SQLException
     */
    public void testDynamicArgs() throws SQLException
    {
        PreparedStatement ps;
        String[]    variants;
        String[][] expected = null;

        // Check look-ahead also for ? in grammar since offset is not reserved
        variants = makeVariants( "select * from t1%", NEXT_ROWS_ONLY, "?", null );
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
        }
        
        
        variants = makeVariants( "select * from t1 order by b%", NEXT_ROWS_ONLY, "?", "?" );
        for ( int j = 0; j < variants.length; j++ )
        {
            // SQL Standard and JDBC limit/offset parameter orders are different
            int offsetParam = ( j == SQL_STANDARD_VARIANT ) ? 1 : 2;
            int fetchParam = ( j == SQL_STANDARD_VARIANT ) ? 2 : 1;
            
            expected = new String[][] {{"1", "3"}, {"1", "4"}};
            ps = prepareStatement( variants[ j ] );

            // Check range errors

            ps.setInt( offsetParam, 0 );
            assertPreparedStatementError(LANG_MISSING_PARMS, ps);

            ps.setInt( offsetParam, -1 );
            ps.setInt( fetchParam, 2 );
            assertPreparedStatementError(LANG_INVALID_ROW_COUNT_OFFSET, ps);

            ps.setInt( offsetParam, 0 );
            ps.setInt( fetchParam, ( j == SQL_STANDARD_VARIANT ) ? 0 : -1 );
            assertPreparedStatementError(LANG_INVALID_ROW_COUNT_FIRST, ps);

            // Check non-integer values
            try {
                ps.setString( offsetParam, "aaa");
            } catch (SQLException e) {
                assertSQLState(LANG_FORMAT_EXCEPTION, e);
            }

            try {
                ps.setString( fetchParam, "aaa");
            } catch (SQLException e) {
                assertSQLState(LANG_FORMAT_EXCEPTION, e);
            }


            // A normal case
            for (int i = 0; i < 2; i++) {
                ps.setInt( offsetParam,2 );
                ps.setInt( fetchParam,2 );
                JDBC.assertFullResultSet(ps.executeQuery(), expected);
            }

            // Now, note that since we now have different values for offset and
            // fetch first, we also exercise reusing the result set for this
            // prepared statement (i.e. the values are computed at execution time,
            // not at result set generation time). Try long value for change.
            ps.setLong( offsetParam, 1L );
            ps.setInt( fetchParam, 3 );
            expected = new String[][]{{"1", "2"}, {"1", "3"}, {"1", "4"}};
            JDBC.assertFullResultSet(ps.executeQuery(), expected);


            //  Try a large number
            ps.setLong( offsetParam, Integer.MAX_VALUE * 2L );
            ps.setInt( fetchParam, 5 );
            JDBC.assertEmpty(ps.executeQuery());
        }
        
        // Mix of prepared and not
        variants = makeVariants( "select * from t1 order by b%", NEXT_ROWS_ONLY, "?", "3" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            ps.setLong(1, 1L);
            JDBC.assertFullResultSet(ps.executeQuery(), expected);
        }

        variants = makeVariants( "select * from t1 order by b%", NEXT_ROWS_ONLY, "4", "?" );
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            ps.setLong(1, 1L);
            JDBC.assertFullResultSet(ps.executeQuery(), new String[][]{{"1", "5"}});
        }

        // Mix of other dyn args and ours:
        variants = makeVariants( "select * from t1 where a = ? order by b%", NEXT_ROWS_ONLY, "?", "3" );
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            ps.setInt(1, 1);
            ps.setLong(2, 1L);
            JDBC.assertFullResultSet(ps.executeQuery(), expected);
        }

        variants = makeVariants( "select * from t1 where a = ? order by b%", NEXT_ROWS_ONLY, "1", "?" );
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            ps.setInt(1, 1);
            ps.setLong(2, 2L);
            expected = new String[][]{{"1", "2"}, {"1", "3"}};
            JDBC.assertFullResultSet(ps.executeQuery(), expected);
        }

        // NULLs not allowed (Note: parameter metadata says "isNullable" for
        // all ? args in Derby...)
        variants = makeVariants( "select * from t1 order by b%", NEXT_ROWS_ONLY, "?", "?" );
        for ( int i = 0; i < variants.length; i++ )
        {
            ps = prepareStatement( variants[ i ] );
            int offsetParam = ( i == SQL_STANDARD_VARIANT ) ? 1 : 2;
            int fetchParam = ( i == SQL_STANDARD_VARIANT ) ? 2 : 1;
            
            ps.setNull( offsetParam, Types.BIGINT );
            ps.setInt( fetchParam, 2 );
            assertPreparedStatementError(LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL, ps);

            ps.setInt( offsetParam,1 );
            ps.setNull( fetchParam, Types.BIGINT );
            assertPreparedStatementError(LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL, ps);
            
            ps.close();
        }
    }

    /**
     * Test dynamic arguments
     *
     * @throws java.sql.SQLException
     */
    public void testDynamicArgsMetaData() throws SQLException
    {

    	//since there is no getParameterMetaData() call available in JSR169 
    	//implementations, do not run this test if we are running JSR169
    	if (JDBC.vmSupportsJSR169()) return;
//IC see: https://issues.apache.org/jira/browse/DERBY-4384

        PreparedStatement ps;
        String[]    variants;

        variants = makeVariants( "select * from t1 where a = ? order by b%", NEXT_ROWS_ONLY, "?", "?" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (String variant : variants)
        {
            ps = prepareStatement( variant );
            
            ParameterMetaData pmd = ps.getParameterMetaData();
            int[] expectedTypes = { Types.INTEGER, Types.BIGINT, Types.BIGINT };

            for (int i = 0; i < 3; i++) {
                assertEquals("Unexpected parameter type",
                             expectedTypes[i], pmd.getParameterType(i+1));
                assertEquals("Derby ? args are nullable",
                             // Why is that? Cf. logic in ParameterNode.setType
                             ParameterMetaData.parameterNullable,
                             pmd.isNullable(i+1));
            }
            ps.close();
        }
    }

    /**
     * Test some additional corner cases in JDBC limit/offset syntax.
     *
     * @throws java.sql.SQLException
     */
    public void testJDBCLimitOffset() throws SQLException
    {
        // LIMIT 0 is allowed. It means: everything from the OFFSET forward
        PreparedStatement   ps = prepareStatement( "select a from t2 order by a { limit ? }" );
        ps.setInt( 1, 0 );
        JDBC.assertFullResultSet
            (
             ps.executeQuery(),
             new String[][] { { "1" }, { "2" }, { "3" }, { "4" }, { "5" } }
             );
        ps.close();

        ps = prepareStatement( "select a from t2 order by a { limit ? offset 3 }" );
        ps.setInt( 1, 0 );
        JDBC.assertFullResultSet
            (
             ps.executeQuery(),
             new String[][] { { "4" }, { "5" } }
             );
        ps.close();

        // mix JDBC and SQL Standard syntax
        ps = prepareStatement
            (
             "select t.a from\n" +
             "( select * from t2 order by a { limit 3 offset 1 } ) t,\n" +
             "( select * from t3 order by a offset 2 rows fetch next 10 rows only ) s\n" +
             "where t.a = s.a order by t.a"
             );
        JDBC.assertFullResultSet
            (
             ps.executeQuery(),
             new String[][] { { "3" }, { "4" } }
             );
        ps.close();
    }

    /**
     * Run a statement with both SQL Standard and JDBC limit/offset syntax. Verify
     * that we get the expected error or results. The statement has a % literal at the
     * point where the offset/fetchFirst and limit/offset clauses are to be inserted.
     */
    private void    vetStatement
        ( Statement stmt, String sqlState, String stub, String fetchFormat, String offset, String fetchFirst, String[][] expectedResults )
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        throws SQLException
    {
        String[]    variants = makeVariants( stub, fetchFormat, offset, fetchFirst );

        for (String text : variants)
        {
            if ( sqlState != null )
            {
                assertStatementError( sqlState, stmt, text );
            }
            else
            {
                queryAndCheck( stmt, text, expectedResults );
            }
        }
    }

    /**
     * Make the SQL Standard and JDBC limit/offset variants of a stub statement,
     * plugging in the given offset and fetch count.
     */
    private String[]    makeVariants
        ( String stub, String fetchFormat, String offset, String fetchFirst )
    {
        String[]    result = new String[ VARIANT_COUNT ];

        result[ SQL_STANDARD_VARIANT ] = makeSQLStandardText( stub, fetchFormat, offset, fetchFirst );
        result[ JDBC_VARIANT ] = makeJDBCText( stub, offset, fetchFirst );

        return result;
    }
    
    /**
     * Substitute the SQL Standard syntax into a stub statement, given an offset and fetch count.
     */
    private String  makeSQLStandardText
        ( String stub, String fetchFormat, String offset, String fetchFirst )
    {
        String  sqlStandardText = "";

        if ( offset != null )
        {
            sqlStandardText = " offset " + offset + " rows ";
        }
        if ( fetchFirst != null )
        {
            sqlStandardText = sqlStandardText + substitute( fetchFormat, PERCENT_TOKEN, fetchFirst );
        }

        sqlStandardText = substitute( stub, PERCENT_TOKEN, sqlStandardText );

        println( sqlStandardText );

        return sqlStandardText;
    }
    /**
     * Substitute JDBC limit/offset syntax into a stub statement, given an offset and fetch count.
     */
    private String  makeJDBCText
        ( String stub, String offset, String fetchFirst )
    {
        String  jdbcText = "";

        if ( offset != null )
        {
            jdbcText = " offset " + offset;
        }
        if ( fetchFirst != null )
        {
            jdbcText = " limit " + fetchFirst + " " + jdbcText;
        }
        else
        {
            jdbcText = "limit 0 " + jdbcText;
        }

        jdbcText = substitute( stub, PERCENT_TOKEN, " { " + jdbcText + " } " );

        println( jdbcText );

        return jdbcText;
    }

    private String  substitute( String stub, String token, String replacement )
    {
        int substitutionIndex = stub.indexOf( token );
        if ( substitutionIndex < 0 ) { fail( "Bad stub: " + stub + ". Can't find token: " + token ); }

        String  prefix = stub.substring( 0, substitutionIndex );
        String  suffix = ( substitutionIndex == stub.length() - 1 ) ?
            "" : stub.substring( substitutionIndex + 1, stub.length() );

        return prefix + replacement + suffix;
    }
    
    private void queryAndCheck(
        Statement stm,
        String queryText,
        String [][] expectedRows) throws SQLException {

        ResultSet rs = stm.executeQuery(queryText);
        JDBC.assertFullResultSet(rs, expectedRows);
    }

}

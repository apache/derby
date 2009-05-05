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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.PreparedStatement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test <result offset clause> and <fetch first clause>.
 */
public class OffsetFetchNextTest extends BaseJDBCTestCase {

    public OffsetFetchNextTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("OffsetFetchNextTest");

        suite.addTest(
            baseSuite("OffsetFetchNextTest:embedded"));
        suite.addTest(
            TestConfiguration.clientServerDecorator(
                baseSuite("OffsetFetchNextTest:client")));

        return suite;
    }

    public static Test baseSuite(String suiteName) {
        return new CleanDatabaseTestSetup(
            new TestSuite(OffsetFetchNextTest.class,
                          suiteName)) {
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
    private static void createSchemaObjects(Statement st)
            throws SQLException
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
     */
    public void testErrors()
            throws Exception
    {
        Statement st = createStatement();

        // Wrong range in row count argument

        assertStatementError("2201X", st,
                             "select * from t1 offset -1 rows");

        assertStatementError("2201W", st,
                             "select * from t1 fetch first 0 rows only");

        assertStatementError("2201W", st,
                             "select * from t1 fetch first -1 rows only");

        // Wrong type in row count argument
        assertStatementError("42X20", st,
                             "select * from t1 fetch first 3.14 rows only");

        // Wrong order of clauses
        assertStatementError("42X01", st,
                             "select * from t1 " +
                             "fetch first 0 rows only offset 0 rows");
    }


    /**
     * Positive tests. Check that the new keyword OFFSET introduced is not
     * reserved so we don't risk breaking existing apps.
     */
    public void testNewKeywordNonReserved()
            throws Exception
    {
        prepareStatement("select a,b as OFFSET from t1 OFFSET 0 rows");

        // Column and table correlation name usage
        prepareStatement("select a,b from t1 AS OFFSET");

        prepareStatement("select a,b OFFSET from t1 OFFSET");
    }


    /**
     * Positive tests.
     */
    public void testOffsetFetchFirstReadOnlyForwardOnlyRS()
            throws Exception
    {
        Statement stm = createStatement();

        /*
         * offset 0 rows (a no-op)
         */

        queryAndCheck(
            stm,
            "select a,b from t1 offset 0 rows",
            new String [][] {
                {"1","1"}, {"1","2"},{"1","3"}, {"1","4"},{"1","5"}});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 0 rows",
            new String [][] {
                {"1","1"}, {"2","1"},{"3","1"}, {"4","1"},{"5","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 0 rows",
            new String [][] {
                {"1","1"}, {"2","2"},{"3","3"}, {"4","4"},{"5","5"}});

        /*
         * offset 1 rows
         */

        queryAndCheck(
            stm,
            "select a,b from t1 offset 1 rows",
            new String [][] {
                {"1","2"},{"1","3"}, {"1","4"},{"1","5"}});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 1 rows",
            new String [][] {
                {"2","1"},{"3","1"}, {"4","1"},{"5","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 1 rows",
            new String [][] {
                {"2","2"},{"3","3"}, {"4","4"},{"5","5"}});

        /*
         * offset 4 rows
         */

        queryAndCheck(
            stm,
            "select a,b from t1 offset 4 rows",
            new String [][] {
                {"1","5"}});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 4 rows",
            new String [][] {
                {"5","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 4 rows",
            new String [][] {
                {"5","5"}});

        /*
         * offset 1 rows fetch 1 row. Use "next"/"rows" syntax
         */
        queryAndCheck(
            stm,
            "select a,b from t1 offset 1 row fetch next 1 rows only",
            new String [][] {
                {"1","2"}});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 1 row fetch next 1 rows only",
            new String [][] {
                {"2","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 1 row  fetch next 1 rows only",
            new String [][] {
                {"2","2"}});

        /*
         * offset 1 rows fetch so many rows we drain rs row. Use "first"/"row"
         * syntax
         */
        queryAndCheck(
            stm,
            "select a,b from t1 offset 1 rows fetch first 10 row only",
            new String [][] {
                {"1","2"},{"1","3"}, {"1","4"},{"1","5"}});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 1 rows fetch first 10 row only",
            new String [][] {
                {"2","1"},{"3","1"}, {"4","1"},{"5","1"}});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 1 rows  fetch first 10 row only",
            new String [][] {
                {"2","2"},{"3","3"}, {"4","4"},{"5","5"}});

        /*
         * offset so many rows that we see empty rs
         */
        queryAndCheck(
            stm,
            "select a,b from t1 offset 10 rows",
            new String [][] {});
        queryAndCheck(
            stm,
            "select a,b from t2 offset 10 rows",
            new String [][] {});
        queryAndCheck(
            stm,
            "select a,b from t3 offset 10 rows",
            new String [][] {});

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
        queryAndCheck(
            stm,
            "select max(a) from t2 group by b offset 0 rows",
            new String [][] {{"5"}});
        queryAndCheck(
            stm,
            "select max(a) from t3 group by b " +
            "    order by max(a) fetch next 2 rows only",
            new String [][] {{"1"},{"2"}});

        /*
         * Combine with union
         */

        queryAndCheck(
            stm,
            "select * from t1 union all select * from t1 " +
            "    fetch first 2 row only",
            new String [][] {{"1","1"}, {"1","2"}});

        /*
         * Combine with join
         */
        queryAndCheck(
            stm,
            "select t2.b, t3.b from t2,t3 where t2.a=t3.a " +
            "    fetch first 2 row only",
            new String [][] {{"1","1"}, {"1","2"}});

        stm.close();
    }


    /**
     * Positive tests.
     */
    public void testOffsetFetchFirstUpdatableForwardOnlyRS()
            throws Exception
    {
        Statement stm = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);

        setAutoCommit(false);

        /*
         * offset 0 rows (a no-op), update a row and verify result
         */
        ResultSet rs = stm.executeQuery("select * from t1  offset 0 rows");
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

        /*
         * offset 1 rows, update a row and verify result
         */
        rs = stm.executeQuery("select * from t1 offset 1 rows");
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
        stm.close();
    }


    /**
     * Positive tests with scrollable read-only.
     */
    public void testOffsetFetchFirstReadOnlyScrollableRS()
            throws Exception
    {
        Statement stm = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);

        /*
         * offset 0 rows (a no-op), update a row and verify result
         */
        ResultSet rs = stm.executeQuery("select * from t1  offset 0 rows");
        rs.next();
        rs.next(); // at row 2
        assertTrue(rs.getInt(2) == 2);
        rs.close();

        /*
         * offset 1 rows, fetch 3 row, check that we have the right ones
         */
        rs = stm.executeQuery(
            "select * from t1 " + "offset 1 rows fetch next 3 rows only");
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

        stm.close();
    }


    /**
     * Positive tests with SUR (Scrollable updatable result set).
     */
    public void testOffsetFetchFirstUpdatableScrollableRS()
            throws Exception
    {
        Statement stm = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_UPDATABLE);

        setAutoCommit(false);

        /*
         * offset 0 rows (a no-op), update a row and verify result
         * also try the "for update" syntax so we see that it still works
         */
        ResultSet rs = stm.executeQuery(
            "select * from t1  offset 0 rows for update");
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

        /*
         * offset 1 rows, fetch 3 row, update some rows and verify result
         */
        rs = stm.executeQuery(
            "select * from t1 offset 1 rows fetch next 3 rows only");
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

        // Test with projection
        rs = stm.executeQuery(
            "select * from t1 where a + 1 < b offset 1 rows");
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

        stm.close();
    }

    /**
     * Positive tests, result set metadata
     */
    public void testMetadata() throws SQLException {
        Statement stm = createStatement();

        ResultSet rs = stm.executeQuery("select * from t1 offset 1 rows");
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
        stm.close();
    }


    /**
     * Test that we see correct traces of the filtering in the statistics
     */
    public void testRunTimeStatistics() throws SQLException {
        Statement stm = createStatement();

        stm.executeUpdate("call syscs_util.syscs_set_runtimestatistics(1)");

        queryAndCheck(
            stm,
            "select a,b from t1 offset 2 rows",
            new String [][] {
                {"1","3"}, {"1","4"},{"1","5"}});

        stm.executeUpdate("call syscs_util.syscs_set_runtimestatistics(0)");

        ResultSet rs = stm.executeQuery(
            "values syscs_util.syscs_get_runtimestatistics()");
        rs.next();
        String plan = rs.getString(1);

        // Verify that the plan shows the filtering (2 rows of 3 seen):
        assertTrue(plan.indexOf("Row Count (1):\n" +
                                "Number of opens = 1\n" +
                                "Rows seen = 3\n" +
                                "Rows filtered = 2") != -1);

        rs.close();
        stm.close();
    }


    /**
     * Test against a bigger table
     */
    public void testBigTable() throws SQLException {
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

        queryAndCheck(
            stm,
            "select i from session.t offset 99999 rows",
            new String [][] {
                {"100000"}});

        stm.executeUpdate("drop table session.t");
        stm.close();
    }


    private void queryAndCheck(
        Statement stm,
        String queryText,
        String [][] expectedRows) throws SQLException {

        ResultSet rs = stm.executeQuery(queryText);
        JDBC.assertFullResultSet(rs, expectedRows);
    }
}

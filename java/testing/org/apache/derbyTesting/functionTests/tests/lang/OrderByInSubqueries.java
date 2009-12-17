/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.OrderByInSubqueries
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

/**
 * Tests for DERBY-4397: allow ORDER BY in subqueries.
 */
public class OrderByInSubqueries extends BaseJDBCTestCase {

    public OrderByInSubqueries(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("OrderByInSubqueries");

        suite.addTest(makeSuite());
        suite.addTest(
             TestConfiguration.clientServerDecorator(makeSuite()));

        return suite;
    }

    /**
     * Construct suite of tests
     *
     * @return A suite containing the test cases.
     */
    private static Test makeSuite()
    {
        return new CleanDatabaseTestSetup(
            new TestSuite(OrderByInSubqueries.class)) {
                protected void decorateSQL(Statement s)
                        throws SQLException {
                    getConnection().setAutoCommit(false);

                    s.execute("create table temp1(s varchar(10))");

                    // GENERATED ALWAYS AS IDENTITY
                    s.execute("create table temp2(" +
                              "i integer not null " +
                              "    generated always as identity," +
                              "s varchar(10))");
                    // DEFAULT value
                    s.execute("create table temp3(" +
                              "i integer not null " +
                              "    generated always as identity," +
                              "s varchar(10)," +
                              "j integer not null " +
                              "    default 66," +
                              "t varchar(10))");

                    // GENERATED ALWAYS AS (expression)
                    s.execute("create table temp4(" +
                              "i integer not null " +
                              "    generated always as identity," +
                              "s varchar(10)," +
                              "j integer not null " +
                              "    generated always as (2*i)," +
                              "t varchar(10))");


                    s.execute("create table t01(c1 int)");
                    s.execute("create table t02(c2 int)");

                    s.execute("create table t_source(c1 int, c2 varchar(10))");
                    s.execute("create table t(i int not null, " +
                              "               constraint c unique (i), " +
                              "               j int, k int)");

                    getConnection().commit();
                }
            };
    }

    /**
     * Test INSERT INTO t SELECT .. FROM .. ORDER BY ..
     */
    public void testInsertSelectOrderBy() throws SQLException {
        //
        // Shows that DERBY-4 is now solved.
        //
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs = null;

        s.execute("insert into temp1 values 'x','a','c','b','a'");
        s.execute("insert into temp2(s) select s from temp1 order by s");
        s.execute("insert into temp2(s) select s as a1 from temp1 order by a1");
        s.execute("insert into temp2(s) select * from temp1 order by s");

        rs = s.executeQuery("select * from temp2");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1", "a"},
                {"2", "a"},
                {"3", "b"},
                {"4", "c"},
                {"5", "x"},
                {"6", "a"},
                {"7", "a"},
                {"8", "b"},
                {"9", "c"},
                {"10", "x"},
                {"11", "a"},
                {"12", "a"},
                {"13", "b"},
                {"14", "c"},
                {"15", "x"}});

        rs = s.executeQuery("select * from temp2 order by i");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1", "a"},
                {"2", "a"},
                {"3", "b"},
                {"4", "c"},
                {"5", "x"},
                {"6", "a"},
                {"7", "a"},
                {"8", "b"},
                {"9", "c"},
                {"10", "x"},
                {"11", "a"},
                {"12", "a"},
                {"13", "b"},
                {"14", "c"},
                {"15", "x"}});

        s.execute("insert into temp2(s) select s as a1 from temp1 order by s");

        // This should be rejected as "no such column" errors:
        assertStatementError(
            "42X04", s,
            "insert into temp2(s) select s as a1 from temp1 order by no_such");

        // A similar example, but with integers rather than strings, and some
        // intermediate select statements to show that the ordering is working.
        //
        s.execute("insert into t01 values (50), (10), (1000), (15), (51)");

        rs = s.executeQuery("select * from t01");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"50"},
                {"10"},
                {"1000"},
                {"15"},
                {"51"}});


        s.execute("insert into t02 select * from t01 order by c1");
        s.execute("insert into t02 select * from t01");
        s.execute("insert into t02 select * from t01 order by c1");
        rs = s.executeQuery("select * from t02");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"10"},
                {"15"},
                {"50"},
                {"51"},
                {"1000"},
                {"50"},
                {"10"},
                {"1000"},
                {"15"},
                {"51"},
                {"10"},
                {"15"},
                {"50"},
                {"51"},
                {"1000"}});

        // Combining ORDER BY and VALUES is not legal SQL, cf.  SQL 2008,
        // section 14.11, Syntactic Rule 17: "A <query expression> simply
        // contained in a <from subquery> shall not be a <table value
        // constructor>. See also discussion in JIRA on DERBY-4413
        // (2009-OCT-23).
        //
        assertStatementError(
             "42X01", s,
             "insert into t02 values 66 order by 1");
        assertStatementError(
             "42X01", s,
             "insert into t02 values (901), (920), (903) order by 1");

        // But this should work:
        s.executeUpdate("delete from t02");
        s.executeUpdate("insert into t02 select 900 from sys.systables " +
                        "                union values 66 order by 1");
        rs = s.executeQuery("select * from t02");
        JDBC.assertFullResultSet(rs, new String[][]{
                 {"66"},
                 {"900"}});

        // other way around:
        s.executeUpdate("delete from t02");
        s.executeUpdate(
            "insert into t02 values 66 " +
            "       union select 900 from sys.systables order by 1");
        rs = s.executeQuery("select * from t02");
        JDBC.assertFullResultSet(rs, new String[][]{
                 {"66"},
                 {"900"}});

        // and, somewhat perversely (since a plain "values 66 order by 1" is
        // illegal), this:
        s.executeUpdate("delete from t02");
        s.executeUpdate("insert into t02 values 66 " +
                        "       union values 66 order by 1");
        rs = s.executeQuery("select * from t02");
        JDBC.assertFullResultSet(rs, new String[][]{
                 {"66"}});


        // UNION
        //
        // ok:
        s.execute("delete from t02");
        s.execute("insert into t02 select * from t01 union all " +
                  "                select * from t01 order by c1");
        rs = s.executeQuery("select * from t02");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"10"},
                {"10"},
                {"15"},
                {"15"},
                {"50"},
                {"50"},
                {"51"},
                {"51"},
                {"1000"},
                {"1000"}});

        // Not ok (c1 is not a column in the union result set, since t02 has
        // column c02.
        assertStatementError(
            "42X78", s,
            "insert into t02 select * from t01 union all " +
            "                select * from t02 order by c1");


        // Complication: project away sort column
        s.execute("delete from t02");
        s.execute("insert into t_source " +
                  "    values (1, 'one'), (2, 'two'), (8, 'three')");
        s.execute("insert into t_source(c1) " +
                  "    select c1 from t_source order by c2 desc");
        rs = s.executeQuery("select * from t_source");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1", "one"},
                {"2", "two"},
                {"8", "three"},
                {"2", null},
                {"8", null},
                {"1", null}});

        rollback();
    }


    /**
     * SELECT subqueries with ORDER BY
     */
    public void testSelectSubqueriesOrderBy() throws SQLException {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.execute(
            "insert into t_source values (1, 'one'), (2, 'two'), (8, 'three')");

        /*
         * Simple SELECT FromSubquery
         */
        rs = s.executeQuery(
            "select * from (select c1 from t_source order by c1 desc) s");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"8"}, {"2"}, {"1"}});

        rs = s.executeQuery(
            "select * from (select c1+1 from t_source order by c1+1 desc) s");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"9"}, {"3"}, {"2"}});

        rs = s.executeQuery(
            "select * from (select c1,c2 from t_source order by c1 desc,2) s");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"8", "three"}, {"2", "two"}, {"1", "one"}});

        // Complication: project away sort column
        rs = s.executeQuery(
            "select * from (select c2 from t_source order by c1 desc) s");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"three"}, {"two"}, {"one"}});

        rs = s.executeQuery(
            "select * from " +
            "    (select c2 from t_source order by c1 desc) s order by 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"one"}, {"three"}, {"two"}});

        /*
         * Simple VALUES FromSubquery
         */
        rs = s.executeQuery(
            "select * from (values (1, 'one'), (2, 'two'), (8, 'three')" +
            "               order by 1 desc) s");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"8", "three"}, {"2", "two"}, {"1", "one"}});


        /*
         * ORDER BY in EXISTS subquery
         */
        rs = s.executeQuery(
            "select c1 from t_source where exists " +
            "    (select c1 from t_source order by c1)");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"}, {"2"}, {"8"}});

        rs = s.executeQuery(
            "select c1 from t_source where exists " +
            "    (select c1 from t_source order by c1 desc)");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"}, {"2"}, {"8"}});

        rs = s.executeQuery(
            "select c1 from t_source where exists " +
            "    (select c1 from t_source order by c2 desc)");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"}, {"2"}, {"8"}});

        rs = s.executeQuery(
            "select c1 from t_source where exists " +
            "    (select c1 from t_source order by c2 desc) order by 1 desc");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"8"}, {"2"}, {"1"}});

        /*
         * NOT EXISTS
         */
        rs = s.executeQuery(
            "select c1 from t_source where not exists " +
            "    (select c1 from t_source order by c2 desc) order by 1 desc");
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
            "select c1 from t_source ot where not exists " +
            "   (select c1 from t_source where ot.c1=(c1/2) order by c2 desc)" +
            "    order by 1 desc");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"8"}, {"2"}});

        /*
         * IN subquery
         */
        s.executeUpdate("insert into t values (1,10,1), (2,40,1)," +
                        "         (3,45,1), (4,46,1), (5,90,1)");
        rs = s.executeQuery(
            "select * from t where i in (select i/5 from t order by 1 desc)");
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "10", "1"}});

        rs = s.executeQuery(
            "select * from t where i in (select i/5 from t order by i/5 desc)");
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "10", "1"}});

        rs = s.executeQuery(
            "select * from t where i in (select i/5 from t order by j)");
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "10", "1"}});


        /*
         * Scalar subquery inside ALL subquery with correlation
         */
        String[][] expected = new String[][]{
            {"1", "10", "1"},
            {"2", "40", "1"}};

        // First without any ORDER BYs
        rs = s.executeQuery(
            "select * from t t_o where i <= all (" +
            "    select i+1 from t where i = t_o.k + (" +
            "        select count(*) from t) - 5)");
        JDBC.assertFullResultSet(rs, expected);

        // Then with ORDER BY at both subquery levels; should be the same result
        rs = s.executeQuery(
            "select * from t t_o where i <= all (" +
            "    select i+1 from t where i = t_o.k + (" +
            "        select count(*) from t order by 1) - 5 " +
            "    order by 1 desc)");
        JDBC.assertFullResultSet(rs, expected);

        rollback();
    }


    /**
     * Test ORDER BY in a view definition
     */
    public void testView() throws SQLException {

        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.executeUpdate(
            "create view v1 as select i from t order by j desc");
        s.executeUpdate(
            "create view v2 as select i from t order by i");
        s.executeUpdate(
            "insert into t values (1,10,1), (2,40,1)," +
            "         (3,45,1), (4,46,1), (5,90,1)");
        rs = s.executeQuery(
            "select i from v1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"5"},{"4"},{"3"},{"2"},{"1"}});

        rs = s.executeQuery(
            "select i from v2");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},{"2"},{"3"},{"4"},{"5"}});

        rollback();
    }

    /**
     * SELECT subqueries with ORDER BY - negative tests
     */
    public void testSelectSubqueriesOrderByNegative() throws SQLException {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.execute(
            "insert into t_source values (1, 'one'), (2, 'two'), (8, 'three')");

        /*
         * Simple SELECT FromSubquery
         */
        assertStatementError(
            "42X04", s,
            "select * from (select c1 from t_source order by c3 desc) s");

        assertStatementError(
            "42X77", s,
            "select * from (select c1 from t_source order by 3 desc) s");


        /*
         * Simple VALUES FromSubquery
         */
        assertStatementError(
            "42X77", s,
            "select * from (values (1, 'one'), (2, 'two'), (8, 'three')" +
            "               order by 3 desc) s");

        /*
         * ORDER BY in EXISTS subquery:
         */
        assertStatementError(
            "42X04", s,
            "select c1 from t_source where exists " +
            "    (select c1 from t_source order by c4)");

        rollback();
    }
}

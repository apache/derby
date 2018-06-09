/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SubqueryFlatteningTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This test verifies that flattening of subqueries works correctly.
 *
 * The test cases in <tt>subqueryFlattening.sql</tt> could be moved to this
 * class when they are converted to JUnit.
 */
public class SubqueryFlatteningTest extends BaseJDBCTestCase {

    public SubqueryFlatteningTest(String name) {
        super(name);
    }

    public static Test suite() {
        // We're testing the SQL layer, so we run the test in embedded
        // mode only.
        return TestConfiguration.embeddedSuite(SubqueryFlatteningTest.class);
    }

    /**
     * Set up the test environment. Turn off auto-commit so that all the test
     * data can easily be removed by the rollback performed in
     * {@code BaseJDBCTestCase.tearDown()}.
     */
    protected void setUp() throws SQLException {
        setAutoCommit(false);
    }

    /**
     * Enable collection of runtime statistics in the current connection.
     * @param s the statement to use for enabling runtime statistics
     */
    private void enableRuntimeStatistics(Statement s) throws SQLException {
        s.execute("call syscs_util.syscs_set_runtimestatistics(1)");
    }

    /**
     * Check that a query returns the expected rows and whether or not it was
     * flattened to an exists join (or not exists join). An error is raised if
     * wrong results are returned or if the query plan is not the expected one.
     *
     * @param s the statement on which the query is executed
     * @param sql the query text
     * @param rows the expected result
     * @param flattenable whether or not we expect the query to be flattened
     * to a (not) exists join
     * @throws SQLException if a database error occurs
     * @throws junit.framework.AssertionFailedError if the wrong results are
     * returned from the query, or if the query plan is not as expected
     */
    private void checkExistsJoin(Statement s, String sql, String[][] rows,
                                 boolean flattenable)
            throws SQLException
    {
        JDBC.assertFullResultSet(s.executeQuery(sql), rows);
        RuntimeStatisticsParser parser =
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertEquals("unexpected plan", flattenable, parser.usedExistsJoin());
    }

    /**
     * DERBY-4001: Test that certain NOT EXISTS/NOT IN/ALL subqueries are
     * flattened, and that their predicates are not pulled out. Their
     * predicates are known to be always false, so when they are (correctly)
     * applied on the subquery, they will cause all the rows from the outer
     * query to be returned. If the predicates are (incorrectly) pulled out to
     * the outer query, the query won't return any rows at all. DERBY-4001.
     */
    public void testNotExistsFlattenablePredicatesNotPulled()
            throws SQLException
    {
        Statement s = createStatement();
        // X must be NOT NULL, otherwise X NOT IN and X < ALL won't be
        // rewritten to NOT EXISTS
        s.execute("create table t (x int not null)");
        s.execute("insert into t values 1,2,3");

        enableRuntimeStatistics(s);

        String[][] allRows = {{"1"}, {"2"}, {"3"}};

        checkExistsJoin(
                s,
                "select * from t where not exists (select x from t where 1<>1)",
                allRows, true);

        checkExistsJoin(
                s,
                "select * from t where x not in (select x from t where 1<>1)",
                allRows, true);

        checkExistsJoin(
                s,
                "select * from t where x < all (select x from t where 1<>1)",
                allRows, true);
    }

    /**
     * DERBY-4001: Test that some ALL subqueries that used to be flattened to
     * a not exists join and return incorrect results, are not flattened.
     * These queries should not be flattened because the generated NOT EXISTS
     * JOIN condition or some of the subquery's predicates could be pushed
     * down into the left side of the join, which is not allowed in a not
     * exists join because the predicates have a completely different effect
     * if they're used on one side of the join than if they're used on the
     * other side of the join.
     */
    public void testAllNotFlattenableToNotExists() throws SQLException {
        Statement s = createStatement();
        // X must be NOT NULL, otherwise rewriting ALL to NOT EXISTS won't even
        // be attempted
        s.execute("create table t (x int not null)");
        s.execute("insert into t values 1,2,3");

        enableRuntimeStatistics(s);

        String[][] allRows = {{"1"}, {"2"}, {"3"}};

        // Join condition is X >= 100, which should make the right side of
        // the not exists join empty and return all rows from the left side.
        // If (incorrectly) pushed down on the left side, no rows will be
        // returned.
        checkExistsJoin(
                s, "select * from t where x < all (select 100 from t)",
                allRows, false);

        // Join condition is 1 >= 100, which should make the right side of
        // the not exists join empty and return all rows from the left side.
        // If (incorrectly) pushed down on the left side, no rows will be
        // returned.
        checkExistsJoin(
                s, "select * from t where 1 < all (select 2 from t)",
                allRows, false);

        // Join condition is X <> 1, which will remove the only interesting
        // row from the left side if (incorrectly) pushed down there.
        checkExistsJoin(
                s, "select * from t where x = all (select 1 from t)",
                new String[][]{{"1"}}, false);

        // Join condition is T1.X >= T2.X which cannot be pushed down on the
        // left side. The predicate in the subquery (T1.X > 100) can be pushed
        // down on the left side and filter out rows that should not be
        // filtered out, so check that this query is not flattened.
        checkExistsJoin(
                s, "select * from t t1 where x < all " +
                "(select x from t t2 where t1.x > 100)",
                allRows, false);

        // Same as above, but with an extra, unproblematic predicate added
        // to the subquery.
        checkExistsJoin(
                s, "select * from t t1 where x < all " +
                "(select x from t t2 where t1.x > 100 and t2.x > 100)",
                allRows, false);

        // Same as above, but since the problematic predicate is ORed with
        // an unproblematic one, it is not possible to push it down on the
        // left side (only ANDed predicates can be split and pushed down
        // separately), so in this case we expect the query to be flattened.
        // (This query worked correctly also before DERBY-4001 was fixed.)
        checkExistsJoin(
                s, "select * from t t1 where x < all " +
                "(select x from t t2 where t1.x > 100 or t2.x > 100)",
                allRows, true);
    }

    /**
     * DERBY-4001: Test that some NOT IN subqueries that used to be flattened
     * to a not exists join and return incorrect results, are not flattened.
     * These queries should not be flattened because the generated NOT EXISTS
     * JOIN condition or some of the subquery's predicates could be pushed
     * down into the left side of the join, which is not allowed in a not
     * exists join because the predicates have a completely different effect
     * if they're used on one side of the join than if they're used on the
     * other side of the join.
     */
    public void testNotInNotFlattenableToNotExists() throws SQLException {
        Statement s = createStatement();
        // X must be NOT NULL, otherwise rewriting NOT IN to NOT EXISTS won't
        // even be attempted
        s.execute("create table t (x int not null)");
        s.execute("insert into t values 1,2,3");

        enableRuntimeStatistics(s);

        String[][] allRows = {{"1"}, {"2"}, {"3"}};

        // Join condition is X = 100, which should make the right side of
        // the not exists join empty and return all rows from the left side.
        // If (incorrectly) pushed down on the left side, no rows will be
        // returned.
        checkExistsJoin(
                s, "select * from t where x not in (select 100 from t)",
                allRows, false);

        // Join condition is 1 = 100, which should make the right side of
        // the not exists join empty and return all rows from the left side.
        // If (incorrectly) pushed down on the left side, no rows will be
        // returned.
        checkExistsJoin(
                s, "select * from t where 1 not in (select 100 from t)",
                allRows, false);

        // Join condition is X = 2, which will remove the interesting rows
        // from the left side if (incorrectly) pushed down there.
        checkExistsJoin(
                s, "select * from t where x not in (select 2 from t)",
                new String[][]{{"1"}, {"3"}}, false);

        // Join condition is T1.X = T2.X which cannot be pushed down on the
        // left side. The predicate in the subquery (T1.X > 100) can be pushed
        // down on the left side and filter out rows that should not be
        // filtered out, so check that this query is not flattened.
        checkExistsJoin(
                s, "select * from t t1 where x not in " +
                "(select x from t t2 where t1.x > 100)",
                allRows, false);

        // Same as above, but with an extra, unproblematic predicate added
        // to the subquery.
        checkExistsJoin(
                s, "select * from t t1 where x not in " +
                "(select x from t t2 where t1.x > 100 and t2.x > 100)",
                allRows, false);

        // Same as above, but since the problematic predicate is ORed with
        // an unproblematic one, it is not possible to push it down on the
        // left side (only ANDed predicates can be split and pushed down
        // separately), so in this case we expect the query to be flattened.
        // (This query worked correctly also before DERBY-4001 was fixed.)
        checkExistsJoin(
                s, "select * from t t1 where x not in " +
                "(select x from t t2 where t1.x > 100 or t2.x > 100)",
                allRows, true);
    }
}

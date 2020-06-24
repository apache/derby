/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.InPredicateTest

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for IN predicates.
 */
public class InPredicateTest extends BaseJDBCTestCase {
    public InPredicateTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This is a test for language features, so running in one
        // configuration should be enough.
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(InPredicateTest.class));
    }

    /**
     * <p>
     * Test case for DERBY-6017. InListOperatorNode optimizes the case
     * where all values in the IN list are constant and represent the same
     * value, but the optimization could get confused if the IN list had
     * constants of different types.
     * </p>
     *
     * <p>
     * For example, a predicate such as {@code x IN (9223372036854775806,
     * 9223372036854775807, 9.223372036854776E18)} would be optimized to
     * {@code x = 9223372036854775806}, which is not an equivalent expression.
     * </p>
     *
     * <p>
     * It is correct to reduce the IN list to a single comparison in this
     * case, since all the values in the IN list should be converted to the
     * dominant type. The dominant type in the list is DOUBLE, and all three
     * values are equal when they are converted to DOUBLE (because DOUBLE can
     * only approximate the integers that are close to Long.MAX_VALUE).
     * However, the simplified expression needs to use the value as a DOUBLE,
     * otherwise it cannot be used as a substitution for all the values in
     * the IN list.
     * </p>
     *
     * <p>
     * DERBY-6017 solves it by optimizing the above predicate to
     * {@code x = CAST(9223372036854775806 AS DOUBLE)}.
     * </p>
     */
    public void testDuplicateConstantsMixedTypes() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.executeUpdate("create table t1(b bigint)");

        String[][] allRows = {
            { Long.toString(Long.MAX_VALUE - 2) },
            { Long.toString(Long.MAX_VALUE - 1) },
            { Long.toString(Long.MAX_VALUE)     },
        };

        // Fill the table with BIGINT values so close to Long.MAX_VALUE that
        // they all degenerate to a single value when converted to DOUBLE.
        PreparedStatement insert = prepareStatement("insert into t1 values ?");
        for (int i = 0; i < allRows.length; i++) {
            insert.setString(1, allRows[i][0]);
            insert.executeUpdate();
        }

        // Expect this query to return all the rows in the table. It used
        // to return only the first row.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select * from t1 where b in " +
                "(9223372036854775805, 9223372036854775806," +
                " 9223372036854775807, 9.223372036854776E18)"),
                allRows);

        // SQL:2003, 8.4 <in predicate> says IN (x,y,z) is equivalent to
        // IN (VALUES x,y,z), and also that x IN (...) is equivalent to
        // x = ANY (...). Verify the correctness of the above result by
        // comparing to the following equivalent queries.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select * from t1 where b in " +
                "(values 9223372036854775805, 9223372036854775806," +
                " 9223372036854775807, 9.223372036854776E18)"),
                allRows);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select * from t1 where b = any " +
                "(values 9223372036854775805, 9223372036854775806," +
                " 9223372036854775807, 9.223372036854776E18)"),
                allRows);
    }

    /**
     * Another test case for DERBY-6017. Derby used to evaluate IN lists by
     * using the semantics of the dominant type of the two values being
     * compared. It should use the dominant type of all the values in the
     * left operand and in the IN list. Verify that it works as expected now.
     */
    public void testMixedTypes() throws SQLException {
        setAutoCommit(false);

        // Test an IN predicate that mixes BIGINT and DOUBLE.

        Statement s = createStatement();
        s.executeUpdate("create table t2(b1 bigint, b2 bigint, d double)");
        s.executeUpdate("insert into t2 values " +
                        "(9223372036854775805, 9223372036854775806, 1)");

        // The first query used to return zero rows. However, the next two
        // queries used to return one row, and SQL:2003, 8.4 <in predicate>
        // says that the three queries are equivalent. Now, they all return
        // one row.

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select true from t2 where b1 in (b2, d)"),
            "true");

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select true from t2 where b1 in (values b2, d)"),
            "true");

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select true from t2 where b1 = any (values b2, d)"),
            "true");

        // Test an IN predicate that mixes INT and REAL. They are supposed
        // to be compared using DOUBLE semantics, but used to be compared as
        // REALs.

        s.executeUpdate("create table t3 (i1 int, r1 real, r2 real)");
        s.executeUpdate("insert into t3 values " +
                        "(2147483645, 2147483645, 2147483645), " +
                        "(2147483645, 2147483645, 0)");

        Object[][] expectedRows = {
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                2147483645,
                2.14748365E9f,
                2.14748365E9f,
            }
        };

        // The first query used to return two rows. However, the next two
        // queries used to return one row, and SQL:2003, 8.4 <in predicate>
        // says that the three queries are equivalent. Now, they all return
        // one row.

        JDBC.assertFullResultSet(
            s.executeQuery("select * from t3 where r1 in (i1, r2)"),
            expectedRows, false);

        JDBC.assertFullResultSet(
            s.executeQuery("select * from t3 where r1 in (values i1, r2)"),
            expectedRows, false);

        JDBC.assertFullResultSet(
            s.executeQuery("select * from t3 where r1 = any (values i1, r2)"),
            expectedRows, false);
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.JoinTest

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for JOINs.
 */
public class JoinTest extends BaseJDBCTestCase {
    private static final String SYNTAX_ERROR = "42X01";
    private static final String AMBIGUOUS_COLNAME = "42X03";

    public JoinTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(JoinTest.class);
    }

    /**
     * Test that the columns returned by a left or right outer join have the
     * correct nullability. In a left outer join, the columns from the left
     * side of the join should have their original nullability, and all the
     * columns from the right side of the join should be nullable. In a right
     * outer join, all the columns from the left side should be nullable,
     * and the columns from the right side should preserve their original
     * nullability. DERBY-4284.
     */
    public void testNullabilityInLeftOrRightOuterJoin() throws SQLException {
        // Turn auto-commit off so that tearDown() can roll back all test data
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t (c1 int not null, c2 int not null, c3 int)");

        // Nullability should be unchanged for columns from the left side
        // (first three columns) and nullable for the ones from the right side).
        ResultSet rs = s.executeQuery(
                "select * from t t1 left outer join t t2 on 1=1");
        JDBC.assertNullability(rs,
                new boolean[]{false, false, true, true, true, true});
        JDBC.assertEmpty(rs);

        // Nullability should be unchanged for columns from the right side of
        // the right outer join, and nullable for the ones from the left side.
        rs = s.executeQuery(
                "select * from t t1 right outer join t t2 on 1=1");
        JDBC.assertNullability(rs,
                new boolean[]{true, true, true, false, false, true});
        JDBC.assertEmpty(rs);

        // CASTs had some problems where they set the nullability too early
        // to get it correctly from the underlying join. Test it here.
        rs = s.executeQuery(
                "select cast(t1.c1 as int), cast(t2.c2 as int) from " +
                "t t1 left outer join t t2 on 1=1");
        JDBC.assertNullability(rs, new boolean[]{false, true});
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
                "select cast(t1.c1 as int), cast(t2.c2 as int) from " +
                "t t1 right outer join t t2 on 1=1");
        JDBC.assertNullability(rs, new boolean[]{true, false});
        JDBC.assertEmpty(rs);

        // Nested outer joins
        rs = s.executeQuery(
                "select t1.c1, t2.c1, t3.c1 from " +
                "t t1 left join (t t2 left join t t3 on 1=1) on 1=1");
        JDBC.assertNullability(rs, new boolean[]{false, true, true});
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
                "select t1.c1, t2.c1, t3.c1 from " +
                "t t1 right join (t t2 right join t t3 on 1=1) on 1=1");
        JDBC.assertNullability(rs, new boolean[]{true, true, false});
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
                "select t1.c1, t2.c1, t3.c1, t4.c1 from " +
                "(t t1 left join t t2 on 1=1) left join " +
                "(t t3 left join t t4 on 1=1) on 1=1");
        JDBC.assertNullability(rs, new boolean[]{false, true, true, true});
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
                "select t1.c1, t2.c1, t3.c1, t4.c1 from " +
                "(t t1 left join t t2 on 1=1) right join " +
                "(t t3 left join t t4 on 1=1) on 1=1");
        JDBC.assertNullability(rs, new boolean[]{true, true, false, true});
        JDBC.assertEmpty(rs);

        rs = s.executeQuery(
                "select t1.c1, t2.c1, t3.c1, t4.c1 from " +
                "(t t1 right join t t2 on 1=1) left join " +
                "(t t3 left join t t4 on 1=1) on 1=1");
        JDBC.assertNullability(rs, new boolean[]{true, false, true, true});
        JDBC.assertEmpty(rs);
    }

    /**
     * Test the CROSS JOIN syntax that was added in DERBY-4355.
     */
    public void testCrossJoins() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        final String[][] T1 = {
            {"1", "one"}, {"2", "two"}, {"3", null},
            {"5", "five"}, {"6", "six"}
        };

        final String[][] T2 = {
            {"1", null}, {"2", "II"}, {"4", "IV"}
        };

        Statement s = createStatement();
        s.execute("create table t1(c1 int, c2 varchar(10))");
        fillTable("insert into t1 values (?,?)", T1);
        s.execute("create table t2(c1 int, c2 varchar(10))");
        fillTable("insert into t2 values (?,?)", T2);

        // Simple join
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2"),
            cross(T1, T2));

        // Self join
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 a cross join t1 b"),
            cross(T1, T1));

        // Change order in select list
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t2.*, t1.* from t1 cross join t2"),
            cross(T2, T1));

        // Multiple joins
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2 cross join t1 t3"),
            cross(T1, cross(T2, T1)));

        // Project one column
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.c2 from t1 cross join t2"),
            project(new int[]{1}, cross(T1, T2)));

        // Project more columns
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.c1, t2.c2, t2.c2 from t1 cross join t2"),
            project(new int[]{0, 3, 3}, cross(T1, T2)));

        // Aggregate function
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select count(*) from t1 cross join t2"),
            Integer.toString(T1.length * T2.length));

        // INNER JOIN using CROSS JOIN + WHERE
        String[][] expectedInnerJoin = new String[][] {
            {"1", "one", "1", null}, {"2", "two", "2", "II"}
        };
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2 where t1.c1=t2.c1"),
            expectedInnerJoin);
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 inner join t2 on t1.c1=t2.c1"),
            expectedInnerJoin);

        // ORDER BY
        JDBC.assertFullResultSet(
            s.executeQuery("select * from t1 cross join t2 " +
                           "order by t1.c1 desc"),
            reverse(cross(T1, T2)));

        // GROUP BY
        JDBC.assertFullResultSet(
            s.executeQuery("select t1.c1, count(t1.c2) from t1 cross join t2 " +
                           "group by t1.c1 order by t1.c1"),
            new String[][]{
                {"1", "3"}, {"2", "3"}, {"3", "0"}, {"5", "3"}, {"6", "3"}
            });

        // Join VALUES expressions
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from (values 1,2) v1 cross join (values 'a','b') v2"),
            new String[][]{{"1", "a"}, {"1", "b"}, {"2", "a"}, {"2", "b"}});

        // Mix INNER and CROSS
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a cross join t2 b inner join t2 c on 1=1"),
            cross(T1, cross(T2, T2)));
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join t2 b on 1=1 cross join t2 c"),
            cross(T1, cross(T2, T2)));
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join (t2 b cross join t2 c) on 1=1"),
            cross(T1, cross(T2, T2)));
        // RESOLVE: The syntax below should be allowed.
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join t2 b cross join t2 c on 1=1"),
            cross(T1, cross(T2, T2)));

        // Check that the implicit nesting is correct.
        // A CROSS B RIGHT C should nest as (A CROSS B) RIGHT C and
        // not as A CROSS (B RIGHT C).
        //
        // 1) Would have failed if nesting was incorrect because A.C1 would be
        //    out of scope for the join specification
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select count(*) from t2 a cross join "+
                           "t1 b right join t2 c on a.c1=c.c1"),
            Integer.toString(T1.length * T2.length));
        // 2) Would have returned returned wrong result if nesting was
        //    incorrect
        String[][] expectedCorrectlyNested =
                new String[][]{{null, null, null, null, "4", "IV"}};
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t2 a cross join t1 b " +
                           "right join t2 c on b.c1=c.c1 where c.c1=4"),
            expectedCorrectlyNested);
        // 3) An explicitly nested query, equivalent to (2), so expect the
        //    same result
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from (t2 a cross join t1 b) " +
                           "right join t2 c on b.c1=c.c1 where c.c1=4"),
            expectedCorrectlyNested);
        // 4) An explicitly nested query, not equivalent to (2) or (3), so
        //    expect different results
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t2 a cross join (t1 b " +
                           "right join t2 c on b.c1=c.c1) where c.c1=4"),
            new String[][] {
                {"1", null, null, null, "4", "IV"},
                {"2", "II", null, null, "4", "IV"},
                {"4", "IV", null, null, "4", "IV"}});

        // ***** Negative tests *****

        // Self join must have alias to disambiguate column names
        assertStatementError(
                AMBIGUOUS_COLNAME, s, "select * from t1 cross join t1");

        // Column name must be qualified if ambiguous
        assertStatementError(
                AMBIGUOUS_COLNAME, s, "select c1 from t1 cross join t2");

        // CROSS JOIN cannot have ON clause, expect syntax error
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 cross join t2 on t1.c1 = t2.c2");

        // Mixed CROSS with INNER/LEFT/RIGHT still needs ON
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 inner join t2 cross join t2 t3");
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 left join t2 cross join t2 t3");
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 right join t2 cross join t2 t3");
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 cross join t2 inner join t2 t3");
    }

    /**
     * Fill a table with rows.
     *
     * @param sql the insert statement used to populate the table
     * @param data the rows to insert into the table
     */
    private void fillTable(String sql, String[][] data) throws SQLException {
        PreparedStatement ins = prepareStatement(sql);
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                ins.setString(j + 1, data[i][j]);
            }
            ins.executeUpdate();
        }
        ins.close();
    }

    /**
     * Calculate the Cartesian product of two tables.
     *
     * @param t1 the rows in the table on the left side
     * @param t2 the rows in the table on the right side
     * @return a two-dimensional array containing the Cartesian product of the
     * two tables (primary ordering same as t1, secondary ordering same as t2)
     */
    private static String[][] cross(String[][] t1, String[][] t2) {
        String[][] result = new String[t1.length * t2.length][];
        for (int i = 0; i < result.length; i++) {
            String[] r1 = t1[i / t2.length];
            String[] r2 = t2[i % t2.length];
            result[i] = new String[r1.length + r2.length];
            System.arraycopy(r1, 0, result[i], 0, r1.length);
            System.arraycopy(r2, 0, result[i], r1.length, r2.length);
        }
        return result;
    }

    /**
     * Project columns from a table.
     *
     * @param cols the column indexes (0-based) to project
     * @param rows the rows in the table
     * @return the projected result
     */
    private static String[][] project(int[] cols, String[][] rows) {
        String[][] result = new String[rows.length][cols.length];
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < cols.length; j++) {
                result[i][j] = rows[i][cols[j]];
            }
        }
        return result;
    }

    /**
     * Reverse the order of rows in a table.
     *
     * @param rows the rows in the table
     * @return the rows in reverse order
     */
    private static String[][] reverse(String[][] rows) {
        String[][] result = new String[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            result[i] = rows[rows.length - 1 - i];
        }
        return result;
    }
}

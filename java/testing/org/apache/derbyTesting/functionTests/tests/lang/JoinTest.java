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
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

/**
 * Test cases for JOINs.
 */
public class JoinTest extends BaseJDBCTestCase {
    private static final String SYNTAX_ERROR = "42X01";
    private static final String AMBIGUOUS_COLNAME = "42X03";
    private static final String COLUMN_NOT_IN_SCOPE = "42X04";
    private static final String NON_COMPARABLE = "42818";
    private static final String NO_COLUMNS = "42X81";
    private static final String TABLE_NAME_NOT_IN_SCOPE = "42X10";
    private static final String VALUES_WITH_NULL = "42X07";

    public JoinTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(JoinTest.class);
    }

    /**
     * DERBY-4365 Test that the NULL values are caught in VALUES clause when it
     * is part of a non-INSERT statement. Throw exception 42X07 for such a
     * case.
     *
     */
    public void testNullabilityInValues() throws SQLException {
        Statement s = createStatement();
        assertStatementError(
        		VALUES_WITH_NULL, s,
        		"select a.* from (values (null)) a left outer join "+
        		"(values ('a')) b on 1=1");
        assertStatementError(
        		VALUES_WITH_NULL, s,
        		"select a.* from (values (null)) a");

        String[][] expectedResult = {
            {"a"},
            {"a"},
            {"b"},
            {"b"},
            {null},
            {null}
        };
        JDBC.assertUnorderedResultSet(s.executeQuery(
        		"select a.* from (values ('a'),('b'),(cast(null as char(1)))) "
        		+ "a left outer join (values ('c'),('d')) b on 1=1"),
        		expectedResult);
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
     * DERBY-4372: Some joins used to miss some rows after an index was
     * created, because the start and stop keys passed to the index scan were
     * wrong if the IN list in the JOIN condition contained a NULL.
     */
    public void testDerby4372() throws SQLException {
        Statement s = createStatement();
        s.execute("create table d4372_1 (a int, b int)");
        s.execute("create table d4372_2 (c int)");
        s.execute("insert into d4372_1 values (1,1),(null,1),(1,null)," +
                "(2,2),(2,null),(null,2),(3,3),(null,3),(3,null),(null,null)");
        s.execute("insert into d4372_2 values (1), (3)");

        String[][] expectedJoinResult = {
            {"1", "1", "1"},
            {null, "1", "1"},
            {"1", null, "1"},
            {"3", "3", "3"},
            {null, "3", "3"},
            {"3", null, "3"}
        };

        // Try a problematic join, but without an index.
        PreparedStatement ps = prepareStatement(
                "select * from d4372_1 join d4372_2 on c in (a, b)");

        JDBC.assertUnorderedResultSet(ps.executeQuery(), expectedJoinResult);

        // Now create an index on C and retry the join. Should still return the
        // same rows, but didn't before DERBY-4372 was fixed.
        s.execute("create index d4372_idx on d4372_2(c)");
        JDBC.assertUnorderedResultSet(ps.executeQuery(), expectedJoinResult);

        s.execute("drop table d4372_1");
        s.execute("drop table d4372_2");
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

    /**
     * Tests for the USING clause added in DERBY-4370.
     */
    public void testUsingClause() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        Statement s = createStatement();

        s.execute("create table t1(a int, b int, c int)");
        s.execute("create table t2(b int, c int, d int)");
        s.execute("create table t3(d int, e varchar(5), f int)");

        s.execute("insert into t1 values (1,2,3),(2,3,4),(4,4,4)");
        s.execute("insert into t2 values (1,2,3),(2,3,4),(5,5,5)");
        s.execute("insert into t3 values " +
                "(2,'abc',3),(4,'def',5),(null,null,null)");

        // Simple one-column USING clauses for the different joins. Expected
        // column order: First, the columns from the USING clause. Then,
        // non-join columns from left side followed by non-join columns from
        // right side.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (b)"),
            new String[][]{{"2", "1", "3", "3", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 inner join t2 using (b)"),
            new String[][]{{"2", "1", "3", "3", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"3", "2", "4", null, null},
                {"4", "4", "4", null, null}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left outer join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"3", "2", "4", null, null},
                {"4", "4", "4", null, null}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 right join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"1", null, null, "2", "3"},
                {"5", null, null, "5", "5"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 right outer join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"1", null, null, "2", "3"},
                {"5", null, null, "5", "5"}});

        // Two-column clauses
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (b, c)"),
            new String[][]{{"2", "3", "1", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (c, b)"),
            new String[][]{{"3", "2", "1", "4"}});

        // Qualified asterisks should expand to all non-join columns
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select t1.* from t1 join t2 using (b, c)"),
            "1");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select t2.* from t1 join t2 using (b, c)"),
            "4");
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.*, t2.* from t1 join t2 using (b, c)"),
            new String[][]{{"1", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.* from t1 left join t2 using (b, c)"),
            new String[][]{{"1"}, {"2"}, {"4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.* from t1 right join t2 using (b, c)"),
            new String[][]{{"1"}, {null}, {null}});

        // USING clause can be in between joins or at the end
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select t3.e from t1 join t2 using (b) join t3 using (d)"),
            "def");
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select t3.e from t1 join t2 join t3 using (d) using (b)"),
            "def");

        // USING can be placed in between or after outer joins as well, but
        // then the results are different (different nesting of the joins).
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 using (b) " +
                           "right join t3 using (d)"),
            new String[][] {
                    {"2", null, null, null, null, "abc", "3"},
                    {"4", "2", "1", "3", "3", "def", "5"},
                    {null, null, null, null, null, null, null}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 " +
                           "right join t3 using (d) using (b)"),
            new String[][] {
                    {"2", "1", "3", "4", "3", "def", "5"},
                    {"3", "2", "4", null, null, null, null},
                    {"4", "4", "4", null, null, null, null}});

        // Should be able to reference a non-join column without qualifier if
        // it's unambiguous.
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select a from t1 join t2 using (b, c)"),
            "1");

        // USING clause should accept quoted identifiers.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (\"B\")"),
            new String[][]{{"2", "1", "3", "3", "4"}});

        // When referencing a join column X without a table qualifier in an
        // outer join, the value should be coalesce(t1.x, t2.x). That is, the
        // value should be non-null if one of the qualified columns is non-null.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select b from t1 left join t2 using (b)"),
                new String[][]{{"2"}, {"3"}, {"4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select b from t1 right join t2 using (b)"),
                new String[][]{{"1"}, {"2"}, {"5"}});
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select d, t2.d, t3.d from t2 left join t3 using (d)"),
            new String[][] {
                {"3", "3", null},
                {"4", "4", "4"},
                {"5", "5", null}});
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select d, t2.d, t3.d from t2 right join t3 using (d)"),
            new String[][] {
                {"2", null, "2"},
                {"4", "4", "4"},
                {null, null, null}});
        JDBC.assertEmpty(s.executeQuery(
            "select * from t2 left join t3 using (d) where d is null"));
        JDBC.assertUnorderedResultSet(s.executeQuery(
            "select * from t2 right join t3 using (d) where d is null"),
            new String[][]{{null, null, null, null, null}});

        // Verify that ORDER BY picks up the correct column.
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                           "order by c desc nulls last"),
            new String[][]{{"4"}, {"4"}, {"3"}});
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                           "order by t1.c desc nulls last"),
            new String[][]{{"4"}, {"4"}, {"3"}});
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                           "order by t2.c desc nulls last"),
            new String[][]{{"3"}, {"4"}, {"4"}});
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                           "order by c desc nulls last fetch next row only"),
            "5");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                           "order by t1.c desc nulls last fetch next row only"),
            "3");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                           "order by t2.c desc nulls last fetch next row only"),
            "5");

        // Aggregate + GROUP BY
        JDBC.assertFullResultSet(
            s.executeQuery("select b, count(t2.b) from t1 left join t2 " +
                           "using (b) group by b order by b"),
            new String[][]{{"2", "1"}, {"3", "0"}, {"4", "0"}});

        // Using aliases to construct common column names.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 table_a(col1, col2, col3) " +
                           "inner join t3 table_b(col1, col2, col3) " +
                           "using (col1)"),
            new String[][] {
                {"2", "3", "4", "abc", "3"},
                {"4", "4", "4", "def", "5"}});

        // ***** Negative tests *****

        // Use of unqualified non-join columns should result in errors if
        // columns with that name exist in both tables.
        assertStatementError(AMBIGUOUS_COLNAME, s,
                "select b from t1 join t2 using (b) join t3 using(c)");
        assertStatementError(AMBIGUOUS_COLNAME, s,
                "select b from t1 join t2 using (c)");
        assertStatementError(AMBIGUOUS_COLNAME, s,
                "select * from t1 join t2 using (b) order by c");

        // Column names in USING should not be qualified.
        assertStatementError(SYNTAX_ERROR, s,
                "select * from t1 join t2 using (t1.b)");

        // USING needs parens even if only one column is specified.
        assertStatementError(SYNTAX_ERROR, s,
                "select * from t1 join t2 using b");

        // Empty column list is not allowed.
        assertStatementError(SYNTAX_ERROR, s,
                "select * from t1 join t2 using ()");

        // Join columns with non-comparable data types should fail (trying to
        // compare INT and VARCHAR).
        assertStatementError(NON_COMPARABLE, s,
                "select * from t2 a(x,y,z) join t3 b(x,y,z) using(y)");

        // The two using clauses come in the wrong order, so expect that
        // column B is not found.
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select t3.e from t1 join t2 join t3 using (b) using (d)");

        // References to non-common or non-existent columns in the using clause
        // should result in an error.
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (a)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (d)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (a,d)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (a,b,c)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (x)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
                "select * from t1 join t2 using (b,c,x)");

        // If two columns in the left table are named B, we should get an
        // error when specifying B as a join column, since we don't know which
        // of the columns to use.
        assertStatementError(AMBIGUOUS_COLNAME, s,
                "select * from (t1 cross join t2) join t2 tt2 using(b)");

        // DERBY-4407: If all the columns of table X are in the USING clause,
        // X.* will expand to no columns. A result should always have at least
        // one column.
        assertStatementError(NO_COLUMNS, s,
                "select x.* from t1 x inner join t1 y using (a,b,c)");
        assertStatementError(NO_COLUMNS, s,
                "select x.* from t1 x left join t1 y using (a,b,c)");
        assertStatementError(NO_COLUMNS, s,
                "select x.* from t1 x right join t1 y using (a,b,c)");

        // DERBY-4410: If X.* expanded to no columns, the result column that
        // immediately followed it (Y.*) would not be expanded, which eventually
        // resulted in a NullPointerException.
        assertStatementError(NO_COLUMNS, s,
                "select x.*, y.* from t1 x inner join t1 y using (a, b, c)");

        // DERBY-4414: If the table name in an asterisked identifier chain does
        // not match the table names of either side in the join, the query
        // should fail gracefully and not throw a NullPointerException.
        assertStatementError(TABLE_NAME_NOT_IN_SCOPE, s,
                "select xyz.* from t1 join t2 using (b)");
    }

    /**
     * Tests for the NATURAL JOIN syntax added in DERBY-4495.
     */
    public void testNaturalJoin() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        final String[][] T1 = {
            {"1", "2", "3"}, {"4", "5", "6"}, {"7", "8", "9"}
        };

        final String[][] T2 = {
            {"4", "3", "2"}, {"1", "2", "3"},  {"3", "2", "1"}
        };

        final String[][] T3 = {{"4", "100"}};

        Statement s = createStatement();
        s.execute("create table t1(a int, b int, c int)");
        s.execute("create table t2(d int, c int, b int)");
        s.execute("create table t3(d int, e int)");

        fillTable("insert into t1 values (?,?,?)", T1);
        fillTable("insert into t2 values (?,?,?)", T2);
        fillTable("insert into t3 values (?,?)", T3);

        // Join on single common column (D)
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t2 natural join t3"),
                new String[][] {{"4", "3", "2", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t3 natural join t2"),
                new String[][] {{"4", "100", "3", "2"}});

        // Join on two common columns (B and C). Expected column ordering:
        //    1) all common columns, same order as in left table
        //    2) all non-common columns from left table
        //    3) all non-common columns from right table
        ResultSet rs = s.executeQuery("select * from t1 natural join t2");
        JDBC.assertColumnNames(rs, new String[] {"B", "C", "A", "D"});
        JDBC.assertUnorderedResultSet(
                rs, new String[][] {{"2", "3", "1", "4"}});

        rs = s.executeQuery("select * from t2 natural join t1");
        JDBC.assertColumnNames(rs, new String[] {"C", "B", "D", "A"});
        JDBC.assertUnorderedResultSet(
                rs, new String[][] {{"3", "2", "4", "1"}});

        // No common column names means cross join
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural join t3"),
                cross(T1, T3));
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 as a(c1, c2, c3) " +
                               "natural join t2 as b(c4, c5, c6)"),
                cross(T1, T2));
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (values 1,2) v1(x) " +
                               "natural join (values 'a','b') v2(y)"),
                new String[][] {{"1","a"}, {"1","b"}, {"2","a"}, {"2","b"}});

        // Join two sub-queries
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (select * from t1) table1 " +
                               "natural join (select * from t2) table2"),
                new String[][] {{"2", "3", "1", "4"}});

        // Expressions with no explicit names are not common columns because
        // we give them different implicit names (typically 1, 2, 3, etc...)
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (select b+c from t1) as x " +
                               "natural join (select b+c from t2) as y"),
                cross(new String[][] {{"5"}, {"11"}, {"17"}}, // b+c in t1
                      new String[][] {{"5"}, {"5"}, {"3"}})); // b+c in t2

        // Expressions with explicit names may be common columns, if the
        // names are equal
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (select b+c c1 from t1) as x " +
                               "natural join (select b+c c1 from t2) as y"),
                new String[][] {{"5"}, {"5"}});

        // Multiple JOIN operators
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural join t2 " +
                               "natural join t3"),
                new String[][] {{"4", "2", "3", "1", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (t1 natural join t2) " +
                               "natural join t3"),
                new String[][] {{"4", "2", "3", "1", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural join " +
                               "(t2 natural join t3)"),
                new String[][] {{"2", "3", "1", "4", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 natural join t2 cross join t3"),
                new String[][] {{"2", "3", "1", "4", "4", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 natural join t2 inner join t3 on 1=1"),
                new String[][] {{"2", "3", "1", "4", "4", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 cross join t2 natural join t3"),
                new String[][] {
                    {"4", "1", "2", "3", "3", "2", "100"},
                    {"4", "4", "5", "6", "3", "2", "100"},
                    {"4", "7", "8", "9", "3", "2", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 inner join t2 on 1=1 natural join t3"),
                new String[][] {
                    {"4", "1", "2", "3", "3", "2", "100"},
                    {"4", "4", "5", "6", "3", "2", "100"},
                    {"4", "7", "8", "9", "3", "2", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 inner join t2 natural join t3 on 1=1"),
                new String[][] {
                    {"1", "2", "3", "4", "3", "2", "100"},
                    {"4", "5", "6", "4", "3", "2", "100"},
                    {"7", "8", "9", "4", "3", "2", "100"}});

        // NATURAL JOIN in INSERT context
        s.execute("create table insert_src (c1 int, c2 int, c3 int, c4 int)");
        s.execute("insert into insert_src select * from t1 natural join t2");
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from insert_src"),
                new String[][] {{"2", "3", "1", "4"}});

        // Asterisked identifier chains (common columns should not be included)
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select t1.* from t1 natural join t2"),
                "1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select t2.* from t1 natural join t2"),
                "4");
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select t1.*, t2.* from t1 natural join t2"),
                new String[][] {{"1", "4"}});

        // NATURAL INNER JOIN (same as NATURAL JOIN because INNER is default)
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural inner join t2"),
                new String[][] {{"2", "3", "1", "4"}});

        // NATURAL LEFT (OUTER) JOIN
        String[][] ljRows = {
            {"2", "3", "1", "4"},
            {"5", "6", "4", null},
            {"8", "9", "7", null}
        };
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural left join t2"),
                ljRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural left outer join t2"),
                ljRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select b, t1.b, t2.b from t1 natural left join t2"),
                new String[][] {
                    {"2", "2", "2"},
                    {"5", "5", null},
                    {"8", "8", null}});

        // NATURAL RIGHT (OUTER) JOIN
        String[][] rjRows = {
            {"1", "2", null, "3"},
            {"2", "3",  "1", "4"},
            {"3", "2", null, "1"}
        };
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural right join t2"),
                rjRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural right outer join t2"),
                rjRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select b, t1.b, t2.b from t1 natural right join t2"),
                new String[][] {
                    {"1", null, "1"},
                    {"2",  "2", "2"},
                    {"3", null, "3"}});

        // ***** Negative tests *****

        // ON or USING clause not allowed with NATURAL
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 natural join t2 on t1.b=t2.b");
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 natural join t2 using (b)");

        // CROSS JOIN cannot be used together with NATURAL
        assertStatementError(
                SYNTAX_ERROR, s,
                "select * from t1 natural cross join t2");

        // T has one column named D, T2 CROSS JOIN T3 has two columns named D,
        // so it's not clear which columns to join on
        assertStatementError(
                AMBIGUOUS_COLNAME, s,
                "select * from t1 t(d,x,y) natural join (t2 cross join t3)");

        // Only common columns, so asterisked identifier chains expand to
        // zero columns
        assertStatementError(
                NO_COLUMNS, s,
                "select x.* from t1 x natural join t1 y");
        assertStatementError(
                NO_COLUMNS, s,
                "select y.* from t1 x natural join t1 y");
        assertStatementError(
                NO_COLUMNS, s,
                "select x.*, y.* from t1 x natural join t1 y");

        // Incompatible types
        assertStatementError(
            NON_COMPARABLE, s,
            "select * from t1 natural join (values ('one', 'two')) v1(a,b)");
    }

    /**
     * Test that ON clauses can contain subqueries (DERBY-4380).
     */
    public void testSubqueryInON() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t1(a int)");
        s.execute("insert into t1 values 1,2,3");
        s.execute("create table t2(b int)");
        s.execute("insert into t2 values 1,2");
        s.execute("create table t3(c int)");
        s.execute("insert into t3 values 2,3");

        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 join t2 on a = some (select c from t3)"),
            new String[][]{{"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 " +
                           "on a = b and b not in (select c from t3)"),
            new String[][]{{"1", "1"}, {"2", null}, {"3", null}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t3 join t2 on exists " +
                           "(select * from t2 join t1 on exists " +
                           "(select * from t3 where c = a))"),
            new String[][]{{"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}});

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select a from t1 join t2 " +
                           "on a = (select count(*) from t3) and a = b"),
            "2");

        // This query used to cause NullPointerException with early versions
        // of the DERBY-4380 patch.
        JDBC.assertEmpty(s.executeQuery(
            "select * from t1 join t2 on exists " +
            "(select * from t3 x left join t3 y on 1=0 where y.c=1)"));
    }


    /**
     * Test that computation of transitive closure of equi-join does not give
     * rise to eternal loop in a case where a predicate of type T1.x = T1.y is
     * added to the closure.
     * @throws SQLException
     */
    public void testDerby4387() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;


        s.executeUpdate("create table c (a int, b int, c int)");
        s.executeUpdate("create table cc (aa int)");

        // Compiling this query gave an infinite loop (would eventually run out
        // of memory though) before the fix:
        rs = s.executeQuery("select * from cc t1, c t2, cc t3 " +
                            "    where t3.aa = t2.a and " +
                            "          t3.aa = t2.b and " +
                            "          t3.aa = t2.c");

        // After the fix the correct joinClauses table should look like this
        // when done (see PredicateList#joinClauseTransitiveClosure variable
        // joinClauses), where EC is equivalence class assigned, and a *
        // denotes a predicate added by the closure computation.
        //
        // [0]: (t1)
        // [1]: (t2)
        //    [0]: 2.1 = 1.1 EC: 0     i.e.  t3.aa == t2.a
        //    [1]: 1.1 = 1.3 EC: 0           t2.a  == t2.c *
        //    [2]: 1.1 = 1.2 EC: 0           t2.a  == t2.b *
        //    [3]: 2.1 = 1.2 EC: 0           t3.aa == t2.b
        //    [4]: 2.1 = 1.3 EC: 0           t3.aa == t2.c
        // [2]: (t3)
        //    [0]: 2.1 = 1.1 EC: 0           t3.aa == t2.a
        //    [1]: 2.1 = 1.2 EC: 0           t3.aa == t2.b
        //    [2]: 2.1 = 1.3 EC: 0           t3.aa == t2.c
        //
        // Before the fix, the derived predicates (e.g. t2.a == t2.b) were
        // added twice and caused an infinite loop.

        rollback();
    }


    /**
     * Derby-4405 improve rewrite of OUTER JOIN to INNER JOIN in presence of
     * null intolerant predicate. It uses a slimmed down toursdb dataset.
     */
    public void testDerby_4405() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.execute("CREATE TABLE COUNTRIES(" +
                  "COUNTRY VARCHAR(26) UNIQUE NOT NULL," +
                  "COUNTRY_ISO_CODE CHAR(2) PRIMARY KEY NOT NULL)");


        s.execute("CREATE TABLE CITIES(" +
                  "CITY_ID INTEGER primary key NOT NULL ," +
                  "COUNTRY VARCHAR(26) NOT NULL," +
                  "AIRPORT VARCHAR(3))");


        s.execute("CREATE TABLE FLIGHTS(" +
                  "FLIGHT_ID CHAR(6) NOT NULL ," +
                  "ORIG_AIRPORT CHAR(3)," +
                  "PRIMARY KEY (FLIGHT_ID))");

        s.execute("CREATE INDEX ORIGINDEX ON FLIGHTS (ORIG_AIRPORT)");

        PreparedStatement ps = getConnection().prepareStatement(
            "insert into COUNTRIES values ( ?,? )");

        insertTourRow(ps, "Afghanistan", "AF");
        insertTourRow(ps, "Albania", "AL");
        insertTourRow(ps, "Algeria", "DZ");
        insertTourRow(ps, "American Samoa", "AS");
        insertTourRow(ps, "Angola", "AO");
        insertTourRow(ps, "Argentina", "AR");
        insertTourRow(ps, "Armenia", "AM");
        insertTourRow(ps, "Australia", "AU");
        insertTourRow(ps, "Austria", "AT");
        insertTourRow(ps, "Azerbaijan", "AZ");
        insertTourRow(ps, "Bahamas", "BS");
        insertTourRow(ps, "Bangladesh", "BD");
        insertTourRow(ps, "Barbados", "BB");
        insertTourRow(ps, "Belgium", "BE");
        insertTourRow(ps, "Belize", "BZ");
        insertTourRow(ps, "Bermuda", "BM");
        insertTourRow(ps, "Bolivia", "BO");
        insertTourRow(ps, "Botswana", "BW");
        insertTourRow(ps, "Brazil", "BR");
        insertTourRow(ps, "Bulgaria", "BG");
        insertTourRow(ps, "Cambodia", "KH");
        insertTourRow(ps, "Cameroon", "CM");
        insertTourRow(ps, "Canada", "CA");
        insertTourRow(ps, "Cape Verde", "CV");
        insertTourRow(ps, "Chile", "CL");
        insertTourRow(ps, "China", "CN");
        insertTourRow(ps, "Colombia", "CO");
        insertTourRow(ps, "Congo", "CG");
        insertTourRow(ps, "Costa Rica", "CR");
        insertTourRow(ps, "Cote d'Ivoire", "CI");
        insertTourRow(ps, "Cuba", "CU");
        insertTourRow(ps, "Czech Republic", "CZ");
        insertTourRow(ps, "Denmark", "DK");
        insertTourRow(ps, "Dominical Republic", "DO");
        insertTourRow(ps, "Ecuador", "EC");
        insertTourRow(ps, "Egypt", "EG");
        insertTourRow(ps, "El Salvador", "SV");
        insertTourRow(ps, "Ethiopia", "ET");
        insertTourRow(ps, "Falkland Islands", "FK");
        insertTourRow(ps, "Fiji", "FJ");
        insertTourRow(ps, "Finland", "FI");
        insertTourRow(ps, "France", "FR");
        insertTourRow(ps, "Georgia", "GE");
        insertTourRow(ps, "Germany", "DE");
        insertTourRow(ps, "Ghana", "GH");
        insertTourRow(ps, "Greece", "GR");
        insertTourRow(ps, "Guadeloupe", "GP");
        insertTourRow(ps, "Guatemala", "GT");
        insertTourRow(ps, "Honduras", "HN");
        insertTourRow(ps, "Hungary", "HU");
        insertTourRow(ps, "Iceland", "IS");
        insertTourRow(ps, "India", "IN");
        insertTourRow(ps, "Indonesia", "ID");
        insertTourRow(ps, "Iran", "IR");
        insertTourRow(ps, "Iraq", "IQ");
        insertTourRow(ps, "Ireland", "IE");
        insertTourRow(ps, "Israel", "IL");
        insertTourRow(ps, "Italy", "IT");
        insertTourRow(ps, "Jamaica", "JM");
        insertTourRow(ps, "Japan", "JP");
        insertTourRow(ps, "Jordan", "JO");
        insertTourRow(ps, "Kenya", "KE");
        insertTourRow(ps, "Lebanon", "LB");
        insertTourRow(ps, "Lithuania", "LT");
        insertTourRow(ps, "Madagascar", "MG");
        insertTourRow(ps, "Malaysia", "MY");
        insertTourRow(ps, "Mali", "ML");
        insertTourRow(ps, "Mexico", "MX");
        insertTourRow(ps, "Morocco", "MA");
        insertTourRow(ps, "Mozambique", "MZ");
        insertTourRow(ps, "Nepal", "NP");
        insertTourRow(ps, "Netherlands", "NL");
        insertTourRow(ps, "New Zealand", "NZ");
        insertTourRow(ps, "Nicaragua", "NI");
        insertTourRow(ps, "Nigeria", "NG");
        insertTourRow(ps, "Norway", "NO");
        insertTourRow(ps, "Pakistan", "PK");
        insertTourRow(ps, "Paraguay", "PY");
        insertTourRow(ps, "Peru", "PE");
        insertTourRow(ps, "Philippines", "PH");
        insertTourRow(ps, "Poland", "PL");
        insertTourRow(ps, "Portugal", "PT");
        insertTourRow(ps, "Russia", "RU");
        insertTourRow(ps, "Samoa", "WS");
        insertTourRow(ps, "Senegal", "SN");
        insertTourRow(ps, "Sierra Leone", "SL");
        insertTourRow(ps, "Singapore", "SG");
        insertTourRow(ps, "Slovakia", "SK");
        insertTourRow(ps, "South Africa", "ZA");
        insertTourRow(ps, "Spain", "ES");
        insertTourRow(ps, "Sri Lanka", "LK");
        insertTourRow(ps, "Sudan", "SD");
        insertTourRow(ps, "Sweden", "SE");
        insertTourRow(ps, "Switzerland", "CH");
        insertTourRow(ps, "Syrian Arab Republic", "SY");
        insertTourRow(ps, "Tajikistan", "TJ");
        insertTourRow(ps, "Tanzania", "TZ");
        insertTourRow(ps, "Thailand", "TH");
        insertTourRow(ps, "Trinidad and Tobago", "TT");
        insertTourRow(ps, "Tunisia", "TN");
        insertTourRow(ps, "Turkey", "TR");
        insertTourRow(ps, "Ukraine", "UA");
        insertTourRow(ps, "United Kingdom", "GB");
        insertTourRow(ps, "United States", "US");
        insertTourRow(ps, "Uruguay", "UY");
        insertTourRow(ps, "Uzbekistan", "UZ");
        insertTourRow(ps, "Venezuela", "VE");
        insertTourRow(ps, "Viet Nam", "VN");
        insertTourRow(ps, "Virgin Islands (British)", "VG");
        insertTourRow(ps, "Virgin Islands (U.S.)", "VI");
        insertTourRow(ps, "Yugoslavia", "YU");
        insertTourRow(ps, "Zaire", "ZR");
        insertTourRow(ps, "Zimbabwe", "ZW");

        ps = getConnection().prepareStatement(
            "insert into CITIES VALUES (?,?,?)");

        insertTourRow(ps, 1, "Netherlands", "AMS");
        insertTourRow(ps, 2, "Greece", "ATH");
        insertTourRow(ps, 3, "New Zealand", "AKL");
        insertTourRow(ps, 4, "Lebanon", "BEY");
        insertTourRow(ps, 5, "Colombia", "BOG");
        insertTourRow(ps, 6, "India", "BOM");
        insertTourRow(ps, 7, "Hungary", "BUD");
        insertTourRow(ps, 8, "Argentina", "BUE");
        insertTourRow(ps, 9, "Egypt", "CAI");
        insertTourRow(ps, 10, "India", "CCU");
        insertTourRow(ps, 11, "South Africa", "CPT");
        insertTourRow(ps, 12, "Venezuela", "CCS");
        insertTourRow(ps, 13, "Morocco", "CAS");
        insertTourRow(ps, 14, "Denmark", "CPH");
        insertTourRow(ps, 15, "Ireland", "DUB");
        insertTourRow(ps, 16, "Switzerland", "GVA");
        insertTourRow(ps, 17, "China", "HKG");
        insertTourRow(ps, 18, "Turkey", "IST");
        insertTourRow(ps, 19, "Indonesia", "JKT");
        insertTourRow(ps, 20, "Afghanistan", "KBL");
        insertTourRow(ps, 21, "Pakistan", "KHI");
        insertTourRow(ps, 22, "Nigeria", "LOS");
        insertTourRow(ps, 23, "Peru", "LIM");
        insertTourRow(ps, 24, "Portugal", "LIS");
        insertTourRow(ps, 25, "United Kingdom", "LHR");
        insertTourRow(ps, 26, "Spain", "MAD");
        insertTourRow(ps, 27, "Philippines", "MNL");
        insertTourRow(ps, 28, "Australia", "MEL");
        insertTourRow(ps, 29, "Mexico", "MEX");
        insertTourRow(ps, 30, "Canada", "YUL");
        insertTourRow(ps, 31, "Russia", "SVO");
        insertTourRow(ps, 32, "Kenya", "NBO");
        insertTourRow(ps, 33, "Japan", "OSA");
        insertTourRow(ps, 34, "Norway", "OSL");
        insertTourRow(ps, 35, "France", "CDG");
        insertTourRow(ps, 36, "Czech Republic", "PRG");
        insertTourRow(ps, 37, "Iceland", "REY");
        insertTourRow(ps, 38, "Brazil", "GIG");
        insertTourRow(ps, 39, "Italy", "FCO");
        insertTourRow(ps, 40, "Chile", "SCL");
        insertTourRow(ps, 41, "Brazil", "GRU");
        insertTourRow(ps, 43, "China", "SHA");
        insertTourRow(ps, 44, "Singapore", "SIN");
        insertTourRow(ps, 45, "Sweden", "ARN");
        insertTourRow(ps, 46, "Australia", "SYD");
        insertTourRow(ps, 47, "United States", "SJC");
        insertTourRow(ps, 48, "Iran", "THR");
        insertTourRow(ps, 49, "Japan", "NRT");
        insertTourRow(ps, 50, "Canada", "YYZ");
        insertTourRow(ps, 51, "Poland", "WAW");
        insertTourRow(ps, 52, "United States", "ALB");
        insertTourRow(ps, 53, "United States", "ABQ");
        insertTourRow(ps, 54, "United States", "ATL");
        insertTourRow(ps, 55, "United States", "BOI");
        insertTourRow(ps, 56, "United States", "BOS");
        insertTourRow(ps, 57, "United States", "CHS");
        insertTourRow(ps, 58, "United States", "MDW");
        insertTourRow(ps, 59, "United States", "CLE");
        insertTourRow(ps, 60, "United States", "DFW");
        insertTourRow(ps, 61, "United States", "DEN");
        insertTourRow(ps, 62, "United States", "DSM");
        insertTourRow(ps, 63, "United States", "FAI");
        insertTourRow(ps, 64, "United States", "HLN");
        insertTourRow(ps, 65, "United States", "HNL");
        insertTourRow(ps, 66, "United States", "HOU");
        insertTourRow(ps, 67, "United States", "JNU");
        insertTourRow(ps, 68, "United States", "MCI");
        insertTourRow(ps, 69, "United States", "LAX");
        insertTourRow(ps, 70, "United States", "MEM");
        insertTourRow(ps, 71, "United States", "MIA");
        insertTourRow(ps, 72, "United States", "MKE");
        insertTourRow(ps, 73, "United States", "MSP");
        insertTourRow(ps, 74, "United States", "BNA");
        insertTourRow(ps, 75, "United States", "MSY");
        insertTourRow(ps, 76, "United States", "JFK");
        insertTourRow(ps, 77, "United States", "OKC");
        insertTourRow(ps, 78, "United States", "PHL");
        insertTourRow(ps, 79, "United States", "PHX");
        insertTourRow(ps, 80, "United States", "STL");
        insertTourRow(ps, 81, "United States", "SLC");
        insertTourRow(ps, 82, "United States", "SAT");
        insertTourRow(ps, 83, "United States", "SAN");
        insertTourRow(ps, 84, "United States", "SFO");
        insertTourRow(ps, 85, "United States", "SJU");
        insertTourRow(ps, 86, "United States", "SEA");
        insertTourRow(ps, 87, "United States", "IAD");

        ps = getConnection().prepareStatement(
            "insert into FLIGHTS values (?,?)");

        insertTourRow(ps, "AA1111", "ABQ");
        insertTourRow(ps, "AA1112", "LAX");
        insertTourRow(ps, "AA1113", "ABQ");
        insertTourRow(ps, "AA1114", "PHX");
        insertTourRow(ps, "AA1115", "ABQ");
        insertTourRow(ps, "AA1116", "OKC");
        insertTourRow(ps, "AA1117", "AKL");
        insertTourRow(ps, "AA1118", "HNL");
        insertTourRow(ps, "AA1119", "AKL");
        insertTourRow(ps, "AA1120", "NRT");
        insertTourRow(ps, "AA1121", "AKL");
        insertTourRow(ps, "AA1122", "SYD");
        insertTourRow(ps, "AA1123", "ALB");
        insertTourRow(ps, "AA1124", "JFK");
        insertTourRow(ps, "AA1125", "ALB");
        insertTourRow(ps, "AA1126", "BOS");
        insertTourRow(ps, "AA1127", "ALB");
        insertTourRow(ps, "AA1128", "IAD");
        insertTourRow(ps, "US1517", "AMS");
        insertTourRow(ps, "US1516", "JFK");
        insertTourRow(ps, "AA1131", "AMS");
        insertTourRow(ps, "AA1132", "ATH");
        insertTourRow(ps, "AA1133", "AMS");
        insertTourRow(ps, "AA1134", "CDG");
        insertTourRow(ps, "AA1135", "ARN");
        insertTourRow(ps, "AA1136", "BOS");
        insertTourRow(ps, "AA1137", "ARN");
        insertTourRow(ps, "AA1138", "SVO");
        insertTourRow(ps, "AA1139", "ARN");
        insertTourRow(ps, "AA1140", "CPH");
        insertTourRow(ps, "AA1141", "ATH");
        insertTourRow(ps, "AA1142", "LHR");
        insertTourRow(ps, "AA1143", "ATH");
        insertTourRow(ps, "AA1144", "CAI");
        insertTourRow(ps, "AA1145", "ATH");
        insertTourRow(ps, "AA1146", "CDG");
        insertTourRow(ps, "AA1147", "ATL");
        insertTourRow(ps, "AA1148", "LAX");
        insertTourRow(ps, "AA1149", "ATL");
        insertTourRow(ps, "AA1150", "DFW");
        insertTourRow(ps, "AA1151", "ATL");
        insertTourRow(ps, "AA1152", "SEA");
        insertTourRow(ps, "AA1153", "BEY");
        insertTourRow(ps, "AA1154", "CAI");
        insertTourRow(ps, "AA1270", "BEY");
        insertTourRow(ps, "AA1269", "MAD");
        insertTourRow(ps, "AA1157", "BEY");
        insertTourRow(ps, "AA1158", "BOM");
        insertTourRow(ps, "AA1159", "BNA");
        insertTourRow(ps, "AA1160", "MIA");
        insertTourRow(ps, "AA1161", "BNA");
        insertTourRow(ps, "AA1162", "JFK");
        insertTourRow(ps, "AA1163", "BNA");
        insertTourRow(ps, "AA1164", "GIG");
        insertTourRow(ps, "US1591", "BOG");
        insertTourRow(ps, "AA1190", "MIA");
        insertTourRow(ps, "AA1167", "BOG");
        insertTourRow(ps, "AA1168", "LIM");
        insertTourRow(ps, "AA1169", "BOG");
        insertTourRow(ps, "AA1170", "GIG");
        insertTourRow(ps, "AA1171", "BOI");
        insertTourRow(ps, "AA1172", "SEA");
        insertTourRow(ps, "AA1173", "BOI");
        insertTourRow(ps, "AA1174", "DSM");
        insertTourRow(ps, "AA1175", "BOI");
        insertTourRow(ps, "AA1176", "HLN");
        insertTourRow(ps, "AA1177", "BOM");
        insertTourRow(ps, "AA1178", "CCU");
        insertTourRow(ps, "AA1179", "BOM");
        insertTourRow(ps, "AA1180", "KHI");
        insertTourRow(ps, "AA1181", "BOM");
        insertTourRow(ps, "AA1182", "HKG");
        insertTourRow(ps, "AA1183", "BOS");
        insertTourRow(ps, "AA1184", "SFO");
        insertTourRow(ps, "AA1185", "BOS");
        insertTourRow(ps, "AA1186", "MIA");
        insertTourRow(ps, "AA1187", "BOS");
        insertTourRow(ps, "AA1188", "IAD");
        insertTourRow(ps, "AA1189", "BUD");
        insertTourRow(ps, "AA1191", "BUD");
        insertTourRow(ps, "AA1192", "SVO");
        insertTourRow(ps, "AA1193", "BUD");
        insertTourRow(ps, "AA1194", "FCO");
        insertTourRow(ps, "AA1195", "CAI");
        insertTourRow(ps, "AA1196", "MIA");
        insertTourRow(ps, "AA1197", "CAI");
        insertTourRow(ps, "AA1198", "IST");
        insertTourRow(ps, "AA1199", "CAI");
        insertTourRow(ps, "AA1200", "GIG");
        insertTourRow(ps, "AA1201", "CAS");
        insertTourRow(ps, "AA1202", "KHI");
        insertTourRow(ps, "AA1203", "CAS");
        insertTourRow(ps, "AA1204", "LOS");
        insertTourRow(ps, "AA1205", "CAS");
        insertTourRow(ps, "AA1206", "MAD");
        insertTourRow(ps, "AA1207", "CCS");
        insertTourRow(ps, "AA1208", "SCL");
        insertTourRow(ps, "AA1209", "CCS");
        insertTourRow(ps, "AA1210", "MEX");
        insertTourRow(ps, "AA1211", "CCS");
        insertTourRow(ps, "AA1212", "BUE");
        insertTourRow(ps, "AA1213", "CCU");
        insertTourRow(ps, "AA1214", "HKG");
        insertTourRow(ps, "AA1215", "CCU");
        insertTourRow(ps, "AA1216", "NRT");
        insertTourRow(ps, "AA1217", "CCU");
        insertTourRow(ps, "AA1218", "SIN");
        insertTourRow(ps, "AA1219", "CDG");
        insertTourRow(ps, "AA1220", "LHR");
        insertTourRow(ps, "AA1221", "CDG");
        insertTourRow(ps, "AA1222", "JFK");
        insertTourRow(ps, "AA1223", "CDG");
        insertTourRow(ps, "AA1224", "SVO");
        insertTourRow(ps, "AA1225", "CHS");
        insertTourRow(ps, "AA1226", "ATL");
        insertTourRow(ps, "AA1227", "CHS");
        insertTourRow(ps, "AA1228", "MCI");
        insertTourRow(ps, "AA1229", "CHS");
        insertTourRow(ps, "AA1230", "MSY");
        insertTourRow(ps, "AA1231", "CLE");
        insertTourRow(ps, "AA1232", "LAX");
        insertTourRow(ps, "AA1233", "CLE");
        insertTourRow(ps, "AA1234", "DFW");
        insertTourRow(ps, "AA1235", "CLE");
        insertTourRow(ps, "AA1236", "MDW");
        insertTourRow(ps, "AA1237", "CPH");
        insertTourRow(ps, "AA1238", "FCO");
        insertTourRow(ps, "AA1239", "CPH");
        insertTourRow(ps, "AA1240", "REY");
        insertTourRow(ps, "AA1241", "CPH");
        insertTourRow(ps, "AA1242", "CDG");
        insertTourRow(ps, "AA1243", "CPT");
        insertTourRow(ps, "AA1244", "LOS");
        insertTourRow(ps, "AA1245", "CPT");
        insertTourRow(ps, "AA1246", "NBO");
        insertTourRow(ps, "AA1247", "CPT");
        insertTourRow(ps, "AA1248", "LHR");
        insertTourRow(ps, "AA1249", "DEN");
        insertTourRow(ps, "AA1250", "SEA");
        insertTourRow(ps, "AA1251", "DEN");
        insertTourRow(ps, "AA1252", "BOI");
        insertTourRow(ps, "AA1253", "DEN");
        insertTourRow(ps, "AA1254", "JFK");
        insertTourRow(ps, "AA1255", "DFW");
        insertTourRow(ps, "AA1256", "SAT");
        insertTourRow(ps, "AA1257", "DFW");
        insertTourRow(ps, "AA1258", "ATL");
        insertTourRow(ps, "AA1259", "DFW");
        insertTourRow(ps, "AA1260", "MIA");
        insertTourRow(ps, "AA1261", "DSM");
        insertTourRow(ps, "AA1262", "MDW");
        insertTourRow(ps, "AA1263", "DSM");
        insertTourRow(ps, "AA1264", "SLC");
        insertTourRow(ps, "AA1265", "DSM");
        insertTourRow(ps, "AA1266", "OKC");
        insertTourRow(ps, "AA1267", "DUB");
        insertTourRow(ps, "AA1268", "LHR");
        insertTourRow(ps, "AA1272", "CDG");
        insertTourRow(ps, "AA1273", "BUE");
        insertTourRow(ps, "AA1274", "SCL");
        insertTourRow(ps, "AA1275", "BUE");
        insertTourRow(ps, "AA1276", "GRU");
        insertTourRow(ps, "US1509", "BUE");
        insertTourRow(ps, "US1508", "MIA");
        insertTourRow(ps, "AA1279", "FAI");
        insertTourRow(ps, "AA1280", "JNU");
        insertTourRow(ps, "AA1281", "FAI");
        insertTourRow(ps, "AA1282", "SEA");
        insertTourRow(ps, "US1443", "FAI");
        insertTourRow(ps, "US1444", "NRT");
        insertTourRow(ps, "AA1285", "FCO");
        insertTourRow(ps, "AA1286", "CDG");
        insertTourRow(ps, "AA1287", "FCO");
        insertTourRow(ps, "AA1288", "CAI");
        insertTourRow(ps, "AA1289", "FCO");
        insertTourRow(ps, "AA1290", "JFK");
        insertTourRow(ps, "AA1291", "GIG");
        insertTourRow(ps, "AA1292", "MIA");
        insertTourRow(ps, "AA1293", "GIG");
        insertTourRow(ps, "AA1294", "LIM");
        insertTourRow(ps, "AA1295", "GIG");
        insertTourRow(ps, "AA1296", "BUE");
        insertTourRow(ps, "US1249", "GRU");
        insertTourRow(ps, "US1250", "CCS");
        insertTourRow(ps, "US1251", "GRU");
        insertTourRow(ps, "US1252", "JFK");
        insertTourRow(ps, "US1253", "GRU");
        insertTourRow(ps, "US1254", "LAX");
        insertTourRow(ps, "AA1053", "GRU");
        insertTourRow(ps, "AA1054", "LIM");
        insertTourRow(ps, "US1255", "GVA");
        insertTourRow(ps, "US1256", "CPH");
        insertTourRow(ps, "US1257", "GVA");
        insertTourRow(ps, "US1258", "LIS");
        insertTourRow(ps, "US1259", "GVA");
        insertTourRow(ps, "US1260", "OSL");
        insertTourRow(ps, "US1266", "HKG");
        insertTourRow(ps, "US1264", "SIN");
        insertTourRow(ps, "US1267", "HLN");
        insertTourRow(ps, "US1268", "SEA");
        insertTourRow(ps, "US1269", "HLN");
        insertTourRow(ps, "US1270", "BOI");
        insertTourRow(ps, "US1271", "HLN");
        insertTourRow(ps, "US1272", "DEN");
        insertTourRow(ps, "US1276", "HNL");
        insertTourRow(ps, "US1274", "NRT");
        insertTourRow(ps, "US1277", "HNL");
        insertTourRow(ps, "US1278", "SYD");
        insertTourRow(ps, "US1281", "HOU");
        insertTourRow(ps, "US1282", "SAT");
        insertTourRow(ps, "US1283", "HOU");
        insertTourRow(ps, "US1284", "IAD");
        insertTourRow(ps, "US1285", "IAD");
        insertTourRow(ps, "US1286", "BOS");
        insertTourRow(ps, "US1287", "IAD");
        insertTourRow(ps, "US1288", "MSP");
        insertTourRow(ps, "US1289", "IAD");
        insertTourRow(ps, "US1290", "MIA");
        insertTourRow(ps, "US1291", "IST");
        insertTourRow(ps, "US1292", "THR");
        insertTourRow(ps, "US1293", "IST");
        insertTourRow(ps, "US1294", "FCO");
        insertTourRow(ps, "US1295", "IST");
        insertTourRow(ps, "US1296", "ATH");
        insertTourRow(ps, "US1381", "JFK");
        insertTourRow(ps, "US1382", "CDG");
        insertTourRow(ps, "US1349", "JFK");
        insertTourRow(ps, "US1300", "LAX");
        insertTourRow(ps, "US1301", "JFK");
        insertTourRow(ps, "US1302", "GRU");
        insertTourRow(ps, "US1303", "JKT");
        insertTourRow(ps, "US1304", "HKG");
        insertTourRow(ps, "US1308", "JKT");
        insertTourRow(ps, "US1307", "SYD");
        insertTourRow(ps, "US1309", "JNU");
        insertTourRow(ps, "US1310", "SEA");
        insertTourRow(ps, "US1311", "JNU");
        insertTourRow(ps, "US1312", "SFO");
        insertTourRow(ps, "US1313", "JNU");
        insertTourRow(ps, "US1314", "HNL");
        insertTourRow(ps, "US1315", "KBL");
        insertTourRow(ps, "US1316", "KHI");
        insertTourRow(ps, "US1317", "KBL");
        insertTourRow(ps, "US1318", "IST");
        insertTourRow(ps, "US1321", "KHI");
        insertTourRow(ps, "US1322", "IST");
        insertTourRow(ps, "US1323", "KHI");
        insertTourRow(ps, "US1324", "IST");
        insertTourRow(ps, "US1325", "KHI");
        insertTourRow(ps, "US1326", "THR");
        insertTourRow(ps, "US1327", "LAX");
        insertTourRow(ps, "US1328", "HNL");
        insertTourRow(ps, "US1329", "LAX");
        insertTourRow(ps, "US1330", "GRU");
        insertTourRow(ps, "US1331", "LAX");
        insertTourRow(ps, "US1332", "NRT");
        insertTourRow(ps, "US1333", "LHR");
        insertTourRow(ps, "US1334", "WAW");
        insertTourRow(ps, "US1335", "LHR");
        insertTourRow(ps, "US1336", "YYZ");
        insertTourRow(ps, "US1337", "LHR");
        insertTourRow(ps, "US1338", "NBO");
        insertTourRow(ps, "US1501", "LIM");
        insertTourRow(ps, "US1340", "MIA");
        insertTourRow(ps, "US1344", "LIM");
        insertTourRow(ps, "US1342", "BUE");
        insertTourRow(ps, "US1345", "LIS");
        insertTourRow(ps, "US1346", "CDG");
        insertTourRow(ps, "US1347", "LIS");
        insertTourRow(ps, "US1348", "CAS");
        insertTourRow(ps, "US1353", "LOS");
        insertTourRow(ps, "US1354", "MAD");
        insertTourRow(ps, "US1355", "LOS");
        insertTourRow(ps, "US1356", "ATH");
        insertTourRow(ps, "US1357", "MAD");
        insertTourRow(ps, "US1358", "CDG");
        insertTourRow(ps, "US1361", "MAD");
        insertTourRow(ps, "US1362", "JFK");
        insertTourRow(ps, "US1363", "MCI");
        insertTourRow(ps, "US1364", "LAX");
        insertTourRow(ps, "US1365", "MCI");
        insertTourRow(ps, "US1366", "DFW");
        insertTourRow(ps, "US1367", "MCI");
        insertTourRow(ps, "US1368", "JFK");
        insertTourRow(ps, "US1379", "MDW");
        insertTourRow(ps, "US1380", "LAX");
        insertTourRow(ps, "US1473", "MDW");
        insertTourRow(ps, "US1474", "JFK");
        insertTourRow(ps, "US1383", "MDW");
        insertTourRow(ps, "US1384", "ATL");
        insertTourRow(ps, "US1385", "MEL");
        insertTourRow(ps, "US1386", "SYD");
        insertTourRow(ps, "US1387", "MEL");
        insertTourRow(ps, "US1388", "SIN");
        insertTourRow(ps, "US1389", "MEL");
        insertTourRow(ps, "US1390", "HNL");
        insertTourRow(ps, "US1391", "MEM");
        insertTourRow(ps, "US1392", "MIA");
        insertTourRow(ps, "US1393", "MEM");
        insertTourRow(ps, "US1394", "JFK");
        insertTourRow(ps, "US1395", "MEM");
        insertTourRow(ps, "US1396", "LAX");
        insertTourRow(ps, "US1397", "MEX");
        insertTourRow(ps, "US1398", "SFO");
        insertTourRow(ps, "US1399", "MEX");
        insertTourRow(ps, "US1400", "LAX");
        insertTourRow(ps, "US1401", "MEX");
        insertTourRow(ps, "US1402", "BOG");
        insertTourRow(ps, "US1403", "MIA");
        insertTourRow(ps, "US1404", "GRU");
        insertTourRow(ps, "US1405", "MIA");
        insertTourRow(ps, "US1406", "LAX");
        insertTourRow(ps, "US1407", "MIA");
        insertTourRow(ps, "US1408", "JFK");
        insertTourRow(ps, "US1409", "MKE");
        insertTourRow(ps, "US1410", "JFK");
        insertTourRow(ps, "US1411", "MKE");
        insertTourRow(ps, "US1412", "MDW");
        insertTourRow(ps, "US1413", "MKE");
        insertTourRow(ps, "US1414", "JFK");
        insertTourRow(ps, "US1415", "MNL");
        insertTourRow(ps, "US1416", "SYD");
        insertTourRow(ps, "US1417", "MNL");
        insertTourRow(ps, "US1418", "TPE");
        insertTourRow(ps, "US1419", "MNL");
        insertTourRow(ps, "US1420", "SIN");
        insertTourRow(ps, "AA1419", "MNL");
        insertTourRow(ps, "AA1420", "HKG");
        insertTourRow(ps, "AA1421", "MNL");
        insertTourRow(ps, "US1422", "HNL");
        insertTourRow(ps, "US1423", "MSP");
        insertTourRow(ps, "US1424", "MDW");
        insertTourRow(ps, "AA1423", "MDW");
        insertTourRow(ps, "AA1424", "MIA");
        insertTourRow(ps, "US1427", "MSY");
        insertTourRow(ps, "US1428", "SFO");
        insertTourRow(ps, "US1429", "MSY");
        insertTourRow(ps, "US1430", "ATL");
        insertTourRow(ps, "US1431", "MSY");
        insertTourRow(ps, "US1432", "JFK");
        insertTourRow(ps, "US1433", "NBO");
        insertTourRow(ps, "US1434", "FCO");
        insertTourRow(ps, "US1435", "NBO");
        insertTourRow(ps, "US1436", "MAD");
        insertTourRow(ps, "US1437", "NBO");
        insertTourRow(ps, "US1438", "CAS");
        insertTourRow(ps, "US1439", "NRT");
        insertTourRow(ps, "US1440", "SYD");
        insertTourRow(ps, "US1441", "NRT");
        insertTourRow(ps, "US1442", "LAX");
        insertTourRow(ps, "US1445", "OKC");
        insertTourRow(ps, "US1446", "SLC");
        insertTourRow(ps, "US1447", "OKC");
        insertTourRow(ps, "US1448", "JFK");
        insertTourRow(ps, "US1449", "OKC");
        insertTourRow(ps, "US1450", "LAX");
        insertTourRow(ps, "US1451", "OSA");
        insertTourRow(ps, "US1452", "NRT");
        insertTourRow(ps, "US1453", "OSA");
        insertTourRow(ps, "US1454", "TPE");
        insertTourRow(ps, "US1455", "OSA");
        insertTourRow(ps, "US1456", "SVO");
        insertTourRow(ps, "US1457", "OSL");
        insertTourRow(ps, "US1458", "PRG");
        insertTourRow(ps, "US1459", "OSL");
        insertTourRow(ps, "US1460", "ARN");
        insertTourRow(ps, "US1461", "OSL");
        insertTourRow(ps, "US1462", "WAW");
        insertTourRow(ps, "AA1462", "OSL");
        insertTourRow(ps, "AA1463", "CDG");
        insertTourRow(ps, "US1463", "PHL");
        insertTourRow(ps, "US1464", "IAD");
        insertTourRow(ps, "US1465", "PHL");
        insertTourRow(ps, "US1466", "MIA");
        insertTourRow(ps, "US1469", "PHX");
        insertTourRow(ps, "US1470", "LAX");
        insertTourRow(ps, "US1471", "PHX");
        insertTourRow(ps, "US1472", "SEA");
        insertTourRow(ps, "US1475", "PRG");
        insertTourRow(ps, "US1476", "CDG");
        insertTourRow(ps, "US1477", "PRG");
        insertTourRow(ps, "US1478", "FCO");
        insertTourRow(ps, "US1479", "PRG");
        insertTourRow(ps, "US1480", "REY");
        insertTourRow(ps, "US1481", "REY");
        insertTourRow(ps, "US1482", "SVO");
        insertTourRow(ps, "US1483", "REY");
        insertTourRow(ps, "US1484", "CDG");
        insertTourRow(ps, "US1485", "REY");
        insertTourRow(ps, "US1486", "DUB");
        insertTourRow(ps, "US1487", "SAN");
        insertTourRow(ps, "US1488", "SFO");
        insertTourRow(ps, "US1489", "SAN");
        insertTourRow(ps, "US1490", "DFW");
        insertTourRow(ps, "US1491", "SAN");
        insertTourRow(ps, "US1492", "MEX");
        insertTourRow(ps, "US1493", "SAT");
        insertTourRow(ps, "US1494", "ATL");
        insertTourRow(ps, "US1495", "SAT");
        insertTourRow(ps, "US1496", "LAX");
        insertTourRow(ps, "US1497", "SAT");
        insertTourRow(ps, "US1498", "MIA");
        insertTourRow(ps, "US1499", "SCL");
        insertTourRow(ps, "US1500", "GRU");
        insertTourRow(ps, "US1503", "SCL");
        insertTourRow(ps, "US1504", "BUE");
        insertTourRow(ps, "US1505", "SEA");
        insertTourRow(ps, "AA1505", "SFO");
        insertTourRow(ps, "US1506", "SEA");
        insertTourRow(ps, "US1507", "JFK");
        insertTourRow(ps, "US1510", "SEL");
        insertTourRow(ps, "US1511", "NRT");
        insertTourRow(ps, "US1514", "SEL");
        insertTourRow(ps, "US1515", "SHA");
        insertTourRow(ps, "US1518", "SFO");
        insertTourRow(ps, "US1519", "SCL");
        insertTourRow(ps, "US1529", "SFO");
        insertTourRow(ps, "US1521", "HNL");
        insertTourRow(ps, "US1522", "SHA");
        insertTourRow(ps, "US1523", "SIN");
        insertTourRow(ps, "US1524", "SHA");
        insertTourRow(ps, "US1525", "HKG");
        insertTourRow(ps, "US1526", "SHA");
        insertTourRow(ps, "US1527", "SVO");
        insertTourRow(ps, "AA1528", "SIN");
        insertTourRow(ps, "AA1529", "SYD");
        insertTourRow(ps, "AA1532", "SIN");
        insertTourRow(ps, "AA1533", "HKG");
        insertTourRow(ps, "US1536", "SJU");
        insertTourRow(ps, "US1537", "CCS");
        insertTourRow(ps, "US1538", "SJU");
        insertTourRow(ps, "US1539", "MEL");
        insertTourRow(ps, "US1540", "SLC");
        insertTourRow(ps, "US1541", "DEN");
        insertTourRow(ps, "US1542", "SLC");
        insertTourRow(ps, "US1543", "SFO");
        insertTourRow(ps, "US1544", "SLC");
        insertTourRow(ps, "US1545", "MDW");
        insertTourRow(ps, "US1546", "STL");
        insertTourRow(ps, "US1547", "MDW");
        insertTourRow(ps, "US1548", "STL");
        insertTourRow(ps, "US1549", "JFK");
        insertTourRow(ps, "US1550", "STL");
        insertTourRow(ps, "US1551", "LAX");
        insertTourRow(ps, "US1552", "SVO");
        insertTourRow(ps, "US1553", "CDG");
        insertTourRow(ps, "US1554", "SVO");
        insertTourRow(ps, "US1555", "NRT");
        insertTourRow(ps, "US1558", "SYD");
        insertTourRow(ps, "US1559", "AKL");
        insertTourRow(ps, "US1560", "SYD");
        insertTourRow(ps, "US1561", "HNL");
        insertTourRow(ps, "US1562", "SYD");
        insertTourRow(ps, "US1563", "HKG");
        insertTourRow(ps, "US1564", "THR");
        insertTourRow(ps, "US1565", "KBL");
        insertTourRow(ps, "US1566", "THR");
        insertTourRow(ps, "US1567", "KHI");
        insertTourRow(ps, "US1568", "THR");
        insertTourRow(ps, "US1569", "CAI");
        insertTourRow(ps, "US1572", "TPE");
        insertTourRow(ps, "US1573", "SYD");
        insertTourRow(ps, "US1574", "TPE");
        insertTourRow(ps, "US1575", "OSA");
        insertTourRow(ps, "US1576", "WAW");
        insertTourRow(ps, "US1577", "PRG");
        insertTourRow(ps, "US1578", "WAW");
        insertTourRow(ps, "US1579", "SVO");
        insertTourRow(ps, "US1580", "WAW");
        insertTourRow(ps, "US1581", "ARN");
        insertTourRow(ps, "US1584", "YUL");
        insertTourRow(ps, "US1585", "JFK");
        insertTourRow(ps, "US1586", "YUL");
        insertTourRow(ps, "US1587", "SFO");
        insertTourRow(ps, "US1588", "YYZ");
        insertTourRow(ps, "US1589", "SEA");
        insertTourRow(ps, "US1590", "YYZ");
        insertTourRow(ps, "US1592", "YYZ");
        insertTourRow(ps, "US1593", "LHR");
        insertTourRow(ps, "AA1600", "SFO");
        insertTourRow(ps, "AA1601", "LAX");
        insertTourRow(ps, "AA1602", "SFO");
        insertTourRow(ps, "AA1603", "LAX");
        insertTourRow(ps, "US1600", "YYZ");
        insertTourRow(ps, "US1601", "SCL");


        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        ResultSet rs;
        RuntimeStatisticsParser rtsp;

        // 0. Only the variant with the subquery did the re-write of outer to
        // inner join prior to this fix. When the re-write is performed, the
        // optimizer chooses a hash join on CITIES, which substantially speeds
        // up the query.

        rs = s.executeQuery("SELECT * FROM CITIES LEFT OUTER JOIN " +
                            "    (SELECT * FROM FLIGHTS, COUNTRIES) S " +
                            "  ON CITIES.AIRPORT = S.ORIG_AIRPORT " +
                            "  WHERE S.COUNTRY_ISO_CODE = 'US'");

        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());

        // 1. Equivalent variant failed to rewrite prior to patch and was slow.
        rs = s.executeQuery("SELECT * FROM CITIES LEFT OUTER JOIN FLIGHTS " +
                            "    INNER JOIN COUNTRIES ON 1=1 " +
                            "    ON CITIES.AIRPORT = FLIGHTS.ORIG_AIRPORT " +
                            "  WHERE COUNTRIES.COUNTRY_ISO_CODE = 'US'");


        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);

        // Check that outer join has been rewritten
        assertFalse(rtsp.usedNLLeftOuterJoin());
        assertTrue(rtsp.usedHashJoin());


        // 1b. Equivalent variant of 1, just use ROJ instead.
        rs = s.executeQuery("SELECT * FROM FLIGHTS " +
                            "    INNER JOIN COUNTRIES ON 1=1 " +
                            "    RIGHT OUTER JOIN CITIES " +
                            "    ON CITIES.AIRPORT = FLIGHTS.ORIG_AIRPORT " +
                            "  WHERE COUNTRIES.COUNTRY_ISO_CODE = 'US'");


        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);

        // Check that outer join has been rewritten
        assertFalse(rtsp.usedNLLeftOuterJoin()); // ROJ is made LOJ in case
                                                 // still used
        assertTrue(rtsp.usedHashJoin());


        // 2. Equivalent variant failed to rewrite prior to patch and was slow.
        rs = s.executeQuery("SELECT * FROM CITIES LEFT OUTER JOIN " +
                            "   (FLIGHTS CROSS JOIN COUNTRIES) " +
                            "  ON CITIES.AIRPORT = FLIGHTS.ORIG_AIRPORT " +
                            "  WHERE COUNTRIES.COUNTRY_ISO_CODE = 'US'");
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);

        // Check that outer join has been rewritten
        assertFalse(rtsp.usedNLLeftOuterJoin());
        assertTrue(rtsp.usedHashJoin());

        // 2b. Equivalent variant of 2, just use ROJ instead.
        rs = s.executeQuery(
            "SELECT * FROM " +
            "   (FLIGHTS CROSS JOIN COUNTRIES) RIGHT OUTER JOIN " +
            "    CITIES ON CITIES.AIRPORT = FLIGHTS.ORIG_AIRPORT " +
            "  WHERE COUNTRIES.COUNTRY_ISO_CODE = 'US'");
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);

        // Check that outer join has been rewritten
        assertFalse(rtsp.usedNLLeftOuterJoin()); // ROJ is made LOJ in case
                                                 // still used
        assertTrue(rtsp.usedHashJoin());

    }


    static void insertTourRow(PreparedStatement ps, String a, String b)
            throws SQLException {
        ps.setString(1, a);
        ps.setString(2, b);
        ps.execute();
    }

    static void insertTourRow(PreparedStatement ps, int a, String b, String c)
            throws SQLException {
        ps.setInt(1,a);
        ps.setString(2, b);
        ps.setString(3, c);
        ps.execute();
    }


    /**
     * DERBY-4679. Verify that when transitive closure generates new criteria
     * into the query, it isn't confused by situations where the same column
     * name appears in a result column list multiple times due to flattening of
     * sub-queries.  
     * <p/>
     * Flattening requires remapping of (table, column) numbers in column
     * references. In cases where the same column name appears in a result
     * column list multiple times, this might earlier lead to remapping
     * (reassigning) wrong (table, column) numbers to column references in join
     * predicates transformed to where clauses as a result of the flattening.
     * <p/>
     * See also DERBY-2526 and DERBY-3023 whose fixes which were partial
     * solutions to the problem of wrong column number remappings confusing
     * the transitive closure of search predicates performed by the
     * preprocessing step of the optimizer.
     */
    public void testDerby_4679() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.execute("create table abstract_instance (" +
                  "    jz_discriminator int, " +
                  "    item_id char(32), " +
                  "    family_item_id char(32), " +
                  "    state_id char(32), " +
                  "    visibility bigint)");

        s.execute("create table lab_resource_operatingsystem (" +
                  "    jz_parent_id char(32), " +
                  "    item_id char(32))");

        s.execute("create table operating_system_software_install (" +
                  "    jz_parent_id char(32), " +
                  "    item_id char(32))");

        s.execute("create table family (" +
                  "    item_id char(32), " +
                  "    root_item_id char(32))");

        s.execute("insert into abstract_instance (" +
                  "    jz_discriminator, " +
                  "    item_id, " +
                  "    family_item_id, " +
                  "    visibility) " +
                  "values (238, 'aaaa', 'bbbb', 0)," +
                  "       (0, 'cccc', 'dddd', 0)," +
                  "       (1, 'eeee', '_5VetVWTeEd-Q8aOqWJPEIQ', 0)");

        s.execute("insert into lab_resource_operatingsystem " +
                  "values ('aaaa', 'cccc')");


        s.execute("insert into operating_system_software_install " +
                  "values ('cccc', 'eeee')");

        s.execute("insert into family " +
                  "values ('dddd', '_5ZDlwWTeEd-Q8aOqWJPEIQ')," +
                  "       ('bbbb', '_5nN9mmTeEd-Q8aOqWJPEIQ')");

        ResultSet rs = s.executeQuery(
            "select distinct t1.ITEM_ID, t1.state_id, t1.JZ_DISCRIMINATOR" +
            "    from " +
            "((((((select * from ABSTRACT_INSTANCE z1 " +
            "      where z1.JZ_DISCRIMINATOR = 238) t1 " +
            "      left outer join LAB_RESOURCE_OPERATINGSYSTEM j1 " +
            "          on (t1.ITEM_ID = j1.JZ_PARENT_ID)) " +
            "     left outer join ABSTRACT_INSTANCE t2" +
            "         on (j1.ITEM_ID = t2.ITEM_ID)) " +
            "    left outer join OPERATING_SYSTEM_SOFTWARE_INSTALL j2" +
            "        on (t2.ITEM_ID = j2.JZ_PARENT_ID))" +
            "   left outer join ABSTRACT_INSTANCE t3 on " +
            "       (j2.ITEM_ID = t3.ITEM_ID) " +
            "  inner join FAMILY t5 on (t2.FAMILY_ITEM_ID = t5.ITEM_ID)) " +
            " inner join FAMILY t7 on (t1.FAMILY_ITEM_ID = t7.ITEM_ID)) " +
            "where (t3.FAMILY_ITEM_ID IN('_5VetVWTeEd-Q8aOqWJPEIQ') and " +
            "      (t5.ROOT_ITEM_ID = '_5ZDlwWTeEd-Q8aOqWJPEIQ') and " +
            "      (t7.ROOT_ITEM_ID ='_5nN9mmTeEd-Q8aOqWJPEIQ') and " +
            "      (t1.VISIBILITY = 0))");

        JDBC.assertFullResultSet(
            rs,
            new String[][]{{"aaaa", null, "238"}});

        // Now, some subqueries instead of a base table t3, since our
        // difficulty lay in binding t3.FAMILY_ITEM_ID in the where clause
        // correctly. Subqueries still broke in the first patch for DERBY-4679.

        // Select subquery variant, cf tCorr
        rs = s.executeQuery(
            "select distinct t1.ITEM_ID, t1.state_id, t1.JZ_DISCRIMINATOR " +
            "    from " +
            "((((((select * from ABSTRACT_INSTANCE z1 " +
            "      where z1.JZ_DISCRIMINATOR = 238) t1 " +
            "      left outer join LAB_RESOURCE_OPERATINGSYSTEM j1 " +
            "          on (t1.ITEM_ID = j1.JZ_PARENT_ID)) " +
            "     left outer join ABSTRACT_INSTANCE t2 " +
            "         on (j1.ITEM_ID = t2.ITEM_ID)) " +
            "    left outer join OPERATING_SYSTEM_SOFTWARE_INSTALL j2" +
            "        on (t2.ITEM_ID = j2.JZ_PARENT_ID))" +
            "   left outer join (select * from ABSTRACT_INSTANCE) tCorr " +
            "       on (j2.ITEM_ID = tCorr.ITEM_ID) " +
            "  inner join FAMILY t5 on (t2.FAMILY_ITEM_ID = t5.ITEM_ID)) " +
            " inner join FAMILY t7 on (t1.FAMILY_ITEM_ID = t7.ITEM_ID)) " +
            "where (tCorr.FAMILY_ITEM_ID IN('_5VetVWTeEd-Q8aOqWJPEIQ') and " +
            "      (t5.ROOT_ITEM_ID = '_5ZDlwWTeEd-Q8aOqWJPEIQ') and " +
            "      (t7.ROOT_ITEM_ID ='_5nN9mmTeEd-Q8aOqWJPEIQ') and " +
            "      (t1.VISIBILITY = 0))");
        JDBC.assertFullResultSet(
            rs,
            new String[][]{{"aaaa", null, "238"}});

        // values subquery variant, cf tCorr
        rs = s.executeQuery(
            "select distinct t1.ITEM_ID, t1.state_id, t1.JZ_DISCRIMINATOR " +
            "    from " +
            "((((((select * from ABSTRACT_INSTANCE z1 " +
            "      where z1.JZ_DISCRIMINATOR = 238) t1 " +
            "      left outer join LAB_RESOURCE_OPERATINGSYSTEM j1 " +
            "          on (t1.ITEM_ID = j1.JZ_PARENT_ID)) " +
            "     left outer join ABSTRACT_INSTANCE t2 " +
            "         on (j1.ITEM_ID = t2.ITEM_ID)) " +
            "    left outer join OPERATING_SYSTEM_SOFTWARE_INSTALL j2 " +
            "        on (t2.ITEM_ID = j2.JZ_PARENT_ID))" +
            "   left outer join " +
            "       (values (238, 'aaaa', 'bbbb', 0)," +
            "       (0, 'cccc', 'dddd', 0)," +
            "       (1, 'eeee', '_5VetVWTeEd-Q8aOqWJPEIQ', 0)) " +
            "       tCorr(jz_discriminator,item_id,family_item_id,visibility)" +
            "       on (j2.ITEM_ID = tCorr.ITEM_ID) " +
            "  inner join FAMILY t5 on (t2.FAMILY_ITEM_ID = t5.ITEM_ID)) " +
            " inner join FAMILY t7 on (t1.FAMILY_ITEM_ID = t7.ITEM_ID)) " +
            "where (tCorr.FAMILY_ITEM_ID IN('_5VetVWTeEd-Q8aOqWJPEIQ') and " +
            "      (t5.ROOT_ITEM_ID = '_5ZDlwWTeEd-Q8aOqWJPEIQ') and " +
            "      (t7.ROOT_ITEM_ID ='_5nN9mmTeEd-Q8aOqWJPEIQ') and " +
            "      (t1.VISIBILITY = 0))");
        JDBC.assertFullResultSet(
            rs,
            new String[][]{{"aaaa", null, "238"}});


        s.executeUpdate("create view tView as select * from ABSTRACT_INSTANCE");

        // view subquery variant, cf tCorr
        rs = s.executeQuery(
            "select distinct t1.ITEM_ID, t1.state_id, t1.JZ_DISCRIMINATOR " +
            "    from " +
            "((((((select * from ABSTRACT_INSTANCE z1 " +
            "      where z1.JZ_DISCRIMINATOR = 238) t1 " +
            "      left outer join LAB_RESOURCE_OPERATINGSYSTEM j1 " +
            "          on (t1.ITEM_ID = j1.JZ_PARENT_ID)) " +
            "     left outer join ABSTRACT_INSTANCE t2 " +
            "         on (j1.ITEM_ID = t2.ITEM_ID)) " +
            "    left outer join OPERATING_SYSTEM_SOFTWARE_INSTALL j2 " +
            "        on (t2.ITEM_ID = j2.JZ_PARENT_ID))" +
            "   left outer join tView on (j2.ITEM_ID = tView.ITEM_ID) " +
            "  inner join FAMILY t5 on (t2.FAMILY_ITEM_ID = t5.ITEM_ID)) " +
            " inner join FAMILY t7 on (t1.FAMILY_ITEM_ID = t7.ITEM_ID)) " +
            "where (tView.FAMILY_ITEM_ID IN('_5VetVWTeEd-Q8aOqWJPEIQ') and " +
            "      (t5.ROOT_ITEM_ID = '_5ZDlwWTeEd-Q8aOqWJPEIQ') and " +
            "      (t7.ROOT_ITEM_ID ='_5nN9mmTeEd-Q8aOqWJPEIQ') and " +
            "      (t1.VISIBILITY = 0))");
        JDBC.assertFullResultSet(
            rs,
            new String[][]{{"aaaa", null, "238"}});

        rollback();
    }

}

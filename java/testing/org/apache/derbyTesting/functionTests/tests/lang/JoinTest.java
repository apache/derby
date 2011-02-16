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

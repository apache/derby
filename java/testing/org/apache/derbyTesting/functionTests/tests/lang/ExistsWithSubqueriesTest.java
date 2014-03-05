/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ExistsWithSubqueriesTest

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

import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * This test contains a variety of cases of EXISTS predicates with subqueries.
 *
 * Several tests ensure that an EXISTS predicate which wraps a set operation--
 * meaning a UNION, INTERSECT, or EXCEPT node--returns the correct results.
 * For example:
 *
 *   select * from ( values 'BAD' ) as T
 *     where exists ((values 1) intersect (values 2))
 *
 * should return zero rows. Prompted by DERBY-2370.
 *
 * A somewhat unrelated test verifies the DERBY-3033 behavior, which
 * involves flattening of subqueries with NOT EXISTS predicates. The
 * issue here is that a flattened NOT EXISTS subquery cannot be used
 * to perform equi-join transitive closure, because the implied predicate
 * that results from the flattening is a NOT EQUALS condition.
 */
public class ExistsWithSubqueriesTest extends BaseJDBCTestCase {
    
    private static final String EXISTS_PREFIX_1 =
        "select * from ( values 'GOT_A_ROW' ) as T where exists (";

    private static final String EXISTS_PREFIX_2 =
        "select j from onerow where exists (";

    /**
     * Create a test with the given name.
     * @param name name of the test.
     *
     */
    public ExistsWithSubqueriesTest(String name)
    {
        super(name);
    }
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
       TestSuite suite = new TestSuite("EXISTS with SET operations");

        /* This is a language/optimization test so behavior will be the
         * same for embedded and client/server.  Therefore we only need
         * to run the test against one or the other; we choose embedded.
         */
        suite.addTest(
            TestConfiguration.embeddedSuite(ExistsWithSubqueriesTest.class));

        /* Wrap the suite in a CleanDatabaseTestSetup that will create
         * and populate the test tables.
         */
        return new CleanDatabaseTestSetup(suite) 
        {
            /**
            * Create and populate the test table.
            */
            protected void decorateSQL(Statement s) throws SQLException
            {
                s.executeUpdate("create table empty (i int)"); 
                s.executeUpdate("create table onerow (j int)");
                s.executeUpdate("insert into onerow values 2");
                s.executeUpdate("create table diffrow (k int)");
                s.executeUpdate("insert into diffrow values 4");
                s.executeUpdate("create table tworows (p int)");
                s.executeUpdate("insert into tworows values 2, 4");
                s.executeUpdate("create table onerow2col (j1 int, j2 int)");
                s.executeUpdate("insert into onerow2col values (2, 2)");
            }
        };
    }

    /**
     * Test queries where the set operation just involves VALUES
     * expressions.
     */
    public void testSetOpsWithVALUES() throws Exception
    {
        Statement st = createStatement();
        String [][] expRS = new String [1][1];

        expRS[0][0] = "GOT_A_ROW";
        checkQuery(st, expRS, EXISTS_PREFIX_1 + "values 1 union values 1)");
        checkQuery(st, expRS, EXISTS_PREFIX_1 + "values 1 intersect values 1)");
        checkQuery(st, expRS, EXISTS_PREFIX_1 + "values 1 except values 0)");

        checkQuery(st, null, EXISTS_PREFIX_1 + "values 1 intersect values 0)");
        checkQuery(st, null, EXISTS_PREFIX_1 + "values 1 except values 1)");
        st.close();
    }

    /**
     * Test queries where the set operation has subqueries which are not
     * correlated to the outer query.  It's important to check for cases
     * where we have explicit columns _and_ cases where we have "*" because
     * the binding codepaths differ and we want to verify both.
     */
    public void testNonCorrelatedSetOps() throws Exception
    {
        Statement st = createStatement();
        String [][] expRS = new String [1][1];

        expRS[0][0] = "GOT_A_ROW";

        // Expect 1 row for the following.

        // Simple UNION with "*".
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select * from diffrow union select * from onerow)");

        // Simple UNION with explicit columns.
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select k from diffrow union select j from onerow)");

        // Simple INTERSECT with "*".
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select * from diffrow intersect select 4 from onerow)");

        // Simple INTERSECT with explicit columns.
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select k from diffrow intersect select 4 from onerow)");

        // Simple EXCEPT with "*".
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select * from diffrow except select * from onerow)");

        // Simple EXCEPT with explicit columns.
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "select k from diffrow except select j from onerow)");

        // EXCEPT with "*" where left and right children have their
        // own preds.
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "(select * from tworows where p = 2) except " +
            "(select * from tworows where p <> 2))");

        // INTERSECT with "*" where left and right children have their
        // own preds.
        checkQuery(st, expRS, EXISTS_PREFIX_1 +
            "(select * from tworows where p = 2) intersect " +
            "(select * from tworows where p = 2))");

        // Expect 0 rows for the following.  Similar queries to
        // above except modified to return no rows.

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select i from empty union select * from empty)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select * from onerow intersect select * from empty)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select j from onerow intersect select i from empty)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select * from empty except select * from onerow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select i from empty except select j from onerow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select * from onerow intersect select * from diffrow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select j from onerow intersect select k from diffrow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select * from onerow except select * from onerow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "select j from onerow except select j from onerow)");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "(select * from tworows where p = 2) intersect " +
            "(select * from tworows where p <> 2))");

        checkQuery(st, null, EXISTS_PREFIX_1 +
            "(select * from tworows where p = 2) except " +
            "(select * from tworows where p = 2))");

        // Negative cases.  These should fail because "oops" is not
        // a valid column in ONEROW.

        assertCompileError("42X04", EXISTS_PREFIX_1 +
            "(select * from onerow where j = 2) intersect " +
            "(select oops from onerow where j <> 2))");

        assertCompileError("42X04", EXISTS_PREFIX_1 +
            "(select * from onerow where j = 2) intersect " +
            "(select * from onerow where oops <> 2))");

        st.close();
    }

    /**
     * Test queries where the set operation has subqueries which are
     * correlated to the outer query.  Subqueries should still be able
     * reference the outer query table and execute without error.
     */
    public void testCorrelatedSetOps() throws Exception
    {
        Statement st = createStatement();
        String [][] expRS = new String [1][1];

        // "2" here is the value that was inserted into "onerow".
        expRS[0][0] = "2";
        
        // Expect 1 row for the following.

        // Right child of UNION has "*" for RCL and references table
        // from outer query.
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select * from diffrow where onerow.j < k)");

        // Right child of UNION has qualified "*" for RCL and references
        // table from outer query.
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select diffrow.* from diffrow where onerow.j < k)");

        // Right child of UNION has explicit RCL and references
        // table from outer query.
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select k from diffrow where onerow.j < k)");

        /* Right child of UNION is itself another EXISTS query whose
         * child is another set operator (INTERSECT). The INTERSECT in
         * turn has a right child which references a table from the
         * outer-most query.
         */
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where exists " +
            "  (select 2 from diffrow intersect " +
            "     select 2 from diffrow where onerow.j < k))");

        /* Right child of UNION is itself another EXISTS query whose
         * child is another set operator (INTERSECT). The INTERSECT in
         * turn has a right child which references a table from the
         * outer-most query.  In this one the INTERSECT returns zero
         * rows.
         */
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select * from diffrow where exists " +
            "  (select 2 from empty intersect " +
            "    select 3 from empty where onerow.j < i))");

        /* Right child of UNION is itself another EXISTS query whose
         * child is another set operator (INTERSECT). The INTERSECT in
         * turn has a right child which references 1) a table from the
         * outer-most query, and 2) a table from the INTERSECT node's
         * "parent" subquery (i.e. from the UNION's right subquery).
         */
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select * from diffrow where exists " +
            "  (select 2 from onerow2col intersect " +
            "    select 3 from empty where onerow.j < diffrow.k))");

        /* Right child of UNION is itself another EXISTS query whose
         * child is another set operator (INTERSECT). The INTERSECT in
         * turn has a right child which references 1) a table from the
         * outer-most query, and 2) a table from the INTERSECT node's
         * "parent" query.  In addition, add another predicate to the
         * UNION's right subquery and make that predicate reference
         * both 1) a table from the outer-most query, and 2) a table
         * in the subquery's own FROM list.  All of this to ensure
         * that binding finds the correct columns at all levels of
         * the query.
         */
        checkQuery(st, expRS, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select * from diffrow where exists " +
            "  (select 2 from onerow2col intersect " +
            "    select 3 from empty where onerow.j < k) " +
            "  and (onerow.j < diffrow.k))");

        // Expect 0 rows for the following.  Similar queries to
        // above except modified to return no rows.

        checkQuery(st, null, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where onerow.j > k)");

        checkQuery(st, null, EXISTS_PREFIX_2 +
              "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where exists " +
            "  (select 2 from diffrow intersect " +
            "     select 3 from diffrow where onerow.j < k))");

        checkQuery(st, null, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where exists " +
            "  (select 2 from empty intersect " +
            "    select 3 from empty where onerow.j < i))");

        checkQuery(st, null, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where exists " +
            "  (select 2 from onerow2col intersect " +
            "    select 3 from empty where onerow.j < diffrow.k))");

        checkQuery(st, null, EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from diffrow where exists " +
            "  (select 2 from onerow2col intersect " +
            "    select 3 from empty where onerow.j < k) " +
            "  and (onerow.j < diffrow.k))");

        // Negative cases.

        // Should fail because left and right children of the UNION
        // have different RCL sizes. (NOTE: Would have passed prior
        // to DERBY-2370, but that was incorrect).
        assertCompileError("42X58", EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 0 union " +
            "select * from onerow2col where onerow.j < j)");

        /* Should fail because there is an explicit subquery ("SELECT *")
         * within the EXISTS query and such a subquery is not allowed to
         * reference outer tables.  So we will be unable to find the
         * column "onerow.j" in this case.
         */
        assertCompileError("42X04", EXISTS_PREFIX_2 +
            "select * from (select 1 from diffrow where 1 = 0 " +
            "union select * from diffrow where onerow.j < k) x)");

        /* Should fail because the UNION's right subquery is trying to
         * select from an outer table.  While the subquery is allowed
         * to reference the outer table in expressions, it cannot
         * include the outer table in its RCL.
         */
        assertCompileError("42X10", EXISTS_PREFIX_2 +
            "select 1 from diffrow where 1 = 1 union " +
            "select onerow.* from diffrow where onerow.j < k)");

        st.close();
    }

    /**
     * Simple helper method to assert the results of the received
     * query.  If the array representing expected results is null
     * then we assert that the query returns no rows.
     */
    private void checkQuery(Statement st, String [][] expRS,
        String query) throws Exception
    {
        ResultSet rs = st.executeQuery(query);
        if (expRS == null)
            JDBC.assertEmpty(rs);
        else
            JDBC.assertFullResultSet(rs, expRS);
        rs.close();
    }

    /**
     * Regression test for Derby-3033.
     *
     * This method constructs a query with the property that it:
     * - contains a NOT EXISTS condition against a correlated subquery
     * - such that if that subquery is flattened, the result is 3 tables
     *   which all have join predicates on the same key.
     * The point of the test is that it is *not* correct to construct
     * a new equijoin predicate between table d3033_a and d3033_c via
     * transitive closure, because the join condition between d3033_b and
     * d3033_c is NOT EXISTS.
     *
     * In the original bug, the compiler/optimizer erroneously generated
     * the extra equijoin predicate, which caused NPE exceptions at
     * runtime due to attempts to reference the non-existent (NOT EXISTS) row.
     *
     * So this test succeeds if it gets the right results and no NPE.
     */
    public void testDerby3033()
        throws Exception
    {
        setupDerby3033();

        PreparedStatement pstmt = prepareStatement(
            "select c1, c2_b " +
            "from (select distinct st.c1,st.c2_b,dsr.c3_a,st.c3_b " +
            "      from " +
            "             d3033_a dsr, " +  // Table order matters here!
            "             d3033_b st " +
            "      where dsr.c4_a is null " +
            "      and   dsr.c2 = ? " +
            "      and   dsr.c1 = st.c1 " +
            "      and   not exists ( " +
            "              select 1 " +
            "              from d3033_c " +
            "              where d3033_c.c1 = st.c1 " +
            "              and   d3033_c.c2 = ? " +
            "              and   d3033_c.c3_c = ? " +
            "              ) " +
            ") temp "
        );
 
        pstmt.setInt(1, 4);
        pstmt.setInt(2, 4);
        pstmt.setInt(3, 100);
 
        String [][]expected = {
            { "1", "100" },
            { "2", "200" },
            { "3", "300" },
        };
        ResultSet rs = pstmt.executeQuery();
        JDBC.assertFullResultSet(rs, expected);
        pstmt.close();
    }

    /**
     * Ensure that the #rows statistics are updated
     */
    private void updateStats(Statement st, String tName)
        throws Exception
    {
        ResultSet rs = st.executeQuery("select * from " + tName);
        int numRows = 0;
        while (rs.next())
            numRows ++;
        rs.close();
    }

    private void setupDerby3033()
        throws Exception
    {
        // The pattern of inserting the data is fairly important, as we
        // are going to do a combination of joins between the three tables
        // and we want both matching data and non-matching data. We load:
        //
        // d3033_a      d3033_b       d3033_c
        // --------     --------      --------
        //    1            1             1
        //    2            2             3
        //    3            3
        //                 4
        //
        // We also load a whole pile of irrelevant data into tables a and c
        // so that the index becomes relevant in the optimizer's analysis,
        // then we create some constraints and indexes and delete the rows
        // from table d3033_c (the NOT EXISTS table).
        //
        Statement s = createStatement();

        s.executeUpdate("create table d3033_a "+
                        "(c1 int, c2 int, c3_a int, c4_a date)");
        s.executeUpdate("create table d3033_b "+
                        "(c1 int primary key not null, c2_b int, c3_b date)");
        s.executeUpdate("create table d3033_c (c1 int, c2 int, c3_c int)");
        s.executeUpdate("insert into d3033_a (c1,c2,c3_a) values(1, 4, 10)");
        s.executeUpdate("insert into d3033_a (c1,c2,c3_a) values(2, 4, 20)");
        s.executeUpdate("insert into d3033_a (c1,c2,c3_a) values(3, 4, 30)");
        s.executeUpdate("insert into d3033_b values(1, 100, CURRENT_DATE)");
        s.executeUpdate("insert into d3033_b values(2, 200, CURRENT_DATE)");
        s.executeUpdate("insert into d3033_b values(3, 300, CURRENT_DATE)");
        s.executeUpdate("insert into d3033_b values(4, 400, CURRENT_DATE)");
        s.executeUpdate("insert into d3033_c values(1, 4, 100)");
        s.executeUpdate("insert into d3033_c values(3, 4, 100)");
            
        PreparedStatement pstmt2 = prepareStatement(
                "insert into d3033_a (c1, c2, c3_a) values (?,?,?)");

        PreparedStatement pstmt = prepareStatement(
                "insert into d3033_b (c1, c2_b, c3_b) values (?,?,?)");

        java.util.Date now = new java.util.Date();
        java.sql.Timestamp nowTS = new java.sql.Timestamp(now.getTime());
        for (int i = 0; i < 15; i++)
        {
            pstmt.setInt(1, 100+i);
            pstmt.setInt(2, 100+i);
            pstmt.setTimestamp(3, nowTS);
            pstmt.executeUpdate();

            for (int j = 0; j < 200; j++)
            {
                pstmt2.setInt(1, 1000+j);
                pstmt2.setInt(2, 100+i); // note "i" here (FK)
                pstmt2.setInt(3, 1000 + (j+1)*10);
                pstmt2.executeUpdate();
            }
        }


        s.executeUpdate("alter table d3033_a add constraint " +
                "d3033_a_fk foreign key (c2) references d3033_b(c1) " +
                "on delete cascade on update no action");

        s.executeUpdate("alter table d3033_c add constraint " +
                "d3033_c_fk foreign key (c1) references d3033_b(c1) " +
                "on delete cascade on update no action");

        s.executeUpdate("delete from d3033_c");

        // Update the statistics on the 3 tables:
        updateStats(s, "d3033_a");
        updateStats(s, "d3033_b");
        updateStats(s, "d3033_c");

        s.close();
    }

    /**
     * Some EXISTS subqueries (and IN subqueries transformed to EXISTS)
     * returned NULL instead of TRUE or FALSE before DERBY-6408. This test
     * case verifies the fix.
     */
    public void testDerby6408() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        // This statement used to return only NULLs.
        JDBC.assertFullResultSet(
                s.executeQuery("values (exists(select * from empty), "
                        + "not exists (select * from empty), "
                        + "not (exists (select * from empty)), "
                        + "not (not exists (select * from empty)))"),
                new String[][] {{"false", "true", "true", "false" }});

        // This similar statement worked even before the fix.
        JDBC.assertFullResultSet(
                s.executeQuery("values (exists(select * from onerow), "
                        + "not exists (select * from onerow), "
                        + "not (exists (select * from onerow)), "
                        + "not (not exists (select * from onerow)))"),
                new String[][] {{"true", "false", "false", "true" }});

        // Now put the same expressions in the SELECT list. Used to return
        // only NULLs.
        JDBC.assertFullResultSet(
                s.executeQuery("select exists(select * from empty), "
                        + "not exists (select * from empty), "
                        + "not (exists (select * from empty)), "
                        + "not (not exists (select * from empty)) from onerow"),
                new String[][] {{"false", "true", "true", "false" }});

        // Check the returned value when used in a WHERE predicate. All of
        // these queries returned one row, but they should return no rows
        // because EXISTS shouldn't return null.
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (exists (select * from empty)) is null"));
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (not exists (select * from empty)) is null"));
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (not (not exists (select * from empty))) is null"));

        // The results were correct even before the fix if the subquery
        // wasn't empty. Verify that they still are.
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (exists (select * from onerow)) is null"));
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (not exists (select * from onerow)) is null"));
        JDBC.assertEmpty(s.executeQuery("select * from onerow "
                + "where (not (not exists (select * from onerow))) is null"));

        // Similar problems were seen in IN subqueries that were rewritten
        // to EXISTS subqueries internally. For example, this query used
        // to return NULL.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values 1 in (select j from onerow)"), "false");

        // If it should evaluate to TRUE, it worked even before the fix.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values 2 in (select j from onerow)"), "true");

        // DERBY-6409: Quantified comparisons can also be rewritten to EXISTS,
        // and these two queries returned wrong results before the fix.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values 1 > all (select 2 from tworows)"), "false");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values 1 < all (select 2 from tworows)"), "true");

        // Verify that EXISTS works in INSERT and UPDATE.
        s.execute("create table d6408(id int generated by default as identity,"
                + " b boolean not null)");

        // This used to fail with
        // ERROR 23502: Column 'B'  cannot accept a NULL value.
        s.execute("insert into d6408(b) values exists (select * from empty), "
                + "not exists (select * from empty), "
                + "exists (select * from onerow), "
                + "not exists (select * from onerow)");

        JDBC.assertFullResultSet(
                s.executeQuery("select b from d6408 order by id"),
                new String[][] {{"false"}, {"true"}, {"true"}, {"false"}});

        // These used to fail with
        // ERROR 23502: Column 'B'  cannot accept a NULL value.
        s.execute("update d6408 set b = exists (select * from empty)");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select distinct b from d6408"), "false");
        s.execute("update d6408 set b = not exists (select * from empty)");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select distinct b from d6408"), "true");

        // These passed even before the fix.
        s.execute("update d6408 set b = exists (select * from onerow)");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select distinct b from d6408"), "true");
        s.execute("update d6408 set b = not exists (select * from onerow)");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select distinct b from d6408"), "false");
    }
}

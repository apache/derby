package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.PredicatePushdownTest

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
public final class PredicatePushdownTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public PredicatePushdownTest(String name) {
        super(name);
    }

    public static Test suite() {
        Properties systemProperties = new Properties();
        systemProperties.setProperty("derby.optimizer.noTimeout","true");
        TestSuite suite = new TestSuite("predicatePushdown Test");
        suite.addTest(new SystemPropertyTestSetup(new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(PredicatePushdownTest.class)),systemProperties));
        return suite;
    }

    public void test_predicatePushdown() throws Exception {
        ResultSet rs = null;
        ResultSetMetaData rsmd;
       
        PreparedStatement pSt;
        CallableStatement cSt;
        Statement st = createStatement();

        String[][] expRS;
        String[] expColNames;

        // Licensed to the Apache Software Foundation (ASF)
        // under one or more contributor license agreements. See
        // the NOTICE file distributed with this work for
        // additional information regarding copyright ownership.
        // The ASF licenses this file to You under the Apache
        // License, Version 2.0 (the "License"); you may not use
        // this file except in compliance with the License. You
        // may obtain a copy of the License at
        // http://www.apache.org/licenses/LICENSE-2.0 Unless
        // required by applicable law or agreed to in writing,
        // software distributed under the License is distributed
        // on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
        // OF ANY KIND, either express or implied. See the
        // License for the specific language governing permissions
        // and limitations under the License. Test predicate
        // pushdown into expressions in a FROM list. As of
        // DERBY-805 this test only looks at pushing predicates
        // into UNION operators, but this test will likely grow as
        // additional predicate pushdown functionality is added to
        // Derby. Note that "noTimeout" is set to true for this
        // test because we print out a lot of query plans and we
        // don't want the plans to differ from one machine to
        // another (which can happen if some machines are faster
        // than others when noTimeout is false). Create the basic
        // tables/views for DERBY-805 testing.

        st
                .executeUpdate("CREATE TABLE \"APP\".\"T1\" (\"I\" INTEGER, \"J\" INTEGER)");

        st.executeUpdate(" insert into t1 values (1, 2), (2, 4), (3, 6), (4, "
                + "8), (5, 10)");

        st
                .executeUpdate(" CREATE TABLE \"APP\".\"T2\" (\"I\" INTEGER, \"J\" INTEGER)");

        st.executeUpdate(" insert into t2 values (1, 2), (2, -4), (3, 6), (4, "
                + "-8), (5, 10)");

        st
                .executeUpdate(" CREATE TABLE \"APP\".\"T3\" (\"A\" INTEGER, \"B\" INTEGER)");

        st.executeUpdate(" insert into T3 values (1,1), (2,2), (3,3), (4,4), "
                + "(6, 24), (7, 28), (8, 32), (9, 36), (10, 40)");

        st.executeUpdate(" insert into t3 (a) values 11, 12, 13, 14, 15, 16, "
                + "17, 18, 19, 20");

        assertUpdateCount(st, 10, " update t3 set b = 2 * a where a > 10");

        st
                .executeUpdate(" CREATE TABLE \"APP\".\"T4\" (\"A\" INTEGER, \"B\" INTEGER)");

        st.executeUpdate(" insert into t4 values (3, 12), (4, 16)");

        st.executeUpdate(" insert into t4 (a) values 11, 12, 13, 14, 15, 16, "
                + "17, 18, 19, 20");

        assertUpdateCount(st, 10, " update t4 set b = 2 * a where a > 10");

        st.executeUpdate(" create view V1 as select i, j from T1 union select "
                + "i,j from T2");

        st.executeUpdate(" create view V2 as select a,b from T3 union select "
                + "a,b from T4");

        // Run compression on the test tables to try to get a
        // consistent set of row count stats for the tables
        // (DERBY-1902, DERBY-3479).

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T1', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T2', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T3', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T4', 1)");
        assertUpdateCount(cSt, 0);

        // Now that we have the basic tables and views for the
        // tests, run some quick queries to make sure that the
        // optimizer will still consider NOT pushing the predicates
        // and will instead do a hash join. The optimizer should
        // choose do this so long as doing so is the best choice,
        // which usually means that we don't have indexes on the
        // tables or else we have relatively small tables. Start
        // by checking the case of small (~20 row) tables. We
        // should see hash joins and table scans in ALL of these
        // cases. 
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
       
      
        

        rs = st.executeQuery("select * from V1, V2 where V1.j = V2.b");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String[][] { { "1", "2", "2", "2" }, { "2", "4", "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        RuntimeStatisticsParser p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());

        rs = st.executeQuery("select * from V2, V1 where V1.j = V2.b");

        expColNames = new String[] { "A", "B", "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2", "1", "2" }, { "4", "4", "2", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());

        // Nested unions.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2 union select * from t1 union select * from t2 ) "
                        + "x1, (select * from t3 union select * from t4 union "
                        + "select * from t4 ) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());


        rs = st
                .executeQuery("select * from (select * from t1 union all select * "
                        + "from t2) x1, (select * from t3 union select * from "
                        + "t4) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "8", "4", "4" },
                { "4", "8", "4", "16" }, { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "-8", "4", "4" },
                { "4", "-8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1, (select * from t3 union all select * from "
                        + "t4) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());


        rs = st
                .executeQuery("select * from (select * from t1 union all select * "
                        + "from t2) x1, (select * from t3 union all select * "
                        + "from t4) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "8", "4", "4" },
                { "4", "8", "4", "16" }, { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "-8", "4", "4" },
                { "4", "-8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan", p.usedTableScan());
        assertTrue("Expected hash join", p.usedHashJoin());

        // Next set of queries tests pushdown of predicates whose
        // column references do not reference base tables--ex. they
        // reference literals, aggregates, or subqueries. We don't
        // check the query plans here, we're just checking to make
        // sure pushdown doesn't cause problems during compilation/
        // execution. In the case of regressions, errors that
        // might show up here include compile-time NPEs,
        // execution-time NPEs, errors saying no predicate was
        // found for a hash join, and/or type comparison errors
        // caused by incorrect column numbers for scoped predicates.

        st.executeUpdate("create table tc (c1 char, c2 char, c3 char, c int)");

        st.executeUpdate(" create view vz (z1, z2, z3, z4) as select distinct "
                + "xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select "
                + "c1, c, c2, c3 from tc) xx1 union select "
                + "'i','j','j',i from t2");

        st.executeUpdate(" create view vz2 (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c, c2, c3 from tc) xx1");

        st.executeUpdate(" create view vz3 (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c, c2, 28 from tc) xx1 union select "
                + "'i','j','j',i from t2");

        st.executeUpdate(" create view vz4 (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c, c2, 28 from tc) xx1 union select "
                + "'i','j','j',i from t2 union select c1, c2, c3, c from tc");

        // For DERBY-1866. The problem for DERBY-1866 was that,
        // when pushing predicates to subqueries beneath UNIONs,
        // the predicates were always being pushed to the *first*
        // table in the FROM list, regardless of whether or not
        // that was actually the correct table. For the test query
        // that uses this view (see below) the predicate is
        // supposed to be pushed to TC, so in order to repro the
        // DERBY-1866 failure we want to make sure that TC is *not*
        // the first table in the FROM list. Thus we use the
        // optimizer override to fix the join order so that TC is
        // the second table.

        st.executeUpdate("create view vz5a (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c2, c3, c from "
                + "--DERBY-PROPERTIES joinOrder=FIXED \n"
                + "t2, tc where tc.c = t2.i) xx1 union "
                + "select 'i','j','j',i from t2");

        // Same as above but target FromTable in subquery is
        // itself another subquery.

        st.executeUpdate("create view vz5b (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c2, c3, c from --DERBY-PROPERTIES "
                + "joinOrder=FIXED \n t2, (select distinct * from tc) tc "
                + "where tc.c = t2.i) xx1 union select 'i','j','j',i from t2");

        // Same as above but target FromTable in subquery is
        // another union node between two subqueries.

        st.executeUpdate("create view vz5c (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c2, c3, c from --DERBY-PROPERTIES "
                + "joinOrder=FIXED \n t2, (select * from tc union select "
                + "* from tc) tc where tc.c = t2.i) xx1 union select "
                + "'i','j','j',i from t2");

        // Same as above but target FromTable in subquery is
        // another full query with unions and subqueries.

        st.executeUpdate("create view vz5d (z1, z2, z3, z4) as select "
                + "distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from "
                + "(select c1, c2, c3, c from --DERBY-PROPERTIES "
                + "joinOrder=FIXED \n t2, (select * from tc union select "
                + "z1 c1, z2 c2, z3 c3, z4 c from vz5b) tc where tc.c "
                + "= t2.i) xx1 union select 'i','j','j',i from t2");

        // Both sides of predicate reference aggregates.

        rs = st
                .executeQuery("select x1.c1 from (select count(*) from t1 union "
                        + "select count(*) from t2) x1 (c1), (select count(*) "
                        + "from t3 union select count(*) from t4) x2 (c2) "
                        + "where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Both sides of predicate reference aggregates, and
        // predicate is pushed through to non-flattenable nested
        // subquery.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1) xx1 union select count(*) from "
                + "t2 ) x1 (c1), (select count(*) from t3 union select "
                + "count(*) from t4) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Both sides of predicate reference aggregates, and
        // predicate is pushed through to non-flattenable nested
        // subquery that is in turn part of a nested union.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1 union select distinct j from t2) "
                + "xx1 union select count(*) from t2 ) x1 (c1), "
                + "(select count(*) from t3 union select count(*) from "
                + "t4) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Left side of predicate references base column, right
        // side references aggregate; predicate is pushed through
        // to non- flattenable nested subquery.

        rs = st.executeQuery("select x1.c1 from (select xx1.c from (select "
                + "distinct c, c1 from tc) xx1 union select count(*) "
                + "from t2 ) x1 (c1), (select count(*) from t3 union "
                + "select count(*) from t4) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Left side of predicate references base column, right
        // side references aggregate; predicate is pushed through
        // to non- flattenable nested subquery.

        rs = st
                .executeQuery("select x1.c1 from (select xx1.c from (select c, c1 "
                        + "from tc) xx1 union select count(*) from t2 ) x1 "
                        + "(c1), (select count(*) from t3 union select "
                        + "count(*) from t4) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Left side of predicate references base column, right
        // side side references aggregate; predicate is pushed
        // through to a subquery in a nested union that has
        // literals in its result column.

        rs = st
                .executeQuery("select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, "
                        + "xx1.c3 from (select c1, c2, c3, c from tc) xx1 "
                        + "union select 'i','j',j,'i' from t2 ) x1 (z1, z2, "
                        + "z3, z4), (select count(*) from t3 union select "
                        + "count (*) from t4) x2 (c2) where x1.z3 = x2.c2");

        expColNames = new String[] { "Z1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Both sides of predicate reference base columns;
        // predicate predicate is pushed through to a subquery in a
        // nested union that has literals in its result column.

        rs = st
                .executeQuery("select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, "
                        + "xx1.c3 from (select c1, c2, c3, c from tc) xx1 "
                        + "union select 'i','j',j,'i' from t2 ) x1 (z1, z2, "
                        + "z3, z4), (select a from t3 union select count (*) "
                        + "from t4) x2 (c2) where x1.z3 = x2.c2");

        expColNames = new String[] { "Z1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "i" }, { "i" }, { "i" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous query, but with aggregate/base column
        // in x2 switched.

        rs = st
                .executeQuery("select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, "
                        + "xx1.c3 from (select c1, c2, c3, c from tc) xx1 "
                        + "union select 'i','j',j,'i' from t2 ) x1 (z1, z2, "
                        + "z3, z4), (select count(*) from t3 union select a "
                        + "from t4) x2 (c2) where x1.z3 = x2.c2");

        expColNames = new String[] { "Z1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Left side references aggregate, right side references
        // base column; predicate is pushed to non-flattenable
        // subquery that is part of a nested union for which one
        // child references a base column and the other references
        // an aggregate.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1) xx1 union select count(*) from "
                + "t2 ) x1 (c1), (select a from t3 union select a from "
                + "t4) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Same as previous query, but both children of inner-most
        // union reference base columns.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1) xx1 union select i from t2 ) x1 "
                + "(c1), (select a from t3 union select a from t4) x2 "
                + "(c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1" }, { "2" }, { "3" }, { "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Left side references aggregate, right side references
        // base column; predicate is pushed to non-flattenable
        // subquery that is part of a nested union for which one
        // child references a base column and the other references
        // an aggregate.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1) xx1 union select count(*) from "
                + "t2 ) x1 (c1), (select i from t2 union select i from "
                + "t1) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous query, but one child of x2 references
        // a literal.

        rs = st.executeQuery("select x1.c1 from (select count(*) from (select "
                + "distinct j from t1) xx1 union select count(*) from "
                + "t2 ) x1 (c1), (select 1 from t2 union select i from "
                + "t1) x2 (c2) where x1.c1 = x2.c2");

        expColNames = new String[] { "C1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Left side of predicate references a base column that is
        // deeply nested inside a subquery, a union, and a view,
        // the latter of which itself has a union between two
        // nested subqueries (whew). And finally, the position of
        // the base column w.r.t the outer query (x1) is different
        // than it is with respect to inner view (vz).

        rs = st
                .executeQuery("select x1.z4 from (select z1, z4, z3 from vz union "
                        + "select '1', 4, '3' from t1 ) x1 (z1, z4, z3), "
                        + "(select distinct j from t2 union select j from t1) "
                        + "x2 (c2) where x1.z4 = x2.c2");

        expColNames = new String[] { "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "4" }, { "2" }, { "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as above but with an expression ("i+1") instead of
        // a numeric literal.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select z1, z4, z3 from vz "
                        + "union select '1', i+1, '3' from t1 ) x1 (z1, z4, "
                        + "z3), (select distinct j from t2 union select j from "
                        + "t1) x2 (c2) where x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "4", "4" }, { "6", "6" },
                { "2", "2" }, { "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous query but with a different nested view
        // (vz2) that is missing the nested union found in vz.

        rs = st
                .executeQuery("select x1.z4 from (select z1, z4, z3 from vz2 union "
                        + "select '1', 4, '3' from t1 ) x1 (z1, z4, z3), "
                        + "(select distinct j from t2 union select j from t1) "
                        + "x2 (c2) where x1.z4 = x2.c2");

        expColNames = new String[] { "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous query but with a different nested view
        // (vz4) that has double-nested unions in it. This is a
        // test case for DERBY-1777.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select z1, z4, z3 from "
                        + "vz4 union select '1', i+1, '3' from t1 ) x1 (z1, "
                        + "z4, z3), (select distinct j from t2 union select j "
                        + "from t1) x2 (c2) where x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "4", "4" }, { "6", "6" },
                { "2", "2" }, { "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Push outer where predicate down into a UNION having a a
        // Select child with more than one table in its FROM list.
        // The predicate should be pushed to the correct table in
        // the Select's FROM list. Prior to the fix for DERBY-1866
        // the predicate was always being pushed to the *first*
        // table, regardless of whether or not that was actually
        // the correct table. Thus the predicate "t1.i = vz5.z4"
        // was getting pushed to table T2 even though it doesn't
        // apply there. The result was an ASSERT failure in sane
        // mode and an IndexOutOfBounds exception in insane mode.
        // NOTE: Use of NESTEDLOOP join strategy ensures the
        // predicate will be pushed (otherwise optimizer might
        // choose to do a hash join and we wouldn't be testing what
        // we want to test).

        rs = st
                .executeQuery("select t1.i, vz5a.* from t1 left outer join vz5a "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t1.i = vz5a.z4");

        expColNames = new String[] { "I", "Z1", "Z2", "Z3", "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "i", "j", "j", "1" },
                { "2", "i", "j", "j", "2" }, { "3", "i", "j", "j", "3" },
                { "4", "i", "j", "j", "4" }, { "5", "i", "j", "j", "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same query as above, but without the optimizer
        // override. In this case there was another error where
        // optimizer state involving the "joinOrder" override (see
        // the definition of vz5a) was not properly reset, which
        // could lead to an infinite loop. This problem was fixed
        // as part of DERBY-1866, as well.

        rs = st
                .executeQuery("select t1.i, vz5a.* from t1 left outer join vz5a on "
                        + "t1.i = vz5a.z4");

        expColNames = new String[] { "I", "Z1", "Z2", "Z3", "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "i", "j", "j", "1" },
                { "2", "i", "j", "j", "2" }, { "3", "i", "j", "j", "3" },
                { "4", "i", "j", "j", "4" }, { "5", "i", "j", "j", "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // More tests for DERBY-1866 using more complicated views.

        rs = st
                .executeQuery("select t1.i, vz5b.* from t1 left outer join vz5b "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t1.i = vz5b.z4");

        expColNames = new String[] { "I", "Z1", "Z2", "Z3", "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "i", "j", "j", "1" },
                { "2", "i", "j", "j", "2" }, { "3", "i", "j", "j", "3" },
                { "4", "i", "j", "j", "4" }, { "5", "i", "j", "j", "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select t1.i, vz5c.* from t1 left outer join vz5c "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t1.i = vz5c.z4");

        expColNames = new String[] { "I", "Z1", "Z2", "Z3", "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "i", "j", "j", "1" },
                { "2", "i", "j", "j", "2" }, { "3", "i", "j", "j", "3" },
                { "4", "i", "j", "j", "4" }, { "5", "i", "j", "j", "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select t1.i, vz5d.* from t1 left outer join vz5d "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t1.i = vz5d.z4");

        expColNames = new String[] { "I", "Z1", "Z2", "Z3", "Z4" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "i", "j", "bokibob", "1" },
                { "1", "i", "j", "j", "1" }, { "2", "i", "j", "bokibob", "2" },
                { "2", "i", "j", "j", "2" }, { "3", "i", "j", "bokibob", "3" },
                { "3", "i", "j", "j", "3" }, { "4", "i", "j", "bokibob", "4" },
                { "4", "i", "j", "j", "4" }, { "5", "i", "j", "bokibob", "5" },
                { "5", "i", "j", "j", "5" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Queries with Select->Union->Select chains having
        // differently- ordered result column lists with some
        // non-column reference expressions. In all of these
        // queries we specify LEFT join and force NESTEDLOOP in
        // order to coerce the optimizer to push predicates to a
        // specific subquery. We do this to ensure that we test
        // predicate pushdown during compilation AND during
        // execution. It's the execution-time testing that is
        // particular important for verifying DERBY-1633
        // functionality. Push predicate to union whose left child
        // has a Select within a Select, both of which have the
        // same result column ordering.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select z1, z4, z3 from vz "
                        + "union select '1', i+1, '3' from t1 ) x1 (z1, z4, "
                        + "z3) left join (select distinct i,j from (select "
                        + "distinct i,j from t2) x3 union select i, j from t1 "
                        + ") x2 (c1, c2) --DERBY-PROPERTIES "
                        + "joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "3", null }, { "4", "4" },
                { "5", null }, { "6", "6" }, { "1", null }, { "2", "2" },
                { "3", null }, { "4", "4" }, { "5", null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Push predicate to union whose left child has a Select
        // within a Select, where the result column lists for the
        // two Selects are different ("i,j" vs "j,i").

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select z1, z4, z3 from vz "
                        + "union select '1', i+1, '3' from t1 ) x1 (z1, z4, "
                        + "z3) left join (select distinct i,j from (select "
                        + "distinct j,i from t2) x3 union select i, j from t1 "
                        + ") x2 (c1, c2) --DERBY-PROPERTIES "
                        + "joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "3", null }, { "4", "4" },
                { "5", null }, { "6", "6" }, { "1", null }, { "2", "2" },
                { "3", null }, { "4", "4" }, { "5", null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Push predicate to union whose left child is itself a
        // nested subquery (through use of the view "vz") and whose
        // right child has an expression in its result column list.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select z1, z4, z3 "
                        + "from vz union select '1', i+1, '3' from t1 ) x1 "
                        + "(z1, z4, z3) --DERBY-PROPERTIES "
                        + "joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "2", "2" }, { null, "-4" },
                { "4", "4" }, { "4", "4" }, { "6", "6" }, { null, "-8" },
                { null, "8" }, { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous but with a different expression.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select z1, z4, z3 "
                        + "from vz union select '1', sin(i), '3' from t1 ) x1 "
                        + "(z1, z4, z3) --DERBY-PROPERTIES "
                        + "joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2.0", "2" }, { null, "-4" },
                { "4.0", "4" }, { null, "6" }, { null, "-8" }, { null, "8" },
                { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous but expression replaced with a regular
        // column reference.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select z1, z4, z3 "
                        + "from vz union select '1', i, '3' from t1 ) x1 (z1, "
                        + "z4, z3) --DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n"
                        + "on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2" }, { "2", "2" }, { null, "-4" },
                { "4", "4" }, { "4", "4" }, { null, "6" }, { null, "-8" },
                { null, "8" }, { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Same as previous but with a different expression and a
        // different subquery (this time using view "vz3").

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select z1, z4, z3 "
                        + "from vz3 union select '1', sin(i), '3' from t1 ) x1 "
                        + "(z1, z4, z3) --DERBY-PROPERTIES "
                        + "joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2.0", "2" }, { null, "-4" },
                { "4.0", "4" }, { null, "6" }, { null, "-8" }, { null, "8" },
                { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Push predicate to chain of unions whose left-most child
        // is itself a nested subquery (through use of the view
        // "vz") and in which the other unions have expressions in
        // their result column lists.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select z1, z4, z3 "
                        + "from vz union select '1', sin(i), '3' from t1 union "
                        + "select '1', 14, '3' from t1 ) x1 (z1, z4, z3) "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2.0", "2" }, { null, "-4" },
                { "4.0", "4" }, { null, "6" }, { null, "-8" }, { null, "8" },
                { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Push predicate to chain of unions whose right-most
        // child is itself a nested subquery (through use of the
        // view "vz") and in which the other unions have
        // expressions in their result column lists.

        rs = st
                .executeQuery("select x1.z4, x2.c2 from (select distinct i,j from "
                        + "(select distinct j,i from t2) x3 union select i, j "
                        + "from t1) x2 (c1, c2) left join (select '1', sin(i), "
                        + "'3' from t1 union select '1', 14, '3' from t1 union "
                        + "select z1, z4, z3 from vz ) x1 (z1, z4, z3) "
                        + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on x1.z4 = x2.c2");

        expColNames = new String[] { "Z4", "C2" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2.0", "2" }, { null, "-4" },
                { "4.0", "4" }, { null, "6" }, { null, "-8" }, { null, "8" },
                { null, "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Cleanup from this set of tests.

        st.executeUpdate("drop view vz");

        st.executeUpdate(" drop view vz2");

        st.executeUpdate(" drop view vz3");

        st.executeUpdate(" drop view vz4");

        st.executeUpdate(" drop view vz5a");

        st.executeUpdate(" drop view vz5d");

        st.executeUpdate(" drop view vz5b");

        st.executeUpdate(" drop view vz5c");

        st.executeUpdate(" drop table tc");

        // Now bump up the size of tables T3 and T4 to the point
        // where use of indexes will cause optimizer to choose
        // nested loop join (and push predicates) instead of hash
        // join. The following insertions put roughly 50,000 rows
        // into T3 and into T4. These numbers are somewhat
        // arbitrary, but please note that reducing the number of
        // rows in these two tables could cause the optimizer to
        // choose to skip pushing and instead use a hash join for
        // some of the test queries. That's not 'wrong' per se,
        // but it's not what we want to test here...

        getConnection().setAutoCommit(false);

        st.executeUpdate(" insert into t3 (a) values 21, 22, 23, 24, 25, 26, "
                + "27, 28, 29, 30");

        st.executeUpdate(" insert into t3 (a) values 31, 32, 33, 34, 35, 36, "
                + "37, 38, 39, 40");

        st.executeUpdate(" insert into t3 (a) values 41, 42, 43, 44, 45, 46, "
                + "47, 48, 49, 50");

        st.executeUpdate(" insert into t3 (a) values 51, 52, 53, 54, 55, 56, "
                + "57, 58, 59, 60");

        st.executeUpdate(" insert into t3 (a) values 61, 62, 63, 64, 65, 66, "
                + "67, 68, 69, 70");

        st.executeUpdate(" insert into t3 (a) values 71, 72, 73, 74, 75, 76, "
                + "77, 78, 79, 80");

        st.executeUpdate(" insert into t3 (a) values 81, 82, 83, 84, 85, 86, "
                + "87, 88, 89, 90");

        st.executeUpdate(" insert into t3 (a) values 91, 92, 93, 94, 95, 96, "
                + "97, 98, 99, 100");

        assertUpdateCount(st, 80, " update t3 set b = 2 * a where a > 20");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 20)");

        st
                .executeUpdate(" insert into t4 (a, b) (select a,b from t3 where a > 20)");

        st
                .executeUpdate(" insert into t3 (a, b) (select a,b from t4 where a > 60)");

        commit();
        getConnection().setAutoCommit(true);

        // See exactly how many rows we inserted, for sanity.

        rs = st.executeQuery("select count(*) from t3");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "54579" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select count(*) from t4");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "48812" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // At this point we create the indexes. Note that we
        // intentionally create the indexes AFTER loading the data,
        // in order ensure that the index statistics are correct.
        // We need the stats to be correct in order for the
        // optimizer to choose the correct plan (i.e. to push the
        // join predicates where possible).

        st
                .executeUpdate("CREATE INDEX \"APP\".\"T3_IX1\" ON \"APP\".\"T3\" (\"A\")");

        st
                .executeUpdate(" CREATE INDEX \"APP\".\"T3_IX2\" ON \"APP\".\"T3\" (\"B\")");

        st
                .executeUpdate(" CREATE INDEX \"APP\".\"T4_IX1\" ON \"APP\".\"T4\" (\"A\")");

        st
                .executeUpdate(" CREATE INDEX \"APP\".\"T4_IX2\" ON \"APP\".\"T4\" (\"B\")");

        // Create the rest of objects used in this test.

        st
                .executeUpdate("CREATE TABLE \"APP\".\"T5\" (\"I\" INTEGER, \"J\" INTEGER)");

        st.executeUpdate(" insert into t5 values (5, 10)");

        st
                .executeUpdate(" CREATE TABLE \"APP\".\"T6\" (\"P\" INTEGER, \"Q\" INTEGER)");

        st.executeUpdate(" insert into t5 values (2, 4), (4, 8)");

        st.executeUpdate(" CREATE TABLE \"APP\".\"XX1\" (\"II\" INTEGER NOT "
                + "NULL, \"JJ\" CHAR(10), \"MM\" INTEGER, \"OO\" "
                + "DOUBLE, \"KK\" BIGINT)");

        st.executeUpdate(" CREATE TABLE \"APP\".\"YY1\" (\"II\" INTEGER NOT "
                + "NULL, \"JJ\" CHAR(10), \"AA\" INTEGER, \"OO\" "
                + "DOUBLE, \"KK\" BIGINT)");

        st.executeUpdate(" ALTER TABLE \"APP\".\"YY1\" ADD CONSTRAINT "
                + "\"PK_YY1\" PRIMARY KEY (\"II\")");

        st.executeUpdate(" ALTER TABLE \"APP\".\"XX1\" ADD CONSTRAINT "
                + "\"PK_XX1\" PRIMARY KEY (\"II\")");

        st.executeUpdate(" create view xxunion as select all ii, jj, kk, mm "
                + "from xx1 union all select ii, jj, kk, mm from xx1 "
                + "union all select ii, jj, kk, mm from xx1 union all "
                + "select ii, jj, kk, mm from xx1 union all select ii, "
                + "jj, kk, mm from xx1 union all select ii, jj, kk, mm "
                + "from xx1 union all select ii, jj, kk, mm from xx1 "
                + "union all select ii, jj, kk, mm from xx1 union all "
                + "select ii, jj, kk, mm from xx1 union all select ii, "
                + "jj, kk, mm from xx1 union all select ii, jj, kk, mm "
                + "from xx1 union all select ii, jj, kk, mm from xx1 "
                + "union all select ii, jj, kk, mm from xx1 union all "
                + "select ii, jj, kk, mm from xx1 union all select ii, "
                + "jj, kk, mm from xx1 union all select ii, jj, kk, mm "
                + "from xx1 union all select ii, jj, kk, mm from xx1 "
                + "union all select ii, jj, kk, mm from xx1 union all "
                + "select ii, jj, kk, mm from xx1 union all select ii, "
                + "jj, kk, mm from xx1 union all select ii, jj, kk, mm "
                + "from xx1 union all select ii, jj, kk, mm from xx1 "
                + "union all select ii, jj, kk, mm from xx1 union all "
                + "select ii, jj, kk, mm from xx1 union all select ii, "
                + "jj, kk, mm from xx1");

        st.executeUpdate(" create view yyunion as select all ii, jj, kk, aa "
                + "from yy1 union all select ii, jj, kk, aa from yy1 "
                + "union all select ii, jj, kk, aa from yy1 union all "
                + "select ii, jj, kk, aa from yy1 union all select ii, "
                + "jj, kk, aa from yy1 union all select ii, jj, kk, aa "
                + "from yy1 union all select ii, jj, kk, aa from yy1 "
                + "union all select ii, jj, kk, aa from yy1 union all "
                + "select ii, jj, kk, aa from yy1 union all select ii, "
                + "jj, kk, aa from yy1 union all select ii, jj, kk, aa "
                + "from yy1 union all select ii, jj, kk, aa from yy1 "
                + "union all select ii, jj, kk, aa from yy1 union all "
                + "select ii, jj, kk, aa from yy1 union all select ii, "
                + "jj, kk, aa from yy1 union all select ii, jj, kk, aa "
                + "from yy1 union all select ii, jj, kk, aa from yy1 "
                + "union all select ii, jj, kk, aa from yy1 union all "
                + "select ii, jj, kk, aa from yy1 union all select ii, "
                + "jj, kk, aa from yy1 union all select ii, jj, kk, aa "
                + "from yy1 union all select ii, jj, kk, aa from yy1 "
                + "union all select ii, jj, kk, aa from yy1 union all "
                + "select ii, jj, kk, aa from yy1 union all select ii, "
                + "jj, kk, aa from yy1");

        // Run compression on the test tables to try to get a
        // consistent set of row count stats for the tables
        // (DERBY-1902, DERBY-3479).

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T1', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T2', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T3', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T4', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T5', 1)");
        assertUpdateCount(cSt, 0);

        cSt = prepareCall(" call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'T6', 1)");
        assertUpdateCount(cSt, 0);

        // And finally, run more extensive tests using the larger
        // tables that have indexes. In these tests the optimizer
        // should consider pushing predicates where possible. We
        // can tell if a predicate has been "pushed" by looking at
        // the query plan information for the tables in question:
        // if the table has an index on a column that is used as
        // part of the pushed predicate, then the optimizer will
        // (for these tests) do an Index scan instead of a Table
        // scan. If the table does not have such an index then the
        // predicate will show up as a "qualifier" for a Table
        // scan. In all of these tests T3 and T4 have appropriate
        // indexes, so if we push a predicate to either of those
        // tables we should see index scans. Neither T1 nor T2 has
        // indexes, so if we push a predicate to either of those
        // tables we should see a qualifier in the table scan
        // information. Predicate push-down should occur for next
        // two queries. Thus we we should see Index scans for T3
        // and T4--and this should be the case regardless of the
        // order of the FROM list.

        rs = st.executeQuery("select * from V1, V2 where V1.j = V2.b");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "2", "2" }, { "2", "4", "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);  
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));
      

        rs = st.executeQuery("select * from V2, V1 where V1.j = V2.b");

        expColNames = new String[] { "A", "B", "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "2", "1", "2" }, { "4", "4", "2", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);  
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));
      
        // Changes for DERBY-805 don't affect non-join predicates
        // (ex. "IN" or one- sided predicates), but make sure
        // things still behave--i.e. these queries should still
        // compile and execute without error. We don't expect to
        // see any predicates pushed to T3 nor T4.

        rs = st.executeQuery("select count(*) from V1, V2 where V1.i in (2,4)");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "404" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        p = SQLUtilities.getRuntimeStatisticsParser(st);  
        assertTrue("Expected table scan on T3", p.usedTableScan("T3"));
        assertTrue("Expected table scan on T4", p.usedTableScan("T4"));
      
        rs = st.executeQuery("select count(*) from V1, V2 where V1.j > 0");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "505" } };

        JDBC.assertFullResultSet(rs, expRS, true);
     
        p = SQLUtilities.getRuntimeStatisticsParser(st);  
        assertTrue("Expected table scan on T3", p.usedTableScan("T3"));
        assertTrue("Expected table scan on T4", p.usedTableScan("T4"));
      
        // Combination of join predicate and non-join predicate:
        // the join predicate should be pushed to V2 (T3 and T4),
        // the non-join predicate should operate as usual.

        rs = st
                .executeQuery("select * from V1, V2 where V1.j = V2.b and V1.i in (2,4)");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "4", "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);  
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));
      

        //  Make
        // sure predicates are pushed even if the subquery is
        // explicit (as opposed to a view). Should see index scans
        // on T3 and T4.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1, (select * from t3 union select * from t4) "
                        + "x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        // In this case optimizer will consider pushing predicate
        // to X1 but will choose not to because it's cheaper to
        // push the predicate to T3. So should see regular table
        // scans on T1 and T2.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1, t3 where x1.i = t3.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "4", "-8", "4", "4" },
                { "4", "8", "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected Table Scan ResultSet for T1", p.usedTableScan("T1"));
        assertTrue("Expected Table Scan ResultSet for T2", p.usedTableScan("T2"));
        
        // UNION
        // ALL should behave just like normal UNION. I.e.
        // predicates should still be pushed to T3 and T4.

        rs = st
                .executeQuery("select * from (select * from t1 union all select * "
                        + "from t2) x1, (select * from t3 union select * from "
                        + "t4) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "8", "4", "4" },
                { "4", "8", "4", "16" }, { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "-8", "4", "4" },
                { "4", "-8", "4", "16" }};
        
        
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        rs = st
                .executeQuery("select * from (select * from t1 union all select * "
                        + "from t2) x1, (select * from t3 union all select * "
                        + "from t4) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "8", "4", "4" },
                { "4", "8", "4", "16" }, { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "12" }, { "4", "-8", "4", "4" },
                { "4", "-8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        
        // Predicate with both sides referencing same UNION isn't a
        // join predicate, so no pushing should happen. So should
        // see regular table scans on all tables.

        rs = st.executeQuery("select * from v1, v2 where V1.i = V1.j");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);
 
        JDBC.assertEmpty(rs);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected Table Scan ResultSet for T1", p.usedTableScan("T1"));
        assertTrue("Expected Table Scan ResultSet for T2", p.usedTableScan("T2"));
        assertTrue("Expected Table Scan ResultSet for T3", p.usedTableScan("T3"));
        assertTrue("Expected Table Scan ResultSet for T4", p.usedTableScan("T4"));
  
        // Pushing predicates should still work even if user
        // specifies explicit column names. In these two queries
        // we push to X2 (T3 and T4).

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1 (c, d), (select * from t3 union select * "
                        + "from t4) x2 (e, f) where x1.c = x2.e");

        expColNames = new String[] { "C", "D", "E", "F" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));


        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1 (a, b), (select * from t3 union select * "
                        + "from t4) x2 (i, j) where x1.a = x2.i");

        expColNames = new String[] { "A", "B", "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" }};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        // In this query the optimizer will consider pushing, but
        // will find that it's cheaper to do a hash join and thus
        // will _not_ push. So we see hash join with table scan on T3.

        rs = st
                .executeQuery("select count(*) from (select * from t1 union select "
                        + "* from t3) x1 (c, d), (select * from t2 union "
                        + "select * from t4) x2 (e, f) where x1.c = x2.e");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
            
        expRS = new String[][] { { "103" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        // DERBY-3819 - this test case consistently fails for 64 bit
        // temporarily (until 3819 is fixed by changing the queries with optimizer directives)
        if (!is64BitJVM()) {
            assertTrue("Expected Table Scan ResultSet for T3", p.usedTableScan("T3"));
            assertTrue("Expected Hash Join",p.usedHashJoin());
        }

        // If we
        // have nested unions, the predicate should get pushed all
        // the way down to the base table(s) for every level of
        // nesting. Should see index scans for T3 and for _both_
        // instances of T4.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2 union select * from t1 union select * from t2 ) "
                        + "x1, (select * from t3 union select * from t4 union "
                        + "select * from t4 ) x2 where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" }};
        
        
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));


        // Nested unions with non-join predicates should work as
        // usual (no change with DERBY-805). So should see scalar
        // qualifiers on scans for all instances of T1 and T2.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2 union select * from t1 union select * from t2 ) "
                        + "x1 where x1.i > 0");

        expColNames = new String[] { "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2" }, { "2", "-4" }, { "2", "4" },
                { "3", "6" }, { "4", "-8" }, { "4", "8" }, { "5", "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        // Expect to see scalar qualifiers with <= operator for four scans.
        p.findString("Operator: <=", 4);
        //  In this
        // case there are no qualifiers, but the restriction is
        // enforced at the ProjectRestrictNode level. That hasn't
        // changed with DERBY-805.

        rs = st
                .executeQuery("select count(*) from (select * from t1 union select "
                        + "* from t2 union select * from t3 union select * "
                        + "from t4 ) x1 (i, b) where x1.i > 0");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "108" }};

        JDBC.assertFullResultSet(rs, expRS, true);
        
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected Table Scan ResultSet for T1", p.usedTableScan("T1"));
        assertTrue("Expected Table Scan ResultSet for T2", p.usedTableScan("T2"));
        assertTrue("Expected Table Scan ResultSet for T3", p.usedTableScan("T3"));
        assertTrue("Expected Table Scan ResultSet for T4", p.usedTableScan("T4"));
  
        // Predicate pushdown should work with explicit use of
        // "inner join" just like it does for implicit join. So
        // should see index scans on T3 and T4.

        rs = st
                .executeQuery("select * from (select * from t1 union select * from "
                        + "t2) x1 inner join (select * from t3 union select * "
                        + "from t4) x2 on x1.j = x2.b");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "2", "2" }, { "2", "4", "4", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        
        // Can't push predicates into VALUES clauses. Predicate should
        // end up at V2 (T3 and T4).

        rs = st.executeQuery("select * from ( select i,j from t2 union values "
                + "(1,1),(2,2),(3,3),(4,4) union select i,j from t1 ) "
                + "x0 (i,j), v2 where x0.i = v2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "1", "1", "1" },
                { "1", "2", "1", "1" }, { "2", "-4", "2", "2" },
                { "2", "2", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "3", "3", "3" }, { "3", "3", "3", "12" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "16" },
                { "4", "4", "4", "4" }, { "4", "4", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "16" }};

        JDBC.assertFullResultSet(rs, expRS, true);

        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));


        // Can't push predicates into VALUES clauses. Optimizer
        // might consider pushing but shouldn't do it; in the end
        // we'll do a hash join between X1 and T2.

        rs = st
                .executeQuery("select * from t2, (select * from t1 union values "
                        + "(3,3), (4,4), (5,5), (6,6)) X1 (a,b) where X1.a = t2.i");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "2" },
                { "2", "-4", "2", "4" }, { "3", "6", "3", "3" },
                { "3", "6", "3", "6" }, { "4", "-8", "4", "4" },
                { "4", "-8", "4", "8" }, { "5", "10", "5", "5" },
                { "5", "10", "5", "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected Hash Join", p.usedHashJoin());
        // Can't
        // push predicates into VALUES clause. We'll try to push
        // it to X1, but it will only make it to T4; it won't make
        // it to T3 because the "other side" of the union with T3
        // is a VALUES clause. So we'll see an index scan on T4
        // and table scan on T3--but the predicate should still be
        // applied to T3 at a higher level (through a
        // ProjectRestrictNode), so we shouldn't get any extra rows.

        rs = st.executeQuery("select * from (select i,j from t2 union values "
                + "(1,1),(2,2),(3,3),(4,4) union select i,j from t1 ) "
                + "x0 (i,j), (select a, b from t3 union values (4, 5), "
                + "(5, 6), (6, 7) union select a, b from t4 ) x1 (a,b) "
                + "where x0.i = x1.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "1", "1", "1" },
                { "1", "2", "1", "1" }, { "2", "-4", "2", "2" },
                { "2", "2", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "3", "3", "3" }, { "3", "3", "3", "12" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "-8", "4", "5" },
                { "4", "-8", "4", "16" }, { "4", "4", "4", "4" },
                { "4", "4", "4", "5" }, { "4", "4", "4", "16" },
                { "4", "8", "4", "4" }, { "4", "8", "4", "5" },
                { "4", "8", "4", "16" }, { "5", "10", "5", "6" }};
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan on T3", p.usedTableScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        
        // Make sure optimizer is still considering predicates for
        // other, non-UNION nodes. Here we should use the
        // predicate to do a hash join between X0 and T5 (i.e. we
        // will not push it down to X0 because a) there are VALUES
        // clauses to which we can't push, and b) it's cheaper to
        // do the hash join).

        rs = st
                .executeQuery("select * from t5, (values (2,2), (4,4) union values "
                        + "(1,1),(2,2),(3,3),(4,4) union select i,j from t1 ) "
                        + "x0 (i,j) where x0.i = t5.i");

        expColNames = new String[] { "I", "J", "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "4", "2", "2" },
                { "2", "4", "2", "4" }, { "4", "8", "4", "4" },
                { "4", "8", "4", "8" }, { "5", "10", "5", "10" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected hash join", p.usedHashJoin());

        // When we
        // have very deeply nested union queries, make sure
        // predicate push- down logic still works (esp. the scoping
        // logic). These queries won't return any results, but the
        // predicate should get pushed to EVERY instance of the
        // base table all the way down. We're just checking to
        // make sure these compile and execute without error. The
        // query plan for these two queries alone would be several
        // thousand lines so we don't print them out. We have
        // other (smaller) tests to check that predicates are
        // correctly pushed through nested unions.

        rs = st
                .executeQuery("select distinct xx0.kk, xx0.ii, xx0.jj from xxunion "
                        + "xx0, yyunion yy0 where xx0.mm = yy0.ii");

        expColNames = new String[] { "KK", "II", "JJ" };
        JDBC.assertColumnNames(rs, expColNames);

        JDBC.assertEmpty(rs);

        rs = st.executeQuery("values (1)");

        rs.next();
        rsmd = rs.getMetaData();

        pSt = prepareStatement("select distinct "
                + "xx0.kk, xx0.ii, xx0.jj from " + "xxunion xx0, "
                + "yyunion yy0 " + "where xx0.mm = yy0.ii and yy0.aa in (?) "
                + "for fetch only");

        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        rs = pSt.executeQuery();
        expColNames = new String[] { "KK", "II", "JJ" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // Predicate push-down should only affect the UNIONs
        // referenced; other UNIONs shouldn't interfere or be
        // affected. Should see table scans for T1 and T2 then an
        // index scan for the first instance of T3 and a table scan
        // for second instance of T3; likewise for two instances of T4.

        rs = st
                .executeQuery("select count(*) from (select * from t1 union select "
                        + "* from t2) x1, (select * from t3 union select * "
                        + "from t4) x2, (select * from t4 union select * from "
                        + "t3) x3 where x1.i = x3.a");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "909" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan on T1", p.usedTableScan("T1"));
        assertTrue("Expected table scan on T2", p.usedTableScan("T2"));
        assertTrue("Expected table scan on T3", p.usedTableScan("T3"));
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected table scan on T4", p.usedTableScan("T4"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        // Here we
        // should see index scans for both instances of T3 and for
        // both instances of T4.

        rs = st
                .executeQuery("select count(*) from (select * from t1 union select "
                        + "* from t2) x1, (select * from t3 union select * "
                        + "from t4) x2, (select * from t4 union select * from "
                        + "t3) x3 where x1.i = x3.a and x3.b = x2.b");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "9" }};
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));


        // Predicates pushed from outer queries shouldn't
        // interfere with inner predicates for subqueries. Mostly
        // checking for correct results here.

        rs = st
                .executeQuery("select * from (select i, b j from t1, t4 where i = "
                        + "j union select * from t2) x1, t3 where x1.j = t3.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "2", "2" },
                { "3", "6", "6", "24" }, { "5", "10", "10", "40" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Inner predicate should be handled as normal, outer
        // predicate should either get pushed to V2 (T3 and T4) or
        // else used for a hash join between x1 and v2.

        rs = st
                .executeQuery("select * from (select i, b j from t1, t4 where i = "
                        + "j union select * from t2) x1, v2 where x1.j = v2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "2", "2" },
                { "3", "6", "6", "24" }, { "5", "10", "10", "40" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        // DERBY-3819 - this test case consistently fails for 64 bit
        // temporarily (until 3819 is fixed by changing the queries with optimizer directives)
        if (!is64BitJVM()) {
            assertTrue("Expected hash join", p.usedHashJoin());
        }
        //  Outer
        // predicate should either get pushed to V2 (T3 and T4) or
        // else used for a hash join; similarly, inner predicate
        // should either get pushed to T3 or else used for hash
        // join between T1 and T3.

        rs = st
                .executeQuery("select * from (select i, j from t1, t3 where i = a "
                        + "union select * from t2) x1, v2 where x1.i = v2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "1", "1" },
                { "2", "-4", "2", "2" }, { "2", "4", "2", "2" },
                { "3", "6", "3", "3" }, { "3", "6", "3", "12" },
                { "4", "-8", "4", "4" }, { "4", "8", "4", "4" },
                { "4", "-8", "4", "16" }, { "4", "8", "4", "16" }};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected hash join", p.usedHashJoin());
 

        // Inner predicates treated as restrictions, outer
        // predicate either pushed to X2 (T2 and T1) or used for
        // hash join between X2 and X1.

        rs = st
                .executeQuery("select * from (select i, b j from t1, t4 where i = "
                        + "j union select * from t2) x1, (select i, b j from "
                        + "t2, t3 where i = j union select * from t1) x2 where "
                        + "x1.j = x2.i");

        expColNames = new String[] { "I", "J", "I", "J" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "2", "2", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        // DERBY-3819 - this test case consistently fails for 64 bit
        // temporarily (until 3819 is fixed by changing the queries with optimizer directives)
        if (!is64BitJVM()) {
            assertTrue("Expected hash join", p.usedHashJoin());
        }
 
        // Following queries deal with nested subqueries, which
        // deserve extra testing because "best paths" for outer
        // queries might not agree with "best paths" for inner
        // queries, so we need to make sure the correct paths
        // (based on predicates that are or are not pushed) are
        // ultimately generated. Predicate should get pushed to V2
        // (T3 and T4).

        rs = st
                .executeQuery("select count(*) from (select i,a,j,b from V1, V2 "
                        + "where V1.j = V2.b ) X3");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2" }};

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

 
        // Multiple subqueries but NO UNIONs. All predicates are
        // used for joins at their current level (no pushing).

        rs = st.executeQuery("select t2.i,p from (select distinct i,p from "
                + "(select distinct i,a from t1, t3 where t1.j = t3.b) "
                + "X1, t6 where X1.a = t6.p) X2, t2 where t2.i = X2.i");

        expColNames = new String[] { "I", "P" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected hash join", p.usedHashJoin());
        assertTrue("Expected table scan on T1", p.usedTableScan("T1"));
        assertTrue("Expected index row to base row for T3", p.usedIndexRowToBaseRow("T3"));
        
        // Multiple, non-flattenable subqueries, but NO UNIONs.  Shouldn't push
        // anything.

        rs = st
                .executeQuery("select x1.j, x2.b from (select distinct i,j from "
                        + "t1) x1, (select distinct a,b from t3) x2 where x1.i "
                        + "= x2.a order by x1.j, x2.b");

        expColNames = new String[] { "J", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "1" }, { "4", "2" }, { "6", "3" },
                { "8", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected distinct scan on T1", p.usedDistinctScan("T1"));
        assertTrue("Expected distinct scan  T3", p.usedDistinctScan("T3"));
    
       
       
        rs = st
                .executeQuery("select x1.j, x2.b from (select distinct i,j from "
                        + "t1) x1, (select distinct a,b from t3) x2, (select "
                        + "distinct i,j from t2) x3, (select distinct a,b from "
                        + "t4) x4 where x1.i = x2.a and x3.i = x4.a order by x1.j, x2.b");

        expColNames = new String[] { "J", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "1" }, { "2", "1" }, { "4", "2" },
                { "4", "2" }, { "6", "3" }, { "6", "3" }, { "8", "4" },
                { "8", "4" }};
        JDBC.assertFullResultSet(rs, expRS, true);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected distinct scan on T1", p.usedDistinctScan("T1"));
        assertTrue("Expected distinct scan  T2", p.usedDistinctScan("T2"));
        assertTrue("Expected distinct scan on T3", p.usedDistinctScan("T3"));
        assertTrue("Expected distinct scan  T4", p.usedDistinctScan("T4"));
   
        // Multiple subqueries that are UNIONs. Outer-most
        // predicate X0.b = X2.j can be pushed to union X0 but NOT
        // to subquery X2. Inner predicate T6.p = X1.i is eligible
        // for being pushed into union X1. In this case outer
        // predicate is pushed to X0 (so we'll see index scans on
        // T3 and T4) but inner predicate is used for a hash join
        // between X1 and T6.

        rs = st
                .executeQuery("select X0.a, X2.i from (select a,b from t4 union "
                        + "select a,b from t3) X0, (select i,j from (select "
                        + "i,j from t1 union select i,j from t2) X1, T6 where "
                        + "T6.p = X1.i) X2 where X0.b = X2.j ");

        expColNames = new String[] { "A", "I" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected hash join", p.usedHashJoin());
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        // Same as
        // above but without the inner predicate (so no hash on T6).

        rs = st
                .executeQuery("select X0.a, X2.i from (select a,b from t4 union "
                        + "select a,b from t3) X0, (select i,j from (select "
                        + "i,j from t1 union select i,j from t2) X1, T6 ) X2 "
                        + "where X0.b = X2.j ");

        expColNames = new String[] { "A", "I" };
        JDBC.assertColumnNames(rs, expColNames);

    

        JDBC.assertEmpty(rs);
        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected index scan on T3", p.usedIndexScan("T3"));
        assertTrue("Expected index scan on T4", p.usedIndexScan("T4"));

        // Same as above, but without the outer predicate. Should
        // see table scan on T3 and T4 (because nothing is pushed).

        rs = st
                .executeQuery("select X0.a, X2.i from (select a,b from t4 union "
                        + "select a,b from t3) X0, (select i,j from (select "
                        + "i,j from t1 union select i,j from t2) X1, T6 where "
                        + "T6.p = X1.i) X2 ");

        expColNames = new String[] { "A", "I" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        p = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue("Expected table scan on T3", p.usedTableScan("T3"));
        assertTrue("Expected table scan on T4", p.usedTableScan("T4"));

        // Additional tests with VALUES clauses. Mostly just
        // checking to make sure these queries compile and execute,
        // and to ensure that all predicates are enforced even if
        // they can't be pushed all the way down into a UNION. So
        // we shouldn't get back any extra rows here. NOTE: Row
        // order is not important in these queries, just so long as
        // the correct rows are returned.

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        assertUpdateCount(cSt, 0);

        rs = st.executeQuery(" select * from (select * from t1 union select * "
                + "from t2) x1, (values (2, 4), (3, 6), (4, 8)) x2 (a, "
                + "b) where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "-4", "2", "4" },
                { "2", "4", "2", "4" }, { "3", "6", "3", "6" },
                { "4", "-8", "4", "8" }, { "4", "8", "4", "8" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // ---------------------------------------------

        rs = st.executeQuery("select * from"
                + "(select * from t1 union (values (1, -1), (2, "
                + "-2), (5, -5))) x1 (i, j),"
                + "(values (2, 4), (3, 6), (4, 8)) x2 (a, b)"
                + "where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "-2", "2", "4" },
                { "2", "4", "2", "4" }, { "3", "6", "3", "6" },
                { "4", "8", "4", "8" }};

        JDBC.assertFullResultSet(rs, expRS);

        rs = st
                .executeQuery(" select * from (select * from t1 union all (values "
                        + "(1, -1), (2, -2), (5, -5))) x1 (i, j), (values (2, "
                        + "4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "4", "2", "4" },
                { "3", "6", "3", "6" }, { "4", "8", "4", "8" },
                { "2", "-2", "2", "4" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from (select * from t1 union (values (1, "
                        + "-1), (2, -2), (5, -5))) x1 (i, j), (values (2, 4), "
                        + "(3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a and x2.b = x1.j");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "4", "2", "4" },
                { "3", "6", "3", "6" }, { "4", "8", "4", "8" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from (values (2, -4), (3, -6), (4, -8) "
                        + "union values (1, -1), (2, -2), (5, -5) ) x1 (i, j), "
                        + "(values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "-4", "2", "4" },
                { "2", "-2", "2", "4" }, { "3", "-6", "3", "6" },
                { "4", "-8", "4", "8" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from (values (2, -4), (3, -6), (4, -8) "
                        + "union values (1, -1), (2, -2), (5, -5) ) x1 (i, j), "
                        + "(values (2, 4), (3, 6), (4, 8)) x2 (a, b) where "
                        + "x1.i = x2.a and x2.b = x1.j");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st
                .executeQuery(" select * from (values (1, -1), (2, -2), (5, -5) "
                        + "union select * from t1) x1 (i,j), (values (2, 4), "
                        + "(3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a");

        expColNames = new String[] { "I", "J", "A", "B" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2", "-2", "2", "4" },
                { "2", "4", "2", "4" }, { "3", "6", "3", "6" },
                { "4", "8", "4", "8" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Clean up DERBY-805 objects.

        st.executeUpdate("drop view v1");

        st.executeUpdate(" drop view v2");

        st.executeUpdate(" drop table t1");

        st.executeUpdate(" drop table t2");

        st.executeUpdate(" drop table t3");

        st.executeUpdate(" drop table t4");

        st.executeUpdate(" drop table t5");

        st.executeUpdate(" drop table t6");

        st.executeUpdate(" drop view xxunion");

        st.executeUpdate(" drop view yyunion");

        st.executeUpdate(" drop table xx1");

        st.executeUpdate(" drop table yy1");

        // DERBY-1633: Nested UNIONs of views with different
        // column orderings leads to incorrectly scoped predicates.
        // We have a lot of different tables and views here to try
        // to cover several different situations. Note that all of
        // the views use DISTINCT because we don't want the views
        // to be flattened and Derby doesn't flatten select queries
        // with DISTINCT in them.

        st.executeUpdate("CREATE TABLE \"APP\".\"T1\" (\"I\" INTEGER, \"D\" "
                + "DOUBLE, \"C\" CHAR(10))");

        st.executeUpdate(" CREATE TABLE \"APP\".\"T2\" (\"I2\" INTEGER, "
                + "\"D2\" DOUBLE, \"C2\" CHAR(10))");

        st.executeUpdate(" CREATE TABLE \"APP\".\"T3\" (\"I3\" INTEGER, "
                + "\"D3\" DOUBLE, \"C3\" CHAR(10))");

        st.executeUpdate(" insert into t1 values (1, -1, '1'), (2, -2, '2')");

        st.executeUpdate(" insert into t2 values (2, -2, '2'), (4, -4, '4'), "
                + "(8, -8, '8')");

        st.executeUpdate(" insert into t3 values (3, -3, '3'), (6, -6, '6'), "
                + "(9, -9, '9')");

        st.executeUpdate(" CREATE TABLE \"APP\".\"T4\" (\"C4\" CHAR(10))");

        st.executeUpdate(" insert into t4 values '1', '2', '3', '4', '5', "
                + "'6', '7', '8', '9'");

        st
                .executeUpdate(" insert into t4 select rtrim(c4) || rtrim(c4) from t4");

        st.executeUpdate(" CREATE TABLE \"APP\".\"T5\" (\"I5\" INTEGER, "
                + "\"D5\" DOUBLE, \"C5\" CHAR(10))");

        st.executeUpdate(" CREATE TABLE \"APP\".\"T6\" (\"I6\" INTEGER, "
                + "\"D6\" DOUBLE, \"C6\" CHAR(10))");

        st.executeUpdate(" insert into t5 values (100, 100.0, '100'), (200, "
                + "200.0, '200'), (300, 300.0, '300')");

        st.executeUpdate(" insert into t6 values (400, 400.0, '400'), (200, "
                + "200.0, '200'), (300, 300.0, '300')");

        st.executeUpdate(" create view v_keycol_at_pos_3 as select distinct i "
                + "col1, d col2, c col3 from t1");

        st.executeUpdate(" create view v1_keycol_at_pos_2 as select distinct "
                + "i2 col1, c2 col3, d2 col2 from t2");

        st.executeUpdate(" create view v2_keycol_at_pos_2 as select distinct "
                + "i3 col1, c3 col3, d3 col2 from t3");

        st.executeUpdate(" create view v1_intersect as select distinct i5 "
                + "col1, c5 col3, d5 col2 from t5");

        st.executeUpdate(" create view v2_intersect as select distinct i6 "
                + "col1, c6 col3, d6 col2 from t6");

        st.executeUpdate(" create view v1_values as select distinct vals1 "
                + "col1, vals2 col2, vals3 col3 from (values (321, "
                + "321.0, '321'), (432, 432.0, '432'), (654, 654.0, "
                + "'654') ) VT(vals1, vals2, vals3)");

        st.executeUpdate(" create view v_union as select distinct i col1, d "
                + "col2, c col3 from t1 union select distinct i3 col1, "
                + "d3 col2, c3 col3 from t3");

        // Chain of UNIONs with left-most child as a view with a
        // an RCL that is ordered differently than that of the
        // UNIONs above it. The right child of the top-level node
        // is a view that is a simple select from a table.

        st
                .executeUpdate("create view topview as (select distinct 'other:' "
                        + "col0, vpos3.col3, vpos3.col1 from v_keycol_at_pos_3 "
                        + "vpos3 union select distinct 't2stuff:' col0, "
                        + "vpos2_1.col3, vpos2_1.col1 from v1_keycol_at_pos_2 "
                        + "vpos2_1 union select distinct 't3stuff:' col0, "
                        + "vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2 )");

        // Chain of UNIONs with left-most child as a view with a
        // an RCL that is ordered differently than that of the
        // UNIONs above it. The right child of the top-level node
        // is a view that is a select from yet another UNION node.

        st.executeUpdate("create view topview2 as (select distinct 'other:' "
                + "col0, vpos3.col3, vpos3.col1 from v_keycol_at_pos_3 "
                + "vpos3 union select distinct 't2stuff:' col0, "
                + "vpos2_1.col3, vpos2_1.col1 from v1_keycol_at_pos_2 "
                + "vpos2_1 union select distinct 't3stuff:' col0, "
                + "vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 "
                + "vpos2_2 union select distinct 'morestuff:' col0, "
                + "vu.col3, vu.col1 from v_union vu )");

        // Chain of UNIONs with left-most child as a view with a
        // an RCL that is ordered differently than that of the
        // UNIONs above it. The left-most child of the last UNION
        // in the chain is an INTERSECT node to which predicates
        // cannot (currently) be pushed. In this case the
        // intersect returns an empty result set.

        st.executeUpdate("create view topview3 (col0, col3, col1) as (select "
                + "distinct 'other:' col0, vpos3.col3, vpos3.col1 from "
                + "v_keycol_at_pos_3 vpos3 intersect select distinct "
                + "'t2stuff:' col0, vpos2_1.col3, vpos2_1.col1 from "
                + "v1_keycol_at_pos_2 vpos2_1 union select distinct "
                + "'t3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from "
                + "v2_keycol_at_pos_2 vpos2_2 union select distinct "
                + "'morestuff:' col0, vu.col3, vu.col1 from v_union vu )");

        // Chain of UNIONs with left-most child as a view with a
        // an RCL that is ordered differently than that of the
        // UNIONs above it. The left-most child of the last UNION
        // in the chain is an INTERSECT node to which predicates
        // cannot (currently) be pushed. In this case the
        // intersect returns a couple of rows.

        st.executeUpdate("create view topview4 (col0, col3, col1) as (select "
                + "distinct 'intersect:' col0, vi1.col3, vi1.col1 from "
                + "v1_intersect vi1 intersect select distinct "
                + "'intersect:' col0, vi2.col3, vi2.col1 from "
                + "v2_intersect vi2 union select distinct 't3stuff:' "
                + "col0, vpos2_2.col3, vpos2_2.col1 from "
                + "v2_keycol_at_pos_2 vpos2_2 union select distinct "
                + "'morestuff:' col0, vu.col3, vu.col1 from v_union vu )");

        // Chain of UNIONs with left-most child as a view with a
        // an RCL that is ordered differently than that of the
        // UNIONs above it. The left-most child of the last UNION
        // in the chain is a view that is a selet from a VALUES
        // list (i.e. no base table).

        st.executeUpdate("create view topview5 (col0, col3, col1) as (select "
                + "distinct 'values:' col0, vv1.col3, vv1.col1 from "
                + "v1_values vv1 union select distinct 'intersect:' "
                + "col0, vi2.col3, vi2.col1 from v2_intersect vi2 "
                + "union select distinct 't3stuff:' col0, "
                + "vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 "
                + "vpos2_2 union select distinct 'morestuff:' col0, "
                + "vu.col3, vu.col1 from v_union vu )");

        // All of the following queries failed at some point while
        // finalizing the fix for DERBY-1633; some failed with
        // error 42818, others failed with execution-time NPEs
        // caused by incorrect (esp. double) remapping. The point
        // here is to see how the top-level predicates are pushed
        // through the nested unions to the bottom-most children.
        // Use of LEFT JOINs with NESTEDLOOP effectively allows us
        // to force the join order and thus to ensure the
        // predicates are pushed to the desired top-level at
        // execution time. All such queries are run once with
        // NESTEDLOOP and once without, to make sure things work in
        // both cases.

        rs = st
                .executeQuery("select * from t4, topview where t4.c4 = topview.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "4", "t2stuff:", "4", "4" }, { "8", "t2stuff:", "8", "8" },
                { "3", "t3stuff:", "3", "3" }, { "6", "t3stuff:", "6", "6" },
                { "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from t4, topview where topview.col3 = t4.c4");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "4", "t2stuff:", "4", "4" }, { "8", "t2stuff:", "8", "8" },
                { "3", "t3stuff:", "3", "3" }, { "6", "t3stuff:", "6", "6" },
                { "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1, topview where "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from t4, topview2 where t4.c4 = topview2.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "morestuff:", "1", "1" },
                { "2", "morestuff:", "2", "2" },
                { "3", "morestuff:", "3", "3" },
                { "6", "morestuff:", "6", "6" },
                { "9", "morestuff:", "9", "9" }, { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "4", "t2stuff:", "4", "4" }, { "8", "t2stuff:", "8", "8" },
                { "3", "t3stuff:", "3", "3" }, { "6", "t3stuff:", "6", "6" },
                { "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview2 x1, topview where "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "morestuff:", "1", "1", "other:", "1", "1" },
                { "morestuff:", "2", "2", "other:", "2", "2" },
                { "morestuff:", "2", "2", "t2stuff:", "2", "2" },
                { "morestuff:", "3", "3", "t3stuff:", "3", "3" },
                { "morestuff:", "6", "6", "t3stuff:", "6", "6" },
                { "morestuff:", "9", "9", "t3stuff:", "9", "9" },
                { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from t4 left join topview on t4.c4 = topview.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "t3stuff:", "3", "3" }, { "4", "t2stuff:", "4", "4" },
                { "5", null, null, null }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "t3stuff:", "9", "9" }, { "11", null, null, null },
                { "22", null, null, null }, { "33", null, null, null },
                { "44", null, null, null }, { "55", null, null, null },
                { "66", null, null, null }, { "77", null, null, null },
                { "88", null, null, null }, { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from t4 left join topview "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t4.c4 "
                + "= topview.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "t3stuff:", "3", "3" }, { "4", "t2stuff:", "4", "4" },
                { "5", null, null, null }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "t3stuff:", "9", "9" }, { "11", null, null, null },
                { "22", null, null, null }, { "33", null, null, null },
                { "44", null, null, null }, { "55", null, null, null },
                { "66", null, null, null }, { "77", null, null, null },
                { "88", null, null, null }, { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st
                .executeQuery(" select * from t4 left join topview on topview.col3 = t4.c4");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "t3stuff:", "3", "3" }, { "4", "t2stuff:", "4", "4" },
                { "5", null, null, null }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "t3stuff:", "9", "9" }, { "11", null, null, null },
                { "22", null, null, null }, { "33", null, null, null },
                { "44", null, null, null }, { "55", null, null, null },
                { "66", null, null, null }, { "77", null, null, null },
                { "88", null, null, null }, { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from t4 left join topview "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview.col3 = t4.c4");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "other:", "1", "1" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "t3stuff:", "3", "3" }, { "4", "t2stuff:", "4", "4" },
                { "5", null, null, null }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "t3stuff:", "9", "9" }, { "11", null, null, null },
                { "22", null, null, null }, { "33", null, null, null },
                { "44", null, null, null }, { "55", null, null, null },
                { "66", null, null, null }, { "77", null, null, null },
                { "88", null, null, null }, { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview on "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from t4 left join topview2 on t4.c4 = "
                + "topview2.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "morestuff:", "1", "1" },
                { "1", "other:", "1", "1" }, { "2", "morestuff:", "2", "2" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "morestuff:", "3", "3" }, { "3", "t3stuff:", "3", "3" },
                { "4", "t2stuff:", "4", "4" }, { "5", null, null, null },
                { "6", "morestuff:", "6", "6" }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "morestuff:", "9", "9" }, { "9", "t3stuff:", "9", "9" },
                { "11", null, null, null }, { "22", null, null, null },
                { "33", null, null, null }, { "44", null, null, null },
                { "55", null, null, null }, { "66", null, null, null },
                { "77", null, null, null }, { "88", null, null, null },
                { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from t4 left join topview2 "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on t4.c4 "
                + "= topview2.col3");

        expColNames = new String[] { "C4", "COL0", "COL3", "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "morestuff:", "1", "1" },
                { "1", "other:", "1", "1" }, { "2", "morestuff:", "2", "2" },
                { "2", "other:", "2", "2" }, { "2", "t2stuff:", "2", "2" },
                { "3", "morestuff:", "3", "3" }, { "3", "t3stuff:", "3", "3" },
                { "4", "t2stuff:", "4", "4" }, { "5", null, null, null },
                { "6", "morestuff:", "6", "6" }, { "6", "t3stuff:", "6", "6" },
                { "7", null, null, null }, { "8", "t2stuff:", "8", "8" },
                { "9", "morestuff:", "9", "9" }, { "9", "t3stuff:", "9", "9" },
                { "11", null, null, null }, { "22", null, null, null },
                { "33", null, null, null }, { "44", null, null, null },
                { "55", null, null, null }, { "66", null, null, null },
                { "77", null, null, null }, { "88", null, null, null },
                { "99", null, null, null } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview2 x1 left join topview on "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "morestuff:", "1", "1", "other:", "1", "1" },
                { "morestuff:", "2", "2", "other:", "2", "2" },
                { "morestuff:", "2", "2", "t2stuff:", "2", "2" },
                { "morestuff:", "3", "3", "t3stuff:", "3", "3" },
                { "morestuff:", "6", "6", "t3stuff:", "6", "6" },
                { "morestuff:", "9", "9", "t3stuff:", "9", "9" },
                { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview2 x1 left join topview "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "morestuff:", "1", "1", "other:", "1", "1" },
                { "morestuff:", "2", "2", "other:", "2", "2" },
                { "morestuff:", "2", "2", "t2stuff:", "2", "2" },
                { "morestuff:", "3", "3", "t3stuff:", "3", "3" },
                { "morestuff:", "6", "6", "t3stuff:", "6", "6" },
                { "morestuff:", "9", "9", "t3stuff:", "9", "9" },
                { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview2 on "
                + "topview2.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview2 "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview2.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "1", "1", "other:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "other:", "2", "2", "other:", "2", "2" },
                { "other:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "other:", "2", "2" },
                { "t2stuff:", "2", "2", "t2stuff:", "2", "2" },
                { "t2stuff:", "4", "4", "t2stuff:", "4", "4" },
                { "t2stuff:", "8", "8", "t2stuff:", "8", "8" },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview3 on "
                + "topview3.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview3 "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview3.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview4 on "
                + "topview4.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview4 "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview4.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview5 on "
                + "topview5.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from topview x1 left join topview5 "
                + "--DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n on "
                + "topview5.col3 = x1.col3");

        expColNames = new String[] { "COL0", "COL3", "COL1", "COL0", "COL3",
                "COL1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] {
                { "other:", "1", "1", "morestuff:", "1", "1" },
                { "other:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "2", "2", "morestuff:", "2", "2" },
                { "t2stuff:", "4", "4", null, null, null },
                { "t2stuff:", "8", "8", null, null, null },
                { "t3stuff:", "3", "3", "morestuff:", "3", "3" },
                { "t3stuff:", "3", "3", "t3stuff:", "3", "3" },
                { "t3stuff:", "6", "6", "morestuff:", "6", "6" },
                { "t3stuff:", "6", "6", "t3stuff:", "6", "6" },
                { "t3stuff:", "9", "9", "morestuff:", "9", "9" },
                { "t3stuff:", "9", "9", "t3stuff:", "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // DERBY-1681: In order to reproduce the issue described
        // in DERBY-1681 we have to have a minimum amount of data
        // in the tables if we have too little, then somehow that
        // affects the plan and we won't see the incorrect results.

        st.executeUpdate("insert into t1 select * from t2");

        st.executeUpdate(" insert into t2 select * from t3");

        st.executeUpdate(" insert into t3 select * from t1");

        st.executeUpdate(" insert into t1 select * from t2");

        st.executeUpdate(" insert into t2 select * from t3");

        st.executeUpdate(" insert into t3 select * from t1");

        st.executeUpdate(" insert into t1 select * from t2");

        st.executeUpdate(" insert into t2 select * from t3");

        st.executeUpdate(" insert into t3 select * from t1");

        st.executeUpdate(" insert into t1 select * from t2");

        st.executeUpdate(" insert into t2 select * from t3");

        st.executeUpdate(" insert into t3 select * from t1");

        // Now can just run one of the queries from DERBY-1633 to
        // test the fix. Before DERBY-1681 this query would return
        // 84 rows and it was clear that the predicate wasn't being
        // enforced after the fix, we should only see 42 rows and
        // for every row the first and second column should be equal.

        rs = st
                .executeQuery("select topview4.col3, x1.col3 from topview x1 left "
                        + "join topview4 on topview4.col3 = x1.col3");

        expColNames = new String[] { "COL3", "COL3" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "1", "1" }, { "1", "1" }, { "2", "2" },
                { "2", "2" }, { "3", "3" }, { "3", "3" }, { "4", "4" },
                { "4", "4" }, { "6", "6" }, { "6", "6" }, { "8", "8" },
                { "8", "8" }, { "9", "9" }, { "9", "9" }, { "1", "1" },
                { "1", "1" }, { "2", "2" }, { "2", "2" }, { "3", "3" },
                { "3", "3" }, { "4", "4" }, { "4", "4" }, { "6", "6" },
                { "6", "6" }, { "8", "8" }, { "8", "8" }, { "9", "9" },
                { "9", "9" }, { "1", "1" }, { "1", "1" }, { "2", "2" },
                { "2", "2" }, { "3", "3" }, { "3", "3" }, { "4", "4" },
                { "4", "4" }, { "6", "6" }, { "6", "6" }, { "8", "8" },
                { "8", "8" }, { "9", "9" }, { "9", "9" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Clean-up from DERBY-1633 and DERBY-1681.

        st.executeUpdate("drop view topview");

        st.executeUpdate(" drop view topview2");

        st.executeUpdate(" drop view topview3");

        st.executeUpdate(" drop view topview4");

        st.executeUpdate(" drop view topview5");

        st.executeUpdate(" drop view v_keycol_at_pos_3");

        st.executeUpdate(" drop view v1_keycol_at_pos_2");

        st.executeUpdate(" drop view v2_keycol_at_pos_2");

        st.executeUpdate(" drop view v1_intersect");

        st.executeUpdate(" drop view v2_intersect");

        st.executeUpdate(" drop view v1_values");

        st.executeUpdate(" drop view v_union");

        st.executeUpdate(" drop table t1");

        st.executeUpdate(" drop table t2");

        st.executeUpdate(" drop table t3");

        st.executeUpdate(" drop table t4");

        st.executeUpdate(" drop table t5");

        st.executeUpdate(" drop table t6");

        getConnection().rollback();
        st.close();
    }
    
    /**
     * Tries to determine the if  the VM we're running in is 32 or 64 bit by 
     * looking at the system properties.
     *
     * @return true if 64 bit
     */
    private static boolean is64BitJVM() {
        // Try the direct way first, by looking for 'sun.arch.data.model'
        String dataModel = getSystemProperty("sun.arch.data.model");
        try {
            if (new Integer(dataModel).intValue() == 64)
                return true;
            else 
                return false;
        } catch (NumberFormatException ignoreNFE) {}

        // Try 'os.arch'
        String arch = getSystemProperty("os.arch");
        // See if we recognize the property value.
        if (arch != null) {
            // Is it a known 32 bit architecture?
            String[] b32 = new String[] {"i386", "x86", "sparc"};
            if (Arrays.asList(b32).contains(arch)) return false;
            // Is it a known 64 bit architecture?
            String[] b64 = new String[] {"amd64", "x86_64", "sparcv9"};
            if (Arrays.asList(b64).contains(arch)) return true;
        }

        // Didn't find out anything.
        BaseTestCase.traceit("Bitness undetermined, sun.arch.data.model='" +
                    dataModel + "', os.arch='" + arch + "', assuming we're 32 bit");
        // we don't know, assume it's 32 bit.
        return false;
    }
}

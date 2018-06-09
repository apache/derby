/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.QueryPlanTest
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that the optimizer chooses the expected query plans for specific
 * queries.
 */
public class QueryPlanTest extends BaseJDBCTestCase {
    public QueryPlanTest(String name) {
        super(name);
    }

    public static Test suite() {
        // Set derby.optimizer.noTimeout to prevent the optimizer from
        // timing out and returning an unexpected plan on slower machines.
        // Run in embedded mode only, since we're only interested in testing
        // functionality in the engine.
        Properties sysprops = new Properties();
        sysprops.setProperty("derby.optimizer.noTimeout", "true");
        return new CleanDatabaseTestSetup(new SystemPropertyTestSetup(
                TestConfiguration.embeddedSuite(QueryPlanTest.class),
                sysprops, true));
    }

    /**
     * Test that we prefer unique indexes if we have equality predicates for
     * the full key, even when the table is empty or almost empty. Although
     * it doesn't matter much for performance when the table is almost empty,
     * using a unique index will most likely need fewer locks and allow more
     * concurrency.
     */
    public void testDerby6011PreferUniqueIndex() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        // Create tables/indexes like the ones used by the Apache ManifoldCF
        // test that had concurrency problems (deadlocks).
        s.execute("CREATE TABLE jobs(id BIGINT PRIMARY KEY)");
        s.execute("CREATE TABLE jobqueue(docpriority FLOAT, "
                + "id BIGINT PRIMARY KEY, priorityset BIGINT, "
                + "docid CLOB NOT NULL, failcount BIGINT, "
                + "status CHAR(1) NOT NULL, dochash VARCHAR(40) NOT NULL, "
                + "isseed CHAR(1), checktime BIGINT, checkaction CHAR(1), "
                + "jobid BIGINT NOT NULL CONSTRAINT jobs_fk REFERENCES jobs, "
                + "failtime BIGINT)");
        s.execute("CREATE UNIQUE INDEX DOCHASH_JOBID_IDX ON "
                + "jobqueue(dochash, jobid)");

        // Enable collection of runtime statistics.
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        // Execute the query that caused problems and verify that it used
        // the unique index. It used to pick a plan that used the JOBS_FK
        // foreign key constraint.
        PreparedStatement ps = prepareStatement(
                "SELECT id,status,checktime FROM jobqueue "
                + "WHERE dochash=? AND jobid=? FOR UPDATE");
        ps.setString(1, "");
        ps.setInt(2, 0);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("JOBQUEUE", "DOCHASH_JOBID_IDX");

        // Check that the optimizer picks the unique index if there is a
        // column that has both a unique and a non-unique index. It used to
        // pick the non-unique index.
        s.execute("create table t1(a int not null, b int not null, " +
                  "c int not null, d int not null, e blob)");
        s.execute("create index idx_t1_a on t1(a)");
        s.execute("create unique index uidx_t1_a on t1(a)");
        ps = prepareStatement("select * from t1 where a = ?");
        ps.setInt(1, 1);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("T1", "UIDX_T1_A");

        // Check that a unique index is preferred also for indexes with
        // two and three columns. Used to pick a non-unique index.
        s.execute("drop index uidx_t1_a");
        s.execute("create index idx_t1_ab on t1(a,b)");
        s.execute("create unique index uidx_t1_ab on t1(a,b)");
        ps = prepareStatement("select * from t1 where a = ? and b = ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("T1", "UIDX_T1_AB");

        s.execute("drop index uidx_t1_ab");
        s.execute("create index idx_t1_abc on t1(a,b,c)");
        s.execute("create unique index uidx_t1_abc on t1(a,b,c)");
        ps = prepareStatement(
                "select * from t1 where a = ? and b = ? and c = ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.setInt(3, 3);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("T1", "UIDX_T1_ABC");

        // The optimizer should prefer a four-column unique to a three-column
        // non-unique index for the query below. Used to pick the three-column
        // non-unique index.
        s.execute("drop index uidx_t1_abc");
        s.execute("create unique index uidx_t1_abcd on t1(a,b,c,d)");
        ps = prepareStatement(
                "select * from t1 where a = ? and b = ? and c = ? and d = ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.setInt(3, 3);
        ps.setInt(4, 4);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("T1", "UIDX_T1_ABCD");

        // Given a covering non-unique index and a non-covering unique index,
        // we want the covering index to be picked. The optimizer used to pick
        // the covering index before the fix. Verify that it still does.
        s.execute("create table t2(a varchar(200) not null, "
                + "b varchar(200) not null, c varchar(200) not null)");
        s.execute("create unique index uidx_t2_ab on t1(a,b)");
        s.execute("create index idx_t2_abc on t2(a,b,c)");
        ps = prepareStatement(
                "select * from t2 where a = ? and b = ? and c = ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.setInt(3, 3);
        JDBC.assertEmpty(ps.executeQuery());
        assertIndex("T2", "IDX_T2_ABC");
    }

    /**
     * Assert that a specific index was used for accessing a table.
     * @param table the table being accessed
     * @param index the index that should be used
     */
    private void assertIndex(String table, String index)
            throws SQLException {
        RuntimeStatisticsParser parser =
                SQLUtilities.getRuntimeStatisticsParser(createStatement());
        if (!parser.usedSpecificIndexForIndexScan(table, index)) {
            fail("Should have used index " + index + " when accessing table " +
                 table + ". Actual plan:\n" + parser);
        }
    }
}

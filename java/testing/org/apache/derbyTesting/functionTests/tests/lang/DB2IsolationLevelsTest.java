/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DB2IsolationLevelsTest
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;


public final class DB2IsolationLevelsTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public DB2IsolationLevelsTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("DB2IsolationLevelsTest Test");

        suite.addTest(TestConfiguration.defaultSuite(
                DB2IsolationLevelsTest.class));
        return suite;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create a table
        createStatement().executeUpdate(
            "create table t1(c1 int not null constraint asdf primary key)");
    }

    @Override
    public void tearDown() throws Exception {
        createStatement().executeUpdate("drop table t1");
        super.tearDown();
    }

    public void test_DB2IsolationLevelsTest() throws Exception
    {
        final Statement st = createStatement();
        final PreparedStatement ps =
                prepareStatement("values current isolation");

        setAutoCommit(false);
        st.executeUpdate("call syscs_util.syscs_set_runtimestatistics(1)");
        st.executeUpdate("insert into t1 values 1");

        /* -----------------------------------------------------------------
         * Session isolation default: (CS) read committed
         */
        assertQueryResult(st.executeQuery("select * from t1"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "read committed isolation level using instantaneous share " +
            "row locking chosen by the optimizer");
        assertIsolation(ps, "CS");

        /* -----------------------------------------------------------------
         * Session isolation: RR (serializable)
         */
        st.executeUpdate("set isolation RR");
        assertIsolation(ps, "RR");
        // Rollback should find nothing to do
        rollback();

        assertQueryResult(st.executeQuery("select * from t1"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "serializable isolation level using share table locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Set isolation back to default
         */
        st.executeUpdate("set isolation reset");
        assertIsolation(ps, " ");

        /* -----------------------------------------------------------------
         * Session isolation: CS (read committed)
         */
        st.executeUpdate("set isolation read committed");
        assertIsolation(ps, "CS");
        // rollback should find nothing to undo
        rollback();

        assertQueryResult(st.executeQuery("select * from t1"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "read committed isolation level using instantaneous share " +
            "row locking chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Set isolation back to default
         */
        st.executeUpdate("set isolation to reset");
        assertIsolation(ps, " ");

        /* -----------------------------------------------------------------
         * Session isolation: RS (read committed)
         */
        st.executeUpdate("set current isolation = RS");
        assertIsolation(ps, "RS");
        // rollback should find nothing to undo
        rollback();

        assertQueryResult(st.executeQuery("select * from t1"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "repeatable read isolation level using share row locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Set isolation back to default
         */
        st.executeUpdate("set isolation reset");
        assertIsolation(ps, " ");

        /* -----------------------------------------------------------------
         * Session isolation: UR (dirty read)
         */
        st.executeUpdate("set isolation = dirty read");
        assertIsolation(ps, "UR");
        // rollback should find nothing to undo
        rollback();

        assertQueryResult(st.executeQuery("select * from t1"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "read uncommitted isolation level using share row locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Session isolation: RR (serializable)
         */
        st.executeUpdate("set isolation serializable");
        assertIsolation(ps, "RR");

        /*
         * Override session serializable (RR) with read committed (CS)
         * on statement level
         */
        assertQueryResult(st.executeQuery("select * from t1 with CS"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "read committed isolation level using instantaneous share " +
            "row locking chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Session isolation: CS (read committed)
         */
        st.executeUpdate("set isolation cursor stability");
        assertIsolation(ps, "CS");

        /*
         * Override session read committed (CS) with serializable (RR)
         * on statement level
         */
        assertQueryResult(st.executeQuery("select * from t1 with RR"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "serializable isolation level using share table locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Session isolation: RR (serializable)
         */
        st.executeUpdate("set isolation serializable");
        assertIsolation(ps, "RR");
        /*
         * Override session RR (serializable) with repeatable read (RS)
         * on statement level
         */
        assertQueryResult(st.executeQuery("select * from t1 with RS"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "repeatable read isolation level using share row locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Session isolation: CS (read committed)
         */
        st.executeUpdate("set current isolation to read committed");
        assertIsolation(ps, "CS");
        /*
         * Override session CS (read committed) with UR (read uncommitted)
         * on statement level
         */
        assertQueryResult(st.executeQuery("select * from t1 with ur"));
        assertPlan(
            st,
            "Index Scan ResultSet for T1 using constraint ASDF at " +
            "read uncommitted isolation level using share row locking " +
            "chosen by the optimizer");

        /* -----------------------------------------------------------------
         * Unknown isolation levels: expect syntax errors
         */
        final String e = "42X01"; // syntax error
        assertStatementError(e, st, "select * from t1 with rw");
        assertStatementError(e, st, "select * from t1 with dirty read");
        assertStatementError(e, st, "select * from t1 with read uncommitted");
        assertStatementError(e, st, "select * from t1 with read committed");
        assertStatementError(e, st, "select * from t1 with cursor stability");
        assertStatementError(e, st, "select * from t1 with repeatable read");
        assertStatementError(e, st, "select * from t1 with serializable");

        /* -----------------------------------------------------------------
         * Check the db2 isolation levels can be used as identifiers
         */
        st.executeUpdate("create table db2iso(cs int, rr int, ur int, rs int)");
        ResultSet rs = st.executeQuery("select cs, rr, ur, rs from db2iso");
        JDBC.assertEmpty(rs);
        rollback();
    }

    private void assertQueryResult(ResultSet rs) throws SQLException {
        JDBC.assertColumnNames(rs, new String [] {"C1"});
        JDBC.assertFullResultSet(rs, new String [][]{{"1"}}, true);
        rs.close();
    }

    private void assertPlan(Statement s, String expected)
            throws SQLException {
        SQLUtilities.getRuntimeStatisticsParser(s).
            assertSequence(new String[]{expected});
    }

    private void assertIsolation(PreparedStatement p, String expected)
            throws SQLException {
        ResultSet rs = p.executeQuery();
        JDBC.assertColumnNames(rs, new String [] {"1"});
        JDBC.assertFullResultSet(rs, new String [][]{{expected}}, true);
        rs.close();
    }
}

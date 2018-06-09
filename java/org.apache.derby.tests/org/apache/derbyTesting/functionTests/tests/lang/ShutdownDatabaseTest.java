/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ShutdownDatabaseTest

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

/**
 * This test confirm that no trouble happens when database , of which active
 * connection exists with , was shut down.
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;

public class ShutdownDatabaseTest extends BaseJDBCTestCase {
    /**
     * Public constructor required for running test as standalone JUnit.
     */

    public ShutdownDatabaseTest(String name) {
        super(name);
    }

    /**
     * Create a suite of tests.
     */
    public static Test suite() {
        // Only run in embedded as running in client/server
        // hits a problem. See DERBY-2477. To see the bug
        // just use the defaultSuite.
        return new CleanDatabaseTestSetup(new BaseTestSuite(
            ShutdownDatabaseTest.class, "ShutdownDatabaseTest"));
    }

    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    protected void setUp() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table " + "TEST_TABLE "
                + "( TEST_COL integer )");
        commit();
        insertIntoTestTable(1, 1000);

    }

    /**
     * to make sure all tables and databases are dropped
     */
    protected void tearDown() throws Exception {
        Statement st = createStatement();
        st.execute("DROP TABLE TEST_TABLE");
        st.close();
        commit();
        super.tearDown();
    }

    /**
     * Tests shutdown with the only transaction was committed.
     */
    public void testOnlyTransactionWasCommited() throws SQLException {
        commit();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "1000");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was committed, and next transaction was committed.
     */
    public void testTwiceCommited() throws SQLException {

        commit();
        insertIntoTestTable(1001, 999);
        commit();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "1999");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was rollbacked, and next transaction was commited.
     */
    public void testOnceRollbackedAndCommited() throws SQLException {

        rollback();
        insertIntoTestTable(1001, 999);
        commit();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "999");
        st.close();
    }

    /**
     * Tests shutdown with the only transaction was rollbacked.
     */
    public void testOnlyTransactionWasRollbacked() throws SQLException {

        rollback();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "0");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was commited, and next transaction was rollbacked.
     */
    public void testOnceCommitedAndRollbacked() throws SQLException {

        commit();
        insertIntoTestTable(1001, 999);
        rollback();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "1000");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was rollbacked, and next transaction was rollbacked.
     */
    public void testTwiceRollbacked() throws SQLException {

        rollback();
        insertIntoTestTable(1001, 999);
        rollback();
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "0");
        st.close();
    }

    /**
     * Tests shutdown with the only transaction was not committed/rollbacked.
     */
    public void testOnlyTransactionWasLeft() throws SQLException {

        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "0");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was committed, and next transaction was not
     * committed/rollbacked yet.
     */
    public void testOnceCommitedAndLeft() throws SQLException {

        commit();
        insertIntoTestTable(1001, 999);
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "1000");
        st.close();
    }

    /**
     * Tests shutdown with the transaction was rollbacked, and next transaction was not
     * committed/rollbacked yet.
     */

    public void testOnceRollbackedAndLeft() throws SQLException {

        rollback();
        insertIntoTestTable(1001, 999);
        assertShutdownOK();
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("select " + "count(*) "
                + "from " + "TEST_TABLE "), "0");
        st.close();
    }

    protected void insertIntoTestTable(int val) throws SQLException {

        PreparedStatement st = null;

        try {
            st = prepareStatement("insert into " + "TEST_TABLE "
                    + "( TEST_COL ) " + "values( ? )");
            st.setInt(1, val);
            st.execute();

        } finally {
            if (st != null) {
                st.close();
                st = null;
            }
        }
    }

    private void insertIntoTestTable(int initialval, int count)
            throws SQLException {

        for (int i = initialval; i < initialval + count; i++) {

            insertIntoTestTable(i);

        }

    }

    protected void assertShutdownOK() throws SQLException {

        Connection conn = getConnection();

        if (usingEmbedded()) {
            DataSource ds = JDBCDataSource.getDataSource();
            JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
            assertTrue(conn.isClosed());
        } else if (usingDerbyNetClient()) {
            ClientDataSource ds = (ClientDataSource) JDBCDataSource
                    .getDataSource();
            ds.setConnectionAttributes("shutdown=true");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
        }

        
    }
}

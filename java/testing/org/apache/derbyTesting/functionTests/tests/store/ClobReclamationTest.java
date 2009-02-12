/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.ClobReclamationTest
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
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Verify that space gets reclaimed for multi-threaded Clob updates
 * 
 */
public class ClobReclamationTest extends BaseJDBCTestCase {

    // Need to adjust NUM_THREADS and expectedNumAllocated.
    // For 2 threads expectedNumAllocated is 5
    // For 100 threads expectedNumAllocated is 201
    private static final int NUM_THREADS = 2;

    private static int expectedNumAllocated = 5;

    public ClobReclamationTest(String name) {
        super(name);

    }

    /**
     * Two threads simultaneously updating a table. Thread 1 updates row 1 with
     * a long value (>32K) Thread 2 updates row with a short clob ("hello");
     * NUMALLOCATEDPAGES should be only 3 after each does 500 updates
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void testMultiThreadedUpdate(final boolean lockTable) throws SQLException,
            InterruptedException {
        // need to do a getConnection or we get a
        // junit assertion that driver is not registered.
        Connection conn = getConnection();
        final String updateString = Formatters.repeatChar("a", 33000);
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final int key = i + 1;
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Connection conn = openDefaultConnection();
                        conn.setAutoCommit(false);
                        ClobReclamationTest.fiveHundredUpdates(conn,
                                updateString, key, lockTable);                      
                    } catch (SQLException e) {
                        fail(e.getMessage());
                    }
                }
            };
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }

        Statement s = createStatement();
        // Check the space table 
        // Should not have grown.
        ResultSet rs = s.executeQuery("SELECT NUMALLOCATEDPAGES FROM "
                + " new org.apache.derby.diag.SpaceTable('APP','CLOBTAB') t"
                + " WHERE CONGLOMERATENAME = 'CLOBTAB'");
        JDBC.assertFullResultSet(rs, new String[][] { { ""
                + expectedNumAllocated } });
    }

    private static void fiveHundredUpdates(Connection conn,
            String updateString, int key, boolean lockTable) throws SQLException {
        PreparedStatement ps = conn
                .prepareStatement("UPDATE CLOBTAB SET C = ? WHERE I = ?");
        for (int i = 0; i < 500; i++) {
            if (lockTable) {
                Statement s = conn.createStatement();
                s.executeUpdate("LOCK TABLE CLOBTAB IN EXCLUSIVE MODE");
             }
            ps.setString(1, updateString);
            ps.setInt(2, key);
            ps.executeUpdate();
            conn.commit();
        }
    }

    /**
     * Test multithreaded clob update using standard row locking
     * @throws SQLException
     * @throws InterruptedException
     */
    public void testMultiThreadedUpdateRowLocking() throws SQLException, InterruptedException {
        testMultiThreadedUpdate(false);
    }
    
    /**
     * Test multithreaded clob update but get an exclusive lock on the
     * table for each update. We can't enable this teset until DERBY-4054 
     * is fixed.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void xtestMultiThreadedUpdateTableLocking() throws SQLException, InterruptedException {
        testMultiThreadedUpdate(true);
    }
    
    public static Test suite() {

        Properties sysProps = new Properties();
        sysProps.put("derby.debug.true", "DaemonTrace");
        Test suite = TestConfiguration.embeddedSuite(ClobReclamationTest.class);
        return new CleanDatabaseTestSetup(new SystemPropertyTestSetup(suite,
                sysProps, true)) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = s.getConnection();
                s
                        .executeUpdate("CREATE TABLE CLOBTAB (I INT  PRIMARY KEY NOT NULL, c CLOB)");
                PreparedStatement ps = conn
                        .prepareStatement("INSERT INTO CLOBTAB VALUES(?,?)");
                String insertString = "hello";
                for (int i = 1; i <= NUM_THREADS; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, insertString);
                    ps.executeUpdate();
                }
            }

        };

    }
}

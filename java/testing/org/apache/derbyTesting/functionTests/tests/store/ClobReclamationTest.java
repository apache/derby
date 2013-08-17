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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
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
     * Two threads simultaneously updating a table. Threads each
     * update a separate row with a long value (>32K). NUMALLOCATED
     * pages should not grow past expected value after 500 updates
     * by each thread.
     * 
     * @param lockTable true if we should get an exclusive lock on the table
     * before update
     * @param updateSingleRow true if we should try updating a single row 
     * instead of different rows
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void testMultiThreadedUpdate(final boolean lockTable, boolean updateSingleRow) throws SQLException,
            InterruptedException {
        // need to do a getConnection or we get a
        // junit assertion that driver is not registered.
        Connection conn = getConnection();
        final String updateString = Formatters.repeatChar("a", 33000);
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final int key = updateSingleRow ? 1 : i + 1;
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
        checkNumAllocatedPages("CLOBTAB",expectedNumAllocated);
    }

    /**
     * Check that table has specified number of allocated pages.
     * 
     * @param table
     * @param expectedAlloc
     * @throws SQLException
     */
    private void checkNumAllocatedPages(String table, int expectedAlloc) throws SQLException {        
        // Check the space table 
        // Should not have grown.

        PreparedStatement ps = prepareStatement("SELECT NUMALLOCATEDPAGES FROM "
                + " new org.apache.derby.diag.SpaceTable('APP',?) t"
                + " WHERE CONGLOMERATENAME = ?");
        ps.setString(1,table);
        ps.setString(2, table);
        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][] { { ""
                + expectedAlloc } });
    }

    /**
     * Check that table has specified number of free pages.
     *
     * @param table
     * @param expectedFree  expected number of free pages.
     * @throws SQLException
     */
    private void checkNumFreePages(
    String  table, 
    int     expectedFree) throws SQLException {        

        // Check the space table 
        // Should not have grown.

        PreparedStatement ps = 
            prepareStatement(
                  "SELECT NUMFREEPAGES FROM "
                + " new org.apache.derby.diag.SpaceTable('APP',?) t"
                + " WHERE CONGLOMERATENAME = ?");

        ps.setString(1, table);
        ps.setString(2, table);
        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][] { { "" + expectedFree } });
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
        testMultiThreadedUpdate(false /* don't lock table */, false /*don't update single row*/);
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
        testMultiThreadedUpdate(true /*lock table */, false /* don't update single row */ );
    }
    
    /**
     * Test multiThreaded update of single row to cause lock contention
     * This will trigger the row lock retry case of DERBY-4055
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void xtestMultiThreadUpdateSingleRow() throws SQLException, InterruptedException {
        testMultiThreadedUpdate(false /*don't lock table */, true /* update single row */ );
    }
    
    /**
     * Make sure we reclaim space on rollback. Cannot enable this test 
     * until DERBY-4057 is fixed.
     * 
     * @throws SQLException
     */
    public void xtestReclamationOnRollback() throws SQLException {
        setAutoCommit(false);
        String insertString = Formatters.repeatChar("a", 33000);
        PreparedStatement ps = prepareStatement("INSERT INTO CLOBTAB2 VALUES(?,?)");
        for (int i = 0; i < 500; i++) {            
            ps.setInt(1, i);
            ps.setString(2, insertString);   
            ps.executeUpdate();
            rollback();
        }
        checkNumAllocatedPages("CLOBTAB2",1);
    }

    /**
     * Test for DERBY-4182.  
     *
     * This test just exercises the abort specific part of DERBY-4182.  After
     * the fix abort of an insert containing a blob will leave the head row, 
     * plus the first page of the overflow chain.  The rest of the chain
     * will be moved to free pages.
     *
     * @throws SQLException
     */
    public void testBlobLinkedListReclamationOnRollback() throws SQLException {
        setAutoCommit(false);

        int clob_length = 200000;

        // pick a clob bigger than 2*max page size
        String insertString = Formatters.repeatChar("a", 200000);
        PreparedStatement ps = 
            prepareStatement("INSERT INTO CLOBTAB3 VALUES(?,?)");

        int numrows = 500;

        for (int i = 0; i < numrows; i++) {            
            ps.setInt(1, i);
            ps.setString(2, insertString);   
            ps.executeUpdate();
            rollback();
        }
        ps.close();

        // until DERBY-4057 fixed expect space to be 2 pages per row plus
        // 1 head page per container.
        checkNumAllocatedPages("CLOBTAB3", (numrows * 2) + 1);

        // expect most free pages to get used by subsequent inserts.  Only 
        // free pages should be from the last remaining aborted insert of
        // the last clob chain.  It should include all of the clob except the
        // head page of the chain: (sizeof(clob) / page size) - 1
        // Derby should default to 32k page size for any table with a clob in
        // it.
 
        // (clob length / page size ) + 
        //     1 page for int divide round off - 1 for the head page.
        checkNumFreePages("CLOBTAB3", (clob_length / 32000) + 1 - 1);
        commit();

        // running inplace compress should reclaim all the remaining aborted
        // insert space, previous to fix inplace compress would leave stranded
        // allocated pages that were part of the clob overflow chains.
        CallableStatement call_compress =
            prepareCall(
                "CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, 1, 1, 1)");

        call_compress.setString(1, "APP");
        call_compress.setString(2, "CLOBTAB3");
        call_compress.executeUpdate();

        // all space except for head page should be reclaimed.
        checkNumAllocatedPages("CLOBTAB3", 1);

        // should be no free pages after the inplace compress of a table with
        // all deleted rows.
        checkNumFreePages("CLOBTAB3", 0);
        commit();
    }
    
    public static Test suite() {
        Test suite = TestConfiguration.embeddedSuite(ClobReclamationTest.class);
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {
                if (SanityManager.DEBUG) {
                    SanityManager.DEBUG_SET("DaemonTrace");
                }

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
                s.executeUpdate("CREATE TABLE CLOBTAB2 (I INT, C CLOB)");                
                s.executeUpdate("CREATE TABLE CLOBTAB3 (I INT, C CLOB)");                
            }

            protected void tearDown() throws Exception {
                if (SanityManager.DEBUG) {
                    SanityManager.DEBUG_CLEAR("DaemonTrace");
                }
                super.tearDown();
            }

        };

    }
}

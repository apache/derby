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

import java.lang.Integer;

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
     * update a separate row with a long value (&gt;32K). NUMALLOCATED
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
        checkNumAllocatedPages("CLOBTAB",expectedNumAllocated, false);
    }

    /**
     * Check that table has specified number of allocated pages.
     * 
     * @param table
     * @param expectedAlloc
     * @throws SQLException
     */
    private void checkNumAllocatedPages(
    String  table, 
    int     expectedAlloc,
    boolean retry_with_sleeps) 
        throws SQLException {        
        // Check the space table 
        // Should not have grown.

        PreparedStatement ps = prepareStatement("SELECT NUMALLOCATEDPAGES FROM "
                + " new org.apache.derby.diag.SpaceTable('APP',?) t"
                + " WHERE CONGLOMERATENAME = ?");
        ps.setString(1, table);
        ps.setString(2, table);

        // initialize previous_alloc_count such that we will always try 
        // at least one sleep/retry.
        int previous_alloc_count = Integer.MAX_VALUE;

        int num_retries = 0;

        for(;;)
        {
            // loop until success, or until sleep/retry does not result
            // in less allocated pages.

            // get 1 row, it has the num allocated page count.
            ResultSet rs = ps.executeQuery();

            rs.next();

            int num_allocated_pages = rs.getInt(1);

            // first check if count is the expected value, if so done.
            if (num_allocated_pages == expectedAlloc)
            {
                // expected result is met, success
                break;
            }
            else if (retry_with_sleeps)
            {
                if (num_allocated_pages < previous_alloc_count)
                {
                    num_retries++;

                    // background thread made progress in last sleep,
                    // try sleeping again, wait longer as this machine
                    // seems to take longer to process the post
                    // commit work.  Most environments just need one
                    // short retry, while enviroments where this does 
                    // not work need much more time.  Make second retry
                    // much longer.
                    try 
                    {
                        if (num_retries <= 1)
                            Thread.sleep(10000);
                        else
                            Thread.sleep(60000 * num_retries);

                    }
                    catch (Exception ex)
                    {
                        // just ignore interrupted sleep
                    }
                }
                else
                {
                    // sleep has not found less alloc rows let the assert fail.
                    assertTrue(
                        "Fail with retries -- num_allocated_pages:" + 
                        num_allocated_pages + 
                        " == expectedAlloc: " + expectedAlloc +
                        " previous_alloc_count: " + previous_alloc_count +
                        " num_retries: " + num_retries, 
                        num_allocated_pages == expectedAlloc); 
                }

                // force at least 2 retries
                if (num_retries > 1)
                    previous_alloc_count = num_allocated_pages;
            }
            else
            {
                // no retries allowed, let assert fail.
                assertTrue(
                    "Fail, no retries -- num_allocated_pages:" + 
                    num_allocated_pages + 
                    " > expectedAlloc: " + expectedAlloc, 
                    num_allocated_pages == expectedAlloc); 
            }

            rs.close();
        } 
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

    private void checkNumFreePagesMax(
    String  table, 
    int     expectedFreeMax) throws SQLException {        

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

        // get 1 row, it has the num free page count and make sure it is
        // lower than the passed in maximum.
        rs.next();

        int numfreerows = rs.getInt(1);

        assertTrue(
            "Fail -- numfreerows:" + numfreerows + 
            " > expectedFreeMax: " + expectedFreeMax, 
            numfreerows < expectedFreeMax); 
        rs.close();
        ps.close();
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
    public void testReclamationOnRollback() throws SQLException {
        setAutoCommit(false);
        String insertString = Formatters.repeatChar("a", 33000);
        PreparedStatement ps = prepareStatement("INSERT INTO CLOBTAB2 VALUES(?,?)");
        for (int i = 0; i < 500; i++) {            
            ps.setInt(1, i);
            ps.setString(2, insertString);   
            ps.executeUpdate();
            rollback();
        }

        // sleep 5 seconds to give background space reclamation worker thread 
        // a chance to reclaim all the aborted insert space.
        
        try 
        {
            Thread.sleep(5000);
        }
        catch (Exception ex)
        {
            // just ignore interrupted sleep
        }

        checkNumAllocatedPages("CLOBTAB2",1,true);
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
        // 1 head page per container.  DERBY-4057 has been fixed so allocated
        // pages is showing up as 1 in my runs.  Am a bit worried that because
        // this is thread and post commit dependent that we may have to play
        // with the "expected" allocated a little to allow for post commit
        // on some machines not running fast enough.  For now just setting
        // to 1 as the head page will remain allocated, all the rest should
        // be free after post commit reclaim.
        checkNumAllocatedPages("CLOBTAB3", 1, true);

        // expect most free pages to get used by subsequent inserts.  Only 
        // free pages should be from the last remaining aborted insert of
        // the last clob chain.  It should include all of the clob except the
        // head page of the chain: (sizeof(clob) / page size) - 1
        // Derby should default to 32k page size for any table with a clob in
        // it.

        // after fix for DERBY-4057 work gets queued immediately after the 
        // rollback, but also the next insert happens at same time.  On
        // my machine I am getting 12 pages free which looks like space from
        // the first abort  gets used for the 3rd insert, space from second
        // abort gets used for the 4th insert ...  Running full test suite
        // on my machine got 14 free pages which is still very good considering
        // before change there were 1000 allocated pages and now there is 1
        // allocated and 14 free.  There is a timing 
        // issue with test and maybe a sleep of some sort is necessary after
        // the rollback.  

        // declaring correct run if only max of free pages from 20 rows worth
        // of free space remains.
        // ((clob length / page size ) * 20) + 
        //     1 page for int divide round off - 1 for the head page.

        // The above check that there is 1 allocated page shows that we
        // are reclaiming space.  The number of free pages left is very
        // machine dependent as it depends on speed of background thread 
        // reclaiming space and the above user loop doing the inserts 
        // concurrent with the background threads reclaiming the aborted
        // inserted space.

        // See DERBY-6775 for better test
        /*
        checkNumFreePagesMax("CLOBTAB3", ((clob_length / 32000) * 20) + 1 - 1);
        commit();
        */

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
        checkNumAllocatedPages("CLOBTAB3", 1, false);

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
                    SanityManager.DEBUG_SET("verbose_heap_post_commit");
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

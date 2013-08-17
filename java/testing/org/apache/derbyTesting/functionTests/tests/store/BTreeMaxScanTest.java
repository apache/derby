/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.BTreeMaxScanTest
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for max queries that scan a B-tree index backwards (DERBY-642).
 */
public class BTreeMaxScanTest extends BaseJDBCTestCase {
    /** List of SanityManager debug flags to reset on teardown. */
    private List<String> traceFlags = new ArrayList<String>();

    public BTreeMaxScanTest(String name) {
        super(name);
    }

    /**
     * Create a test suite with all the test cases in this class.
     */
    public static Test suite() {
        // This is a test for engine functionality, so skip client/server.
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(BTreeMaxScanTest.class));
    }

    /**
     * Tear down the test environment.
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        if (SanityManager.DEBUG) {
            for (String flag : traceFlags) {
                SanityManager.DEBUG_PRINT(
                        flag, "Disable tracing for " + getName());
                SanityManager.DEBUG_CLEAR(flag);
            }
        }
    }

    // TESTS

    /**
     * Test that a max scan which cannot immediately lock the rightmost row
     * in the index, restarts the scan when it wakes up to see if the row it
     * waited for is still the maximum row. Tests
     * BTreeMaxScan#positionAtStartPosition.
     */
    public void testRestartScanAfterWaitOnMaxRow() throws Exception {
        setAutoCommit(false);

        // Populate a table with test data
        Statement s = createStatement();
        s.execute("create table t(x int, y int)");

        PreparedStatement ins = prepareStatement("insert into t(x) values ?");
        for (int i = 1; i <= 800; i++) {
            ins.setInt(1, i);
            ins.executeUpdate();
        }

        s.execute("create index i on t(x)");

        commit();

        // Make sure the rightmost row in the index is locked
        s.execute("update t set y = 0 where x = 800");

        Connection c2 = openDefaultConnection();
        try {
            c2.setAutoCommit(false);
            Statement s2 = c2.createStatement();

            // In a different thread, in a different transaction, start a max
            // scan that will be blocked trying to lock the rightmost row.
            Result r = asyncGetSingleResult(s2,
                    "select max(x) from t --derby-properties index=i");

            // Give the other thread two seconds to start executing and hit
            // the lock.
            Thread.sleep(2000L);

            // Now insert a row with a higher value, so that the row the other
            // thread is waiting for is no longer the maximum row.
            ins.setInt(1, 801);
            ins.executeUpdate();

            // Commit, release locks, and allow the other thread to continue.
            commit();

            // Now we expect the other thread to be able to continue. It should
            // restart the scan because it couldn't lock the rightmost row, and
            // it should therefore see the newly inserted value 801.
            assertEquals("801", r.get());

        } finally {
            c2.rollback();
            c2.close();
        }

        dropTable("T");
        commit();
    }

    /**
     * Test that scanners that work in opposite directions don't deadlock. The
     * test case has two threads running B-tree forward scans in parallel with
     * two threads running B-tree (backward) max scans.
     */
    public void testOppositeScanDirections() throws Exception {
        // Trace latch conflicts to see which parts of the repositioning code
        // we exercise. This test case is supposed to test simple latch
        // conflicts between forward scanners and backward scanners, and should
        // result in "Couldn't get latch nowait, will retry" being written to
        // derby.log when latch conflicts occur.
        setTraceFlag("BTreeMaxScan.latchConflict");

        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t(x int)");

        // Insert a couple pages worth of rows, only the first 100 of them
        // being non-null. The null values makes the max scan need to scan
        // backwards across page boundaries to find a qualifying row.
        PreparedStatement ins = prepareStatement("insert into t values ?");
        final String[][] tableContents = new String[800][];
        for (int i = 1; i <= tableContents.length; i++) {
            String value = (i <= 100) ? Integer.toString(i) : null;
            ins.setString(1, value);
            ins.executeUpdate();
            tableContents[i - 1] = new String[] { value };
        }

        s.execute("create index i on t(x)");

        commit();

        // Now start four threads. Two scanning the B-tree in the forward
        // direction, and two scanning in the backward direction (max scans).
        // These threads should not interfere with each other.
        String forwardSQL = "select x from t --derby-properties index=i";
        String backwardSQL = "select max(x) from t --derby-properties index=i";

        final PreparedStatement[] pss = {
            openDefaultConnection().prepareStatement(forwardSQL),
            openDefaultConnection().prepareStatement(forwardSQL),
            openDefaultConnection().prepareStatement(backwardSQL),
            openDefaultConnection().prepareStatement(backwardSQL),
        };

        final Exception[] exceptions = new Exception[pss.length];

        final Thread[] threads = new Thread[pss.length];

        for (int i = 0; i < pss.length; i++) {
            final int threadNo = i;
            threads[i] = new Thread() {
                public void run() {
                    // The forward scan is expected to return all rows in
                    // the table, the backward (max) scan only the highest
                    // non-null row.
                    String[][] expected = (threadNo < 2) ?
                            tableContents : new String[][] {{"100"}};
                    try {
                        for (int j = 0; j < 1000; j++) {
                            ResultSet rs = pss[threadNo].executeQuery();
                            JDBC.assertFullResultSet(rs, expected);
                        }
                    } catch (Exception e) {
                        exceptions[threadNo] = e;
                    }
                }
            };
            threads[i].start();
        }

        for (int i = 0; i < pss.length; i++) {
            threads[i].join();
            pss[i].getConnection().close();
        }

        for (int i = 0; i < exceptions.length; i++) {
            if (exceptions[i] != null) {
                throw exceptions[i];
            }
        }

        dropTable("T");
        commit();
    }

    /**
     * <p>
     * Test that latch conflicts between forward scans and backward (max) scans
     * are resolved without deadlocking or other errors when the rightmost
     * leaf page of the B-tree is empty. In that case, the backward scan must
     * restart, since it doesn't have any saved position to return to.
     * </p>
     *
     * <p>
     * The test is performed by running two threads that scan the leaves of
     * the B-tree in the forward direction, while at the same time two threads
     * do a backward max scan on the same B-tree. In parallel with the threads
     * that scan the index, the main thread will repeatedly delete the rows
     * with the highest values in order to create a window where the scans may
     * see an empty page, sleep a little, and then re-insert the deleted rows.
     * </p>
     */
    public void testEmptyRightmostLeaf() throws Exception {
        // Trace latch conflicts to see that we exercise the code path that
        // handles repositioning after waiting for a latch when moving away
        // from an empty leaf at the far-right end of the B-tree. When this
        // code is exercised, we'll see "Restart scan from rightmost leaf"
        // printed to derby.log.
        setTraceFlag("BTreeMaxScan.latchConflict");

        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t(x int)");

        // Insert a couple pages worth of rows.
        PreparedStatement ins = prepareStatement("insert into t values ?");
        for (int i = 1; i <= 800; i++) {
            ins.setInt(1, i);
            ins.executeUpdate();
        }

        s.execute("create index i on t(x)");

        commit();

        // Now start four threads. Two scanning the B-tree in the forward
        // direction, and two scanning in the backward direction (max scans).
        // These threads should not interfere with each other.
        String forwardSQL = "select x from t --derby-properties index=i";
        String backwardSQL = "select max(x) from t --derby-properties index=i";

        final PreparedStatement[] pss = {
            openDefaultConnection().prepareStatement(forwardSQL),
            openDefaultConnection().prepareStatement(forwardSQL),
            openDefaultConnection().prepareStatement(backwardSQL),
            openDefaultConnection().prepareStatement(backwardSQL),
        };

        final Exception[] exceptions = new Exception[pss.length];

        final Thread[] threads = new Thread[pss.length];

        final AtomicInt threadCount = new AtomicInt();

        for (int i = 0; i < pss.length; i++) {
            final int threadNo = i;
            // Set the isolation level to read uncommitted so that the scans
            // don't take any locks. We do this because we want to test latch
            // conflicts, and if the scans take read locks, they'll spend most
            // of the time waiting for the write thread to release its locks.
            pss[i].getConnection().setTransactionIsolation(
                    Connection.TRANSACTION_READ_UNCOMMITTED);
            threads[i] = new Thread() {
                public void run() {
                    try {
                        for (int j = 0; j < 1000; j++) {
                            ResultSet rs = pss[threadNo].executeQuery();
                            if (threadNo < 2) {
                                // This is a full forward scan (SELECT *) of
                                // the B-tree, so expect it to see between 400
                                // and 800 rows.
                                int rowCount = JDBC.assertDrainResults(rs);
                                if (rowCount < 400 || rowCount > 800) {
                                    fail("Unexpected row count: " + rowCount);
                                }
                            } else {
                                // This is a max scan, so expect a single
                                // row that contains a value between 400 and
                                // 800.
                                assertTrue(rs.next());
                                int max = rs.getInt(1);
                                if (max < 400 || max > 800) {
                                    fail("Unexpected max value: " + max);
                                }
                                assertFalse(rs.next());
                                rs.close();
                            }
                        }
                    } catch (Exception e) {
                        exceptions[threadNo] = e;
                    } finally {
                        threadCount.decrement();
                    }
                }
            };
            threads[i].start();
            threadCount.increment();
        }

        // As long as the scanner threads are running, periodically delete
        // and re-insert the last 400 rows. This empties the rightmost leaf
        // page(s) and makes it possible for the scanners to encounter the
        // situation where they need to reposition after a latch conflict
        // while being positioned on an empty page with no saved position. The
        // post-commit worker will eventually remove the pointers to the empty
        // leaf, so we need to do this repeatedly and hope that the timing
        // will be right at least once so that we exercise the desired path.
        PreparedStatement deleteRows =
                prepareStatement("delete from t where x > 400");
        PreparedStatement insertRows =
                prepareStatement("insert into t select x+400 from t");
        while (threadCount.get() > 0) {
            // Delete rows in range [401, 800].
            assertEquals("deleted rows", 400, deleteRows.executeUpdate());
            commit();

            // Sleep a little while so that we don't fill the empty page
            // before the scanners have seen it.
            Thread.sleep(100L);

            // Re-insert rows in range [401, 800].
            assertEquals("inserted rows", 400, insertRows.executeUpdate());
            commit();
        }

        for (int i = 0; i < pss.length; i++) {
            threads[i].join();
            pss[i].getConnection().close();
        }

        for (int i = 0; i < exceptions.length; i++) {
            if (exceptions[i] != null) {
                throw exceptions[i];
            }
        }

        dropTable("T");
        commit();
    }

    /**
     * <p>
     * Test that B-tree max scans reposition correctly after waiting for a
     * lock on the last row and detect any new max value inserted while the
     * scan was waiting for the lock.
     * </p>
     *
     * <p>
     * <b>Note:</b> Currently, B-tree max scans always take a table lock when
     * running with serializable isolation level, so the scans in this test
     * case will not actually be blocked waiting for a row lock. The test case
     * is added to verify that the scans behave correctly if the lock mode is
     * changed in the future.
     * </p>
     */
    public void testSerializable() throws Exception {
        setAutoCommit(false);

        getConnection().
                setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Statement s = createStatement();
        s.execute("create table t(x int, y int)");
        s.execute("insert into t(x) values 0,1,2,3,4,null,null,null");
        s.execute("create index i on t(x)");

        PreparedStatement ps = prepareStatement(
                "select max(x) from t --derby-properties index=i");

        JDBC.assertSingleValueResultSet(ps.executeQuery(), "4");

        commit();

        // Set up another transaction that holds an exclusive lock on the
        // row with the max value.
        final Connection c2 = openDefaultConnection();
        final Statement s2 = c2.createStatement();
        c2.setAutoCommit(false);
        s2.execute("update t set y = x where x = 4");

        final Exception[] exception = new Exception[1];

        Thread t = new Thread() {
            public void run() {
                try {
                    // Wait a little while so that the main thread gets time
                    // to start the scan and get blocked by the lock on the
                    // highest row.
                    Thread.sleep(1000L);
                    // While the main thread is still blocked, insert a new
                    // max value.
                    s2.execute("insert into t(x) values 5");
                    // Commit, release the locks, and let the main thread
                    // continue.
                    c2.commit();
                } catch (Exception sqle) {
                    exception[0] = sqle;
                }
            }
        };

        t.start();

        // The two max scans should return the same value since they are
        // in the same transaction and the isolation level is serializable.
        // The first scan will be blocked by the lock held by the other
        // transaction. When it wakes up, it should see the newly inserted
        // row.
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "5");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "5");
        commit();

        t.join();
        s2.close();
        c2.rollback();
        c2.close();

        // Did the other thread fail?
        if (exception[0] != null) {
            throw exception[0];
        }

        dropTable("T");
        commit();
    }

    // HELPER METHODS

    /**
     * This class represents a result from an asynchronous operation.
     */
    private static class Result {
        private boolean complete;
        private Exception ex;
        private String value;

        synchronized void error(Exception ex) {
            this.ex = ex;
            this.complete = true;
            notifyAll();
        }

        synchronized void set(String value) {
            this.value = value;
            this.complete = true;
            notifyAll();
        }

        synchronized String get() throws Exception {
            while (!complete) {
                wait();
            }
            if (ex != null) {
                throw ex;
            } else {
                return value;
            }
        }
    }

    /**
     * Execute a statement asynchronously and return an object that can be
     * used to retrieve the result once the statement is complete. The
     * statement should return a single value.
     *
     * @param s the statement object to use for execution
     * @param sql the SQL to execute
     * @return a {@code Result} object that allows retrieval of the result
     * once it's available
     */
    private static Result asyncGetSingleResult(
            final Statement s, final String sql) {
        final Result result = new Result();

        Thread t = new Thread() {
            public void run() {
                try {
                    ResultSet rs = s.executeQuery(sql);
                    assertEquals("expected single value",
                            1, rs.getMetaData().getColumnCount());
                    assertTrue("empty result", rs.next());
                    String val = rs.getString(1);
                    assertFalse("multiple rows", rs.next());
                    rs.close();
                    result.set(val);
                } catch (Exception e) {
                    result.error(e);
                }
            }
        };

        t.start();

        return result;
    }

    /**
     * If running with a debug build and derby.tests.trace is true, enable
     * tracing for messages with the specified flag.
     *
     * @param flag the debug flag to enable
     */
    private void setTraceFlag(String flag) {
        if (SanityManager.DEBUG && TestConfiguration.getCurrent().doTrace()) {
            SanityManager.DEBUG_PRINT(flag, "Enable tracing for " + getName());
            SanityManager.DEBUG_SET(flag);
            traceFlags.add(flag);
        }
    }

    /**
     * Poor man's replacement for java.util.concurrent.atomic.AtomicInteger
     * that runs on platforms where java.util.concurrent isn't available.
     */
    private static class AtomicInt {
        private int i;
        synchronized void increment() {
            i++;
        }
        synchronized void decrement() {
            i--;
        }
        synchronized int get() {
            return i;
        }
    }
}

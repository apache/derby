/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DeadlockDetectionTest
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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.Barrier;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This test verifies that the deadlock detection algorithm is able to
 * recognize certain cycles in the wait graph as deadlocks.
 */
public class DeadlockDetectionTest extends BaseJDBCTestCase {

    /** SQLState for deadlock exceptions. */
    private final static String DEADLOCK = "40001";

    public static Test suite() {
        // Deadlock detection is engine functionality, so only test embedded.
        Test test =
                TestConfiguration.embeddedSuite(DeadlockDetectionTest.class);

        // Reduce the deadlock timeout since this test expects deadlocks, and
        // we want to detect them quickly in order to reduce the test time.
        // We don't expect any wait timeouts, so set the wait timeout
        // sufficiently high to prevent that queries time out before we have
        // set up the deadlock on slow machines.
        test = DatabasePropertyTestSetup.setLockTimeouts(test, 1, 30);

        return new CleanDatabaseTestSetup(test);
    }

    public DeadlockDetectionTest(String name) {
        super(name);
    }

    /**
     * Test case to verify the fix for DERBY-3980. A simple deadlock was not
     * detected, and was reported as a lock timeout.
     */
    public void testDerby3980_repeatable_read() throws Exception {
        Statement s = createStatement();
        s.executeUpdate("create table derby3980 (i int)");
        s.executeUpdate("insert into derby3980 values 1956, 180, 456, 3");

        // Set up two threads.
        Thread[] threads = new Thread[2];
        Connection[] conns = new Connection[threads.length];

        // This barrier lets the two threads wait for each other so that both
        // can obtain a read lock before going on trying to obtain the write
        // lock. If one thread goes ahead and obtains the write lock before the
        // other thread has obtained the read lock, we won't see a deadlock.
        final Barrier readLockBarrier = new Barrier(threads.length);

        // Exceptions seen by the threads.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        final List<Exception> exceptions =
                Collections.synchronizedList(new ArrayList<Exception>());

        // Start the two threads. Both should first obtain a read lock, and
        // when both have the read lock, they should try to lock the same row
        // exclusively. They'll be blocking each other, and we have a deadlock.
        for (int i = 0; i < threads.length; i++) {
            final Connection c = openDefaultConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);

            final PreparedStatement select = c.prepareStatement(
                    "select * from derby3980 where i = 456");
            final PreparedStatement update = c.prepareStatement(
                    "update derby3980 set i = 456 where i = 456");

            threads[i] = new Thread() {
                public void run() {
                    try {
                        JDBC.assertSingleValueResultSet(
                                select.executeQuery(), "456");

                        // Now we've got the read lock. Wait until all threads
                        // have it before attempting to get the write lock.
                        readLockBarrier.await();

                        // All threads have the read lock. Now all should try
                        // to update the row and thereby create a deadlock.
                        assertUpdateCount(update, 1);

                        // We got the write lock too. End the transaction.
                        c.rollback();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            };

            conns[i] = c;
            threads[i].start();
        }

        // Threads have started, wait for them to complete.
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
            conns[i].rollback();
            conns[i].close();
        }

        // Verify that we only got deadlock exceptions.
        for (Exception e : exceptions) {
            if (e instanceof SQLException) {
                assertSQLState(DEADLOCK, (SQLException) e);
            } else {
                // What's this? Report it.
                throw e;
            }
        }

        // And we should only get one exception. (One transaction should be
        // picked as victim, the other one should be able to complete.)
        assertEquals("Number of victims", 1, exceptions.size());
    }

    /**
     * Test case for DERBY-5073. A deadlock involving three transactions was
     * not reported when there were other transactions waiting for the same
     * locks. The deadlock was detected, and a victim chosen. But the victim
     * would recheck the deadlock and conclude that it wasn't part of it, and
     * it would pick a new victim that would also recheck and come to the same
     * conclusion. This would go on until the wait timeout had expired, and
     * an exception would be throws, although not a deadlock.
     */
    public void testDerby5073_dodgy_victims() throws Exception {
        Statement s = createStatement();
        s.executeUpdate("create table derby5073(x int primary key, y int)");
        s.executeUpdate("insert into derby5073(x) values 0, 1, 2");

        // We want six connections. Three that are involved in the deadlock,
        // and three that try to obtain locks on the same rows without
        // actually being part of the deadlock.
        Connection[] conns = new Connection[6];
        Thread[] threads = new Thread[conns.length];
        for (int i = 0; i < conns.length; i++) {
            conns[i] = openDefaultConnection();
            conns[i].setAutoCommit(false);
        }

        // Three transactions take an exclusive lock on one row each.
        for (int i = 3; i < 6; i++) {
            PreparedStatement ps = conns[i].prepareStatement(
                    "update derby5073 set y = x where x = ?");
            ps.setInt(1, i % 3);
            assertUpdateCount(ps, 1);
        }

        // Then try to lock the rows in three other transactions and in the
        // three transactions that already have locked the rows exclusively.
        // The transactions that have exclusive locks should attempt to lock
        // another row than the one they already have locked, otherwise there
        // will be no deadlock.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        final List<Exception> exceptions =
                Collections.synchronizedList(new ArrayList<Exception>());
        for (int i = 0; i < threads.length; i++) {
            final PreparedStatement ps = conns[i].prepareStatement(
                    "select x from derby5073 where x = ?");

            // Which row to lock. Add one to the thread number to make sure
            // that the threads don't attempt to lock the same row that they
            // already have locked above.
            final int row = (i + 1) % 3;
            ps.setInt(1, row);

            // The query will have to wait, so execute it in a separate thread.
            threads[i] = new Thread() {
                public void run() {
                    try {
                        JDBC.assertSingleValueResultSet(
                                ps.executeQuery(), Integer.toString(row));
                        ps.getConnection().commit();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            };

            threads[i].start();

            // The bug is only seen if the first three threads are already
            // waiting for the locks when the last three threads (those
            // involved in the deadlock) start waiting. So take a little nap
            // here after we've started the third thread (index 2) to allow
            // the first three threads to enter the waiting state.
            if (i == 2) Thread.sleep(100L);
        }

        // Wait for all threads to finish.
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
            conns[i].rollback();
            conns[i].close();
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (Exception e : exceptions) {
            if (e instanceof SQLException) {
                assertSQLState(DEADLOCK, (SQLException) e);
            } else {
                // What's this? Report it.
                throw e;
            }
        }

        // And we should only get one exception. (One transaction should be
        // picked as victim, the other ones should be able to complete.)
        assertEquals("Number of victims", 1, exceptions.size());
    }
}

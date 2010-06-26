/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.engine.LockInterruptTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that other threads are able to proceed if one thread is interrupted
 * when it's waiting for a lock. Regression test case for DERBY-4711.
 */
public class LockInterruptTest extends BaseJDBCTestCase {
    /** SQLState for exception thrown because of interrupts. */
    private final static String INTERRUPTED = "08000";

    /** Lock timeout in seconds used in this test. */
    private final static int LOCK_TIMEOUT = 60;

    /** Deadlock timeout in seconds used in this test. */
    private final static int DEADLOCK_TIMEOUT = LOCK_TIMEOUT / 2;

    public LockInterruptTest(String name) {
        super(name);
    }

    public static Test suite() {
        // Only run in embedded mode since we cannot interrupt the engine
        // thread from the network client.
        Test test = TestConfiguration.embeddedSuite(LockInterruptTest.class);

        // Set the lock timeout to a known value so that we know what to
        // expect for timeouts.
        test = DatabasePropertyTestSetup.setLockTimeouts(
                test, DEADLOCK_TIMEOUT, LOCK_TIMEOUT);

        return new CleanDatabaseTestSetup(test);
    }

    public void testInterruptLockWaiter() throws Exception {
        setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("create table derby4711(x int)");
        commit();

        // Obtain a table lock in order to block the waiter threads.
        s.executeUpdate("lock table derby4711 in share mode");

        // Create first waiter thread.
        Waiter t1 = new Waiter();
        t1.start();
        Thread.sleep(2000); // give t1 time to become the first waiter

        // Create second waiter thread.
        Waiter t2 = new Waiter();
        t2.start();
        Thread.sleep(2000); // give t2 time to enter the wait queue

        // Now that the queue of waiters has been set up, interrupt the
        // first thread.
        t1.interrupt();

        // Release the table lock to allow the waiters to proceed.
        commit();

        // Wait for the threads to complete before checking their state.
        t1.join();
        t2.join();

        // The first thread should fail because it was interrupted.
        Throwable e1 = t1.throwable;
        assertNotNull("First thread should fail because of interrupt", e1);
        if (!(e1 instanceof SQLException)) {
            fail("Unexpected exception from first thread", e1);
        }
        assertSQLState(INTERRUPTED, (SQLException) e1);

        // The second thread should be able to complete successfully.
        Throwable e2 = t2.throwable;
        if (e2 != null) {
            fail("Unexpected exception from second thread", e2);
        }

        // And the second thread should be able to complete in less time than
        // the deadlock timeout (before DERBY-4711, it would wait for a
        // timeout before obtaining the lock, even if the lock was available
        // long before).
        if (t2.elapsedTime >= DEADLOCK_TIMEOUT * 1000) {
            fail("Second thread needed " + t2.elapsedTime +
                 " ms to complete. Probably stuck waiting for a lock.");
        }

        // Expect that the second thread managed to insert a row.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from derby4711"), "1");
    }

    /**
     * Thread class that opens a new connection and attempts to insert a
     * row into a table.
     */
    private class Waiter extends Thread {
        private final Connection c;
        private final PreparedStatement ps;

        private Throwable throwable;
        private long elapsedTime;

        private Waiter() throws SQLException {
            c = openDefaultConnection();
            ps = c.prepareStatement("insert into derby4711 values 1");
        }

        public void run() {
            try {
                runWaiter();
            } catch (Throwable t) {
                throwable = t;
            }
        };

        private void runWaiter() throws SQLException {
            long start = System.currentTimeMillis();
            try {
                ps.executeUpdate();
            } finally {
                ps.close();
                c.close();
            }
            elapsedTime = System.currentTimeMillis() - start;
        }
    }
}

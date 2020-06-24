/*
  Class org.apache.derbyTesting.functionTests.tests.store.Derby4676Test

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Regression test for DERBY-4676.
 */
public class Derby4676Test extends BaseJDBCTestCase {
    /** List of {@code HelperThread}s used in the test. */
    private List<HelperThread> threads;

    public Derby4676Test(String name) {
        super(name);
    }

    /** Create a suite of tests. */
    public static Test suite() {
        return TestConfiguration.defaultSuite(Derby4676Test.class);
    }

    /** Set up the test environment. */
    protected void setUp() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        threads = new ArrayList<HelperThread>();
    }

    /** Tear down the test environment. */
    protected void tearDown() throws Exception {
        super.tearDown();

        List<HelperThread> localThreads = threads;
        threads = null;

        // First, wait for all threads to terminate and close all connections.
        for (int i = 0; i < localThreads.size(); i++) {
            HelperThread t = localThreads.get(i);
            t.join();
            Connection c = t.conn;
            if (c != null && !c.isClosed()) {
                c.rollback();
                c.close();
            }
        }

        // Then check if any of the helper threads failed.
        for (int i = 0; i < localThreads.size(); i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            HelperThread t = localThreads.get(i);
            if (t.exception != null) {
                fail("Helper thread failed", t.exception);
            }
        }
    }

    /**
     * <p>
     * Regression test case for DERBY-4676. Before the fix, fetching a row by
     * its row location would sometimes fail with a NullPointerException if
     * the row was deleted while the fetch operation was waiting for a lock.
     * </p>
     */
    public void testConcurrentFetchAndDelete() throws Exception {
        // Create a table to use in the test. Note that we need to have a
        // non-covering index on the table so that the row location is fetched
        // from the index and used to look up the row in the heap. If the
        // index covers all the columns, we won't fetch the row location from
        // it and the bug won't be reproduced.
        Statement s = createStatement();
        s.execute("create table t(x int, y int)");
        s.execute("create index idx on t(x)");

        // Create a thread that repeatedly inserts and deletes a row.
        HelperThread thread = new HelperThread() {
            void body(Connection conn) throws Exception {
                Thread.sleep(1000); // Wait for the select loop to start so
                                    // that the insert/delete loop doesn't
                                    // complete before it has started.
                Statement s = conn.createStatement();
                for (int i = 0; i < 1000; i++) {
                    s.execute("insert into t values (1,2)");
                    s.execute("delete from t");
                }
                s.close();
            }
        };

        startThread(thread);

        // As long as the insert/delete thread is running, try to read the
        // rows of the table using the index. This used to cause intermittent
        // NullPointerExceptions.
        while (thread.isAlive()) {
            JDBC.assertDrainResults(s.executeQuery(
                "select * from t --derby-properties index=idx"));
        }
    }

    /**
     * Helper class for running database operations in a separate thread and
     * in a separate transaction.
     */
    private abstract class HelperThread extends Thread {
        Exception exception;
        Connection conn;

        public void run() {
            try {
                conn = openDefaultConnection();
                body(conn);
            } catch (Exception ex) {
                exception = ex;
            }
        }

        abstract void body(Connection conn) throws Exception;
    }

    /**
     * Start a helper thread and register it for automatic clean-up in
     * {@link #tearDown()}.
     *
     * @param thread the helper thread to start
     */
    private void startThread(HelperThread thread) {
        thread.start();
        threads.add(thread);
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.storetests.st_derby715

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

package org.apache.derbyTesting.functionTests.tests.storetests;

import java.sql.Connection;
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

The purpose of this test is to reproduce JIRA DERBY-715:

Sometimes a deadlock would be incorrectly reported as a timeout.  The
bug seemed to always reproduce at least once if the following test
was run (at least one of the iterations in the loop would get an
incorrect timeout vs. a deadlock).

**/

public class st_derby715 extends BaseJDBCTestCase {
    private Barrier barrier;
    private List<Throwable> errors;

    public st_derby715(String name) {
        super(name);
    }

    public static Test suite() {
        Test test = TestConfiguration.embeddedSuite(st_derby715.class);
        test = DatabasePropertyTestSetup.setLockTimeouts(test, 1, 60);
        test = new CleanDatabaseTestSetup(test);
        return test;
    }

    @Override
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * Run two threads, where thread 1 first reads from table A and then
     * inserts a row into table B, and thread 2 first reads from table B
     * and then inserts a row into table A. This should cause a deadlock
     * in one of the threads. Before DERBY-715, sometimes a timeout would
     * be raised instead of a deadlock.
     */
    public void test_st_derby715() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table a (a integer)");
        stmt.executeUpdate("create table b (b integer)");
        stmt.close();
        commit();

        Connection c1 = openDefaultConnection();
        Connection c2 = openDefaultConnection();
        Statement stmt1 = c1.createStatement();
        Statement stmt2 = c2.createStatement();

        // Run the test five times.
        for (int i = 0; i < 5; i++) {
            barrier = new Barrier(2);
            errors = Collections.synchronizedList(new ArrayList<Throwable>());
            Thread test1 = new WorkerThread(stmt1, "Thread 1", "a", "b");
            Thread test2 = new WorkerThread(stmt2, "Thread 2", "b", "a");
            test1.start();
            test2.start();
            test1.join();
            test2.join();

            // We expect exactly one of the threads to fail, and that it
            // failed with a deadlock.

            assertFalse("Both threads succeeded", errors.isEmpty());

            if (errors.size() > 1) {
                for (Throwable t: errors) {
                    printStackTrace(t);
                }
                fail("Both threads failed");
            }

            Throwable t = errors.get(0);
            if (t instanceof SQLException) {
                assertSQLState("40001", (SQLException) t);
                println("Got expected deadlock: " + t);
            } else {
                fail("Unexpected exception", t);
            }
        }

        stmt1.close();
        stmt2.close();
    }

    @Override
    protected void tearDown() throws Exception {
        barrier = null;
        errors = null;
        super.tearDown();
    }

    private class WorkerThread extends Thread {
        private final Statement stmt;
        private final String id;
        private final String readTable;
        private final String writeTable;

        WorkerThread(Statement stmt, String id,
                     String readTable, String writeTable) {
            this.stmt = stmt;
            this.id = id;
            this.readTable = readTable;
            this.writeTable = writeTable;
        }

        @Override
        public void run() {
            try {
                _run();
            } catch (Throwable t) {
                errors.add(t);
            }
        }

        private void _run() throws SQLException, InterruptedException {
            println(id + " before selecting from " + readTable);
            JDBC.assertEmpty(stmt.executeQuery("select * from " + readTable));
            println(id + " after reading all rows");

            // Wait till the other thread has completed reading and is ready
            // to insert a row.
            barrier.await();

            println(id + " before inserting into " + writeTable);
            stmt.execute("insert into " + writeTable + " values (1)");
            println(id + " after inserting");

            stmt.getConnection().rollback();
        }
    }
}

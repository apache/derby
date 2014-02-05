/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.store.LockTableVtiTest

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
// Note: This test could be refined by modifying the BaseJDBCTestCase
//       method assertStatementError(new String[],Statement,String)
//       and all methods down that chain to search for the variable
//       values in the SQL error messages as well, in this case, in this
//       case, to check for 'exclusive' or 'share' in error X0202.

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.Barrier;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the printing of the WAIT state in the LOCK TABLE.
 */
public class LockTableVtiTest extends BaseJDBCTestCase {

    /**
     * List of threads (AsyncThread objects) to wait for after running the test.
     */
    private List<AsyncThread> threads = new ArrayList<AsyncThread>();
    
    public LockTableVtiTest(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     * The suite is wrapped in a DatabasePropertyTestSetup to set
     * the lock wait timeout.
     *
     * @return A suite containing embedded fixtures
     */
    public static Test suite() {
        Properties properties = new Properties();
        // setting to 60, which is the default, for starters
        properties.setProperty("derby.locks.waitTimeout", "60");

        Test suite = TestConfiguration.embeddedSuite (LockTableVtiTest.class);
        suite = new DatabasePropertyTestSetup(suite, properties, true);
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the schemas and table used in the test cases.
             *
             * @throws SQLException
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();
                conn.setAutoCommit(false);
                s.executeUpdate("create table account " +
                     "(a int primary key not null, b int)");
                s.executeUpdate("insert into account values (0,1)");
                s.executeUpdate("insert into account values (1,1)");
                s.executeUpdate("insert into account values (2,1)");
                conn.commit();
            }
        };
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Tear-down the fixture by removing the tables and schemas
     * @throws Exception
     */
    protected void tearDown() throws Exception {
        // Rollback all uncommitted operations so that we don't hold any
        // locks that may block the other threads.
        rollback();
        for (AsyncThread thread : threads) {
            thread.waitFor();
        }
        threads = null;

        // All the other threads have finished. Now, remove everything from
        // the APP schema so that we don't leave anything around for subsequent
        // tests.
        setAutoCommit(false); // required by JDBC.dropSchema()
        try {
            Statement s = createStatement();
            s.executeUpdate("drop table account");
            JDBC.dropSchema(getConnection().getMetaData(), "APP");
        } catch(SQLException sqe) {
            if (!(sqe.getSQLState().equalsIgnoreCase("42X05")
                    || sqe.getSQLState().equalsIgnoreCase("42Y55")))
            {
                fail("oops in teardown, encountered some other error than " +
                    "'object does not exist' or " +
                    "'cannot drop object because it doesn't exist'");
                sqe.printStackTrace();
                sqe.getMessage();
            }
        }
        super.tearDown();
    }

    /**
     * Tests to make sure that WAIT state is displayed in lock
     * table output
     * 
     * @exception Exception
     */
    public void testDisplayWaitState() throws Exception {
        Statement s = createStatement();
        setAutoCommit(false);
        // setting to -1 (wait for ever) to improve timing control
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
            "('derby.locks.waitTimeout','-1')");
        
        BaseJDBCTestCase.assertUpdateCount(
                s, 3, "update account set b = b + 11");
        
        // Object used for synchronization between the main thread and the
        // helper thread. The main thread uses it to tell the helper thread
        // that it has started the scan. The helper thread uses it
        // to tell the main thread that it is ready.
        // Both threads should wait until the other thread
        // has reached the barrier before continuing.
        final Barrier barrier = new Barrier(2);
        
        // start the second thread and make it do the same update
        new AsyncThread(new AsyncTask() {
            public void doWork(Connection conn) throws Exception {
                conn.setAutoCommit(false);
                Statement s = conn.createStatement();
                // note: asserts in this inner class do not make the test fail
                // so, just executing it here
                s.executeUpdate("update account set b = b + 11");
                s.close();
                
                // Tell the main thread that we've locked the row
                barrier.await();
                
                // The main thread now can continue - give it a
                // second to do its stuff
                //Thread.sleep(1000L);
                // we check that the 'wait' state is gone at the main thread,
                // it would not cause the test to fail if we checked it here.
            }
        });
        
        // now select from syscs_diag.lock_table, don't wait more than minute.
        int totalWait = 0;
        boolean found=false;
        do {
            totalWait += 500;
            Thread.sleep(500);
            // we want to look for 'WAIT' state. There will also
            // be one of more 'GRANT' state locks, likely background threads,
            // but we're not interested in those here.
            found=getWaitState();
        } while (!found && totalWait < 6000);
        // defer the assert until we've alerted the async thread
        // commit will release the lock
        commit();
        
        // set the timeout back so things can timeout.
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                "('derby.locks.waitTimeout','5')");
        commit();
        
        // Now that we've found the wait state, tell the helper thread we
        // are done
        barrier.await();
        
        // now that we've released the helper thread, we can safely let
        // the test fail if the results of the earlier check were bad
        assertTrue("expected to find a 'WAIT' state, but did not", found);
        
        // confirm that there is no 'wait' state now
        assertFalse("expected to not find a 'WAIT' state, but did", 
                getWaitState());
    }
    
    /**
     * See if there is a 'WAIT' lock
     * @return true if there was a WAIT lock, false if not
     * @throws SQLException
     */
    private boolean getWaitState() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT state from syscs_diag.lock_table " +
                "where state like 'WAIT' order by state");
        String result="";
        
        try {
            rs.next();
            result = rs.getString(1);
        } catch (Exception e) {
            // assume the exception is because there is no 'wait' state...
            //e.printStackTrace();
        }
        rs.close();
        if (result != null && result.length() > 0)
            return true;
        else
            return false;
    }
    
    /**
     * Interface that should be implemented by classes that define a
     * database task that is to be executed asynchronously in a separate
     * transaction.
     */
    private static interface AsyncTask {
        void doWork(Connection conn) throws Exception;
    }

    /**
     * Class that executes an {@code AsyncTask} object.
     */
    private class AsyncThread implements Runnable {

        private final Thread thread = new Thread(this);
        private final AsyncTask task;
        private Exception error;

        /**
         * Create an {@code AsyncThread} object and starts a thread executing
         * the task. Also put the {@code AsyncThread} object in the list of
         * threads in the parent object to make sure the thread is waited for
         * and its errors detected in the {@code tearDown()} method.
         *
         * @param task the task to perform
         */
        public AsyncThread(AsyncTask task) {
            this.task = task;
            thread.start();
            threads.add(this);
        }

        /**
         * Open a database connection and perform the task. Roll back the
         * transaction when finished. Any exception thrown will be caught and
         * rethrown when the {@code waitFor()} method is called.
         */
        public void run() {
            try {
                Connection conn = openDefaultConnection();
                try {
                    task.doWork(conn);
                } finally {
                    JDBC.cleanup(conn);
                }
            } catch (Exception e) {
                error = e;
            }
        }

        /**
         * Wait for the thread to complete. If an error was thrown during
         * execution, rethrow the execption here.
         * @throws Exception if an error happened while performing the task
         */
        void waitFor() throws Exception {
            thread.join();
            if (error != null) {
                throw error;
            }
        }
    }
}

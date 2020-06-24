/*

   Derby - Class org.apache.derbyTesting.functionsTests.tests.memorydb.DropWhileConnectingTest

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
package org.apache.derbyTesting.functionTests.tests.memorydb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

/**
 * <em>WARNING: This test isn't finalized!</em>
 *
 * Tests the behavior when several threads are accessing the database
 * and one thread comes in and drops the database.
 * The success of the test is defined by allowing only certain exceptions
 * from the access threads / connections. During connection, the following
 * exceptions are allowed:
 * <ul> <li>XJ004: Database not found.</li>
 *      <li>?????: Database access blocked.<li>
 * </ul>
 * During normal operation, the set is:
 * <ul>
 *      <li>XJ001*: Shutdown exception.</li>
 *      <li>08003: No current connection.</li>
 *  </ul>
 * <p>
 * This test has a known weakness in that it doesn't execute long-running
 * queries. It is not clear how these react when the database is dropped
 * under their feet.
 */
public class DropWhileConnectingTest
        extends BaseJDBCTestCase {

    public DropWhileConnectingTest(String name) {
        super(name);
    }

    public void testConcurrentAccessAndDrop()
            throws SQLException {
        final String url = "jdbc:derby:memory:testDB";
        // Database owner is the default user APP.
        Connection con =
                MemoryDbManager.getSharedInstance().createDatabase("testDB");
        con.close();
        String threadsStr = getSystemProperty("derby.tests.threadCount");
        if (threadsStr == null) {
            threadsStr = "20";
        }
        int accessThreads = Integer.parseInt(threadsStr);
        println("threadCount=" + accessThreads);
        Report report = new Report(getFailureFolder(), accessThreads);
        // Start the access threads.
        for (int i=0; i < accessThreads; i++) {
            Thread t = new Thread(new AccessThread(report, url));
            t.start();
        }
        // Signal start, then wait a little before deleting the database.
        report.start();
        sleep(2500);
        try {
            MemoryDbManager.getSharedInstance().dropDatabase("testDB");
            fail("Dropping database should have raised exception.");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }
        println("Drop database request executed.");
        // Wait for all the threads to finish (they may be sleeping).
        while (!report.allThreadsDone()) {
            println("Waiting for " + report.remainingThreads() +
                    " remaining thread(s) to finish...");
            sleep(500);
        }
        assertFalse(report.toString(), report.hasUnexpectedExceptions());
        println(report.toString());
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
       return new BaseTestSuite(DropWhileConnectingTest.class);
    }

    /**
     * Simple report class holding results from the test run. Also used to
     * control the start of the worker threads.
     */
    private static class Report {
        /** Sync object used to start the threads. */
        private final Object sync = new Object();
        //@GuardedBy("sync")
        private boolean ready;

        /** Failure folder where any exceptions will be logged to file. */
        private final File failureFolder;
        /** Writer used to log stack traces, lazily initialized. */
        private PrintWriter writer;
        /** The number of successful connections made by the worker threads. */
        private final int[] accessCounts;
        /** Any unexpected exceptions encountered by the worker threads. */
        private final Throwable[] exceptions;
        private int threadsDone;
        private boolean hasExceptions;
       
        /**
         * Creates a report object.
         *
         * @param failureFolder where to write exceptions to
         * @param accessThreads number of worker threads
         */
        public Report(File failureFolder, int accessThreads) {
            this.failureFolder = failureFolder;
            accessCounts = new int[accessThreads];
            exceptions = new Throwable[accessThreads];
        }

        public synchronized boolean hasUnexpectedExceptions() {
            return this.hasExceptions;
        }

        /**
         * Reports the access count for the specified worker thread.
         *
         * @param id worker thread id, must be in the range [0, threadCount&gt;
         * @param accessCount number of successful accesses made to the db
         */
        public synchronized void reportAccessCount(int id, int accessCount) {
            accessCounts[id] = accessCount;
            threadsDone++;
        }

        /**
         * Reports an unexpected error and the access count for the specified
         * worker thread.
         *
         * @param id worker thread id, must be in the range [0, threadCount&gt;
         * @param accessCount number of successful accesses made to the db
         * @param error error to report
         */
        public synchronized void reportError(int id, int accessCount,
                                             Throwable error) {
            reportAccessCount(id, accessCount);
            exceptions[id] = error;
            hasExceptions = true;
            // Print the stack trace to file.
            dumpToFile(id, error);
        }

        /**
         * Tells if all the access threads have finished.
         *
         * @return {@code true} if all threads have finished,
         *      {@code false} otherwise.
         */
        public synchronized boolean allThreadsDone() {
            return (threadsDone == accessCounts.length);
        }

        public synchronized int remainingThreads() {
            return accessCounts.length - threadsDone;
        }

        public Object getSync() {
            return this.sync;
        }

        /**
         * Tells if the test is ready to start.
         *
         * @return {@code true} if the access threads can start.
         */
        public boolean ready() {
            synchronized (sync) {
                return ready;
            }
        }

        public void start() {
            synchronized (sync) {
                ready = true;
                sync.notifyAll();
            }
        }

        public synchronized String toString() {
            int totalAccessCount = 0;
            for (int i=0; i < accessCounts.length; i++) {
                int c = accessCounts[i];
                if (c > 0) {
                    totalAccessCount += c;
                }
            }
            String sep = "\n";
            StringBuffer sb = new StringBuffer(sep +
                    "Number of access threads: ").append(accessCounts.length).
                    append(sep);
            sb.append("Access count: " + totalAccessCount).append(sep);
            if (hasExceptions) {
                sb.append("Exceptions (see " + failureFolder +
                        "/exceptions.log):" + sep);
                for (int i=0; i < exceptions.length; i++) {
                    Throwable t = exceptions[i];
                    if (t instanceof SQLException) {
                        SQLException s = (SQLException)t;
                        sb.append("id=").append(i).append(" : (").
                        append(s.getSQLState()).append(") ").
                        append(s.getMessage()).append(sep);
                    } else if (t != null) {
                        sb.append("id=").append(i).append(" : (     ) ").
                        append(t.getMessage()).append(sep);

                       
                    }
                }
            }
            return sb.toString();
        }

        /**
         * Dumps the exception to file.
         *
         * @param id internal id for the thread that got the exception
         * @param exception the exception to dump
         */
        private void dumpToFile(int id, Throwable exception) {
            if (writer == null) {
                try {
                    writer = new PrintWriter(
                            PrivilegedFileOpsForTests.getFileOutputStream(
                            new File(failureFolder, ("exceptions.log"))));
                    writer.println(new java.util.Date());
                } catch (IOException ioe) {
                    alarm("Failed to create exception log file: " +
                            ioe.getMessage());
                }
            }
            if (writer != null) {
                writer.println("-----");
                writer.println("id=" + id);
                writer.println("--");
                exception.printStackTrace(writer);
                writer.flush();
            }
        }
    }

    /**
     * Access thread connection to the database and performing a simple SQL
     * select query. Will accept a few specific exceptions as the database
     * is deleted "under its feet", all other exceptions are considered a
     * failure and will be reported.
     */
    private static class AccessThread
            implements Runnable {

        private static final Object LOCK = new Object();
        //@GuardedBy("LOCK")
        private static int idCounter = 0;
        /** Whether to wait at certain points in the execution. */
        private static final boolean noWait;
        static {
            String tmp = getSystemProperty("derby.tests.noWait");
            noWait = Boolean.valueOf(tmp).booleanValue();
            println("noWait=" + noWait);
        }

        private final int id;
        private final Report master;
        private final String url;
        private final Random rnd = new Random();
        /** Flag used to avoid waiting at multiple points in the execution. */
        private boolean waited;

        public AccessThread(Report master, String url) {
            synchronized (LOCK) {
                this.id = idCounter++;                
            }
            this.master = master;
            // Connect with a different user than the DBO. There is no real
            // reaon for doing this, other than making sure this user will
            // be blocked out by Derby during the shutdown.
            this.url = url + ";user=test;password=test";
        }

        public void run() {
            int access = 0;
            Connection con = null;
            // Wait for signal to start testing.
            while (!master.ready()) {
                synchronized (master.getSync()) {
                    try {
                        master.getSync().wait();
                    } catch (InterruptedException ie) {
                        // Ignore, just check the loop condition again.
                    }
                }
            }
            // Start accessing the database.
            try {
                while (true) {
                    waited = false;
                    try {
                        con = DriverManager.getConnection(url);
                        access++;
                    } catch (SQLException sqle) {
                        // See if the exception says database not found.
                        // An additional check would be to make sure this
                        // happens after the database has been dropped, and
                        // that it is not caused by a bug during boot.
                        if (sqle.getSQLState().equals("XJ004")) {
                            master.reportAccessCount(id, access);
                            break;
                        }
                        // TODO: Adjust SQLState or remove.
                        if (sqle.getSQLState().equals("XJ005")) {
                            // Attempt blocked, keep pounding on the database.
                            allowWait(false);
                            continue;
                        }
                        // The connection process failed unexpectedly.
                        throw sqle;
                    }
                    // The set of allowed exceptions here is different from
                    // the one during connection.
                    try {
                        Statement stmt = con.createStatement();
                        allowWait(true);
                        ResultSet rs = stmt.executeQuery(
                            "select * from sys.systables order by random()");
                        allowWait(true);
                        while (rs.next()) {
                            allowWait(true);
                            rs.getString(1);
                        }
                        rs.close();
                        stmt.close();
                        con.close();
                        allowWait(false);
                    } catch (SQLException sqle) {
                        // Accept no current connection here
                        if (sqle.getSQLState().equals("08003")) {
                            master.reportAccessCount(id, access);
                        } else if (sqle.getSQLState().equals("XJ001") &&
                                sqle.getMessage().indexOf("ShutdownException")
                                                                        != -1) {
                            master.reportAccessCount(id, access);
                        } else {
                            master.reportError(id, access, sqle);
                        }
                    }
                }
            } catch (Throwable t) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6824
                if (t instanceof org.apache.derby.shared.common.error.ShutdownException){
                    // Not sure if this is a good thing yet.
                    System.out.println(
                            "Got ShutdownException (extends RuntimeException)");
                    master.reportAccessCount(id, access);
                } else {
                    master.reportError(id, access, t);
                }
            }
        }

        /**
         * Method mostly doing nothing, but sometimes it decides to put the
         * thread to sleep for a little while.
         */
        private void allowWait(boolean onlyWaitOnce) {
            if (!noWait && ((!waited && onlyWaitOnce) || !onlyWaitOnce)) {
                int split = rnd.nextInt(100);
                if (split >= 97) {
                    // Potentially a long sleep
                    sleep(100 + (long)(rnd.nextDouble() * 1200));
                    waited = true;
                } else if (split > 80){
                    sleep((long)(rnd.nextDouble() * 100));
                    waited = true;
                }
            }
        }
    }
}

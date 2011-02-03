/*
  Class org.apache.derbyTesting.functionTests.tests.store.InterruptResilienceTest

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

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Random;
import java.lang.Math;
import java.util.Properties;

/**
 * Test started as a test reproduce and verify fix for DERBY-151.  Later
 * evolved into test for DERBY-4741.
 */

public class InterruptResilienceTest extends BaseJDBCTestCase
{

    public InterruptResilienceTest(String name)
    {
        super(name);
    }

    // Share the main thread's configuration with the server side threads.
    private static volatile TestConfiguration thisConf;

    protected static Test makeSuite(String name)
    {
        TestSuite suite = new TestSuite(name);

        Test est = TestConfiguration.embeddedSuite(
            InterruptResilienceTest.class);
        Test cst = TestConfiguration.clientServerSuite(
            InterruptResilienceTest.class);

        est = TestConfiguration.singleUseDatabaseDecorator(est);
        cst = TestConfiguration.singleUseDatabaseDecorator(cst);
        // Cut down on running time:
        Properties p = new Properties();
        p.put("derby.system.durability", "test");
        p.put("derby.infolog.append", "true");

        suite.addTest(
                new SystemPropertyTestSetup(est, p, true));

        suite.addTest(
                new SystemPropertyTestSetup(cst, p, true));
        return suite;
    }

    public static Test suite()
    {
        String testName = "InterruptResilienceTest";

        if (! isSunJVM()) {
            // DERBY-4463 test fails on IBM VMs. Remove this
            // exception when that issue is solved.
            println("Test skipped for this VM, cf. DERBY-4463");
            return new TestSuite(testName);
        }

        if (!JDBC.vmSupportsJDBC3()) {
            println("Test skipped for this VM, " +
                    "DriverManager is not supported with JSR169");
            return new TestSuite(testName);
        }

        if (hasInterruptibleIO()) {
            println("Test skipped due to interruptible IO.");
            println("This is default on Solaris/Sun Java <= 1.6, use " +
                    "-XX:-UseVMInterruptibleIO if available.");
            return new TestSuite(testName);
        }

        return makeSuite(testName);
    }

    protected void setUp()
            throws java.lang.Exception {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        } catch (Exception e) {
        }
        super.setUp();
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t1(x int primary key)");
        stmt.executeUpdate("create table mtTab(i bigint, " +
                           "inserter varchar(40), " +
                           "primary key(i, inserter))");
        stmt.close();

        thisConf = TestConfiguration.getCurrent();
        threadNo = 0;    // counter for multiple threads tests
        allDone = false; // flag for threads to terminate
    }

    /**
     * Clean up the connection maintained by this test.
     */
    protected void tearDown()
            throws java.lang.Exception {

        Statement stmt = createStatement();
        stmt.executeUpdate("drop table t1");
        stmt.executeUpdate("drop table mtTab");
        stmt.close();

        super.tearDown();
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correposnds to #testRAFWriteInterrupted.
    public static void tstRAFwriteInterrupted() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        c.setAutoCommit(false);
        PreparedStatement insert = null;
        long seen = 0;
        long lost = 0;
        try {
            insert = c.prepareStatement("insert into t1 values (?)");

            // About 75000 iterations is needed to see any concurrency
            // wait on RawDaemonThread during recovery, cf.
            // running with debug flag "RAF4" for RAFContainer4.
            for (int i = 0; i < 100000; i++) {
                if (i % 1000 == 0) {
                    c.commit();
                }

                // Make sure to interrupt after commit, since log writing isn't
                // safe for interrupts (on Solaris only) yet.
                Thread.currentThread().interrupt();

                insert.setLong(1, i);
                insert.executeUpdate();

                assertTrue("interrupt flag lost", Thread.interrupted());
            }
        } finally {
            // always clear flag
            Thread.interrupted();

            if (insert != null) {
                try {
                    insert.close(); // already closed by error
                } catch (SQLException e) {
                }
            }

            c.close();
        }
    }

    public void testRAFWriteInterrupted () throws SQLException {
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure tstRAFWriteInterrupted () modifies sql data " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.InterruptResilienceTest.tstRAFwriteInterrupted' " +
            "language java parameter style java");

        s.executeUpdate("call tstRAFWriteInterrupted()");
    }


    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testRAFReadWriteMultipleThreads.
    public static void tstRAFReadWriteMultipleThreads() throws Exception {

        //--------------------
        // part 1
        //--------------------

        Connection c = DriverManager.getConnection("jdbc:default:connection");

        ArrayList workers = new ArrayList();

        ArrayList interruptors = new ArrayList();

        for (int i = 0; i < NO_OF_THREADS; i++) {
            WorkerThread w = new WorkerThread(
                thisConf.openDefaultConnection(),
                false /* read */,
                NO_OF_MT_OPS);

            workers.add(w);

            w.start();
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }

            InterruptorThread it = new InterruptorThread(w, 500);
            interruptors.add(it);
            it.start();
        }

        for (int i = 0; i < workers.size(); i++) {
            WorkerThread w = (WorkerThread)workers.get(i);
            w.join();

            if (w.e != null) {
                fail("WorkerThread " + i, w.e);
            }
        }

        allDone = true;

        for (int i = 0; i < interruptors.size(); i++) {
            ((Thread)interruptors.get(i)).join();
        }

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from mtTab");

        JDBC.assertSingleValueResultSet(
            rs, Long.toString(NO_OF_THREADS * NO_OF_MT_OPS));

        //--------------------
        // part 2
        //--------------------

        // Reset thread state variables
        allDone = false;
        threadNo = 0;

        workers.clear();
        interruptors.clear();

        for (int i = 0; i < NO_OF_THREADS; i++) {
            WorkerThread w = new WorkerThread(
                // This will be an embedded connection always since for the
                // server thread current cf will be JUNIT_CONFIG.
                thisConf.openDefaultConnection(),
                true,
                NO_OF_MT_OPS);

            workers.add(w);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }

            InterruptorThread it = new InterruptorThread(w, 500);
            interruptors.add(it);
            it.start();
        }

        // Wait till here to start works, so interruptors don't get too late to
        // the game
        for (int i = 0; i < workers.size(); i++) {
            ((Thread)workers.get(i)).start();
        }

        for (int i = 0; i < workers.size(); i++) {
            WorkerThread w = (WorkerThread)workers.get(i);
            w.join();

            if (w.e != null) {
                fail("WorkerThread " + i, w.e);
            }
        }

        allDone = true;

        for (int i = 0; i < interruptors.size(); i++) {
            ((Thread)interruptors.get(i)).join();
        }

        c.close();
    }


    static class InterruptorThread extends Thread {
        private WorkerThread myVictim;
        private int millisBetweenShots;

        public InterruptorThread(WorkerThread v, int m){
            super();
            myVictim = v;
            millisBetweenShots = m;
        }

        public void run() {
            setName("InterruptorThread. Thread #" + getThreadNo());
            println("Running " + getName() +
                    " with victim " + myVictim.getName());

            int shots = 0;

            while (!allDone) {
                try {
                    Thread.sleep(millisBetweenShots);
                    myVictim.interrupt();
                    shots++;
                } catch (Exception e) {
                }
            }

            println(getName() + " shot " + shots +
                    " interrupts at " + myVictim.getName());
        }
    }


    static class WorkerThread extends Thread {
        private final boolean readertest;
        private final long noOps;
        public Exception e; // if any seen
        private Connection c;

        public WorkerThread(Connection c, boolean readertest, long noOps) {
            super();
            this.readertest = readertest;
            this.noOps = noOps;
            this.c = c;
        }

        public void run() {
            int threadNo = getThreadNo();
            int interruptsSeen = 0;

            setName("WorkerThread. Thread#" + threadNo);
            println("Running " + getName());

            try {
                c.setAutoCommit(false);

                String pStmtText =
                    readertest ?
                    "select * from mtTab where i=?" :
                    "insert into mtTab values (?,?)";
                PreparedStatement s = c.prepareStatement(pStmtText);

                Random rnd = new Random();

                for (long ops = 0; ops < noOps; ops++) {

                    if (readertest) {
                        // Arbitrarily select one of the rows int the tables to
                        // read
                        long candidate = randAbs(rnd.nextLong()) % noOps;
                        s.setLong(1, candidate);

                        // Since when we query, we might see 08000 if the
                        // interrupt flag is set when the rs.getNextRow calls
                        // checkCancellationFlag, we must be prepared to
                        // reestablish connection.

                        try {
                            ResultSet rs = s.executeQuery();
                            rs.next();
                            if (interrupted()) {
                                interruptsSeen++;
                            }

                            assertEquals("wrong row content",
                                         candidate, rs.getLong(1));

                            rs.close();
                        } catch (SQLException e) {
                            if ("08000".equals(e.getSQLState())) {
                                c = thisConf.openDefaultConnection();
                                s = c.prepareStatement(pStmtText);
                                assertTrue(interrupted());
                                interruptsSeen++;
                                continue;
                            } else {
                                throw new Exception("expected 08000, saw" + e);
                            }
                        }

                        c.commit();

                        if (interrupted()) {
                            interruptsSeen++;
                        }
                    } else {
                        s.setLong(1, ops);
                        s.setString(2, getName());
                        s.executeUpdate();

                        if (Thread.interrupted()) {
                            interruptsSeen++;
                        }

                        c.commit();

                        if (interrupted()) {
                            interruptsSeen++;
                        }
                    }
                }
                s.close();
            } catch (Exception e) {
                this.e = e;
            } finally {
                try { c.close(); } catch (Exception e) {}
            }

            println("Thread " + getName() + " saw " + interruptsSeen +
                    " interrupts");
        }
    }

    // Number of parallel threads to use
    static int NO_OF_THREADS = 3;

    static long NO_OF_MT_OPS = 10000;

    // Counter to enumerate threads for tests employing several threads.  Reset
    // for each test in setUp.
    private static int threadNo;

    synchronized static int getThreadNo() {
        return ++threadNo;
    }

    private static long randAbs(long l) {
        if (l == Long.MIN_VALUE) {
            return Long.MAX_VALUE; // 2's complement, so no way to make value
                                   // positive
        } else {
            return Math.abs(l);
        }
    }

    // Signal to threads to stop whatever they are doing. Reset
    // for each test in setUp.
    static volatile boolean allDone;

    /**
     * MT write (part 1) and read (part 2) test under interrupt shower.  This
     * stess tests the NIO random access file interrupt channel recovery in
     * RAFContainer4.
     */
    public void testRAFReadWriteMultipleThreads () throws SQLException {
        Statement s = createStatement();

        s.executeUpdate(
            "create procedure tstRAFReadWriteMultipleThreads () " +
            "modifies sql data " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstRAFReadWriteMultipleThreads' " +
            "language java parameter style java");

        s.executeUpdate("call tstRAFReadWriteMultipleThreads()");
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testLongQueryInterrupt
    public static void tstInterruptLongQuery() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();

        try {
            Thread.currentThread().interrupt();
            ResultSet rs = s.executeQuery(
                "select * from sys.syscolumns");
            while (rs.next()) {};
            fail("expected CONN_INTERRUPT");
        } catch (SQLException e) {
            assertSQLState("expected CONN_INTERRUPT", "08000", e);
            // assertTrue(c.isClosed()); // DERBY-4993
            assertTrue(Thread.interrupted());
        }
    }

    // Test that query if interrupted will get stopped as expected in
    // BasicNoPutResultSetImpl#checkCancellationFlag
    public void testLongQueryInterrupt() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure tstInterruptLongQuery() " +
            "reads sql data " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstInterruptLongQuery' " +
            "language java parameter style java");
        try {
            s.executeUpdate("call tstInterruptLongQuery()");
            fail("expected 40XC0 exception");
        } catch (SQLException e) {
            assertSQLState("expected 40XC0", "40XC0", e); // dead statement
            assertTrue(c.isClosed());
        }

    }


    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testInterruptBatch
    public static void tstInterruptBatch() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        s.executeUpdate("create table tmp(i int)");
        PreparedStatement ps = c.prepareStatement("insert into tmp values (?)");

        // fill batch:
        for (int i=0; i < 10; i++) {
            s.addBatch("insert into tmp values (" + i + ")");
        }

        s.executeBatch(); // should work OK, since no interrupt present

        // refill batch:
        for (int i=0; i < 10; i++) {
            s.addBatch("insert into tmp values (" + i + ")");
        }

        try {
            Thread.currentThread().interrupt();
            s.executeBatch();
            fail("expected CONN_INTERRUPT");
        } catch (SQLException e) {
            assertSQLState("expected CONN_INTERRUPT", "08000", e);
            // assertTrue(c.isClosed()); // DERBY-4993
            assertTrue(Thread.interrupted());
        }
    }


    // Test that batched statements, if interrupted, will get stopped as
    // expected.
    public void testInterruptBatch() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        setAutoCommit(false);

        s.executeUpdate(
            "create procedure tstInterruptBatch() " +
            "modifies sql data " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstInterruptBatch' " +
            "language java parameter style java");
        try {
            s.executeUpdate("call tstInterruptBatch()");
            fail("expected 40XC0 exception");
        } catch (SQLException e) {
            assertSQLState("expected 40XC0", "40XC0", e); // dead statement
            assertTrue(c.isClosed());
        }

        setAutoCommit(false);
        s = createStatement();
        // The table created inside stored routine should be gone:
        s.executeUpdate("create table tmp(i int)");
        rollback();

    }
}

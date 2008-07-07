/*

   Derby - Class
   org.apache.derbyTesting.functionTests.tests.multi.StressMultiTest

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

package org.apache.derbyTesting.functionTests.tests.multi;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Stresstest running any number of threads performing "random" operations on
 * a database for any number of minutes. Default values are 10 threads for 10
 * minutes. The operations are create, select, insert, update and rollback.
 *
 * To test with a different number of threads and minutes set up a suite by
 * calling getSuite(int threads, int minutes). See StressMulti50x59.java for
 * an example.
 */
public class StressMultiTest extends BaseJDBCTestCase {

    private static int THREADS = 10; //Default number of threads we will run.
    private static int MINUTES = 10; //Default number of minutes we will run.
    private static boolean DEBUG = false; //Force verbosity, used for debugging.

    private Thread threads[] = null;
    private TestResult testResult = null;
    private Random rnd = new Random();
    private boolean complete = false;

    public StressMultiTest(String s) {
        super(s);
    }

    /**
     * Set up the testsuite to run in embedded, client and encryption mode.
     * Default run is 10 threads for 10 minutes in each mode
     */
    public static Test suite() {
        Properties sysprops = System.getProperties();
        sysprops.put("derby.locks.deadlockTimeout", "2");
        sysprops.put("derby.locks.waitTimeout", "3");
        sysprops.put("derby.language.logStatementText", "true");
        sysprops.put("derby.storage.keepTransactionLog", "true");
        sysprops.put("derby.infolog.append", "true");

        TestSuite embedded = new TestSuite("StressMultiTest:embedded");
        embedded.addTestSuite(StressMultiTest.class);

        TestSuite client = new TestSuite("StressMultiTest:client");
        client.addTest(TestConfiguration.clientServerDecorator(
                new TestSuite(StressMultiTest.class)));

        TestSuite encrypted = new TestSuite("StressMultiTest:encrypted");
        encrypted.addTestSuite(StressMultiTest.class);

        TestSuite unencrypted = new TestSuite("StressMultiTest:unencrypted");
        unencrypted.addTest((embedded));
        unencrypted.addTest((client));

        TestSuite suite = new TestSuite("StressMultiTest, " + THREADS +
                " Threads " + MINUTES + " Minutes");
        suite.addTest(newCleanDatabase(unencrypted));
        //Encrypted uses a different database so it needs its own newCleanDatabase
        suite.addTest(Decorator.encryptedDatabase(newCleanDatabase(encrypted)));

        return suite;
    }

    /**
     * Get a testsuite that runs the specified number of threads
     * for the specified number of minutes.
     *
     * @param threads
     * @param minutes
     * @return
     */
    public static Test suite(int threads, int minutes) {
        THREADS = threads;
        MINUTES = minutes;
        return suite();
    }

    /*
     * Create a CleanDatabaseTestSetup that sets up the testdatabase.
     */
    private static Test newCleanDatabase(TestSuite s) {
        return new CleanDatabaseTestSetup(s) {
        /**
         * Creates the database objects used in the test cases.
         *
         * @throws SQLException
         */
        protected void decorateSQL(Statement s) throws SQLException {
            s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                    + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME " +
                    "'org.apache.derbyTesting.functionTests.util.Formatters" +
                    ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
            s.execute("CREATE FUNCTION RANDOM() RETURNS DOUBLE EXTERNAL " +
                    "NAME 'java.lang.Math.random' LANGUAGE JAVA PARAMETER " +
                    "STYLE JAVA");
            s.execute("create table main(x int not null primary key," +
                    " y varchar(2000))");
            s.execute("insert into main values(1, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(2, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(3, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(4, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(5, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(6, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(7, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(8, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(9, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(10, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(12, PADSTRING('aaaa',2000))");
            s.execute("insert into main values(13, PADSTRING('aaaa',2000))");
            s.execute("create table main2(x int not null primary key," +
                    " y varchar(2000))");
            s.execute("insert into main2 values(1, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(2, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(3, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(4, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(5, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(6, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(7, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(8, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(9, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(10, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(12, PADSTRING('aaaa',2000))");
            s.execute("insert into main2 values(13, PADSTRING('aaaa',2000))");
            getConnection().commit();
        };
    };
    }


    /*
     * (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    public void setUp() throws Exception{
        super.setUp();
        this.getTestConfiguration().setVerbosity(DEBUG);
    }

    /*
     * Make sure we clear the fields when done.
     */
    public void tearDown() throws Exception{
        testResult = null;
        rnd = null;
        threads = null;
        super.tearDown();
    }

    /**
     * This is the actual fixture run by the JUnit framework.
     * Creates all the runnables we need and pass them on to
     * runTestCaseRunnables
     */
    public void testStressMulti() {
        StressMultiRunnable[] tct = new StressMultiRunnable[THREADS];

        for (int i = 0; i < tct.length; i++) {
           tct[i] = new StressMultiRunnable ("Tester" + i, MINUTES);
        }
        runTestCaseRunnables (tct);
        tct = null;
     }


    /*
     * Create all the threads and run them until they finish.
     *
     * @param runnables
     */
    protected void runTestCaseRunnables(final StressMultiRunnable[] runnables) {
        if (runnables == null) {
            throw new IllegalArgumentException("runnables is null");
        }
        threads = new Thread[runnables.length];

        //Create threads
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(runnables[i]);
        }
        //Run the threads
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        //Wait for the threads to finish
        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        } catch (InterruptedException ignore) {
            BaseTestCase.println("Thread join interrupted.");
        }
        threads = null;
    }

    public void run(final TestResult result) {
        testResult = result;
        super.run(result);
        testResult = null;
    }

    /*
     * Handles any exceptions we get in the threads
     */
    private void handleException(final Throwable t, String message) {
        synchronized(testResult) {
            println("Exception handled!!: "+ message + " - " + t);
            complete = true; //This stops the tests.
            if(t instanceof AssertionFailedError) {
                testResult.addFailure(this, (AssertionFailedError)t);
            }
            else {
                testResult.addError(this, t);
            }
        }
    }


    /**
     * This subclass contains the actual stresstests that will be run in
     * multiple threads.
     */
    class StressMultiRunnable implements Runnable {
        String name;
        Connection con;
        long starttime;
        long runtime; //How long will this thread run

        public StressMultiRunnable(String name, int minutes) {
            super();
            this.name = name;
            starttime = System.currentTimeMillis();
            runtime = minutes*60*1000; //convert min to ms
            try {
                con = openDefaultConnection();
                con.setAutoCommit(false);
            } catch (SQLException e) {
                println(e.toString());
            }
        }

       /*
        * Do the work. Runs an infinite loop picking random operations from
        * the methods below until either complete == true, ie. there was a
        * failure in one of the threads, or the specified time has passed.
        */
        public void run() {
            try {
                int i = 0;
                while (!complete) {
                    i++;
                    int r = rnd.nextInt(100);

                    if (r < 10) {
                        String n = "x";
                        switch (rnd.nextInt(4)) {
                            case 0: n = "a"; break;
                            case 1: n = "x"; break;
                            case 2: n = "y"; break;
                            case 3: n = "z"; break;
                        }
                        create(n);
                        println(name + " - Run " + i + " - Create " + n + " " +
                                new Date(System.currentTimeMillis()).toString());
                    } else if (r < 25){
                        String n = "main";
                        if (rnd.nextInt(2) == 1) n = "main2";
                        roll(n);
                        println(name + " - Run " + i + " - Roll " + n + " " +
                                new Date(System.currentTimeMillis()).toString());
                    } else if (r < 40){
                        String n = "main";
                        if (rnd.nextInt(2) == 1) n = "main2";
                        insert(n);
                        println(name + " - Run " + i + " - Insert " + n + " " +
                                new Date(System.currentTimeMillis()).toString());
                    } else if (r < 60){
                        String n = "main";
                        if (rnd.nextInt(2) == 1) n = "main2";
                        update(n);
                        println(name + " - Run " + i + " - Update " +
                                n + " " +
                                new Date(System.currentTimeMillis()).toString());
                    } else if (r <= 99){
                        String n = "main";
                        if (rnd.nextInt(2) == 1) n = "main2";
                        select(n);
                        println(name + " - Run " + i + " - Select " + n + " " +
                                new Date(System.currentTimeMillis()).toString());
                    }
                    //Break the loop if the running time is reached.
                    if ((starttime + runtime) <= System.currentTimeMillis()) {
                        println(name + " - STOPPING - " +
                                new Date(System.currentTimeMillis()).toString());
                        break;
                    }
                    Thread.sleep(rnd.nextInt(10)); //Just to spread them out a bit.
                }
            }
            catch(Throwable t) {
               println("Exception in " + name + ": " + t);
               handleException(t, name + " - " +
                       new Date(System.currentTimeMillis()).toString());
            }
            println(name + " terminated!");
        }


        /********* Below are the tasks done by the threads  ******************/

        /**
         * Create a table with the given name and then drop it
         *
         * @param table
         * @throws SQLException
         */
        private void create(String table) throws SQLException {
            Statement s = con.createStatement();
            try {
                s.execute("create table " + table + " (x int)");
                s.execute("insert into " + table + " values (1)");
                s.execute("insert into " + table + " values (1)");
                s.execute("insert into " + table + " values (1)");
                s.execute("insert into " + table + " values (1)");
                s.execute("insert into " + table + " values (1)");
                s.execute("drop table " + table);
                con.commit();
            } catch (SQLException se) {
                String e = se.getSQLState();
                if (e.equals("X0X08") || e.equals("X0X05") || e.equals("42X05")
                        || e.equals("42Y55") || e.equals("42000")
                        || e.equals("40001") || e.equals("40XL1")
                        || e.equals("40XL2") || e.equals("42Y07")
                        || e.equals("42Y55")) {
                    //Ignore these
                } else {
                    throw se;
                }
            } finally {
                s = null;
            }
        }

        /**
         * Insert a random value into the given table.
         * Table names can be main or main2.
         *
         * @param table
         * @throws SQLException
         */
        private void insert(String table) throws SQLException {
            Statement s = con.createStatement();
            try {
                s.executeUpdate("insert into " + table
                        + " values (random() * 1000 + 100, 'rand')");
                con.commit();
            } catch (SQLException se) {
                String e = se.getSQLState();
                if (e.equals("42000") || e.equals("23505") || e.equals("40001")
                        || e.equals("40XL1") || e.equals("40XL2")
                        || e.equals("42Y07") || e.equals("42Y55")) {
                    // ignore these
                } else {
                    throw se;
                }
            }finally {
                s = null;
            }
        }

        /**
         * insert a value into the given table, then rollback.
         * Table names are main or main2.
         *
         * @param table
         * @throws SQLException
         */
        private void roll(String table) throws SQLException {
            Statement s = con.createStatement();
            con.setAutoCommit(false);
            try {
                s.executeUpdate("insert into " + table
                                + " values (666, '666')");
                con.rollback();
            } catch (SQLException se) {
                String e = se.getSQLState();
                if (e.equals("X0X05") || e.equals("42X05") || e.equals("42Y55")
                        || e.equals("42000") || e.equals("23505")
                        || e.equals("40001") || e.equals("40XL1")
                        || e.equals("40XL2") || e.equals("42Y07")
                        || e.equals("42Y55")) {
                    // ignore these
                } else {
                    throw se;
                }
            }finally {
                s = null;
            }
        }

        /**
          * Select * from the given table. Table names are main or main2.
          *
          * @param table
          * @throws SQLException
          */
        private void select(String table) throws SQLException {
            Statement s = con.createStatement();
            try {
                s.executeQuery("select * from " + table);
            } catch (SQLException se) {
                String e = se.getSQLState();
                if (e.equals("42Y55") || e.equals("42000") || e.equals("40001")
                        || e.equals("40XL1") || e.equals("40XL2")
                        || e.equals("42Y07")) {
                    // ignore these
                } else {
                    throw se;
                }
            }finally {
                s = null;
            }
        }

        /**
         * Update the given table. Table names are main or main2.
         *
         * @param table
         * @throws SQLException
         */
        private void update(String table) throws SQLException {
            Statement s = con.createStatement();
            try {
                s.executeUpdate("update " + table
                        + " main set y = 'zzz' where x = 5");
            } catch (SQLException se) {
                String e = se.getSQLState();
                if (e.equals("42Y55") || e.equals("42000") || e.equals("40001")
                        || e.equals("40XL1") || e.equals("40XL2")
                        || e.equals("42Y07")) {
                    // ignore these
                } else {
                    throw se;
                }
            } finally {
                s = null;
            }
        }
    }
}

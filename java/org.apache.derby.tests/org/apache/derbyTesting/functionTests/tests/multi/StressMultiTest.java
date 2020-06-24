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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Stress-test running any number of threads performing "random" operations on
 * a database for any number of minutes. Default values are 10 threads for 10
 * minutes. The operations are create, select, insert, update and rollback.
 *
 * To test with a different number of threads and minutes set up a suite by
 * calling getSuite(int threads, int minutes). See StressMulti10x1.java for
 * an example.
 * 
 * To run only the embedded run, use embeddedSuite(int threads, int minutes).
 * 
 * The test will fail on the first exception thrown by any of the treads and
 * this will be the reported cause of failure and the threads will be stopped.
 * Other threads may throw exceptions before they have time to stop and these
 * can be found in the log, but they will not be reported as errors or failures
 * by the test.
 * 
 * SQLExceptions are reported as failures and any other exceptions as errors.
 * 
 * Some SQLExceptions are ignored by the test, but will show up in the log.
 * 
 */
public class StressMultiTest extends BaseJDBCTestCase {

    /**
     * The number of threads the test will run. Default is 10
     */
    private static int THREADS = 10;
    
    /**
     * The number of minutes the test will run. Default is 10.
     */
    private static int MINUTES = 10;
    
    private static final String THREADSMINUTES = "derby.tests.ThreadsMinutes";
    
    /**
     * Force verbosity, used for debugging. Will print alot of information
     * to the screen. 
     */
    private static boolean DEBUG = false;
    
    /**
     * This holds the first throwable thrown by by any of the threads,
     *  and will thrown as the cause of failure for the fixture.   
     */
    private Throwable thrown = null;
    
    private Thread threads[] = null;
    private Random rnd = new Random();
    
    /**
     * Setting this will cause the threads to terminate normally.
     */
    private boolean complete = false;
    
    
    public StressMultiTest(String s) {
        super(s);
    }

    /**
     * Set up the testsuite to run in embedded, client and encryption mode.
     * Default run is 10 threads for 10 minutes in each mode
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
        Properties dbprops = new Properties();
        dbprops.put("derby.locks.deadlockTimeout", "2");
        dbprops.put("derby.locks.waitTimeout", "3");

        Properties sysprops = new Properties();
        sysprops.put("derby.storage.keepTransactionLog", "true");
        sysprops.put("derby.language.logStatementText", "true");
        sysprops.put("derby.infolog.append", "true");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test embedded = new BaseTestSuite(StressMultiTest.class);
        embedded = new SystemPropertyTestSetup(embedded,sysprops,true);
        embedded = new DatabasePropertyTestSetup(embedded,dbprops);
        // make this a singleUseDatabase so the datbase and 
        // transaction log will be preserved.
        embedded = TestConfiguration.singleUseDatabaseDecorator(newCleanDatabase(embedded));
        // SystemPropertyTestSetup for static properties 
        // does not work for client because shutting down the
        // engine causes protocol errors on the client. Run
        // with -Dderby.storage.keepTransactionLog=true if
        // you need to save the transaction log for client.
        Test client = TestConfiguration.clientServerDecorator(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
                new BaseTestSuite(StressMultiTest.class));
        client = newCleanDatabase(new DatabasePropertyTestSetup(client,dbprops));
        Test encrypted = new BaseTestSuite(StressMultiTest.class);
        // SystemPropertyTestSetup for static properties 
        // does not work for encrypted databases because the
        // database has to be rebooted and we don't have access
        // to the boot password (local to Decorator.encryptedDatabase()
        // Run with -Dderby.storage.keepTransactionLog=true if you 
        // need to save the transaction log for encrypted.
        BaseTestSuite unencrypted =
            new BaseTestSuite("StressMultiTest:unencrypted");

        unencrypted.addTest((embedded));
        unencrypted.addTest((client));

        BaseTestSuite suite =
            new BaseTestSuite("StressMultiTest, " + THREADS +
                               " Threads " + MINUTES + " Minutes");

        suite.addTest(newCleanDatabase(unencrypted));
        //Encrypted uses a different database so it needs its own newCleanDatabase
        suite.addTest(Decorator.encryptedDatabase(new DatabasePropertyTestSetup(newCleanDatabase(encrypted),dbprops)));
//IC see: https://issues.apache.org/jira/browse/DERBY-1764

        return suite;
    }

    /**
     * Get a testsuite that runs all the 3 runs (embedded, client and encrypted) 
     * with the given number of threads for the given number of minutes.
     *
     * @param threads
     * @param minutes
     * @return  suite after changing <code>THREADS</code> and 
     * <code> MINUTES </code>
     */
    public static Test suite(int threads, int minutes) {
        THREADS = threads;
        MINUTES = minutes;
        return suite();
    }
    
    /**
     * Get at testsuite that runs only the embedded suite with 
     * the given number of threads for the given number of minutes. 
     * 
     * @param threads
     * @param minutes
     */
    public static Test embeddedSuite(int threads, int minutes) {
        THREADS = threads;
        MINUTES = minutes;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
        Properties dbprops = new Properties();
        dbprops.put("derby.locks.deadlockTimeout", "2");
        dbprops.put("derby.locks.waitTimeout", "3");
        dbprops.put("derby.language.logStatementText", "true");
        dbprops.put("derby.storage.keepTransactionLog", "true");
        Properties sysprops = new Properties();
        sysprops.put("derby.storage.keepTransactionLog", "true");
        sysprops.put("derby.language.logStatementText", "true");
        sysprops.put("derby.infolog.append", "true");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test embedded = new BaseTestSuite(StressMultiTest.class);
        embedded = new SystemPropertyTestSetup(embedded,sysprops,true);
        embedded = new DatabasePropertyTestSetup(embedded,dbprops);
        embedded = TestConfiguration.singleUseDatabaseDecorator(newCleanDatabase(embedded));
        return embedded;
    }
    
    /*
     * Create a CleanDatabaseTestSetup that sets up the testdatabase.
     */
    private static Test newCleanDatabase(Test s) {
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
        
        // Let -Dderby.tests.ThreadsMinutes=TTxMM override.
//IC see: https://issues.apache.org/jira/browse/DERBY-4429
        String optThreadsMinutes = getSystemProperty(THREADSMINUTES);
        if ( optThreadsMinutes != null )
        { // Syntax: '99x22' meaning 99 threads 22 minutes.
            int xPos = optThreadsMinutes.indexOf("x");
            try{
                // Assuming xPos >= 1 : substring or parseInt will catch it.
                THREADS = Integer.parseInt(optThreadsMinutes.substring(0, xPos));
                MINUTES = Integer.parseInt(optThreadsMinutes.substring(xPos+1, optThreadsMinutes.length()));
            }
            catch ( Exception e) {
                alarm("Illegal value for '"+THREADSMINUTES+"': '"
                        +optThreadsMinutes+"' - " +e.getMessage()
                        +". Threads: " + THREADS +", minutes: " + MINUTES);
            }
            traceit("Threads: " + THREADS +", minutes: " + MINUTES);
        }
    }

    /*
     * Make sure we clear the fields when done.
     */
    public void tearDown() throws Exception{
        rnd = null;
        threads = null;
        super.tearDown();
    }

    /**
     * This is the actual fixture run by the JUnit framework.
     * Creates all the runnables we need and pass them on to
     * runTestCaseRunnables. If any exception was thrown
     * when they are done it is thrown for JUnit to catch and
     * handle normally.
     */
    public void testStressMulti() throws Throwable{
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
        thrown = null;
        
        StressMultiRunnable[] tct = new StressMultiRunnable[THREADS];

        for (int i = 0; i < tct.length; i++) {
           tct[i] = new StressMultiRunnable ("Tester" + i, MINUTES);
        }
        runTestCaseRunnables (tct);
        tct = null;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
        if (thrown!=null) throw thrown;
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

    
    /*
     * Handles any exceptions we get in the threads
     */
    private synchronized void handleException(final Throwable t, String message) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
            complete = true; //This stops the threads.
            if (thrown == null) {
                if(t instanceof AssertionFailedError) {
                    thrown = (AssertionFailedError) t;
                } else if (t instanceof SQLException) {
                    ByteArrayOutputStream b = new ByteArrayOutputStream();
                    t.printStackTrace(new PrintStream(b));
                    thrown = new AssertionFailedError("Caused by: \n" + b.toString());
                }
                else {
                    thrown = t;
                }
                println("Exception handled!!: "+ message + " - " + t);
            } else 
            println("Exception discarded because another was already caught and the threads are terminating..:\n"+ message + " - " + t);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
            runtime = minutes*60*1000; //convert minutes to ms
            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
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
                
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
                        || e.equals("42Y55")
                        ) {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
//IC see: https://issues.apache.org/jira/browse/DERBY-3789
                ResultSet rs = s.executeQuery("select * from " + table);
                JDBC.assertDrainResults(rs);
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

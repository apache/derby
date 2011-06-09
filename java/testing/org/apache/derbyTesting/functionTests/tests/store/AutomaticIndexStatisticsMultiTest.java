/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.AutomaticIndexStatisticsMultiTest

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.IndexStatsUtil;
import org.apache.derbyTesting.junit.IndexStatsUtil.IdxStats;

/**
 * Tests that the update triggering code can handle a high amount of requests,
 * both when the data dictionary is operating in cached mode and in DDL mode.
 */
public class AutomaticIndexStatisticsMultiTest
    extends BaseJDBCTestCase {

    private static final String TAB = "MTSEL";
    private static final int _100K = 100000;

    public AutomaticIndexStatisticsMultiTest(String name) {
        super(name);
    }

    public void testMTSelect()
            throws Exception {
        prepareTable(true);

        // Create threads compiling a select query for the table
        int threadCount = 5;
        long runTime = 10000;
        MTCompileThread[] compileThreads = new MTCompileThread[threadCount];
        for (int i=0; i < threadCount; i++) {
            compileThreads[i] =
                    new MTCompileThread(openDefaultConnection(), runTime);
        }

        // Start the threads and let them run for a while.
        Thread[] threads = new Thread[threadCount];
        for (int i=0; i < threadCount; i++) {
            threads[i] = new Thread(compileThreads[i]);
            threads[i].start();
        }

        // Wait for the threads to finish.
        for (int i=0; i < threadCount; i++) {
            threads[i].join(600*1000); // Time out after 600 seconds.
        }

        int total = 0;
        int totalError = 0;
        for (int i=0; i < threadCount; i++) {
            int count = compileThreads[i].getCount();
            int errors = compileThreads[i].getErrorCount();
            total += count;
            totalError += errors;
        }
        println("TOTAL = " + total + " (of which " + totalError + " errors)");
        assertEquals(0, totalError);

        verifyStatistics();
        // Shutdown database to log daemon stats (if logging is enabled).
        getTestConfiguration().shutdownDatabase();
    }

    public void testMTSelectWithDDL()
            throws Exception {
        prepareTable(true);

        // Create threads compiling a select query for the table
        int threadCount = 5;
        long runTime = 10000;
        MTCompileThread[] compileThreads = new MTCompileThread[threadCount];
        for (int i=0; i < threadCount; i++) {
            compileThreads[i] =
                    new MTCompileThread(openDefaultConnection(), runTime);
        }

        // Start the drop/create thread.
        MTCreateDropThread createThread = new MTCreateDropThread(
                openDefaultConnection(), runTime);
        Thread[] threads = new Thread[threadCount +1];
        threads[0] = new Thread(createThread);
        threads[0].start();

        // Start the threads and let them run for a while.
        for (int i=0; i < threadCount; i++) {
            threads[i+1] = new Thread(compileThreads[i]);
            threads[i+1].start();
        }

        // Wait for the threads to finish.
        for (int i=0; i < threadCount; i++) {
            threads[i].join(600*1000); // Time out after 600 seconds.
        }

        int total = 0;
        int totalError = 0;
        for (int i=0; i < threadCount; i++) {
            int count = compileThreads[i].getCount();
            int errors = compileThreads[i].getErrorCount();
            total += count;
            totalError += errors;
        }
        println("TOTAL = " + total + " (of which " + totalError +
                " errors) CREATES = " + createThread.getCreates());
        assertEquals(0, totalError);

        verifyStatistics();
        // Shutdown database to log daemon stats (if logging is enabled).
        getTestConfiguration().shutdownDatabase();
    }

    private void verifyStatistics()
            throws SQLException {
        // DERBY-5097: On machines with a single core/CPU the load generated
        // by the test threads may cause the index statistics daemon worker
        // thread to be "starved". Add a timeout to give it a chance to do
        // what it has been told to do.
        IndexStatsUtil stats = new IndexStatsUtil(getConnection(), 5000);
        IdxStats[] myStats = stats.getStatsTable(TAB, 2);
        for (int i=0; i < myStats.length; i++) {
            IdxStats s = myStats[i];
            assertEquals(_100K, s.rows);
            switch (s.lcols) {
                case 1:
                    assertEquals(10, s.card);
                    break;
                case 2:
                    assertEquals(_100K, s.card);
                    break;
                default:
                    fail("unexpected number of leading columns: " + s.lcols);
            }
        }
    }

    private void prepareTable(boolean dropIfExists)
            throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = getConnection().getMetaData().getTables(
                null, null, TAB, null);
        if (rs.next()) {
            assertFalse(rs.next());
            rs.close();
            if (dropIfExists) {
                println("table " + TAB + " already exists, dropping");
                stmt.executeUpdate("drop table " + TAB);
            } else {
                println("table " + TAB + " already exists, reusing");
                return;
            }
        } else {
            rs.close();
        }

        stmt.executeUpdate("create table " + TAB + " (val1 int, val2 int)");
        stmt.executeUpdate("create index " +
                "mtsel_idx on " + TAB + " (val1, val2)");

        setAutoCommit(false);
        PreparedStatement ps = prepareStatement(
                "insert into " + TAB + " values (?,?)");
        // Insert blocks of 10000 rows.
        int blockCount =    10;
        int blockSize =  10000;
        for (int i=0; i < blockCount; i++) {
            ps.setInt(1, i);
            for (int j=0; j < blockSize; j++) {
                ps.setInt(2, j);
                ps.addBatch();
            }
            ps.executeBatch();
            commit();
            println("inserted block " + (i+1) + "/" + blockCount);
        }
        setAutoCommit(true);
    }

    public static Test suite() {
        return new TestSuite(AutomaticIndexStatisticsMultiTest.class);
    }

    private static class MTCompileThread
            implements Runnable {

        private final Random rand = new Random();
        private final Connection con;
        private final long runTime;
        private int count;
        private int errorCount;

        public MTCompileThread(Connection con, long runTime)
                throws SQLException {
            this.con = con;
            this.runTime = runTime;
        }

        public void run() {
            final long started = System.currentTimeMillis();
            int counter = 0;
            while (System.currentTimeMillis() - started < runTime) {
                try {
                    con.prepareStatement("select * from mtsel where " +
                                (++counter) + " = " + counter + " AND val2 = " +
                                (1 + rand.nextInt(10)));
                } catch (SQLException sqle) {
                    errorCount++;
                }
                count++;
            }
        }

        public int getCount() {
            return count;
        }

        public int getErrorCount() {
            return errorCount;
        }
    }

    private static class MTCreateDropThread
            implements Runnable {

        private final Connection con;
        private final long runTime;
        private long creates;

        public MTCreateDropThread(Connection con, long runTime)
                throws SQLException {
            this.con = con;
            this.runTime = runTime;
        }

        public void run() {
            final long started = System.currentTimeMillis();
            try {
                ResultSet rs = con.getMetaData().getTables(
                        null, null, "TMPTABLE", null);
                boolean lastWasCreate = rs.next();
                rs.close();
                Statement stmt = con.createStatement();
                while (System.currentTimeMillis() - started < runTime) {
                    if (lastWasCreate) {
                        // Drop the table
                        stmt.executeUpdate("drop table TMPTABLE");
                    } else {
                        // Create the table
                        stmt.executeUpdate("create table TMPTABLE("
                                + "i int primary key, v varchar(30), b blob)");
                        creates++;
                    }
                    lastWasCreate = !lastWasCreate;
                }
            } catch (SQLException sqle) {
                fail("create drop thread crashed: " + sqle.getMessage());
            }
        }

        public long getCreates() {
            return creates;
        }
    }
}

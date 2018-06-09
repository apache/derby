/*

   Derby - Class org.apache.derbyTesting.perf.basic.jdbc.ClobAccessTest

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
package org.apache.derbyTesting.perf.basic.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;
import org.apache.derbyTesting.perf.clients.BackToBackLoadGenerator;
import org.apache.derbyTesting.perf.clients.Client;
import org.apache.derbyTesting.perf.clients.DBFiller;
import org.apache.derbyTesting.perf.clients.LoadGenerator;
import org.apache.derbyTesting.perf.clients.SingleRecordFiller;
import org.apache.derbyTesting.perf.clients.SingleRecordSelectClient;

/**
 * A series of tests accessing Clobs in various ways.
 * <p>
 * These tests are intended to detect Clob performance regressions. Before
 * committing a patch that might change the Clob performance characteristics,
 * first run these tests on a clean build and then with the patch applied. The
 * results can only be compared when both runs are done on the same machine.
 * <p>
 * The results are time taken to execute the test. Lower duration is better
 * (improvement). Currently the results are printed to standard out. There is
 * one exception, which is {@code testConcurrency}. For this test, the
 * throughput is printed and it will always run for a fixed amount of time.
 * <p>
 * The tests are written with two axis in mind: read-only vs update and small vs
 * large. These axis were chosen based on the Clob implementation at the time.
 * In the context of this test, small means the Clob is represented as a string
 * by the Derby store and large means the Clob is represtend as a stream into
 * the Derby store. When a Clob is modified, an in-memory or on disk temporary
 * copy is created. The performance of these temporary representations are
 * tested with the tests that modify the Clob content.
 * <p>
 * System properties controlling test behavior:
 * <dl>
 *      <dt>derby.tests.disableSmallClobs</dt>
 *      <dd>Whether or not to disable the testing of small Clobs.</dd>
 *      <dt>derby.tests.disableLargeClobs</dt>
 *      <dd>Whether or not to disable the testing of large Clobs.</dd>
 *      <dt>derby.tests.disableConcurrencyTest</dt>
 *      <dd>Whether or not to disable the concurrency test.</dd>
 *      <dt>derby.tests.largeClobSize</dt>
 *      <dd>Size of the large Clobs in MB, 15 MB is the default.</dd>
 *      <dt>derby.tests.runLargeClobTests</dt>
 *      <dd>A list of one or more tests to run. Only tests using large Clobs
 *          should be specified, but this is not enforced. Example:
 *          <tt>testFetchLargeClobPieceByPiece,testLargeClobGetLength</tt></dd>
 * </dl>
 *
 * <p>
 * <b>NOTE</b>: Currently there are no tests for the client driver (network)
 * or for encrypted Clobs.
 */
public class ClobAccessTest
        extends JDBCPerfTestCase {

    private static final boolean disableSmallClobs =
            Boolean.getBoolean("derby.tests.disableSmallClobs");
    private static final boolean disableLargeClobs =
            Boolean.getBoolean("derby.tests.disableLargeClobs");
    private static final boolean disableConcurrencyTest =
            Boolean.getBoolean("derby.tests.disableConcurrencyTest");
    /**
     * A list of one or more tests to be run. Only tests using a large Clob
     * should be specified.
     */
    private static final String runLargeClobTests =
            System.getProperty("derby.tests.runLargeClobTests", null);
    private static final int largeClobSizeMB =
            Integer.getInteger("derby.tests.largeClobSize", 15).intValue();


    /** Maximum buffer size to use. */
    private static final int MAX_BSIZE = 32676;

    /**
     * Instantiates a new test that will be run the specified number of
     * iterations and repeated as specified.
     *
     * @param name name of the test to instantiate
     * @param iterations number of iterations per repetition
     * @param repeats number of repetitions
     */
    public ClobAccessTest(String name, int iterations, int repeats) {
        super(name, iterations, repeats);
    }

    /**
     * Set autocommit to false by default.
     */
    public void initializeConnection(Connection conn)
            throws SQLException {
        conn.setAutoCommit(false);
    }

    public static Test suite() {
        BaseTestSuite mainSuite = new BaseTestSuite("ClobAccessTest suite");
        if (!disableSmallClobs) {
            int iters = 50;
            int reps = 1;
            println("Adding small Clob tests.");
            BaseTestSuite smallSuite = new BaseTestSuite("Small Clob suite");
            smallSuite.addTest(new ClobAccessTest(
                    "testFetchSmallClobs", iters, reps));
            smallSuite.addTest(new ClobAccessTest(
                    "testFetchSmallClobsInaccurateLength", iters, reps));
            smallSuite.addTest(new ClobAccessTest(
                    "testModifySmallClobs", iters, reps));
            mainSuite.addTest(smallSuite);
        }
        if (!disableLargeClobs) {
            int iters = 5;
            int reps = 1;
            String[] tests = new String[] {
                    "testFetchLargeClobs",
                    "testFetchLargeClobsModified",
                    "testFetchLargeClobWithStream",
                    "testFetchLargeClobOneByOneCharBaseline",
                    "testFetchLargeClobOneByOneCharModified",
                    "testFetchLargeClobOneByOneChar",
                    "testFetchLargeClobPieceByPiece",
                    "testFetchLargeClobPieceByPieceModified",
                    "testLargeClobGetLength",
                    "testLargeClobGetLengthModified",
                    "testLargeClobTruncateLengthMinusOne",
                    "testFetchLargeClobPieceByPieceBackwards",
                };
            // See if the user has overridden which tests to run.
            if (runLargeClobTests != null) {
                String[] specifiedTests = runLargeClobTests.split(",");
                if (specifiedTests.length > 0) {
                    tests = specifiedTests;
                }
            }
            println("Adding " + tests.length + " large Clob tests.");
            BaseTestSuite largeSuite = new BaseTestSuite("Large Clob suite");
            for (int i=0; i < tests.length; i++) {
                largeSuite.addTest(new ClobAccessTest(tests[i] , iters, reps));
            }
            mainSuite.addTest(largeSuite);
        }
        if (!disableConcurrencyTest) {
            mainSuite.addTest(new ClobAccessTest("testConcurrency", 1, 1));
        }
        return new CleanDatabaseTestSetup(mainSuite) {
            protected void decorateSQL(Statement stmt)
                    throws SQLException {
                initializeClobData(stmt);
            }
        };
    }

    /**
     * Fetches a number of small Clobs, getting the content using getSubString.
     * <p>
     * The exact length of the clob is used when getting the string.
     */
    public void testFetchSmallClobs()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob, length from smallClobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int clobLength = rs.getInt(2);
            String content = clob.getSubString(1, clobLength);
        }
        rs.close();
    }

    /**
     * Fetches a number of small Clobs, getting the content using getSubString.
     * <p>
     * A too long length of the clob is used when getting the string.
     */
    public void testFetchSmallClobsInaccurateLength()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob, length from smallClobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int unusedLength = rs.getInt(2);
            String content = clob.getSubString(1, 100);
        }
        rs.close();
    }

    /**
     * Test fetching the content after adding a single character at the end.
     */
    public void testModifySmallClobs()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob, length from smallClobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int length = rs.getInt(2);
            clob.setString(length, "X");
            String content = clob.getSubString(1, 100);
        }
        rs.close();
    }

    public void testFetchLargeClobs()
            throws IOException, SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs");
        ResultSet rs = ps.executeQuery();
        char[] charBuf = new char[16*1024]; // 16 KB
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            Reader content = clob.getCharacterStream();
            long remaining = rs.getInt(2);
            while (remaining > 0) {
                remaining -= content.read(charBuf);
            }
            content.close();
        }
        rs.close();
    }

    public void testFetchLargeClobsModified()
            throws IOException, SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs");
        ResultSet rs = ps.executeQuery();
        char[] charBuf = new char[16*1024]; // 16 KB
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            clob.setString(1, "X");
            Reader content = clob.getCharacterStream();
            long remaining = rs.getInt(2);
            while (remaining > 0) {
                remaining -= content.read(charBuf);
            }
            content.close();
        }
        rs.close();
    }

    /**
     * Fetches a single Clob and reads it char by char, but utilizing a
     * buffered stream to get a lower time bound on the read operation.
     */
    public void testFetchLargeClobOneByOneCharBaseline()
            throws IOException, SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            Reader content = clob.getCharacterStream();
            BufferedReader bufferedContent = new BufferedReader(content);
            long remaining = rs.getInt(2);
            while (bufferedContent.read() != -1) {
                remaining--;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    public void testFetchLargeClobOneByOneChar()
            throws IOException, SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            Reader content = clob.getCharacterStream();
            long remaining = rs.getInt(2);
            while (content.read() != -1) {
                remaining--;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    public void testFetchLargeClobOneByOneCharModified()
            throws IOException, SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            long remaining = rs.getInt(2);
            clob.setString(++remaining, "X");
            Reader content = clob.getCharacterStream();
            while (content.read() != -1) {
                remaining --;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    /**
     * Tests that repositioning within the current internal character buffer is
     * cheap.
     * <p>
     * Note that the positions used in this test have been chosen based on the
     * internal buffer size (8KB), which is an implementation detail.
     *
     * @throws SQLException if the test fails
     */
    public void testFetchLargeClobPieceByPieceBackwards()
            throws IOException, SQLException {
        boolean modifyClob = false;
        final int intBufSize = 8192; // Implementation detail.
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int remaining = rs.getInt(2);
            if (modifyClob) {
                // Modify the Clob to create a temporary copy in memory or on
                // disk (depends on the Clob size).
                long modifyStart = System.currentTimeMillis();
                clob.setString(++remaining, "X");
                println("Clob modification duration: " +
                        (System.currentTimeMillis() - modifyStart) + " ms");
            }
            // Go close to the middle of the Clob on a buffer border, then
            // subtract the piece size to avoid repositioning.
            final int pieceSize = 10;
            final long pos = (remaining / 2 / intBufSize) *
                    intBufSize - pieceSize;
            for (int i=0; i < intBufSize; i += pieceSize) {
                String str = clob.getSubString(
                        pos -i, pieceSize);
            }
        }
        rs.close();
    }

    /**
     * Fetches a "large" Clob piece by piece using getSubString.
     */
    public void testFetchLargeClobPieceByPiece()
            throws IOException, SQLException {
        fetchPieceByPiece(false);
    }

    /**
     * Fetches a "large" Clob piece by piece using getSubString.
     * <p>
     * The Clob is modified before fetched to create a temporary Clob
     * representation in memory / on disk.
     */
    public void testFetchLargeClobPieceByPieceModified()
            throws IOException, SQLException {
        fetchPieceByPiece(true);
    }
    
    /**
     * Fetches a "large" Clob piece by piece using getSubString.
     *
     * @param modifyClob whether to modify the Clob before fetching it
     *      (determines the internal Derby Clob representation)
     */
    private void fetchPieceByPiece(final boolean modifyClob)
            throws IOException, SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int remaining = rs.getInt(2);
            Reader myReader = new LoopingAlphabetReader(remaining);
            if (modifyClob) {
                // Modify the Clob to create a temporary copy in memory or on
                // disk (depends on the Clob size).
                long modifyStart = System.currentTimeMillis();
                clob.setString(++remaining, "X");
                println("Clob modification duration: " +
                        (System.currentTimeMillis() - modifyStart) + " ms");
            }
            long pos = 1;
            while (remaining > 0) {
                String str = clob.getSubString(
                        pos, Math.min(MAX_BSIZE, remaining));
                myReader.skip(Math.min(MAX_BSIZE, remaining) -1);
                pos += str.length();
                remaining -= str.length();
                // Avoid failure on the last char when Clob is modified.
                if (!modifyClob || remaining != 0) {
                    assertEquals(myReader.read(), str.charAt(str.length() -1));
                }
            }
        }
        rs.close();
    }

    public void testFetchLargeClobWithStream()
            throws IOException, SQLException {
        boolean modifyClob = false;
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 5");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int remaining = rs.getInt(2);
            Reader myReader = new LoopingAlphabetReader(remaining);
            if (modifyClob) {
                // Modify the Clob to create a temporary copy in memory or on
                // disk (depends on the Clob size).
                long modifyStart = System.currentTimeMillis();
                clob.setString(++remaining, "X");
                println("Clob modification duration: " +
                        (System.currentTimeMillis() - modifyStart) + " ms");
            }
            Reader clobReader = clob.getCharacterStream();
            char[] buf = new char[MAX_BSIZE];
            while (remaining > 0) {
                int read = clobReader.read(buf, 0, Math.min(MAX_BSIZE, remaining));
                myReader.skip(read -1);
                remaining -= read;
                assertEquals(myReader.read(), buf[read -1]);
            }
        }
        rs.close();
        
    }

    /**
     * Tests if the Clob length is cached.
     */
    public void testLargeClobGetLength() throws SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 7");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            long remaining = rs.getInt(2);
            // This should be cached. Lots of data have to be skipped otherwise.
            for (int i=0; i < 50; i++) {
                assertEquals(remaining, clob.length());
            }
        }
        rs.close();
    }

    /**
     * Tests if the Clob length is cached.
     */
    public void testLargeClobGetLengthModified() throws SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 7");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            clob.setString(1, "X");
            long remaining = rs.getInt(2);
            // This should be cached. Lots of data have to be skipped otherwise.
            for (int i=0; i < 50; i++) {
                assertEquals(remaining, clob.length());
            }
        }
        rs.close();
    }

    /**
     * Tests the speed of transferring data from the store to local temporary
     * storage as part of the truncate operation.
     */
    public void testLargeClobTruncateLengthMinusOne()
            throws SQLException {
        // Select just one Clob.
        PreparedStatement ps = prepareStatement(
                "select dClob, length from largeClobs where id = 8");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            int length = rs.getInt(2);
            clob.truncate(length -1);
        }
    }

    /**
     * Runs a test using multiple threads.
     * <p>
     * This test intends to detect problems with small Clobs and general
     * problems with concurrency.
     * <p>
     * <b>NOTE</b>: To produce more reliable numbers, please run the performance
     * client independently outside this JUnit test framework. Performance also
     * suffers greatly with SANE builds.
     */
    public void testConcurrency()
            throws InterruptedException, SQLException {

        final int records = 100000;
        final int tables = 1;
        final int threads = 16;
        DBFiller filler = new SingleRecordFiller(
                records, tables, java.sql.Types.CLOB, false, false);
        Connection conn = getConnection();
        println("initializing database...");
        filler.fill(conn);
        conn.close();

        Client[] clients = new Client[threads];
        for (int i = 0; i < clients.length; i++) {
            Connection c = openDefaultConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            clients[i] = new SingleRecordSelectClient(
                    records, tables, java.sql.Types.CLOB, false, false);
            clients[i].init(c);
        }

        final int warmupSec = 30;
        final int steadySec = 60;
        LoadGenerator gen = new BackToBackLoadGenerator();
        gen.init(clients);
        println("starting warmup...");
        gen.startWarmup();
        Thread.sleep(1000L * warmupSec);
        println("entering steady state...");
        gen.startSteadyState();
        Thread.sleep(1000L * steadySec);
        println("stopping threads...");
        gen.stop();
        // Should get the printstream used by the test harness here.
        gen.printReport(System.out);
    }

    /**
     * Generates test data.
     */
    private static void initializeClobData(Statement stmt)
            throws SQLException {
        Connection con = stmt.getConnection();
        con.setAutoCommit(false);
        if (!disableSmallClobs) {
            println("Generating small Clobs test data.");
            // Insert small Clob data.
            try {
                stmt.executeUpdate("drop table smallClobs");
            } catch (SQLException sqle) {
                assertSQLState("42Y55", sqle);
            }
            stmt.executeUpdate(
                    "create table smallClobs (dClob clob, length int)");
            PreparedStatement smallClobInsert = con.prepareStatement(
                    "insert into smallClobs values (?,?)");
            // Insert 15 000 small clobs.
            for (int clobCounter = 1; clobCounter < 15001; clobCounter++) {
                String content = Integer.toString(clobCounter);
                smallClobInsert.setString(1, content);
                smallClobInsert.setInt(2, content.length());
                smallClobInsert.executeUpdate();
                if (clobCounter % 1000 == 0) {
                    con.commit();
                }
            }
            con.commit();
        }

        if (!disableLargeClobs) {
            println("Generating large Clobs test data.");
            // Insert large Clob data.
            try {
                stmt.executeUpdate("drop table largeClobs");
            } catch (SQLException sqle) {
                assertSQLState("42Y55", sqle);
            }
            stmt.executeUpdate("create table largeClobs (" +
                    "id int unique not null, dClob clob, length int)");
            PreparedStatement largeClobInsert = con.prepareStatement(
                    "insert into largeClobs values (?,?,?)");
            // Insert some large Clobs.
            final int size = largeClobSizeMB*1024*1024; // 15 MB default
            for (int clobCounter = 1; clobCounter < 11; clobCounter++) {
                largeClobInsert.setInt(1, clobCounter);
                largeClobInsert.setCharacterStream(
                        2, new LoopingAlphabetReader(size), size);
                largeClobInsert.setInt(3, size);
                largeClobInsert.executeUpdate();
                println("Inserted large Clob #" + (clobCounter -1));
            }
            con.commit();
        }
    }
}

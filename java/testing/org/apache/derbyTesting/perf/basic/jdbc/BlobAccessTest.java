/*

   Derby - Class org.apache.derbyTesting.perf.basic.jdbc.BlobAccessTest

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
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
 * A series of tests accessing Blobs in various ways.
 * <p>
 * These tests are intended to detect Blob performance regressions. Before
 * committing a patch that might change the Blob performance characteristics,
 * first run these tests on a clean build and then with the patch applied. The
 * results can only be compared when both runs are done on the same machine.
 * <p>
 * The results are the time taken to execute the test. Lower duration is better
 * (improvement). Currently the results are printed to standard out. There is
 * one exception, which is {@code testConcurrency}. For this test, the
 * throughput is printed and it will always run for a fixed amount of time.
 * <p>
 * The tests are written with two axis in mind: read-only vs update and small vs
 * large. These axis were chosen based on the Blob implementation at the time.
 * In the context of this test, small means the Blob is represented as a string
 * by the Derby store and large means the Blob is represtend as a stream into
 * the Derby store. When a Blob is modified, an in-memory or on disk temporary
 * copy is created. The performance of these temporary representations are
 * tested with the tests that modify the Blob content.
 * <p>
 * System properties controlling test behavior:
 * <ul><li>derby.tests.disableSmallBlobs</li>
 *     <li>derby.tests.disableLargeBlobs</li>
 *     <li>derby.tests.disableConcurrencyTest</li>
 *     <li>derby.tests.largeBlobSize (in MB, 15 is the default)</li>
 * </ul>
 *
 * <p>
 * <b>NOTE</b>: Currently there are no tests for the client driver (network)
 * or for encrypted Blobs.
 */
public class BlobAccessTest
        extends JDBCPerfTestCase {

    private static final boolean disableSmallBlobs =
            Boolean.getBoolean("derby.tests.disableSmallBlobs");
    private static final boolean disableLargeBlobs =
            Boolean.getBoolean("derby.tests.disableLargeBlobs");
    private static final boolean disableConcurrencyTest =
            Boolean.getBoolean("derby.tests.disableConcurrencyTest");
    private static final int largeBlobSizeMB =
            Integer.getInteger("derby.tests.largeBlobSize", 15).intValue();

    private static final int FETCH_GETBYTES = 0;
    private static final int FETCH_GETBINARYSTREAM = 1;

    /**
     * Instantiates a new test that will be run the specified number of
     * iterations and repeated as specified.
     *
     * @param name name of the test to instantiate
     * @param iterations number of iterations per repetition
     * @param repeats number of repetitions
     */
    public BlobAccessTest(String name, int iterations, int repeats) {
        super(name, iterations, repeats);
    }

    /**
     * Set autocommit to false by default.
     */
    public void initializeConnection(Connection conn)
            throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Generates a suite of tests.
     * <p>
     * The required test data will be generated. Note that a subset of the
     * tests can be disabled by using a system property.
     *
     * @return A suite of tests.
     */
    public static Test suite() {
        BaseTestSuite mainSuite = new BaseTestSuite("BlobAccessTest suite");
        if (!disableSmallBlobs) {
            int iters = 50;
            int reps = 3;
            println("Adding small Blob tests.");
            BaseTestSuite smallSuite = new BaseTestSuite("Small Blob suite");
            smallSuite.addTest(new BlobAccessTest(
                    "testFetchSmallBlobs", iters, reps));
            smallSuite.addTest(new BlobAccessTest(
                    "testFetchSmallBlobsInaccurateLength", iters, reps));
            smallSuite.addTest(new BlobAccessTest(
                    "testModifySmallBlobs", iters, reps));
            mainSuite.addTest(smallSuite);
        }
        if (!disableLargeBlobs) {
            int iters = 5;
            int reps = 3;
            println("Adding large Blob tests.");
            BaseTestSuite largeSuite = new BaseTestSuite("Large Blob suite");
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobs", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobOneByOneByteBaseline", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobOneByOneByteModified", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobOneByOneByte", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlob", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobModified", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobPieceByPiece", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testFetchLargeBlobPieceByPieceModified", iters, reps));
            largeSuite.addTest(new BlobAccessTest(
                    "testLargeBlobGetLength", iters, reps));
            mainSuite.addTest(largeSuite);
        }
        if (!disableConcurrencyTest) {
            mainSuite.addTest(new BlobAccessTest("testConcurrency", 1, 1));
        }
        return new CleanDatabaseTestSetup(mainSuite) {
            protected void decorateSQL(Statement stmt)
                    throws SQLException {
                try {
                    initializeBlobData(stmt);
                } catch (UnsupportedEncodingException uee) {
                    // Compiled with JDK 1.4, can't use constructor.
                    SQLException sqle = new SQLException();
                    sqle.initCause(uee);
                    throw sqle;
                }
            }
        };
    }

    /**
     * Fetches a number of small Blobs, getting the content using getBytes.
     * <p>
     * The exact length of the Blob is used when getting the bytes.
     */
    public void testFetchSmallBlobs()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from smallBlobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            int blobLength = rs.getInt(2);
            byte[] content = Blob.getBytes(1, blobLength);
        }
        rs.close();
    }

    /**
     * Fetches a number of small Blobs, getting the content using getBytes.
     * <p>
     * A too long length of the Blob is used when getting the bytes.
     */
    public void testFetchSmallBlobsInaccurateLength()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from smallBlobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            int unusedLength = rs.getInt(2);
            byte[] content = Blob.getBytes(1, 100);
        }
        rs.close();
    }

    /**
     * Test fetching the content after adding a single byte at the end.
     */
    public void testModifySmallBlobs()
            throws SQLException, UnsupportedEncodingException {
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from smallBlobs");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            int length = rs.getInt(2);
            Blob.setBytes(length, "X".getBytes("US-ASCII"));
            byte[] content = Blob.getBytes(1, 100);
        }
        rs.close();
    }

    /**
     * Fetches a number of Blobs using a rather large read buffer with
     * {@code getBinaryStream}.
     */
    public void testFetchLargeBlobs()
            throws IOException, SQLException {
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs");
        ResultSet rs = ps.executeQuery();
        byte[] byteBuf = new byte[16*1024]; // 16 KB
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            InputStream content = Blob.getBinaryStream();
            long remaining = rs.getInt(2);
            while (remaining > 0) {
                remaining -= content.read(byteBuf);
            }
            content.close();
        }
        rs.close();
    }

    /**
     * Fetches a single Blob and reads it byte by byte, but utilizing a
     * buffered stream to get a lower time bound on the read operation.
     */
    public void testFetchLargeBlobOneByOneByteBaseline()
            throws IOException, SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            InputStream content = Blob.getBinaryStream();
            BufferedInputStream bufferedContent =
                    new BufferedInputStream(content);
            long remaining = rs.getInt(2);
            while (bufferedContent.read() != -1) {
                remaining--;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    /**
     * Fetches a single Blob and reads it byte by byte.
     */
    public void testFetchLargeBlobOneByOneByte()
            throws IOException, SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            InputStream content = Blob.getBinaryStream();
            long remaining = rs.getInt(2);
            while (content.read() != -1) {
                remaining--;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    /**
     * Fetches a single Blob and reads it byte by byte after it has first been
     * modified.
     * <p>
     * The point of modifiying the Blob is to make Derby use the writable Blob
     * representation (different implementation).
     */
    public void testFetchLargeBlobOneByOneByteModified()
            throws IOException, SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob Blob = rs.getBlob(1);
            long remaining = rs.getInt(2);
            Blob.setBytes(++remaining, "X".getBytes("US-ASCII"));
            InputStream content = Blob.getBinaryStream();
            while (content.read() != -1) {
                remaining --;
            }
            content.close();
            assertEquals(0, remaining);
        }
        rs.close();
    }

    /**
     * Fetches a single Blob by reading it piece by piece with {@code getBytes}.
     */
    public void testFetchLargeBlobPieceByPiece()
            throws IOException, SQLException {
        fetchBlobPieceByPiece(false, FETCH_GETBYTES);
    }

    /**
     * Fetches a single Blob by reading it piece by piece with {@code getBytes}.
     */
    public void testFetchLargeBlobPieceByPieceModified()
            throws IOException, SQLException {
        fetchBlobPieceByPiece(true, FETCH_GETBYTES);
    }

    /**
     * Fetches a single Blob by reading it in chunks with
     * {@code getBinaryStream}.
     */
    public void testFetchLargeBlob()
            throws IOException, SQLException {
        fetchBlobPieceByPiece(false, FETCH_GETBINARYSTREAM);
    }

    /**
     * Fetches a single Blob by reading it in chunks with
     * {@code getBinaryStream}.
     */
    public void testFetchLargeBlobModified()
            throws IOException, SQLException {
        fetchBlobPieceByPiece(true, FETCH_GETBINARYSTREAM);
    }

    /**
     * Fetches a "large" Blob piece by piece using getBytes.
     *
     * @param modifyBlob whether to modify the Blob before fetching it
     *      (determines the internal Derby Blob representation)
     */
    private void fetchBlobPieceByPiece(boolean modifyBlob, int fetchMode)
            throws IOException, SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 4");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob blob = rs.getBlob(1);
            long remaining = rs.getInt(2);
            if (modifyBlob) {
                // Modify the Blob to create a temporary copy in memory or on
                // disk (depends on the Blob size).
                long modifyStart = System.currentTimeMillis();
                blob.setBytes(++remaining, new byte[] {(byte)'X'});
                println("Blob modification duration: " +
                        (System.currentTimeMillis() - modifyStart) + " ms");
            }
            long pos = 1;
            int MAX_SIZE = 32676;
            switch (fetchMode) {
                case FETCH_GETBYTES:
                    while (remaining > 0) {
                        byte[] bytes = blob.getBytes(
                                pos, (int)Math.min(MAX_SIZE, remaining));
                        pos += bytes.length;
                        remaining -= bytes.length;
                    }
                    break;
                case FETCH_GETBINARYSTREAM:
                    InputStream stream = blob.getBinaryStream();
                    byte[] buf = new byte[MAX_SIZE];
                    while (remaining > 0) {
                        int read = stream.read(buf);
                        pos += read;
                        remaining -= read;
                    }
                    stream.close();
                    break;
                default:
                    fail("Unknown fetch mode: " + fetchMode);
            }
        }
        rs.close();
    }

    /**
     * Tests if the Blob length is cached.
     */
    public void testLargeBlobGetLength() throws SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 7");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob blob = rs.getBlob(1);
            long length = rs.getInt(2);
            // This should be cached. Have to skip lots of data otherwise.
            for (int i=0; i < 50; i++) {
                assertEquals(length, blob.length());
            }
        }
        rs.close();
    }

    /**
     * Tests if the Blob length is cached.
     */
    public void testLargeBlobGetLengthModified() throws SQLException {
        // Select just one Blob.
        PreparedStatement ps = prepareStatement(
                "select dBlob, length from largeBlobs where id = 7");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob blob = rs.getBlob(1);
            blob.setBytes(1, new byte[] {(byte)'X'});
            long length = rs.getInt(2);
            // This should be cached. Have to skip lots of data otherwise.
            for (int i=0; i < 50; i++) {
                assertEquals(length, blob.length());
            }
        }
        rs.close();
    }

    /**
     * Runs a test using multiple threads.
     * <p>
     * This test intends to detect problems with small Blobs and general
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
                records, tables, java.sql.Types.BLOB, false, false);
        Connection conn = getConnection();
        println("initializing database...");
        filler.fill(conn);
        conn.close();

        Client[] clients = new Client[threads];
        for (int i = 0; i < clients.length; i++) {
            Connection c = openDefaultConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            clients[i] = new SingleRecordSelectClient(
                    records, tables, java.sql.Types.BLOB, false, false);
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
    private static void initializeBlobData(Statement stmt)
            throws SQLException, UnsupportedEncodingException {
        Connection con = stmt.getConnection();
        con.setAutoCommit(false);
        if (!disableSmallBlobs) {
            println("Generating small Blobs test data.");
            // Insert small Blob data.
            try {
                stmt.executeUpdate("drop table smallBlobs");
            } catch (SQLException sqle) {
                assertSQLState("42Y55", sqle);
            }
            stmt.executeUpdate(
                    "create table smallBlobs (dBlob Blob, length int)");
            PreparedStatement smallBlobInsert = con.prepareStatement(
                    "insert into smallBlobs values (?,?)");
            // Insert 15 000 small Blobs.
            for (int BlobCounter = 1; BlobCounter < 15001; BlobCounter++) {
                byte[] content =
                        Integer.toString(BlobCounter).getBytes("US-ASCII");
                smallBlobInsert.setBytes(1, content);
                smallBlobInsert.setInt(2, content.length);
                smallBlobInsert.executeUpdate();
                if (BlobCounter % 1000 == 0) {
                    con.commit();
                }
            }
            con.commit();
        }

        if (!disableLargeBlobs) {
            println("Generating large Blobs test data.");
            // Insert large Blob data.
            try {
                stmt.executeUpdate("drop table largeBlobs");
            } catch (SQLException sqle) {
                assertSQLState("42Y55", sqle);
            }
            stmt.executeUpdate("create table largeBlobs (" +
                    "id int unique not null, dBlob Blob, length int)");
            PreparedStatement largeBlobInsert = con.prepareStatement(
                    "insert into largeBlobs values (?,?,?)");
            // Insert some large Blobs.
            final int size = largeBlobSizeMB*1024*1024; // 15 MB default
            for (int BlobCounter = 1; BlobCounter < 11; BlobCounter++) {
                largeBlobInsert.setInt(1, BlobCounter);
                largeBlobInsert.setBinaryStream(
                        2, new LoopingAlphabetStream(size), size);
                largeBlobInsert.setInt(3, size);
                largeBlobInsert.executeUpdate();
            }
            con.commit();
        }
    }
}

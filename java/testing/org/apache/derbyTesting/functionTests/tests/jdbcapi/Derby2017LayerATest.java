/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby2017LayerATest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tests that inserts with streams whose lengths differs from the the length
 * specified don't insert data into the database when they shouldn't.
 * <p>
 * The test uses various combinations of auto-commit and rollback.
 * <p>
 * TODO: Enable this test as part of the JDBCAPI suite when DERBY-2017 is fixed.
 */
public class Derby2017LayerATest
        extends BaseJDBCTestCase {

    public Derby2017LayerATest(String name) {
        super(name);
    }

    /**
     * Returns a suite running most of the tests with both the client driver
     * and the embedded driver, and some of the tests only with the client
     * driver.
     *
     * @return A suite of tests.
     */
    public static Test suite() {
        TestSuite ts = new TestSuite();
        ts.addTest(
                TestConfiguration.defaultSuite(Derby2017LayerATest.class));
        // Run the tests below with the client driver only.
        TestSuite clientSuite = new TestSuite("Client only tests");
        clientSuite.addTest(new Derby2017LayerATest(
                "cs_FailedStreamInsertBufferBoundaries"));
        clientSuite.addTest(new Derby2017LayerATest(
                "cs_StreamInsertBufferBoundary"));
        ts.addTest(TestConfiguration.clientServerDecorator(clientSuite));

        return ts;
    }

    /**
     * Tests inserts around some selected buffer boundaries. This test verifies
     * that the client and server can sucessfully insert values of various
     * lengths. It will work also before the fix for DERBY-2017, but will fail
     * if an incorrect fix is applied.
     */
    public void cs_StreamInsertBufferBoundary()
            throws IOException, SQLException {
        rollback();
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("create table t2017_len (len int, c clob)");
        } catch (SQLException sqle) {
            assertSQLState("X0Y32", sqle);
            stmt.executeUpdate("delete from t2017_len");
        }
        commit();
        setAutoCommit(false);

        // Note that when using layer A streaming the data is converted to
        // UTF-16 on the wire.
        PreparedStatement ps =
                prepareStatement("insert into t2017_len values (?,?)");
        // Test small values.
        for (int i=0; i < 512; i++) {
            ps.setInt(1, i);
            ps.setCharacterStream(2, new LoopingAlphabetReader(i), i);
            ps.executeUpdate();
        }
        commit();

        // Test values at the buffer boundary. Assumes UTF-16 and a ~32 KB
        // transmit buffer.
        for (int i=16000; i < 18000; i++) {
            ps.setInt(1, i);
            ps.setCharacterStream(2, new LoopingAlphabetReader(i), i);
            ps.executeUpdate();
            // Commit periodically.
            if (i % 1000 == 0) {
                commit();
            }
        }
        commit();

        for (int i=32500; i < 33000; i++) {
            ps.setInt(1, i);
            ps.setCharacterStream(2, new LoopingAlphabetReader(i), i);
            ps.executeUpdate();
        }
        commit();

        // Verify the data, basically making sure the status flag isn't
        // included as part of the user data.
        ResultSet rs = stmt.executeQuery("select len, c from t2017_len");
        int rows = 0;
        while (rs.next()) {
            rows++;
            assertEquals(new LoopingAlphabetReader(rs.getInt(1)),
                         rs.getCharacterStream(2));
        }
    }

    /**
     * Runs some failing inserts around buffer boundaries.
     */
    public void cs_FailedStreamInsertBufferBoundaries()
            throws IOException, SQLException {
        int[] INSERT;
        for (int i=0; i < 1024; i++) {
            INSERT = new int[] {
                8*1000+i,
                16*1000+i,
                32*1000+i,
                16*1000+i, // This will fail (forced length mismatch)
                32*1000+i, // This will fail (forced length mismatch)
                48*1000+i,
                0+i,
            };
            // We test only one combination of auto-commit and rollback here.
            doInsertTest(INSERT, true, false);
        }
    }

    public void testFailedStreamInsertLong()
            throws IOException, SQLException {
        int[] INSERT = new int[] {
            10*1024+1,
            89*1024+3,
            32*1024,
            64*1024, // This will fail (forced length mismatch)
            99*1024, // This will fail (forced length mismatch)
            1,
            197*1024,
        };
        doInsertTest(INSERT, false, false);
        doInsertTest(INSERT, false, true);
        doInsertTest(INSERT, true, false);
        doInsertTest(INSERT, true, true);
    }

    /**
     * Inserts data by reading from streams, where two of these will thrown
     * an {@code IOException}. Data from these streams should not be committed.
     */
    public void testFailedStreamInsertIOException()
            throws IOException, SQLException {
        String[] INSERT = new String[] {
                "row 1", "row 2", "row 3",
                "IGNORE", "IGNORE",
                "row 6", "row 7"
            };
        String[][] MASTER = new String[][] {
                {"row 1"}, {"row 2"}, {"row 3"},
                {"row 6"}, {"row 7"}
            };

        rollback();
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("create table t2017 (c clob)");
        } catch (SQLException sqle) {
            assertSQLState("X0Y32", sqle);
            stmt.executeUpdate("delete from t2017");
        }
        commit();

        setAutoCommit(true);
        PreparedStatement ps = prepareStatement("insert into t2017 values (?)");
        // Insert the 3 first rows.
        for (int i=0; i < 3; i++) {
            ps.setString(1, INSERT[i]);
            assertEquals(1, ps.executeUpdate());
        }

        // Insert the 4th and 5th row with a stream that throws an IOException.
        // Partial data shouldn't be inserted into the database.

        Reader r4 = new FailingReader(518, 500);
        ps.setCharacterStream(1, r4, 518);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
        }

        Reader r5 = new FailingReader(67*1024, 42*1024);
        ps.setCharacterStream(1, r5, 67*1024);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
        }

        // The errors above should have statement severity. Insert the last two
        // rows.
        for (int i=5; i < INSERT.length; i++) {
            ps.setString(1, INSERT[i]);
            assertEquals(1, ps.executeUpdate());
        }

        // Select data in the table, compare to MASTER
        ResultSet rs = stmt.executeQuery("select * from t2017");
        JDBC.assertFullResultSet(rs, MASTER);
    }

    public void testFailedStreamInsert()
            throws IOException, SQLException {
        String[] INSERT = new String[] {
                "This is row 1",
                "This is row 2",
                "This is row 3",
                "This is row 4, a bit too long",
                "This is row 5, a bit too short",
                "This is row 6",
                "This is row 7",
            };
        String[][] MASTER = new String[][] {
                {"This is row 1"},
                {"This is row 2"},
                {"This is row 3"},
                //{"This is row 4, a bit too long"},
                //{"This is row 5, a bit too short"},
                {"This is row 6"},
                {"This is row 7"},
            };

        doInsertTest(INSERT, MASTER, false, false);
    }

    public void testFailedStreamInsertAutoCommit()
            throws IOException, SQLException {
        String[] INSERT = new String[] {
                "This is row 1",
                "This is row 2",
                "This is row 3",
                "This is row 4, a bit too long",
                "This is row 5, a bit too short",
                "This is row 6",
                "This is row 7",
            };
        String[][] MASTER = new String[][] {
                {"This is row 1"},
                {"This is row 2"},
                {"This is row 3"},
                //{"This is row 4, a bit too long"},
                //{"This is row 5, a bit too short"},
                {"This is row 6"},
                {"This is row 7"},
            };
        doInsertTest(INSERT, MASTER, true, false);
    }

    public void testFailedStreamInsertRollbackOnError()
            throws IOException, SQLException {
        String[] INSERT = new String[] {
                "This is row 1",
                "This is row 2",
                "This is row 3",
                "This is row 4, a bit too long",
                "This is row 5, a bit too short",
                "This is row 6",
                "This is row 7",
            };
        String[][] MASTER = new String[][] {
                //{"This is row 1"},
                //{"This is row 2"},
                //{"This is row 3"},
                //{"This is row 4, a bit too long"},
                //{"This is row 5, a bit too short"},
                {"This is row 6"},
                {"This is row 7"},
            };
        doInsertTest(INSERT, MASTER, false, true);
    }

    public void testFailedStreamInsertAutoCommitRollbackOnError()
            throws IOException, SQLException {
        String[] INSERT = new String[] {
                "This is row 1",
                "This is row 2",
                "This is row 3",
                "This is row 4, a bit too long",
                "This is row 5, a bit too short",
                "This is row 6",
                "This is row 7",
            };
        String[][] MASTER = new String[][] {
                {"This is row 1"},
                {"This is row 2"},
                {"This is row 3"},
                //{"This is row 4, a bit too long"},
                //{"This is row 5, a bit too short"},
                {"This is row 6"},
                {"This is row 7"},
            };
        doInsertTest(INSERT, MASTER, true, true);
    }

    /**
     * Performs the base test cycle; insert 3 valid rows, try to insert 2
     * invalid rows, insert 2 valid rows.
     * <p>
     * The outcome depends on whether auto-commit is on, and whether a rollback
     * is issued when an insert fails.
     *
     * @param INSERT the data to insert
     * @param MASTER the expected outcome
     * @param autoCommit the auto-commit state to use
     * @param rollbackOnError whether or not to issue a rollback if an insert
     *      fails
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    private void doInsertTest(String[] INSERT, String[][] MASTER,
                              boolean autoCommit, boolean rollbackOnError)
            throws IOException, SQLException {
        // A few sanity checks.
        assertEquals("Expects 7 rows", 7, INSERT.length);
        assertTrue(MASTER.length < INSERT.length);

        rollback();
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("create table t2017 (c clob)");
        } catch (SQLException sqle) {
            assertSQLState("X0Y32", sqle);
            stmt.executeUpdate("delete from t2017");
        }
        commit();

        setAutoCommit(autoCommit);
        PreparedStatement ps = prepareStatement("insert into t2017 values (?)");
        // Insert the 3 first rows.
        for (int i=0; i < 3; i++) {
            ps.setString(1, INSERT[i]);
            assertEquals(1, ps.executeUpdate());
        }

        // Insert the 4th row with a stream that's longer than the specified
        // length, then the 5th row that's shorter. Both should fail, and the
        // data shouldn't be inserted into the database.

        Reader r4 = new StringReader(INSERT[3]);
        ps.setCharacterStream(1, r4, INSERT[3].length() - 5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed, stream too long");
        } catch (SQLException sqle) {
            // The states are different between client and embedded.
            assertSQLState(usingEmbedded() ? "XSDA4" : "XN015", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        Reader r5 = new StringReader(INSERT[4]);
        ps.setCharacterStream(1, r5, INSERT[4].length() + 5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed, stream too short");
        } catch (SQLException sqle) {
            // The states are different between client and embedded.
            assertSQLState(usingEmbedded() ? "XSDA4" : "XN017", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        // The errors above should have statement severity. Insert the last
        // two rows and make sure the transaction commits.
        for (int i=5; i < INSERT.length; i++) {
            ps.setString(1, INSERT[i]);
            assertEquals(1, ps.executeUpdate());
        }

        if (!autoCommit) {
            commit();
        }

        // Select data in the table, compare to MASTER
        ResultSet rs = stmt.executeQuery("select * from t2017");
        JDBC.assertFullResultSet(rs, MASTER);
    }

    /**
     * Performs the base test cycle; insert 3 valid rows, try to insert 2
     * invalid rows, insert 2 valid rows.
     * <p>
     * The outcome depends on whether auto-commit is on, and whether a rollback
     * is issued when an insert fails.
     *
     * @param INSERT the data to insert
     * @param MASTER the expected outcome
     * @param autoCommit the auto-commit state to use
     * @param rollbackOnError whether or not to issue a rollback if an insert
     *      fails
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    private void doInsertTest(int[] INSERT,
                              boolean autoCommit, boolean rollbackOnError)
            throws IOException, SQLException {
        // A few sanity checks.
        assertEquals("Expects 7 rows", 7, INSERT.length);

        rollback();
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("create table t2017_id (id int unique, c clob)");
        } catch (SQLException sqle) {
            assertSQLState("X0Y32", sqle);
            stmt.executeUpdate("delete from t2017_id");
        }
        commit();

        setAutoCommit(autoCommit);
        PreparedStatement ps =
                prepareStatement("insert into t2017_id values (?, ?)");
        // Insert the 3 first rows (id is 1-based).
        for (int i=0; i < 3; i++) {
            ps.setInt(1, i+1);
            int length = INSERT[i];
            ps.setCharacterStream(2, new LoopingAlphabetReader(length), length);
            assertEquals(1, ps.executeUpdate());
        }

        // Insert the 4th row with a stream that's longer than the specified
        // length, then the 5th row that's shorter. Both should fail, and the
        // data shouldn't be inserted into the database.

        Reader r4 = new LoopingAlphabetReader(INSERT[3]);
        ps.setInt(1, 4);
        ps.setCharacterStream(2, r4, INSERT[3] - 5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed, stream too long");
        } catch (SQLException sqle) {
            // The states are different between client and embedded.
            assertSQLState(usingEmbedded() ? "XSDA4" : "XN015", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        Reader r5 = new LoopingAlphabetReader(INSERT[4]);
        ps.setInt(1, 5);
        ps.setCharacterStream(2, r5, INSERT[4] + 5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed, stream too short");
        } catch (SQLException sqle) {
            // The states are different between client and embedded.
            assertSQLState(usingEmbedded() ? "XSDA4" : "XN017", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        // The errors above should have statement severity. Insert the last
        // two rows and make sure the transaction commits.
        for (int i=5; i < INSERT.length; i++) {
            ps.setInt(1, i+1);
            int length = INSERT[i];
            ps.setCharacterStream(2, new LoopingAlphabetReader(length), length);
            assertEquals(1, ps.executeUpdate());
        }

        if (!autoCommit) {
            commit();
        }

        // Make sure we have the expected number of rows.
        ResultSet rs = stmt.executeQuery("select count(*) from t2017_id");
        rs.next();
        assertEquals((rollbackOnError && !autoCommit ? 2 : 5), rs.getInt(1));

        // Select data in the table, compare to what we expect.
        rs = stmt.executeQuery( "select * from t2017_id order by id asc");
        // Check rows 1-4 if rollback on error is false.
        if (autoCommit || !rollbackOnError) {
            for (int i=0; i < 3; i++) {
                rs.next();
                int id = rs.getInt(1);
                assertTrue(id - 1 == i);
                assertEquals(new LoopingAlphabetReader(INSERT[i]),
                             rs.getCharacterStream(2));
            }
        }
        // Check rows 6 and 7.
        for (int i=5; i < 7; i++) {
            rs.next();
            int id = rs.getInt(1);
            assertTrue(id - 1 == i);
            assertEquals(new LoopingAlphabetReader(INSERT[i]),
                         rs.getCharacterStream(2));
        }
        assertFalse(rs.next());
        rs.close();
    }

    /**
     * WARNING: This reader is not a general purpose reader!!!
     * <p>
     * Reader thrown an exception when a certain amount of characters has been
     * returned (or is about to be returned).
     */
    public static class FailingReader
            extends Reader {

        private final LoopingAlphabetReader in;
        private final long failAtPos;
        private long pos;

        /**
         * Creates a new failing reader.
         *
         * @param length the total length of the source
         * @param failAtPos the position to fail at (specifying zero or a
         *      negative value causes an exception on the first read request)
         */
        public FailingReader(long length, long failAtPos) {
            this.failAtPos = failAtPos;
            this.in = new LoopingAlphabetReader(length);
        }

        public int read()
                throws IOException {
            // If we failed once, just keep failing on subsequent requests.
            pos++;
            int ret = in.read();
            if (pos >= failAtPos) {
                throw new IOException("forced exception");
            }
            return ret;
        }

        public int read(char[] cbuf, int off, int len)
                throws IOException {
            // If we failed once, just keep failing on subsequent requests.
            // Try to return some valid data before failing.
            if (pos == 0 && failAtPos > 1) {
                len = (int)Math.min(failAtPos -1, len);
            }
            int ret = in.read(cbuf, off, len);
            if (ret != -1) {
                pos += ret;
            }
            if (pos >= failAtPos) {
                throw new IOException("forced exception");
            }
            return ret;
        }

        public void close() {
            in.close();
        }
    }
}

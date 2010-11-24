/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby2017LayerBTest

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.ByteArrayInputStream;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby2017LayerATest.*;

/**
 * Tests that inserts with streams that throws an {@code IOException} don't
 * insert data into the database when they shouldn't.
 * <p>
 * The test uses various combinations of auto-commit and rollback.
 */
public class Derby2017LayerBTest
        extends BaseJDBCTestCase {

    public Derby2017LayerBTest(String name) {
        super(name);
    }

    /**
     * Returns a suite running the test with both the client driver and the
     * embedded driver.
     *
     * @return A suite of tests.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(Derby2017LayerBTest.class);
    }

    public void testStreamInsertCharBufferBoundary()
            throws IOException, SQLException {
        // NOTE: Many of these lengths are implementation dependent, and the
        //       code paths in LayerBStreamedEXTDTAReaderInputStream may change
        //       if the implementation of certain points of the DRDA protocol
        //       changes.
        int[] lengths = new int[] {
                1,
                16383,
                0,
                32756,
                36383,
                16384,
                192*1024, // Just a longer stream
            };

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

        PreparedStatement ps =
                prepareStatement("insert into t2017_len values (?,?)");
        for (int length : lengths) {
            ps.setInt(1, length);
            ps.setCharacterStream(2, new LoopingAlphabetReader(length));
            ps.executeUpdate();
        }

        // Verify the data, basically making sure the status flag isn't
        // included as part of the user data.
        ResultSet rs = stmt.executeQuery("select len, c from t2017_len");
        int rows = 0;
        while (rs.next()) {
            rows++;
            int length = rs.getInt(1);
            assertEquals(new LoopingAlphabetReader(length),
                         rs.getCharacterStream(2));
        }
        assertEquals(lengths.length, rows);
    }

    /**
     * Attempt to insert data with failing streams of various lengths.
     * <p>
     * None of the inserts should be successful, as an {@code IOException} is
     * thrown by all of the streams.
     */
    public void testFailedStreamInsertCharBufferBoundariesImpl()
            throws IOException, SQLException {
        // NOTE: Many of these lengths are implementation dependent, and the
        //       code paths in LayerBStreamedEXTDTAReaderInputStream may change
        //       if the implementation of certain points of the DRDA protocol
        //       changes.
        int[] lengths = new int[] {
                1,
                16383,
                0,
                32756,
                36383,
                16384,
                192*1024, // Just a longer stream
            };

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

        PreparedStatement ps =
                prepareStatement("insert into t2017_len values (?,?)");
        // Fail at the very beginning of the stream.
        for (int length : lengths) {
            ps.setInt(1, length);
            ps.setCharacterStream(2, new FailingReader(length, -1));
            try {
                ps.executeUpdate();
                fail("Should have failed (length=" + length + ")");
            } catch (SQLException sqle) {
                // TODO: Check when exception handling has been settled.
                //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            }
        }

        // Fail around half-way into the stream.
        for (int length : lengths) {
            ps.setInt(1, length);
            ps.setCharacterStream(2,
                    new FailingReader(length, length / 2));
            try {
                ps.executeUpdate();
                fail("Should have failed (length=" + length + ")");
            } catch (SQLException sqle) {
                // TODO: Check when exception handling has been settled.
                //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            }
        }

        // Fail at the very end of the stream.
        for (int length : lengths) {
            ps.setInt(1, length);
            ps.setCharacterStream(2,
                    new FailingReader(length, length -1));
            try {
                ps.executeUpdate();
                fail("Should have failed (length=" + length + ")");
            } catch (SQLException sqle) {
                // TODO: Check when exception handling has been settled.
                //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            }
        }

        // Verify that there is no data in the table (all failed).
        ResultSet rs = stmt.executeQuery("select count(*) from t2017_len");
        rs.next();
        assertEquals(0, rs.getInt(1));
    }

    public void testFailedStreamInsertChar()
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

    public void testFailedStreamInsertCharAutoCommit()
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

    public void testFailedStreamInsertCharRollbackOnError()
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

    public void testFailedStreamInsertCharAutoCommitRollbackOnError()
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

    public void testFailedStreamInsertBinary()
            throws IOException, SQLException {
        byte[][] INSERT = generateDefaultInsert();
        String[][] MASTER = generateMaster(INSERT, new int[] {3, 4});
        doInsertTest(INSERT, MASTER, false, false);
    }

    public void testFailedStreamInsertBinaryAutoCommit()
            throws IOException, SQLException {
        byte[][] INSERT = generateDefaultInsert();
        String[][] MASTER = generateMaster(INSERT, new int[] {3, 4});
        doInsertTest(INSERT, MASTER, true, false);
    }

    public void testFailedStreamInsertBinaryRollbackOnError()
            throws IOException, SQLException {
        byte[][] INSERT = generateDefaultInsert();
        String[][] MASTER = generateMaster(INSERT, new int[] {0, 1, 2, 3, 4});
        doInsertTest(INSERT, MASTER, false, true);
    }

    public void testFailedStreamInsertBinaryAutoCommitRollbackOnError()
            throws IOException, SQLException {
        byte[][] INSERT = generateDefaultInsert();
        String[][] MASTER = generateMaster(INSERT, new int[] {3, 4});
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
            ps.setCharacterStream(1, new StringReader(INSERT[i]));
            assertEquals(1, ps.executeUpdate());
        }

        // Insert the 4th and 5th row with a stream that throws an exception.
        // Partial data read shouldn't be inserted into the database.

        Reader r4 = new FailingReader(10, 3);
        ps.setCharacterStream(1, r4);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        Reader r5 = new FailingReader(35002, 35001);
        ps.setCharacterStream(1, r5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        // The errors above should have statement severity. Insert the last
        // two rows and commit.
        for (int i=5; i < INSERT.length; i++) {
            ps.setCharacterStream(1, new StringReader(INSERT[i]));
            assertEquals(1, ps.executeUpdate());
        }

        if (!autoCommit) {
            commit();
        }

        // Select data in the table, compare to MASTER
        ResultSet rs = stmt.executeQuery("select * from t2017");
        JDBC.assertUnorderedResultSet(rs, MASTER);
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
    private void doInsertTest(byte[][] INSERT, String[][] MASTER,
                              boolean autoCommit, boolean rollbackOnError)
            throws IOException, SQLException {
        // A few sanity checks.
        assertEquals("Expects 7 rows", 7, INSERT.length);
        assertTrue(MASTER.length < INSERT.length);

        rollback();
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("create table t2017_binary (b blob)");
        } catch (SQLException sqle) {
            assertSQLState("X0Y32", sqle);
            stmt.executeUpdate("delete from t2017_binary");
        }
        commit();

        setAutoCommit(autoCommit);
        PreparedStatement ps = prepareStatement(
                "insert into t2017_binary values (?)");
        // Insert the 3 first rows.
        for (int i=0; i < 3; i++) {
            ps.setBinaryStream(1, new ByteArrayInputStream(INSERT[i]));
            assertEquals(1, ps.executeUpdate());
        }

        // Insert the 4th and 5th row with a stream that throws an exception.
        // Partial data read shouldn't be inserted into the database.

        InputStream r4 = new FailingInputStream(new FailingReader(10, 3));
        ps.setBinaryStream(1, r4);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        InputStream r5 = new FailingInputStream(
                                    new FailingReader(35002, 35001));
        ps.setBinaryStream(1, r5);
        try {
            ps.executeUpdate();
            fail("Insert should have failed");
        } catch (SQLException sqle) {
            // TODO: Check when exception handling has been settled.
            // The states are different between client and embedded.
            //assertSQLState(usingEmbedded() ? "XSDA4" : "XJ001", sqle);
            if (rollbackOnError) {
                rollback();
            }
        }

        // The errors above should have statement severity. Insert the last
        // two rows and commit.
        for (int i=5; i < INSERT.length; i++) {
            ps.setBinaryStream(1, new ByteArrayInputStream(INSERT[i]));
            assertEquals(1, ps.executeUpdate());
        }

        if (!autoCommit) {
            commit();
        }

        // Select data in the table, compare to MASTER
        ResultSet rs = stmt.executeQuery("select * from t2017_binary");
        JDBC.assertUnorderedResultSet(rs, MASTER);
    }

    /**
     * Simple and <b>non-conforming</b> input stream that will fail after a
     * specified number of bytes read.
     */
    private static class FailingInputStream
            extends InputStream {

        private final FailingReader in;

        public FailingInputStream(FailingReader in) {
            this.in = in;
        }

        public int read()
                throws IOException {
            int c = in.read();
            return (byte)c;
        }
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.StreamTruncationTest

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
package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests string data value truncation when the data is inserted with a stream.
 * <p>
 * Tests that truncation does indeed happen for CHAR, VARCHAR and CLOB.
 * Truncation is not allowed for LONG VARCHAR columns.
 * <p>
 * There are two aspects to consider; specified length vs unspecified length
 * (lengthless override), and small vs large. In this regard, small is when the
 * stream content fits into the internal buffer of the conversion reader
 * ({@code ReaderToUTF8Stream}). In this test, the buffer is assumed to be
 * approximately 32KB.
 */
public class StreamTruncationTest
    extends BaseJDBCTestCase {

    /**
     * Assumed conversion buffer size.
     * @see org.apache.derby.iapi.types.ReaderToUTF8Stream
     */
    public static final int CONV_BUFFER_SIZE = 32*1024;

    // Column index constants
    public static final int CLOB = 2;
    public static final int VARCHAR = 3;
    public static final int LONGVARCHAR = 4;
    public static final int CHAR = 5;

    /** Name of table with the "small" columns. */
    public static final String TABLE_SMALL = "TRUNCATE_SMALL";
    /** Name of table with the "large" columns. */
    public static final String TABLE_LARGE = "TRUNCATE_LARGE";
    /** Small size (smaller than the conversion buffer). */
    public static final int SMALL_SIZE = CONV_BUFFER_SIZE / 2;
    /** Large size (larger than the conversion buffer). */
    public static final int LARGE_SIZE = CONV_BUFFER_SIZE * 2;
    /* Size used for the large VARCHAR column. */
    public static final int LARGE_VARCHAR_SIZE = 32672;
    /* Size used for the CHAR column. */
    public static final int CHAR_SIZE = 138;

    /** Id/counter to use for the inserted rows. */
    private static AtomicInteger ID = new AtomicInteger(1);

    public StreamTruncationTest(String name) {
        super(name);
    }

    public void setUp()
            throws SQLException {
        setAutoCommit(false);
    }

    public void testCharWithLength()
            throws IOException, SQLException {
        charSmall(false);
    }

    public void testCharWithoutLength()
            throws IOException, SQLException {
        charSmall(true);
    }

    public void testSmallVarcharWithLength()
            throws IOException, SQLException {
        generalTypeSmall(VARCHAR, false);
    }

    public void testSmallVarcharWithoutLength()
            throws IOException, SQLException {
        generalTypeSmall(VARCHAR, true);
    }

    public void testLargeVarcharWithLength()
            throws IOException, SQLException {
        generalTypeLarge(VARCHAR, false);
    }

    public void testLargeVarcharWithoutLength()
            throws IOException, SQLException {
        generalTypeLarge(VARCHAR, true);
    }

    public void testLongVarcharWithLength()
            throws IOException, SQLException {
        generalTypeSmall(LONGVARCHAR, false);
    }

    public void testLongVarcharWithoutLength()
            throws IOException, SQLException {
        generalTypeSmall(LONGVARCHAR, true);
    }

    public void testSmallClobWithLength()
            throws IOException, SQLException {
        generalTypeSmall(CLOB, false);
    }

    public void testSmallClobWithoutLength()
            throws IOException, SQLException {
        generalTypeSmall(CLOB, true);
    }

    public void testLargeClobWithLength()
            throws IOException, SQLException {
        generalTypeLarge(CLOB, false);
    }

    public void testLargeClobWithoutLength()
            throws IOException, SQLException {
        generalTypeLarge(CLOB, true);
    }

    /**
     * Executes a set of insertions into the larger of the columns.
     *
     * @param colIndex column index to insert into, which also determines the
     *      type to test
     * @param lengthless {@code true} if a lengthless override should be used,
     *      {@code false} if the length of the stream shall be specified when
     *      inserted
     * @throws IOException if reading from the source stream fails
     * @throws SQLException if something goes wrong
     */
    private void generalTypeLarge(int colIndex, boolean lengthless)
            throws IOException, SQLException {
        insertLarge(colIndex, lengthless, LARGE_SIZE, 0); // Fits
        insertLarge(colIndex, lengthless, LARGE_SIZE -99, 15); // Fits
        insertLarge(colIndex, lengthless, LARGE_SIZE + 189, 189); // Truncate
        insertLarge(colIndex, lengthless, LARGE_SIZE, 250); // Fits
        insertLarge(colIndex, lengthless, LARGE_SIZE + 180, 0); // Should fail
        insertLarge(colIndex, lengthless, LARGE_SIZE + 180, 17); // Should fail
    }

    /**
     * Executes a set of insertions into the smaller of the columns.
     *
     * @param colIndex column index to insert into, which also determines the
     *      type to test
     * @param lengthless {@code true} if a lengthless override should be used,
     *      {@code false} if the length of the stream shall be specified when
     *      inserted
     * @throws IOException if reading from the source stream fails
     * @throws SQLException if something goes wrong
     */
    private void generalTypeSmall(int colIndex, boolean lengthless)
            throws IOException, SQLException {
        insertSmall(colIndex, lengthless, SMALL_SIZE, 0); // Fits
        insertSmall(colIndex, lengthless, SMALL_SIZE -99, 15); // Fits
        insertSmall(colIndex, lengthless, SMALL_SIZE + 189, 189); // Truncate
        insertSmall(colIndex, lengthless, SMALL_SIZE, 250); // Fits
        insertSmall(colIndex, lengthless, SMALL_SIZE + 180, 0); // Should fail
        insertSmall(colIndex, lengthless, SMALL_SIZE + 180, 17); // Should fail
    }

    /**
     * Executes a set of insertions into the CHAR column.
     *
     * @param lengthless {@code true} if a lengthless override should be used,
     *      {@code false} if the length of the stream shall be specified when
     *      inserted
     * @throws IOException if reading from the source stream fails
     * @throws SQLException if something goes wrong
     */
    private void charSmall(boolean lengthless)
            throws IOException, SQLException {
        insertSmall(CHAR, lengthless, CHAR_SIZE, 0); // Fits
        insertSmall(CHAR, lengthless, CHAR_SIZE -10, 4); // Fits
        insertSmall(CHAR, lengthless, CHAR_SIZE + 189, 189); // Should truncate
        insertSmall(CHAR, lengthless, CHAR_SIZE, 20); // Fits
        insertSmall(CHAR, lengthless, CHAR_SIZE + 180, 0); // Should fail
        insertSmall(CHAR, lengthless, CHAR_SIZE + 180, 17); // Should fail
    }

    /**
     * Inserts a small (smaller than internal conversion buffer) string value.
     *
     * @param colIndex column to insert into (see constants)
     * @param lengthless whether the length of the stream should be specified
     *      or not on insertion
     * @param totalLength the total character length of the stream to insert
     * @param blanks number of trailing blanks in the stream
     * @return The id of the row inserted.
     *
     * @throws IOException if reading from the source stream fails
     * @throws SQLException if something goes wrong, or the test fails
     */
    private int insertSmall(int colIndex, boolean lengthless,
                            int totalLength, int blanks)
            throws IOException, SQLException {
        int id = ID.getAndAdd(1);
        PreparedStatement ps = prepareStatement(
                "insert into " + TABLE_SMALL + " values (?,?,?,?,?)");
        ps.setInt(1, id);
        ps.setNull(2, Types.CLOB);
        ps.setNull(3, Types.VARCHAR);
        ps.setNull(4, Types.LONGVARCHAR);
        ps.setNull(5, Types.CHAR);

        int colWidth = SMALL_SIZE;
        if (colIndex == LONGVARCHAR) {
            colWidth = 32700;
        }
        int expectedLength = Math.min(totalLength, colWidth);
        // Length of CHAR is always the defined length due to padding.
        if (colIndex == CHAR) {
            colWidth = expectedLength = CHAR_SIZE;
        }
        println("totalLength=" + totalLength + ", blanks=" + blanks +
                ", colWidth=" + colWidth + ", expectedLength=" +
                expectedLength);
        Reader source = new LoopingAlphabetReader(totalLength,
                CharAlphabet.modernLatinLowercase(), blanks);
        // Now set what we are going to test.
        if (lengthless) {
            ps.setCharacterStream(colIndex, source);
        } else {
            ps.setCharacterStream(colIndex, source, totalLength);
        }
        try {
            // Exceute the insert.
            assertEquals(1, ps.executeUpdate());
            if (totalLength > expectedLength) {
                assertTrue(totalLength - blanks <= expectedLength);
            }

            // Fetch the value.
            assertEquals(expectedLength,
                    getStreamLength(TABLE_SMALL, colIndex, id));
        } catch (SQLException sqle) {
            // Sanity check of the length.
            if (colIndex == LONGVARCHAR) {
                // Truncation is not allowed.
                assertTrue(totalLength > expectedLength);
            } else {
                // Total length minus blanks must still be larger then the
                // expected length.
                assertTrue(totalLength - blanks > expectedLength);
            }
            // The error handling here is very fuzzy...
            // This will hopefully be fixed, such that the exception thrown
            // will always be 22001. Today this is currently wrapped by several
            // other exceptions.
            String expectedState = "XSDA4";
            if (colIndex == CHAR || colIndex == VARCHAR) {
                if (lengthless) {
                    expectedState = "XJ001";
                } else {
                    if (!usingEmbedded()) {
                        expectedState = "XJ001";
                    } else {
                        expectedState = "22001";
                    }
                }
            }
            assertSQLState(expectedState, sqle);
        }
        return id;
    }

    /**
     * Inserts a large (largerer than internal conversion buffer) string value.
     *
     * @param colIndex column to insert into (see constants)
     * @param lengthless whether the length of the stream should be specified
     *      or not on insertion
     * @param totalLength the total character length of the stream to insert
     * @param blanks number of trailing blanks in the stream
     * @return The id of the row inserted.
     *
     * @throws IOException if reading from the source stream fails
     * @throws SQLException if something goes wrong, or the test fails
     */
    private int insertLarge(int colIndex, boolean lengthless,
                            int totalLength, int blanks)
            throws IOException, SQLException {
        // Not used here, see insertSmall.
        assertTrue(colIndex != CHAR && colIndex != LONGVARCHAR);

        int id = ID.getAndAdd(1);
        PreparedStatement ps = prepareStatement(
                "insert into " + TABLE_LARGE + " values (?,?,?)");
        ps.setInt(1, id);
        ps.setNull(2, Types.CLOB);
        ps.setNull(3, Types.VARCHAR);

        int colWidth = (colIndex == VARCHAR ? LARGE_VARCHAR_SIZE : LARGE_SIZE);
        int expectedLength = Math.min(totalLength, colWidth);
        println("totalLength=" + totalLength + ", blanks=" + blanks +
                ", colWidth=" + colWidth + ", expectedLength=" +
                expectedLength);
        Reader source = new LoopingAlphabetReader(totalLength,
                CharAlphabet.modernLatinLowercase(), blanks);
        // Now set what we are going to test.
        if (lengthless) {
            ps.setCharacterStream(colIndex, source);
        } else {
            ps.setCharacterStream(colIndex, source, totalLength);
        }
        try {
            // Exceute the insert.
            assertEquals(1, ps.executeUpdate());
            if (totalLength > expectedLength) {
                assertTrue(totalLength - blanks <= expectedLength);
            }

            // Fetch the value.
            assertEquals(expectedLength,
                    getStreamLength(TABLE_LARGE, colIndex, id));
        } catch (SQLException sqle) {
            // Sanity check of the length.
            // Total length minus blanks must still be larger then the
            // expected length.
            assertTrue(totalLength - blanks > expectedLength);
            // The error handling here is very fuzzy...
            // This will hopefully be fixed, such that the exception thrown
            // will always be 22001. Today this is currently wrapped by several
            // other exceptions.
            String expectedState = "XSDA4";
            if (colIndex == VARCHAR) {
                if (lengthless) {
                    expectedState = "XJ001";
                } else {
                    if (!usingEmbedded()) {
                        expectedState = "XJ001";
                    } else {
                        expectedState = "22001";
                    }
                }
            }
            assertSQLState(expectedState, sqle);
        }
        return id;
    }

    /**
     * Obtains the length of the data value stored in the specified table,
     * column index and id (primary key).
     *
     * @param table table name
     * @param colIndex column index
     * @param id id of the row to fetch
     * @return The length in characters of the string data value fetched.
     * @throws IOException if reading the stream fails
     * @throws SQLException if something goes wrong
     */
    private int getStreamLength(String table, int colIndex, int id)
            throws IOException, SQLException {
        Statement sFetch =  createStatement();
        ResultSet rs = sFetch.executeQuery("select * from " + table +
                " where id = " + id);
        assertTrue(rs.next());
        Reader dbSource = rs.getCharacterStream(colIndex);
        int observedLen = 0;
        char[] buf = new char[1024];
        while (true) {
            int read = dbSource.read(buf);
            if (read == -1) {
                break;
            }
            observedLen += read;
        }
        rs.close();
        return observedLen;
    }

    /**
     * Returns the suite of tests.
     * <p>
     * Two tables are created for the test.
     *
     * @return A suite of tests.
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(TestConfiguration.defaultSuite(
                StreamTruncationTest.class, false)) {
                    protected void decorateSQL(Statement stmt)
                            throws SQLException {
                        stmt.executeUpdate(
                                "create table " + TABLE_SMALL + " (" +
                                "ID int primary key, " +
                                "CLOBDATA clob(" + SMALL_SIZE + ")," +
                                "VCHARDATA varchar(" + SMALL_SIZE + ")," +
                                "LVCHARDATA long varchar," +
                                "CHARDATA char(" + CHAR_SIZE + "))");
                        stmt.executeUpdate(
                                "create table " + TABLE_LARGE + " (" +
                                "ID int primary key, " +
                                "CLOBDATA clob(" + LARGE_SIZE + ")," +
                                "VCHARDATA varchar(" + LARGE_VARCHAR_SIZE +
                                "))");
                        stmt.close();
                    }
            };
    }
}

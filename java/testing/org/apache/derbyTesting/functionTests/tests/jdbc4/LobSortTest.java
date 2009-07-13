/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.LobSortTest

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

/**
 * Executes sorting of LOB values based on the length of the LOB or a random
 * value. The intention is to test code determining the length of the LOBs and
 * also the code materializing the LOB values. The tests don't verify that the
 * sort order is actually correct. This test is a good candidate for being run
 * with hard or soft upgraded databases.
 * <p>
 * Note that the seed used for the random number generator is included in the
 * name of the test methods. Knowing the seed enables debugging by being able
 * to rerun a specific test sequence that failed. The random number generator
 * is only used during data insertion.
 * <p>
 * See DERBY-4245.
 * <p>
 * <em>NOTE</em>: This test is sensitive to the JVM heap size, which is one of
 * the factors determining whether the sort is done internally or externally.
 * The bug for a clean database would only occur with the external sort.
 */
public class LobSortTest
        extends BaseJDBCTestCase {

    /** The seed used for the random number generator. */
    private static final long SEED = System.currentTimeMillis();

    public LobSortTest(String name) {
        super(name);
    }

    /**
     * Overridden naming method which includes the seed used for the random
     * generator.
     * <p>
     * The seed is required if one wants to replay a specific sequence for
     * debugging purposes.
     *
     * @return The name of the test.
     */
    public String getName() {
        return (super.getName() + "-" + SEED);
    }

    public void testBlobMixed()
            throws SQLException {
        fetchIterateGetLengthBlob(
                "select blen, b from MIXED_LOBS order by length(b)");
    }

    public void testBlobSmall()
            throws SQLException {
        fetchIterateGetLengthBlob("select blen, b from MIXED_LOBS " +
                                  "where blen < 2000 order by length(b)");
    }

    public void testBlobLarge()
            throws SQLException {
        fetchIterateGetLengthBlob("select blen, b from MIXED_LOBS " +
                    "where blen > 34000 order by length(b)");
    }

    public void testBlobClob()
            throws SQLException {
        fetchIterateGetLengthBlob(
                "select blen, b from MIXED_LOBS order by length(c), length(b)");
    }

    public void testBlobRandom()
            throws SQLException {
        fetchIterateGetLengthBlob(
                "select blen, b from MIXED_LOBS order by rnd");
    }

    public void testClobMixed()
            throws SQLException {
        fetchIterateGetLengthClob(
                "select clen, c from MIXED_LOBS order by length(c)");
    }

    public void testClobSmall()
            throws SQLException {
        fetchIterateGetLengthClob("select clen, c from MIXED_LOBS " +
                                  "where clen < 2000 order by length(c)");
    }

    public void testClobLarge()
            throws SQLException {
        fetchIterateGetLengthClob("select clen, c from MIXED_LOBS " +
                    "where clen > 34000 order by length(c)");
    }

    public void testClobBlob()
            throws SQLException {
        fetchIterateGetLengthClob(
                "select clen, c from MIXED_LOBS order by length(b), length(c)");
    }

    public void testClobRandom()
            throws SQLException {
        fetchIterateGetLengthClob(
                "select clen, c from MIXED_LOBS order by rnd");
    }

    /**
     * Executes the specified query two times, materializes the Blob on the
     * first run and gets the length through {@code Blob.length} on the second.
     * <p>
     * Note that the query must select a Blob column at index one and the length
     * at index two.
     *
     * @param sql query to execute
     * @throws SQLException if the test fails for some reason
     */
    private void fetchIterateGetLengthBlob(String sql)
            throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        // Materialize the BLOB value.
        while (rs.next()) {
            assertEquals(rs.getInt(1), rs.getBytes(2).length);
        }
        rs.close();
        rs = stmt.executeQuery(sql);
        // Get the BLOB value length through Blob.length
        while (rs.next()) {
            assertEquals(rs.getInt(1), (int)rs.getBlob(2).length());
        }
        rs.close();
        stmt.close();
    }

    /**
     * Executes the specified query two times, materializes the Clob on the
     * first run and gets the length through {@code Clob.length} on the second.
     * <p>
     * Note that the query must select a Clob column at index one and the length
     * at index two.
     *
     * @param sql query to execute
     * @throws SQLException if the test fails for some reason
     */
    private void fetchIterateGetLengthClob(String sql)
            throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        // Materialize the CLOB value.
        while (rs.next()) {
            assertEquals(rs.getInt(1), rs.getString(2).length());
        }
        rs.close();
        rs = stmt.executeQuery(sql);
        // Get the CLOB value length through Clob.length
        while (rs.next()) {
            assertEquals(rs.getInt(1), (int)rs.getClob(2).length());
        }
        rs.close();
        stmt.close();
    }

    public static Test suite() {
        Properties props = new Properties();
        // Adjust sort buffer size to trigger the bug situation with less data.
        props.setProperty("derby.storage.sortBufferMax", "4");
        TestSuite suite = new TestSuite(LobSortTest.class,
                                        "LobSortTestEmbedded");
        return new CleanDatabaseTestSetup(
                new SystemPropertyTestSetup(suite, props, true)) {
            /**
             * Generates a table with Blob and Clobs of mixed size.
             */
            protected void decorateSQL(Statement s)
                    throws SQLException {
                Random rnd = new Random(SEED);
                Connection con = s.getConnection();
                con.setAutoCommit(false);
                s.executeUpdate("create table MIXED_LOBS (" +
                        "c clob, clen int, b blob, blen int, rnd int)");
                PreparedStatement ps = con.prepareStatement(
                        "insert into MIXED_LOBS values (?,?,?,?,?)");
                // Make sure we get at least one zero-length CLOB and BLOB.
                ps.setString(1, "");
                ps.setInt(2, 0);
                ps.setBytes(3, new byte[0]);
                ps.setInt(4, 0);
                ps.setInt(5, rnd.nextInt());
                ps.executeUpdate();
                for (int i=0; i < 100; i++) {
                    CharAlphabet ca = getCharAlphabet(1 + rnd.nextInt(3));
                    int length = (int)(rnd.nextDouble() * 64.0 * 1024.0);
                    if (rnd.nextInt(1000) < 500) {
                        // Specify the length.
                        ps.setCharacterStream( 1,
                                new LoopingAlphabetReader(length, ca), length);
                    } else {
                        // Don't specify the length.
                        ps.setCharacterStream(1,
                                new LoopingAlphabetReader(length, ca));
                    }
                    ps.setInt(2, length);
                    length = (int)(rnd.nextDouble() * 64.0 * 1024.0);
                    if (rnd.nextInt(1000) < 500) {
                        // Specify the length.
                        ps.setBinaryStream(3,
                                new LoopingAlphabetStream(length), length);
                    } else {
                        // Don't specify the length.
                        ps.setBinaryStream(3,
                                new LoopingAlphabetStream(length));
                    }
                    ps.setInt(4, length);
                    ps.setInt(5, rnd.nextInt());
                    ps.executeUpdate();
                }
                con.commit();
                ps.close();
            }

            /**
             * Returns a character alphabet.
             */
            private CharAlphabet getCharAlphabet(int i) {
                switch (i) {
                    case 1:
                        return CharAlphabet.modernLatinLowercase();
                    case 2:
                        return CharAlphabet.tamil();
                    case 3:
                        return CharAlphabet.cjkSubset();
                    default:
                        fail("Unknown alphabet identifier: " + i);
                }
                // Will never be reached.
                return null;
            }
        };
    }
}

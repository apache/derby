/*
 * Derby - Class org.apache.derbyTesting.perf.basic.jdbc.IndexScanTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.perf.basic.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Test the performance of different kinds of index scans. This test was
 * written in order to test the performance impact of the changes made in
 * DERBY-2991.
 */
public class IndexScanTest extends JDBCPerfTestCase {

    /** The prepared statement for the SQL expression that we want to test. */
    private PreparedStatement preparedStmt;

    /**
     * Create an instance of the test.
     * @param name the name of the method to call in the test
     * @param iterations the number of iteration in each test run
     * @param repeats the number of times each test run is repeated
     */
    private IndexScanTest(String name, int iterations, int repeats) {
        super(name, iterations, repeats);
    }

    /**
     * Get the prepared statement that we use in the test. If this is the
     * first time the method is called on this object, prepare the statement
     * and then return it.
     * @param sql the SQL text to prepare if the statement isn't already
     *            compiled
     * @return the prepared statement to use in this test
     */
    private PreparedStatement getOrPrepareStatement(String sql)
            throws SQLException {
        if (preparedStmt == null) {
            preparedStmt = prepareStatement(sql);
        }
        return preparedStmt;
    }

    /**
     * Do the necessary clean-up after running the test.
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        // Null out the statement to allow it to be gc'ed. It will be closed
        // automatically by the framework.
        preparedStmt = null;
    }

    /**
     * Create a test suite with all the test cases in this class.
     * @return a test suite
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("IndexScanTest");
        suite.addTest(new IndexScanTest("varchar10", 5000, 4));
        suite.addTest(new IndexScanTest("varchar100", 5000, 4));
        suite.addTest(new IndexScanTest("varchar1000", 5000, 4));
        suite.addTest(new IndexScanTest("varcharAll", 5000, 4));
        suite.addTest(new IndexScanTest("decimal1column", 5000, 4));
        suite.addTest(new IndexScanTest("decimal10columns", 5000, 4));

        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement s) throws SQLException {
                // Create a table with some character data and decimal data
                // that we can use in our tests.
                s.execute("CREATE TABLE T (VC10 VARCHAR(10), " +
                        "VC100 VARCHAR(100), " +
                        "VC1000 VARCHAR(1000), " +
                        "DEC1 DECIMAL(10,10), " +
                        "DEC2 DECIMAL(10,10), " +
                        "DEC3 DECIMAL(10,10), " +
                        "DEC4 DECIMAL(10,10), " +
                        "DEC5 DECIMAL(10,10), " +
                        "DEC6 DECIMAL(10,10), " +
                        "DEC7 DECIMAL(10,10), " +
                        "DEC8 DECIMAL(10,10), " +
                        "DEC9 DECIMAL(10,10), " +
                        "DEC10 DECIMAL(10,10))");

                // Fill the table with 1000 rows containing random data.
                PreparedStatement ps = s.getConnection().prepareStatement(
                        "INSERT INTO T(VC10,VC100,VC1000,DEC1,DEC2,DEC3," +
                        "DEC4,DEC5,DEC6,DEC7,DEC8,DEC9,DEC10) VALUES (?,?,?," +
                        "RANDOM(),RANDOM(),RANDOM(),RANDOM(),RANDOM()," +
                        "RANDOM(),RANDOM(),RANDOM(),RANDOM(),RANDOM())");
                char[] chars = new char[1000];
                Random r = new Random();
                for (int i = 0; i < 1000; i++) {
                    fillWithRandomChars(r, chars);
                    ps.setString(1, new String(chars, 0, 10));
                    ps.setString(2, new String(chars, 0, 100));
                    ps.setString(3, new String(chars, 0, 1000));
                    ps.executeUpdate();
                }
                ps.close();

                // Create various indexes on the table.
                s.execute("CREATE INDEX T_VC10 ON T(VC10)");
                s.execute("CREATE INDEX T_VC100 ON T(VC100)");
                s.execute("CREATE INDEX T_VC1000 ON T(VC1000)");
                s.execute("CREATE INDEX T_VC_ALL ON T(VC10,VC100,VC1000)");
                s.execute("CREATE INDEX T_DEC1 ON T(DEC1)");
                s.execute("CREATE INDEX T_DEC_ALL ON T(DEC1,DEC2,DEC3,DEC4," +
                        "DEC5,DEC6,DEC7,DEC8,DEC9,DEC10)");
            }
        };
    }

    /**
     * Fill a {@code char} array with random characters.
     * @param r a random number generator
     * @param chars the array to fill
     */
    private static void fillWithRandomChars(Random r, char[] chars) {
        String alphabet =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < chars.length; i++) {
            chars[i] = alphabet.charAt(r.nextInt(alphabet.length()));
        }
    }

    /**
     * Test the performance of an index scan on a VARCHAR(10) column.
     */
    public void varchar10() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
                "SELECT VC10 FROM T --DERBY-PROPERTIES index=T_VC10");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertNotNull(rs.getString(1));
        }
        rs.close();
    }

    /**
     * Test the performance of an index scan on a VARCHAR(100) column.
     */
    public void varchar100() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
                "SELECT VC100 FROM T --DERBY-PROPERTIES index=T_VC100");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertNotNull(rs.getString(1));
        }
        rs.close();
    }

    /**
     * Test the performance of an index scan on a VARCHAR(1000) column.
     */
    public void varchar1000() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
                "SELECT VC1000 FROM T --DERBY-PROPERTIES index=T_VC1000");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertNotNull(rs.getString(1));
        }
        rs.close();
    }

    /**
     * Test the performance of an index scan with a compound index on
     * columns with type VARCHAR(10), VARCHAR(100) and VARCHAR(1000).
     */
    public void varcharAll() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
          "SELECT VC10,VC100,VC1000 FROM T --DERBY-PROPERTIES index=T_VC_ALL");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            for (int col = 1; col <= 3; col++) {
                assertNotNull(rs.getString(col));
            }
        }
        rs.close();
    }

    /**
     * Test the performance of an index scan on a DECIMAL(10,10) column.
     */
    public void decimal1column() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
                "SELECT DEC1 FROM T --DERBY-PROPERTIES index=T_DEC1");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertNotNull(rs.getBigDecimal(1));
        }
        rs.close();
    }

    /**
     * Test the performance of an index scan on a compound index on ten
     * DECIMAL(10,10) columns.
     */
    public void decimal10columns() throws SQLException {
        PreparedStatement ps = getOrPrepareStatement(
                "SELECT DEC1,DEC2,DEC3,DEC4,DEC5,DEC6,DEC7,DEC8,DEC9,DEC10 " +
                "FROM T --DERBY-PROPERTIES index=T_DEC_ALL");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            for (int col = 1; col <= 10; col++) {
                assertNotNull(rs.getBigDecimal(col));
            }
        }
        rs.close();
    }
}

/*
 * Derby - Class org.apache.derbyTesting.perf.basic.jdbc.SelectDistinctTest
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

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Performance tests for SELECT DISTINCT.
 */
public class SelectDistinctTest extends JDBCPerfTestCase {

    /** The prepared statement to be used in the test body. */
    private PreparedStatement ps;

    /**
     * Name of the test case for CHAR(5), and the name of the table used by
     * that test.
     */
    private static final String TEST_CHAR5 = "selectDistinctChar5";

    /**
     * The number of distinct rows to be expected in the test for CHAR(5).
     * Since the table contains all permutations of "ABCDE", there should be
     * 5! distinct rows.
     */
    private static final int EXPECTED_DISTINCT_CHAR5 = 5 * 4 * 3 * 2;

    /**
     * Name of the test case for CHAR(5) FOR BIT DATA, and the name of the
     * table used by that test.
     */
    private static final String TEST_BINARY5 = "selectDistinctBinary5";

    /**
     * The number of distinct rows to be expected in the test for CHAR(5) FOR
     * BIT DATA. Should be the same as in the test for CHAR(5).
     */
    private static final int EXPECTED_DISTINCT_BINARY5 =
            EXPECTED_DISTINCT_CHAR5;

    /**
     * Create a test case.
     *
     * @param name the name of the test method, and the name of the table used
     * in the test
     * @param iterations the number of iterations in each test
     * @param repeats the number of times each test should be repeated
     */
    private SelectDistinctTest(String name, int iterations, int repeats) {
        super(name, iterations, repeats);
    }

    /**
     * Create the suite of all test cases in this class.
     *
     * @return all test cases in this class
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("SelectDistinctTest");
        suite.addTest(new SelectDistinctTest(TEST_CHAR5, 2000, 4));
        suite.addTest(new SelectDistinctTest(TEST_BINARY5, 2000, 4));

        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("CREATE TABLE " + TEST_CHAR5 + "(COL CHAR(5))");
                // insert all permutations of 'ABCDE'
                s.execute("INSERT INTO " + TEST_CHAR5 +
                        " SELECT V1.X||V2.X||V3.X||V4.X||V5.X FROM " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') V1(X), " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') V2(X), " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') V3(X), " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') V4(X), " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') V5(X) " +
                        "WHERE (SELECT COUNT(*) FROM " +
                        "(VALUES 'A', 'B', 'C', 'D', 'E') VV(X) " +
                        "WHERE VV.X NOT IN (V1.X,V2.X,V3.X,V4.X,V5.X)) = 0");
                // make some duplicates
                for (int i = 0; i < 3; i++) {
                    s.execute("INSERT INTO " + TEST_CHAR5 + " " +
                            "SELECT * FROM " + TEST_CHAR5);
                }

                s.execute("CREATE TABLE " + TEST_BINARY5 +
                        "(COL CHAR(5) FOR BIT DATA)");
                s.execute("CREATE FUNCTION CHAR_TO_BINARY(STR CHAR(5)) " +
                        "RETURNS CHAR(5) FOR BIT DATA " +
                        "LANGUAGE JAVA PARAMETER STYLE JAVA " +
                        "EXTERNAL NAME '" + SelectDistinctTest.class.getName() +
                        ".charToBinary' NO SQL");
                s.execute("INSERT INTO " + TEST_BINARY5 +
                        " SELECT CHAR_TO_BINARY(COL) FROM " + TEST_CHAR5);
            }
        };
    }

    /**
     * Set up the test environment.
     */
    protected void setUp() throws SQLException {
        ps = prepareStatement("SELECT DISTINCT * FROM " + getName());
    }

    /**
     * Tear down the test environment.
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        ps = null; // automatically closed in super.tearDown()
    }

    /**
     * Stored function that converts an SQL CHAR value to an SQL CHAR FOR BIT
     * DATA value.
     *
     * @param s the string to convert to binary data
     * @return binary UTF-8 representation of the string
     */
    public static byte[] charToBinary(String s)
            throws UnsupportedEncodingException {
        return s.getBytes("UTF-8");
    }

    /**
     * Test case for SELECT DISTINCT on a CHAR(5) column.
     */
    public void selectDistinctChar5() throws SQLException {
        JDBC.assertDrainResults(ps.executeQuery(), EXPECTED_DISTINCT_CHAR5);
    }

    /**
     * Test case for SELECT DISTINCT on a CHAR(5) FOR BIT DATA column.
     */
    public void selectDistinctBinary5() throws SQLException {
        JDBC.assertDrainResults(ps.executeQuery(), EXPECTED_DISTINCT_BINARY5);
    }
}

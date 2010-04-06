/*

Derby - Class org.apache.derbyTesting.perf.basic.jdbc.HeapScan

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
package org.apache.derbyTesting.perf.basic.jdbc;


import java.sql.*;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Heap Scan tests
 *
 */
public class HeapScan extends JDBCPerfTestCase {

    PreparedStatement select = null;
    private PreparedStatement selectWithPred;
    protected static String tableName = "SCANTEST";
    protected static int rowcount = 10000;
    private boolean binaryData;

    /**
     * @return suite of tests
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("HeapScanTests");
        suite.addTest(baseSuite("HeapScan:CHAR", false));
        suite.addTest(baseSuite("HeapScan:BINARY", true));
        return suite;
    }

    /**
     * Create a suite of all the tests in this class with the appropriate
     * decorator.
     *
     * @param name the name of the returned test suite
     * @param binaryData whether or not these tests should use binary data
     * instead of character data
     * @return a test suite
     */
    private static Test baseSuite(String name, boolean binaryData) {
        int iterations = 700, repeats = 4;

        TestSuite heapScan = new TestSuite(name);
        heapScan.addTest(new HeapScan("Scan100", binaryData,
                                      iterations, repeats));
        heapScan.addTest(new HeapScan("Scan100GetData", binaryData,
                                      iterations, repeats));
        heapScan.addTest(new HeapScan("Scan100WithPredicate", binaryData,
                                      iterations, repeats));
        return new BaseLoad100TestSetup(
                heapScan, rowcount, tableName, binaryData);
    }

    /**
     * Scan tests.
     * @param name test name
     * @param iterations iterations of the test to measure
     * @param repeats number of times to repeat the test
     */
    public HeapScan(String name,int iterations, int repeats)
    {
        this(name, false, iterations, repeats);
    }

    /**
     * Scan tests.
     * @param name test name
     * @param binaryData whether or not binary data should be used instead
     *                   of character data
     * @param iterations iterations of the test to measure
     * @param repeats number of times to repeat the test
     */
    public HeapScan(String name, boolean binaryData,
                    int iterations, int repeats)
    {
        super(name,iterations,repeats);
        this.binaryData = binaryData;
    }

    /**
     * Do the necessary setup for the test ,prepare the statement
     */
    public void setUp() throws Exception {

        select = openDefaultConnection().prepareStatement("SELECT * FROM "+tableName);

        // Create a SELECT statement that uses predicates. Also initialize
        // the predicates with some data of the correct type for this test
        // (either character data or binary data).
        selectWithPred = prepareStatement(
                "SELECT * FROM " + tableName + " WHERE " +
                "c6=? OR c7=? OR c8=? OR c9=?");
        Object predicate = "abcdef";
        if (binaryData) {
            predicate = ((String) predicate).getBytes("US-ASCII");
        }
        for (int i = 1; i <= 4; i++) {
            selectWithPred.setObject(i, predicate);
        }
    }


    /**
     * Override initializeConnection to set the autocommit to false
     */
    public void initializeConnection(Connection conn)
    throws SQLException
    {
        conn.setAutoCommit(false);
    }


    /**
     * This test simply tests a heap scan which iterates through all the
     * rows in the columns. The column data are not retrieved using getXXX
     * @throws Exception
     */
    public void Scan100() throws Exception
    {

        ResultSet rs = select.executeQuery();
        int actualCount = 0;
        while (rs.next()) {
            actualCount++;
        }

        assertEquals(actualCount,rowcount);
        rs.close();
        getConnection().commit();

    }

    /**
     * This test simply tests a heap scan which iterates through all the
     * rows in the columns. The column data are retrieved using getXXX
     * @throws Exception
     */
    public void Scan100GetData() throws Exception
    {
        ResultSet rs = select.executeQuery();

        int actualCount = 0;
        while (rs.next()) {

            int i1 = rs.getInt(1);
            int i2 = rs.getInt(2);
            int i3 = rs.getInt(3);
            int i4 = rs.getInt(4);
            int i5 = rs.getInt(5);

            Object c6 = rs.getObject(6);
            Object c7 = rs.getObject(7);
            Object c8 = rs.getObject(8);
            Object c9 = rs.getObject(9);

            actualCount++;
        }
        assertEquals(actualCount,rowcount);
        getConnection().commit();
        rs.close();
    }

    /**
     * Test the performance of a table scan that needs to compare all the
     * char values in the table with some specified values. Used to test the
     * performance gains in DERBY-4608.
     */
    public void Scan100WithPredicate() throws SQLException {
        ResultSet rs = selectWithPred.executeQuery();
        assertFalse("should be empty", rs.next());
        rs.close();
        commit();
    }

    /**
     * Cleanup - close resources opened in this test.
     **/
    public void tearDown() throws Exception {

        select.close();
        select = null;
        selectWithPred = null; // will be closed in super.tearDown()
        super.tearDown();
    }
}

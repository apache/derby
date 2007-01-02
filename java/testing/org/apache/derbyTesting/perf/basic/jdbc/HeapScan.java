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
    protected static String tableName = "SCANTEST";
    protected static int rowcount = 10000;

    /**
     * @return suite of tests
     */
    public static Test suite()
    {
        int iterations = 700, repeats = 4;

        TestSuite heapScan = new TestSuite("HeapScanTests");
        heapScan.addTest(new HeapScan("Scan100",iterations,repeats));
        heapScan.addTest(new HeapScan("Scan100GetData",iterations,repeats));
        return new BaseLoad100TestSetup(heapScan,rowcount,tableName);
    }

    /**
     * Scan tests.
     * @param name test name
     * @param iterations iterations of the test to measure
     * @param repeats number of times to repeat the test
     */
    public HeapScan(String name,int iterations, int repeats)
    {
        super(name,iterations,repeats);
    }

    /**
     * Do the necessary setup for the test ,prepare the statement
     */
    public void setUp() throws Exception {

        select = openDefaultConnection().prepareStatement("SELECT * FROM "+tableName);
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

            String c6 = rs.getString(6);
            String c7 = rs.getString(7);
            String c8 = rs.getString(8);
            String c9 = rs.getString(9);

            actualCount++;
        }
        assertEquals(actualCount,rowcount);
        getConnection().commit();
        rs.close();
    }

    /**
     * Cleanup - close resources opened in this test.
     **/
    public void tearDown() throws Exception {

        select.close();
        select = null;
        super.tearDown();
    }
}

/*

 Derby - Class org.apache.derbyTesting.perf.basic.jdbc.CoveredIdxScan

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

/**
 * Index scan tests.
 *
 */
public class CoveredIdxScan extends HeapScan {

    /**
     * @return suite of tests
     */
    public static Test suite()
    {
        TestSuite scan = new TestSuite("CoveredIdxScanTests");
        int iterations = 700, repeats = 4;

        scan.addTest(new CoveredIdxScan("ScanCoveredIdxInt",iterations,repeats));

        return new BaseLoad100IdxTestSetup(scan,rowcount*2,tableName);


    }

    /**
     * Constructor
     * @param name testname
     * @param iterations iterations for the test to measure
     * @param repeats number of times to repeat the test
     */
    public CoveredIdxScan(String name,int iterations, int repeats)
    {
        super(name,iterations,repeats);
    }

    /**
     * Do the necessary setup for the test ,prepare the statement
     */
    public void setUp() throws Exception {

        select = prepareStatement("SELECT i1 FROM " + tableName +
        " WHERE i1 > ? and i1 <= ?");
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
     * This test simply tests a covered index scan and retrieves an int column
     * Scan starts from 1/4 into the data set and set to end 3/4 into the
     * dataset
     * @throws Exception
     */
    public void ScanCoveredIdxInt() throws Exception
    {

        // set begin scan to start 1/4 into the data set.
        select.setInt(1, ((rowcount * 2) / 4));

        // set end scan to end 3/4 into the data set.
        select.setInt(2, (((rowcount * 2) / 4) * 3));

        ResultSet rs = select.executeQuery();

        int actualCount = 0;
        int i = 0;
        while (rs.next())
        {
            i = rs.getInt(1);
            actualCount++;
        }
        assertEquals(rowcount,actualCount);
        rs.close();
        commit();
    }


    /**
     * Cleanup - close resources opened in this test.
     */
    public void tearDown() throws Exception {
        super.tearDown();
    }
}

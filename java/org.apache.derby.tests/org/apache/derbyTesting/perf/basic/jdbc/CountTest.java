/*

Derby - Class org.apache.derbyTesting.perf.basic.jdbc.CountTest

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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Add tests to measure performance of count.
 */
public class CountTest extends JDBCPerfTestCase {

    PreparedStatement select = null;
    static String tableName = "COUNTTEST";
    protected static int rowcount = 10000;

    /**
     * Suite of tests to return.
     */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("CountTest");
        int iterations = 1200, repeats = 4;

        suite.addTest(new CountTest("Count100",iterations,repeats));
        return new BaseLoad100TestSetup(suite,rowcount,tableName);
    }

    /**
     * Constructor -create a CountTest
     * @param name testname
     * @param iterations iterations for the test to measure
     * @param repeats number of times to repeat the test
     */
    public CountTest(String name,int iterations, int repeats)
    {
        super(name,iterations,repeats);
    }

    /**
     * setup for the test.
     **/
    public void setUp() throws Exception {

        select = openDefaultConnection().prepareStatement("SELECT COUNT(i1) FROM "+tableName);
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
     * Execute the count query for the BaseLoad100TestSetup schema.
     * @throws Exception
     */
    public void Count100() throws Exception
    {
        ResultSet rs = select.executeQuery();
        rs.next();
        assertEquals(rowcount,rs.getInt(1));
        rs.close();
        getConnection().commit();
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

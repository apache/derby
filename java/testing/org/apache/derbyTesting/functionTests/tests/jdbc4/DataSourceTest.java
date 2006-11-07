/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.DataSourceTest

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

import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.*;
import javax.sql.*;

/**
 * Tests of the <code>javax.sql.DataSource</code> JDBC40 API.
 */

public class DataSourceTest extends BaseJDBCTestCase {
    
    //Default DataSource that will be used by the tests
    private DataSource ds = null;
    
    /**
     *
     * Create a test with the given name.
     *
     * @param name name of the test.
     *
     */
    public DataSourceTest(String name) {
        super(name);
    }
    
    /**
     * Create a default DataSource
     */
    public void setUp() {
        ds = JDBCDataSource.getDataSource();
    }
    
    /**
     * 
     * Initialize the ds to null once the tests that need to be run have been 
     * run
     */
    public void tearDown() {
        ds = null;
    }

    public void testIsWrapperForDataSource() throws SQLException {
        assertTrue(ds.isWrapperFor(DataSource.class));
    }

    public void testIsNotWrapperForPoolDataSource() throws SQLException {
        assertFalse(ds.isWrapperFor(ConnectionPoolDataSource.class));
    }

    public void testIsNotWrapperForXADataSource() throws SQLException {
        assertFalse(ds.isWrapperFor(XADataSource.class));
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(ds.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapDataSource() throws SQLException {
        DataSource ds2 = ds.unwrap(DataSource.class);
        assertSame("Unwrap returned wrong object.", ds, ds2);
    }

    public void testUnwrapConnectionPoolDataSource() {
        try {
            ConnectionPoolDataSource cpds =
                ds.unwrap(ConnectionPoolDataSource.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapXADataSource() {
        try {
            XADataSource xads = ds.unwrap(XADataSource.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = ds.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(DataSourceTest.class);
    }
}

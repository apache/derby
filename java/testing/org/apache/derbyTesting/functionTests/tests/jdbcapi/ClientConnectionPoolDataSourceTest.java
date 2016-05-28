/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ClientConnectionPoolDataSourceTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.PooledConnection;

import junit.framework.Test;
import org.apache.derby.jdbc.ClientConnectionPoolDataSourceInterface;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Basic tests of the {@code ConnectionPoolDataSource} in the client driver.
 */
public class ClientConnectionPoolDataSourceTest
    extends BaseJDBCTestCase {

    public ClientConnectionPoolDataSourceTest(String name) {
        super(name);
    }

    /**
     * Verify that handling of the {@code maxStatements} property is working.
     */
    public void testMaxStatementsProperty() throws Exception {
        ClientConnectionPoolDataSourceInterface cDs;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientConnectionPoolDataSource");
            cDs = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40");
            cDs = (ClientConnectionPoolDataSourceInterface) clazz.getConstructor().newInstance();
        }
        // Check the default value.
        assertEquals("Unexpected default value", 0, cDs.getMaxStatements());
        cDs.setMaxStatements(25);
        // Verify that the new value has been set.
        assertEquals("New value not set", 25, cDs.getMaxStatements());
        // Try a negative value
        try {
            cDs.setMaxStatements(-99);
            fail("Negative values should not be allowed: " +
                    cDs.getMaxStatements());
        } catch (IllegalArgumentException iae) {
            // As expected, continue the test.
        }
        // Try setting it to zero to disable statement pooling.
        cDs.setMaxStatements(0);
        assertEquals("New value not set", 0, cDs.getMaxStatements());
    }

    /**
     * Tests basic connectivity when connection is obtained from a connection
     * pool data source without statement pooling.
     *
     * @throws SQLException if database operations fail
     */
    public void testGetConnectionNoStatementPooling()
            throws SQLException {
        ClientConnectionPoolDataSourceInterface cDs =
            (ClientConnectionPoolDataSourceInterface)J2EEDataSource.
                getConnectionPoolDataSource();
        // Make sure statement pooling is disabled.
        cDs.setMaxStatements(0);
        assertEquals(0, cDs.getMaxStatements());
        verifyConnection(cDs);
    }

    /**
     * Tests basic connectivity when connection is obtained from a connection
     * pool data source with statement pooling enabled.
     *
     * @throws SQLException if database operations fail
     */
    public void testGetConnectionWithStatementPooling()
            throws SQLException {
        ClientConnectionPoolDataSourceInterface cDs =
            (ClientConnectionPoolDataSourceInterface)J2EEDataSource.
                getConnectionPoolDataSource();
        // Enable statement pooling.
        cDs.setMaxStatements(27);
        assertTrue(cDs.getMaxStatements() > 0);
        verifyConnection(cDs);
    }

    /**
     * Do some basic verification on a connection obtained from the data source.
     *
     * @param cDs data source to get connection from
     * @throws SQLException if a JDBC operation fails
     */
    private void verifyConnection(ClientConnectionPoolDataSourceInterface cDs)
            throws SQLException {
        J2EEDataSource.setBeanProperty(cDs, "createDatabase", "create");
        PooledConnection pc = cDs.getPooledConnection();
        // Get a connection and make sure we can access the database.
        Connection con = pc.getConnection();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from sys.systables");
        JDBC.assertDrainResultsHasData(rs);
        PreparedStatement ps1 = con.prepareStatement("values 31");
        JDBC.assertSingleValueResultSet(ps1.executeQuery(), "31");
        ps1.close();
        PreparedStatement ps2 = con.prepareStatement("values 31");
        // The physical statement is supposed to be the same, but not the
        // logical prepared statements (if pooling is used).
        assertNotSame(ps1, ps2);
        JDBC.assertSingleValueResultSet(ps2.executeQuery(), "31");
        // Close everything
        stmt.close();
        ps2.close();
        con.close();
        pc.close();
    }

    /**
     * Returns a suite that will run only in the client-server configuration.
     *
     * @return A client-server suite with all the tests.
     */
    public static Test suite() {
        // The tests are run in the client-server configuration only, because
        // the code being tests does not exist in the embedded driver.
        return TestConfiguration.clientServerSuite(
                ClientConnectionPoolDataSourceTest.class);
    }
}

/*

   Derby - Class 
   org.apache.derbyTesting.functionTests.tests.jdbcapi.ConnectionPoolDataSourceAuthenticationTest

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

import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

//Extends AuthenticationTest.java which only holds DataSource calls.
//This class implements the checks for ConnectionPoolDataSources
public class PoolDSAuthenticationTest extends AuthenticationTest {

    /** Creates a new instance of the Test */
    public PoolDSAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses ConnectionPoolDataSource and so is not suitable for 
        // JSR169
        if (JDBC.vmSupportsJSR169())
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("ConnectionPoolDataSource not available" +
                " with JSR169; empty test");
        else {
            BaseTestSuite suite =
                new BaseTestSuite("PoolDSAuthenticationTest");

            suite.addTest(baseSuite("PoolDSAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(baseSuite(
                "PoolDSAuthenticationTest:client")));
            return suite;
        }
    }
    
    // baseSuite takes advantage of setting system properties as defined
    // in AuthenticationTest
    public static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite("PoolDSAuthenticationTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        Test test = new PoolDSAuthenticationTest(
            "testConnectShutdownAuthentication");
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
        setBaseProps(suite, test);
        
        test = new PoolDSAuthenticationTest("testUserFunctions");
        setBaseProps(suite, test);

        test = new PoolDSAuthenticationTest("testNotFullAccessUsers");
        setBaseProps(suite, test);
        
        test = new PoolDSAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        setBaseProps(suite, test);

        // only part of this fixture runs with network server / client
        test = new PoolDSAuthenticationTest("testGreekCharacters");
        setBaseProps(suite, test);
        
        test = new PoolDSAuthenticationTest("testSystemShutdown");
        setBaseProps(suite, test);

        // The test needs to run in a new single use database as we're
        // setting a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }

    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        pds.getPooledConnection(user, password).close();
    }

    protected void assertConnectionWOUPOK(
        String dbName, String user, String password)
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        pds.getPooledConnection().close();
    }

    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        try {
            pds.getPooledConnection(user, password);
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertConnectionWOUPFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        try {
            pds.getPooledConnection();
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownUsingSetShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-2296
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase", "shutdown");
        try {
            pds.getPooledConnection(user, password);
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }
    
    protected void assertShutdownUsingConnAttrsOK(
        String dbName, String user, String password) throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(
            pds, "connectionAttributes", "shutdown=true");
        try {
            pds.getPooledConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase","shutdown");
        try {
            pds.getPooledConnection();
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-2296
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        try {
            pds.getPooledConnection(user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-2296
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        try {
            pds.getPooledConnection();
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    // using an empty dbName is interpreted as system shutdown
    protected void assertSystemShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
        JDBCDataSource.clearStringBeanProperty(pds, "databaseName");
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        try {
            pds.getPooledConnection();
            fail("expected system shutdown resulting in XJ015 error");
        } catch (SQLException e) {
            // expect XJ015, system shutdown, on successful shutdown
            assertSQLState("XJ015", e);
        }
    }

    protected void assertSystemShutdownFail(
            String expectedError, String dbName, String user, String password)
    throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
        JDBCDataSource.clearStringBeanProperty(pds, "databaseName");
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(pds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(pds, "user", user);
        JDBCDataSource.setBeanProperty(pds, "password", password);
        try {
            pds.getPooledConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedError, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
        // Reset to no user/password though client requires
        // a valid name, so reset to the default
        if (usingDerbyNetClient())
            JDBCDataSource.setBeanProperty(pds, "user", "APP");
        else
            JDBCDataSource.clearStringBeanProperty(pds, "user");
        JDBCDataSource.clearStringBeanProperty(pds, "password");
        JDBCDataSource.setBeanProperty(pds, "databaseName", dbName);
        try {
            pds.getPooledConnection();
            fail("expected connection to fail");
        } catch (SQLException e) {
            assertSQLState("08004", e);
        }
    }
}

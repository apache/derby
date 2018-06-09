/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.jdbcapi.XADSAuthenticationTest

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
import javax.sql.XADataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

//Extends AuthenticationTest.java which only holds XADataSource calls.
//This class implements the checks for XADataSources
public class XADSAuthenticationTest extends AuthenticationTest {
    
    /** Creates a new instance of the Test */
    public XADSAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses XADataSource and so is not suitable for JSR169
        if (JDBC.vmSupportsJSR169())
            return new BaseTestSuite("");
        else {
            BaseTestSuite suite = new BaseTestSuite("XADSAuthenticationTest");
            suite.addTest(baseSuite("XADSAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("XADSAuthenticationTest:client")));
            return suite;
        }
    }
    
    // baseSuite takes advantage of setting system properties as defined
    // in AuthenticationTest
    public static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite("XADSAuthenticationTest");

        Test test = new XADSAuthenticationTest(
            "testConnectShutdownAuthentication");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest("testUserFunctions");
        setBaseProps(suite, test);

        test = new XADSAuthenticationTest("testNotFullAccessUsers");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        setBaseProps(suite, test);

        // only part of this fixture runs with network server / client
        test = new XADSAuthenticationTest("testGreekCharacters");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest("testSystemShutdown");
        setBaseProps(suite, test);

        // The test needs to run in a new single use database as we're setting
        // a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException 
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        xads.getXAConnection(user, password).close();

    }

    protected void assertConnectionWOUPOK(
            String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        xads.getXAConnection().close();
    }
    
    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection(user, password);
            fail("Connection should've been refused/failed");
        } catch (SQLException sqle) {
            assertSQLState(expectedSqlState, sqle);
        }
    }

    protected void assertConnectionWOUPFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownUsingSetShutdownOK(
            String dbName, String user, String password) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
            xads, "shutdownDatabase", "shutdown");
        try {
            xads.getXAConnection(user, password);
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownUsingConnAttrsOK(
        String dbName, String user, String password) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
            xads, "connectionAttributes", "shutdown=true");
        try {
            xads.getXAConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
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
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection(user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
            
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    protected void assertSystemShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected system shutdown resulting in XJ015 error");
        } catch (SQLException e) {
            // expect XJ015, system shutdown, on successful shutdown
            assertSQLState("XJ015", e);
        }
    }

    protected void assertSystemShutdownFail(
        String expectedError, String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedError, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        // Reset to no user/password though client requires
        // a valid name, so reset to the default
        if (usingDerbyNetClient())
            JDBCDataSource.setBeanProperty(xads, "user", "APP");
        else
            JDBCDataSource.clearStringBeanProperty(xads, "user");
        JDBCDataSource.clearStringBeanProperty(xads, "password");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection();
            fail("expected connection to fail");
        } catch (SQLException e) {
            assertSQLState("08004", e);
        }
    }
}

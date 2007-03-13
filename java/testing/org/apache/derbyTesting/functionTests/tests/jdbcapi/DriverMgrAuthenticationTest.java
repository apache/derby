/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.jdbcapi.DriverMgrAuthenticationTest

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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


// Extends AuthenticationTest.java which only holds DataSource calls
// this class uses some of the same methods but uses DriverManager to
// obtain connections
public class DriverMgrAuthenticationTest extends AuthenticationTest {

    /** Creates a new instance of the Test */
    public DriverMgrAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses driverManager and so is not suitable for JSR169
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("DriverManager not available with JSR169;" +
                "empty DriverMgrAuthenticationTest");
        else {
            TestSuite suite = new TestSuite("DriverMgrAuthenticationTest");
            suite.addTest(
                baseSuite("DriverMgrAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("DriverMgrAuthenticationTest:client")));
            return suite;
        }
    }
    
    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("DriverMgrAuthenticationTest");

        // set a user at system level
        java.lang.System.setProperty("derby.user.system", "admin");
        java.lang.System.setProperty("derby.user.mickey", "mouse");
        
        // Use DatabasePropertyTestSetup decorator to set the user properties
        // required by this test (and shutdown the database for the
        // property to take effect).
        Properties props = new Properties();
        props.setProperty("derby.infolog.append", "true");
        props.setProperty("derby.debug.true", "AuthenticationTrace");

        Test test = new DriverMgrAuthenticationTest(
            "testConnectShutdownAuthentication");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        // DatabasePropertyTestSsetup uses SYSCS_SET_DATABASE_PROPERTY
        // so that is database level setting.
        test = new DriverMgrAuthenticationTest("testUserFunctions");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));

        test = new DriverMgrAuthenticationTest("testNotFullAccessUsers");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        test = new DriverMgrAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));

        // only part of this fixture runs with network server / client
        test = new DriverMgrAuthenticationTest("testGreekCharacters");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        // The test needs to run in a new single use database as we're
        // setting a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            assertNotNull(DriverManager.getConnection(url, user, password));
        }
        catch (SQLException e) {
                throw e;
        }
    }

    // getConnection(), using url connection attributes
    protected void assertConnectionWOUPOK(
        String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = url + ";user=" + user + ";password=" + password;
        try {
            assertNotNull(DriverManager.getConnection(url2));
        }
        catch (SQLException e) {
                throw e;
        }
    }

    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url, user, password);
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
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = url + ";user=" + user + ";password=" + password;
        try {
            DriverManager.getConnection(url2);
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownOK(
        String dbName, String user, String password)
    throws SQLException {

        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName) +
        ";shutdown=true";
    try {
        DriverManager.getConnection(url, user, password);
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    // differs from assertShutdownOK by using getConnection(url)
    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {

        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        url = url + ";shutdown=true;user=" + user + ";password=" + password;
        try {
            DriverManager.getConnection(url, null);
            fail ("expected a error after shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }
    
    protected void assertShutdownFail(
            String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName) +
            ";shutdown=true";      
        try {
            DriverManager.getConnection(url, user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    // differs from assertShutdownFail in using getConnection(url)
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException 
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            DriverManager.getConnection(url2);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    protected void assertSystemShutdownOK(
        String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        if (usingDerbyNetClient() && dbName=="")
            // The junit test harness has kicked off the test will hang when 
            // we attempt to shutdown the system - most likely because we're
            // shutting down the system while the network server thread is
            // still alive, so it gets confused...
            return;
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            DriverManager.getConnection(url2);
            fail("expected successful shutdown");
        } catch (SQLException e) {
            assertSQLState("XJ015", e);
        }
    }
    
    protected void assertSystemShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl();
        if (usingDerbyNetClient() && dbName=="")
            // The junit test harness has kicked off the test will hang when 
            // we attempt to shutdown the system - most likely because we're
            // shutting down the system while the network server thread is
            // still alive, so it gets confused...
            return;
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            DriverManager.getConnection(url2, user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        // this method needs to not use default user/pwd (APP, APP).
        
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url);
            fail("expected connection to fail");
        }
        catch (SQLException e) {
            assertSQLState("08004", e);
        }
    }
}

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
import java.util.Properties;
import javax.sql.XADataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.jdbc.ClientXADataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

//Extends AuthenticationTest.java which only holds XADataSource calls.
//This class implements the checks for XADataSources
public class XADSAuthenticationTest extends AuthenticationTest {
    
    private static XADataSource xads;
    
    /** Creates a new instance of the Test */
    public XADSAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses driverManager and so is not suitable for JSR169
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("");
        else {
            TestSuite suite = new TestSuite("XADSAuthenticationTest");
            suite.addTest(baseSuite("XADSAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("XADSAuthenticationTest:client")));
            return suite;
        }
    }
    
    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("XADSAuthenticationTest");

        // set a user at system level
        java.lang.System.setProperty("derby.user.system", "admin");
        java.lang.System.setProperty("derby.user.mickey", "mouse");
        
        // Use DatabasePropertyTestSetup decorator to set the user properties
        // required by this test (and shutdown the database for the
        // property to take effect).
        Properties props = new Properties();
        props.setProperty("derby.infolog.append", "true");
        props.setProperty("derby.debug.true", "AuthenticationTrace");

        Test test = new XADSAuthenticationTest(
            "testConnectShutdownAuthentication");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        // DatabasePropertyTestSsetup uses SYSCS_SET_DATABASE_PROPERTY
        // so that is database level setting.
        test = new XADSAuthenticationTest("testUserFunctions");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));

        test = new XADSAuthenticationTest("testNotFullAccessUsers");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        test = new XADSAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));

        // only part of this fixture runs with network server / client
        test = new XADSAuthenticationTest("testGreekCharacters");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
        
        // The test needs to run in a new single use database as we're setting
        // a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException 
    {
        xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            assertNotNull(xads.getXAConnection(user, password));
        } catch (SQLException sqle) {
            throw sqle;
        }
    }

    protected void assertConnectionWOUPOK(
            String dbName, String user, String password)
    throws SQLException
    {
        xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            assertNotNull(xads.getXAConnection());
        }
        catch (SQLException e) {
            throw e;
        }
    }
    
    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        xads = J2EEDataSource.getXADataSource();
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
        xads = J2EEDataSource.getXADataSource();
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
    
    protected void assertShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        if (usingEmbedded())
        {
            xads = J2EEDataSource.getXADataSource();
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
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setDatabaseName(dbName);
            xads.setConnectionAttributes("shutdown=true");
            try {
                xads.getXAConnection(user, password);
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
        }
    }

    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {
        if (usingEmbedded())
        {
            xads = J2EEDataSource.getXADataSource();
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
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setDatabaseName(dbName);
            xads.setConnectionAttributes(
                "shutdown=true;user=" + user + ";password=" + password);
            try {
                xads.getXAConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
        }
    }

    protected void assertShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        if (usingEmbedded()) 
        {
            xads = J2EEDataSource.getXADataSource();
            JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
            JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
            try {
                xads.getXAConnection(user, password);
                fail("expected failed shutdown");
            } catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
            }
        }
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setDatabaseName(dbName);
            xads.setConnectionAttributes("shutdown=true");
            try {
                xads.getXAConnection(user, password);
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
            }
        }
    }
            
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        if (usingEmbedded()) 
        {
            xads = J2EEDataSource.getXADataSource();
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
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setDatabaseName(dbName);
            xads.setConnectionAttributes(
                "shutdown=true;user=" + user + ";password=" + password);
            try {
                xads.getXAConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
            }
        }
    }

    protected void assertSystemShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        if (usingEmbedded())
        {
            xads = J2EEDataSource.getXADataSource();
            JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
            JDBCDataSource.setBeanProperty(xads, "user", user);
            JDBCDataSource.setBeanProperty(xads, "password", password);
            JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
            try {
                xads.getXAConnection();
                fail("expected system shutdown resulting in XJ015 error");
            } catch (SQLException e) {
                // expect XJ015, system shutdown, on successful shutdown
                assertSQLState("XJ015", e);
            }
        }
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setDatabaseName(dbName);
            xads.setConnectionAttributes(
                "shutdown=true;user=" + user + ";password=" + password);
            try {
                xads.getXAConnection(user, password);
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect XJ015 on successful shutdown
                assertSQLState("XJ015", e);
            }
        }
    }

    // Note, we need a separate method for fail & OK because something
    // the framework will add the wrong details. If we use
    // getDataSource(dbName), we don't get a successful XJ015, ever,
    // if we use getDataSource(), it appears the user/password on connect
    // is ignored, at least, we get XJ015 anyway.
    // 
    protected void assertSystemShutdownFail(
        String expectedError, String dbName, String user, String password)
    throws SQLException {
        if (usingEmbedded())
        {
            xads = J2EEDataSource.getXADataSource();
            JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
            JDBCDataSource.setBeanProperty(xads, "user", user);
            JDBCDataSource.setBeanProperty(xads, "password", password);
            try {
                xads.getXAConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                assertSQLState(expectedError, e);
            }
        }
        else if (usingDerbyNetClient())
        {
            ClientXADataSource xads = 
                (ClientXADataSource)J2EEDataSource.getXADataSource();
            xads.setConnectionAttributes(
                    "shutdown=true;user=" + user + ";password=" + password);
            try {
                xads.getXAConnection(user, password);
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                assertSQLState(expectedError, e);
            }
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        // can't rely on junit framework automatic methods for they'll
        // default the user / password which need to remain empty
        if (usingDerbyNetClient())
        {
            ClientXADataSource xads = new ClientXADataSource();
            xads.setDatabaseName(dbName);
            try {
                xads.getXAConnection();
                fail("expected connection to fail");
            } catch (SQLException e) {
                assertSQLState("08004", e);
            }
        }
        else if (usingEmbedded()) 
        {
            EmbeddedXADataSource xads = new EmbeddedXADataSource();
            xads.setDatabaseName(dbName);
            try {
                xads.getXAConnection();
                fail("expected connection to fail");
            } catch (SQLException e) {
                assertSQLState("08004", e);
            }
        }
    }
}
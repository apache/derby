/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.jdbcapi.InvalidLDAPServerAuthenticationTest

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


public class InvalidLDAPServerAuthenticationTest extends BaseJDBCTestCase {

    /** Creates a new instance of the Test */
    public InvalidLDAPServerAuthenticationTest(String name) {
        super(name);
    }

    /**
     * Ensure all connections are not in auto commit mode.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public static Test suite() {
        if (JDBC.vmSupportsJSR169())
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite(
                "InvalidLDAPServerAuthenticationTest - cannot" +
                " run with JSR169 - missing functionality for " +
                "org.apache.derby.iapi.jdbc.AuthenticationService");
        
        // security manager would choke attempting to resolve to the invalid
        // LDAPServer, so run without
        BaseTestSuite suite =
            new BaseTestSuite("InvalidLDAPServerAuthenticationTest");

        suite.addTest(SecurityManagerSetup.noSecurityManager(baseSuite(
                "testInvalidLDAPServerConnectionError")));
        suite.addTest(TestConfiguration.clientServerDecorator(
                SecurityManagerSetup.noSecurityManager(
                baseSuite("testInvalidLDAPServerConnectionError"))));
        return suite;            
    }

    public static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        Test test = new InvalidLDAPServerAuthenticationTest("testInvalidLDAPServerConnectionError");
        suite.addTest(test);

        // This test needs to run in a new single use database without connect
        // for shutdown after, as we're going to make the database unusable
        return TestConfiguration.singleUseDatabaseDecoratorNoShutdown(suite);
    }

    protected void setDatabaseProperty(
            String propertyName, String value, Connection conn) 
    throws SQLException {
        CallableStatement setDBP =  conn.prepareCall(
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, propertyName);
        setDBP.setString(2, value);
        setDBP.execute();
        setDBP.close();
    }

    public void testInvalidLDAPServerConnectionError() throws SQLException {
        // setup 
        Connection conn = getConnection();
        // set the ldap properties
        setDatabaseProperty("derby.connection.requireAuthentication", "true", conn);
        setDatabaseProperty("derby.authentication.provider", "LDAP", conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-5941
        setDatabaseProperty("derby.authentication.server", "noSuchServer.invalid", conn);
        setDatabaseProperty("derby.authentication.ldap.searchBase", "o=dnString", conn);
        setDatabaseProperty("derby.authentication.ldap.searchFilter","(&(objectClass=inetOrgPerson)(uid=%USERNAME%))", conn);
        commit();
        conn.setAutoCommit(true);
        conn.close();
        // shutdown the database as system, so the properties take effect
        TestConfiguration.getCurrent().shutdownDatabase();
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        
        // actual test. 
        // first, try datasource connection
        DataSource ds = JDBCDataSource.getDataSource(dbName);

        try {
            ds.getConnection();
            fail("expected java.net.UnknownHostException for datasource");
        } catch (SQLException se) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
            if (JDBC.vmSupportsJNDI()) {
                assertSQLState("08004", se);
                // with network server, the java.net.UnknownHostException will
                // be in derby.log, the client only gets a 08004 and somewhat
                // misleading warning ('Reason: userid or password invalid')
                println( "Saw SQLException with message = " + se.getMessage() );

                if (usingEmbedded()) {
                    assertTrue(se.getMessage().
                               indexOf("java.net.UnknownHostException") > 1);
                }
            } else {
                // Expect boot to fail, LDAP authentication requires JNDI
                assertSQLState("XJ040", se);
            }
        }

        // driver manager connection
        String url2 = TestConfiguration.getCurrent().getJDBCUrl(dbName);

        try {
            DriverManager.getConnection(url2,"user","password").close();
            fail("expected java.net.UnknownHostException for driver");
        } catch (SQLException se) {
            if (JDBC.vmSupportsJNDI()) {
                assertSQLState("08004", se);
                // with network server, the java.net.UnknownHostException will
                // be in derby.log, the client only gets a 08004 and somewhat
                // misleading warning ('Reason: userid or password invalid')
                if (usingEmbedded()) {
                    assertTrue(se.getMessage().
                               indexOf("java.net.UnknownHostException") > 1);
                }
            } else {
                // Expect boot to fail, LDAP authentication requires JNDI
                assertSQLState("XJ040", se);
            }
        }
        
        // we need to shutdown the system, or the failed connections
        // cling to db.lck causing cleanup to fail.
        // we *can* shutdown because we don't have authentication required
        // set at system level (only database level).
        shutdownSystem();
    }
    
    protected void shutdownSystem()throws SQLException {
        DataSource ds;
        if (usingEmbedded())
        {
            ds = JDBCDataSource.getDataSource();
            JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        }
        else
        {
            // note: with network server/client, you can't set the databaseName
            // to null, that results in error 08001 - Required DataSource
            // property databaseName not set.
            // so, we rely on passing of an empty string for databaseName,
            // which in the current code is interpreted as system shutdown.
            ds = JDBCDataSource.getDataSource("");
        }
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection();
        } catch (SQLException e) {
            //do nothing;
        }
    }
}

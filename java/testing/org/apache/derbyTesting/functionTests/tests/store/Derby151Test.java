/*
  Class org.apache.derbyTesting.functionTests.tests.store.Derby151Test

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
package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.shared.common.sanity.SanityManager;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;

/**
 *   Test to reproduce and verify fix for DERBY-151.
 */

public class Derby151Test extends BaseJDBCTestCase
{

    public Derby151Test(String name)
    {
        super(name);
    }


    protected static Test baseSuite(String name)
    {
        TestSuite suite = new TestSuite(name);

        if (!JDBC.vmSupportsJSR169()) {
            // JSR169 cannot run with tests with stored procedures
            // that do database access - for they require a
            // DriverManager connection to jdbc:default:connection;
            // DriverManager is not supported with JSR169.
            suite.addTestSuite(Derby151Test.class);
            return new CleanDatabaseTestSetup(
                new TestSuite(Derby151Test.class, name));
        } else {
            return suite;
        }
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("Derby151Test");

        suite.addTest(
            baseSuite("Derby151Test:embedded"));

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                baseSuite("Derby151Test:c/s")));

        return suite;
    }

    protected void setUp()
            throws java.lang.Exception {
        super.setUp();

        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE d151(x int primary key)");
        stmt.close();
    }

    /**
     * Clean up the connection maintained by this test.
     */
    protected void tearDown()
            throws java.lang.Exception {

        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE d151");
        stmt.close();

        super.tearDown();
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread.
    public static void d151() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");

        PreparedStatement insert = null;
        try {
            insert = c.prepareStatement("insert into d151 values (?)");

            for (int i = 0; i < 10000; i++) {
                insert.setInt(1, i);
                insert.executeUpdate();
                Thread.currentThread().interrupt();
            }
        } finally {
            if (insert != null) {
                try {
                    insert.close(); // already closed by error
                } catch (SQLException e) {
                }
            }

            c.close();
        }
    }

    public void testD151 () throws SQLException {
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure D151 () MODIFIES SQL DATA " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.Derby151Test.d151' " +
            "language java parameter style java");

        try {
            s.executeUpdate("call D151()");

            // We were not able to prokove any error, but that should not fail
            // the test; the results here may depend on VMs possibly.  So just
            // report this fact in verbose mode:

            println("Not able to test fix for DERBY-151: No interrupt seen");
        } catch (SQLException e) {
            assertSQLState("XSDG9", e);
        }
    }
}

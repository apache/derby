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
        suite.addTestSuite(Derby151Test.class);
        return new CleanDatabaseTestSetup(
            new TestSuite(Derby151Test.class, name));
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


    public void testD151 () throws SQLException {
        PreparedStatement insert =
            prepareStatement("insert into d151 values (?)");
        try {
            for (int i = 0; i < 10000; i++) {
                insert.setInt(1, i);
                insert.executeUpdate();
                Thread.currentThread().interrupt();
            }

            // We were not able to prokove any error, but that should not fail
            // the test; the results here may depend on VMs possibly.  So just,
            // report this fact in verbose mode:

            println("Not able to test fix for DERBY-151: No interrupt seen");

        } catch (SQLException e) {
            assertSQLState("XSDG9", e);
        }
        insert.close(); // already closed by error
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("Derby151Test");
        suite.addTest(
            baseSuite("Derby151Test:embedded"));

        // Note: We are not adding a client/Server version since the explicit
        // interrupt may (will) upset the communication socket to the client.
        // I see 08006 SQL state on OpenSolaris/JDK1.6.
        //
        //    :
        // org.apache.derby.client.am.DisconnectException:
        //                    A communications error has been detected: null.
        //    :
        // java.io.InterruptedIOException
        //    at java.net.SocketOutputStream.socketWrite0(Native Method)
        //    at java.net.SocketOutputStream.socketWrite(
        //                                         SocketOutputStream.java:92)
        //    at java.net.SocketOutputStream.write(SocketOutputStream.java:136)
        //
        // which happened before any error in RAFContainer4.


        return suite;
    }
}

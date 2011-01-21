/*
  Class org.apache.derbyTesting.functionTests.tests.store.InterruptResilienceTest

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
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;

/**
 *   Test to reproduce and verify fix for DERBY-151.
 */

public class InterruptResilienceTest extends BaseJDBCTestCase
{

    public InterruptResilienceTest(String name)
    {
        super(name);
    }


    protected static Test baseSuite(String name)
    {
        TestSuite suite = new TestSuite(name);

        if (JDBC.vmSupportsJDBC3()) {
            // We need a JDBC level that supports DriverManager in order
            // to run tests that access the database from a stored procedure
            // using DriverManager and jdbc:default:connection.
            // DriverManager is not supported with JSR169.

            suite.addTestSuite(InterruptResilienceTest.class);
            return new CleanDatabaseTestSetup(suite);
        } else {
            return suite;
        }
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("InterruptResilienceTest");
        if (! isSunJVM()) {
            // DERBY-4463 test fails on IBM VMs. Remove this
            // exception when that issue is solved.
            println("Test skipped for this VM, cf. DERBY-4463");
            return suite;
        }

        if (hasInterruptibleIO()) {
            println("Test skipped due to interruptible IO.");
            println("This is default on Solaris/Sun Java <= 1.6, use " +
                    "-XX:-UseVMInterruptibleIO if available.");
            return suite;
        }

        suite.addTest(
            baseSuite("InterruptResilienceTest:embedded"));

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                baseSuite("InterruptResilienceTest:c/s")));

        return suite;
    }

    protected void setUp()
            throws java.lang.Exception {
        super.setUp();

        Statement stmt = createStatement();
        stmt.executeUpdate("CREATE TABLE irt(x int primary key)");
        stmt.close();
    }

    /**
     * Clean up the connection maintained by this test.
     */
    protected void tearDown()
            throws java.lang.Exception {

        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE irt");
        stmt.close();

        super.tearDown();
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread.
    public static void irt() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        c.setAutoCommit(false);
        PreparedStatement insert = null;
        long seen = 0;
        long lost = 0;
        try {
            insert = c.prepareStatement("insert into irt values (?)");

            // About 75000 iterations is needed to see any concurrency
            // wait on RawDaemonThread during recovery, cf.
            // running with debug flag "RAF4Recovery".
            for (int i = 0; i < 100000; i++) {
                if (i % 1000 == 0) {
                    c.commit();
                }

                // Make sure to interrupt after commit, since log writing isn't
                // safe for interrupts (on Solaris only) yet.
                Thread.currentThread().interrupt();

                insert.setInt(1, i);
                insert.executeUpdate();

                if (Thread.interrupted()) { // test and reset
                    seen++;
                    // println(ff() + "interrupt seen");
                } else {
                    // println(ff() + "interrupt lost");
                    lost++;
                }

            }
        } finally {
            // always clear flag
            Thread.interrupted();

            if (insert != null) {
                try {
                    insert.close(); // already closed by error
                } catch (SQLException e) {
                }
            }

            c.close();
            println("interrupts recovered: " + seen);
            println("interrupts lost: " + lost + " (" +
                    (lost * 100.0/(seen + lost)) + "%)");
        }
    }

    public void testIRT () throws SQLException {
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure IRT () MODIFIES SQL DATA " +
            "external name 'org.apache.derbyTesting.functionTests" +
            ".tests.store.InterruptResilienceTest.irt' " +
            "language java parameter style java");


        s.executeUpdate("call IRT()");

    }


    // private static String ff() {
    //     return Thread.currentThread().getName();
    // }
}

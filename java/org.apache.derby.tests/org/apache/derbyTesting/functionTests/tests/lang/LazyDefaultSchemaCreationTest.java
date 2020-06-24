/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the lazy creation functionality of default schema: the schema
 * is only first created when the first database object is created in
 * the schema.
 */
public class LazyDefaultSchemaCreationTest extends BaseJDBCTestCase {

    final private static String LOCK_TIMEOUT = "40XL1";

    /**
     * Creates a new {@code LazyDefaultSchemaCreationTest} instance.
     *
     * @param name the name of the test
     */
    public LazyDefaultSchemaCreationTest(String name) {
        super(name);
    }


    /**
     * Reproduces hang seen in DERBY-48
     */
    public void testDerby48testNewSchemaHang () throws SQLException
    {
        Connection c1 = openUserConnection("newuser");
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        // Will auto-create schema NEWUSER:
        s1.executeUpdate("create table t1(i int)");
        s1.close();

        // DERBY-48: The next connect causes a hang on write lock the
        // new schema row being created by c1 that is not yet
        // committed if the fix for DERBY-48 is not yet in place.
        // The fix makes the the auto-create happen in a nested transaction
        // which commit immediately, so the hang should not be present.

        Connection c2 = null;

        try {
            c2 = openUserConnection("newuser");
        } catch (SQLException e) {
            if (e.getSQLState().equals(LOCK_TIMEOUT)) {
                c1.rollback();
                c1.close();
                fail("DERBY-48 still seen", e);
            } else {
                throw e;
            }
        }

        c1.rollback();

        // Since the auto-create happened in a nested transaction
        // which has committed, the schema should still be around
        // after the rollback. Note that this is a side-effect of the
        // fix for DERBY-48, not required behavior for SQL, but it is
        // user visible behavior, so we test it here to make sure that
        // patch works as intended:

        JDBC.assertSingleValueResultSet(
            c1.createStatement().executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='NEWUSER'"),
            "NEWUSER");

        c1.rollback();

        c1.close();
        c2.close();
    }

    /**
     * Test that we recover from self locking in the auto-create
     * nested transaction (cf solution for DERBY-48).
     */
    public void testDerby48SelfLockingRecovery () throws SQLException
    {
        Connection c1 = openUserConnection("newuser");
        c1.setAutoCommit(false);
        c1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement s1 = c1.createStatement();

        // Set read locks in parent transaction
        s1.executeQuery("select count(*) from sys.sysschemas");

        // ..which conflicts with the auto-create in a subtransaction
        // which will self-lock here, but should recover to try again
        // in outer transaction:
        s1.executeUpdate("create table t1(i int)");

        JDBC.assertSingleValueResultSet(
            s1.executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='NEWUSER'"),
            "NEWUSER");

        c1.rollback();

        // Since the fallback does the auto-create of the schema in
        // the outer transaction, a rollback will remove it:
        JDBC.assertEmpty(
            s1.executeQuery
            ("select * from sys.sysschemas where schemaname='NEWUSER'"));

        c1.rollback();
    }

    /**
     * Test that we do get to see the self locking in the auto-create
     * nested transaction (cf solution for DERBY-48) when deadlock
     * detection is on, i.e. 40XL2 (LOCK_TIMEOUT_LOG) rather than
     * 40XL1 (LOCK_TIMEOUT) happens.
     *
     * After fix for DERBY-5564 LOCK_TIMEOUT will be returned whether
     * diagnostics are on or not.
     */
    public void testDerby48SelfLockingRecoveryDeadlockDetectionOn ()
            throws SQLException
    {
        Connection c1 = openUserConnection("newuser");
        c1.setAutoCommit(false);
        c1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement s1 = c1.createStatement();

        // Set read locks in parent transaction
        s1.executeQuery("select count(*) from sys.sysschemas");

        // ..which conflicts with the auto-create in a subtransaction
        // which will self-lock here, but should throw now:
        // in outer transaction:
        try {
            s1.executeUpdate("create table t1(i int)");
            fail("Expected exception " + LOCK_TIMEOUT);
        } catch (SQLException e) {
            assertSQLState("Expected state: ", LOCK_TIMEOUT, e);
        }

        JDBC.assertEmpty(
            s1.executeQuery
            ("select * from sys.sysschemas where schemaname='NEWUSER'"));

        c1.rollback();
    }

    /**
     * Test that the timeout lock diagnostics do not create an
     * infinite recursion as in DERBY-3678 (although that particular
     * use case will not cause an infinite recursion after the fix to
     * DERBY-48). The scenario in this test case does create the
     * infinite recursion prior to the fix of DERBY-3678, however.
     *
     * After fix for DERBY-5564 LOCK_TIMEOUT SQL state should be returned
     * for a lock timeout whether diagnostics are on or not.
     */
    public void testDerby3678 ()
//IC see: https://issues.apache.org/jira/browse/DERBY-3678
            throws SQLException
    {
        Connection c1 = openUserConnection("newuser");
        Connection c2 = null;

        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        // set locks in connection 1:
        s1.executeUpdate("create schema newuser");
        s1.executeUpdate("create table t(i int)");

        // ..which conflicts with the next connect
        try {
            c2 = openUserConnection("newuser");
            fail("Expected exception " + LOCK_TIMEOUT);
        } catch (SQLException e) {
            assertSQLState("Expected state: ", LOCK_TIMEOUT, e);
        } finally {
            c1.rollback();
        }
    }

    /**
     * Test that implicit schema creation of other schemas besides
     * the initial default schema is still transactional.
     */
    public void testOtherImplicitSchemaCreation () throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-48
        Connection c1 = openUserConnection("newuser");
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        // Will auto-create schema OTHERSCHEMA:
        s1.executeUpdate("create table otherschema.t1(i int)");
        s1.close();

        JDBC.assertSingleValueResultSet(
            c1.createStatement().executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='OTHERSCHEMA'"),
            "OTHERSCHEMA");

        c1.rollback();

        JDBC.assertEmpty(
            c1.createStatement().executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='OTHERSCHEMA'"));

        c1.rollback();
        c1.close();
    }

    public void testDerby3043CheckConstraint() throws SQLException
    {
        // Demonstrate the DERBY-3043 workaround: if the table name is
        // schema-qualified, check constraints do not cause a problem,
        // and the named schema is automatically created if it does
        // not yet exist:
        Connection c0 = openUserConnection("frogs");
        Statement s0 = c0.createStatement();

        JDBC.assertEmpty( s0.executeQuery
            ("select * from sys.sysschemas where schemaname='FROGS'"));
        JDBC.assertEmpty( s0.executeQuery
            ("select * from sys.sysschemas where schemaname='NOSUCH'"));

        // A simple example, which should work whether or not the
        // DERBY-3043 fix is in place

        s0.executeUpdate("create table frogs.users2(username varchar(16) " +
                        "CHECK(LENGTH(username)>7))");

        // Demonstrate that any schema is lazy-created, not just the
        // default schema which matches the username:

        s0.executeUpdate("create table nosuch.users(username varchar(16) " +
                        "CHECK(LENGTH(username)>7))");

        // Schemas FROGS and NOSUCH have been lazy-created:

        JDBC.assertSingleValueResultSet( s0.executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='FROGS'"),
            "FROGS");
        JDBC.assertSingleValueResultSet( s0.executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='NOSUCH'"),
            "NOSUCH");
        c0.close();

        // Now verify that the test cases from DERBY-3043 pass:

        Connection c1 = openUserConnection("blogs");

        Statement s1 = c1.createStatement();
    
        // At the beginning, the schema 'blogs' does not exist.

        JDBC.assertEmpty( s1.executeQuery
            ("select * from sys.sysschemas where schemaname='BLOGS'"));

        // Should work, but without the DERBY-3043 fix will get a
        // "Schema blogs does not exist" error

        s1.executeUpdate("create table users(username varchar(16) " +
                        "CHECK(LENGTH(username)>7))");

        // Another slightly more complicated example, which requires
        // the DERBY-3043 fix again to work.

        s1.executeUpdate("CREATE TABLE BLOGSCOM__BLOGS__USERS(" +
                "PK INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY," +
                "username VARCHAR(16) NOT NULL " +
                "  CONSTRAINT BLOGSCOM__BLOGS__USERS_UNIQUE_username UNIQUE " +
                "  CONSTRAINT BLOGSCOM__BLOGS__USERS_PASSWORD_username " +
                "    CHECK(LENGTH(username)>7)," +
                "password VARCHAR (32672) NOT NULL , " +
                "PRIMARY KEY(PK))"); 

        // Schema BLOGS should have been lazy-created:

        JDBC.assertSingleValueResultSet( s1.executeQuery(
                "select schemaname from sys.sysschemas " +
                "where schemaname='BLOGS'"),
            "BLOGS");

        c1.close();
    }


protected void  tearDown() throws Exception {
        try {
            createStatement().executeUpdate("drop schema newuser restrict");
        } catch (SQLException e) {
            // If not created by the fixture:
            assertSQLState("Expected state: ", "42Y07", e);
        }

        super.tearDown();
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("LazyDefaultSchemaCreationTest");

        BaseTestSuite[] suites = {
            new BaseTestSuite("LazyDefaultSchemaCreationTest:embedded"),
            new BaseTestSuite("LazyDefaultSchemaCreationTest:clientServer") };

        for (int i=0; i < 2; i++) {
            suites[i].addTest(DatabasePropertyTestSetup.setLockTimeouts
                          (new LazyDefaultSchemaCreationTest
                           ("testDerby48testNewSchemaHang"),2,1));

            suites[i].addTest(DatabasePropertyTestSetup.setLockTimeouts
                          (new LazyDefaultSchemaCreationTest
                           ("testDerby48SelfLockingRecovery"),2,1));

            Properties p = new Properties();
            p.setProperty("derby.locks.deadlockTrace", "true");

            suites[i].addTest
                (DatabasePropertyTestSetup.setLockTimeouts
                 (new DatabasePropertyTestSetup
                  (new LazyDefaultSchemaCreationTest
                   ("testDerby48SelfLockingRecoveryDeadlockDetectionOn"),
                   p, false),
                  2,   // deadlock timeout
                  1)); // wait timeout

//IC see: https://issues.apache.org/jira/browse/DERBY-3678
            suites[i].addTest
                (DatabasePropertyTestSetup.setLockTimeouts
                 (new DatabasePropertyTestSetup
                  (new LazyDefaultSchemaCreationTest
                   ("testDerby3678"),
                   p, false),
                  2,   // deadlock timeout
                  1)); // wait timeout

            suites[i].addTest(new CleanDatabaseTestSetup(
                new LazyDefaultSchemaCreationTest(
                    "testDerby3043CheckConstraint")));

            if (i == 0) {
                suite.addTest(suites[i]);
            } else {
                suite.addTest(
                    TestConfiguration.clientServerDecorator(suites[i]));
            }


        }

        return suite;
    }
}

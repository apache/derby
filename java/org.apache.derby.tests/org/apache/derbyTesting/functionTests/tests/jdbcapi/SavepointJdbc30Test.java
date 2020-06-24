/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi/SavepointJdbc30Test
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the new class Savepoint in JDBC 30. Also, test some mix and match of
 * defining savepoints through JDBC and SQL testing both callable and prepared
 * statements meta data
 * 
 */

public class SavepointJdbc30Test extends BaseJDBCTestCase {

    /**
     * Create a test
     * 
     * @param name
     */
    public SavepointJdbc30Test(String name) {
        super(name); 
    }

    /**
     * Set up the test suite for embedded mode, client mode, and embedded mode
     * with XADataSources 
     * 
     * @return A suite containing embedded, client and embedded with XA suites
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("SavepointJdbc30_JSR169Test suite");

        // Get the tests for embedded
        BaseTestSuite embedded = new BaseTestSuite(
                "SavepointJdbc30_JSR169Test:embedded");
        embedded.addTestSuite(SavepointJdbc30Test.class);
        embedded.addTest(getEmbeddedSuite("SavepointJdbc30_JSR169Test:"
                + "embedded only"));
        suite.addTest(embedded);

        // Get the tests for client.
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite client =
            new BaseTestSuite("SavepointJdbc30_JSR169Test:client");

        client.addTestSuite(SavepointJdbc30Test.class);
        suite.addTest(TestConfiguration.clientServerDecorator(client));

        // Repeat the embedded tests obtaining a connection from
        // an XA data source if it is supported. This is not supported
        // under JSR169.
        if (JDBC.vmSupportsJDBC3()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            embedded = new BaseTestSuite(
//IC see: https://issues.apache.org/jira/browse/DERBY-3568
            "SavepointJdbc30_JSR169Test:embedded XADataSource");
            embedded.addTestSuite(SavepointJdbc30Test.class);
            embedded.addTest(getEmbeddedSuite("SavepointJdbc30_JSR169Test:"
        			+ "embedded only XADataSource"));
            suite.addTest(TestConfiguration.connectionXADecorator(embedded));
            //        	 Repeat the client tests obtaining a connection from
            // an XA data source if it is supported. This is not supported
            // under JSR169.
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            client = new BaseTestSuite(
                "SavepointJdbc30_JSR169Test:client XADatasource");

            client.addTestSuite(SavepointJdbc30Test.class);
            suite.addTest(TestConfiguration.clientServerDecorator(TestConfiguration.connectionXADecorator(client)));        	
        }	

//IC see: https://issues.apache.org/jira/browse/DERBY-4885
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 1, 2)) {
            /**
             * Creates the database objects used in the test cases.
             * 
             * @throws SQLException
             */
            protected void decorateSQL(Statement s) throws SQLException {
                /* Create a table */
                s.execute("create table t1 (c11 int, c12 smallint)");
                s.execute("create table t2 (c11 int)");
                getConnection().commit();

            }
        };

    }

    /**
     * Create a testsuite containing the tests that can only run in embedded
     * mode. These tests have names starting with x and are added automatically.
     */
    private static Test getEmbeddedSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite embedded = new BaseTestSuite(name);
        Method[] methods = SavepointJdbc30Test.class.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (m.getParameterTypes().length > 0
                    || !m.getReturnType().equals(Void.TYPE)) {
                continue;
            }
            String methodName = m.getName();
            if (methodName.startsWith("x")) {
                embedded.addTest(new SavepointJdbc30Test(methodName));
            }
        }
        return embedded;
    }

    /**
     * Set up the test environment.
     */
    protected void setUp() throws Exception {
        super.setUp();
        // Keep Autocommit off
        getConnection().setAutoCommit(false);
        // Clear the tables created by the decorator
//IC see: https://issues.apache.org/jira/browse/DERBY-5114
        Statement s = createStatement();
        s.execute("truncate table t1");
        s.execute("truncate table t2");
        commit();
    }

    /**
     * Test1. It should not be possible to set a savepoint if autocommit is on.
     */
    public void testNoSavepointsIfAutoCommit() throws SQLException {
        Connection con = getConnection();
        con.setAutoCommit(true);
        try {
            con.setSavepoint(); // will throw exception because auto commit is
            // true
            fail("No unnamed savepoints allowed if autocommit is true");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XJ010", se);
        }
        // Test 1a
        try {
            con.setSavepoint("notallowed"); // will throw exception because auto
            // commit is true
            fail("No named savepoints allowed if autocommit is true");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XJ010", se);
        }
    }

    /**
     * Test2 - After releasing a savepoint, should be able to reuse it.
     */
    public void testReusingSavepoints() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        con.releaseSavepoint(savepoint1);
        con.setSavepoint("s1");
        con.rollback();
    }

    /**
     * Test3 - Named savepoints can't pass null for name
     */
    public void testNullName() throws SQLException {
        Connection con = getConnection();
        try {
            con.setSavepoint(null);
            fail("FAIL 3 Null savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XJ011", se);
        }
        con.rollback();
    }

    /**
     * Test4 - Verify names/ids of named/unnamed savepoints named savepoints
     * don't have an id. unnamed savepoints don't have a name (internally, all
     * our savepoints have names, but for unnamed savepoint, that is not exposed
     * through jdbc api)
     * 
     * @throws SQLException
     */
    public void testNamesAndIds() throws SQLException {
        Connection con = getConnection();
        try {
            Savepoint savepoint1 = con.setSavepoint();
            savepoint1.getSavepointId();
            // following should throw exception for unnamed savepoint
            savepoint1.getSavepointName();
            fail("FAIL 4 getSavepointName on id savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XJ014", se);
        }
        con.rollback();
        try {
            Savepoint savepoint1 = con.setSavepoint("s1");
            savepoint1.getSavepointName();
            // following should throw exception for named savepoint
            savepoint1.getSavepointId();
            fail("FAIL 4 getSavepointId on named savepoint ");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XJ013", se);
        }
        con.rollback();
    }

    /**
     * TEST 5a and 5b for bug 4465 test 5a - create two savepoints in two
     * different transactions and release the first one in the subsequent
     * transaction
     */
    public void testBug4465() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        con.commit();
        // The following savepoint was earlier named s1. Changed it to s2 while
        // working on DRDA support
        // for savepoints. The reason for that is as follows
        // The client translates all savepoint jdbc calls to equivalent sql and
        // hence
        // if the 2 savepoints in
        // different connections are named the same, then the release savepoint
        // below will get converted to
        // RELEASE TO SAVEPOINT s1 and that succeeds because the 2nd connection
        // does have a savepoint named s1.
        // Hence we don't really check what we intended to check which is trying
        // to release a savepoint created
        // in a different transaction
        con.setSavepoint("s2");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        try {
            con.releaseSavepoint(savepoint1);
            fail("FAIL 5a - release savepoint from a different transaction "
                    + "did not raise error");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.commit();

        // test 5b - create two savepoints in two different transactions
        // and rollback the first one in the subsequent transaction
        savepoint1 = con.setSavepoint("s1");
        con.commit();
        // The following savepoint was earlier named s1. Changed it to s2 while
        // working on DRDA support
        // for savepoints. The reason for that is as follows
        // The client translates all savepoint jdbc calls to equivalent sql and
        // hence
        // if the 2 savepoints in
        // different connections are named the same, then the rollback savepoint
        // below will get converted to
        // ROLLBACK TO SAVEPOINT s1 and that succeeds because the 2nd connection
        // does have a savepoint named s1.
        // Hence we don't really check what we intended to check which is trying
        // to rollback a savepoint created
        // in a different transaction
        con.setSavepoint("s2");
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        try {
            con.rollback(savepoint1);
            fail("FAIL 5b - rollback savepoint from a different transaction "
                    + "did not raise error");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.commit();
    }

    /**
     * test 6a - create a savepoint release it and then create another with the
     * same name. and release the first one
     */
    public void testReleaseReleasedSavepoint() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        con.releaseSavepoint(savepoint1);
        // The following savepoint was earlier named s1. Changed it to s2 while
        // working on DRDA support
        // for savepoints. The reason for that is as follows
        // The client translates all savepoint jdbc calls to equivalent sql and
        // hence
        // if the 2 savepoints in
        // a transaction are named the same, then the release savepoint below
        // will get converted to
        // RELEASE TO SAVEPOINT s1 and that succeeds because there is a valid
        // savepoint named s1.
        con.setSavepoint("s2");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        try {
            con.releaseSavepoint(savepoint1);
            fail("FAIL 6a - releasing a released savepoint did not raise error");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.commit();
    }

    /**
     * test 6b - create a savepoints release it and then create another with the
     * same name. and rollback the first one
     */
    public void testRollbackReleasedSavepoint() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        con.releaseSavepoint(savepoint1);
        // The following savepoint was earlier named s1. Changed it to s2 while
        // working on DRDA support
        // for savepoints. The reason for that is as follows
        // The client translates all savepoint jdbc calls to equivalent sql and
        // hence
        // if the 2 savepoints in
        // a transaction are named the same, then the rollback savepoint below
        // will get converted to
        // ROLLBACK TO SAVEPOINT s1 and that succeeds because there is a valid
        // savepoint named s1.
        con.setSavepoint("s2");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        try {
            con.rollback(savepoint1);
            fail("FAIL 6b - rollback a released savepoint did not raise error");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.commit();
    }

    /**
     * Test 6c: TEST case just for bug 4467 // Test 10 - create a named
     * savepoint with the a generated name savepoint1 =
     * con2.setSavepoint("SAVEPT0"); // what exactly is the correct behaviour
     * here? try { savepoint2 = con2.setSavepoint(); } catch (SQLException se) {
     * System.out.println("Expected Exception is " + se.getMessage()); }
     * con2.commit();
     */
    public void testReleaseSavepointFromOtherTransaction() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        Connection con2 = openDefaultConnection();
        try {
            con2.releaseSavepoint(savepoint1);
            fail("FAIL 6c - releasing another transaction's savepoint did "
                    + "not raise error");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("XJ010", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("XJ008", se);
            }
        }
        con.commit();
        con2.commit();
    }

    /**
     * Test 7a: BUG 4468 - should not be able to pass a savepoint from a
     * different transaction for release/rollback
     */
    public void testSwapSavepointsAcrossConnectionAndRelease()
            throws SQLException {
        Connection con = getConnection();
        Connection con2 = openDefaultConnection();
        con2.setAutoCommit(false);
        Savepoint savepoint1 = con2.setSavepoint("s1");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        con.setSavepoint("s1");
        try {
            con.releaseSavepoint(savepoint1);
            fail("FAIL 7a - releasing a another transaction's savepoint did "
                    + "not raise error");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B502", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("XJ097", se);
            }

        }
        con.commit();
        con2.commit();
    }

    /**
     * Test 7b - swap savepoints across connections
     */
    public void testSwapSavepointsAcrossConnectionsAndRollback()
            throws SQLException {
        Connection con = getConnection();
        Connection con2 = openDefaultConnection();
        con2.setAutoCommit(false);
        Savepoint savepoint1 = con2.setSavepoint("s1");
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        con.setSavepoint("s1");
        try {
            con.rollback(savepoint1);
            fail("FAIL 7b - rolling back a another transaction's savepoint "
                    + "did not raise error");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B502", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("XJ097", se);
            }
        }
        con.commit();
        con2.commit();
    }

    /*
     * following section attempts to call statement in a method to do a negative
     * test because savepoints are not supported in a trigger however, this
     * cannot be done because a call is not supported in a trigger. leaving the
     * test here for later reference for when we support the SQL version // bug
     * 4507 - Test 8 test all 4 savepoint commands inside the trigger code
     * System.out.println("Test 8a set savepoint(unnamed) command inside the
     * trigger code"); s.executeUpdate("create trigger trig1 before insert on t1
     * for each statement call
     * org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionSetSavepointUnnamed()");
     * try {
     * 
     * s.executeUpdate("insert into t1 values(1,1)"); System.out.println("FAIL
     * 8a set savepoint(unnamed) command inside the trigger code"); } catch
     * (SQLException se) { System.out.println("Expected Exception is " +
     * se.getMessage()); } s.executeUpdate("drop trigger trig1");
     * 
     * System.out.println("Test 8b set savepoint(named) command inside the
     * trigger code"); s.executeUpdate("create trigger trig2 before insert on t1
     * for each statement call
     * org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionSetSavepointNamed()");
     * try { s.executeUpdate("insert into t1 values(1,1)");
     * System.out.println("FAIL 8b set savepoint(named) command inside the
     * trigger code"); } catch (SQLException se) { System.out.println("Expected
     * Exception is " + se.getMessage()); } s.executeUpdate("drop trigger
     * trig2");
     * 
     * System.out.println("Test 8c release savepoint command inside the trigger
     * code"); s.executeUpdate("create trigger trig3 before insert on t1 for
     * each statement call
     * org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionReleaseSavepoint()");
     * try { s.executeUpdate("insert into t1 values(1,1)");
     * System.out.println("FAIL 8c release savepoint command inside the trigger
     * code"); } catch (SQLException se) { System.out.println("Expected
     * Exception is " + se.getMessage()); } s.executeUpdate("drop trigger
     * trig3");
     * 
     * System.out.println("Test 8d rollback savepoint command inside the trigger
     * code"); s.executeUpdate("create trigger trig4 before insert on t1 for
     * each statement call
     * org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionRollbackSavepoint()");
     * try { s.executeUpdate("insert into t1 values(1,1)");
     * System.out.println("FAIL 8d rollback savepoint command inside the trigger
     * code"); } catch (SQLException se) { System.out.println("Expected
     * Exception is " + se.getMessage()); } s.executeUpdate("drop trigger
     * trig4"); con.rollback();
     */// end commented out test 8
    /**
     * Test 9 test savepoint name and verify case sensitivity
     */
    public void testSavepointName() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("myname");
        String savepointName = savepoint1.getSavepointName();
        assertEquals(savepointName, "myname");
        con.rollback();
    }

    /**
     * Test 10 test savepoint name case sensitivity
     */
    public void testNameCaseSensitivity() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("MyName");
        String savepointName = savepoint1.getSavepointName();
        assertEquals(savepointName, "MyName");
        con.rollback();
    }

    /**
     * Test 11 rolling back a savepoint multiple times - should work
     */
    public void testRollbackMultipleTimes() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("MyName");
        con.rollback(savepoint1);
        con.rollback(savepoint1);
        con.rollback();
    }

    /**
     * Test 12 releasing a savepoint multiple times - should not work
     */
    public void testReleaseMultipleTimes() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("MyName");
        con.releaseSavepoint(savepoint1);
        try {
            con.releaseSavepoint(savepoint1);
            fail("FAIL 12 releasing a savepoint multiple times should fail");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.rollback();
    }

    /**
     * Test 13 shouldn't be able to use a savepoint from earlier transaction
     * after setting autocommit on and off
     */
    public void testSavepointFromEarlierTransactionAfterToggleAutocommit()
            throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("MyName");
        con.setAutoCommit(true);
        con.setAutoCommit(false);
        Savepoint savepoint2 = con.setSavepoint("MyName1");
        try {// shouldn't be able to use savepoint from earlier tranasaction
            // after setting autocommit on and off
            con.releaseSavepoint(savepoint1);
            fail("FAIL 13 shouldn't be able to use a savepoint from earlier "
                    + "transaction after setting autocommit on and off");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.releaseSavepoint(savepoint2);
        con.rollback();
    }

    /**
     * Test 14 cause a transaction rollback and that should release the internal
     * savepoint array
     */
    public void testRollbackReleasesSavepointArray() throws SQLException {
        Connection con = getConnection();
        Connection con2 = openDefaultConnection();
        con2.setAutoCommit(false);
        Statement s1, s2;
        s1 = createStatement();
        s1.executeUpdate("insert into t1 values(1,1)");
        s1.executeUpdate("insert into t1 values(2,0)");
        con.commit();
        s1.executeUpdate("update t1 set c11=c11+1 where c12 > 0");
        s2 = con2.createStatement();
        Savepoint savepoint1 = con2.setSavepoint("MyName");
        try {// following will get lock timeout which will rollback
            // transaction on c2
            s2.executeUpdate("update t1 set c11=c11+1 where c12 < 1");
            fail("FAIL 14 should have gotten lock time out");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("40XL1", se);
        }
        try {// the transaction rollback above should have removed the
            // savepoint MyName
            con2.releaseSavepoint(savepoint1);
            fail("FAIL 14 A non-user initiated transaction rollback should "
                    + "release the internal savepoint array");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        con.rollback();
        con2.rollback();
        s1.execute("delete from t1");
        con.commit();
    }

    /** Test 15 Check savepoints in batch */
    public void testSavepointsInBatch() throws SQLException {
        Connection con = getConnection();
        Statement s = createStatement();
        s.execute("delete from t1");
        s.addBatch("insert into t1 values(1,1)");
        s.addBatch("insert into t1 values(2,2)");
        Savepoint savepoint1 = con.setSavepoint();
        s.addBatch("insert into t1 values(3,3)");
        s.executeBatch();
        con.rollback(savepoint1);

        assertTableRowCount("T1", 0);
        con.rollback();
    }

    /** Test 16 grammar check for savepoint sq1 */
    public void testGrammarCheck() throws SQLException {
        Statement s = getConnection().createStatement();
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS");
            fail("FAIL 16 Should have gotten exception for missing ON ROLLBACK "
                    + "RETAIN CURSORS");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42X01", se);
        }
        try {
            s.executeUpdate("SAVEPOINT s1 UNIQUE ON ROLLBACK RETAIN CURSORS "
                    + "ON ROLLBACK RETAIN CURSORS");
            fail("FAIL 16 Should have gotten exception for multiple ON ROLLBACK "
                    + "RETAIN CURSORS");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42613", se);
        }
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK"
                    + " RETAIN LOCKS");
            fail("FAIL 16 Should have gotten exception for multiple ON ROLLBACK "
                    + "RETAIN LOCKS");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42613", se);
        }
        try {
            s.executeUpdate("SAVEPOINT s1 UNIQUE UNIQUE ON ROLLBACK RETAIN "
                    + "LOCKS ON ROLLBACK RETAIN CURSORS");
            fail("FAIL 16 Should have gotten exception for multiple UNIQUE keywords");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42613", se);
        }
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN CURSORS ON ROLLBACK "
                + "RETAIN LOCKS");
        s.executeUpdate("RELEASE TO SAVEPOINT s1");
        getConnection().rollback();
    }

    /** Test 17 */
    public void testNoNestedSavepointsWhenUsingSQL() throws SQLException {
        Statement s = getConnection().createStatement();
        s.executeUpdate("SAVEPOINT s1 UNIQUE ON ROLLBACK RETAIN LOCKS ON "
                + "ROLLBACK RETAIN CURSORS");
        try {
            s.executeUpdate("SAVEPOINT s2 UNIQUE ON ROLLBACK RETAIN "
                    + "LOCKS ON ROLLBACK RETAIN CURSORS");
            fail("FAIL 17a Should have gotten exception for nested savepoints");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);
        }
        s.executeUpdate("RELEASE TO SAVEPOINT s1");
        s.executeUpdate("SAVEPOINT s2 UNIQUE ON ROLLBACK RETAIN LOCKS ON "
                + "ROLLBACK RETAIN CURSORS");
        getConnection().rollback();

        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON "
                    + "ROLLBACK RETAIN CURSORS");
            fail("FAIL 17b Should have gotten exception for nested savepoints");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);
        }
        getConnection().rollback();
    }

    /** Test 18 */
    public void testNoNestedSavepointsInsideJdbcSavepoint() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint();
        Statement s = getConnection().createStatement();
        // Following SQL savepoint will fail because we are trying to nest it
        // inside JDBC savepoint
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK"
                    + " RETAIN CURSORS");
            fail("FAIL 18 shouldn't be able set SQL savepoint nested inside "
                    + "JDBC savepoints");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);
        }
        // rollback the JDBC savepoint. Now since there are no user defined
        // savepoints, we can define SQL savepoint
        con.releaseSavepoint(savepoint1);
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON "
                + "ROLLBACK RETAIN CURSORS");
        con.rollback();
    }

    /** Test 19 */
    public void testNoNestedSavepointsInsideSqlSavepoint() throws SQLException {
        Statement s = getConnection().createStatement();
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        try {
            s.executeUpdate("SAVEPOINT s2 ON ROLLBACK RETAIN LOCKS ON ROLLBACK"
                    + " RETAIN CURSORS");
            fail("FAIL 19 shouldn't be able set SQL savepoint nested inside "
                    + "SQL savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);
        }
        // rollback the SQL savepoint. Now since there are no user defined
        // savepoints, we can define SQL savepoint
        s.executeUpdate("RELEASE TO SAVEPOINT s1");
        s.executeUpdate("SAVEPOINT s2 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        getConnection().rollback();
    }

    /** Test 20 */
    public void testRollbackSqlSavepointSameAsJdbc() throws SQLException {
        Connection con = getConnection();
        Statement s = createStatement();
        s.executeUpdate("DELETE FROM T1");
        con.commit();
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
        s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
        s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
        // Rollback to SQL savepoint and should see changes rolledback
        s.execute("ROLLBACK TO SAVEPOINT s1");

        ResultSet rs1 = s.executeQuery("select count(*) from t1");
        rs1.next();
        assertEquals(rs1.getInt(1), 0);
        con.rollback();
    }

    /** Test 21 */
    public void testReleaseSqlSavepointAndRollback() throws SQLException {
        Connection con = getConnection();
        Statement s = createStatement();
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
        s.executeUpdate("INSERT INTO T1 VALUES(3,1)");
        // Release the SQL savepoint and then rollback the transaction and
        // should see changes rolledback
        s.executeUpdate("RELEASE TO SAVEPOINT s1");
        con.rollback();
        ResultSet rs1 = s.executeQuery("select count(*) from t1");
        rs1.next();
        assertEquals(rs1.getInt(1), 0);
        con.rollback();
    }

    /** Test 22 */
    public void testNoSqlSavepointStartingWithSYS() throws SQLException {
        Statement s = createStatement();
        try {
            s.executeUpdate("SAVEPOINT SYSs2 ON ROLLBACK RETAIN LOCKS ON "
                    + "ROLLBACK RETAIN CURSORS");
            fail("FAIL 22 shouldn't be able to create a SQL savepoint starting "
                    + "with name SYS");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42939", se);
        }
        getConnection().rollback();
    }

    /**
     * Test 23 - bug 5817 - make savepoint and release non-reserved keywords
     */
    public void testBug5817() throws SQLException {
        Statement s = createStatement();
        s.execute("create table savepoint (savepoint int, release int)");
        ResultSet rs1 = s.executeQuery("select count(*) from savepoint");
        rs1.next();
        assertEquals(" There should have been 0 rows in the table, but found "
                + rs1.getInt(1), rs1.getInt(1), 0);

        s.execute("SAVEPOINT savepoint ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        s.executeUpdate("INSERT INTO savepoint VALUES(1,1)");
        s.execute("RELEASE SAVEPOINT savepoint");
        rs1 = s.executeQuery("select count(*) from savepoint");
        rs1.next();
        assertEquals("There should have been 1 rows in the table, but found "
                + rs1.getInt(1), rs1.getInt(1), 1);

        s.execute("SAVEPOINT release ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        s.executeUpdate("INSERT INTO savepoint VALUES(2,1)");
        s.execute("ROLLBACK TO SAVEPOINT release");
        rs1 = s.executeQuery("select count(*) from savepoint");
        rs1.next();
        assertEquals(
                "ERROR: There should have been 1 rows in the table, but found "
                        + rs1.getInt(1), rs1.getInt(1), 1);

        s.execute("RELEASE SAVEPOINT release");
        getConnection().rollback();
    }

    /**
     * Test 24 Savepoint name can't exceed 128 characters
     */
    public void testNameLengthMax128Chars() throws SQLException {
        try {
            getConnection()
                    .setSavepoint(
                            "MyName12345678901234567890123456789"
                                    + "01234567890123456789012345678901234567890123456789012345"
                                    + "678901234567890123456789012345678901234567890");
            fail("FAIL 24 shouldn't be able to create a SQL savepoint with "
                    + "name exceeding 128 characters");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42622", se);
        }
        getConnection().rollback();
    }

    /** Test 25 */
    public void testNoSqlSavepointStartingWithSYSThroughJdbc()
            throws SQLException {
        try {
            getConnection().setSavepoint("SYSs2");
            fail("FAIL 25 shouldn't be able to create a SQL savepoint starting with name SYS through jdbc");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("42939", se);
        }
        getConnection().rollback();
    }

    /**
     * bug 4451 - Test 26a pass Null value to rollback bug 5374 - Passing a null
     * savepoint to rollback or release method used to give a npe in JCC it
     * should give a SQLException aying "Cannot rollback to a null savepoint"
     */
    public void testRollbackNullSavepoint() throws SQLException {
        try {
            getConnection().rollback((Savepoint) null);
            fail("FAIL 26a rollback of null savepoint did not raise error ");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B001", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("3B502", se);
            }
        }
    }

    /**
     * Test 26b pass Null value to releaseSavepoint
     */
    public void testReleaseNullSavepoint() throws SQLException {
        try {
            getConnection().releaseSavepoint((Savepoint) null);
            fail("FAIL 26b release of null savepoint did not raise error ");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B001", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("3B502", se);
            }
        }
    }

    /**
     * Test that savepoint names can have double-quote characters. The client
     * driver used to fail with a syntax error when the names contained such
     * characters. DERBY-5170.
     */
    public void testQuotes() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table test_quotes(x int)");
        s.execute("insert into test_quotes values 1");

        Savepoint sp = getConnection().setSavepoint("a \" b ' c");

        s.execute("insert into test_quotes values 2");

        getConnection().rollback(sp);

        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from test_quotes"),
                "1");

        getConnection().releaseSavepoint(sp);
    }

    /** ********************* */

    /*
     * The following tests have nested savepoints through JDBC calls. When
     * coming through the network client these nested JDBC savepoint calls are
     * translated into equivalent SQL savepoint statements. But we do not allow
     * nested savepoints coming through SQL statements and hence these tests
     * can't be run under DRDA framework.
     */

    /**
     * Test40 - We internally generate a unique name for unnamed savepoints. If
     * a named savepoint uses the currently used internal savepoint name, we
     * won't get an exception thrown for it because we prepend external saves
     * with "e." to avoid name conflicts.
     */
    public void xtestNoConflictWithGeneratedName() throws SQLException {
        Connection con = getConnection();

        con.setSavepoint();
        con.setSavepoint("i.SAVEPT0");
        con.rollback();
    }

    /**
     * Test41 - Rolling back to a savepoint will release all the savepoints
     * created after that savepoint.
     */
    public void xtestRollbackWillReleaseLaterSavepoints() throws SQLException {
        Connection con = getConnection();
        
        Statement s = createStatement();

        // Make sure T1 is empty (testcase running order might have left content!):
//IC see: https://issues.apache.org/jira/browse/DERBY-3824
        s.execute("DELETE FROM T1");
        
        Savepoint savepoint1 = con.setSavepoint();

        s.executeUpdate("INSERT INTO T1 VALUES(1,1)");

        Savepoint savepoint2 = con.setSavepoint("s1");
        s.executeUpdate("INSERT INTO T1 VALUES(2,1)");

        Savepoint savepoint3 = con.setSavepoint("s2");
        s.executeUpdate("INSERT INTO T1 VALUES(3,1)");

        // Rollback to first named savepoint s1. This will internally release
        // the second named savepoint s2.
        con.rollback(savepoint2);
        assertTableRowCount("T1", 1);

        // Trying to release second named savepoint s2 should throw exception.
        try {
            con.releaseSavepoint(savepoint3);
            fail("FAIL 41a release of rolled back savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
        // Trying to rollback second named savepoint s2 should throw exception.
        try {
            con.rollback(savepoint3);
            fail("FAIL 41b release of rolled back savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }

        // Release the unnamed named savepoint.
        con.rollback(savepoint1);
        assertTableRowCount("T1", 0);
        con.rollback();
    }

    /**
     * Test42 - Rollback on a connection will release all the savepoints created
     * for that transaction
     */
    public void xtestRollbackWillReleaseActiveSavepoints() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint();
        con.rollback();
        try {
            con.rollback(savepoint1);
            fail("FAIL 42 release of rolled back savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
    }

    /**
     * Test42a - Commit on a connection will release all the savepoints created
     * for that transaction
     */
    public void xtestCommitWillReleaseActiveSavepoints() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint();
        con.commit();
        try {
            con.rollback(savepoint1);
            fail("FAIL 42a Rollback after commit.");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B001", se);
        }
    }

    /**
     * Test43 - After releasing a savepoint, should be able to reuse it.
     */
    public void xtestReuseNameAfterRelease() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("s1");
        try {
            con.setSavepoint("s1");
            fail("Should not be able to set two savepoints with the same name");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B501", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("3B002", se);
            }
        }
        con.releaseSavepoint(savepoint1);
        con.setSavepoint("s1");
        con.rollback();
    }

    /**
     * Test 45 reuse savepoint name after rollback - should not work
     */
    public void xtestReuseNameAfterRollback() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint("MyName");
        con.rollback(savepoint1);
        try {
            con.setSavepoint("MyName");
            fail("FAIL 45 reuse of savepoint name after rollback should fail");
        } catch (SQLException se) {
            // Expected exception.
            if (usingEmbedded()) {
                assertSQLState("3B501", se);
            } else if (usingDerbyNetClient()) {
                assertSQLState("3B002", se);
            }
        }
        con.rollback();
    }

    /**
     * Test 46 bug 5145 Cursors declared before and within the savepoint unit
     * will be closed when rolling back the savepoint
     */
    public void xtestCursorsCloseOnRollback() throws SQLException {
        Connection con = getConnection();
        Statement sWithHold = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        Statement s = createStatement();
        s.executeUpdate("DELETE FROM T1");
        s.executeUpdate("INSERT INTO T1 VALUES(19,1)");
        s.executeUpdate("INSERT INTO T1 VALUES(19,2)");
        s.executeUpdate("INSERT INTO T1 VALUES(19,3)");
        ResultSet rs1 = s.executeQuery("select * from t1");
        rs1.next();
        ResultSet rs1WithHold = sWithHold.executeQuery("select * from t1");
        rs1WithHold.next();
        Savepoint savepoint1 = con.setSavepoint();
        ResultSet rs2 = s.executeQuery("select * from t1");
        rs2.next();
        ResultSet rs2WithHold = sWithHold.executeQuery("select * from t1");
        rs2WithHold.next();
        con.rollback(savepoint1);
        try {// resultset declared outside the savepoint unit should be
            // closed at this point after the rollback to savepoint
            rs1.next();
            fail("FAIL 46 shouldn't be able to use a resultset (declared "
                    + "before the savepoint unit) after the rollback to savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XCL16", se);
        }
        try {// holdable resultset declared outside the savepoint unit should
            // be closed at this point after the rollback to savepoint
            rs1WithHold.next();
            fail("FAIL 46 shouldn't be able to use a holdable resultset "
                    + "(declared before the savepoint unit) after the rollback "
                    + "to savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XCL16", se);
        }
        try {// resultset declared within the savepoint unit should be closed
            // at this point after the rollback to savepoint
            rs2.next();
            fail("FAIL 46 shouldn't be able to use a resultset (declared within "
                    + "the savepoint unit) after the rollback to savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XCL16", se);
        }
        try {// holdable resultset declared within the savepoint unit should
            // be closed at this point after the rollback to savepoint
            rs2WithHold.next();
            fail("FAIL 46 shouldn't be able to use a holdable resultset "
                    + "(declared within the savepoint unit) after the rollback "
                    + "to savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("XCL16", se);
        }
        con.rollback();
    }

    /**
     * Test 47 multiple tests for getSavepointId()
     */
    public void xtestGetSavepoint() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint();
        Savepoint savepoint2 = con.setSavepoint();
        savepoint1.getSavepointId();
        savepoint2.getSavepointId();
        con.releaseSavepoint(savepoint2);
        savepoint2 = con.setSavepoint();
        savepoint2.getSavepointId();
        con.commit();
        savepoint2 = con.setSavepoint();
        savepoint2.getSavepointId();
        con.rollback();
        savepoint2 = con.setSavepoint();
        savepoint2.getSavepointId();
        con.rollback();
    }

    /**
     * Test 48
     */
    public void xtestNestedSavepoints() throws SQLException {
        Connection con = getConnection();
        Savepoint savepoint1 = con.setSavepoint();
        Savepoint savepoint2 = con.setSavepoint();
        Statement s = createStatement();
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK"
                    + " RETAIN CURSORS");
            fail("FAIL 48 shouldn't be able set SQL savepoint nested inside "
                    + "JDBC/SQL savepoints");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);
        }
        // rollback JDBC savepoint but still can't have SQL savepoint because
        // there is still one JDBC savepoint
        con.releaseSavepoint(savepoint2);
        try {
            s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK"
                    + " RETAIN CURSORS");
            fail("FAIL 48 Should have gotten exception for nested SQL savepoint");
        } catch (SQLException se) {
            // Expected exception.
            assertSQLState("3B002", se);

        }
        con.releaseSavepoint(savepoint1); // rollback last JDBC savepoint and
        // now try SQL savepoint again
        s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK "
                + "RETAIN CURSORS");
        con.rollback();
    }

}

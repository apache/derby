/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.RolesTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This JUnit tests the SQL roles feature. This feature relies on
 * sqlAuthorization being set. Upgrade tests are not handled here.
 *
 * The tests are run in the cross product:
 *
 *    {client/server, embedded} x
 *    {no sqlAuthorization/sqlAuthorization} x
 *    {data base owner, other user }
 *
 */
public class RolesTest extends BaseJDBCTestCase
{
    /* internal state */
    private final int _authLevel;
    private final String _user;
    private final String _userPassword;
    private Connection _conn;
    private Statement _stm;

    /* test execution security context: one of two below */
    private final static int NO_SQLAUTHORIZATION=0;
    private final static int SQLAUTHORIZATION=1;

    private final static String pwSuffix = "pwSuffix";

    /* SQL states */
    private final static String sqlAuthorizationRequired = "42Z60";
    private final static String syntaxError = "42X01";
    // temporary until feature fully implemented:
    private final static String notImplemented = "0A000";

    /**
     * Users used by all suites when when authLevel == SQLAUTHORIZATION.
     * The TestConfiguration.sqlAuthorizationDecorator decorator presumes
     * TEST_DBO as dbo, so add it to set of valid users. It uses a fresh db
     * 'dbsqlauth', not 'wombat'.
     */
    private final static String[] users = {"TEST_DBO", "DonaldDuck"};

    private boolean isDbo() {
        return users[0].equals(this._user);
    }

    /**
     * Create a new instance of RolesTest.
     *
     * @param name Fixture name
     * @param authLevel authentication level with which test is run
     * @param user Database user
     * @param userPassword Database user's password
     */

    public RolesTest(String name, int authLevel,
                     String user, String userPassword)
    {
        super(name);
        this._authLevel = authLevel;
        this._user = user;
        this._userPassword = userPassword;
    }


    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     *         Client/server suite commented out to speed up this test as
     *         it does not add much value given the nature of the changes
     *         (SQL language only).
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("RolesTest");

        /* Negative syntax tests */
        suite.addTest(negativeSyntaxSuite("suite: negative syntax, embedded"));

        // suite.addTest(
        //     TestConfiguration.clientServerDecorator(
        //         negativeSyntaxSuite("suite: negative syntax, client")));

        /* Positive tests */
        suite.addTest(
            positiveSuite("suite: positive, embedded"));

        // suite.addTest(
        //     TestConfiguration.clientServerDecorator(
        //         positiveSuite("suite: positive, client")));

        return suite;
    }

    /**
     *
     * Construct suite of tests for negative syntax
     *
     * @param framework Derby framework indication
     * @return A suite containing the test cases for negative syntax
     * incarnated for the two security levels no sqlAuthorization, and
     * sqlAuthorization, The latter has an instance for dbo, and one
     * for an ordinary user, so there are in all three incarnations of
     * tests.
     */
    private static Test negativeSyntaxSuite(String framework)
    {
        Test tests[] = new Test[SQLAUTHORIZATION+1]; // one per authLevel

        /* Tests running without sql authorization set.
         */
        TestSuite noauthSuite = new TestSuite(
            "suite: security level=noSqlAuthorization");
        noauthSuite.addTest(new RolesTest("testNegativeSyntax",
                                          NO_SQLAUTHORIZATION,
                                          null,
                                          null));
        tests[NO_SQLAUTHORIZATION] = noauthSuite;

        /* Tests running with sql authorization set.
         * First decorate with users, then with authentication +
         * sqlAuthorization.
         */
        tests[SQLAUTHORIZATION] = wrapTest("testNegativeSyntax");


        TestSuite suite = new TestSuite("roles:"+framework);
        suite.addTest(tests[NO_SQLAUTHORIZATION]);
        suite.addTest(tests[SQLAUTHORIZATION]);

        return suite;
    }



    /**
     * Wraps the negative syntax fixture in decorators to run with
     * data base owner and one other valid user in sqlAuthorization
     * mode.
     */

    /**
     * Test negative syntax for roles.
     *
     * @throws SQLException
     */
    public void testNegativeSyntax() throws SQLException
    {
        println("testNegativeSyntax: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        _conn = getConnection();
        _stm  = _conn.createStatement();

        doStmt("create role none", // none is reserved word
               syntaxError, syntaxError, syntaxError);
        doStmt("create role current_role", // current_role is reserved word
               syntaxError, syntaxError, syntaxError);
    }

    /**
     *
     * Construct suite of positive tests
     *
     * @param framework Derby framework indication
     *
     * @return A suite containing the positive test cases incarnated only
     * for security level sqlAuthorization.
     *
     * It has one instance for dbo, and one for an ordinary user, so there
     * are in all three incarnations of tests.
     */
    private static Test positiveSuite(String framework)
    {
        Test tests[] = new Test[SQLAUTHORIZATION+1]; // one per authLevel
        /* Tests running without sql authorization set.
         */
        TestSuite noauthSuite = new TestSuite(
            "suite: security level=noSqlAuthorization");
        noauthSuite.addTest(new RolesTest("testPositive",
                                          NO_SQLAUTHORIZATION,
                                          null,
                                          null));
        tests[NO_SQLAUTHORIZATION] = noauthSuite;
        /* Tests running with sql authorization set.
         * First decorate with users, then with authentication +
         * sqlAuthorization.
         */
        TestSuite suite = new TestSuite("roles:"+framework);
        tests[SQLAUTHORIZATION] = wrapTest("testPositive");

        suite.addTest(tests[NO_SQLAUTHORIZATION]);
        suite.addTest(tests[SQLAUTHORIZATION]);

        return suite;
    }

    /**
     * Wraps in decorators to run with data base owner and one other
     * valid user in sqlAuthorization mode.
     *
     * @param testName test to wrap
     */
    private static Test wrapTest(String testName)
    {
        // add decorator for different users authenticated
        TestSuite usersSuite =
            new TestSuite("suite: security level=sqlAuthorization");

        // First decorate with users, then with authorization
        // decorator
        for (int userNo = 0; userNo < users.length; userNo++) {
            usersSuite.addTest
                (TestConfiguration.changeUserDecorator
                 (new RolesTest(testName,
                                SQLAUTHORIZATION,
                                users[userNo],
                                users[userNo].concat(pwSuffix)),
                  users[userNo],
                  users[userNo].concat(pwSuffix)));
        }

        return TestConfiguration.sqlAuthorizationDecorator(
            DatabasePropertyTestSetup.builtinAuthentication(
                usersSuite, users, pwSuffix));
    }

    /**
     * Positive tests for roles (well, positive for dbo at least!)
     *
     * @throws SQLException
     */
    public void testPositive() throws SQLException
    {
        println("testPositive: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        _conn = getConnection();
        _stm  = _conn.createStatement();

        // create
        doStmt("create role foo",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("create role bar",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("create role role", // role is not reserved word
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("create role trigger",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("create role \"NONE\"", // quoted role id should work
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // grant
        doStmt("grant foo to authid", // authid: user or role
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("grant foo, role, bar to authid1, authid2, authid3",
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // grant: parser look-ahead tests to discern grant role from
        // grant privilege
        doStmt("grant trigger to authid",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("grant trigger, foo to authid",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("grant trigger, foo to public",
               sqlAuthorizationRequired, notImplemented, notImplemented);


        // set
        doStmt("set role foo",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("set role 'FOO'",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("set role none",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doDynamicSetRole(_conn);

        // revoke

        doStmt("revoke foo from authid", // authid: user or role
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("revoke foo, role, bar from authid1, authid2, authid3",
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // revoke: parser look-ahead tests to discern revoke role from
        // revoke privilege
        doStmt("revoke trigger from authid",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("revoke trigger, foo from authid",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("revoke trigger, foo from public",
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // drop
        doStmt("drop role foo",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("drop role role",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("drop role trigger",
               sqlAuthorizationRequired, notImplemented, notImplemented);
        doStmt("drop role \"NONE\"",
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // current_role
        doStmt("values current_role",
               sqlAuthorizationRequired, notImplemented, notImplemented);

        // column default current_role
        doStmt("create table foo(str varchar(128) default current_role)",
               sqlAuthorizationRequired, notImplemented, notImplemented);
    }

    // Minion to analyze outcome. If state string is empty, we expect success
    // for that combination of authentication level and user (dbo or not).
    private void doStmt(String stmt,
                        String noAuthState,
                        String authDboState,
                        String authNotDboState) {
        try {
            _stm.execute(stmt);
            if (_authLevel == NO_SQLAUTHORIZATION) {
                if (noAuthState != null) {
                    fail("exception " + noAuthState + " expected: (" + stmt);
                }
            } else { // SQLAUTHORIZATION
                if (isDbo()) {
                    if (authDboState != null) {
                        fail("exception " + noAuthState + " expected: (" +
                             stmt);
                    }
                } else {
                    if (authNotDboState != null) {
                        fail("exception " + noAuthState + " expected: (" +
                             stmt);
                    }
                }
            }
        } catch (SQLException e) {
            if (_authLevel == NO_SQLAUTHORIZATION) {
                if (noAuthState == null) {
                    fail("stmt " + stmt + " failed with exception " +
                         e.getSQLState());
                } else {
                    assertSQLState("Stmt " + stmt, noAuthState, e);
                }

            } else { // SQLAUTHORIZATION
                if (isDbo()) {
                    if (authDboState == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState());
                    } else {
                        assertSQLState("Stmt " + stmt, authDboState, e);
                    }
                } else {
                    if (authNotDboState == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState());
                    } else {
                        assertSQLState("Stmt " + stmt, authNotDboState, e);
                    }
                }
            }
        }
    }


    private void doDynamicSetRole(Connection conn) {
        PreparedStatement pstmt = null;

        try {
            pstmt = conn.prepareStatement("set role ?");

            if (_authLevel == NO_SQLAUTHORIZATION) {
                fail("set role ? should have failed; no sqlAuthorization");
            }
        } catch (SQLException e) {
             if (_authLevel == NO_SQLAUTHORIZATION) {
                 assertSQLState(sqlAuthorizationRequired, e);
                 return;
             } else {
                 // fail("prepare of set role ? failed:" + e);
                 assertSQLState(notImplemented, e);
                 return;
             }
        }

        try {
            pstmt.setString(1, "foo");
            int rowcnt = pstmt.executeUpdate();
            assertEquals(rowcnt, 0, "rowcount from set role ? not 0");
        } catch (SQLException e) {
            assertSQLState(notImplemented, e);
        }


        try {
            pstmt.setString(1, "\"NONE\"");
            int rowcnt = pstmt.executeUpdate();
            assertEquals(rowcnt, 0, "rowcount from set role ? not 0");
        } catch (SQLException e) {
            assertSQLState(notImplemented, e);
        }
    }


    private void assertEquals(int a, int b, String txt) {
        if (a!=b) {
            fail(txt);
        }
    }
}

/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RolesTest

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
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
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
    private final static String syntaxError              = "42X01";
    private final static String roleDboOnly              = "4251A";
    private final static String invalidRole              = "0P000";
    private final static String tooLongId                = "42622";
    private final static String revokeWarn               = "01007";
    private final static String notIdle                  = "25001";
    private final static String invalidRoleName          = "4293A";

    private int MAX_IDENTIFIER_LENGTH = 128;
    /**
     * Users used by all suites when when authLevel == SQLAUTHORIZATION.
     * The TestConfiguration.sqlAuthorizationDecorator decorator presumes
     * TEST_DBO as dbo, so add it to set of valid users. It uses a fresh db
     * 'dbsqlauth', not 'wombat'.
     */
    private final static String[] users = {"TEST_DBO", "DonaldDuck"};
    private final static int dboIndex        = 0;
    private final static int nonDboIndex = 1;

    private boolean isDbo()
    {
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

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                negativeSyntaxSuite("suite: negative syntax, client")));

        /* Positive syntax tests */
        suite.addTest(positiveSyntaxSuite("suite: positive syntax, embedded"));

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                positiveSyntaxSuite("suite: positive syntax, client")));

        /* Positive tests */
        suite.addTest(
            positiveSuite("suite: positive, embedded"));

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                positiveSuite("suite: positive, client")));

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
        TestSuite suite = new TestSuite("roles:"+framework);

        /* Tests running without sql authorization set.
         */
        TestSuite noauthSuite = new TestSuite(
            "suite: security level=noSqlAuthorization");
        noauthSuite.addTest(new RolesTest("testNegativeSyntax",
                                          NO_SQLAUTHORIZATION,
                                          null,
                                          null));
        suite.addTest(noauthSuite);

        // Tests running with sql authorization set.
        suite.addTest(wrapInAuthorization("testNegativeSyntax"));

        return suite;
    }


    /**
     *
     * Construct suite of tests for positive syntax (edge cases)
     *
     * @param framework Derby framework indication
     * @return A suite containing the test cases for  syntax
     * edge cases. Incarnated only for sqlAuthorization, dbo.
     */
    private static Test positiveSyntaxSuite(String framework)
    {
        String dbo = users[dboIndex];
        String dbopw = dbo.concat(pwSuffix);

        Test t = (TestConfiguration.changeUserDecorator
                  (new RolesTest("testPositiveSyntax",
                                 SQLAUTHORIZATION,
                                 dbo, dbopw),
                   dbo, dbopw));

        return TestConfiguration.sqlAuthorizationDecorator(
            DatabasePropertyTestSetup.builtinAuthentication(
                t, users, pwSuffix));
    }


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

        char[] longname = new char[MAX_IDENTIFIER_LENGTH + 1];
        java.util.Arrays.fill(longname, 'a');
        String nameWithMoreThanMaxChars = new String(longname);

        doStmt("create role " + nameWithMoreThanMaxChars,
               tooLongId, tooLongId, tooLongId);
        // Check SYS-prefix ban
        doStmt("create role sysrole",
               sqlAuthorizationRequired, invalidRoleName, invalidRoleName);
        doStmt("create role \"SYSROLE\"",
               sqlAuthorizationRequired, invalidRoleName, invalidRoleName);
        _stm.close();
    }


    /**
     * Test syntax edge cases (positive)
     *
     * @throws SQLException
     */
    public void testPositiveSyntax() throws SQLException
    {
        println("testPositiveSyntax");
        String n_a     = null; // auth level not used for this test
        int    n_a_cnt = -1;   //

        // sanity check:
        if (!isDbo()) { throw new SQLException("test error"); }


        _conn = getConnection();
        _stm  = _conn.createStatement();

        // "trigger" is not reserved word, but required tweaking of grammar
        // so we add a regression test for it.
        doStmt("create role trigger", n_a, null, n_a);

        // "role" is not a reserved word, either:
        doStmt("create role role", n_a, null, n_a);

        // Check that role name can be longer than present user name
        // (max 30, cf.  Limits.DB2_MAX_USERID_LENGTH).
        String nameWithMoreThan30Chars = ("r123456789" +
                                          "0123456789" +
                                          "01234567890"); // 31 long
        doStmt("create role " + nameWithMoreThan30Chars,
               n_a, null, n_a);

        assertSysRolesRowCount(n_a_cnt, 3, n_a_cnt);

        doStmt("grant trigger to foo", n_a, null, n_a);
        doStmt("grant role to foo", n_a, null, n_a);
        doStmt("revoke trigger from foo", n_a, null, n_a);
        doStmt("revoke role from foo", n_a, null, n_a);

        doStmt("set role " + nameWithMoreThan30Chars, n_a, null, n_a);

        doStmt("create table mytab(i int)", n_a, null, n_a);
        doStmt("grant select on mytab to " + nameWithMoreThan30Chars,
               n_a, null, n_a);
        doStmt("revoke select on mytab from " + nameWithMoreThan30Chars,
               n_a, null, n_a);
        doStmt("drop table mytab", n_a, null, n_a);

        doStmt("drop role trigger", n_a, null, n_a);
        doStmt("drop role role", n_a, null, n_a);
        doStmt("drop role " + nameWithMoreThan30Chars, n_a, null, n_a);

        assertSysRolesRowCount(n_a_cnt, 0, n_a_cnt);

        _stm.close();
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
        /*
         * Tests running without sql authorization set.  The purpose
         * of this is just to make sure the proper errors are given.
         */
        TestSuite noauthSuite = new TestSuite(
            "suite: security level=noSqlAuthorization");
        noauthSuite.addTest(new RolesTest("testPositive",
                                          NO_SQLAUTHORIZATION,
                                          null,
                                          null));

        /* Tests running with sql authorization set.
         * First decorate with users, then with authentication +
         * sqlAuthorization.
         */
        TestSuite suite = new TestSuite("roles:"+framework);

        suite.addTest(noauthSuite);
        suite.addTest(wrapInAuthorization("testPositive"));

        return suite;
    }

    /**
     * Wrap in decorators to run with data base owner and one other
     * valid user in sqlAuthorization mode.
     *
     * @param testName test to wrap
     */
    private static Test wrapInAuthorization(String testName)
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
     * Side effect from the dbo run are needed for the nonDbo run
     * which follows (since only dbo can create and grant roles).
     *
     * @throws SQLException
     */
    public void testPositive() throws SQLException
    {
        println("testPositive: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        _conn = getConnection();
        _stm  = _conn.createStatement();


        /*
         * CREATE ROLE
         */
        doStmt("create role foo",
               sqlAuthorizationRequired, null , roleDboOnly);
        doStmt("create role bar",
               sqlAuthorizationRequired, null , roleDboOnly);
        doStmt("create role role", // role is not reserved word
               sqlAuthorizationRequired, null , roleDboOnly);
        doStmt("create role admin",
                sqlAuthorizationRequired, null , roleDboOnly);
        doStmt("create role \"NONE\"", // quoted role id should work
                sqlAuthorizationRequired, null , roleDboOnly);


        /*
         * GRANT <role>
         */
        doStmt("grant foo to authid", // authid: user or role
               sqlAuthorizationRequired, null , roleDboOnly);

        // this grant also grant role bar to the non-dbo user
        // so we can set it when running as non-dbo.
        doStmt("grant foo, role, bar to authid1, authid2, " +
               users[nonDboIndex],
               sqlAuthorizationRequired, null , roleDboOnly);

        doStmt("grant admin to authid",
               sqlAuthorizationRequired, null , invalidRole);
        doStmt("grant admin, foo to authid",
               sqlAuthorizationRequired, null , invalidRole);
        doStmt("grant admin, foo to public",
               sqlAuthorizationRequired, null , invalidRole);

        // These grants will no be explicitly revoked, count on drop
        // role to void them!
        doStmt("grant admin to a,b,c",
               sqlAuthorizationRequired, null , invalidRole);
        doStmt("grant foo,bar to admin",
               sqlAuthorizationRequired, null , roleDboOnly);

        assertSysRolesRowCount(0, 23,
                               // nonDbo run: foo, bar still in
                               // place, used for testing SET ROLE for
                               // non-dbo user. foo granted to public,
                               // bar granted to nonDbo, so 4!
                               4);


        /*
         * SET ROLE
         */
        doStmt("set role foo",
               sqlAuthorizationRequired, null , null /* through public */);
        doStmt("set role 'FOO'",
               sqlAuthorizationRequired, null, null);
        
        // JSR169 cannot run with tests with stored procedures that do
        // database access - for they require a DriverManager connection to
        // jdbc:default:connection; DriverManager is not supported with JSR169
        if (!JDBC.vmSupportsJSR169())
            doSetRoleInsideStoredProcedures("FOO");

        doStmt("set role none",
               sqlAuthorizationRequired, null , null);

        doDynamicSetRole(_conn);

        doStmt("set role bar",
               sqlAuthorizationRequired, null , null /* direct grant */);
        doStmt("set role role",
               sqlAuthorizationRequired, null , invalidRole);

        /* Test that we cannot set role while in non-idle state */
        _conn.setAutoCommit(false);
        doStmt("select * from SYS.SYSROLES", null, null, null);
        doStmt("set role role",
               sqlAuthorizationRequired, notIdle , notIdle);
        _conn.commit();
        _conn.setAutoCommit(true);



        /*
         * CURRENT_ROLE
         */
        ResultSet rs = doQuery("values current_role",
                               sqlAuthorizationRequired, null , null);
        assertRoleInRs(rs, "ROLE", "BAR");

        /*
         * REVOKE role
         */
        doStmt("revoke foo from authid", // authid: user or role
               sqlAuthorizationRequired, null , roleDboOnly);
        doStmt("revoke foo, role, bar from authid1, authid2",
               sqlAuthorizationRequired, null , roleDboOnly);
        // revoke everything from nonDbo also, except bar
        doStmt("revoke foo, role from " + users[nonDboIndex],
               sqlAuthorizationRequired, null , roleDboOnly);


        doStmt("revoke admin from authid",
               sqlAuthorizationRequired, null , invalidRole);
        doStmtWithWarnings("revoke admin from authid",
                           new String[]{sqlAuthorizationRequired, null},
                           new String[]{null, revokeWarn},
                           new String[]{invalidRole, null},
                           false);
        doStmt("revoke admin, foo from authid",
               sqlAuthorizationRequired, null , invalidRole);
        // leave foo granted to public
        doStmt("revoke admin from public",
               sqlAuthorizationRequired, null , invalidRole);



        /*
         * DEFAULT CURRENT_ROLE
         */
        doStmt("create table foo(str varchar(128) default current_role)",
               sqlAuthorizationRequired, null , null );

        /*
         * GRANT TABLE PERMISSION to role
         * Should get auto-dropped when role is dropped
         */
        doStmt("grant select, insert on foo to admin",
               sqlAuthorizationRequired, null , null );
        /*
         * GRANT COLUMN PERMISSION to role
         * Should get auto-dropped when role is dropped
         */
        doStmt("grant select (str), update (str) on foo to admin",
               sqlAuthorizationRequired, null , null );
        /*
         * GRANT ROUTINE PERMISSION to role
         * Should get auto-dropped when role is dropped
         */
        doStmt("create function f1() returns int" +
               "  language java parameter style java" +
               "  external name 'org.apache.derbyTesting." +
               "functionTests.tests.lang.RolesTest.f1'" +
               "  no sql called on null input",
               null, null, null);
        doStmt("grant execute on function f1 to admin",
               sqlAuthorizationRequired, null , null );

        assertSysTablePermsRowCount(0,
                                    // role admin not dropped yet:
                                    1,
                                    // role admin has been dropped, so
                                    // this run's grant to admin is de
                                    // facto to a user named admin:
                                    1);

        assertSysColPermsRowCount(0, 2, 2);

        assertSysRoutinePermsRowCount(5, // 5 pre-existing grants to PUBLIC
                                      6,
                                      6);

        /*
         * DROP ROLE
         */

        // Dbo run: don't drop foo and bar, so they survive to next run,
        // a non-dbo can set them, otherwise drop all roles and
        // premissions.
        doStmt("drop role role",
               sqlAuthorizationRequired, null , roleDboOnly);

        doStmt("drop role admin",
               sqlAuthorizationRequired, null , roleDboOnly);
        assertSysTablePermsRowCount(0, 0,
                                    // nonDbo run: role admin has
                                    // been dropped, so this run's
                                    // grant to admin is de facto to a
                                    // user named admin:
                                    1);
        assertSysColPermsRowCount(0, 0,
                                  // nonDbo run: role admin has
                                  // been dropped, so this run's grant
                                  // to admin is de facto to a user
                                  // named admin:
                                  2);
        assertSysRoutinePermsRowCount(5, 5,
                                      //  nonDbo run: role admin
                                      // has been dropped, so this
                                      // run's grant to admin is de
                                      // facto to a user named admin:
                                      6);

        doStmt("drop role \"NONE\"",
               sqlAuthorizationRequired, null , roleDboOnly);


        /*
         * REVOKE permissions for nonDbo run
         */
        doStmt("revoke select, insert on foo from admin",
               sqlAuthorizationRequired, null , null );
        doStmt("revoke select (str), update (str) on foo from admin",
               sqlAuthorizationRequired, null , null );
        doStmt("revoke execute on function f1 from admin restrict",
               sqlAuthorizationRequired, null , null );

        // assert blank slate
        assertSysTablePermsRowCount(0,0,0);
        assertSysColPermsRowCount(0,0,0);
        assertSysRoutinePermsRowCount(5,5,5);

        // roles foo and bar survive to nonDbo run and beyond:
        assertSysRolesRowCount(0, 4, 4);

        _stm.close();
    }


    protected void setUp() throws Exception
    {
        super.setUp();

        _stm = createStatement();

        if (_authLevel == SQLAUTHORIZATION && isDbo()) {

            // We need to clean away roles when we run the dbo run the
            // second time around (client/server). The reason is that
            // the dbo run has a side-effect: it leaves roles for the
            // non-dbo run to play with, and that run can't remove the
            // roles (since non-dbo can't drop roles).
            try {
                _stm.executeUpdate("drop role foo");
                _stm.executeUpdate("drop role bar");
            } catch (SQLException se) {
            }
        }

        try {
            _stm.executeUpdate("drop function f1");
            _stm.executeUpdate("drop table foo");
        } catch (SQLException se) {
        }

        _stm.close();
    }


    protected void tearDown() throws Exception
    {
        if (_stm != null) {
            _stm.close();
            _stm = null;
        }

        if (_conn != null) {
            _conn = null;
        }

        super.tearDown();
    }


    private void doStmt(String stmt,
                             String noAuthState,
                             String authDboState,
                             String authNotDboState)
    {
        doStmt(stmt, noAuthState, authDboState, authNotDboState, false);
    }


    private ResultSet doQuery(String stmt,
                              String noAuthState,
                              String authDboState,
                              String authNotDboState)
    {
        return doStmt(stmt, noAuthState, authDboState, authNotDboState, true);
    }


    private ResultSet doStmt(String stmt,
                             String noAuthState,
                             String authDboState,
                             String authNotDboState,
                             boolean query)
    {
        return doStmtWithWarnings(stmt,
                                  new String[]{noAuthState,null},
                                  new String[]{authDboState, null},
                                  new String[]{authNotDboState, null},
                                  query);
    }

    // Minion to analyze outcome. If state string is empty, we expect success
    // for that combination of authentication level and user (dbo or not).
    // State arrays: element 0: expected error, element 1: expected warning
    private ResultSet doStmtWithWarnings(String stmt,
                                         String[] noAuthState,
                                         String[] authDboState,
                                         String[] authNotDboState,
                                         boolean query)
    {
        ResultSet result = null;

        try {
            if (query) {
                result = _stm.executeQuery(stmt);
            } else {
                _stm.execute(stmt);
            }

            if (_authLevel == NO_SQLAUTHORIZATION) {
                if (noAuthState[0] != null) {
                    fail("exception " + noAuthState[0] + " expected: (" + stmt);
                }
                if (noAuthState[1] != null) {
                    SQLWarning w = _stm.getWarnings();
                    assertNotNull("Expected warning but found none", w);
                    assertSQLState(noAuthState[1], w);
                }

            } else { // SQLAUTHORIZATION
                if (isDbo()) {
                    if (authDboState[0] != null) {
                        fail("exception " + noAuthState[0] + " expected: (" +
                             stmt);
                    }
                    if (authDboState[1] != null) {
                        SQLWarning w = _stm.getWarnings();
                        assertNotNull("Expected warning but found none", w);
                        assertSQLState(authDboState[1], w);
                    }
                } else {
                    if (authNotDboState[0] != null) {
                        fail("exception " + noAuthState[0] + " expected: (" +
                             stmt);
                    }
                    if (authNotDboState[1] != null) {
                        SQLWarning w = _stm.getWarnings();
                        assertNotNull("Expected warning but found none", w);
                        assertSQLState(authNotDboState[1], w);
                    }
                }
            }
        } catch (SQLException e) {
            if (_authLevel == NO_SQLAUTHORIZATION) {
                if (noAuthState[0] == null) {
                    fail("stmt " + stmt + " failed with exception " +
                         e.getSQLState());
                } else {
                    assertSQLState("Stmt " + stmt, noAuthState[0], e);
                }

            } else { // SQLAUTHORIZATION
                if (isDbo()) {
                    if (authDboState[0] == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState());
                    } else {
                        assertSQLState("Stmt " + stmt, authDboState[0], e);
                    }
                } else {
                    if (authNotDboState[0] == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState());
                    } else {
                        assertSQLState("Stmt " + stmt, authNotDboState[0], e);
                    }
                }
            }
        }

        return result;
    }


    private void doDynamicSetRole(Connection conn)
    {
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
                 fail("prepare of set role ? failed:" + e);
             }
        }

        try {
            pstmt.setString(1, "BAR");
            int rowcnt = pstmt.executeUpdate();
            assertEquals(rowcnt, 0, "rowcount from set role ? not 0");
        } catch (SQLException e) {
            fail("execute of set role ? failed: [foo]" + e);
        }


        try {
            pstmt.setString(1, null);
            int rowcnt = pstmt.executeUpdate();
            assertEquals(rowcnt, 0, "rowcount from set role ? not 0");
        } catch (SQLException e) {
            fail("execute of set role ? failed: [NONE] " + e);
        }

        if (isDbo()) {
            // not granted to non-dbo, so don't try..
            String n_a     = null; // auth level not used for this test

            try {
                pstmt.setString(1, "NONE");
                int rowcnt = pstmt.executeUpdate();
                assertEquals(rowcnt, 0, "rowcount from set role ? not 0");
                ResultSet rs = doQuery("values current_role", n_a, null , n_a );
                assertRoleInRs(rs, "NONE", n_a);
                rs.close();
            } catch (SQLException e) {
                fail("execute of set role ? failed: [NONE] " + e);
            }
        }

        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
            }
        }
    }


    /* Test that current role is handled correctly when inside a
     * stored procedure.  The SQL standard requires we have an
     * "authorization stack", see section 4.34.1.1. This implies that
     * current role is popped at end of stored procedure.
     * We test two levels deep.
     */
    private void doSetRoleInsideStoredProcedures(String currRole)
            throws SQLException
    {
        if (_authLevel != NO_SQLAUTHORIZATION) {
            String n_a = null; // auth level not used for this test

            doStmt("create procedure p2(role varchar(255))" +
                   "  dynamic result sets 1 language java parameter style java"+
                   "  external name 'org.apache.derbyTesting." +
                   "functionTests.tests.lang.RolesTest.p2'" +
                   "  modifies sql data",
                   n_a, null, null);
            doStmt("create function f2(role varchar(255))" +
                   "  returns int language java parameter style java" +
                   "  external name 'org.apache.derbyTesting." +
                   "functionTests.tests.lang.RolesTest.f2'" +
                   "  reads sql data",
                   n_a, null, null);
            doStmt("call p2('" + currRole + "')",
                   n_a , null , null );

            // Dynamic result set: At what time should CURRENT_ROLE be
            // evaluated?  Logically at the inside, so it should be
            // "BAR" also when accessed on outside. I think. Anyway,
            // that's what's implemented: the activation of the call
            // is still live and holds the current role as it was
            // inside the nested scope even when the procedure call
            // has returned.
            ResultSet prs = _stm.getResultSet();
            assertRoleInRs(prs, "BAR", "BAR");
            prs.close();

            // check that role didn't get impacted by change inside p2
            // too 'BAR':
            ResultSet rs = doQuery("values current_role",
                                   n_a , null , null );
            assertRoleInRs(rs, currRole, currRole);
            rs.close();

            rs = doQuery("values f2('" + currRole + "')",
                         n_a , null , null );
            rs.close();

            doStmt("drop procedure p2", n_a, null, null);
            doStmt("drop function  f2", n_a, null, null);
        }
    }

    private void assertSystableRowCount(String table,
                                        int rcNoAuth,
                                        int rcDbo,
                                        int rcMereMortal)
        throws SQLException
    {
        ResultSet rs = _stm.executeQuery(
                "SELECT COUNT(*) FROM " + table);
        rs.next();
        assertEquals(table + " row count:",
                     _authLevel == NO_SQLAUTHORIZATION ? rcNoAuth :
                     (isDbo() ? rcDbo : rcMereMortal),
                     rs.getInt(1));
        rs.close();
    }


    private void assertSysRolesRowCount(int rcNoAuth,
                                        int rcDbo,
                                        int rcMereMortal)
        throws SQLException
    {

        if (TestConfiguration.getCurrent().isVerbose()) {
            dumpSysRoles();
        }

        assertSystableRowCount("SYS.SYSROLES",
                               rcNoAuth, rcDbo, rcMereMortal);
    }


    private void assertSysTablePermsRowCount(int rcNoAuth,
                                             int rcDbo,
                                             int rcMereMortal)
        throws SQLException
    {

        if (TestConfiguration.getCurrent().isVerbose()) {
            dumpSysTablePerms();
        }

        assertSystableRowCount("SYS.SYSTABLEPERMS",
                               rcNoAuth, rcDbo, rcMereMortal);
    }


    private void assertSysColPermsRowCount(int rcNoAuth,
                                           int rcDbo,
                                           int rcMereMortal)
        throws SQLException
    {

        if (TestConfiguration.getCurrent().isVerbose()) {
            dumpSysColPerms();
        }

        assertSystableRowCount("SYS.SYSCOLPERMS",
                               rcNoAuth, rcDbo, rcMereMortal);
    }


    private void assertSysRoutinePermsRowCount(int rcNoAuth,
                                               int rcDbo,
                                               int rcMereMortal)
        throws SQLException
    {

        if (TestConfiguration.getCurrent().isVerbose()) {
            dumpSysRoutinePerms();
        }

        assertSystableRowCount("SYS.SYSROUTINEPERMS",
                               rcNoAuth, rcDbo, rcMereMortal);
    }


    private void dumpSysRoles() throws SQLException
    {

        ResultSet rs = _stm.executeQuery
            ("SELECT * FROM SYS.SYSROLES ORDER BY ROLEID");

        println("SYS.SYSROLES:");

        while (rs.next()) {
            println("r=" + rs.getString(1) + " -ee:" + rs.getString(2) +
                    " -or:" + rs.getString(3) + " a:" + rs.getString(4) +
                    " d:" + rs.getString(5));
        }

        rs.close();
    }


    private void dumpSysTablePerms() throws SQLException
    {
        ResultSet rs = _stm.executeQuery
            ("SELECT * FROM SYS.SYSTABLEPERMS");

        println("SYS.SYSTABLEPERMS:");

        while (rs.next()) {
            println("id: " + rs.getString(1) +
                    " -ee:" + rs.getString(2) +
                    " -or:" + rs.getString(3) +
                    " tableid:" + rs.getString(4) +
                    " S:" + rs.getString(5) +
                    " D:" + rs.getString(6) +
                    " I:" + rs.getString(7) +
                    " U:" + rs.getString(8) +
                    " R:" + rs.getString(9) +
                    " T:" + rs.getString(10));
        }

        rs.close();
    }


    private void dumpSysColPerms() throws SQLException
    {

        ResultSet rs = _stm.executeQuery
            ("SELECT * FROM SYS.SYSCOLPERMS");

        println("SYS.SYSCOLPERMS:");

        while (rs.next()) {
            println("id: " + rs.getString(1) +
                    " -ee:" + rs.getString(2) +
                    " -or:" + rs.getString(3) +
                    " tableid:" + rs.getString(4) +
                    " type:" + rs.getString(5) +
                    " col#:" + rs.getString(6));
        }

        rs.close();
    }


    private void dumpSysRoutinePerms() throws SQLException
    {

        ResultSet rs = _stm.executeQuery
            ("SELECT * FROM SYS.SYSROUTINEPERMS");

        println("SYS.SYSROUTINEPERMS:");

        while (rs.next()) {
            println("id: " + rs.getString(1) +
                    " -ee:" + rs.getString(2) +
                    " -or:" + rs.getString(3) +
                    " alias:" + rs.getString(4) +
                    " grantopt:" + rs.getString(5));
        }

        rs.close();
    }


    private void assertRoleInRs(ResultSet rs,
                                String dboRole,
                                String notDboRole)
        throws SQLException
    {

        if (_authLevel == NO_SQLAUTHORIZATION) {
            assertNull(rs);
        } else {
            assertTrue("result set empty", rs.next());
            String actualRole = rs.getString(1);

            if (isDbo()) {
                assertTrue("role is " + actualRole + ", expected " + dboRole,
                           dboRole.equals(actualRole));
            } else {
                assertTrue("role is " + actualRole + ", expected " + notDboRole,
                           notDboRole.equals(actualRole));
            }

            // cardinality should be 1
            assertFalse("result set not empty", rs.next());
        }
    }

    private void assertEquals(int a, int b, String txt)
    {
        if (a!=b) {
            fail(txt);
        }
    }

    private static void assertRsSingleStringValue(ResultSet rs,
                                                  String expectedValue)
            throws SQLException
    {

        assertTrue("result set empty", rs.next());
        String actualValue = rs.getString(1);

        assertTrue("string is " + actualValue + ", expected " + expectedValue,
                   actualValue.equals(expectedValue));

        // cardinality should be 1
        assertFalse("result set not empty", rs.next());
    }


    /**
     * Utility function used to test auto-drop of grant routine
     * permission to a role
     * @return 1
     */
    public static int f1()
    {
        return 1;
    }


    /**
     * Utility procedure used to test that current role
     * is stacked correctly according to dynamic scope.
     */
    public static void p2(String roleOutside, ResultSet[] rs1)
            throws SQLException
    {
        Connection conn1 = null;
        Connection conn2 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps =
                conn1.prepareStatement("values current_role");

            // check that we inherit role correctly
            ResultSet rs = ps.executeQuery();
            assertRsSingleStringValue(rs, roleOutside);
            rs.close();

            // set the role to something else
            Statement stm = conn1.createStatement();
            stm.execute("set role bar");
            rs = ps.executeQuery();

            // check that role got set
            assertRsSingleStringValue(rs, "BAR");

            // another nesting level to test authorization stack even more
            stm.execute(
                "create procedure calledNestedFromP2(role varchar(255))" +
                "  language java parameter style java" +
                "  external name 'org.apache.derbyTesting." +
                "functionTests.tests.lang.RolesTest.calledNestedFromP2'" +
                "  modifies sql data");
            conn1.commit(); // need to be idle
            stm.execute("call calledNestedFromP2('BAR')");

            rs = ps.executeQuery();

            // check that role didn't get impacted by change inside
            // calledNestedFromP2 too 'FOO':
            assertRsSingleStringValue(rs, "BAR");
            stm.execute("drop procedure calledNestedFromP2");

            // Test that the role is shared by another nested
            // connection also.
            conn2 = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps2 =
                conn2.prepareStatement("values current_role");
            ResultSet rs2 = ps2.executeQuery();
            assertRsSingleStringValue(rs2, "BAR");

            // Pass out CURRENT_ROLE in a dynamic result set.
            rs = ps.executeQuery();
            rs1[0] = rs;

        } finally {

            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (Exception e) {
                }
            }

            if (conn2 != null) {
                try {
                    conn2.close();
                } catch (Exception e) {
                }
            }
        }

    }

    /**
     * Called from p2 so we get to test with a call stack 3 levels
     * deep.
     */
    public static void calledNestedFromP2(String roleOutside)
            throws SQLException
    {
        Connection conn1 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps =
                conn1.prepareStatement("values current_role");

            // check that we inherit role correctly
            ResultSet rs = ps.executeQuery();
            assertRsSingleStringValue(rs, roleOutside);
            rs.close();

            // set the role to something else
            Statement stm = conn1.createStatement();
            stm.execute("set role foo");
            rs = ps.executeQuery();

            // check that role got set
            assertRsSingleStringValue(rs, "FOO");

        } finally {
            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (Exception e) {
                }
            }
        }
    }


    /**
     * Utility function used to test that current role
     * is stacked correctly according to scope.
     */
    public static int f2(String roleOutside) throws SQLException
    {
        Connection conn1 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps =
                conn1.prepareStatement("values current_role");

            // check that we inherit role correctly
            ResultSet rs = ps.executeQuery();
            assertRsSingleStringValue(rs, roleOutside);
            rs.close();

            // set the role to something else
            Statement stm = conn1.createStatement();
            stm.execute("set role bar");
            rs = ps.executeQuery();

            // check that role got set
            assertRsSingleStringValue(rs, "BAR");

        } finally {

            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (Exception e) {
                }
            }

        }
        return 1;
    }
}

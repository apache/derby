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
import javax.sql.PooledConnection;
import javax.sql.ConnectionPoolDataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.J2EEDataSource;

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
    private final static String userException            = "38000";
    private final static String userAlreadyExists        = "X0Y68";
    private final static String invalidPUBLIC            = "4251B";
    private final static String loginFailed              = "08004";
    private final static String roleGrantCircularity     = "4251C";
    private final static String idParseError             = "XCXA0";

    private int MAX_IDENTIFIER_LENGTH = 128;
    /**
     * Users used by all suites when when authLevel == SQLAUTHORIZATION.
     * The TestConfiguration.sqlAuthorizationDecorator decorator presumes
     * TEST_DBO as dbo, so add it to set of valid users. It uses a fresh db
     * 'dbsqlauth', not 'wombat'.
     */
    private final static String[] users =
        {"TEST_DBO", "DonaldDuck", "\"additional\"\"user\""};

    private final static int
        dboIndex            = 0; // used for connections
    private final static int
        nonDboIndex         = 1; // used for connections
    private final static int
        additionaluserIndex = 2; // *not* used for connections

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

        /* Semantic tests */
        suite.addTest(
            semanticSuite("suite: semantic, embedded"));

        suite.addTest(
            TestConfiguration.clientServerDecorator(
                semanticSuite("suite: semantic, client")));

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
        doStmt("create role public",
               syntaxError, syntaxError, syntaxError);
        doStmt("create role \"PUBLIC\"",
               sqlAuthorizationRequired, invalidPUBLIC, roleDboOnly);
        doStmt("grant \"PUBLIC\" to " + users[1],
               sqlAuthorizationRequired, invalidPUBLIC, invalidPUBLIC);
        doStmt("revoke \"PUBLIC\" from " + users[1],
               sqlAuthorizationRequired, invalidPUBLIC, invalidPUBLIC);
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
     * Construct suite of semantic tests
     *
     * @param framework Derby framework indication
     *
     * @return A suite containing the semantic test cases incarnated only
     * for security level sqlAuthorization.
     *
     * It has one instance for dbo, and one for an ordinary user, so there
     * are in all three incarnations of tests.
     */
    private static Test semanticSuite(String framework)
    {
        /*
         * Tests running without sql authorization set.  The purpose
         * of this is just to make sure the proper errors are given.
         */
        TestSuite noauthSuite = new TestSuite(
            "suite: security level=noSqlAuthorization");
        noauthSuite.addTest(new RolesTest("testSemantics",
                                          NO_SQLAUTHORIZATION,
                                          null,
                                          null));

        /* Tests running with sql authorization set.
         * First decorate with users, then with authentication +
         * sqlAuthorization.
         */
        TestSuite suite = new TestSuite("roles:"+framework);

        suite.addTest(noauthSuite);
        suite.addTest(wrapInAuthorization("testSemantics"));

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

        // First decorate with users (except "additionaluser"), then
        // with authorization decorator
        for (int userNo = 0; userNo <= users.length - 2; userNo++) {
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
     * Semantic tests for roles.
     * Side effect from the dbo run are needed for the nonDbo run
     * which follows (since only dbo can create and grant roles).
     *
     * @throws SQLException
     */
    public void testSemantics() throws SQLException
    {
        println("testSemantics: auth=" + this._authLevel +
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

        // Verify that we can't create a role which has the same auth
        // id as a known user (DERBY-3673).
        //
        // a) built-in user:
        doStmt("create role " + users[dboIndex], sqlAuthorizationRequired,
               userAlreadyExists, roleDboOnly);

        // specified with mixed case : DonaldDuck
        doStmt("create role " + users[nonDboIndex],
                sqlAuthorizationRequired, userAlreadyExists, roleDboOnly);

        // delimited identifier with embedded text quote inside
        doStmt("create role " + users[additionaluserIndex],
                sqlAuthorizationRequired, userAlreadyExists, roleDboOnly);


        // b) A grant to this auth id exists (see setup), even though
        // it is not a built-in user, so the presumption is, it is a
        // user defined externally:
        doStmt("create role whoever", sqlAuthorizationRequired,
               userAlreadyExists, roleDboOnly);

        // c) A schema exists which has an authid we did not see
        // through properties; user has been removed, but his schema
        // lingers..
        doStmt("create role schemaowner", sqlAuthorizationRequired,
               userAlreadyExists, roleDboOnly);

        testLoginWithUsernameWhichIsARole();

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

        checkGrantCircularity();

        /*
         * SET ROLE
         */
        doStmt("set role foo",
               sqlAuthorizationRequired, null , null /* through public */);
        doStmt("set role 'FOO'",
               sqlAuthorizationRequired, null, null);

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
        assertRoleInRs(rs, "\"ROLE\"", "\"BAR\"");

        if (rs != null) {
            rs.close();
        }

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
                                    // + grant to whoever int setup
                                    2,
                                    // role admin has been dropped, so
                                    // this run's grant to admin is de
                                    // facto to a user named admin
                                    1);

        assertSysColPermsRowCount(0, 2, 2);

        assertSysRoutinePermsRowCount(6, // 5 pre-existing grants to PUBLIC
                                      7,
                                      7);

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
        assertSysTablePermsRowCount(0,
                                    // grant to whoever in setup:
                                    1,
                                    // nonDbo run: role admin has
                                    // been dropped, so this run's
                                    // grant to admin is de facto to a
                                    // user named admin
                                    1);
        assertSysColPermsRowCount(0, 0,
                                  // nonDbo run: role admin has
                                  // been dropped, so this run's grant
                                  // to admin is de facto to a user
                                  // named admin:
                                  2);
        assertSysRoutinePermsRowCount(6, 6,
                                      //  nonDbo run: role admin
                                      // has been dropped, so this
                                      // run's grant to admin is de
                                      // facto to a user named admin:
                                      7);

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

        // assert (almost) blank slate
        assertSysTablePermsRowCount(0,
                                    // grant to whoever in setup:
                                    1,
                                    0);
        assertSysColPermsRowCount(0,0,0);
        assertSysRoutinePermsRowCount(6,6,6);

        // roles foo and bar survive to nonDbo run and beyond:
        assertSysRolesRowCount(0, 4, 4);

        _stm.close();

        testCurrentRoleIsReset();
    }


    /**
     * Create a user that has the same name as a role and try to
     * log in with it; should be denied. This will catch cases
     * where we can't check up front when roles are created,
     * e.g. external authentication, or users are added after the
     * role is created (DERBY-3681).
     * @exception Exception
     */
    private void testLoginWithUsernameWhichIsARole() throws SQLException {
        if (_authLevel == SQLAUTHORIZATION && isDbo()) {
            _stm.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                         "('derby.user.soonarole', 'whatever')");

            // should work, not defined as a role yet
            openDefaultConnection("soonarole","whatever").close();

            // remove the user so we can create a role
            _stm.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                         "('derby.user.soonarole', NULL)");
            _stm.execute("create role soonarole");

            // reintroduce the colliding user name now that we have a role
            _stm.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                         "('derby.user.soonarole', 'whatever')");

            try {
                // should fail now
                openDefaultConnection("soonarole","whatever").close();
                fail("Exception expected connecting with " +
                     "user name equal to a role");
            } catch (SQLException e) {
                assertSQLState(loginFailed, e);
            }

            _stm.execute("drop role soonarole");
            _stm.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                         "('derby.user.soonarole', NULL)");
        }
    }


    /**
     * Verifies that the current role is reset when creating a new logical
     * connection.
     * <p>
     * The test is run in a non-statement pooling configuration first,
     * and then with statement pooling enabled if the environment supports it.
     * <p>
     * The test pattern is borrowed from the test case in J2EEDataSourceTest.
     *
     * @see org.apache.derbyTesting.functionTests.tests.jdbcapi.J2EEDataSourceTest#testSchemaIsReset
     *
     * @throws SQLException if something goes wrong
     */
    private void testCurrentRoleIsReset()
            throws SQLException {

        if (_authLevel == SQLAUTHORIZATION && isDbo() /* once is enough */) {
            final String user = "DonaldDuck";
            final String passwd = user.concat(pwSuffix);
            ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
            // Test without statement pooling first.
            doTestCurrentRoleIsReset(cpDs.getPooledConnection(user, passwd),
                                     user);

            // Try to enable statement pooling.
            // This is currently only implemented in the client driver.
            if (usingDerbyNetClient()) {
                J2EEDataSource.setBeanProperty(
                    cpDs, "maxStatements",new Integer(7));
                doTestCurrentRoleIsReset(cpDs.getPooledConnection(user, passwd),
                                         user);
            }
        }
    }

    /**
     * Executes a test sequence to make sure the current role is reset between
     * logical connections.
     *
     * @param pc pooled connection to get logical connections from
     * @param user name of  for the connection
     * @throws SQLException if something goes wrong...
     */
    private void doTestCurrentRoleIsReset(PooledConnection pc, String user)
            throws SQLException {

        Connection con = pc.getConnection();
        Statement stmt = con.createStatement();
        String n_a     = null; // auth level not used for this test

        JDBC.assertCurrentSchema(con, user.toUpperCase());

        // Change the role.
        stmt.execute("set role bar");
        ResultSet rs = stmt.executeQuery("values current_role");
        assertRoleInRs(rs, "\"BAR\"", n_a);
        rs.close();
        stmt.close();

        // Close the logical connection and get a new one and make sure the
        // current role has been reset.
        con.close();
        con = pc.getConnection();
        stmt = con.createStatement();
        rs = stmt.executeQuery("values current_role");
        assertRoleInRs(rs, null, n_a);
        rs.close();
        stmt.close();
        con.close();
    }


    private void checkGrantCircularity() {
        if (isDbo()) {
            // Test circularity in role grant relation given this a
            // priori graph:
            //
            //          s8
            //           |   s1<---s3
            //           | / |
            //           V/  |
            //          s4   |
            //         / |\  |
            //        /  V \ V
            //       s5  s6  s2
            //           |
            //           s7

            String NA = null; // we only run this as dbo

            for(int i=1; i <= 8; i++) {
                doStmt("create role s" + i, NA, null, NA);
            }

            // This establishes the role grant graph shown above. None
            // of these grants should fail.
            doStmt("grant s1 to s2", NA, null, NA);
            doStmt("grant s3 to s1", NA, null, NA);
            doStmt("grant s1 to s4", NA, null, NA);
            doStmt("grant s4 to s2", NA, null, NA);
            doStmt("grant s4 to s6", NA, null, NA);
            doStmt("grant s4 to s5", NA, null, NA);
            doStmt("grant s6 to s7", NA, null, NA);
            doStmt("grant s8 to s4", NA, null, NA);

            // These statements all represent illegal grants in that
            // they would cause a circularity, so we expect all to
            // throw.
            doStmt("grant s1 to s1", NA, roleGrantCircularity, NA);
            doStmt("grant s2 to s3", NA, roleGrantCircularity, NA);
            doStmt("grant s2 to s8", NA, roleGrantCircularity, NA);
            doStmt("grant s7 to s1", NA, roleGrantCircularity, NA);
            doStmt("grant s7 to s4", NA, roleGrantCircularity, NA);
            doStmt("grant s7 to s6", NA, roleGrantCircularity, NA);
            doStmt("grant s7 to s3", NA, roleGrantCircularity, NA);
            doStmt("grant s2 to s1", NA, roleGrantCircularity, NA);
            doStmt("grant s2 to s8", NA, roleGrantCircularity, NA);
            doStmt("grant s2 to s4", NA, roleGrantCircularity, NA);
            doStmt("grant s6 to s1", NA, roleGrantCircularity, NA);
            doStmt("grant s6 to s8", NA, roleGrantCircularity, NA);
            doStmt("grant s6 to s3", NA, roleGrantCircularity, NA);
            doStmt("grant s6 to s4", NA, roleGrantCircularity, NA);
            doStmt("grant s5 to s1", NA, roleGrantCircularity, NA);
            doStmt("grant s5 to s3", NA, roleGrantCircularity, NA);
            doStmt("grant s5 to s4", NA, roleGrantCircularity, NA);
            doStmt("grant s5 to s8", NA, roleGrantCircularity, NA);

            for(int i=1; i <= 8; i++) {
                doStmt("drop role s" + i, NA, null, NA);
            }
        }
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

        if (_authLevel == SQLAUTHORIZATION && isDbo()) {
            // create a table grant to an (uknown) user WHOEVER.
            // This is used to test that create role detects the
            // presence of existing user ids before allowing a
            // role creation with that id.
            _stm.executeUpdate("create table t1(i int)");
            _stm.executeUpdate("grant select on t1 to whoever");

            // create a schema for (uknown) user SCHEMAOWNER.
            // This is used to test that create role detects the
            // presence of existing user ids before allowing a
            // role creation with that id.
            _stm.executeUpdate(
                "create schema lingerSchema authorization schemaowner");
        }

        _stm.close();
    }


    protected void tearDown() throws Exception
    {
        if (_authLevel == SQLAUTHORIZATION &&  isDbo()) {
            _stm = createStatement();

            try {
                _stm.executeUpdate("revoke select on t1 from whoever");
                _stm.executeUpdate("drop table t1");
                _stm.executeUpdate("drop schema lingerSchema restrict");
            } catch (SQLException se) {
                System.err.println("Test error + " + se);
            }
        }

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
                        fail("exception " + authDboState[0] + " expected: (" +
                             stmt);
                    }
                    if (authDboState[1] != null) {
                        SQLWarning w = _stm.getWarnings();
                        assertNotNull("Expected warning but found none", w);
                        assertSQLState(authDboState[1], w);
                    }
                } else {
                    if (authNotDboState[0] != null) {
                        fail("exception " + authNotDboState[0] +
                             " expected: (" + stmt);
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
                         e.getSQLState(), e);
                } else {
                    assertSQLState("Stmt " + stmt, noAuthState[0], e);
                }

            } else { // SQLAUTHORIZATION
                if (isDbo()) {
                    if (authDboState[0] == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState(), e);
                    } else {
                        assertSQLState("Stmt " + stmt, authDboState[0], e);
                    }
                } else {
                    if (authNotDboState[0] == null) {
                        fail("stmt " + stmt + " failed with exception " +
                             e.getSQLState(), e);
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
                 fail("prepare of set role ? failed:" + e, e);
             }
        }

        try {
            pstmt.setString(1, "BAR");
            int rowcnt = pstmt.executeUpdate();
            assertEquals("rowcount from set role ? not 0", rowcnt, 0);
        } catch (SQLException e) {
            fail("execute of set role ? failed: [foo]" + e, e);
        }


        try {
            pstmt.setString(1, "");
            int rowcnt = pstmt.executeUpdate();
            fail("Expected syntax error on identifier");
        } catch (SQLException e) {
            assertSQLState(idParseError ,e);
        }

        try {
            pstmt.setString(1, null);
            int rowcnt = pstmt.executeUpdate();
            fail("Expected syntax error on identifier");
        } catch (SQLException e) {
            assertSQLState(idParseError ,e);
        }


        if (isDbo()) {
            // not granted to non-dbo, so don't try..
            String n_a     = null; // auth level not used for this test

            try {
                pstmt.setString(1, "NONE");
                int rowcnt = pstmt.executeUpdate();
                assertEquals("rowcount from set role ? not 0", rowcnt, 0);
                ResultSet rs = doQuery("values current_role", n_a, null , n_a );
                assertRoleInRs(rs, "\"NONE\"", n_a);
                rs.close();
            } catch (SQLException e) {
                fail("execute of set role ? failed: [NONE] " + e, e);
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
     * "authorization stack", see section 4.34.1.1 and
     * 4.27.3. Initially tested here, but now moved to
     * lang/SQLSessionContextTest.
     */



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
            println("uuid=" + rs.getString(1) +
                    " r=" + rs.getString(2) + " -ee:" + rs.getString(3) +
                    " -or:" + rs.getString(4) + " a:" + rs.getString(5) +
                    " d:" + rs.getString(6));
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
            if (isDbo()) {
                JDBC.assertSingleValueResultSet(rs, dboRole);
            } else {
                JDBC.assertSingleValueResultSet(rs, notDboRole);
            }
        }
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
}

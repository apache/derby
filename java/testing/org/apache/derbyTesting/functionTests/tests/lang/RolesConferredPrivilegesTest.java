/*

  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RolesConferredPrivilegesTest

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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.ArrayList;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * This tests that SQL roles actually confer the correct privileges, that is,
 * when privileges are granted to one or more roles and those roles are granted
 * to other roles, to one or more users or to PUBLIC, sessions can make use of
 * them correctly.
 */
public class RolesConferredPrivilegesTest extends BaseJDBCTestCase
{
    private final static String pwSuffix = "pwSuffix";

    /* SQL states */
    private final static String NOEXECUTEPERMISSION  = "42504";
    private final static String NOTABLEPERMISSION    = "42500";
    private final static String NOCOLUMNPERMISSION   = "42502";
    private final static String TABLENOTFOUND        = "42X05";
    private final static String OBJECTNOTFOUND       = "42X94";
    private final static String FKVIOLATION          = "23503";
    private final static String CHECKCONSTRAINTVIOLATED = "23513";
    private final static String ALREADYCLOSED        = "XJ012";
    private final static String CONSTRAINTDROPPED    = "01500";
    private final static String VIEWDROPPED          = "01501";
    private final static String TRIGGERDROPPED       = "01502";
    private final static String UNRELIABLE           = "42Y39";

    private final static String[] users = {"test_dbo", "DonaldDuck", "MickeyMouse"};

    /**
     * Create a new instance of RolesConferredPrivilegesTest.
     *
     * @param name Fixture name
     */
    public RolesConferredPrivilegesTest(String name)
    {
        super(name);
    }


    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("RolesConferredPrivilegesTest");

        suite.addTest(makeSuite());

        // suite.addTest(
        //     TestConfiguration.clientServerDecorator(makeSuite()));

        return suite;
    }

    /**
     * Construct suite of tests
     *
     * @return A suite containing the test cases.
     */
    private static Test makeSuite()
    {
        /* Tests running with sql authorization set.  First decorate
         * with clean database, then with authentication +
         * sqlAuthorization.
         */
        Test clean = new CleanDatabaseTestSetup(
            new TestSuite(RolesConferredPrivilegesTest.class)) {
                protected void decorateSQL(Statement s)
                        throws SQLException {
                    /*
                     *          a1            a2         a3
                     *         / | \           |          |
                     *        /  b  +--------> c          d
                     *       j   |              \        /
                     *           e---+           \      /
                     *            \   \           \    /
                     *             \   \---------+ \  /
                     *              \             \_ f
                     *               \             /
                     *                \           /
                     *                 \         /
                     *                  \       /
                     *                   \     /
                     *                    \   /
                     *                      h
                     */
                    s.execute("create role a1");
                    s.execute("create role j");
                    s.execute("create role b");
                    s.execute("create role e");
                    s.execute("create role h");
                    s.execute("create role a2");
                    s.execute("create role c");
                    s.execute("create role f");
                    s.execute("create role a3");
                    s.execute("create role d");

                    s.execute("grant a1 to j");
                    s.execute("grant a1 TO b");
                    s.execute("grant b TO e");
                    s.execute("grant e TO h");
                    s.execute("grant a1 TO c");
                    s.execute("grant e TO f");
                    s.execute("grant a2 TO c");
                    s.execute("grant c TO f");
                    s.execute("grant f TO h");
                    s.execute("grant a3 TO d");
                    s.execute("grant d TO f");

                    s.execute("create schema s1");
                    s.execute
                        ("create function s1.f1( ) returns int " +
                         "language java parameter style java external name " +
                         "'org.apache.derbyTesting.functionTests.tests.lang." +
                         "RolesConferredPrivilegesTest.s1f1' " +
                         "no sql called on null input");
                    s.execute
                        ("create function s1.f2( ) returns int " +
                         "language java parameter style java external name " +
                         "'org.apache.derbyTesting.functionTests.tests.lang." +
                         "RolesConferredPrivilegesTest.s1f1' " +
                         "no sql called on null input");
                    s.execute
                        ("create table s1.t1(" +
                         "c1 int unique, c2 int unique, c3 int unique, " +
                         "primary key (c1,c2,c3))");
                    // We made columns all unique so we can test references
                    // privilege for all columns.
                    s.execute(
                        "create procedure s1.calledNested()" +
                        "  language java parameter style java" +
                        "  external name " +
                        "'org.apache.derbyTesting.functionTests.tests.lang." +
                        "RolesConferredPrivilegesTest.calledNested' " +
                        "  modifies sql data");
                    s.execute
                        ("create function s1.getCurrentRole() " +
                         "returns varchar(30)" +
                         "language java parameter style java external name " +
                         "'org.apache.derbyTesting.functionTests.tests.lang." +
                         "RolesConferredPrivilegesTest.getCurrentRole' " +
                         " reads sql data");
                }
            };

        return
            TestConfiguration.sqlAuthorizationDecorator(
                DatabasePropertyTestSetup.singleProperty(
                    DatabasePropertyTestSetup.builtinAuthentication(
                        clean, users, pwSuffix),
                    // Increase default statementCacheSize since we compile a
                    // lot:
                    "derby.language.statementCacheSize", "1000"));
    }

    private final static int GRANT = 0;
    private final static int REVOKE = 1;

    // Action type for assert methods.
    private final static int NOPRIV = 0;
    private final static int VIAUSER = 1;
    private final static int VIAROLE = 2;

    // The "guts" of GRANT/REVOKE privilege strings. Prepend GRANT/REVOKE and
    // append TO/FROM authorizationId [RESTRICT] as the case may be.
    private final static String g_r    = "references on s1.t1 ";
    private final static String g_r_c1 = "references (c1) on s1.t1 ";
    private final static String g_r_c2 = "references (c2) on s1.t1 ";
    private final static String g_r_c3 = "references (c3) on s1.t1 ";
    private final static String g_u    = "update on s1.t1 ";
    private final static String g_u_c1 = "update (c1) on s1.t1 ";
    private final static String g_u_c2 = "update (c2) on s1.t1 ";
    private final static String g_u_c3 = "update (c3) on s1.t1 ";
    private final static String g_u_c1_c2_c3 = "update (c1,c2,c3) on s1.t1 ";
    private final static String g_i    = "insert on s1.t1 ";
    private final static String g_s    = "select on s1.t1 ";
    private final static String g_s_c1 = "select (c1) on s1.t1 ";
    private final static String g_s_c2 = "select (c2) on s1.t1 ";
    private final static String g_s_c3 = "select (c3) on s1.t1 ";
    private final static String g_d    = "delete on s1.t1 ";
    private final static String g_t    = "trigger on s1.t1 ";
    private final static String g_e    = "execute on function s1.f1 ";
    private final static String g_e_f2 = "execute on function s1.f2 ";

    // Collections of privileges
    private final static String[] g_all =
        new String[] {g_r, g_r_c1, g_r_c2, g_r_c3,
                      g_u, g_u_c1, g_u_c2, g_u_c3,
                      g_i,
                      g_s, g_s_c1, g_s_c2, g_s_c3,
                      g_d,
                      g_t,
                      g_e};
    // column level (for privilege types applicable)
    private final static String[] g_all_col =
        new String[] {g_r_c1, g_r_c2, g_r_c3,
                      g_u_c1, g_u_c2, g_u_c3,
                      g_i,
                      g_s_c1, g_s_c2, g_s_c3,
                      g_d,
                      g_t,
                      g_e};
    // table level
    private final static String[] g_all_tab =
        new String[] {g_r, g_u, g_i, g_s, g_d, g_t, g_e};

    private final static String[][]grantRevokes =
        new String[][] {g_all, g_all_col, g_all_tab};

    /**
     * Basic test that checks that privileges granted to a role are applicable
     * when the user sets the current role to that role (or to a role that
     * inherits that role).
     *
     * @throws SQLException
     */
    public void atestConferredPrivileges() throws SQLException
    {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        // try with role granted to both a user and to public
        String[] grantees = new String[] {"DonaldDuck", "public"};

        Connection c = openUserConnection("DonaldDuck");

        for (int gNo = 0; gNo < grantees.length; gNo++ ) {
            /*
             * Grant a role to a session's user and test that privileges apply
             * when granted to any applicable role.
             */
            s.executeUpdate("grant h to " + grantees[gNo]);

            String[] applicableFor_h =
                new String[] {"h", "a1", "a2", "a3", "b", "e", "c", "f", "d"};
            String[] notApplicableFor_h =
                new String[] {"j"};
            setRole(c, "h");

            // Test that all privileges apply when granted to the current role
            // or to an inherited role of the current role, cf graph above.
            for (int i=0; i < applicableFor_h.length; i++) {
                assertAllforRole(VIAROLE, c, applicableFor_h[i]);
            }

            // Test that no privileges apply when granted to a role NOT
            // inherited by the current role.
            for (int i=0; i < notApplicableFor_h.length; i++) {
                assertAllforRole(NOPRIV, c, notApplicableFor_h[i]);
            }

            /*
             * Test that no privileges apply when we set role to none.
             */
            setRole(c, "none");

            for (int i=0; i < applicableFor_h.length; i++) {
                assertAllforRole(NOPRIV, c, applicableFor_h[i]);
            }

            /*
             * Test that when one link in the graph "cycle" is broken,
             * privileges still apply.
             */
            s.executeUpdate("grant f to " + grantees[gNo]);
            setRole(c, "f");
            assertAllforRole(VIAROLE, c, "a1");
            // one arc gone
            s.executeUpdate("revoke a1 from c");
            assertAllforRole(VIAROLE, c, "a1");
            // both arcs gone
            s.executeUpdate("revoke a1 from b");
            assertAllforRole(NOPRIV, c, "a1");
            // resurrect other arc
            s.executeUpdate("grant a1 to b");
            assertAllforRole(VIAROLE, c, "a1");
            // restore things
            s.executeUpdate("grant a1 to c");
            s.executeUpdate("revoke f from " + grantees[gNo]);

            /*
             * Revoke the role from the current session's user and verify that
             * privileges no longer apply when granted to any heretofore
             * applicable role.
             */
            setRole(c, "h");

            s.executeUpdate("revoke h from " + grantees[gNo]);

            for (int i=0; i < applicableFor_h.length; i++) {
                assertAllforRole(NOPRIV, c, applicableFor_h[i]);
            }

            /*
             * Test when role is dropped when still a current role
             */
            s.executeUpdate("grant h to " + grantees[gNo]);
            setRole(c, "h");

            // Test that all privileges apply when granted to the current role
            // or to an inherited role of the current role, cf graph above.
            for (int i=0; i < applicableFor_h.length; i++) {
                assertAllforRole(VIAROLE, c, applicableFor_h[i]);
            }

            s.executeUpdate("drop role h");

            for (int i=0; i < applicableFor_h.length; i++) {
                assertAllforRole(NOPRIV, c, applicableFor_h[i]);
            }

            // restore the dropped role
            s.executeUpdate("create role h");
            s.executeUpdate("grant e to h");
            s.executeUpdate("grant f to h");
        }

        c.close();
        s.close();
        dboConn.close();
    }


    /**
     * When a view, a trigger or a constraint requires a privilege by way of
     * the current role (or by way of a role inherited by the current role) at
     * creation time (SELECT, TRIGGER or REFERENCES privilege respectively), a
     * dependency is also registered against the current role. Whenever that
     * role (it need no longer be current) - or indeed one of its inherited
     * roles - is revoked (from the current user or from a role in the closure
     * of the original current role) or dropped, the dependent view, trigger or
     * constraint is potentially invalidated (the single dependency is against
     * the original current role, not against the potentially n-ary set of
     * roles in the closure of the current role used to find the required
     * privileges). Due to DERBY-1632, currently the objects are dropped
     * instead of being potentially revalidated.
     *
     * These tests check that invalidation actually happens and leads to a
     * dropping of the dependent view (there are also no revalidation
     * possibilities in play here, so even when DERBY-1632 is fixed these tests
     * should work).
     */
    public void testViewInvalidation() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        SQLWarning w;
        /*
        * 3-dimensional search space:
        *
        * Which role we grant the role to (direct to a role or to a role it
        * inherits)
        *    X
        * Whether the role is granted directly to the session user or to PUBLIC.
        *    X
        * Whether we grant the entire underlying table or just the column
        * needed.
        */
        String[] grantToThisRole = new String[] {"a2", "h"};
        String[] roleGrantees = new String[] {"DonaldDuck", "public"};
        String[] tabAndColSelectsPerms = new String[] {g_s, g_s_c1};
        String createViewString = "create view v as select c1 from s1.t1";

        for (int r = 0; r < grantToThisRole.length; r++) {
            for (int gNo = 0; gNo < roleGrantees.length; gNo++ ) {
                for (int i = 0; i < tabAndColSelectsPerms.length; i++) {
                    /*
                     * Create a view on the basis of a select privilege via a
                     * role.
                     */
                    s.executeUpdate("grant h to " + roleGrantees[gNo]);

                    doGrantRevoke(GRANT, "test_dbo", tabAndColSelectsPerms[i],
                                  grantToThisRole[r]);

                    setRole(c, "h");
                    cStmt.executeUpdate(createViewString);

                    assertViewExists(true, c, "v");

                    /*
                     * Setting another role does not affect the view once
                     * defined.
                     */
                    setRole(c, "none");
                    assertViewExists(true, c, "v");

                    /*
                     * Remove privileges from role, and the view should be
                     * gone.
                     */
                     doGrantRevoke(REVOKE, "test_dbo", tabAndColSelectsPerms[i],
                                   grantToThisRole[r], VIEWDROPPED);
                    assertViewExists(false, c, "v");

                    /*
                     * Revoking the role should also invalidate view
                     */
                    doGrantRevoke(GRANT, "test_dbo", tabAndColSelectsPerms[i],
                                  grantToThisRole[r]);

                    setRole(c, "h");
                    cStmt.executeUpdate(createViewString);
                    assertViewExists(true, c, "v");

                    s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                    w = s.getWarnings();
                    assertSQLState(VIEWDROPPED, w);
                    assertViewExists(false, c, "v");

                    /*
                     * Check that user privilege and/or PUBLIC privilege is
                     * preferred over role privilege if available. This is not
                     * standard SQL, but useful behavior IMHO as long as Derby
                     * can't revalidate via another path (DERBY-1632) - lest a
                     * role revoke or drop causes an invalidation when user has
                     * discretionary privilege. Cf. also comment on priority of
                     * user vs public in DERBY-1611.
                     */
                    String[] directGrantee = roleGrantees;

                    for (int u = 0; u < directGrantee.length; u++) {
                        s.executeUpdate("grant h to " + roleGrantees[gNo]);
                        doGrantRevoke(GRANT, "test_dbo",
                                      tabAndColSelectsPerms[i],
                                      directGrantee[u]);

                        setRole(c, "h");

                        // Now we have select privilege two ways, via role and
                        // via user.
                        cStmt.executeUpdate(createViewString);

                        // Now revoke role priv and see that view is still
                        // unaffected.
                        s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                        assertViewExists(true, c, "v");

                        // Take away user privilege, too.
                        doGrantRevoke(REVOKE, "test_dbo",
                                      tabAndColSelectsPerms[i],
                                      directGrantee[u],
                                      VIEWDROPPED);
                        assertViewExists(false, c, "v");
                    }

                    // clean up
                    doGrantRevoke(REVOKE, "test_dbo",tabAndColSelectsPerms[i],
                                  grantToThisRole[r]);
                }
            }
        }

        /*
         * Dropping a role should also invalidate a dependent view.
         *
         * (We do this test outside the loop above for simplicity of
         * reestablish role graph after the drop..)
         */

        // drop the current role
        doGrantRevoke(GRANT, "test_dbo", g_s, "h");
        s.executeUpdate("grant h to DonaldDuck");

        setRole(c, "h");
        cStmt.executeUpdate(createViewString);
        assertViewExists(true, c, "v");

        s.executeUpdate("drop role h");

        w = s.getWarnings();
        assertSQLState(VIEWDROPPED, w);
        assertViewExists(false, c, "v");

        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");

        // re-establish role graph
        s.executeUpdate("create role h");
        s.executeUpdate("grant e to h");
        s.executeUpdate("grant f to h");

        // drop an inherited role needed
        doGrantRevoke(GRANT, "test_dbo", g_s, "a3");
        s.executeUpdate("grant h to DonaldDuck");

        setRole(c, "h");
        cStmt.executeUpdate(createViewString);
        assertViewExists(true, c, "v");

        s.executeUpdate("drop role a3");

        w = s.getWarnings();
        assertSQLState(VIEWDROPPED, w);
        assertViewExists(false, c, "v");

        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");

        // re-establish role graph
        s.executeUpdate("create role a3");
        s.executeUpdate("grant a3 to d");

        cStmt.close();
        c.close();
        s.close();
        dboConn.close();
    }


    /**
     * @see #testViewInvalidation
     */
    public void testTriggerInvalidation() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        SQLWarning w;

        /*
        * 2-dimensional search space:
        *
        * Which role we grant the role to (direct to a role or to a role it
        * inherits)
        *    X
        * Whether the role is granted directly to the session user or to PUBLIC.
        */
        String[] grantToThisRole = new String[] {"a2", "h"};
        String[] roleGrantees = new String[] {"DonaldDuck", "public"};
        String createTriggerString =
            "create trigger t after insert on s1.t1 values 1";

        for (int r = 0; r < grantToThisRole.length; r++) {
            for (int gNo = 0; gNo < roleGrantees.length; gNo++ ) {
                /*
                 * Create a trigger on the basis of a trigger privilege via a
                 * role.
                 */
                s.executeUpdate("grant h to " + roleGrantees[gNo]);

                doGrantRevoke(GRANT, "test_dbo", g_t, grantToThisRole[r]);

                setRole(c, "h");
                cStmt.executeUpdate(createTriggerString);

                assertTriggerExists(true, c, "t");
                cStmt.executeUpdate(createTriggerString);
                /*
                 * Setting another role does not affect the trigger once
                 * defined.
                 */
                setRole(c, "none");
                assertTriggerExists(true, c, "t");

                setRole(c, "h");
                cStmt.executeUpdate(createTriggerString);

                // Remove privileges from role, and the trigger should be
                // gone.
                doGrantRevoke(REVOKE, "test_dbo", g_t, grantToThisRole[r],
                              TRIGGERDROPPED);

                assertTriggerExists(false, c, "t");

                /*
                 * Revoking the role should also invalidate trigger
                 */
                doGrantRevoke(GRANT, "test_dbo", g_t, grantToThisRole[r]);

                setRole(c, "h");
                cStmt.executeUpdate(createTriggerString);
                assertTriggerExists(true, c, "t");
                cStmt.executeUpdate(createTriggerString);

                s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                w = s.getWarnings();
                assertSQLState(TRIGGERDROPPED, w);
                assertTriggerExists(false, c, "t");

                /*
                 * Check that user privilege and/or PUBLIC privilege is
                 * preferred over role privilege if available. This is not
                 * standard SQL, but useful behavior IMHO as long as Derby
                 * can't revalidate via another path (DERBY-1632) - lest a
                 * role revoke or drop causes an invalidation when user has
                 * discretionary privilege. Cf. also comment on priority of
                 * user vs public in DERBY-1611.
                 */
                String[] directGrantee = roleGrantees;

                for (int u = 0; u < directGrantee.length; u++) {
                    s.executeUpdate("grant h to " + roleGrantees[gNo]);
                    doGrantRevoke(GRANT, "test_dbo", g_t, directGrantee[u]);

                    setRole(c, "h");

                    // Now we have trigger privilege two ways,a via role and
                    // via user.
                    cStmt.executeUpdate(createTriggerString);

                    // Now revoke role priv and see that trigger is still
                    // unaffected.
                    s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                    assertTriggerExists(true, c, "t");
                    cStmt.executeUpdate(createTriggerString);

                    // take away user privilege, too
                    doGrantRevoke(REVOKE, "test_dbo",g_t,directGrantee[u],
                                  TRIGGERDROPPED);
                    assertTriggerExists(false, c, "t");
                }

                // clean up
                doGrantRevoke(REVOKE, "test_dbo", g_t, grantToThisRole[r]);
            }
        }

        /*
         * Dropping a role should also invalidate a dependent trigger.
         *
         * (We do this test outside the loop above for simplicity of
         * reestablish role graph after the drop..)
         */
        doGrantRevoke(GRANT, "test_dbo", g_t, "h");
        s.executeUpdate("grant h to DonaldDuck");

        setRole(c, "h");
        cStmt.executeUpdate(createTriggerString);
        assertTriggerExists(true, c, "t");
        cStmt.executeUpdate(createTriggerString);

        s.executeUpdate("drop role h");
        w = s.getWarnings();
        assertSQLState(TRIGGERDROPPED, w);
        assertTriggerExists(false, c, "t");

        doGrantRevoke(REVOKE, "test_dbo", g_t, "h");

        // re-establish role graph
        s.executeUpdate("create role h");
        s.executeUpdate("grant e to h");
        s.executeUpdate("grant f to h");

        /*
         * Dropping an EXECUTE privilege used in a trigger body will not drop
         * the trigger if the EXECUTE privilege is revoked from a user
         * directly, since this currently requires the RESTRICT
         * keyword. However, revoking a role does not carry the RESTRICT
         * keyword, so any execution privilege conferred through a role is
         * revoked, too, and any dependent object, for example a trigger in
         * example below, will be dropped.
         */
        doGrantRevoke(GRANT, "test_dbo", g_t, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e, "h");
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");

        cStmt.executeUpdate
            ("create trigger t after insert on s1.t1 values s1.f1()");
        assertTriggerExists(true, c, "t");

        cStmt.executeUpdate
            ("create trigger t after insert on s1.t1 values s1.f1()");

        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(TRIGGERDROPPED, w);
        assertTriggerExists(false, c, "t");

        doGrantRevoke(REVOKE, "test_dbo", g_t, "h");
        doGrantRevoke(REVOKE, "test_dbo", g_e, "h");

        /*
         * Check that dependency on role and subsequent invalidation happens
         * for a mix of column SELECT privileges granted to user, public and
         * role (due to tricky logic in this implementation,
         * cf. DDLConstantAction#storeViewTriggerDependenciesOnPrivileges
         */

        // SELECT privileges to {public, role} x
        // TRIGGER privilege to {user, role}
        String triggerPrivGrantees[] = new String[] {"h", "DonaldDuck"};
        for (int i=0; i < triggerPrivGrantees.length; i++) {
            s.executeUpdate("grant h to DonaldDuck");
            setRole(c, "h");
            doGrantRevoke(GRANT, "test_dbo", g_t, triggerPrivGrantees[i]);
            doGrantRevoke(GRANT, "test_dbo", g_s_c1, "public");
            doGrantRevoke(GRANT, "test_dbo", g_s_c2, "h");
            cStmt.executeUpdate
                ("create trigger t after insert on s1.t1 " +
                 "select c1,c2 from s1.t1");
            s.executeUpdate("revoke h from DonaldDuck");
            w = s.getWarnings();
            assertSQLState(TRIGGERDROPPED, w);
            assertTriggerExists(false, c, "t");
            doGrantRevoke(REVOKE, "test_dbo", g_t, triggerPrivGrantees[i]);
            doGrantRevoke(REVOKE, "test_dbo", g_s_c1, "public");
            doGrantRevoke(REVOKE, "test_dbo", g_s_c2, "h");
        }

        // SELECT privileges to {user, role} x
        // TRIGGER privilege to {user, role}
        for (int i=0; i < triggerPrivGrantees.length; i++) {
            s.executeUpdate("grant h to DonaldDuck");
            setRole(c, "h");
            doGrantRevoke(GRANT, "test_dbo", g_t, triggerPrivGrantees[i]);
            doGrantRevoke(GRANT, "test_dbo", g_s_c1, "DonaldDuck");
            doGrantRevoke(GRANT, "test_dbo", g_s_c2, "h");
            cStmt.executeUpdate
                ("create trigger t after insert on s1.t1 " +
                 "select c1,c2 from s1.t1");
            s.executeUpdate("revoke h from DonaldDuck");
            w = s.getWarnings();
            assertSQLState(TRIGGERDROPPED, w);
            assertTriggerExists(false, c, "t");
            doGrantRevoke(REVOKE, "test_dbo", g_t,  triggerPrivGrantees[i]);
            doGrantRevoke(REVOKE, "test_dbo", g_s_c1, "DonaldDuck");
            doGrantRevoke(REVOKE, "test_dbo", g_s_c2, "h");
        }

        // SELECT privileges to {user, public, role} x
        // TRIGGER privilege to {user, role}
        for (int i=0; i < triggerPrivGrantees.length; i++) {
            s.executeUpdate("grant h to DonaldDuck");
            setRole(c, "h");
            doGrantRevoke(GRANT, "test_dbo", g_t, triggerPrivGrantees[i]);
            doGrantRevoke(GRANT, "test_dbo", g_s_c1, "DonaldDuck");
            doGrantRevoke(GRANT, "test_dbo", g_s_c2, "public");
            doGrantRevoke(GRANT, "test_dbo", g_s_c3, "h");
            cStmt.executeUpdate
                ("create trigger t after insert on s1.t1 " +
                 "select c1,c2,c3 from s1.t1");
            s.executeUpdate("revoke h from DonaldDuck");
            w = s.getWarnings();
            assertSQLState(TRIGGERDROPPED, w);
            assertTriggerExists(false, c, "t");
            doGrantRevoke(REVOKE, "test_dbo", g_t, triggerPrivGrantees[i]);
            doGrantRevoke(REVOKE, "test_dbo", g_s_c1, "DonaldDuck");
            doGrantRevoke(REVOKE, "test_dbo", g_s_c2, "public");
            doGrantRevoke(REVOKE, "test_dbo", g_s_c3, "h");
        }

        cStmt.close();
        c.close();
        s.close();
        dboConn.close();
    }


    /**
     * @see #testViewInvalidation
     */
    public void testConstraintInvalidation() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        SQLWarning w;
        /*
        * 3-dimensional search space:
        *
        * Which role we grant the role to (direct to a role or to a role it
        * inherits)
        *    X
        * Whether the role is granted directly to the session user or to PUBLIC.
        *    X
        * Whether we grant the entire underlying table or just the column
        * needed.
        */
        String[] grantToThisRole = new String[] {"a2", "h"};
        String[] roleGrantees = new String[] {"DonaldDuck", "public"};
        String[][] tabAndColReferencesPerms =
            new String[][] {{g_r}, {g_r_c1, g_r_c2, g_r_c3}};

        String createTableString =
            "create table t (i int not null, j int, k int)";
        String dropTableString = "drop table t";
        String addConstraintString =
            "alter table t add constraint fk " +
            "foreign key(i,j,k) references s1.t1";

        cStmt.executeUpdate(createTableString);

        for (int r = 0; r < grantToThisRole.length; r++) {
            for (int gNo = 0; gNo < roleGrantees.length; gNo++ ) {
                for (int i = 0; i < tabAndColReferencesPerms.length; i++) {
                    /*
                     * Create a foreign key constraint on the basis of a
                     * references privilege via a role.
                     */
                    s.executeUpdate("grant h to " + roleGrantees[gNo]);

                    doGrantRevoke(GRANT, "test_dbo",
                                  tabAndColReferencesPerms[i],
                                  grantToThisRole[r]);

                    setRole(c, "h");
                    cStmt.executeUpdate(addConstraintString);

                    assertFkConstraintExists(true, c, "t");

                    /*
                     * Setting another role does not affect the constraint once
                     * defined.
                     */
                    setRole(c, "none");
                    assertFkConstraintExists(true, c, "t");

                    // Remove privileges from role, and the constraint should be
                    // gone.
                    doGrantRevoke
                        (REVOKE, "test_dbo",
                         tabAndColReferencesPerms[i],
                         grantToThisRole[r],
                         new String[]{CONSTRAINTDROPPED, null, null});

                    assertFkConstraintExists(false, c, "t");

                    /*
                     * Revoking the role should also invalidate constraint
                     */
                    doGrantRevoke(GRANT, "test_dbo",
                                  tabAndColReferencesPerms[i],
                                  grantToThisRole[r]);

                    setRole(c, "h");
                    cStmt.executeUpdate(addConstraintString);
                    assertFkConstraintExists(true, c, "t");

                    s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                    assertFkConstraintExists(false, c, "t");

                    /*
                     * Check that user privilege and/or PUBLIC privilege is
                     * preferred over role privilege if available. This is not
                     * standard SQL, but useful behavior IMHO as long as Derby
                     * can't revalidate via another path (DERBY-1632) - lest a
                     * role revoke or drop causes an invalidation when user has
                     * discretionary privilege. Cf. also comment on priority of
                     * user vs public in DERBY-1611.
                     */
                    String[] directGrantee = roleGrantees;

                    for (int u = 0; u < directGrantee.length; u++) {
                        s.executeUpdate("grant h to " + roleGrantees[gNo]);
                        doGrantRevoke(GRANT, "test_dbo",
                                      tabAndColReferencesPerms[i],
                                      directGrantee[u]);

                        setRole(c, "h");

                        // Now we have references privilege two ways, via role
                        // and via user.
                        cStmt.executeUpdate(addConstraintString);

                        // Now revoke role priv and see that constraints is
                        // still unaffected.
                        s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                        assertFkConstraintExists(true, c, "t");

                        // take away user privilege, too
                        doGrantRevoke
                            (REVOKE, "test_dbo",
                             tabAndColReferencesPerms[i],
                             directGrantee[u],
                             new String[]{CONSTRAINTDROPPED, null, null});
                        assertFkConstraintExists(false, c, "t");
                    }

                    // clean up
                    doGrantRevoke
                        (REVOKE, "test_dbo",
                         tabAndColReferencesPerms[i],
                         grantToThisRole[r]);
                }
            }
        }

        /*
         * Dropping a role should also invalidate a dependent constraint.
         *
         * (We do this test outside the loop above for simplicity of
         * reestablish role graph after the drop..)
         */
        doGrantRevoke(GRANT, "test_dbo", g_r, "h");
        s.executeUpdate("grant h to DonaldDuck");

        setRole(c, "h");
        cStmt.executeUpdate(addConstraintString);
        assertFkConstraintExists(true, c, "t");

        s.executeUpdate("drop role h");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertFkConstraintExists(false, c, "t");


        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");


        // re-establish role graph
        s.executeUpdate("create role h");
        s.executeUpdate("grant e to h");
        s.executeUpdate("grant f to h");

        /*
         * For FOREIGN KEY constraint, check that dependency on role and
         * subesquent invalidation happens for a mix of column privileges
         * granted to user, public and role (due to tricky logic in this
         * implementation,
         * cf. DDLConstantAction#storeConstraintDependenciesOnPrivileges
         */
        // {role, role}
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo",
                      new String[] {g_r_c1, g_r_c2, g_r_c3}, "h");
        cStmt.executeUpdate
            ("alter table t add constraint fk foreign key(i,j,k) " +
             "references s1.t1");

        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertFkConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo",
                      new String[] {g_r_c1, g_r_c2, g_r_c3}, "h");

        // {public, role}
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_r_c1, "public");
        doGrantRevoke(GRANT, "test_dbo", g_r_c2, "h");
        doGrantRevoke(GRANT, "test_dbo", g_r_c3, "h");
        cStmt.executeUpdate("alter table t add constraint fk " +
                            "foreign key(i,j,k) references s1.t1");

        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertFkConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c1, "public");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c2, "h");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c3, "h");

        // {user, role}
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_r_c1, "DonaldDuck");
        doGrantRevoke(GRANT, "test_dbo", g_r_c2, "h");
        doGrantRevoke(GRANT, "test_dbo", g_r_c3, "h");
        cStmt.executeUpdate("alter table t add constraint fk " +
                            "foreign key(i,j,k) references s1.t1");
        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertFkConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c1, "DonaldDuck");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c2, "h");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c3, "h");

        // {user, public, role}
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_r_c1, "DonaldDuck");
        doGrantRevoke(GRANT, "test_dbo", g_r_c2, "public");
        doGrantRevoke(GRANT, "test_dbo", g_r_c3, "h");
        cStmt.executeUpdate("alter table t add constraint fk " +
                            "foreign key(i,j,k) references s1.t1");
        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertFkConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c1, "DonaldDuck");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c2, "public");
        doGrantRevoke(REVOKE, "test_dbo", g_r_c3, "h");

        // Try the same as above but with EXECUTE privilege instead of
        // REFERENCES for a CHECK constraint
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e, "h");
        cStmt.executeUpdate("alter table t add constraint ch " +
                            "check(i < s1.f1())");
        assertCheckConstraintExists(true, c, "t");
        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertCheckConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo", g_e, "h");

        // Try the same as above but with two EXECUTE privileges
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e_f2, "DonaldDuck");
        cStmt.executeUpdate("alter table t add constraint ch " +
                            "check(i < (s1.f1() + s1.f2()))");
        assertCheckConstraintExists(true, c, "t");
        s.executeUpdate("revoke h from DonaldDuck");
        w = s.getWarnings();
        assertSQLState(CONSTRAINTDROPPED, w);
        assertCheckConstraintExists(false, c, "t");
        doGrantRevoke(REVOKE, "test_dbo", g_e, "h");
        doGrantRevoke(REVOKE, "test_dbo", g_e_f2, "DonaldDuck");

        // Try the same as above but with multiple CHECK constraints to verify
        // that only those affected by a revoke are impacted.
        s.executeUpdate("grant h to DonaldDuck");
        setRole(c, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e, "h");
        doGrantRevoke(GRANT, "test_dbo", g_e_f2, "DonaldDuck");
        cStmt.executeUpdate
            ("create table tmp(i int constraint ct1 check(i < s1.f1())," +
             "                 j int constraint ct2 check(j < s1.f2()))");
        s.executeUpdate("revoke h from DonaldDuck");

        // This should only impact ct1
        try {
            cStmt.executeUpdate("insert into tmp values (6, -1)");
        } catch (SQLException e) {
            fail("expected success", e);
        }

        try {
            cStmt.executeUpdate("insert into tmp values (6, 6)");
            fail("ct2 should remain");
        } catch (SQLException e) {
            assertSQLState(CHECKCONSTRAINTVIOLATED, e);
        }

        cStmt.executeUpdate("alter table tmp drop constraint ct2");
        doGrantRevoke(REVOKE, "test_dbo", g_e, "h");
        doGrantRevoke(REVOKE, "test_dbo", g_e_f2, "DonaldDuck");
        cStmt.executeUpdate("drop table tmp");

        cStmt.executeUpdate(dropTableString);

        cStmt.close();
        c.close();
        s.close();
        dboConn.close();
    }

    /**
     * DERBY-4191
     * There are times when no column is selected from a table in the from
     * list. At such a time, we should make sure that we make sure there
     * is atleast some kind of select privilege available on that table for
     * the query to succeed. eg of such queries
     * select count(*) from t1
     * select count(1) from t1
     * select 1 from t1
     * select t1.c1 from t1, t2
     * 
     * In addition, the subquery inside of a NON-select query should require
     * select privilege on the tables involved in the subquery eg
     * update dbo.t set a = ( select max(a1) + 2 from dbo.t1 )
     * update dbo.t set a = ( select max(b1) + 2 from dbo.t2 )
     * For both the queries above, in addition to update privilege requirement
     * on dbo.t(a), we need to require select privileges on columns/tables
     * within the select list. So for first query, the user should have select
     * privilege on dbo.t1 or dbo.t1(a1). Similarly, for 2nd query, the user
     * should have select privilege on dbo.t2 or dbo.t2(b1) 
     * @throws SQLException
     */
    public void testMinimumSelectPrivilege() throws SQLException {
        Connection dboConn = getConnection();
        Statement stmtDBO = dboConn.createStatement();

        Connection cDD = openUserConnection("DonaldDuck");
        Statement stmtDD = cDD.createStatement();

        Connection cMM = openUserConnection("MickeyMouse");
        Statement stmtMM = cMM.createStatement();

        stmtDBO.executeUpdate("create role role1");
        stmtDBO.executeUpdate("grant role1 to MickeyMouse");

        stmtDD.executeUpdate("create table DDtable1(c11 int, c12 int)");
        stmtDD.executeUpdate("insert into DDtable1 values(1, 2)");
        stmtDD.executeUpdate("create table DDtable2(c21 int, c22 int)");
        stmtDD.executeUpdate("insert into DDtable2 values(3, 4)");
        
        stmtMM.executeUpdate("set role role1");
        try {
        	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }
        try {
        	stmtMM.executeUpdate("update DonaldDuck.DDtable1 set c11 = " +
        			" (select c21 from DonaldDuck.DDtable2)");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }

        stmtDD.executeUpdate("grant select(c12) on DDtable1 to role1");
        stmtDD.executeUpdate("grant update on DDtable1 to role1");
    	stmtMM.executeQuery("select c12 from DonaldDuck.DDtable1");
        try {
        	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }
        try {
        	stmtMM.executeUpdate("update DonaldDuck.DDtable1 set c11 = " +
        			" (select c21 from DonaldDuck.DDtable2)");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }

        stmtDD.executeUpdate("grant select(c11) on DDtable1 to role1");
    	stmtMM.executeQuery("select c12 from DonaldDuck.DDtable1");
    	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1");
        try {
        	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1, " +
        			"DonaldDuck.DDtable2");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42500", e);
        }
        try {
        	stmtMM.executeQuery("update DonaldDuck.DDtable1 set c11 = " +
        			" (select c21 from DonaldDuck.DDtable2)");
        	fail("select should have failed");
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }

        stmtDD.executeUpdate("grant select(c21) on DDtable2 to role1");
    	stmtMM.executeQuery("select c12 from DonaldDuck.DDtable1");
    	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1");
    	stmtMM.executeQuery("select c11 from DonaldDuck.DDtable1, " +
    			"DonaldDuck.DDtable2");
    	stmtMM.executeUpdate("update DonaldDuck.DDtable1 set c11 = " +
    			" (select c21 from DonaldDuck.DDtable2)");
    }

    /**
     * Test that a prepared statement can no longer execute after its required
     * privileges acquired via the current role are no longer applicable.
     */
    public void testPSInvalidation() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();

        /*
        * 3-dimensional search space:
        *
        * Which role we grant the role to (direct to a role or to a role it
        * inherits)
        *    X
        * Whether the role is granted directly to the session user or to PUBLIC.
        *    X
        * Whether we grant the entire underlying table or just the column
        * needed.
        */
        String[] grantToThisRole = new String[] {"a2", "h"};
        String[] roleGrantees = new String[] {"DonaldDuck", "public"};
        String[][] privilegeStmts =
            new String[][] {{g_s, "select c1 from s1.t1"},
                            {g_s_c1, "select c1 from s1.t1"},
                            {g_e, "values s1.f1()"},
                            {g_u, "update s1.t1 set c1=0"},
                            {g_u_c1_c2_c3, "update s1.t1 set c1=0"},
                            {g_i, "insert into s1.t1 values (5,5,5)"}};

        PreparedStatement ps = null;

        for (int r = 0; r < grantToThisRole.length; r++) {
            for (int gNo = 0; gNo < roleGrantees.length; gNo++ ) {
                for (int i = 0; i < privilegeStmts.length; i++) {
                    /*
                     * Create a ps on the basis of a select privilege via a
                     * role.
                     */
                    s.executeUpdate("grant h to " + roleGrantees[gNo]);

                    doGrantRevoke(GRANT, "test_dbo", privilegeStmts[i][0],
                                  grantToThisRole[r]);

                    setRole(c, "h");
                    ps = c.prepareStatement(privilegeStmts[i][1]);

                    assertPsWorks(true, ps);

                    /*
                     * Setting another role should make the ps fail, since we
                     * no longer have the privilege.
                     */
                    setRole(c, "none");
                    assertPsWorks(false, ps);
                    // set it back:
                    setRole(c, "h");
                    assertPsWorks(true, ps);

                    /*
                     * Remove privileges from role, and the execute should
                     * fail.
                     */
                    doGrantRevoke(REVOKE, "test_dbo", privilegeStmts[i][0],
                                  grantToThisRole[r]);

                    assertPsWorks(false, ps);
                    doGrantRevoke(GRANT, "test_dbo", privilegeStmts[i][0],
                                  grantToThisRole[r]);

                    /*
                     * Revoking the role should also make the ps fail, since we
                     * no longer have the privilege.
                     */
                    setRole(c, "h");
                    assertPsWorks(true, ps);

                    s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                    assertPsWorks(false, ps);

                    /*
                     * Check that prepared statements are reprepared if there
                     * is another applicable privilege, when the privilege
                     * granted via a role is used first and that role is
                     * revoked.
                     */
                    String[] directGrantee = roleGrantees;

                    // iterate over granting role h to {user, PUBLIC}
                    for (int u = 0; u < directGrantee.length; u++) {
                        s.executeUpdate("grant h to " + roleGrantees[gNo]);

                        setRole(c, "h");
                        assertPsWorks(true, ps);

                        doGrantRevoke(GRANT, "test_dbo",
                                      privilegeStmts[i][0],
                                      directGrantee[u]);

                        // Now we have select privilege two ways, via role and
                        // via user or PUBLIC.
                        // Now revoke role priv and see that ps is still
                        // unaffected.
                        s.executeUpdate("revoke h from " + roleGrantees[gNo]);
                        assertPsWorks(true, ps);

                        // Take away user privilege, too.
                        doGrantRevoke(REVOKE, "test_dbo",
                                      privilegeStmts[i][0],
                                      directGrantee[u]);
                        assertPsWorks(false, ps);
                    }

                    // clean up
                    doGrantRevoke(REVOKE, "test_dbo",privilegeStmts[i][0],
                                  grantToThisRole[r]);
                }
            }
        }

        /*
         * Dropping a role should also cause a dependent ps fail.
         *
         * (We do this test outside the loop above for simplicity of
         * reestablish role graph after the drop..)
         *
         */
        for (int i=0; i < privilegeStmts.length; i++) {
            doGrantRevoke(GRANT, "test_dbo", privilegeStmts[i][0], "h");
            s.executeUpdate("grant h to DonaldDuck");

            setRole(c, "h");
            ps = c.prepareStatement(privilegeStmts[i][1]);
            assertPsWorks(true, ps);

            s.executeUpdate("drop role h");
            assertPsWorks(false, ps);

            doGrantRevoke(REVOKE, "test_dbo", privilegeStmts[i][0], "h");

            // re-establish role graph
            s.executeUpdate("create role h");
            s.executeUpdate("grant e to h");
            s.executeUpdate("grant f to h");
        }

        cStmt.close();
        c.close();
        s.close();
        dboConn.close();
    }


    /**
     * Test behavior for when there are open result sets on prepared statements
     * that require privileges obtained via the current role and something
     * changes in the middle of accessing the result set. We should be able to
     * finish using the result set.
     */
    public void testOpenRs() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        ResultSet rs = null;
        String select = "select * from s1.t1";

        PreparedStatement ps = dboConn.prepareStatement(
            "insert into s1.t1 values (?,?,?)");
        for (int i=0; i < 5; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3, i);
            ps.execute();
        }


        /*
         * Select privilege revoked
         */
        // Auto-commit on
        doGrantRevoke(GRANT, "test_dbo", g_s, "h");
        s.execute("grant h to DonaldDuck");
        setRole(c, "h");
        rs = cStmt.executeQuery(select);

        rs.next();
        // Now remove privilege in middle of rs reading
        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");

        // check that we can read the next row
        rs.next();
        rs.close();

        // Auto-commit off
        c.setAutoCommit(false);
        doGrantRevoke(GRANT, "test_dbo", g_s, "h");
        setRole(c, "h");
        rs = cStmt.executeQuery(select);

        rs.next();
        c.commit();
        // Now remove privilege in middle of rs reading
        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");

        // check that we can read the next row
        rs.next();
        rs.close();
        c.setAutoCommit(true);

        /*
         * Role privilege revoked
         */
        // Auto-commit on
        doGrantRevoke(GRANT, "test_dbo", g_s, "h");
        s.execute("grant h to DonaldDuck");
        setRole(c, "h");
        rs = cStmt.executeQuery(select);

        rs.next();
        // Now remove privilege in middle of rs reading
        s.execute("revoke h from DonaldDuck");

        // check that we can read the next row
        rs.next();
        rs.close();

        // Auto-commit off
        c.setAutoCommit(false);
        s.execute("grant h to DonaldDuck");
        setRole(c, "h");
        rs = cStmt.executeQuery(select);

        rs.next();
        c.commit();
        // Now remove privilege in middle of rs reading
        s.execute("revoke h from DonaldDuck");

        // check that we can read the next row
        rs.next();
        rs.close();
        c.setAutoCommit(true);
        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");


        /*
         * Current role changed
         */
        // Auto-commit on
        doGrantRevoke(GRANT, "test_dbo", g_s, "h");
        s.execute("grant h to DonaldDuck");
        setRole(c, "h");
        c.setAutoCommit(true);
        rs = cStmt.executeQuery(select);

        rs.next();
        // Now change role in middle of rs reading
        setRole(c, "none");

        // check that we can read the next row
        rs.next();
        rs.close();

        // Auto-commit off
        c.setAutoCommit(false);
        setRole(c, "h");
        rs = cStmt.executeQuery(select);

        rs.next();
        // Now remove privilege in middle of rs reading
        c.commit();
        setRole(c, "none");

        // check that we can read the next row
        rs.next();
        rs.close();
        c.setAutoCommit(true);
        doGrantRevoke(REVOKE, "test_dbo", g_s, "h");

        // clean up
        s.executeUpdate("delete from s1.t1");
        c.close();
        dboConn.close();
    }


    /**
     * Test that DEFAULT CURRENT_ROLE works as expected
     * See DERBY-3897.
     */
    public void testDefaultCurrentRole() throws SQLException {
        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();
        s.execute("grant h to DonaldDuck");

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        setRole(c, "h");

        // CREATE TABLE
        cStmt.executeUpdate
            ("create table t(role varchar(128) default current_role)");
        cStmt.executeUpdate("insert into t values default");
        ResultSet rs = cStmt.executeQuery("select * from t");
        JDBC.assertSingleValueResultSet(rs, "\"H\"");
        rs.close();
        cStmt.executeUpdate("drop table t");

        // ALTER TABLE
        cStmt.executeUpdate("create table t(i int)");
        cStmt.executeUpdate("insert into t values 1");
        cStmt.executeUpdate
            ("alter table t " +
             "add column role varchar(10) default current_role");
        rs = cStmt.executeQuery("select * from t");
        JDBC.assertFullResultSet(rs, new String[][]{{"1",  "\"H\""}});
        rs.close();
        cStmt.executeUpdate("drop table t");

        // do the same from within a stored procedure

        s.execute("grant execute on procedure s1.calledNested to DonaldDuck");

        if (!JDBC.vmSupportsJSR169()) {
            // JSR169 cannot run with tests with stored procedures
            // that do database access - for they require a
            // DriverManager connection to jdbc:default:connection;
            // DriverManager is not supported with JSR169.
            cStmt.executeUpdate("call s1.calledNested()");
        }

        setRole(c, "none");

        cStmt.close();
        s.execute("revoke h from DonaldDuck");
        s.execute("revoke execute on procedure s1.calledNested " +
                  "from DonaldDuck restrict");
        s.close();

        c.close();
        dboConn.close();
    }


    /**
     * Test that CURRENT_ROLE works as expected in some miscellaneous contexts.
     * See DERBY-3897.
     */
    public void testCurrentRoleInWeirdContexts() throws SQLException {
        if (JDBC.vmSupportsJSR169()) {
            // JSR169 cannot run with tests with stored procedures
            // that do database access - for they require a
            // DriverManager connection to jdbc:default:connection;
            // DriverManager is not supported with JSR169.
            return;
        }

        Connection dboConn = getConnection();
        Statement s = dboConn.createStatement();
        setRole(dboConn, "a1");
        s.execute("create table trackCreds(usr varchar(30), role varchar(30))");
        s.executeUpdate("create table t(i int)");
        s.execute("grant insert on t to DonaldDuck");
        s.execute("grant h to DonaldDuck");

        // From within a trigger body:
        s.execute("create trigger tr after insert on t " +
                  "insert into trackCreds values (current_user, current_role)");

        Connection c = openUserConnection("DonaldDuck");
        Statement cStmt = c.createStatement();
        setRole(c, "h");
        cStmt.executeUpdate("insert into test_dbo.t values 1");

        ResultSet rs = s.executeQuery("select * from trackCreds");
        JDBC.assertFullResultSet(rs, new String[][]{{"DONALDDUCK",  "\"H\""}});
        rs.close();
        setRole(c, "none");
        cStmt.close();

        // From within a CHECK constraint, that we get an error
        try {
            s.execute("create table strange(role varchar(30) " +
                      "check (role = current_role))");
            fail("current_role inside a check constraint should be denied");
        } catch (SQLException e) {
            assertSQLState(UNRELIABLE, e);
        }

        // From within a function, called from a CHECK constraint
        // executed as a substatement as part of ALTER TABLE.
        // In this case, the session context stack contains two elements
        // referenced from the three activations thus:
        //
        // top level "alter table" act.          -> top level session context
        // substatement (check constraint act.)  -> top level session context
        // nested connnection in getCurrentRole,
        //            "values current_role" act. -> pushed session context
        //
        // Before DERBY-3897 the call to s1.getCurrentRole would yield null
        // because the pushed session context for getCurrentRole would inherit
        // a wrong (newly created) session context from the CHECK constraint
        // substatement's activation. After DERBY-3897, the substatement
        // correctly inherits the session context of "alter table"s
        // activation, so the pushed session context for getCurrentRole will
        // be correct, too.
        //
        s.execute("create table strange(i int)");
        s.execute("insert into strange values null");
        s.execute("alter table strange " +
                  "add constraint s check (s1.getCurrentRole() = '\"A1\"')");

        s.execute("revoke h from DonaldDuck");
        s.execute("revoke insert on t from DonaldDuck");
        setRole(dboConn, "none");
        s.execute("drop table t");
        s.execute("drop table strange");
        s.execute("drop table trackCreds");
        s.close();

        c.close();
        dboConn.close();
    }



    /**
     * stored function: s1.f1
     */
    public static int s1f1() {
        return 0;
    }


    private void assertAllforRole(int hasPrivilege,
                                  Connection c,
                                  String grantee) throws SQLException {
        for (int i=0; i < grantRevokes.length; i++) {
            doGrantRevoke(GRANT, "test_dbo", grantRevokes[i], grantee);
            assertEverything(hasPrivilege, c, null);
            doGrantRevoke(REVOKE, "test_dbo", grantRevokes[i], grantee);

            // check that when priv is revoked we no longer have it
            if (hasPrivilege == VIAROLE) {
                assertEverything(NOPRIV, c, null);
            }

        }
    }


    private void assertEverything(int hasPrivilege,
                                  String user,
                                  String role) throws SQLException {

        Connection c = openUserConnection(user);
        assertEverything(hasPrivilege, c, role);
        c.close();
    }


    private void assertEverything(int hasPrivilege,
                                  Connection c,
                                  String role) throws SQLException {
        if (role != null) {
            setRole(c, role);
        }

        String[] columns = new String[] {"c1", "c2"};
        String schema = "s1";
        String table = "t1";
        String function = "f1";

        assertSelectPrivilege
            (hasPrivilege, c, schema, table, columns);
        assertSelectPrivilege
            (hasPrivilege, c, schema, table, null);
        assertInsertPrivilege
            (hasPrivilege, c, schema, table, null);
        assertUpdatePrivilege
            (hasPrivilege, c, schema, table, columns);
        assertUpdatePrivilege
            (hasPrivilege, c, schema, table, null);
        assertDeletePrivilege
            (hasPrivilege, c, schema, table);
        assertReferencesPrivilege
            (hasPrivilege, c, schema, table, columns);
        assertReferencesPrivilege
            (hasPrivilege, c, schema, table, null);
        assertTriggerPrivilege
            (hasPrivilege, c, schema, table);
        assertExecutePrivilege(hasPrivilege, c, schema, function);
    }


    /**
     * Assert that a user has execute privilege on a given function
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param function the name of the function to check
     * @throws SQLException throws all exceptions
     */
    private void assertExecutePrivilege(int hasPrivilege,
                                        String user,
                                        String role,
                                        String schema,
                                        String function) throws SQLException {
        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertExecutePrivilege(hasPrivilege, c, schema, function);
        c.close();
    }


    /**
     * Assert that a user has execute privilege on a given function
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param function the name of the function to check
     * @throws SQLException throws all exceptions
     */
    private void assertExecutePrivilege(int hasPrivilege,
                                        Connection c,
                                        String schema,
                                        String function) throws SQLException {
        Statement stm = c.createStatement();

        try {
            ResultSet rs =
                stm.executeQuery("values " + schema + "." + function + "()");

            rs.next();
            rs.close();
            stm.close();

            if (hasPrivilege == NOPRIV) {
                fail("expected no EXECUTE privilege on function. " +
                     formatArgs(c, schema, function));
            }
        } catch (SQLException e) {
            if (stm != null) {
                stm.close();
            }

            if (hasPrivilege == NOPRIV)
                assertSQLState(NOEXECUTEPERMISSION, e);
            else {
                fail("Unexpected lack of execute privilege. " +
                     formatArgs(c, schema, function), e);
            }
        }
    }


    /**
     * Assert that a user has trigger privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @throws SQLException throws all exceptions
     */
    private void assertTriggerPrivilege(int hasPrivilege,
                                        String user,
                                        String role,
                                        String schema,
                                        String table) throws SQLException {
        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertTriggerPrivilege(hasPrivilege, c, schema, table);
        c.close();
    }


    /**
     * Assert that a user has trigger execute privilege on a given table /
     * column set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @throws SQLException throws all exceptions
     */
    private void assertTriggerPrivilege(int hasPrivilege,
                                        Connection c,
                                        String schema,
                                        String table) throws SQLException {
        Statement s = c.createStatement();
        String triggerName = table + "Trigger";

        try {
            int i = s.executeUpdate
                ("create trigger " + triggerName + " after insert on " +
                 schema + "." + table + " for each row values 1");

            if (hasPrivilege != NOPRIV) {
                assertEquals(0, i);
            }

            s.execute("drop trigger " + triggerName);

            if (hasPrivilege == NOPRIV) {
                fail("expected no TRIGGER privilege on table. " +
                     formatArgs(c, schema, table));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(NOTABLEPERMISSION, e);
            } else {
                fail("Unexpected lack of trigger privilege. " +
                     formatArgs(c, schema, table), e);
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "TRIGGER", schema, table, null);

    }


    /**
     * Assert that a user has references privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @param columns the name of the columns to check, or null
     * @throws SQLException throws all exceptions
     */
    private void assertReferencesPrivilege(int hasPrivilege,
                                           String user,
                                           String role,
                                           String schema,
                                           String table,
                                           String[] columns)
            throws SQLException {

        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertReferencesPrivilege(hasPrivilege, c, schema, table, columns);
        c.close();
    }


    /**
     * Assert that a user has references privilege on a given table / column
     * set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    private void assertReferencesPrivilege(int hasPrivilege,
                                           Connection c,
                                           String schema,
                                           String table,
                                           String[] columns)
            throws SQLException {

        Statement s = c.createStatement();

        columns = ((columns == null)
                   ? getAllColumns(schema, table)
                   : columns);

        for (int i = 0; i < columns.length; i++) {
            try {
                s.execute("create table referencestest (c1 int" +
                          " references " + schema + "." +
                          table + "(" + columns[i] + "))" );

                s.execute("drop table referencestest");

                if (hasPrivilege == NOPRIV) {
                    fail("Unexpected references privilege. " +
                         formatArgs(c, schema, table,
                                    new String[]{columns[i]}));
                }
            } catch (SQLException e) {
                if (hasPrivilege == NOPRIV) {
                    assertSQLState(NOCOLUMNPERMISSION, e);
                } else {
                    fail("Unexpected lack of references privilege. " +
                         formatArgs(c, schema, table,
                                    new String[]{columns[i]}),
                         e);
                }
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "REFERENCES", schema, table, columns);
    }


    /**
     * Assert that a user has update privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @param columns the name of the columns to check, or null
     * @throws SQLException throws all exceptions
     */
    private void assertUpdatePrivilege(int hasPrivilege,
                                       String user,
                                       String role,
                                       String schema,
                                       String table,
                                       String[] columns)
            throws SQLException {

        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertUpdatePrivilege(hasPrivilege, c, schema, table, columns);
        c.close();
    }


    /**
     * Assert that a user has update privilege on a given table / column set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    private void assertUpdatePrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String[] columns)
            throws SQLException {

        String[] checkColumns =
            (columns == null) ? getAllColumns(schema, table) : columns;

        Statement s = c.createStatement();
        int columnCount = 0;
        boolean checkCount;

        for (int i = 0; i < checkColumns.length; i++) {
            checkCount = false;

            try {
                // Try to get count of rows to verify update rows. We may not
                // have select privilege on the column, in which case, we
                // simply don't verify the count.
                try {
                    ResultSet countRS =
                        s.executeQuery("select count(" + checkColumns[i] +
                                       ") from " + schema + "." + table);

                    if (!countRS.next()) {
                        fail("Could not get count on " + checkColumns[i] +
                             " to verify update");
                    }

                    columnCount = countRS.getInt(1);
                    checkCount = true;
                } catch (SQLException e) {
                    assertSQLState(NOCOLUMNPERMISSION, e);
                }

                int actualCount =
                    s.executeUpdate("update " + schema + "." + table +
                                    " set " + checkColumns[i] + "= 0");

                if (hasPrivilege != NOPRIV && checkCount) {
                    // update count should equal select count
                    assertEquals(columnCount, actualCount);
                }

                if (hasPrivilege == NOPRIV) {
                    fail("expected no UPDATE privilege on  " +
                         formatArgs(c, schema, table,
                                    new String[]{checkColumns[i]}));
                }
            } catch (SQLException e) {
                if (hasPrivilege == NOPRIV) {
                    assertSQLState(NOCOLUMNPERMISSION, e);
                } else {
                    fail("Unexpected lack of privilege to update. " +
                         formatArgs(c, schema, table,
                                    new String[]{checkColumns[i]}));
                }
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "UPDATE", schema, table, columns);
    }


    /**
     * Assert that a user has insert privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @param columns the name of the columns to check, or null
     * @throws SQLException throws all exceptions
     */
    private void assertInsertPrivilege(int hasPrivilege,
                                       String user,
                                       String role,
                                       String schema,
                                       String table,
                                       String[] columns)
            throws SQLException {

        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertInsertPrivilege(hasPrivilege, c, schema, table, columns);
        c.close();
    }


    /**
     * Assert that a user has insert privilege on a given table / column set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    private void assertInsertPrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String[] columns)
            throws SQLException {

        Statement s = c.createStatement();

        try {
            int i = s.executeUpdate("insert into " + schema + "." +
                                    table + " values (0,0,0)");
            if (hasPrivilege == NOPRIV) {
                fail("expected no INSERT privilege on table, " +
                     formatArgs(c, schema, table, columns));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(NOTABLEPERMISSION, e);
            } else {
                fail("Unexpected lack of insert privilege. " +
                     formatArgs(c, schema, table, columns), e);
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "INSERT", schema, table, columns);
    }


    /**
     * Assert that a user has select privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @param columns the name of the columns to check, or null
     * @throws SQLException throws all exceptions
     */
    private void assertSelectPrivilege(int hasPrivilege,
                                       String user,
                                       String role,
                                       String schema,
                                       String table,
                                       String[] columns)
            throws SQLException {

        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertSelectPrivilege(hasPrivilege, c, schema, table, columns);
        c.close();
    }


    /**
     * Assert that a user has select privilege on a given table / column set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    private void assertSelectPrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String[] columns) throws SQLException {
      assertSelectPrivilege(hasPrivilege, c, schema, 
        		table, columns, NOCOLUMNPERMISSION);
      assertSelectConstantPrivilege(hasPrivilege, c, schema, 
        		table, NOTABLEPERMISSION);
      assertSelectCountPrivilege(hasPrivilege, c, schema, 
        		table, columns, NOTABLEPERMISSION);
    }

    /**
     * Assert that a user has select privilege at the table(s) level  or 
     * atleast on one column from each of the tables involved in the 
     * query when running a select query which selects count(*) or
     * count(constant) from the tables.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns used for error handling if ran into exception
     * @param sqlState expected state if hasPrivilege == NOPRIV
     * @throws SQLException throws all exceptions
     */
    private void assertSelectCountPrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String[] columns,
                                       String sqlState) throws SQLException {
        Statement s = c.createStatement();

        try {
            s.execute("select count(*) from " + schema + "." + table);

            if (hasPrivilege == NOPRIV) {
                fail("expected no SELECT privilege on table " +
                     formatArgs(c, schema, table, columns));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(sqlState, e);
            } else {
                fail("Unexpected lack of select privilege. " +
                     formatArgs(c, schema, table, columns), e);
            }
        }

        try {
            s.execute("select count('a') from " + schema + "." + table);

            if (hasPrivilege == NOPRIV) {
                fail("expected no SELECT privilege on table " +
                     formatArgs(c, schema, table, columns));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(sqlState, e);
            } else {
                fail("Unexpected lack of select privilege. " +
                     formatArgs(c, schema, table, columns), e);
            }
        }

        s.close();
    }

    /**
     * Assert that a user has select privilege at the table(s) level  or 
     * atleast on one column from each of the tables involved in the 
     * query when running a select query which only selects constants from 
     * the tables.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param sqlState expected state if hasPrivilege == NOPRIV
     * @throws SQLException throws all exceptions
     */
    private void assertSelectConstantPrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String sqlState) throws SQLException {
        Statement s = c.createStatement();

        try {
            s.execute("select 1 from " + schema + "." + table);

            if (hasPrivilege == NOPRIV) {
                fail("expected no SELECT privilege on table " +
                     formatArgs(c, schema, table));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(sqlState, e);
            } else {
                fail("Unexpected lack of select privilege. " +
                     formatArgs(c, schema, table), e);
            }
        }

        s.close();
    }


    /**
     * Assert that a user has select privilege on a given table / column set.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @param sqlState expected state if hasPrivilege == NOPRIV
     * @throws SQLException throws all exceptions
     */
    private void assertSelectPrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table,
                                       String[] columns,
                                       String sqlState) throws SQLException {
        Statement s = c.createStatement();

        try {
            s.execute("select " + columnListAsString(columns) +
                      " from " + schema + "." + table);

            if (hasPrivilege == NOPRIV) {
                fail("expected no SELECT privilege on table " +
                     formatArgs(c, schema, table, columns));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(sqlState, e);
            } else {
                fail("Unexpected lack of select privilege. " +
                     formatArgs(c, schema, table, columns), e);
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "SELECT", schema, table, columns);
    }


    /**
     * Check that a given view exists (select privilege assumed) or not by
     * selecting from it. The connection user must supposed to be the owner for
     * this to work.
     */
    private void assertViewExists(boolean exists,
                                  Connection c,
                                  String table) throws SQLException {
        Statement s = c.createStatement();
        try {
            s.execute("select * from " + table);
            if (!exists) {
                fail("Table expected not to exist: " + table);
            }
         } catch (SQLException e) {
             if (exists) {
                 fail("Table expected to exist: " + table, e);
             }
             assertSQLState(TABLENOTFOUND, e);
         }
         s.close();
    }


    /**
     * Check that a given trigger exists (select privilege assumed) or not.
     * NOTE: It is destructive, since the test is by dropping the trigger.  The
     * connection user must supposed to be the owner for this to work.
     */
    private void assertTriggerExists(boolean exists,
                                     Connection c,
                                     String trigger) throws SQLException {
        Statement s = c.createStatement();
        try {
            s.execute("drop trigger " + trigger);
            if (!exists) {
                fail("Trigger expected not to exist: " + trigger);
            }
         } catch (SQLException e) {
             if (exists) {
                 fail("Trigger expected to exist: " + trigger, e);
             }
             assertSQLState(OBJECTNOTFOUND, e);
         }
         s.close();
    }


    /**
     * Check that a given foregin key constraint exists by the following
     * method: We insert a value that is not present in the referenced table so
     * the foreign key constraint will fail if the constraint is present. The
     * connection user must be the owner for this to work.
     */
    private void assertFkConstraintExists(boolean exists,
                                        Connection c,
                                        String table)
            throws SQLException {
        assertConstraintExists(exists, c, table, FKVIOLATION);
    }

    /**
     * Check that a given check constraint exists by the following method: We
     * insert a value that does not satify the check constraint. The connection
     * user must be the owner for this to work.
     */
    private void assertCheckConstraintExists(boolean exists,
                                        Connection c,
                                        String table)
            throws SQLException {
        assertConstraintExists(exists, c, table, CHECKCONSTRAINTVIOLATED);
    }


    private void assertConstraintExists(boolean exists,
                                        Connection c,
                                        String table,
                                        String sqlState)
            throws SQLException {

        Statement s = c.createStatement();
        try {
            s.execute("insert into " + table + " values (6,6,6)");
            s.execute("delete from " + table);

            if (exists) {
                fail("Table expected to have a constraint: " + table);
            }
         } catch (SQLException e) {
             if (!exists) {
                 fail("Table expected not to have a constraint: " + table, e);
             }
             assertSQLState(sqlState, e);
         }
         s.close();
    }


    /**
     * Check that a given prepared statement can be executed.
     */
    private void assertPsWorks(boolean works,
                               PreparedStatement ps) throws SQLException {

        ps.getConnection().setAutoCommit(false);

        try {
            boolean b = ps.execute();
            ResultSet rs = ps.getResultSet();
            if (rs != null) {
                rs.next();
                rs.close();
            }
            ps.getConnection().rollback();
            ps.getConnection().setAutoCommit(true);

            if (!works) {
                fail("Prepared statement expected to fail.");
            }
         } catch (SQLException e) {
            ps.getConnection().setAutoCommit(true);

            if (works) {
                 fail("Prepared statement expected to work.", e);
             }
             assertSQLState
                 (new String[]{NOCOLUMNPERMISSION,
                               NOEXECUTEPERMISSION,
                               NOTABLEPERMISSION},
                  e);
         }
    }


    /**
     * Assert that a user has delete privilege on a given table
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param role to use, or null if we do not want to set the role
     * @param schema the schema to check
     * @param table the name of the table to check
     * @throws SQLException throws all exceptions
     */
    private void assertDeletePrivilege(int hasPrivilege,
                                       String user,
                                       String role,
                                       String schema,
                                       String table)
            throws SQLException {

        Connection c = openUserConnection(user);

        if (role != null) {
            setRole(c, role);
        }

        assertDeletePrivilege(hasPrivilege, c, schema, table);
        c.close();
    }


    /**
     * Assert that a user has delete privilege on a given table.
     *
     * @param hasPrivilege whether or not the user has the privilege
     * @param c connection to use
     * @param schema the schema to check
     * @param table the table to check
     * @throws SQLException throws all exceptions
     */
    private void assertDeletePrivilege(int hasPrivilege,
                                       Connection c,
                                       String schema,
                                       String table) throws SQLException {

        Statement s = c.createStatement();

        try {
            s.execute("delete from " + schema + "." + table);

            if (hasPrivilege == NOPRIV) {
                fail("expected no DELETE privilege on table " +
                     formatArgs(c, schema, table));
            }
        } catch (SQLException e) {
            if (hasPrivilege == NOPRIV) {
                assertSQLState(NOTABLEPERMISSION, e);
            } else {
                fail("Unexpected lack of delete privilege. " +
                     formatArgs(c, schema, table), e);
            }
        }

        s.close();

        assertPrivilegeMetadata
            (hasPrivilege, c, "DELETE", schema, table, null);
    }

    /**
     * Assert that a specific privilege exists by checking the
     * database metadata available to a user.
     *

     * @param hasPrivilege Is != NOPRIV if we expect the caller to have the
     *                      privilege
     * @param c user connection
     * @param type type of privilege, e.g. SELECT, INSERT, DELETE, etc.
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check, or all columns if null
     * @throws SQLException
     */
    private void assertPrivilegeMetadata(int hasPrivilege,
                                         Connection c,
                                         String type,
                                         String schema,
                                         String table,
                                         String[] columns) throws SQLException {
        ResultSet rs;
        Statement stm = c.createStatement();
        rs = stm.executeQuery("values current_user");
        rs.next();
        String user = rs.getString(1);
        rs.close();
        stm.close();

        if (isOwner(schema, user)) {
            //  NOTE: Does not work for table owner, who has no manifest entry
            //  corresponding to the privilege in SYSTABLEPERMS.
            return;
        }

        if (hasPrivilege == VIAROLE) {
            // No DatabaseMetaData for roles. We could of course check
            // SYS.SYSROLES and the privilege tables but then we would have to
            // essentially rebuild the whole role privilege computation
            // machinery in this test.. ;)
            return;
        }

        DatabaseMetaData dm = c.getMetaData();

        rs = dm.getTablePrivileges
            (null, JDBC.identifierToCNF(schema), JDBC.identifierToCNF(table));

        boolean found = false;

        // check getTablePrivileges
        if (columns == null) {
            while (rs.next())
                {
                    // Also verify that grantor and is_grantable can be
                    // obtained Derby doesn't currently support the for grant
                    // option, the grantor is always the object owner - in this
                    // test, test_dbo, and is_grantable is always 'NO'.
                    assertEquals(JDBC.identifierToCNF("test_dbo"),
                                 rs.getString(4));
                    assertEquals("NO", rs.getString(7));

                    if (rs.getString(6).equals(type)) {
                        String privUser = rs.getString(5);

                        if (privUser.equals(user) ||
                                privUser.equals(
                                    JDBC.identifierToCNF("public"))) {
                            found = true;
                        }
                    }
                }
            assertEquals(hasPrivilege == VIAUSER, found);
            rs.close();
        }

        // check getColumnPrivileges()
        ResultSet cp = null;
        if (columns == null) {
            /*
             * Derby does not record table level privileges in SYSCOLPERMS, so
             * the following does not work. If it is ever changed so that
             * getColumnPrivileges returns proper results for table level
             * privileges, this(*) can be reenabled.
             *
             * (*) See GrantRevokeTest.
             */
        } else {
            // or, check that all given columns have privilege or not as the
            // case may be
            int noFound = 0;

            for (int i = 0; i < columns.length; i++) {
                cp = dm.getColumnPrivileges(null,
                                            JDBC.identifierToCNF(schema),
                                            JDBC.identifierToCNF(table),
                                            JDBC.identifierToCNF(columns[i]));

                while (cp.next()) {
                    // also verify that grantor and is_grantable are valid
                    // Derby doesn't currently support for grant, so
                    // grantor is always the object owner - in this test,
                    // test_dbo, and getColumnPrivileges casts 'NO' for
                    // is_grantable for supported column-related privileges
                    assertEquals(JDBC.identifierToCNF("test_dbo"),
                                 cp.getString(5));
                    assertEquals("NO", cp.getString(8));

                    if (cp.getString(7).equals(type)) {
                        String privUser = cp.getString(6);

                        if (privUser.equals(user) ||
                                privUser.equals(
                                    JDBC.identifierToCNF("public"))) {
                            noFound++;
                        }
                    }
                }
            }

            if (hasPrivilege == VIAUSER) {
                assertEquals(columns.length, noFound);
            } else {
                assertEquals(0, noFound);
            }
        }

        if (cp != null) {
            cp.close();
        }
    }

    private boolean isOwner(String schema, String user) throws SQLException {
        Connection c = getConnection();
        Statement stm = c.createStatement();
        ResultSet rs = stm.executeQuery
            ("select schemaname, authorizationid from sys.sysschemas " +
             "where schemaname='" + JDBC.identifierToCNF(schema) + "'");
        rs.next();
        boolean result = rs.getString(2).equals(JDBC.identifierToCNF(user));
        rs.close();
        stm.close();
        return result;
    }


    /**
     * Get all the columns in a given schema / table
     *
     * @return an array of Strings with the column names
     * @throws SQLException
     */
    private String[] getAllColumns(String schema, String table)
            throws SQLException
    {
        Connection c = getConnection();
        DatabaseMetaData dbmd = c.getMetaData();
        ArrayList<String> columnList = new ArrayList<String>();
        ResultSet rs =
            dbmd.getColumns( (String) null, schema, table, (String) null);

        while(rs.next())
            {
                columnList.add(rs.getString(4));
            }

        return columnList.toArray(new String[columnList.size()]);
    }

    /**
     * Return the given String array as a comma separated String
     *
     * @param columns an array of columns to format
     * @return a comma separated String of the column names
     */
    private static String columnListAsString(String[] columns) {
        if (columns == null) {
            return "*";
        }

        StringBuffer sb = new StringBuffer(columns[0]);

        for (int i = 1; i < columns.length; i++ ) {
            sb.append("," + columns[i]);
        }

        return sb.toString();
    }

    /**
     * Format the table arguments used by the various assert* methods for
     * printing.
     */
    private static String formatArgs(Connection c,
                                     String schema,
                                     String table,
                                     String[] columns) throws SQLException {

        return
            formatArgs(c, schema, table) +
            "(" + columnListAsString(columns) + ")";
    }


    /**
     * Format the dbObject arguments used by the various assert* methods for
     * printing.
     */
    private static String formatArgs(Connection c,
                                     String schema,
                                     String dbObject)  throws SQLException {

        ResultSet rs;
        Statement stm =  c.createStatement();
        rs = stm.executeQuery("values current_user");
        rs.next();
        String user = rs.getString(1);

        rs = c.createStatement().executeQuery("values current_role");
        rs.next();
        String role = rs.getString(1);
        rs.close();
        stm.close();

        return
            "User: " + user +
            (role == null ? "" : " Role: " + role) +
            " Object: " +
            schema + "." +
            dbObject;
    }

    /**
     * Set the given role for the current session.
     */
    private void setRole(Connection c, String role) throws SQLException {
        PreparedStatement ps;

        if (role.toUpperCase().equals("NONE")) {
            ps = c.prepareStatement("set role none");
        } else {
            ps = c.prepareStatement("set role ?");
            ps.setString(1, role);
        }

        ps.execute();
        ps.close();
    }

    /**
     * Perform a bulk grant or revoke action for grantee
     */
    private void doGrantRevoke(int action,
                               String grantor,
                               String[] actionStrings,
                               String grantee,
                               String[] warningExpected)
            throws SQLException {
        Connection c = openUserConnection(grantor);
        Statement s = c.createStatement();

        for (int i=0; i < actionStrings.length; i++) {
            s.execute(
                (action == GRANT ? "grant " : "revoke ") +
                actionStrings[i] +
                (action == GRANT ? " to " : " from ") +
                grantee +
                (action == REVOKE && actionStrings[i].startsWith
                 ("execute") ? " restrict" : ""));

            if (warningExpected[i] != null) {
                assertSQLState(warningExpected[i], s.getWarnings());
            }
        }

        s.close();
        c.close();
    }


    /**
     * Perform a bulk grant or revoke action for grantee
     */
    private void doGrantRevoke(int action,
                               String grantor,
                               String[] actionStrings,
                               String grantee)
            throws SQLException {
        String[] warns = new String[actionStrings.length];
        doGrantRevoke(action, grantor, actionStrings, grantee, warns);
    }

    /**
     * Perform a bulk grant or revoke action for grantee
     */
    private void doGrantRevoke(int action,
                               String grantor,
                               String actionString,
                               String grantee,
                               String warningExpected) throws SQLException {
        doGrantRevoke(action, grantor, new String[] {actionString}, grantee,
                      new String[]{warningExpected});
    }

    /**
     * Perform a bulk grant or revoke action for grantee
     */
    private void doGrantRevoke(int action,
                               String grantor,
                               String actionString,
                               String grantee) throws SQLException {
        doGrantRevoke(action, grantor, new String[] {actionString}, grantee);
    }


    private String CNFUser2user(String CNFUser) {
        for (int i = 0; i < users.length; i++) {
            if (JDBC.identifierToCNF(users[i]).equals(CNFUser)) {
                return users[i];
            }
        }
        fail("test error");
        return null;
    }

    private void assertSQLState(String[] ok_states, SQLException e) {
        String state = e.getSQLState();
        boolean found = false;

        for (int i = 0; i < ok_states.length; i++) {
            if (ok_states[i].equals(state)) {
                found = true;
            }
        }

        if (!found) {
            StringBuffer b = new StringBuffer();
            b.append("Exception ");
            b.append(state);
            b.append(" found, one of ");
            for (int i =  0; i < ok_states.length; i++) {
                b.append(ok_states[i]);
                if (i !=  ok_states.length - 1) {
                    b.append('|');
                }
            }
            b.append(" expected");
            fail(b.toString());
        }
    }

    public static void calledNested()
            throws SQLException
    {
        Connection c = null;

        try {
            c = DriverManager.getConnection("jdbc:default:connection");
            Statement cStmt = c.createStatement();

            // CREATE TABLE
            cStmt.executeUpdate
                ("create table t(role varchar(128) default current_role)");
            cStmt.executeUpdate("insert into t values default");
            ResultSet rs = cStmt.executeQuery("select * from t");
            JDBC.assertSingleValueResultSet(rs, "\"H\"");
            rs.close();
            cStmt.executeUpdate("drop table t");

            // ALTER TABLE
            cStmt.executeUpdate("create table t(i int)");
            cStmt.executeUpdate("insert into t values 1");
            cStmt.executeUpdate
                ("alter table t " +
                 "add column role varchar(10) default current_role");
            rs = cStmt.executeQuery("select * from t");
            JDBC.assertFullResultSet(rs, new String[][]{{"1",  "\"H\""}});
            rs.close();
            cStmt.executeUpdate("drop table t");
            cStmt.close();
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Exception e) {
                }
            }
        }
    }


    public static String getCurrentRole()
            throws SQLException
    {
        Connection c = null;

        try {
            c = DriverManager.getConnection("jdbc:default:connection");
            Statement cStmt = c.createStatement();

            ResultSet rs = cStmt.executeQuery("values current_role");
            rs.next();
            String result = rs.getString(1);
            rs.close();
            cStmt.close();
            return result;
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Exception e) {
                }
            }
        }
    }
}


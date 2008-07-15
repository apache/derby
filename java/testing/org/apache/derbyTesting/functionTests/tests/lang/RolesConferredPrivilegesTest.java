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

    private final static String[] users = {"test_dbo", "DonaldDuck"};

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
                        ("create table s1.t1(" +
                         "c1 int unique, c2 int unique, c3 int unique, " +
                         "primary key (c1,c2,c3))");
                    // We made columns all unique so we can test references
                    // privilege for all columns.
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
    private final static String g_i    = "insert on s1.t1 ";
    private final static String g_s    = "select on s1.t1 ";
    private final static String g_s_c1 = "select (c1) on s1.t1 ";
    private final static String g_s_c2 = "select (c2) on s1.t1 ";
    private final static String g_s_c3 = "select (c3) on s1.t1 ";
    private final static String g_d    = "delete on s1.t1 ";
    private final static String g_t    = "trigger on s1.t1 ";
    private final static String g_e    = "execute on function s1.f1 ";

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
    public void testConferredPrivileges() throws SQLException
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
        assertDeletePrivilege
            (hasPrivilege, c, schema, table);
        assertInsertPrivilege
            (hasPrivilege, c, schema, table, null);
        assertUpdatePrivilege
            (hasPrivilege, c, schema, table, columns);
        assertUpdatePrivilege
            (hasPrivilege, c, schema, table, null);
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
        assertSelectPrivilege
            (hasPrivilege, c, schema, table, columns, NOCOLUMNPERMISSION);
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
        DatabaseMetaData dbmd = getConnection().getMetaData();
        ArrayList columnList = new ArrayList();
        ResultSet rs =
            dbmd.getColumns( (String) null, schema, table, (String) null);

        while(rs.next())
            {
                columnList.add(rs.getString(4));
            }

        return (String[]) columnList.toArray(new String[]{});
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
     * Set the given role for the current session, or NONE if null
     */
    private void setRole(Connection c, String role) throws SQLException {
        // if (role == null) {   // Cf. discussion in DERBY-3137.
        //     role = "none";
        // }
        if (role != null &&  JDBC.identifierToCNF(role).equals("NONE")) {
            role = null;
        }

        PreparedStatement ps = c.prepareStatement("set role ?");
        ps.setString(1, JDBC.identifierToCNF(role));
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
                               String warningExpected)
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

            if (warningExpected != null) {
                assertSQLState(warningExpected, s.getWarnings());
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
        doGrantRevoke(action, grantor, actionStrings, grantee, null);
    }
 }

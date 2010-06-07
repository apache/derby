/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SQLSessionContextTest

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
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

/**
 * SQLSessionContextTest tests the SQL session context stacking,
 * cf. SQL 4.37.3. The testing of one of the SQL session state
 * variable, the current role, relies on sqlAuthorization being set,
 * so use sqlAuthorization=true.
 *
 * State variables tested:
 *   current role            (SET ROLE <role>, CURRENT_ROLE)
 *   current default schema  (SET SCHEMA <schema>, CURRENT SCHEMA)
 *
 * SET SCHEMA and SET ROLE are SQL SESSION statements and are not
 * transaction-initiating, cf. SQL section 4.33.
 *
 * The tests are run in the cross product:
 *
 *    {client/server, embedded} x
 *    {data base owner}
 *
 * See also RoutinesDefinersRightsTest, which tests the current user part of
 * the SQLSessionContext.
 */
public class SQLSessionContextTest extends BaseJDBCTestCase
{
    /* internal state */
    private Connection _conn;
    private Statement _stm;

    private final static String pwSuffix = "pwSuffix";

    /**
     * SQL states used by test
     */
    private final static String userException = "38000";

    /**
     * The TestConfiguration.sqlAuthorizationDecorator decorator presumes
     * TEST_DBO as dbo, so add it to set of valid users. It uses a fresh db
     * 'dbsqlauth', not 'wombat'.
     */
    private final static String[] users = {"TEST_DBO"};

    /**
     * SQL keywords for session context state variables
     */
    private final static String[] variableKeywords =
        new String[]{"role", "schema"};

    /**
     * SQL prefix used to retrieve current value of a session context
     * state variable. Cf. variableKeywords.
     */
    private final static String[] currentPrefix =
        new String[]{"current_", "current "};

    /**
     * Create a new instance of SQLSessionContextTest.
     *
     * @param name Fixture name
     */

    public SQLSessionContextTest(String name)
    {
        super(name);
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
        TestSuite suite = new TestSuite("SQLSessionContextTest");

        /* Positive tests */
        if (!JDBC.vmSupportsJSR169()) {
            // JSR169 cannot run with tests with stored procedures
            // that do database access - for they require a
            // DriverManager connection to jdbc:default:connection;
            // DriverManager is not supported with JSR169.
            suite.addTest(
                positiveSuite("suite: positive, embedded"));

            suite.addTest(
                TestConfiguration.clientServerDecorator(
                    positiveSuite("suite: positive, client")));
        }

        return suite;
    }

    /**
     * Construct suite of positive tests
     *
     * @param framework Derby framework indication
     *
     * @return A suite containing the positive test cases incarnated only
     * for security level sqlAuthorization.
     */
    private static Test positiveSuite(String framework)
    {
        /* Tests running with sql authorization set.
         * First decorate with users, then with authentication +
         * sqlAuthorization.
         */

        // add decorator for different users authenticated
        TestSuite usersSuite =
            new TestSuite("suite: positiveSuite");

        // First decorate with users, then with authorization
        // decorator
        for (int userNo = 0; userNo < users.length; userNo++) {
            usersSuite.addTest
                (TestConfiguration.changeUserDecorator
                 (new SQLSessionContextTest("testPositive"),
                  users[userNo],
                  users[userNo].concat(pwSuffix)));
        }

        return TestConfiguration.sqlAuthorizationDecorator(
            DatabasePropertyTestSetup.builtinAuthentication(
                usersSuite, users, pwSuffix));
    }

    /**
     * Positive tests for SQL session state.
     *
     * @throws SQLException
     */
    public void testPositive() throws SQLException
    {
        PreparedStatement[]ps = new PreparedStatement[]{null, null};

        for (int i= 0; i < variableKeywords.length; i++) {
            String curr = currentPrefix[i] + variableKeywords[i];
            ps[i] = _conn.prepareStatement("values " + curr);
        }

        println("testPositive user="+getTestConfiguration().getUserName());

        /*
         * SET SESSION STATE variables
         */
        _stm.executeUpdate("set role outermost");
        _stm.executeUpdate("set schema outermost");

        /**
         * The SQL standard requires we have an SQL session context
         * see section 4.27.3, which is stacked. The stack is pushed
         * at entry to a stored procedure or function.  The current
         * SQL session state is popped at end of stored procedure.  We
         * test two levels deep.
         */
        _stm.execute("call test_dbo.p2()");

        // Dynamic result set: At what time should CURRENT_<state> be
        // evaluated?  Logically at the inside (inside the nested
        // SQL session context), so it should be "LEVEL2" also when
        // accessed on outside (outer SQL session context) I
        // think. Anyway, that's what's implemented: the
        // activation of the call is still live and holds (the
        // final state of) the nested SQL session context
        // even when the procedure call has
        // returned.
        ResultSet prs = _stm.getResultSet(); // role
        assertCurrent("role", prs, "LEVEL2");

        _stm.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        prs = _stm.getResultSet();
        assertCurrent("schema", prs, "LEVEL2");
        prs.close();

        // check that state didn't get impacted by change inside p2
        //
        for (int i= 0; i < variableKeywords.length; i++) {
            ResultSet rs = ps[i].executeQuery();
            assertCurrent(variableKeywords[i], rs, "OUTERMOST");
            rs.close();
        }

        ResultSet rs = _stm.executeQuery("values test_dbo.f2()");


        // Gotcha: we need to fetch first row for embedded driver
        // to actually call f2. The client driver prefetches, so
        // call to f2 will get performed even without the next():
        rs.next();
        rs.close();

        // check that state didn't get impacted by change inside f2
        //
        for (int i= 0; i < variableKeywords.length; i++) {
            rs = ps[i].executeQuery();
            assertCurrent(variableKeywords[i], rs, "OUTERMOST");
            rs.close();
        }

        // check that f2 doesn't impact f22
        rs = _stm.executeQuery("values test_dbo.f2() + test_dbo.f22()");
        rs.next();
        rs.close();

        // check that pushed SQL session context was popped also
        // when the callee gets an exception
        try {
            rs = _stm.executeQuery("values test_dbo.f3()");
            rs.next();
        } catch (SQLException e) {
            assertSQLState(userException, e);
        }

        for (int i= 0; i < 1; i++) {
            rs = ps[i].executeQuery();
            assertCurrent(variableKeywords[i], rs, "OUTERMOST");
            rs.close();
        }

        // Test that when a nested routine drops a role/schema the
        // caller's stacked value is correctly reset. See also javadoc
        // for dropper.
        _stm.execute("call test_dbo.dropper()");

        String[] expected = new String[]{null, "TEST_DBO"};
        for (int i= 0; i < 1; i++) {
            rs = ps[i].executeQuery();
            assertCurrent(variableKeywords[i], rs, expected[i]);
            rs.close();
        }

        for (int i= 0; i < variableKeywords.length; i++) {
            ps[i].close();
        }

        // DERBY-3897: See
        //
        // RolesConferredPrivilegesTest#testDefaultCurrentRole and
        // RolesConferredPrivilegesTest#testCurrentRoleInWeirdContexts
        //
        // which are also relevant tests for SQLSessionContext.
    }


    protected void setUp() throws Exception
    {
        super.setUp();

        _stm = createStatement();
        _conn = getConnection();

        _stm.executeUpdate("create role outermost");
        _stm.executeUpdate("create role level2");
        _stm.executeUpdate("create role innermost");
        _stm.executeUpdate("create schema outermost");
        _stm.executeUpdate("create schema level2");
        _stm.executeUpdate("create schema innermost");

        _stm.executeUpdate(
            "create procedure p2()" +
            "  dynamic result sets 2 language java parameter style java"+
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.p2'" +
            "  modifies sql data");
        _stm.executeUpdate(
            "create function f2()" +
            "  returns int language java parameter style java" +
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.f2'" +
            "  reads sql data");
        _stm.executeUpdate(
            "create function f22()" +
            "  returns int language java parameter style java" +
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.f22'" +
            "  reads sql data");
        _stm.executeUpdate(
            "create function f3()" +
            "  returns int language java parameter style java" +
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.f3'" +
            "  reads sql data");
        _stm.executeUpdate(
            "create procedure calledNestedFromP2(state VARCHAR(255))" +
            "  language java parameter style java" +
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.calledNestedFromP2'"
            +
            "  modifies sql data");
        _stm.executeUpdate(
            "create procedure dropper()" +
            "  language java parameter style java" +
            "  external name 'org.apache.derbyTesting." +
            "functionTests.tests.lang.SQLSessionContextTest.dropper'" +
            "  modifies sql data");
    }


    protected void tearDown() throws Exception
    {
        try {
            _stm.executeUpdate("set schema test_dbo");
            // "OUTERMOST" role/schema is dropped as part of the test,
            // cf. stored procedure 'dropper'.
            _stm.executeUpdate("drop role level2");
            _stm.executeUpdate("drop role innermost");
            _stm.executeUpdate("drop schema level2 restrict");
            _stm.executeUpdate("drop schema innermost restrict");
            _stm.executeUpdate("drop procedure p2");
            _stm.executeUpdate("drop function f2");
            _stm.executeUpdate("drop function f22");
            _stm.executeUpdate("drop function f3");
            _stm.executeUpdate("drop procedure calledNestedFromP2");
            _stm.executeUpdate("drop procedure dropper");
        }
        finally {
            if (_stm != null) {
                _stm = null;
            }

            if (_conn != null) {
                _conn = null;
            }

            super.tearDown();
        }

    }


    private static void assertCurrent(String sessionVar,
                                      ResultSet rs,
                                      String expected)
            throws SQLException
    {
        assertTrue("result set empty", rs.next());
        String actualCurrent = rs.getString(1);

        if (sessionVar.equals("role") && expected != null) {
            // returned current_role is a delimited identifer, which is SQL
            // standard compliant. current_schema returns case normal form,
            // which is not.
            expected = "\"" + expected + "\"";
        }

        if (expected != null) {
            assertTrue(sessionVar + ": current is " + actualCurrent +
                       ", expected " + expected,
                       expected.equals(actualCurrent));
        } else {
            assertTrue(sessionVar + ": current is " + actualCurrent +
                       ", expected null",
                       actualCurrent == null);
        }



        // cardinality should be 1
        assertFalse("result set not empty", rs.next());
    }


    /**
     * Utility procedure used to test that current session state is
     * stacked correctly in a nested SQL session context.
     */
    public static void p2(ResultSet[] rs1, ResultSet[] rs2)
            throws Throwable
    {
        Connection conn1 = null;
        Connection conn2 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");

            for (int i= 0; i < variableKeywords.length; i++) {

                String curr = currentPrefix[i] + variableKeywords[i];

                PreparedStatement ps =
                    conn1.prepareStatement("values " + curr);

                // check that we inherit context correctly
                // current role, cf. SQL 4.34.1.1 and 4.27.3
                // current default schema, cf. SQL 4.37.3 and 4.37.5
                ResultSet rs = ps.executeQuery();
                assertCurrent(variableKeywords[i], rs, "OUTERMOST");
                rs.close();

                // set the state to "LEVEL2"
                Statement stm = conn1.createStatement();
                stm.executeUpdate("set " + variableKeywords[i] + " level2");
                rs = ps.executeQuery();

                // check that state got set
                assertCurrent(variableKeywords[i], rs, "LEVEL2");

                // another nesting level to test session context stack
                // even more
                conn1.commit(); // need to be idle
                stm.executeUpdate("call test_dbo.calledNestedFromP2('"
                                  + variableKeywords[i] + "')");

                rs = ps.executeQuery();

                // check that state didn't get impacted by change inside
                // calledNestedFromP2 to 'INNERMOST':
                assertCurrent(variableKeywords[i], rs, "LEVEL2");
                rs.close();

                // Test that the state is shared by another nested
                // connection also.
                conn2 = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement ps2 =
                    conn2.prepareStatement("values " + curr);
                rs = ps2.executeQuery();
                assertCurrent(variableKeywords[i], rs, "LEVEL2");
                rs.close();
                ps2.close();
            }

            for (int i= 0; i < variableKeywords.length; i++) {
                // Pass out CURRENT <state> in a dynamic result set.
                String curr = currentPrefix[i] + variableKeywords[i];
                PreparedStatement ps =
                    conn1.prepareStatement("values " + curr);
                ResultSet rs = ps.executeQuery();

                if (variableKeywords[i].equals("role")) {
                    rs1[0] = rs;
                } else {
                    rs2[0] = rs;
                }
            }
        } catch (Throwable e) {
            // Print here if debug, since on outside we only see 38000
            // (user error)
            println("err: " + e);

            throw e;

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
    public static void calledNestedFromP2(String stateString)
            throws SQLException
    {
        Connection conn1 = null;
        Connection conn2 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");

            // CURRENT_ROLE  vs CURRENT SCHEMA syntax:
            String curr = stateString.equals("role")
                ? "current_role"
                : "current schema";

            PreparedStatement ps =
                conn1.prepareStatement("values " + curr);

            // check that we inherit state correctly
            ResultSet rs = ps.executeQuery();
            assertCurrent(stateString, rs, "LEVEL2");
            rs.close();

            // set the state to "INNERMOST"
            Statement stm = conn1.createStatement();
            stm.executeUpdate("set " + stateString + " innermost");
            rs = ps.executeQuery();

            // check that state got set
            assertCurrent(stateString, rs, "INNERMOST");
            rs.close();
            ps.close();

            // Test that the session context is shared by another
            // nested connection also.
            conn2 = DriverManager.getConnection("jdbc:default:connection");
            ps = conn2.prepareStatement("values " + curr);
            rs = ps.executeQuery();
            assertCurrent(stateString, rs, "INNERMOST");
            rs.close();
            ps.close();

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
     * Utility function used to test that state variables are stacked
     * correctly in a nested SQL session context.
     */
    public static int f2() throws SQLException
    {
        Connection conn1 = null;
        Connection conn2 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");

            for (int i= 0; i < variableKeywords.length; i++) {
                String curr = currentPrefix[i] + variableKeywords[i];

                PreparedStatement ps =
                    conn1.prepareStatement("values " + curr);

                // check that we inherit context correctly
                ResultSet rs = ps.executeQuery();
                assertCurrent(variableKeywords[i], rs, "OUTERMOST");
                rs.close();

                // set the state to "LEVEL2"
                Statement stm = conn1.createStatement();
                stm.executeUpdate("set " + variableKeywords[i] + " level2");
                rs = ps.executeQuery();

                // check that state got set
                assertCurrent(variableKeywords[i], rs, "LEVEL2");

                ps.close();

                // Test that the state is shared by another nested
                // connection also.
                conn2 = DriverManager.getConnection("jdbc:default:connection");
                ps = conn2.prepareStatement("values " + curr);
                rs = ps.executeQuery();
                assertCurrent(variableKeywords[i], rs, "LEVEL2");
                rs.close();
                ps.close();
            }

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

        return 1;
    }


    /**
     * Used to test that a parallel (subsequent) function call in the
     * same statement (called after f2 which changes state) is not
     * affected by the preceding function (f2) call's changes.
     *
     */
    public static int f22() throws SQLException
    {
        Connection conn1 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");

            for (int i= 0; i < variableKeywords.length; i++) {
                String curr = currentPrefix[i] + variableKeywords[i];

                PreparedStatement ps =
                    conn1.prepareStatement("values " + curr);

                // check that we inherit context correctly
                ResultSet rs = ps.executeQuery();
                assertCurrent(variableKeywords[i], rs, "OUTERMOST");
                rs.close();
                ps.close();
            }

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


    /**
     * Utility function used to test that current state is stacked
     * correctly in a nested SQL session context.  This particular
     * function will throw and exception. This is used to test that
     * the cleaning up pops the SQL session stack also.
     */
    public static int f3() throws SQLException
    {
        Connection conn1 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");
            // set the state to something else
            Statement stm = conn1.createStatement();
            stm.executeUpdate("set role level2");
            stm.executeUpdate("set schema level2");

            PreparedStatement ps =
                conn1.prepareStatement("values 42");
            ResultSet rs = ps.executeQuery();
            rs.next();
            // force an exception, rs. is empty, next should throw now
            rs.next();
            String s = rs.getString(1);
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


    /**
     * Test that when a nested routine drops a role/schema, the
     * current value is correctly reset. For roles, the current role
     * is unchanged, since it is lazily checked (and potentially reset
     * to NONE if it no longer exists or it is no longer granted to
     * session user) only when it is attempted used for anything. For
     * schema, the current schema should revert back to the session's
     * default schema. This holds for all frames on the session
     * context stack (see also caller's check).
     */
    public static void dropper() throws SQLException
    {
        Connection conn1 = null;

        try {
            conn1 = DriverManager.getConnection("jdbc:default:connection");

            // Drop current contexts
            Statement stm = conn1.createStatement();
            stm.executeUpdate("drop role outermost");
            stm.executeUpdate("drop schema outermost restrict");
            stm.close();

            String[] expected = new String[]{null, "TEST_DBO"};

            // check that we revert correctly
            for (int i= 0; i < variableKeywords.length; i++) {
                String curr = currentPrefix[i] + variableKeywords[i];

                PreparedStatement ps =
                    conn1.prepareStatement("values " + curr);

                ResultSet rs = ps.executeQuery();
                assertCurrent(variableKeywords[i], rs, expected[i]);
                rs.close();
                ps.close();
            }
        } finally {
            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (Exception e) {
                }
            }
        }
    }
}

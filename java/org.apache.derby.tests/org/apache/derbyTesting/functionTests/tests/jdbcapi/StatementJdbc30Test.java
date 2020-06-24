/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.StatementJdbc30Test

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the Statement class in JDBC 30. This test converts the old
 * jdbcapi/statementJdbc30.java test to JUnit.
 */

public class StatementJdbc30Test extends BaseJDBCTestCase {
    private static final String CLIENT_SUITE_NAME = 
        "StatementJdbc30Test:client";
//IC see: https://issues.apache.org/jira/browse/DERBY-3198

    /**
     * Create a test with the given name.
     * 
     * @param name
     *            name of the test.
     */

    public StatementJdbc30Test(String name) {
        super(name);
    }

    /**
     * Create suite containing client and embedded tests and to run all tests in
     * this class
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("StatementJdbc30Test");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(baseSuite("StatementJdbc30Test:embedded"));
        suite
                .addTest(TestConfiguration
                        .clientServerDecorator(baseSuite(CLIENT_SUITE_NAME)));
//IC see: https://issues.apache.org/jira/browse/DERBY-3198

        return suite;
    }

    private static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(name);
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTestSuite(StatementJdbc30Test.class);

//IC see: https://issues.apache.org/jira/browse/DERBY-3198
        if  (name.equals(CLIENT_SUITE_NAME)) {
            // These test CAN be run in embedded mode as well, but
            // they're only meaningful in c/s mode and also take quite
            // a bit of time to run.
            suite.addTest(new StatementJdbc30Test
                          ("xtestMultiExecWithQueryTimeout"));
            suite.addTest(new StatementJdbc30Test
                          ("xtestMaxOpenStatementsWithQueryTimeout"));
        }

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables used in the test cases.
             * 
             * @exception SQLException
             *                if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {

                /**
                 * Creates the table used in the test cases.
                 * 
                 */
                stmt.execute("create table tab1 (i int, s smallint, r real)");
                stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");
            }
        };
    }

    /**
     * Tests reading data from database
     * 
     * @exception SQLException
     *                if error occurs
     */
    public void testReadingData() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs;

        // read the data just for the heck of it
        rs = stmt.executeQuery("select * from tab1");
        assertTrue(rs.next());

        rs.close();
    }

    /**
     * Tests stmt.getMoreResults(int)
     * 
     * @exception SQLException
     *                if error occurs
     */
    public void testGetMoreResults() throws SQLException {

        Statement stmt = createStatement();
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
//IC see: https://issues.apache.org/jira/browse/DERBY-3484

    }

    /**
     * Tests stmt.executeUpdate(String, int) with NO_GENERATED_KEYS.
     * 
     * @exception SQLException
     *                if error occurs
     */
    public void testInsertNoGenKeys() throws SQLException {

        Statement stmt = createStatement();
        stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)",
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
                Statement.NO_GENERATED_KEYS);
        assertNull("Expected NULL ResultSet after stmt.execute()", stmt
                .getGeneratedKeys());

    }

    /**
     * Tests stmt.executeUpdate(String, int[]) After doing an insert into a
     * table that doesn't have a generated column, the test should fail.
     * 
     * @throws SQLException
     */
    public void testExecuteUpdateNoAutoGenColumnIndex() throws SQLException {

        Statement stmt = createStatement();

        int[] columnIndexes = new int[2];
        columnIndexes[0] = 1;
        columnIndexes[1] = 2;
        try {
            stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)",
                    columnIndexes);
            fail("FAIL -- executeUpdate should have failed...");
        } catch (SQLException ex) {
            assertFailedExecuteUpdateForColumnIndex(ex);
        }
    }

    /**
     * Tests stmt.executeUpdate(String, String[]) After doing an insert into a
     * table that doesn't have a generated column, the test should fail.
     * 
     * @throws SQLException
     */
    public void testExecuteUpdateNoAutoGenColumnName() throws SQLException {

        Statement stmt = createStatement();

        String[] columnNames = new String[2];
        columnNames[0] = "I";
        columnNames[1] = "S";
        try {
            stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)",
                    columnNames);
            fail("FAIL -- executeUpdate should have failed...");
        } catch (SQLException ex) {
            assertFailedExecuteUpdateForColumnName(ex);
        }
    }

    /**
     * Tests stmt.execute(String, int) with NO_GENERATED_KEYS.
     * 
     * @exception SQLException
     *                if error occurs
     */
    public void testSelectNoGenKeys() throws SQLException {

        Statement stmt = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
        stmt.execute("select * from tab1", Statement.NO_GENERATED_KEYS);
        assertNull("Expected NULL ResultSet after stmt.execute()", stmt
                .getGeneratedKeys());

    }

    /**
     * After doing an insert into a table that doesn't have a generated column,
     * the test should fail.
     * 
     * @throws SQLException
     */
    public void testExecuteNoAutoGenColumnIndex() throws SQLException {

        Statement stmt = createStatement();

        int[] columnIndexes = new int[2];
        columnIndexes[0] = 1;
        columnIndexes[1] = 2;
        try {
            stmt.execute("insert into tab1 values(2, 3, 4.1)", columnIndexes);
            fail("FAIL -- executeUpdate should have failed...");
        } catch (SQLException ex) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2942
//IC see: https://issues.apache.org/jira/browse/DERBY-2942
            assertFailedExecuteUpdateForColumnIndex(ex);
        }
    }

    /**
     * Assert executeUpdateForColumnIndex failed. There are different SQLStates 
     * for ColumnName(X0X0E) and ColumnIndex(X0X0F) as well as client and server
     * 
     * @param ex
     */
    private void assertFailedExecuteUpdateForColumnIndex(SQLException ex) {
        // In network client we only check columnIndex array length,
        // so throw a different error.
        if (usingDerbyNetClient()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2653
            assertSQLState("X0X0D", ex);
        } else {
            assertSQLState("X0X0E", ex);
        }
    }

    /**
     * Assert executeUpdateForColumnName failed. There are different SQLStates 
     * for ColumnIndex(X0X0F) and ColumnNam(X0X0E) as well as client and server.
     *
     * @param ex
     */
    private void assertFailedExecuteUpdateForColumnName(SQLException ex) {
        // Derby client complains that the array is too long.
        // Embedded is smart enough to know which column caused the problem.
        if (usingDerbyNetClient()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2653
            assertSQLState("X0X0D", ex);
        } else {
            assertSQLState("X0X0F", ex);
        }
    }
    /**
     * After doing an insert into a table that doesn't have a generated column,
     * the test should fail.
     * 
     * @throws SQLException
     */
    public void testExecuteNoAutoGenColumnName() throws SQLException {

        Statement stmt = createStatement();
        
            String[] columnNames = new String[2];
            columnNames[0] = "I";
            columnNames[1] = "S";
            try {
                stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)",
                        columnNames);
                fail("FAIL -- executeUpdate should have failed...");
            } catch (SQLException ex) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2942
//IC see: https://issues.apache.org/jira/browse/DERBY-2942
                assertFailedExecuteUpdateForColumnName(ex);
            }
        
    }

    /**
     * DERBY-3198: Verify that a statement can be executed
     * more than 32000 times, even when query timeout is enabled.
     */
    public void xtestMultiExecWithQueryTimeout() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3198
        Statement stmt = createStatement();
        stmt.setQueryTimeout(10);
        for (int i = 0; i < 33000; ++i) {
            ResultSet rs = stmt.executeQuery("VALUES(1)");
            rs.close();
        }
    }

    /**
     * DERBY-3198: Verify that we can have at least 16383 open Statements with
     * query timeout. With query timeout, each Statement holds on to 2
     * Section objects until it is closed.
     */
    public void xtestMaxOpenStatementsWithQueryTimeout() throws SQLException {
        // Disable auto-commit for this test case. Otherwise, closing all the
        // statements in tearDown() will take forever, since every close() will
        // force a commit. DERBY-5524.
        setAutoCommit(false);

        Statement[] stmts = new Statement[16500];
        int i = 0;
        try {
            for (; i < 16500; ++i) {
                stmts[i] = createStatement();
                stmts[i].setQueryTimeout(10);
                stmts[i].executeQuery("VALUES(1)");
            }
        } catch (SQLException e) {
            assertSQLState("XJ200",e);
            assertTrue("16383 >= (i="+ i +")", 16383 >= i);
        }  
    }


    /**
     * Testing stmt.getResultSetHoldability()
     * 
     * @throws SQLException
     */
    public void testGetResultSetHoldability() throws SQLException {

        Statement stmt = createStatement();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt
                .getResultSetHoldability());

    }

    /**
     * Testing stmt.getGeneratedKeys()
     * 
     * @throws SQLException
     */
    public void testGetGenerateKeys() throws SQLException {

        Statement stmt = createStatement();
        assertNull(stmt.getGeneratedKeys());

    }
}

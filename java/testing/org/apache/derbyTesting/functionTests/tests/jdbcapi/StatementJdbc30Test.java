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
import junit.framework.TestSuite;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the Statement class in JDBC 30. This test converts the old
 * jdbcapi/statementJdbc30.java test to JUnit.
 */

public class StatementJdbc30Test extends BaseJDBCTestCase {

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
        TestSuite suite = new TestSuite("StatementJdbc30Test");

        suite.addTest(baseSuite("StatementJdbc30Test:embedded"));
        suite
                .addTest(TestConfiguration
                        .clientServerDecorator(baseSuite("StatementJdbc30Test:client")));

        return suite;
    }

    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);

        suite.addTestSuite(StatementJdbc30Test.class);

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
        assertFalse(stmt.getMoreResults(JDBC30Translation.CLOSE_CURRENT_RESULT));

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
                JDBC30Translation.NO_GENERATED_KEYS);
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
        stmt.execute("select * from tab1", JDBC30Translation.NO_GENERATED_KEYS);
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
        /*
         * DERBY-2943 -- execute() and executeUpdate() return different
         * SQLState in embedded and network client
         * 
         */
        if (usingDerbyNetClient()) {
            assertSQLState("0A000", ex);
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
        /*
         * DERBY-2943 -- execute() and executeUpdate() return different
         * SQLState in embedded and network client
         *
         */
        if (usingDerbyNetClient()) {
            assertSQLState("0A000", ex);
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
                assertFailedExecuteUpdateForColumnName(ex);
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
